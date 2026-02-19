mod agg;

use crate::queries::aggregates::agg::Agg;
use crate::queries::orderer::QueryOrderer;
use crate::{
    eval::{EvalError, EvalResult, Interpreter},
    queries::Sources,
    values::QueryValue,
};
use eventql_parser::{
    App, ExprRef, Limit, Query, Session, Value,
    prelude::{Type, Typed},
};
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::{mem, vec};

fn instantiate_aggregate(session: &Session, app: &App) -> EvalResult<Agg> {
    if let Some(Type::App {
        aggregate: true, ..
    }) = session.global_scope().get(app.func)
    {
        let fun_name = session.arena().get_str(app.func);
        return if fun_name.eq_ignore_ascii_case("count") {
            Ok(Agg::count())
        } else if fun_name.eq_ignore_ascii_case("avg") {
            Ok(Agg::avg())
        } else if fun_name.eq_ignore_ascii_case("unique") {
            Ok(Agg::unique())
        } else {
            Err(EvalError::Runtime(
                format!("unknown aggregate function: {fun_name}").into(),
            ))
        };
    }

    Err(EvalError::Runtime(
        "expected an aggregate function but got a regular function instead".into(),
    ))
}

enum AggLayout {
    Regular(HashMap<App, Agg>),
    Grouped {
        base: HashMap<App, Agg>,
        value: Value,
        having: Option<ExprRef>,
        aggs: HashMap<QueryValue, HashMap<App, Agg>>,
    },
}

impl AggLayout {
    fn load(session: &Session, query: &Query<Typed>) -> EvalResult<Self> {
        let mut aggs = HashMap::new();

        Self::load_expr(&mut aggs, session, query.projection)?;

        if let Some(group_by) = &query.group_by {
            if let Some(predicate) = group_by.predicate {
                Self::load_expr(&mut aggs, session, predicate)?;
            }

            if let Some(order_by) = query.order_by {
                Self::load_expr(&mut aggs, session, order_by.expr)?;
            }

            Ok(Self::Grouped {
                base: aggs,
                value: session.arena().get_expr(group_by.expr).value,
                having: group_by.predicate,
                aggs: Default::default(),
            })
        } else {
            Ok(Self::Regular(aggs))
        }
    }

    fn load_expr(aggs: &mut HashMap<App, Agg>, session: &Session, expr: ExprRef) -> EvalResult<()> {
        match session.arena().get_expr(expr).value {
            Value::App(app) => {
                if let Entry::Vacant(entry) = aggs.entry(app) {
                    entry.insert(instantiate_aggregate(session, &app)?);
                }
            }

            Value::Record(fields) => {
                for field in session.arena().get_rec(fields) {
                    Self::load_expr(aggs, session, field.expr)?;
                }
            }

            Value::Array(arr) => {
                for expr in session.arena().get_vec(arr) {
                    Self::load_expr(aggs, session, *expr)?;
                }
            }

            Value::Binary(binary) => {
                Self::load_expr(aggs, session, binary.lhs)?;
                Self::load_expr(aggs, session, binary.rhs)?;
            }

            Value::Unary(unary) => Self::load_expr(aggs, session, unary.expr)?,
            Value::Group(expr) => Self::load_expr(aggs, session, expr)?,

            _ => {}
        }

        Ok(())
    }
}

#[derive(Default)]
struct AggEvaluator {
    buffer: Vec<QueryValue>,
}

impl AggEvaluator {
    fn fold(&mut self, interpreter: &Interpreter, kind: &mut AggLayout) -> EvalResult<()> {
        match kind {
            AggLayout::Regular(aggs) => self.fold_aggs(interpreter, aggs),

            AggLayout::Grouped {
                base, value, aggs, ..
            } => {
                let key = interpreter.eval(*value)?;
                let aggs = aggs.entry(key).or_insert_with(|| base.clone());

                self.fold_aggs(interpreter, aggs)
            }
        }
    }

    fn fold_aggs(
        &mut self,
        interpreter: &Interpreter,
        aggs: &mut HashMap<App, Agg>,
    ) -> EvalResult<()> {
        for (app, agg) in aggs.iter_mut() {
            for arg in interpreter.session.arena().get_vec(app.args) {
                self.buffer.push(interpreter.eval_expr(*arg)?);
            }

            agg.fold(&self.buffer);
            self.buffer.clear();
        }

        Ok(())
    }

    fn complete(
        &mut self,
        interpreter: &Interpreter,
        kind: &mut AggLayout,
        query: &Query<Typed>,
    ) -> EvalResult<()> {
        match kind {
            AggLayout::Regular(aggs) => {
                let value = self.complete_aggs(interpreter, aggs, query.projection)?;
                self.buffer.push(value);
            }

            AggLayout::Grouped { aggs, having, .. } => {
                let having = having.as_ref().copied();

                if let Some(order_by) = query.order_by {
                    let mut orderer = QueryOrderer::new(order_by.order);

                    for aggs in aggs.values() {
                        if let Some(predicate) = having {
                            let value = self.complete_aggs(interpreter, aggs, predicate)?;
                            if !matches!(value, QueryValue::Bool(true)) {
                                continue;
                            }
                        }

                        let sort_key = self.complete_aggs(interpreter, aggs, order_by.expr)?;
                        let value = self.complete_aggs(interpreter, aggs, query.projection)?;
                        orderer.insert(sort_key, value);
                    }

                    if orderer.prepare_for_streaming().is_some() {
                        while let Some(value) = orderer.next() {
                            self.buffer.push(value);
                        }
                    }
                } else {
                    for aggs in aggs.values() {
                        let value = self.complete_aggs(interpreter, aggs, query.projection)?;
                        self.buffer.push(value);
                    }
                }
            }
        }

        Ok(())
    }

    fn complete_aggs(
        &mut self,
        interpreter: &Interpreter,
        aggs: &HashMap<App, Agg>,
        expr: ExprRef,
    ) -> EvalResult<QueryValue> {
        match interpreter.session.arena().get_expr(expr).value {
            Value::App(app) if aggs.contains_key(&app) => {
                // safe: guarded by contains_key above
                Ok(aggs[&app].complete())
            }

            Value::Array(arr) => {
                let vec = interpreter.session.arena().get_vec(arr);
                let mut values = Vec::with_capacity(vec.len());

                for expr in vec {
                    values.push(self.complete_aggs(interpreter, aggs, *expr)?);
                }

                Ok(QueryValue::Array(values))
            }

            Value::Record(rec) => {
                let mut props = BTreeMap::new();

                for field in interpreter.session.arena().get_rec(rec) {
                    let value = self.complete_aggs(interpreter, aggs, field.expr)?;
                    let name = interpreter.session.arena().get_str(field.name);
                    props.insert(name.to_owned(), value);
                }

                Ok(QueryValue::Record(props))
            }

            Value::Binary(binary) => {
                let lhs = self.complete_aggs(interpreter, aggs, binary.lhs)?;
                let rhs = self.complete_aggs(interpreter, aggs, binary.rhs)?;

                interpreter.eval_binary(binary.operator, &lhs, &rhs)
            }

            Value::Unary(unary) => {
                let value = self.complete_aggs(interpreter, aggs, unary.expr)?;
                interpreter.eval_unary(unary.operator, &value)
            }

            Value::Group(expr) => self.complete_aggs(interpreter, aggs, expr),

            x => interpreter.eval(x),
        }
    }
}

pub struct AggQuery<'a> {
    srcs: Sources<'a>,
    interpreter: Interpreter<'a>,
    query: Query<Typed>,
    layout: AggLayout,
    evaluator: AggEvaluator,
    completed: bool,
    results: vec::IntoIter<QueryValue>,
}

impl<'a> AggQuery<'a> {
    pub fn new(srcs: Sources<'a>, session: &'a Session, query: Query<Typed>) -> EvalResult<Self> {
        let kind = AggLayout::load(session, &query)?;

        Ok(Self {
            srcs,
            query,
            layout: kind,
            interpreter: Interpreter::new(session),
            completed: false,
            results: Default::default(),
            evaluator: Default::default(),
        })
    }
}

impl<'a> Iterator for AggQuery<'a> {
    type Item = EvalResult<QueryValue>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.completed {
                if let Some(result) = self.results.next() {
                    return Some(Ok(result));
                }

                return None;
            }

            let outcome = if let Some(outcome) = self.srcs.fill(self.interpreter.env_mut()) {
                outcome
            } else {
                self.completed = true;
                if let Err(e) =
                    self.evaluator
                        .complete(&self.interpreter, &mut self.layout, &self.query)
                {
                    return Some(Err(e));
                }

                let mut buffer = mem::take(&mut self.evaluator.buffer);
                if let Some(limit) = self.query.limit {
                    match limit {
                        Limit::Skip(n) => {
                            let n = n as usize;
                            if n < buffer.len() {
                                buffer.drain(..n);
                            } else {
                                buffer.clear();
                            }
                        }
                        Limit::Top(n) => {
                            buffer.truncate(n as usize);
                        }
                    }
                }
                self.results = buffer.into_iter();
                continue;
            };

            if let Err(e) = outcome {
                return Some(Err(e));
            }

            match self.interpreter.eval_predicate(&self.query) {
                Ok(true) => {}
                Ok(false) => continue,
                Err(e) => return Some(Err(e)),
            }

            if let Err(e) = self.evaluator.fold(&self.interpreter, &mut self.layout) {
                return Some(Err(e));
            }
        }
    }
}
