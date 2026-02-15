mod agg;

use crate::queries::aggregates::agg::Agg;
use crate::queries::orderer::QueryOrderer;
use crate::{
    eval::{EvalError, EvalResult, Interpreter},
    queries::Sources,
    values::QueryValue,
};
use eventql_parser::{
    App, ExprRef, Query, Session, Value,
    prelude::{Type, Typed},
};
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::{mem, vec};

pub trait Aggregate {
    fn fold(&mut self, params: &[QueryValue]);
    fn complete(&self) -> QueryValue;
}

fn instantiate_aggregate(session: &Session, app: &App) -> Agg {
    if let Type::App {
        aggregate: true, ..
    } = session
        .global_scope()
        .get(app.func)
        .expect("func to be defined")
    {
        let fun_name = session.arena().get_str(app.func);
        return if fun_name.eq_ignore_ascii_case("count") {
            Agg::count()
        } else if fun_name.eq_ignore_ascii_case("avg") {
            Agg::avg()
        } else if fun_name.eq_ignore_ascii_case("unique") {
            Agg::unique()
        } else {
            unreachable!("impossible as such function wouldn't pass the static analysis")
        };
    }

    panic!("STATIC ANALYSIS BUG: expected an aggregate function but got a regular instead")
}

enum AggKind {
    Regular(HashMap<App, Agg>),
    Grouped {
        base: HashMap<App, Agg>,
        value: Value,
        having: Option<ExprRef>,
        aggs: HashMap<QueryValue, HashMap<App, Agg>>,
    },
}

impl AggKind {
    fn load(session: &Session, query: &Query<Typed>) -> Self {
        let mut aggs = HashMap::new();

        Self::load_expr(&mut aggs, session, query.projection);

        if let Some(group_by) = &query.group_by {
            Self::load_expr(&mut aggs, session, group_by.expr);

            if let Some(predicate) = group_by.predicate {
                Self::load_expr(&mut aggs, session, predicate);
            }

            if let Some(order_by) = query.order_by {
                Self::load_expr(&mut aggs, session, order_by.expr);
            }

            Self::Grouped {
                base: aggs,
                value: session.arena().get_expr(group_by.expr).value,
                having: group_by.predicate,
                aggs: Default::default(),
            }
        } else {
            Self::Regular(aggs)
        }
    }

    fn load_expr(aggs: &mut HashMap<App, Agg>, session: &Session, expr: ExprRef) {
        match session.arena().get_expr(expr).value {
            Value::App(app) => {
                if let Entry::Vacant(entry) = aggs.entry(app) {
                    entry.insert(instantiate_aggregate(session, &app));
                }
            }

            Value::Record(fields) => {
                for field in session.arena().get_rec(fields) {
                    Self::load_expr(aggs, session, field.expr);
                }
            }

            _ => {}
        }
    }
}

#[derive(Default)]
struct EvalAgg {
    buffer: Vec<QueryValue>,
}

impl EvalAgg {
    fn fold(&mut self, interpreter: &Interpreter, kind: &mut AggKind) -> EvalResult<()> {
        match kind {
            AggKind::Regular(aggs) => self.fold_aggs(interpreter, aggs),

            AggKind::Grouped {
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
        }

        self.buffer.clear();

        Ok(())
    }

    fn complete(
        &mut self,
        interpreter: &Interpreter,
        kind: &mut AggKind,
        query: &Query<Typed>,
    ) -> EvalResult<()> {
        match kind {
            AggKind::Regular(aggs) => {
                let value = self.complete_aggs(interpreter, aggs, query.projection)?;
                self.buffer.push(value);
            }

            AggKind::Grouped { aggs, having, .. } => {
                let having = having.as_ref().copied();

                if let Some(order) = query.order_by.map(|o| o.order) {
                    let mut orderer = QueryOrderer::new(order);

                    for (key, aggs) in aggs.iter() {
                        if let Some(predicate) = having {
                            let value = self.complete_aggs(interpreter, aggs, predicate)?;
                            if !matches!(value, QueryValue::Bool(true)) {
                                continue;
                            }
                        }

                        let value = self.complete_aggs(interpreter, aggs, query.projection)?;
                        orderer.insert(key.clone(), value);
                    }

                    orderer.prepare_for_streaming();

                    while let Some(value) = orderer.next() {
                        self.buffer.push(value);
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
                let agg = aggs.get(&app).unwrap();
                Ok(agg.complete())
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

            _ => Err(EvalError::Runtime(
                "unreachable code path in aggregate computation".into(),
            )),
        }
    }
}

pub struct AggQuery<'a> {
    srcs: Sources<'a>,
    interpreter: Interpreter<'a>,
    query: Query<Typed>,
    kind: AggKind,
    agg_eval: EvalAgg,
    completed: bool,
    results: vec::IntoIter<QueryValue>,
}

impl<'a> AggQuery<'a> {
    pub fn new(srcs: Sources<'a>, session: &'a Session, query: Query<Typed>) -> Self {
        let kind = AggKind::load(session, &query);

        Self {
            srcs,
            query,
            kind,
            interpreter: Interpreter::new(session),
            completed: false,
            results: Default::default(),
            agg_eval: Default::default(),
        }
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
                    self.agg_eval
                        .complete(&self.interpreter, &mut self.kind, &self.query)
                {
                    return Some(Err(e));
                }

                self.results = mem::take(&mut self.agg_eval.buffer).into_iter();
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

            if let Err(e) = self.agg_eval.fold(&self.interpreter, &mut self.kind) {
                return Some(Err(e));
            }
        }
    }
}
