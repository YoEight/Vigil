mod agg;

use crate::queries::aggregates::agg::Agg;
use crate::queries::orderer::QueryOrderer;
use crate::{
    eval::{EvalError, EvalResult, Interpreter},
    queries::Sources,
    values::QueryValue,
};
use case_insensitive_hashmap::CaseInsensitiveHashMap;
use eventql_parser::prelude::Expr;
use eventql_parser::{
    App, ExprRef, Order, Query, Session, Value,
    prelude::{Type, Typed},
};
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::vec;

pub trait Aggregate {
    fn fold(&mut self, params: &[QueryValue]);
    fn complete(&self) -> QueryValue;
}

enum AggState {
    Single(Agg),
    Record(CaseInsensitiveHashMap<AggState>),
}

impl AggState {
    fn from(session: &Session, query: &Query<Typed>) -> Self {
        match session.arena().get_expr(query.projection).value {
            eventql_parser::Value::Record(fields) => {
                let mut aggs = CaseInsensitiveHashMap::new();

                for field in session.arena().get_rec(fields) {
                    let field_name = session.arena().get_str(field.name);
                    if let Value::App(app) = session.arena().get_expr(field.expr).value {
                        aggs.insert(
                            field_name,
                            Self::Single(instantiate_aggregate(session, &app)),
                        );
                        continue;
                    }

                    aggs.insert(field_name, Self::Single(Agg::unique()));
                }

                Self::Record(aggs)
            }

            eventql_parser::Value::App(app) => Self::Single(instantiate_aggregate(session, &app)),

            _ => unreachable!("we expect an aggregate expression so this case should never happen"),
        }
    }

    fn complete(&self) -> QueryValue {
        match self {
            AggState::Single(agg) => agg.complete(),
            AggState::Record(aggs) => {
                let mut props = BTreeMap::new();

                for (key, agg) in aggs.iter() {
                    props.insert(key.as_ref().to_owned(), agg.complete());
                }

                QueryValue::Record(props)
            }
        }
    }
}

fn instantiate_ordered_aggregate<'a>(
    session: &'a Session,
    order_by: &'a eventql_parser::OrderBy,
) -> Option<AggOrdered> {
    let expr = session.arena().get_expr(order_by.expr);
    if let eventql_parser::Value::App(app) = expr.value {
        Some(AggOrdered {
            expr,
            agg: AggState::Single(instantiate_aggregate(session, &app)),
        })
    } else {
        None
    }
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

struct EvalAgg {
    buffer: Vec<QueryValue>,
}

impl EvalAgg {
    fn fold(&mut self, interpreter: &Interpreter, kind: &mut AggKind) -> EvalResult<()> {
        match kind {
            AggKind::Regular(aggs) => self.fold_aggs(interpreter, aggs),

            AggKind::Grouped { base, value, aggs } => {
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

            AggKind::Grouped { aggs, .. } => {
                if let Some(order) = query.order_by.map(|o| o.order) {
                    let mut orderer = QueryOrderer::new(order);

                    for (key, aggs) in aggs.iter() {
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

            _ => Err(EvalError::Runtime(
                "unreachable code path in aggregate computation".into(),
            )),
        }
    }
}

enum Emit {
    Single(AggState),
    Grouped {
        ordered: Option<Order>,
        aggs: HashMap<String, AggGroup>,
    },
}

struct AggGroup {
    ordered: Option<AggOrdered>,
    state: AggState,
}

impl AggGroup {
    fn update_order_agg(&mut self, interpreter: &Interpreter) -> EvalResult<()> {
        if let Some(agg) = &mut self.ordered {
            eval_agg_value(interpreter, agg.expr.value, &mut agg.agg)?;
        }

        Ok(())
    }
}

struct AggOrdered {
    expr: Expr,
    agg: AggState,
}

pub struct AggQuery<'a> {
    srcs: Sources<'a>,
    interpreter: Interpreter<'a>,
    query: Query<Typed>,
    emit: Emit,
    completed: bool,
    session: &'a Session,
    results: vec::IntoIter<QueryValue>,
}

impl<'a> AggQuery<'a> {
    pub fn new(srcs: Sources<'a>, session: &'a Session, query: Query<Typed>) -> Self {
        let emit = if query.group_by.is_some() {
            Emit::Grouped {
                ordered: query.order_by.as_ref().map(|o| o.order),
                aggs: Default::default(),
            }
        } else {
            Emit::Single(AggState::from(session, &query))
        };

        Self {
            srcs,
            query,
            interpreter: Interpreter::new(session),
            emit,
            session,
            completed: false,
            results: Default::default(),
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

                match &self.emit {
                    Emit::Single(agg) => self.results = vec![agg.complete()].into_iter(),
                    Emit::Grouped { ordered, aggs } => {
                        let mut results = Vec::new();
                        if let Some(order) = ordered {
                            let mut orderer = QueryOrderer::new(*order);
                            for group in aggs.values() {
                                let key = if let Some(agg) = &group.ordered {
                                    agg.agg.complete()
                                } else {
                                    QueryValue::Null
                                };
                                orderer.insert(key, group.state.complete())
                            }

                            orderer.prepare_for_streaming()?;
                            while let Some(value) = orderer.next() {
                                results.push(value);
                            }
                        } else {
                            for group in aggs.values() {
                                results.push(group.state.complete());
                            }
                        }

                        self.results = results.into_iter();
                    }
                }

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

            let agg = if let Emit::Grouped { aggs, .. } = &mut self.emit
                && let Some(group_by) = &self.query.group_by
            {
                let group_key = match self
                    .interpreter
                    .eval(self.session.arena().get_expr(group_by.expr).value)
                {
                    Err(e) => return Some(Err(e)),
                    Ok(value) => match value {
                        QueryValue::String(s) => s.clone(),
                        QueryValue::Number(n) => n.to_string(),
                        QueryValue::Bool(b) => b.to_string(),
                        QueryValue::DateTime(date_time) => date_time.to_string(),
                        QueryValue::Date(naive_date) => naive_date.to_string(),
                        QueryValue::Time(naive_time) => naive_time.to_string(),
                        _ => {
                            return Some(Err(EvalError::Runtime(
                                "unexpected group by value".into(),
                            )));
                        }
                    },
                };

                let agg_group = aggs.entry(group_key).or_insert_with(|| AggGroup {
                    ordered: self
                        .query
                        .order_by
                        .as_ref()
                        .and_then(|o| instantiate_ordered_aggregate(self.session, o)),
                    state: AggState::from(self.session, &self.query),
                });

                if let Err(e) = agg_group.update_order_agg(&self.interpreter) {
                    return Some(Err(e));
                }

                &mut agg_group.state
            } else if let Emit::Single(agg) = &mut self.emit {
                agg
            } else {
                return Some(Err(EvalError::Runtime(
                    "wrong code path when running aggregate query".into(),
                )));
            };

            let proj_expr = self.session.arena().get_expr(self.query.projection);
            if let Err(e) = eval_agg_value(&self.interpreter, proj_expr.value, agg) {
                return Some(Err(e));
            }
        }
    }
}
