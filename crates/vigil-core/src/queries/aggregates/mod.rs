mod average;
mod count;
mod unique;

use crate::queries::orderer::QueryOrderer;
use crate::{
    eval::{EvalError, EvalResult, Interpreter},
    queries::{
        Sources,
        aggregates::{average::AverageAggregate, count::CountAggregate, unique::UniqueAggregate},
    },
    values::QueryValue,
};
use case_insensitive_hashmap::CaseInsensitiveHashMap;
use eventql_parser::prelude::Expr;
use eventql_parser::{
    App, Order, Query, Session,
    prelude::{Type, Typed},
};
use std::collections::{BTreeMap, HashMap};
use std::vec;

pub trait Aggregate {
    fn fold(&mut self, params: &[QueryValue]);
    fn complete(&self) -> QueryValue;
}

enum AggState {
    Single(Box<dyn Aggregate>),
    Record(CaseInsensitiveHashMap<AggState>),
}

impl AggState {
    fn from(session: &Session, query: &Query<Typed>) -> Self {
        match session.arena().get_expr(query.projection).value {
            eventql_parser::Value::Record(fields) => {
                let mut aggs = CaseInsensitiveHashMap::new();

                for field in session.arena().get_rec(fields) {
                    let field_name = session.arena().get_str(field.name);
                    if let eventql_parser::Value::App(app) =
                        session.arena().get_expr(field.expr).value
                    {
                        aggs.insert(
                            field_name,
                            Self::Single(instantiate_aggregate(session, &app)),
                        );
                        continue;
                    }

                    let agg: Box<dyn Aggregate> = Box::new(UniqueAggregate::default());
                    aggs.insert(field_name, Self::Single(agg));
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

fn instantiate_aggregate(session: &Session, app: &App) -> Box<dyn Aggregate> {
    if let Type::App {
        aggregate: true, ..
    } = session
        .global_scope()
        .get(app.func)
        .expect("func to be defined")
    {
        let fun_name = session.arena().get_str(app.func);
        return if fun_name.eq_ignore_ascii_case("count") {
            Box::new(CountAggregate::default())
        } else if fun_name.eq_ignore_ascii_case("avg") {
            Box::new(AverageAggregate::default())
        } else if fun_name.eq_ignore_ascii_case("unique") {
            Box::new(UniqueAggregate::default())
        } else {
            unreachable!("impossible as such function wouldn't pass the static analysis")
        };
    }

    panic!("STATIC ANALYSIS BUG: expected an aggregate function but got a regular instead")
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

fn eval_agg_value(
    interpreter: &Interpreter,
    value: eventql_parser::Value,
    state: &mut AggState,
) -> EvalResult<()> {
    match (state, value) {
        (AggState::Single(agg), eventql_parser::Value::App(app)) => {
            let fn_args = interpreter.session.arena().get_vec(app.args);
            let mut args = Vec::with_capacity(fn_args.len());

            for arg in fn_args {
                args.push(interpreter.eval(interpreter.session.arena().get_expr(*arg).value)?);
            }

            agg.fold(args.as_slice());

            Ok(())
        }

        (AggState::Single(agg), value) => {
            let value = interpreter.eval(value)?;

            agg.fold(&[value]);

            Ok(())
        }

        (AggState::Record(aggs), eventql_parser::Value::Record(props)) => {
            for prop in interpreter.session.arena().get_rec(props) {
                if let Some(agg) = aggs.get_mut(interpreter.session.arena().get_str(prop.name)) {
                    eval_agg_value(
                        interpreter,
                        interpreter.session.arena().get_expr(prop.expr).value,
                        agg,
                    )?;
                    continue;
                }

                return Err(EvalError::Runtime("tagged aggregate not found".into()));
            }

            Ok(())
        }

        _ => Err(EvalError::Runtime(
            "invalid aggregate evaluation code path".into(),
        )),
    }
}
