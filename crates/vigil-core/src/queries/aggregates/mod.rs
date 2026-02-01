mod average;
mod count;
mod unique;

use std::collections::{BTreeMap, HashMap};

use case_insensitive_hashmap::CaseInsensitiveHashMap;
use eventql_parser::{
    App, Query, Type,
    prelude::{AnalysisOptions, Typed},
};

use crate::{
    eval::{EvalError, EvalResult, Interpreter},
    queries::{
        Sources,
        aggregates::{average::AverageAggregate, count::CountAggregate, unique::UniqueAggregate},
    },
    values::QueryValue,
};

pub trait Aggregate {
    fn fold(&mut self, params: &[QueryValue]);
    fn complete(&self) -> QueryValue;
}

enum AggState {
    Single(Box<dyn Aggregate>),
    Record(CaseInsensitiveHashMap<AggState>),
}

impl AggState {
    fn from(options: &AnalysisOptions, query: &Query<Typed>) -> Self {
        match &query.projection.value {
            eventql_parser::Value::Record(fields) => {
                let mut aggs = CaseInsensitiveHashMap::new();

                for field in fields.iter() {
                    if let eventql_parser::Value::App(app) = &field.value.value {
                        aggs.insert(
                            field.name.clone(),
                            Self::Single(instanciate_aggregate(options, app)),
                        );
                        continue;
                    }

                    let agg: Box<dyn Aggregate> = Box::new(UniqueAggregate::default());
                    aggs.insert(field.name.clone(), Self::Single(agg));
                }

                Self::Record(aggs)
            }

            eventql_parser::Value::App(app) => Self::Single(instanciate_aggregate(options, app)),

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

fn instanciate_aggregate(options: &AnalysisOptions, app: &App) -> Box<dyn Aggregate> {
    if let Type::App {
        aggregate: true, ..
    } = options
        .default_scope
        .entries
        .get(app.func.as_str())
        .expect("func to be defined")
    {
        return if app.func.eq_ignore_ascii_case("count") {
            Box::new(CountAggregate::default())
        } else if app.func.eq_ignore_ascii_case("avg") {
            Box::new(AverageAggregate::default())
        } else if app.func.eq_ignore_ascii_case("unique") {
            Box::new(UniqueAggregate::default())
        } else {
            unreachable!("impossible as such function wouldn't pass the static analysis")
        };
    }

    panic!("STATIC ANALYSIS BUG: expected an aggregate function but got a regular instead")
}

enum Emit {
    Single(AggState),
    Grouped(HashMap<String, AggState>),
}

pub struct AggQuery<'a> {
    srcs: Sources<'a>,
    interpreter: Interpreter<'a>,
    query: &'a Query<Typed>,
    emit: Emit,
    completed: bool,
    results: Vec<QueryValue>,
}

impl<'a> AggQuery<'a> {
    pub fn new(srcs: Sources<'a>, options: &'a AnalysisOptions, query: &'a Query<Typed>) -> Self {
        let emit = if query.group_by.is_some() {
            Emit::Grouped(Default::default())
        } else {
            Emit::Single(AggState::from(options, query))
        };

        Self {
            srcs,
            query,
            interpreter: Interpreter::new(options),
            emit,
            completed: false,
            results: Vec::new(),
        }
    }
}

impl<'a> Iterator for AggQuery<'a> {
    type Item = EvalResult<QueryValue>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.completed {
                if let Some(result) = self.results.pop() {
                    return Some(Ok(result));
                }

                return None;
            }

            let outcome = if let Some(outcome) = self.srcs.fill(self.interpreter.env_mut()) {
                outcome
            } else {
                self.completed = true;

                match &self.emit {
                    Emit::Single(agg) => self.results.push(agg.complete()),
                    Emit::Grouped(groups) => {
                        for group in groups.values() {
                            self.results.push(group.complete());
                        }
                    }
                }

                continue;
            };

            if let Err(e) = outcome {
                return Some(Err(e));
            }

            if let Some(predicate) = &self.query.predicate {
                match self.interpreter.eval_predicate(&predicate.value) {
                    Ok(false) => continue,
                    Ok(true) => {}
                    Err(e) => return Some(Err(e)),
                }
            }

            let agg = if let Emit::Grouped(grouped) = &mut self.emit
                && let Some(group_by) = &self.query.group_by
            {
                let group_key = match self.interpreter.eval(&group_by.expr.value) {
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

                grouped
                    .entry(group_key)
                    .or_insert_with(|| AggState::from(self.interpreter.options(), self.query))
            } else if let Emit::Single(agg) = &mut self.emit {
                agg
            } else {
                return Some(Err(EvalError::Runtime(
                    "wrong code path when running aggregate query".into(),
                )));
            };

            if let Err(e) = eval_agg_value(&self.interpreter, &self.query.projection.value, agg) {
                return Some(Err(e));
            }
        }
    }
}

fn eval_agg_value(
    interpreter: &Interpreter,
    value: &eventql_parser::Value,
    state: &mut AggState,
) -> EvalResult<()> {
    match (state, value) {
        (AggState::Single(agg), eventql_parser::Value::App(app)) => {
            let mut args = Vec::with_capacity(app.args.len());

            for arg in &app.args {
                args.push(interpreter.eval(&arg.value)?);
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
            for prop in props {
                if let Some(agg) = aggs.get_mut(prop.name.as_str()) {
                    eval_agg_value(interpreter, &prop.value.value, agg)?;
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
