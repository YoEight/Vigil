use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use eventql_parser::{Session, Type};
use ordered_float::OrderedFloat;
use serde::Serialize;
use std::collections::BTreeMap;

use crate::eval::{EvalError, EvalResult};

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub enum QueryValue {
    Null,
    String(String),
    Number(OrderedFloat<f64>),
    Bool(bool),
    Record(BTreeMap<String, QueryValue>),
    Array(Vec<QueryValue>),
    DateTime(DateTime<Utc>),
    Date(NaiveDate),
    Time(NaiveTime),
}

impl QueryValue {
    #[cfg(test)]
    pub fn as_str_or_panic(&self) -> &str {
        if let Self::String(s) = self {
            return s.as_str();
        }

        panic!("expected a string but got something else")
    }

    pub fn from(value: serde_json::Value) -> QueryValue {
        match value {
            serde_json::Value::Null => QueryValue::Null,
            serde_json::Value::Bool(b) => QueryValue::Bool(b),
            serde_json::Value::Number(number) => QueryValue::Number(
                number
                    .as_f64()
                    .expect("we don't use arbitrary precision")
                    .into(),
            ),
            serde_json::Value::String(s) => QueryValue::String(s),
            serde_json::Value::Array(values) => {
                let values = values.into_iter().map(Self::from).collect::<Vec<_>>();

                QueryValue::Array(values)
            }
            serde_json::Value::Object(map) => {
                let mut props = BTreeMap::new();
                for (name, value) in map {
                    props.insert(name, Self::from(value));
                }

                QueryValue::Record(props)
            }
        }
    }

    pub fn build_from_type_expectation(
        session: &Session,
        value: serde_json::Value,
        expectation: Type,
    ) -> EvalResult<QueryValue> {
        match expectation {
            Type::Unspecified => Ok(Self::from(value)),
            Type::Number => {
                if let serde_json::Value::Number(n) = value {
                    Ok(QueryValue::Number(
                        n.as_f64().expect("we don't use arbitrary precision").into(),
                    ))
                } else {
                    Ok(QueryValue::Null)
                }
            }
            Type::String | Type::Subject => {
                if let serde_json::Value::String(s) = value {
                    Ok(QueryValue::String(s))
                } else {
                    Ok(QueryValue::Null)
                }
            }
            Type::Bool => {
                if let serde_json::Value::Bool(b) = value {
                    Ok(QueryValue::Bool(b))
                } else {
                    Ok(QueryValue::Null)
                }
            }
            Type::Array(tpe) => {
                if let serde_json::Value::Array(values) = value {
                    let values = values
                        .into_iter()
                        .map(|v| {
                            Self::build_from_type_expectation(
                                session,
                                v,
                                session.arena().get_type(tpe),
                            )
                        })
                        .collect::<EvalResult<Vec<_>>>()?;

                    Ok(QueryValue::Array(values))
                } else {
                    Ok(QueryValue::Null)
                }
            }
            Type::Record(map) => {
                if let serde_json::Value::Object(values) = value {
                    let map = session.arena().get_type_rec(map);
                    let mut props = BTreeMap::new();

                    for (prop_name, prop_value) in values {
                        let prop_value =
                            if let Some(str_ref) = session.arena().str_ref(prop_name.as_str()) {
                                if let Some(tpe) = map.get(&str_ref).copied() {
                                    Self::build_from_type_expectation(session, prop_value, tpe)?
                                } else {
                                    Self::from(prop_value)
                                }
                            } else {
                                Self::from(prop_value)
                            };

                        props.insert(prop_name, prop_value);
                    }

                    Ok(QueryValue::Record(props))
                } else {
                    Ok(QueryValue::Null)
                }
            }

            Type::App { .. } => Err(EvalError::Runtime(
                "unexpected function type in value construction".into(),
            )),

            Type::Date => {
                if let serde_json::Value::String(s) = value
                    && let Ok(date) = s.parse::<NaiveDate>()
                {
                    Ok(QueryValue::Date(date))
                } else {
                    Ok(QueryValue::Null)
                }
            }

            Type::Time => {
                if let serde_json::Value::String(s) = value
                    && let Ok(time) = s.parse::<NaiveTime>()
                {
                    Ok(QueryValue::Time(time))
                } else {
                    Ok(QueryValue::Null)
                }
            }

            Type::DateTime => {
                if let serde_json::Value::String(s) = value
                    && let Ok(date_time) = s.parse::<DateTime<Utc>>()
                {
                    Ok(QueryValue::DateTime(date_time))
                } else {
                    Ok(QueryValue::Null)
                }
            }

            // currently we don't custom type but will change
            Type::Custom(_) => Ok(QueryValue::Null),
        }
    }
}
