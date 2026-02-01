use std::collections::BTreeMap;

use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use eventql_parser::Type;
use serde::Serialize;

#[derive(Clone, Serialize)]
pub enum QueryValue {
    Null,
    String(String),
    Number(f64),
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

    pub fn from(value: serde_json::Value, _tpe: &Type) -> QueryValue {
        match value {
            serde_json::Value::Null => QueryValue::Null,
            serde_json::Value::Bool(b) => QueryValue::Bool(b),
            serde_json::Value::Number(number) => {
                QueryValue::Number(number.as_f64().expect("we don't use arbitrary precision"))
            }
            serde_json::Value::String(s) => QueryValue::String(s),
            serde_json::Value::Array(values) => {
                let values = values
                    .into_iter()
                    .map(|v| Self::from(v, _tpe))
                    .collect::<Vec<_>>();

                QueryValue::Array(values)
            }
            serde_json::Value::Object(map) => {
                let mut props = BTreeMap::new();
                for (name, value) in map {
                    props.insert(name, Self::from(value, _tpe));
                }

                QueryValue::Record(props)
            }
        }
    }

    pub fn build_from_type_expectation(value: serde_json::Value, expectation: &Type) -> QueryValue {
        match expectation {
            Type::Unspecified => Self::from(value, expectation),
            Type::Number => {
                if let serde_json::Value::Number(n) = value {
                    QueryValue::Number(n.as_f64().expect("we don't use arbitrary precision"))
                } else {
                    QueryValue::Null
                }
            }
            Type::String | Type::Subject => {
                if let serde_json::Value::String(s) = value {
                    QueryValue::String(s)
                } else {
                    QueryValue::Null
                }
            }
            Type::Bool => {
                if let serde_json::Value::Bool(b) = value {
                    QueryValue::Bool(b)
                } else {
                    QueryValue::Null
                }
            }
            Type::Array(tpe) => {
                if let serde_json::Value::Array(values) = value {
                    let values = values
                        .into_iter()
                        .map(|v| Self::build_from_type_expectation(v, tpe.as_ref()))
                        .collect();

                    QueryValue::Array(values)
                } else {
                    QueryValue::Null
                }
            }
            Type::Record(map) => {
                if let serde_json::Value::Object(mut values) = value {
                    let mut props = BTreeMap::new();

                    for (name, tpe) in map.iter() {
                        let value = if let Some(value) = values.remove(name) {
                            Self::build_from_type_expectation(value, tpe)
                        } else {
                            QueryValue::Null
                        };

                        // TODO - we might just not insert the value if not present, sparing the clone allocation
                        props.insert(name.clone(), value);
                    }

                    QueryValue::Record(props)
                } else {
                    QueryValue::Null
                }
            }

            // this one is unlikely because the user cannot expect a function at that level
            Type::App {
                args: _x,
                result: _y,
                aggregate: _z,
            } => todo!("use a proper result type so we can track it if it happens in real life"),

            Type::Date => {
                if let serde_json::Value::String(s) = value
                    && let Ok(date) = s.parse::<NaiveDate>()
                {
                    QueryValue::Date(date)
                } else {
                    QueryValue::Null
                }
            }

            Type::Time => {
                if let serde_json::Value::String(s) = value
                    && let Ok(time) = s.parse::<NaiveTime>()
                {
                    QueryValue::Time(time)
                } else {
                    QueryValue::Null
                }
            }

            Type::DateTime => {
                if let serde_json::Value::String(s) = value
                    && let Ok(date_time) = s.parse::<DateTime<Utc>>()
                {
                    QueryValue::DateTime(date_time)
                } else {
                    QueryValue::Null
                }
            }

            // currenlty we don't custom type but will change
            Type::Custom(_) => QueryValue::Null,
        }
    }
}
