use std::cmp::Ordering;
use std::collections::BTreeMap;

use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use eventql_parser::{Session, Type};
use ordered_float::OrderedFloat;
use serde::Serialize;

#[derive(Clone, Serialize)]
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

impl PartialEq for QueryValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::String(a), Self::String(b)) => a == b,
            (Self::Bool(a), Self::Bool(b)) => a == b,
            (Self::Number(a), Self::Number(b)) => a == b,
            (Self::DateTime(a), Self::DateTime(b)) => a == b,
            (Self::Date(a), Self::Date(b)) => a == b,
            (Self::Time(a), Self::Time(b)) => a == b,
            (Self::Array(a), Self::Array(b)) => a == b,
            (Self::Record(a), Self::Record(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for QueryValue {}

impl PartialOrd for QueryValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for QueryValue {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Null, Self::Null) => Ordering::Equal,
            (Self::String(a), Self::String(b)) => a.cmp(b),
            (Self::Bool(a), Self::Bool(b)) => a.cmp(b),
            (Self::Number(a), Self::Number(b)) => a.total_cmp(b),
            (Self::DateTime(a), Self::DateTime(b)) => a.cmp(b),
            (Self::Date(a), Self::Date(b)) => a.cmp(b),
            (Self::Time(a), Self::Time(b)) => a.cmp(b),
            (Self::Array(a), Self::Array(b)) => a.cmp(b),
            (Self::Record(a), Self::Record(b)) => a.cmp(b),
            _ => self.type_order().cmp(&other.type_order()),
        }
    }
}

impl QueryValue {
    pub fn type_order(&self) -> u8 {
        match self {
            QueryValue::Null => 0,
            QueryValue::String(_) => 1,
            QueryValue::Number(_) => 2,
            QueryValue::Bool(_) => 3,
            QueryValue::Record(_) => 4,
            QueryValue::Array(_) => 5,
            QueryValue::DateTime(_) => 6,
            QueryValue::Date(_) => 7,
            QueryValue::Time(_) => 8,
        }
    }

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
    ) -> QueryValue {
        match expectation {
            Type::Unspecified => Self::from(value),
            Type::Number => {
                if let serde_json::Value::Number(n) = value {
                    QueryValue::Number(n.as_f64().expect("we don't use arbitrary precision").into())
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
                        .map(|v| {
                            Self::build_from_type_expectation(
                                session,
                                v,
                                session.arena().get_type(tpe),
                            )
                        })
                        .collect();

                    QueryValue::Array(values)
                } else {
                    QueryValue::Null
                }
            }
            Type::Record(map) => {
                if let serde_json::Value::Object(values) = value {
                    let map = session.arena().get_type_rec(map);
                    let mut props = BTreeMap::new();

                    for (prop_name, prop_value) in values {
                        let prop_value = if let Some(tpe) = map.get(prop_name.as_str()).copied() {
                            Self::build_from_type_expectation(session, prop_value, tpe)
                        } else {
                            Self::from(prop_value)
                        };

                        props.insert(prop_name, prop_value);
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

            // currently we don't custom type but will change
            Type::Custom(_) => QueryValue::Null,
        }
    }
}
