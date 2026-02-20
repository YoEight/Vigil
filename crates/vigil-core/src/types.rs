use std::collections::BTreeMap;

use eventql_parser::{Session, prelude::Type};
use serde::Serialize;
use uuid::Uuid;

use crate::{eval::EvalResult, values::QueryValue};

#[derive(Default, Clone, Serialize)]
pub struct Event {
    pub spec_version: String,
    pub id: Uuid,
    pub source: String,
    pub subject: String,
    pub event_type: String,
    pub datacontenttype: String,
    pub data: Vec<u8>,
}

impl Event {
    pub fn project(&self, session: &Session, expected: Type) -> EvalResult<QueryValue> {
        if let Type::Record(rec) = expected {
            let mut props = BTreeMap::new();
            for (name, value) in session.arena().get_type_rec(rec) {
                let name = session.arena().get_str(*name).to_owned();
                match name.as_str() {
                    "spec_version" => match value {
                        Type::String => {
                            props.insert(
                                name,
                                QueryValue::String(self.spec_version.as_str().into()),
                            );
                        }

                        _ => {
                            props.insert(name, QueryValue::Null);
                        }
                    },

                    "id" => match value {
                        Type::String => {
                            props.insert(name, QueryValue::String(self.id.to_string()));
                        }

                        _ => {
                            props.insert(name, QueryValue::Null);
                        }
                    },

                    "source" => match value {
                        Type::String => {
                            props.insert(name, QueryValue::String(self.source.clone()));
                        }

                        _ => {
                            props.insert(name, QueryValue::Null);
                        }
                    },

                    "subject" => match value {
                        Type::String | Type::Subject => {
                            props.insert(name, QueryValue::String(self.subject.as_str().into()));
                        }

                        _ => {
                            props.insert(name, QueryValue::Null);
                        }
                    },

                    "type" => match value {
                        Type::String => {
                            props.insert(name, QueryValue::String(self.event_type.as_str().into()));
                        }

                        _ => {
                            props.insert(name, QueryValue::Null);
                        }
                    },

                    "datacontenttype" => match value {
                        Type::String => {
                            props.insert(
                                name,
                                QueryValue::String(self.datacontenttype.as_str().into()),
                            );
                        }

                        _ => {
                            props.insert(name, QueryValue::Null);
                        }
                    },

                    "data" => match value {
                        Type::String => {
                            props.insert(
                                name,
                                QueryValue::String(unsafe {
                                    str::from_utf8_unchecked(self.data.as_slice()).into()
                                }),
                            );
                        }

                        Type::Record(_) | Type::Unspecified => {
                            match self.datacontenttype.as_str() {
                                "application/json" => {
                                    if let Ok(payload) = serde_json::from_slice(&self.data) {
                                        props.insert(
                                            name,
                                            QueryValue::build_from_type_expectation(
                                                session, payload, *value,
                                            )?,
                                        );
                                    } else {
                                        props.insert(name, QueryValue::Null);
                                    }
                                }

                                _ => {
                                    props.insert(name, QueryValue::Null);
                                }
                            }
                        }

                        _ => {
                            props.insert(name, QueryValue::Null);
                        }
                    },

                    _ => {
                        props.insert(name, QueryValue::Null);
                    }
                }
            }

            Ok(QueryValue::Record(props))
        } else {
            Ok(QueryValue::Null)
        }
    }
}
