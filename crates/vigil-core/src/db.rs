use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    str::Split,
};

use eventql_parser::{
    Query, Session,
    prelude::{Type, Typed},
};
use serde::Serialize;
use thiserror::Error;
use uuid::Uuid;

use crate::{
    eval::EvalResult,
    queries::{QueryProcessor, Sources, aggregates::AggQuery, events::EventQuery},
    values::QueryValue,
};

#[derive(Debug, Error, Serialize)]
pub enum Error {
    #[error(transparent)]
    Query(eventql_parser::prelude::Error),

    #[error("subject cannot start with a '/'")]
    IllegalSubject,
}

impl From<eventql_parser::prelude::Error> for Error {
    fn from(value: eventql_parser::prelude::Error) -> Self {
        Self::Query(value)
    }
}

pub type Result<A> = std::result::Result<A, Error>;

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
    fn project(&self, session: &Session, expected: Type) -> EvalResult<QueryValue> {
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

#[derive(Default)]
pub struct Subject {
    name: String,
    events: Vec<usize>,
    nodes: HashMap<String, Subject>,
}

impl Subject {
    fn entries<'a>(&mut self, mut path: impl Iterator<Item = &'a str>) -> &mut Vec<usize> {
        let name = path.next().unwrap_or_default();

        if !name.is_empty() {
            return self
                .nodes
                .entry(name.to_owned())
                .or_insert_with(|| {
                    let name = if self.name.is_empty() {
                        name.to_owned()
                    } else {
                        format!("{}/{}", self.name, name)
                    };

                    Self {
                        name,
                        ..Default::default()
                    }
                })
                .entries(path);
        }

        &mut self.events
    }
}

pub enum Subjects<'a> {
    Dive {
        split: Split<'a, char>,
        current: &'a Subject,
    },

    Browse {
        queue: VecDeque<&'a Subject>,
    },
}

impl<'a> Subjects<'a> {
    pub fn new(path: &'a str, subject: &'a Subject) -> Self {
        Self::Dive {
            split: path.split('/'),
            current: subject,
        }
    }

    pub fn all(root: &'a Subject) -> Self {
        Self::Browse {
            queue: VecDeque::from_iter([root]),
        }
    }
}

impl<'a> Iterator for Subjects<'a> {
    type Item = &'a Subject;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self {
                Subjects::Dive { split, current } => {
                    let path = split.next().unwrap_or_default();

                    if path.trim().is_empty() {
                        let mut queue = VecDeque::new();

                        queue.push_back(*current);

                        *self = Self::Browse { queue };
                        continue;
                    }

                    *current = current.nodes.get(path)?;
                }

                Subjects::Browse { queue } => {
                    let current = queue.pop_front()?;

                    queue.extend(current.nodes.values());

                    return Some(current);
                }
            }
        }
    }
}

pub struct IndexedEvents<'a, I> {
    indexes: I,
    events: &'a [Event],
}

impl<'a, I> IndexedEvents<'a, I> {
    pub fn new(indexes: I, events: &'a [Event]) -> Self {
        Self { indexes, events }
    }
}

impl<'a, I> Iterator for IndexedEvents<'a, I>
where
    I: Iterator<Item = usize> + 'a,
{
    type Item = &'a Event;

    fn next(&mut self) -> Option<Self::Item> {
        let idx = self.indexes.next()?;
        self.events.get(idx)
    }
}

pub struct Db {
    types: HashMap<String, Vec<usize>>,
    subjects: Subject,
    events: Vec<Event>,
    session: Session,
}

impl Default for Db {
    fn default() -> Self {
        Self {
            types: Default::default(),
            subjects: Default::default(),
            events: vec![],
            session: Session::builder().use_stdlib().build(),
        }
    }
}

impl Db {
    pub fn append(&mut self, subject: &str, events: Vec<Event>) -> Result<()> {
        if subject.starts_with('/') {
            return Err(Error::IllegalSubject);
        }

        let subject_entries = self.subjects.entries(subject.split('/'));
        let mut next_id = self.events.len();

        for event in events {
            // index by types
            self.types
                .entry(event.event_type.clone())
                .or_default()
                .push(next_id);

            // index by subject
            subject_entries.push(next_id);

            // store the event in the persistent storage
            self.events.push(event);
            next_id += 1;
        }

        Ok(())
    }

    pub fn iter_types<'a>(&'a self, tpe: &'a str) -> impl Iterator<Item = &'a Event> + 'a {
        let type_events = self
            .types
            .get(tpe)
            .map(Vec::as_slice)
            .unwrap_or_default()
            .iter()
            .copied();

        IndexedEvents::new(type_events, self.events.as_slice())
    }

    pub fn iter_subject_events<'a>(
        &'a self,
        path: &'a str,
    ) -> impl Iterator<Item = &'a Event> + 'a {
        let subject_events =
            Subjects::new(path, &self.subjects).flat_map(|sub| sub.events.iter().copied());

        IndexedEvents::new(subject_events, self.events.as_slice())
    }

    pub fn iter_subjects<'a>(&'a self) -> impl Iterator<Item = &'a String> + 'a {
        Subjects::all(&self.subjects).filter_map(|sub| {
            if sub.name.is_empty() {
                None
            } else {
                Some(&sub.name)
            }
        })
    }

    pub fn run_query(&mut self, query: &str) -> Result<QueryProcessor<'_>> {
        let query = self.session.parse(query)?;
        let query = self.session.run_static_analysis(query)?;

        Ok(self.catalog(query))
    }

    fn catalog(&self, query: Query<Typed>) -> QueryProcessor<'_> {
        let mut srcs = Sources::default();
        for query_src in &query.sources {
            match &query_src.kind {
                eventql_parser::SourceKind::Name(name) => {
                    let name = self.session.arena().get_str(*name);

                    let proc = if let Some(tpe) = query.meta.scope.get(query_src.binding.name) {
                        if name.eq_ignore_ascii_case("events") {
                            QueryProcessor::generic(
                                self.events
                                    .iter()
                                    .map(move |e| e.project(&self.session, tpe)),
                            )
                        } else if name.eq_ignore_ascii_case("eventtypes") {
                            QueryProcessor::generic(
                                self.types
                                    .keys()
                                    .map(|event_type| Ok(QueryValue::String(event_type.clone()))),
                            )
                        } else if name.eq_ignore_ascii_case("subjects") {
                            QueryProcessor::generic(
                                self.iter_subjects()
                                    .map(|s| Ok(QueryValue::String(s.clone()))),
                            )
                        } else {
                            QueryProcessor::empty()
                        }
                    } else {
                        QueryProcessor::empty()
                    };

                    srcs.insert(query_src.binding.name, proc);
                }

                eventql_parser::SourceKind::Subject(path) => {
                    if let Some(tpe) = query.meta.scope.get(query_src.binding.name) {
                        let path = self.session.arena().get_str(*path);

                        srcs.insert(
                            query_src.binding.name,
                            QueryProcessor::generic(
                                self.iter_subject_events(path)
                                    .map(move |e| e.project(&self.session, tpe)),
                            ),
                        );

                        continue;
                    }

                    srcs.insert(query_src.binding.name, QueryProcessor::empty());
                }

                eventql_parser::SourceKind::Subquery(sub_query) => {
                    let name = query_src.binding.name;
                    // TODO - get rid of that unnecessary clone
                    let row = self.catalog(sub_query.as_ref().clone());

                    srcs.insert(name, row);
                }
            }
        }

        if query.meta.aggregate {
            match AggQuery::new(srcs, &self.session, query) {
                Ok(agg_query) => QueryProcessor::Aggregate(agg_query),
                Err(e) => QueryProcessor::Errored(Some(e)),
            }
        } else {
            QueryProcessor::Regular(EventQuery::new(srcs, &self.session, query))
        }
    }
}

// TODO - in due time
// #[derive(Copy, Clone, Default)]
// struct Consts {
//     now: Option<DateTime<Utc>>,
// }

// impl Consts {
//     fn now(&mut self) -> DateTime<Utc> {
//         if let Some(dt) = &self.now {
//             return *dt;
//         }

//         let now = Utc::now();
//         self.now = Some(now);
//         now
//     }
// }
