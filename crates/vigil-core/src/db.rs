use std::{
    collections::{HashMap, VecDeque, hash_map},
    iter, slice,
    str::Split,
};

use eventql_parser::{
    Query, parse_query,
    prelude::{AnalysisOptions, Typed},
};
use serde::Serialize;
use thiserror::Error;
use uuid::Uuid;

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

#[derive(Default, Serialize)]
pub struct Event {
    pub spec_version: String,
    pub id: Uuid,
    pub source: String,
    pub subject: String,
    pub event_type: String,
    pub datacontenttype: String,
    pub data: String,
}

#[derive(Default)]
pub struct Subject {
    events: Vec<usize>,
    nodes: HashMap<String, Subject>,
}

impl Subject {
    fn entries<'a>(&mut self, mut path: impl Iterator<Item = &'a str>) -> &mut Vec<usize> {
        let name = path.next().unwrap_or_default();

        if name != "" {
            return self.nodes.entry(name.to_owned()).or_default().entries(path);
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

#[derive(Default)]
pub struct Db {
    types: HashMap<String, Vec<usize>>,
    subjects: Subject,
    events: Vec<Event>,
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

    pub fn iter_type<'a>(&'a self, tpe: &'a str) -> impl Iterator<Item = &'a Event> + 'a {
        let type_events = self
            .types
            .get(tpe)
            .map(Vec::as_slice)
            .unwrap_or_default()
            .iter()
            .copied();

        IndexedEvents::new(type_events, self.events.as_slice())
    }

    pub fn iter_subject<'a>(&'a self, path: &'a str) -> impl Iterator<Item = &'a Event> + 'a {
        let subject_events =
            Subjects::new(path, &self.subjects).flat_map(|sub| sub.events.iter().copied());

        IndexedEvents::new(subject_events, self.events.as_slice())
    }

    pub fn query(&self, query: &str) -> Result<Vec<serde_json::Value>> {
        let events = vec![];
        let query = parse_query(query)?.run_static_analysis(&AnalysisOptions::default())?;

        let info = query.meta;

        Ok(events)
    }
}

type Row<'a> = Box<dyn Iterator<Item = Output<'a>> + 'a>;

pub enum Output<'a> {
    Event(&'a Event),
    Record(serde_json::Map<String, serde_json::Value>),
}

#[derive(Default)]
struct Source<'a> {
    bindings: HashMap<&'a str, Row<'a>>,
}

type Sources<'a> = HashMap<&'a str, Row<'a>>;

pub struct EventQuery<'a> {
    srcs: Sources<'a>,
    query: &'a Query<Typed>,
    buffer: HashMap<&'a str, Output<'a>>,
}

impl<'a> EventQuery<'a> {
    pub fn new(srcs: Sources<'a>, query: &'a Query<Typed>) -> Self {
        Self {
            srcs,
            query,
            buffer: Default::default(),
        }
    }
}

impl<'a> Iterator for EventQuery<'a> {
    type Item = Output<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.buffer.clear();

        for (binding, row) in self.srcs.iter_mut() {
            self.buffer.insert(binding, row.next()?);
        }

        todo!()
    }
}

struct Node<'a> {
    value: &'a eventql_parser::Value,
    visited: bool,
}

impl<'a> Node<'a> {
    fn new(value: &'a eventql_parser::Value) -> Self {
        Self {
            value,
            visited: false,
        }
    }
}

fn evaluate_predicate<'a>(
    env: &HashMap<&'a str, Output<'a>>,
    value: &eventql_parser::Value,
) -> bool {
    let mut stack = vec![Node::new(value)];
    let mut result = false;

    while let Some(node) = stack.pop() {}

    result
}

fn catalog<'a>(db: &'a Db, scope: usize, query: &'a Query<Typed>) -> Row<'a> {
    let mut srcs = Sources::new();
    for query_src in &query.sources {
        match &query_src.kind {
            eventql_parser::SourceKind::Name(name) => {
                if name.eq_ignore_ascii_case("events") {
                    srcs.insert(
                        &query_src.binding.name,
                        Box::new(db.events.iter().map(Output::Event)),
                    );

                    continue;
                }

                srcs.insert(&query_src.binding.name, Box::new(iter::empty()));
            }

            eventql_parser::SourceKind::Subject(path) => {
                srcs.insert(
                    &query_src.binding.name,
                    Box::new(db.iter_subject(path).map(Output::Event)),
                );
            }

            eventql_parser::SourceKind::Subquery(sub_query) => {
                let row = catalog(db, scope + 1, sub_query);

                srcs.insert(&query_src.binding.name, row);
            }
        }
    }

    Box::new(EventQuery::new(srcs, query))
}
