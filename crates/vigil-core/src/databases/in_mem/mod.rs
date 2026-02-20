mod events;
mod subject;

use std::collections::HashMap;

use eventql_parser::{Session, Type};

use crate::{
    databases::{
        Error,
        in_mem::{
            events::IndexedEvents,
            subject::{Subject, Subjects},
        },
    },
    planner::{DataProvider, query_plan},
    queries::QueryProcessor,
    types::Event,
    values::QueryValue,
};

pub struct InMemDb {
    types: HashMap<String, Vec<usize>>,
    subjects: Subject,
    events: Vec<Event>,
    session: Session,
}

impl InMemDb {
    pub fn append(&mut self, subject: &str, events: Vec<Event>) -> super::Result<()> {
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
            Subjects::new(path, &self.subjects).flat_map(|sub| sub.events().iter().copied());

        IndexedEvents::new(subject_events, self.events.as_slice())
    }

    pub fn iter_subjects<'a>(&'a self) -> impl Iterator<Item = &'a str> + 'a {
        Subjects::all(&self.subjects).filter_map(|sub| {
            if sub.name().is_empty() {
                None
            } else {
                Some(sub.name())
            }
        })
    }

    pub fn run_query(&mut self, query: &str) -> super::Result<QueryProcessor<'_>> {
        let query = self.session.parse(query)?;
        let query = self.session.run_static_analysis(query)?;

        Ok(query_plan(&self.session, self, query))
    }
}

impl Default for InMemDb {
    fn default() -> Self {
        Self {
            types: Default::default(),
            subjects: Default::default(),
            events: vec![],
            session: Session::builder().use_stdlib().build(),
        }
    }
}

impl DataProvider for InMemDb {
    fn instantiate_named_data_source<'a>(
        &'a self,
        name: &'a str,
        inferred_type: Type,
    ) -> Option<QueryProcessor<'a>> {
        if name.eq_ignore_ascii_case("events") {
            Some(QueryProcessor::generic(
                self.events
                    .iter()
                    .map(move |e| e.project(&self.session, inferred_type)),
            ))
        } else if name.eq_ignore_ascii_case("eventtypes") {
            Some(QueryProcessor::generic(self.types.keys().map(
                |event_type| Ok(QueryValue::String(event_type.clone())),
            )))
        } else if name.eq_ignore_ascii_case("subjects") {
            Some(QueryProcessor::generic(
                self.iter_subjects()
                    .map(|s| Ok(QueryValue::String(s.to_owned()))),
            ))
        } else {
            None
        }
    }

    fn instantiate_subject_data_source<'a>(
        &'a self,
        subject: &'a str,
        inferred_type: Type,
    ) -> Option<QueryProcessor<'a>> {
        Some(QueryProcessor::generic(
            self.iter_subject_events(subject)
                .map(move |e| e.project(&self.session, inferred_type)),
        ))
    }
}
