use crate::{
    eval::{EvalError, EvalResult},
    queries::{aggregates::AggQuery, events::EventQuery},
    values::QueryValue,
};
use eventql_parser::StrRef;
use std::collections::HashMap;

pub mod aggregates;
pub mod events;
mod orderer;

pub type Buffer = HashMap<StrRef, QueryValue>;

pub enum QueryProcessor<'a> {
    Regular(EventQuery<'a>),
    Aggregate(AggQuery<'a>),
    Errored(Option<EvalError>),
    Generic(Box<dyn Iterator<Item = EvalResult<QueryValue>> + 'a>),
}

impl<'a> QueryProcessor<'a> {
    pub fn empty() -> Self {
        Self::Errored(None)
    }

    pub fn generic<I>(proc: I) -> Self
    where
        I: Iterator<Item = EvalResult<QueryValue>> + 'a,
    {
        Self::Generic(Box::new(proc))
    }
}

impl<'a> Iterator for QueryProcessor<'a> {
    type Item = EvalResult<QueryValue>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            QueryProcessor::Regular(query) => query.next(),
            QueryProcessor::Aggregate(query) => query.next(),
            QueryProcessor::Generic(proc) => proc.next(),
            QueryProcessor::Errored(err_opt) => Some(Err(err_opt.take()?)),
        }
    }
}

#[derive(Default)]
pub struct Sources<'a> {
    inner: HashMap<StrRef, QueryProcessor<'a>>,
}

impl<'a> Sources<'a> {
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&StrRef, &mut QueryProcessor<'a>)> {
        self.inner.iter_mut()
    }

    pub fn insert(&mut self, key: StrRef, proc: QueryProcessor<'a>) {
        self.inner.insert(key, proc);
    }

    pub fn fill(&mut self, buffer: &mut Buffer) -> Option<EvalResult<()>> {
        for (binding, row) in self.iter_mut() {
            match row.next()? {
                Ok(value) => {
                    buffer.insert(*binding, value);
                }

                Err(e) => return Some(Err(e)),
            }
        }

        Some(Ok(()))
    }
}
