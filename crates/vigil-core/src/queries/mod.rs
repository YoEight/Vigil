use crate::{eval::EvalResult, values::QueryValue};
use eventql_parser::StrRef;
use std::collections::HashMap;

pub mod aggregates;
pub mod events;
mod orderer;

pub type Row = Box<dyn Iterator<Item = EvalResult<QueryValue>>>;
pub type Buffer = HashMap<StrRef, QueryValue>;

#[derive(Default)]
pub struct Sources {
    inner: HashMap<StrRef, Row>,
}

impl Sources {
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&StrRef, &mut Row)> {
        self.inner.iter_mut()
    }

    pub fn insert(&mut self, key: StrRef, row: Row) {
        self.inner.insert(key, row);
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
