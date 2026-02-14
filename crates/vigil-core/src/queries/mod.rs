use crate::{eval::EvalResult, values::QueryValue};
use eventql_parser::StrRef;
use std::collections::HashMap;

pub mod aggregates;
pub mod events;
mod orderer;

pub type Row<'a> = Box<dyn Iterator<Item = EvalResult<QueryValue>> + 'a>;
pub type Buffer = HashMap<StrRef, QueryValue>;

#[derive(Default)]
pub struct Sources<'a> {
    inner: HashMap<StrRef, Row<'a>>,
}

impl<'a> Sources<'a> {
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&StrRef, &mut Row<'a>)> {
        self.inner.iter_mut()
    }

    pub fn insert(&mut self, key: StrRef, row: Row<'a>) {
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
