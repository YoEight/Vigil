use std::collections::HashMap;

use crate::{eval::EvalResult, values::QueryValue};

pub mod aggregates;
pub mod events;

pub type Row<'a> = Box<dyn Iterator<Item = EvalResult<QueryValue>> + 'a>;
pub type Buffer<'a> = HashMap<&'a str, QueryValue>;

#[derive(Default)]
pub struct Sources<'a> {
    inner: HashMap<&'a str, Row<'a>>,
}

impl<'a> Sources<'a> {
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&&'a str, &mut Row<'a>)> {
        self.inner.iter_mut()
    }

    pub fn insert(&mut self, key: &'a str, row: Row<'a>) {
        self.inner.insert(key, row);
    }

    pub fn fill(&mut self, buffer: &mut Buffer<'a>) -> Option<EvalResult<()>> {
        for (binding, row) in self.iter_mut() {
            match row.next()? {
                Ok(value) => {
                    buffer.insert(binding, value);
                }

                Err(e) => return Some(Err(e)),
            }
        }

        Some(Ok(()))
    }
}
