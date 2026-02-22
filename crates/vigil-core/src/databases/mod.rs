use serde::Serialize;
use thiserror::Error;

pub mod fs;
pub mod in_mem;

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

pub const KB: usize = 1_024;
pub const MB: usize = KB * 1_024;
pub const GB: usize = MB * 1_024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct LogOffset(u64);

impl LogOffset {
    pub fn new(segment_id: u32, offset: u32) -> Self {
        Self(((segment_id as u64) << 32) | offset as u64)
    }

    pub fn segment_id(&self) -> u32 {
        (self.0 >> 32) as u32
    }

    pub fn offset(&self) -> u32 {
        self.0 as u32
    }
}
