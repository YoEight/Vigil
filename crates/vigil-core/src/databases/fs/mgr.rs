use bytes::Bytes;

use crate::databases::{
    LogOffset,
    fs::wal::{self, LogContentType, LogRecord},
};

pub struct LogManager {}

impl LogManager {
    pub fn append(&self, lsn: u64, ct: LogContentType, data: Bytes) -> wal::Result<LogOffset> {
        todo!()
    }

    pub fn read_at(&self, offset: LogOffset) -> wal::Result<LogRecord> {
        todo!()
    }

    pub fn read(&self, lsn: u64) -> wal::Result<LogRecord> {
        todo!()
    }
}
