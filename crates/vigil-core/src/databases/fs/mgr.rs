use crate::databases::fs::wal::LogSegment;
use crate::databases::{
    LogOffset,
    fs::wal::{self, LogContentType, LogRecord},
};
use bytes::Bytes;
use indexmap::IndexMap;

pub struct LogManager {
    _segments: IndexMap<u32, LogSegment>,
}

impl LogManager {
    pub fn append(
        &mut self,
        _lsn: u64,
        _ct: LogContentType,
        _data: Bytes,
    ) -> wal::Result<LogOffset> {
        todo!()
    }

    pub fn read_at(&mut self, _offset: LogOffset) -> wal::Result<LogRecord> {
        todo!()
    }

    pub fn read(&mut self, _lsn: u64) -> wal::Result<LogRecord> {
        todo!()
    }
}
