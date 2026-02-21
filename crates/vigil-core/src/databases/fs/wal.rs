use std::{hash::Hash, mem};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::Serialize;

use crate::databases::fs::blocks::Blocks;
use crate::databases::{
    MB,
    fs::blocks::{self, Block, BlocksMut, ClosedBlock},
};

pub const MAGIC_NUM: u32 = 0x57414C00;
pub const SEGMENT_VERSION: u16 = 0x01;
pub const SEGMENT_HEADER_SIZE: usize = 128;
pub const SEGMENT_FOOTER_SIZE: usize = 128;
pub const SEGMENT_SIZE: usize = 256 * MB;

pub const RECORD_MIN_SIZE: usize = mem::size_of::<u8>()
     + mem::size_of::<u8>()
     + mem::size_of::<u16>() // data len
     + mem::size_of::<u32>(); // CRC 32

#[derive(Serialize, Debug)]
pub enum LogError {
    WrongFileFormat,
    TooSmall,
    LengthMismatch,
    ChecksumMismatch,
    SegmentCorrupted,
    BlockError(blocks::BlockError),
}

pub type Result<A> = std::result::Result<A, LogError>;

impl From<blocks::BlockError> for LogError {
    fn from(value: blocks::BlockError) -> Self {
        Self::BlockError(value)
    }
}

#[derive(PartialEq, Eq, Copy, Clone, Serialize, Debug)]
pub struct LogSegHeader {
    pub version: u16,
    pub segment_id: u64,
}

impl LogSegHeader {
    pub fn serialize_into(&self, buf: &mut BytesMut) {
        buf.reserve(SEGMENT_HEADER_SIZE);
        buf.put_u32_le(MAGIC_NUM);
        buf.put_u16_le(self.version);
        buf.put_u64_le(self.segment_id);
        buf.put_bytes(0, SEGMENT_HEADER_SIZE - buf.len());
    }

    pub fn try_deserialize_from(mut bytes: Bytes) -> Result<Self> {
        if bytes.len() < SEGMENT_HEADER_SIZE {
            return Err(LogError::TooSmall);
        }

        if bytes.get_u32_le() != MAGIC_NUM {
            return Err(LogError::WrongFileFormat);
        }

        let version = bytes.get_u16_le();
        let segment_id = bytes.get_u64_le();

        Ok(Self {
            version,
            segment_id,
        })
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy, Serialize)]
pub struct LogSegFooter {
    pub sealed: bool,
    pub first_lsn: u64,
    pub last_lsn: u64,
    pub checksum: u32,
}

impl LogSegFooter {
    pub fn new(first_lsn: u64) -> Self {
        Self {
            sealed: false,
            first_lsn,
            last_lsn: 0,
            checksum: 0,
        }
    }

    pub fn serialize_into(&self, buf: &mut BytesMut) {
        buf.reserve(SEGMENT_FOOTER_SIZE);
        let prefix = mem::size_of::<u32>() + mem::size_of::<u8>() + 2 * mem::size_of::<u64>();

        buf.put_u32_le(MAGIC_NUM);
        buf.put_u8(if self.sealed { 0x01 } else { 0x00 });
        buf.put_u64_le(self.first_lsn);

        if self.sealed {
            buf.put_u64_le(self.last_lsn);
            buf.put_bytes(0, SEGMENT_FOOTER_SIZE - (prefix + mem::size_of::<u32>()));
            buf.put_u32_le(self.checksum);
        } else {
            buf.put_u64_le(0);
            buf.put_bytes(0, SEGMENT_FOOTER_SIZE - (prefix + mem::size_of::<u32>()));
            buf.put_u32_le(0);
        }
    }

    pub fn try_deserialize_from(mut bytes: Bytes) -> Result<Self> {
        if bytes.remaining() < SEGMENT_FOOTER_SIZE {
            return Err(LogError::TooSmall);
        }

        let mut checksum_part =
            bytes.slice(SEGMENT_FOOTER_SIZE - mem::size_of::<u32>()..SEGMENT_FOOTER_SIZE);

        if bytes.get_u32_le() != MAGIC_NUM {
            return Err(LogError::WrongFileFormat);
        }

        let sealed = match bytes.get_u8() {
            0x00 => false,
            0x01 => true,
            _ => return Err(LogError::WrongFileFormat),
        };

        let first_lsn = bytes.get_u64_le();
        let mut last_lsn = 0u64;
        let mut checksum = 0u32;

        if sealed {
            last_lsn = bytes.get_u64_le();
            checksum = checksum_part.get_u32_le();
        }

        Ok(Self {
            sealed,
            first_lsn,
            last_lsn,
            checksum,
        })
    }
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
pub enum LogOp {
    Unknown(u8),
    Put,
    Delete,
}

impl From<u8> for LogOp {
    fn from(value: u8) -> Self {
        match value {
            0x01 => Self::Put,
            0x02 => Self::Delete,
            x => Self::Unknown(x),
        }
    }
}

impl From<LogOp> for u8 {
    fn from(value: LogOp) -> Self {
        match value {
            LogOp::Unknown(x) => x,
            LogOp::Put => 0x01,
            LogOp::Delete => 0x02,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
pub enum LogContentType {
    Unknown(u8),
    Json,
}

impl From<LogContentType> for u8 {
    fn from(value: LogContentType) -> Self {
        match value {
            LogContentType::Unknown(x) => x,
            LogContentType::Json => 0x01,
        }
    }
}

impl From<u8> for LogContentType {
    fn from(value: u8) -> Self {
        match value {
            0x01 => Self::Json,
            x => Self::Unknown(x),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize)]
pub struct LogRecord {
    pub lsn: u64,
    pub op: LogOp,
    pub content_type: LogContentType,
    pub data: Bytes,
}

impl LogRecord {
    pub fn size_on_disk(&self) -> usize {
        mem::size_of::<u64>() // lsn
          + mem::size_of::<u8>() // wal op
          + mem::size_of::<u8>() // content type
          + mem::size_of::<u16>() // data len
          + self.data.len()
          + mem::size_of::<u32>() // CRC 32
    }

    pub fn serialize_into(&self, blocks: &mut BlocksMut) -> blocks::Result<ClosedBlock> {
        let mut buf = blocks.open(self.size_on_disk())?;
        buf.put_u64_le(self.lsn)?;
        buf.put_u8(self.op.into())?;
        buf.put_u8(self.content_type.into())?;
        buf.put_u16_le(self.data.len() as u16)?;
        buf.put_bytes(self.data.clone())?;

        let checksum = crc32fast::hash(buf.written_bytes());
        buf.put_u32_le(checksum)?;

        buf.finalize()
    }

    pub fn try_deserialize_from(mut bytes: Block) -> Result<Self> {
        if bytes.remaining() < RECORD_MIN_SIZE {
            return Err(LogError::TooSmall);
        }

        let lsn = bytes.get_u64_le()?;
        let op = bytes.get_u8()?.into();
        let content_type = bytes.get_u8()?.into();
        let data_len = bytes.get_u16_le()?;
        let data = bytes.copy_to_bytes(data_len as usize)?;
        let content = bytes.read_content();
        let checksum = bytes.get_u32_le()?;

        if checksum != crc32fast::hash(content.as_ref()) {
            return Err(LogError::ChecksumMismatch);
        }

        Ok(Self {
            lsn,
            op,
            content_type,
            data,
        })
    }
}

#[derive(Debug, Copy, Clone)]
pub struct LogSegment {
    pub header: LogSegHeader,
    pub footer: LogSegFooter,
}

impl LogSegment {
    pub fn new(segment_id: u64) -> Self {
        Self {
            header: LogSegHeader {
                version: SEGMENT_VERSION,
                segment_id,
            },

            footer: LogSegFooter {
                sealed: false,
                first_lsn: u64::MAX,
                last_lsn: u64::MAX,
                checksum: u32::MAX,
            },
        }
    }

    pub fn first_lsn(&self) -> u64 {
        self.footer.first_lsn
    }

    pub fn last_lsn(&self) -> Option<u64> {
        if self.is_sealed() {
            return Some(self.footer.last_lsn);
        }

        None
    }

    pub fn is_sealed(&self) -> bool {
        self.footer.sealed
    }

    pub fn record_writer(&self, blocks: BlocksMut) -> Option<LogSegmentRecordWriter> {
        if self.is_sealed() {
            return None;
        }

        Some(LogSegmentRecordWriter {
            blocks,
            cached_last_lsn: None,
        })
    }

    pub fn record_reader(&self, blocks: Blocks) -> LogSegmentRecordReader {
        LogSegmentRecordReader::new(blocks)
    }
}

impl Hash for LogSegment {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.header.segment_id.hash(state);
    }
}

impl PartialEq for LogSegment {
    fn eq(&self, other: &Self) -> bool {
        self.header.segment_id == other.header.segment_id
    }
}

impl Eq for LogSegment {}

impl PartialOrd for LogSegment {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for LogSegment {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.header.segment_id.cmp(&other.header.segment_id)
    }
}

pub struct LogSegmentRecordWriter {
    blocks: BlocksMut,
    cached_last_lsn: Option<u64>,
}

impl LogSegmentRecordWriter {
    pub fn cached_last_lsn(&self) -> Option<u64> {
        self.cached_last_lsn
    }

    pub fn append(&mut self, record: &LogRecord) -> blocks::Result<u32> {
        record.serialize_into(&mut self.blocks)?;
        self.cached_last_lsn = Some(record.lsn);

        Ok(self.blocks.projected_offset())
    }

    pub fn finalize(self) -> BlocksMut {
        self.blocks
    }
}

pub struct LogSegmentRecordReader {
    blocks: Blocks,
}

impl LogSegmentRecordReader {
    pub fn new(blocks: Blocks) -> Self {
        Self { blocks }
    }

    pub fn next_record(&mut self) -> Result<Option<LogRecord>> {
        if let Some(block) = self.blocks.next_block()? {
            return Ok(Some(LogRecord::try_deserialize_from(block)?));
        }

        Ok(None)
    }
}
