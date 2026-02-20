use std::mem;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::Serialize;

pub const MAGIC_NUM: u32 = 0x57414C00;
pub const VERSION: u16 = 0x01;
pub const SEGMENT_HEADER_SIZE: usize = 128;
pub const RECORD_MIN_SIZE: usize = mem::size_of::<u32>() // record len
     + mem::size_of::<u8>()
     + mem::size_of::<u8>()
     + mem::size_of::<u16>() // data len
     + mem::size_of::<u32>() // CRC 32
     + mem::size_of::<u32>(); // record len

#[derive(Serialize, Debug)]
pub enum WalError {
    WrongFileFormat,
    TooSmall,
    LenghMismatch,
    ChecksumMismatch,
}

#[derive(PartialEq, Eq, Copy, Clone, Serialize, Debug)]
pub struct WalSegHeader {
    pub version: u16,
    pub segment_id: u64,
}

impl WalSegHeader {
    pub fn serialize_into(&self, buf: &mut BytesMut) {
        buf.reserve(SEGMENT_HEADER_SIZE);
        buf.put_u32_le(MAGIC_NUM);
        buf.put_u16_le(self.version);
        buf.put_u64_le(self.segment_id);
        buf.put_bytes(0, SEGMENT_HEADER_SIZE - buf.len());
    }

    pub fn try_deserialize_from(mut bytes: Bytes) -> Result<Self, WalError> {
        if bytes.len() < SEGMENT_HEADER_SIZE {
            return Err(WalError::TooSmall);
        }

        if bytes.get_u32_le() != MAGIC_NUM {
            return Err(WalError::WrongFileFormat);
        }

        let version = bytes.get_u16_le();
        let segment_id = bytes.get_u64_le();

        Ok(Self {
            version,
            segment_id,
        })
    }
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
pub enum WalOp {
    Unknown(u8),
    Put,
    Delete,
}

impl From<u8> for WalOp {
    fn from(value: u8) -> Self {
        match value {
            0x01 => Self::Put,
            0x02 => Self::Delete,
            x => Self::Unknown(x),
        }
    }
}

impl From<WalOp> for u8 {
    fn from(value: WalOp) -> Self {
        match value {
            WalOp::Unknown(x) => x,
            WalOp::Put => 0x01,
            WalOp::Delete => 0x02,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
pub enum WalContentType {
    Unknown(u8),
    Json,
}

impl From<WalContentType> for u8 {
    fn from(value: WalContentType) -> Self {
        match value {
            WalContentType::Unknown(x) => x,
            WalContentType::Json => 0x01,
        }
    }
}

impl From<u8> for WalContentType {
    fn from(value: u8) -> Self {
        match value {
            0x01 => Self::Json,
            x => Self::Unknown(x),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize)]
pub struct WalRecord {
    pub lsn: u64,
    pub op: WalOp,
    pub content_type: WalContentType,
    pub data: Bytes,
}

impl WalRecord {
    pub fn size_on_disk(&self) -> usize {
        mem::size_of::<u32>() // frame len
          + mem::size_of::<u64>() // lsn
          + mem::size_of::<u8>() // wal op
          + mem::size_of::<u8>() // content type
          + mem::size_of::<u16>() // data len
          + self.data.len()
          + mem::size_of::<u32>() // CRC 32
          + mem::size_of::<u32>() // frame len
    }

    pub fn serialize_into(&self, buf: &mut BytesMut) {
        buf.reserve(self.size_on_disk());
        let len = self.size_on_disk() as u32;
        buf.put_u32_le(len);
        buf.put_u64_le(self.lsn);
        buf.put_u8(self.op.into());
        buf.put_u8(self.content_type.into());
        buf.put_u16_le(self.data.len() as u16);
        buf.put_slice(self.data.as_ref());

        let checksum = crc32fast::hash(&buf[mem::size_of::<u32>()..]);
        buf.put_u32_le(checksum);
        buf.put_u32_le(len);
    }

    pub fn try_deserialize_from(mut bytes: Bytes) -> Result<Self, WalError> {
        if bytes.len() < RECORD_MIN_SIZE {
            return Err(WalError::TooSmall);
        }

        let content = bytes.clone();

        let pre_len = bytes.get_u32_le();
        let lsn = bytes.get_u64_le();
        let op = bytes.get_u8().into();
        let content_type = bytes.get_u8().into();
        let data_len = bytes.get_u16_le();
        let data = bytes.copy_to_bytes(data_len as usize);
        let checksum = bytes.get_u32_le();
        let leading_offset = mem::size_of::<u32>()
            + mem::size_of::<u64>()
            + mem::size_of::<u8>()
            + mem::size_of::<u8>()
            + mem::size_of::<u16>()
            + data.len();

        if checksum != crc32fast::hash(&content[mem::size_of::<u32>()..leading_offset]) {
            return Err(WalError::ChecksumMismatch);
        }

        let suf_len = bytes.get_u32_le();

        if pre_len != suf_len {
            return Err(WalError::LenghMismatch);
        }

        Ok(Self {
            lsn,
            op,
            content_type,
            data,
        })
    }
}
