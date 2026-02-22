use std::mem;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::Serialize;

pub const MIDPOINT_SECTION_SIZE: usize = 128;
pub const MIDPOINT_SIZE: usize = mem::size_of::<u64>() + mem::size_of::<u32>();
pub const MIDPOINT_MAX_COUNT: usize = MIDPOINT_SECTION_SIZE / MIDPOINT_SIZE;

#[derive(Debug, Serialize)]
pub enum BlockError {
    OutOfSpace,
    WroteTooMuch,
    WroteTooLittle,
    InvalidBlockFormat,
    NotEnoughDataLeft,
    OffsetOutOfBound,
    TooManyMidpoints,
}

pub type Result<A> = std::result::Result<A, BlockError>;

#[derive(Debug, Clone, Serialize)]
pub struct Block {
    pub offset: usize,
    pub read: usize,
    pub data: Bytes,
}

impl Block {
    pub fn get_u64_le(&mut self) -> Result<u64> {
        if self.data.remaining() < self.read + mem::size_of::<u64>() {
            return Err(BlockError::NotEnoughDataLeft);
        }

        let value = (&self.data[self.read..(self.read + mem::size_of::<u64>())]).get_u64_le();
        self.read += mem::size_of::<u64>();

        Ok(value)
    }

    pub fn get_u32_le(&mut self) -> Result<u32> {
        if self.data.remaining() < self.read + mem::size_of::<u32>() {
            return Err(BlockError::NotEnoughDataLeft);
        }

        let value = (&self.data[self.read..(self.read + mem::size_of::<u32>())]).get_u32_le();
        self.read += mem::size_of::<u32>();

        Ok(value)
    }

    pub fn get_u16_le(&mut self) -> Result<u16> {
        if self.data.remaining() < self.read + mem::size_of::<u16>() {
            return Err(BlockError::NotEnoughDataLeft);
        }

        let value = (&self.data[self.read..(self.read + mem::size_of::<u16>())]).get_u16_le();
        self.read += mem::size_of::<u16>();

        Ok(value)
    }

    pub fn get_u8(&mut self) -> Result<u8> {
        if self.data.remaining() < self.read + mem::size_of::<u8>() {
            return Err(BlockError::NotEnoughDataLeft);
        }

        let value = (&self.data[self.read..(self.read + mem::size_of::<u8>())]).get_u8();
        self.read += mem::size_of::<u8>();

        Ok(value)
    }

    pub fn copy_to_bytes(&mut self, cnt: usize) -> Result<Bytes> {
        if self.data.remaining() < self.read + cnt {
            return Err(BlockError::NotEnoughDataLeft);
        }

        let bytes = self.data.clone().split_off(self.read).copy_to_bytes(cnt);

        self.read += cnt;

        Ok(bytes)
    }

    pub fn remaining(&self) -> usize {
        self.data
            .remaining()
            .checked_sub(self.read)
            .unwrap_or_default()
    }

    pub fn read_content(&self) -> Bytes {
        self.data.clone().split_to(self.read)
    }
}

#[derive(Clone)]
pub struct Blocks {
    start_offset: usize,
    read: usize,
    bytes: Bytes,
}

impl Blocks {
    pub fn new(start_offset: usize, bytes: Bytes) -> Self {
        Self {
            start_offset,
            read: 0,
            bytes,
        }
    }

    pub fn next_block(&mut self) -> Result<Option<Block>> {
        if self.bytes.remaining() < mem::size_of::<u32>() {
            return Ok(None);
        }

        let local_offset = self.read;
        let prefix = self.bytes.get_u32_le() as usize;

        if self.bytes.remaining() < prefix + mem::size_of::<u32>() {
            return Err(BlockError::InvalidBlockFormat);
        }

        let data = self.bytes.copy_to_bytes(prefix);
        let suffix = self.bytes.get_u32_le() as usize;
        if prefix != suffix {
            return Err(BlockError::InvalidBlockFormat);
        }

        self.read += prefix + 2 * mem::size_of::<u32>();

        Ok(Some(Block {
            offset: self.start_offset + local_offset,
            read: 0,
            data,
        }))
    }

    pub fn at(&self, offset: u32) -> Result<Self> {
        let offset = offset as usize;

        if offset < self.start_offset || self.start_offset + self.bytes.len() < offset {
            return Err(BlockError::OffsetOutOfBound);
        }

        Ok(Self {
            start_offset: offset,
            read: 0,
            bytes: self.bytes.clone().split_off(offset - self.start_offset),
        })
    }
}

#[derive(Serialize)]
pub struct Midpoint {
    pub lsn: u64,
    pub offset: u32,
}

pub struct Midpoints {
    inner: Vec<Midpoint>,
}

impl Midpoints {
    pub fn offset_for_lsn(&self, lns: u64) -> u32 {
        if self.inner.is_empty() {
            return 0;
        }

        match self.inner.binary_search_by(|mid| mid.lsn.cmp(&lns)) {
            Ok(idx) => self.inner[idx].offset,
            Err(idx) => self.inner[idx].offset,
        }
    }

    pub fn serialize_into(&self, buf: &mut BytesMut) -> Result<()> {
        if self.inner.len() > MIDPOINT_MAX_COUNT {
            return Err(BlockError::TooManyMidpoints);
        }

        buf.reserve(MIDPOINT_SECTION_SIZE);
        let mut written = 0usize;

        for mid in &self.inner {
            buf.put_u64_le(mid.lsn);
            buf.put_u32_le(mid.offset);

            written += size_of::<u64>() + size_of::<u32>();
        }

        let mid_offset = MIDPOINT_SECTION_SIZE - size_of::<u16>();
        buf.advance(mid_offset - written);
        buf.put_u16_le(self.inner.len() as u16);

        Ok(())
    }
}

pub struct BlockMutArgs {
    pub limit: usize,
    pub offset: usize,
    pub last_mid_offset: u32,
}

#[derive(Serialize)]
pub struct BlocksMut {
    limit: usize,
    offset: usize,
    buf: BytesMut,
    mid_freq: u32,
    last_mid_offset: u32,
    midpoints: Vec<u32>,
}

impl BlocksMut {
    pub fn new(args: BlockMutArgs, buf: BytesMut) -> Self {
        Self {
            limit: args.limit,
            offset: args.offset,
            buf,
            last_mid_offset: args.last_mid_offset,
            mid_freq: args.limit as u32 / 10,
            midpoints: Vec::new(),
        }
    }

    #[cfg(test)]
    pub fn empty(limit: usize) -> Self {
        Self::empty_with_offset(limit, 0)
    }

    #[cfg(test)]
    pub fn empty_with_offset(limit: usize, offset: usize) -> Self {
        Self::new(
            BlockMutArgs {
                limit,
                offset,
                last_mid_offset: 0,
            },
            BytesMut::new(),
        )
    }

    pub fn midpoints(&self) -> &[u32] {
        &self.midpoints
    }

    pub fn available_space(&self) -> usize {
        self.limit
            .checked_sub(self.offset + self.buf.len())
            .unwrap_or_default()
    }

    pub fn open(&mut self, need: usize) -> Result<OpenedBlock<'_>> {
        let actual_need = mem::size_of::<u32>() + need + mem::size_of::<u32>();

        if self.available_space() < actual_need {
            return Err(BlockError::OutOfSpace);
        }

        self.buf.reserve(actual_need);
        let block_start_offset = self.offset + self.buf.len();
        self.buf.put_u32_le(need as u32);
        let buf_start_offset = self.buf.len();

        Ok(OpenedBlock {
            need,
            block_start_offset,
            buf_start_offset,
            inner: self,
            written: 0,
        })
    }

    pub fn projected_offset(&self) -> u32 {
        (self.offset + self.buf.len()) as u32
    }

    #[cfg(test)]
    pub fn bytes_mut(&mut self) -> &mut BytesMut {
        &mut self.buf
    }

    #[cfg(test)]
    pub fn freeze(self) -> Blocks {
        Blocks {
            start_offset: self.offset,
            read: 0,
            bytes: self.buf.freeze(),
        }
    }
}

#[derive(Serialize)]
pub struct OpenedBlock<'a> {
    need: usize,
    block_start_offset: usize,
    buf_start_offset: usize,
    written: usize,
    inner: &'a mut BlocksMut,
}

#[derive(Serialize)]
pub struct ClosedBlock(());

impl OpenedBlock<'_> {
    pub fn put_u32_le(&mut self, value: u32) -> Result<()> {
        self.written += mem::size_of::<u32>();

        if self.written > self.need {
            return Err(BlockError::WroteTooMuch);
        }

        self.inner.buf.put_u32_le(value);

        Ok(())
    }

    pub fn put_u64_le(&mut self, value: u64) -> Result<()> {
        self.written += mem::size_of::<u64>();

        if self.written > self.need {
            return Err(BlockError::WroteTooMuch);
        }

        self.inner.buf.put_u64_le(value);

        Ok(())
    }

    pub fn put_u16_le(&mut self, value: u16) -> Result<()> {
        self.written += mem::size_of::<u16>();

        if self.written > self.need {
            return Err(BlockError::WroteTooMuch);
        }

        self.inner.buf.put_u16_le(value);

        Ok(())
    }

    pub fn put_u8(&mut self, value: u8) -> Result<()> {
        self.written += mem::size_of::<u8>();

        if self.written > self.need {
            return Err(BlockError::WroteTooMuch);
        }

        self.inner.buf.put_u8(value);

        Ok(())
    }

    pub fn put_bytes(&mut self, value: Bytes) -> Result<()> {
        self.written += value.len();

        if self.written > self.need {
            return Err(BlockError::WroteTooMuch);
        }

        self.inner.buf.put_slice(value.as_ref());

        Ok(())
    }

    pub fn zeroes(&mut self, cnt: usize) -> Result<()> {
        self.written += cnt;

        if self.written > self.need {
            return Err(BlockError::WroteTooMuch);
        }

        self.inner.buf.put_bytes(0, cnt);

        Ok(())
    }

    pub fn written_bytes(&self) -> &[u8] {
        &self.inner.buf[self.buf_start_offset..(self.buf_start_offset + self.written)]
    }

    pub fn finalize(self) -> Result<ClosedBlock> {
        if self.written < self.need {
            return Err(BlockError::WroteTooLittle);
        }

        self.inner.buf.put_u32_le(self.need as u32);
        if (self.inner.projected_offset() - self.inner.last_mid_offset) > self.inner.mid_freq {
            self.inner.midpoints.push(self.block_start_offset as u32);
            self.inner.last_mid_offset = self.block_start_offset as u32;
        }

        Ok(ClosedBlock(()))
    }
}
