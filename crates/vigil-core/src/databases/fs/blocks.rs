use std::mem;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::Serialize;

#[derive(Debug, Serialize)]
pub enum BlockError {
    OutOfSpace,
    WroteTooMuch,
    WroteTooLittle,
    InvalidBlockFormat,
    NotEnoughDataLeft,
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
}

#[derive(Serialize)]
pub struct BlocksMut {
    limit: usize,
    offset: usize,
    buf: BytesMut,
}

impl BlocksMut {
    pub fn new(limit: usize, offset: usize, buf: BytesMut) -> Self {
        Self { limit, offset, buf }
    }

    #[cfg(test)]
    pub fn empty(limit: usize) -> Self {
        Self::new(limit, 0, BytesMut::new())
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
        self.buf.put_u32_le(need as u32);

        Ok(OpenedBlock {
            need,
            start_offset: self.buf.len(),
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
    start_offset: usize,
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
        &self.inner.buf[self.start_offset..(self.start_offset + self.written)]
    }

    pub fn finalize(self) -> Result<ClosedBlock> {
        if self.written < self.need {
            return Err(BlockError::WroteTooLittle);
        }

        self.inner.buf.put_u32_le(self.need as u32);

        Ok(ClosedBlock(()))
    }
}
