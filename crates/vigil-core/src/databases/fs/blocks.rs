use std::{io, mem};

use bytes::{BufMut, Bytes, BytesMut};
use serde::Serialize;

#[derive(Debug, Serialize)]
pub enum BlockError {
    OutOfSpace,
    WroteTooMuch,
    WroteTooLittle,
}

pub type Result<A> = std::result::Result<A, BlockError>;

#[derive(Debug, Clone, Serialize)]
pub struct Block {
    pub offset: usize,
    pub data: Bytes,
}

pub struct Blocks {}

impl Blocks {
    pub fn next(&mut self) -> io::Result<Option<Block>> {
        todo!()
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
