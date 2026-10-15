use bytes::{BufMut as _, BytesMut};
use rmcp::transport::async_rw::JsonRpcMessageCodec;
use serde::{Serialize, de::DeserializeOwned};
use std::io::{self, Write};
use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};
use tokio_util::codec::{Decoder, Encoder};

pub(super) struct BoundedCodec<T> {
    decoder: JsonRpcMessageCodec<T>,
    maximum: Arc<AtomicU32>,
    decoder_maximum: u32,
}

impl<T> BoundedCodec<T> {
    pub(super) fn new(maximum: Arc<AtomicU32>) -> Self {
        let decoder_maximum = maximum.load(Ordering::Acquire);
        Self {
            decoder: JsonRpcMessageCodec::new_with_max_length(decoder_maximum as usize),
            maximum,
            decoder_maximum,
        }
    }

    fn refresh_decoder_bound(&mut self) {
        let maximum = self.maximum.load(Ordering::Acquire);
        if maximum != self.decoder_maximum {
            self.decoder = JsonRpcMessageCodec::new_with_max_length(maximum as usize);
            self.decoder_maximum = maximum;
        }
    }
}

impl<T: DeserializeOwned> Decoder for BoundedCodec<T> {
    type Item = T;
    type Error = io::Error;

    fn decode(&mut self, buffer: &mut BytesMut) -> io::Result<Option<T>> {
        self.refresh_decoder_bound();
        self.decoder.decode(buffer).map_err(Into::into)
    }

    fn decode_eof(&mut self, buffer: &mut BytesMut) -> io::Result<Option<T>> {
        self.refresh_decoder_bound();
        self.decoder.decode_eof(buffer).map_err(Into::into)
    }
}

impl<T: Serialize> Encoder<T> for BoundedCodec<T> {
    type Error = io::Error;

    fn encode(&mut self, item: T, buffer: &mut BytesMut) -> io::Result<()> {
        let start = buffer.len();
        let maximum = start
            .checked_add(self.maximum.load(Ordering::Acquire) as usize)
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
        let result = serde_json::to_writer(
            BoundedWrite {
                buffer: &mut *buffer,
                maximum,
            },
            &item,
        );
        if let Err(error) = result {
            buffer.truncate(start);
            return Err(io::Error::new(io::ErrorKind::InvalidData, error));
        }
        buffer.put_u8(b'\n');
        Ok(())
    }
}

struct BoundedWrite<'a> {
    buffer: &'a mut BytesMut,
    maximum: usize,
}

impl Write for BoundedWrite<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self
            .buffer
            .len()
            .checked_add(bytes.len())
            .is_none_or(|length| length > self.maximum)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "MCP message exceeds its byte bound",
            ));
        }
        self.buffer.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
