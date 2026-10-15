use bytes::{BufMut as _, BytesMut};
use rmcp::transport::async_rw::JsonRpcMessageCodec;
use serde::{Serialize, de::DeserializeOwned};
use std::io::{self, Write};
use tokio_util::codec::{Decoder, Encoder};

pub(super) struct BoundedCodec<T> {
    decoder: JsonRpcMessageCodec<T>,
    maximum: usize,
}

impl<T> BoundedCodec<T> {
    pub(super) fn new(maximum: usize) -> Self {
        Self {
            decoder: JsonRpcMessageCodec::new_with_max_length(maximum),
            maximum,
        }
    }
}

impl<T: DeserializeOwned> Decoder for BoundedCodec<T> {
    type Item = T;
    type Error = io::Error;

    fn decode(&mut self, buffer: &mut BytesMut) -> io::Result<Option<T>> {
        self.decoder.decode(buffer).map_err(Into::into)
    }

    fn decode_eof(&mut self, buffer: &mut BytesMut) -> io::Result<Option<T>> {
        self.decoder.decode_eof(buffer).map_err(Into::into)
    }
}

impl<T: Serialize> Encoder<T> for BoundedCodec<T> {
    type Error = io::Error;

    fn encode(&mut self, item: T, buffer: &mut BytesMut) -> io::Result<()> {
        let start = buffer.len();
        let maximum = start
            .checked_add(self.maximum)
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
