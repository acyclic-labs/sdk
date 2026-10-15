use bytes::{BufMut as _, BytesMut};
use rmcp::transport::async_rw::JsonRpcMessageCodec;
use serde::{Serialize, de::DeserializeOwned};
use std::io::{self, Write};
use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};
use tokio_util::codec::{Decoder, Encoder};

pub(super) trait ByteLimit {
    fn maximum(&self) -> u32;
}

impl ByteLimit for AtomicU32 {
    fn maximum(&self) -> u32 {
        self.load(Ordering::Acquire)
    }
}

pub(super) struct BoundedCodec<T, L: ByteLimit = AtomicU32> {
    decoder: JsonRpcMessageCodec<T>,
    maximum: Arc<L>,
    decoder_maximum: u32,
}

impl<T, L: ByteLimit> BoundedCodec<T, L> {
    pub(super) fn new(maximum: Arc<L>) -> Self {
        let decoder_maximum = maximum.maximum();
        Self {
            decoder: JsonRpcMessageCodec::new_with_max_length(decoder_maximum as usize),
            maximum,
            decoder_maximum,
        }
    }

    fn refresh_decoder_bound(&mut self) {
        let maximum = self.maximum.maximum();
        if maximum != self.decoder_maximum {
            self.decoder = JsonRpcMessageCodec::new_with_max_length(maximum as usize);
            self.decoder_maximum = maximum;
        }
    }
}

impl<T: DeserializeOwned, L: ByteLimit> Decoder for BoundedCodec<T, L> {
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

impl<T: Serialize, L: ByteLimit> Encoder<T> for BoundedCodec<T, L> {
    type Error = io::Error;

    fn encode(&mut self, item: T, buffer: &mut BytesMut) -> io::Result<()> {
        let start = buffer.len();
        let maximum = start
            .checked_add(self.maximum.maximum() as usize)
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

#[cfg(test)]
mod tests {
    use super::*;
    use loom::sync::{atomic::AtomicU32 as ModelLimit, mpsc};

    impl ByteLimit for ModelLimit {
        fn maximum(&self) -> u32 {
            self.load(Ordering::Acquire)
        }
    }

    #[test]
    fn catalog_to_tool_handoff_updates_the_real_encoder_and_decoder() {
        loom::model(|| {
            let limit = Arc::new(ModelLimit::new(16));
            let follower_limit = Arc::clone(&limit);
            let (ready_send, ready_receive) = mpsc::channel();
            let (tool_send, tool_receive) = mpsc::channel();
            let follower = loom::thread::spawn(move || {
                let mut codec = BoundedCodec::<serde_json::Value, _>::new(follower_limit);
                ready_send.send(()).unwrap();
                tool_receive.recv().unwrap();
                let value = serde_json::json!("x".repeat(32));
                let mut bytes = BytesMut::new();
                codec.encode(value.clone(), &mut bytes).unwrap();
                assert_eq!(codec.decode(&mut bytes).unwrap(), Some(value));
            });
            ready_receive.recv().unwrap();
            limit.store(64, Ordering::Release);
            tool_send.send(()).unwrap();
            follower.join().unwrap();
        });
    }
}
