use crate::{Arc, AtomicU64, Ordering};
use bytes::{BufMut as _, BytesMut};
use rmcp::transport::async_rw::JsonRpcMessageCodec;
use serde::{Serialize, de::DeserializeOwned};
use std::io::{self, Write};
use tokio_util::codec::{Decoder, Encoder};

/// One shared frame policy, published atomically for both directions.
/// An absent policy adds no byte ceiling; a supplied zero admits no payload.
pub struct FrameBytePolicy {
    // Bit 32 marks Some; the low 32 bits retain every u32 value, including zero.
    // None is zero storage, not a byte-capacity fallback.
    encoded: AtomicU64,
}

impl FrameBytePolicy {
    /// Retains the selected policy without allocation or I/O.
    pub fn new(maximum: impl Into<Option<u32>>) -> Self {
        Self {
            encoded: AtomicU64::new(Self::encode(maximum.into())),
        }
    }

    /// Publishes the policy observed by the next encoder or decoder operation.
    pub fn set(&self, maximum: impl Into<Option<u32>>) {
        self.encoded
            .store(Self::encode(maximum.into()), Ordering::Release);
    }

    fn get(&self) -> Option<u32> {
        let [a, b, c, d, present, ..] = self.encoded.load(Ordering::Acquire).to_le_bytes();
        (present != 0).then_some(u32::from_le_bytes([a, b, c, d]))
    }

    fn encode(maximum: Option<u32>) -> u64 {
        maximum.map_or(0, |maximum| (1_u64 << 32) | u64::from(maximum))
    }
}

/// JSON-line codec over one shared, atomically published optional byte policy.
pub struct BoundedCodec<T> {
    decoder: JsonRpcMessageCodec<T>,
    maximum: Arc<FrameBytePolicy>,
    decoder_maximum: Option<u32>,
}

impl<T> BoundedCodec<T> {
    /// Creates a codec that observes updates to the same original policy owner.
    pub fn new(maximum: Arc<FrameBytePolicy>) -> Self {
        let decoder_maximum = maximum.get();
        Self {
            decoder: Self::decoder(decoder_maximum),
            maximum,
            decoder_maximum,
        }
    }

    fn decoder(maximum: Option<u32>) -> JsonRpcMessageCodec<T> {
        match maximum {
            Some(maximum) => JsonRpcMessageCodec::new_with_max_length(maximum as usize),
            None => JsonRpcMessageCodec::new(),
        }
    }

    fn refresh_decoder_bound(&mut self) {
        let maximum = self.maximum.get();
        if maximum != self.decoder_maximum {
            self.decoder = Self::decoder(maximum);
            self.decoder_maximum = maximum;
        }
    }
}

impl<T: DeserializeOwned> Decoder for BoundedCodec<T> {
    type Item = T;
    type Error = io::Error;

    fn decode(&mut self, buffer: &mut BytesMut) -> io::Result<Option<T>> {
        self.refresh_decoder_bound();
        loop {
            let before = buffer.len();
            let message = self.decoder.decode(buffer).map_err(io::Error::from)?;
            if message.is_some() || buffer.len() == before {
                return Ok(message);
            }
            // rmcp returns None for an ignored notification even when a
            // complete following frame remains. Consume it before waiting
            // for more input; strict progress makes the loop finite.
        }
    }

    fn decode_eof(&mut self, buffer: &mut BytesMut) -> io::Result<Option<T>> {
        match self.decode(buffer)? {
            Some(message) => Ok(Some(message)),
            None => self.decoder.decode_eof(buffer).map_err(Into::into),
        }
    }
}

impl<T: Serialize> Encoder<T> for BoundedCodec<T> {
    type Error = io::Error;

    fn encode(&mut self, item: T, buffer: &mut BytesMut) -> io::Result<()> {
        let start = buffer.len();
        let maximum = self
            .maximum
            .get()
            .map(|maximum| {
                start
                    .checked_add(maximum as usize)
                    .ok_or(io::ErrorKind::InvalidInput)
            })
            .transpose()?;
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
    maximum: Option<usize>,
}

impl Write for BoundedWrite<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self
            .buffer
            .len()
            .checked_add(bytes.len())
            .is_none_or(|length| self.maximum.is_some_and(|maximum| length > maximum))
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

#[cfg(all(test, not(loom)))]
mod frame_tests {
    use super::*;

    #[test]
    fn published_policy_keeps_zero_distinct_from_absence_and_preserves_output() -> io::Result<()> {
        let policy = Arc::new(FrameBytePolicy::new(Some(0)));
        let mut codec = BoundedCodec::<serde_json::Value>::new(Arc::clone(&policy));
        let value = serde_json::json!({"payload": "x".repeat(65_537)});
        let mut output = BytesMut::from(b"retained".as_slice());
        assert!(codec.encode(value.clone(), &mut output).is_err());
        assert_eq!(output.as_ref(), b"retained");

        policy.set(None);
        codec.encode(value.clone(), &mut output)?;
        let mut frame = output.split_off(b"retained".len());
        assert_eq!(codec.decode(&mut frame)?, Some(value.clone()));
        assert!(frame.is_empty());
        assert_eq!(output.as_ref(), b"retained");

        policy.set(64);
        assert!(codec.encode(value, &mut output).is_err());
        assert_eq!(output.as_ref(), b"retained");
        Ok(())
    }

    #[test]
    fn skipped_notification_does_not_hide_the_next_buffered_frame() -> io::Result<()> {
        // The current rmcp server enum accepts extension notifications.
        // Use a response-only receiver to exercise an actual ignored frame.
        #[derive(Debug, serde::Deserialize, PartialEq)]
        struct Response {
            id: u32,
            result: serde_json::Value,
        }
        let messages = b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/private\"}\n{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}\n";
        let expected = Response {
            id: 1,
            result: serde_json::json!({}),
        };
        let mut codec = BoundedCodec::<Response>::new(Arc::new(FrameBytePolicy::new(256)));
        assert_eq!(
            codec.decode(&mut BytesMut::from(messages.as_slice()))?,
            Some(expected)
        );

        // Actual dependency behavior is the negative control: None asks a
        // framed reader to wait despite an already buffered response.
        let mut underlying = JsonRpcMessageCodec::<Response>::new_with_max_length(256);
        let mut pending = BytesMut::from(messages.as_slice());
        assert!(
            underlying
                .decode(&mut pending)
                .map_err(io::Error::from)?
                .is_none()
        );
        assert!(!pending.is_empty());
        Ok(())
    }
}

#[cfg(all(test, loom))]
mod tests {
    use super::*;
    use loom::sync::mpsc;

    fn handoff_model(omit_publication: bool, selected: Option<u32>) {
        loom::model(move || {
            let limit = Arc::new(FrameBytePolicy::new(16));
            let follower_limit = Arc::clone(&limit);
            let (ready_send, ready_receive) = mpsc::channel();
            let (tool_send, tool_receive) = mpsc::channel();
            let follower = loom::thread::spawn(move || {
                let mut codec = BoundedCodec::<serde_json::Value>::new(follower_limit);
                ready_send.send(()).unwrap();
                tool_receive.recv().unwrap();
                let value = serde_json::json!("x".repeat(32));
                let mut bytes = BytesMut::new();
                codec
                    .encode(value.clone(), &mut bytes)
                    .expect("tool bound must reach encoder");
                assert_eq!(codec.decode(&mut bytes).unwrap(), Some(value));
            });
            ready_receive.recv().unwrap();
            // Negative control omits the catalog-to-tool bound publication.
            if !omit_publication {
                limit.set(selected);
            }
            tool_send.send(()).unwrap();
            follower.join().unwrap();
        });
    }

    #[test]
    fn catalog_to_tool_handoff_updates_the_real_encoder_and_decoder() {
        for selected in [None, Some(64)] {
            handoff_model(false, selected);
        }
    }

    #[test]
    #[should_panic(expected = "tool bound must reach encoder")]
    fn negative_control_missing_publication_is_detected() {
        handoff_model(true, None);
    }
}
