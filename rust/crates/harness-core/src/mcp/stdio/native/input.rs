//! Blocking stdio admission over the same codec as native MCP clients.
use super::{Arc, AtomicU32, codec::BoundedCodec};
use bytes::BytesMut;
use serde_json::Value;
use std::io::{self, Read};
use tokio_util::codec::Decoder as _;

/// Bounded JSON lines from a blocking native pipe, without waiting for EOF.
///
/// Framing, CRLF handling and JSON decoding belong to the shared MCP codec.
/// A framing, decoding or I/O failure ends the connection.
pub struct JsonLineReader<R> {
    input: R,
    codec: BoundedCodec<Value>,
    buffer: BytesMut,
    maximum: usize,
    ended: bool,
}

impl<R: Read> JsonLineReader<R> {
    /// Admit at most `maximum` bytes before a newline, plus one detection byte.
    pub fn new(input: R, maximum: u32) -> Self {
        Self {
            input,
            codec: BoundedCodec::new(Arc::new(AtomicU32::new(maximum))),
            buffer: BytesMut::new(),
            maximum: maximum as usize,
            ended: false,
        }
    }

    fn read_next(&mut self) -> io::Result<Option<Value>> {
        loop {
            if let Some(value) = self.codec.decode(&mut self.buffer)? {
                return Ok(Some(value));
            }
            let mut chunk = [0; 8192];
            let admitted = self
                .maximum
                .saturating_sub(self.buffer.len())
                .saturating_add(1);
            let length = admitted.min(chunk.len());
            let chunk = chunk.get_mut(..length).ok_or(io::ErrorKind::InvalidInput)?;
            match self.input.read(chunk) {
                Ok(0) => {
                    self.ended = true;
                    return self.codec.decode_eof(&mut self.buffer);
                }
                Ok(count) => self
                    .buffer
                    .extend_from_slice(chunk.get(..count).ok_or(io::ErrorKind::InvalidData)?),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
    }
}

impl<R: Read> Iterator for JsonLineReader<R> {
    type Item = io::Result<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.ended {
            return None;
        }
        match self.read_next() {
            Ok(value) => value.map(Ok),
            Err(error) => {
                self.ended = true;
                Some(Err(error))
            }
        }
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use serde_json::json;

    struct OpenPeer {
        remaining: usize,
    }
    impl Read for OpenPeer {
        fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
            if self.remaining == 0 {
                // The peer stays open; asking for another byte would block.
                return Err(io::Error::new(io::ErrorKind::WouldBlock, "peer stays open"));
            }
            let count = bytes.len().min(self.remaining);
            bytes
                .get_mut(..count)
                .ok_or(io::ErrorKind::InvalidInput)?
                .fill(b'x');
            self.remaining -= count;
            Ok(count)
        }
    }

    #[test]
    fn newline_less_open_peer_is_rejected_before_another_read() {
        let mut reader = JsonLineReader::new(OpenPeer { remaining: 65_537 }, 65_536);
        let error = reader
            .next()
            .expect("frame result")
            .expect_err("oversized frame");
        assert_ne!(error.kind(), io::ErrorKind::WouldBlock);
        assert_eq!(reader.input.remaining, 0);
        assert!(reader.buffer.len() <= 65_537);
        assert!(reader.next().is_none(), "failed connections stay closed");

        // Negative control: the same still-open peer with the bound removed
        // asks for more input instead of rejecting the oversized frame.
        let mut unbounded = JsonLineReader::new(OpenPeer { remaining: 65_537 }, u32::MAX);
        let error = unbounded
            .next()
            .expect("frame result")
            .expect_err("open peer");
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    }

    #[test]
    fn shared_codec_handles_crlf_multiple_frames_and_trailing_eof() -> io::Result<()> {
        let bytes = b"{\"id\":1}\r\n{\"id\":2}\n{\"id\":3}";
        let values = JsonLineReader::new(bytes.as_slice(), 64).collect::<io::Result<Vec<_>>>()?;
        assert_eq!(values, [json!({"id":1}), json!({"id":2}), json!({"id":3})]);
        Ok(())
    }

    #[test]
    fn buffered_frames_do_not_request_more_bytes_from_an_open_pipe() -> io::Result<()> {
        struct OpenAfter(std::io::Cursor<Vec<u8>>);
        impl Read for OpenAfter {
            fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
                match self.0.read(bytes)? {
                    0 => Err(io::ErrorKind::WouldBlock.into()),
                    count => Ok(count),
                }
            }
        }
        let mut reader = JsonLineReader::new(
            OpenAfter(std::io::Cursor::new(b"{\"id\":1}\r\n{\"id\":2}\n".to_vec())),
            64,
        );
        assert_eq!(reader.next().expect("first frame")?, json!({"id":1}));
        assert_eq!(reader.next().expect("second frame")?, json!({"id":2}));
        Ok(())
    }
}
