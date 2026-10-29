//! Protobuf message boundaries over a byte stream with arbitrary chunking.
use super::{StreamError, decode_frame, wire};

/// Incremental length-delimited session frames. Retains only an incomplete
/// frame; complete contiguous frames decode directly from their input chunk.
#[derive(Default)]
pub struct FrameDecoder {
    grpc: bool,
    prefix: [u8; 10],
    prefix_len: usize,
    length: Option<usize>,
    body: Vec<u8>,
}

impl FrameDecoder {
    /// Standard uncompressed gRPC envelopes: one flag byte and a big-endian
    /// u32 length. The message admission and incomplete body buffer are shared
    /// with protobuf length-delimited framing.
    #[must_use]
    pub fn grpc() -> Self {
        Self {
            grpc: true,
            ..Self::default()
        }
    }

    /// Consume bytes through the next frame, leaving any following frames in
    /// `input`. An incomplete frame returns `None` after consuming the chunk.
    pub fn read(&mut self, input: &mut &[u8]) -> Result<Option<wire::SessionFrame>, StreamError> {
        while self.length.is_none() {
            let Some((&byte, rest)) = input.split_first() else {
                return Ok(None);
            };
            *input = rest;
            if self.prefix_len == self.prefix.len() {
                return Err(StreamError::InvalidResponse);
            }
            self.prefix[self.prefix_len] = byte;
            self.prefix_len += 1;
            let length = if self.grpc {
                if self.prefix[0] != 0 {
                    return Err(if self.prefix[0] == 1 {
                        StreamError::Unsupported
                    } else {
                        StreamError::InvalidResponse
                    });
                }
                if self.prefix_len != 5 {
                    continue;
                }
                u32::from_be_bytes(
                    self.prefix[1..5]
                        .try_into()
                        .map_err(|_| StreamError::InvalidResponse)?,
                ) as usize
            } else {
                if byte & 0x80 != 0 {
                    if self.prefix_len == self.prefix.len() {
                        return Err(StreamError::InvalidResponse);
                    }
                    continue;
                }
                let length = prost::decode_length_delimiter(&self.prefix[..self.prefix_len])
                    .map_err(|_| StreamError::InvalidResponse)?;
                if prost::length_delimiter_len(length) != self.prefix_len {
                    return Err(StreamError::InvalidResponse);
                }
                length
            };
            if length > crate::MAX_COMMAND_BYTES {
                return Err(StreamError::LimitExceeded);
            }
            self.length = Some(length);
        }
        let length = self.length.ok_or(StreamError::InvalidResponse)?;
        let frame = if self.body.is_empty() && input.len() >= length {
            let (frame, rest) = input.split_at(length);
            *input = rest;
            decode_frame(frame)?
        } else {
            let received = (length - self.body.len()).min(input.len());
            let (part, rest) = input.split_at(received);
            *input = rest;
            self.body.extend_from_slice(part);
            if self.body.len() != length {
                return Ok(None);
            }
            let frame = decode_frame(&self.body)?;
            self.body.clear();
            frame
        };
        self.prefix_len = 0;
        self.length = None;
        Ok(Some(frame))
    }

    /// EOF is valid only between complete messages. An advertised but missing
    /// body, partial prefix or truncated body is a typed response failure.
    pub fn finish(&self) -> Result<(), StreamError> {
        if self.prefix_len == 0 && self.length.is_none() {
            Ok(())
        } else {
            Err(StreamError::InvalidResponse)
        }
    }
}

/// Encode directly into the final gRPC envelope; no intermediate payload copy.
pub fn encode_grpc_frame(frame: &wire::SessionFrame) -> Result<Vec<u8>, StreamError> {
    use prost::Message as _;
    let length = frame.encoded_len();
    if length > crate::MAX_COMMAND_BYTES {
        return Err(StreamError::LimitExceeded);
    }
    let mut bytes = Vec::with_capacity(5 + length);
    bytes.push(0);
    bytes.extend_from_slice(
        &u32::try_from(length)
            .map_err(|_| StreamError::LimitExceeded)?
            .to_be_bytes(),
    );
    frame
        .encode(&mut bytes)
        .map_err(|_| StreamError::InvalidResponse)?;
    Ok(bytes)
}
