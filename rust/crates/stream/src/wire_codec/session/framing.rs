//! Protobuf message boundaries over a byte stream with arbitrary chunking.
use super::{StreamError, decode_frame, wire};

#[derive(Debug, thiserror::Error)]
enum FramingFailure {
    #[error("session length prefix exceeds {bytes} bytes")]
    Prefix { bytes: usize },
    #[error("unsupported session gRPC flag {flag}")]
    Flag { flag: u8 },
    #[error("noncanonical session length {length} encoded in {bytes} bytes")]
    Noncanonical { length: usize, bytes: usize },
    #[error("session frame length {length} exceeds maximum {maximum}")]
    Length { length: usize, maximum: usize },
    #[error(
        "session frame ended with {prefix_bytes} prefix bytes, advertised {length:?}, received {received}"
    )]
    Incomplete {
        prefix_bytes: usize,
        length: Option<usize>,
        received: usize,
    },
}

/// Incremental length-delimited session frames. Retains only an incomplete
/// frame; complete contiguous frames decode directly from their input chunk.
pub struct FrameDecoder<T = wire::SessionFrame> {
    grpc: bool,
    prefix: [u8; 10],
    prefix_len: usize,
    length: Option<usize>,
    body: Vec<u8>,
    maximum: usize,
    decode: fn(&[u8]) -> Result<T, StreamError>,
}

impl Default for FrameDecoder {
    fn default() -> Self {
        Self::with_decoder(crate::MAX_COMMAND_BYTES, decode_frame)
    }
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
}

impl<T> FrameDecoder<T> {
    /// Uses the same framing for another canonical message. The decoder owns
    /// validation at the external boundary; `maximum` bounds one frame only.
    pub fn with_decoder(maximum: usize, decode: fn(&[u8]) -> Result<T, StreamError>) -> Self {
        Self {
            grpc: false,
            prefix: [0; 10],
            prefix_len: 0,
            length: None,
            body: Vec::new(),
            maximum,
            decode,
        }
    }

    /// Consume bytes through the next frame, leaving any following frames in
    /// `input`. An incomplete frame returns `None` after consuming the chunk.
    pub fn read(&mut self, input: &mut &[u8]) -> Result<Option<T>, StreamError> {
        while self.length.is_none() {
            let Some((&byte, rest)) = input.split_first() else {
                return Ok(None);
            };
            *input = rest;
            if self.prefix_len == self.prefix.len() {
                return Err(
                    StreamError::InvalidResponse.with_source(FramingFailure::Prefix {
                        bytes: self.prefix_len,
                    }),
                );
            }
            self.prefix[self.prefix_len] = byte;
            self.prefix_len += 1;
            let length = if self.grpc {
                if self.prefix[0] != 0 {
                    return Err((if self.prefix[0] == 1 {
                        StreamError::Unsupported
                    } else {
                        StreamError::InvalidResponse
                    })
                    .with_source(FramingFailure::Flag {
                        flag: self.prefix[0],
                    }));
                }
                if self.prefix_len != 5 {
                    continue;
                }
                u32::from_be_bytes(
                    self.prefix[1..5]
                        .try_into()
                        .map_err(|source| StreamError::InvalidResponse.with_source(source))?,
                ) as usize
            } else {
                if byte & 0x80 != 0 {
                    if self.prefix_len == self.prefix.len() {
                        return Err(StreamError::InvalidResponse.with_source(
                            FramingFailure::Prefix {
                                bytes: self.prefix_len,
                            },
                        ));
                    }
                    continue;
                }
                let length = prost::decode_length_delimiter(&self.prefix[..self.prefix_len])
                    .map_err(|source| StreamError::InvalidResponse.with_source(source))?;
                if prost::length_delimiter_len(length) != self.prefix_len {
                    return Err(StreamError::InvalidResponse.with_source(
                        FramingFailure::Noncanonical {
                            length,
                            bytes: self.prefix_len,
                        },
                    ));
                }
                length
            };
            if length > self.maximum {
                return Err(
                    StreamError::LimitExceeded.with_source(FramingFailure::Length {
                        length,
                        maximum: self.maximum,
                    }),
                );
            }
            self.length = Some(length);
        }
        let length = self.length.ok_or_else(|| self.incomplete())?;
        let frame = if self.body.is_empty() && input.len() >= length {
            let (frame, rest) = input.split_at(length);
            *input = rest;
            (self.decode)(frame)?
        } else {
            let received = (length - self.body.len()).min(input.len());
            let (part, rest) = input.split_at(received);
            *input = rest;
            self.body.extend_from_slice(part);
            if self.body.len() != length {
                return Ok(None);
            }
            let frame = (self.decode)(&self.body)?;
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
            Err(self.incomplete())
        }
    }

    fn incomplete(&self) -> StreamError {
        StreamError::InvalidResponse.with_source(FramingFailure::Incomplete {
            prefix_bytes: self.prefix_len,
            length: self.length,
            received: self.body.len(),
        })
    }
}

/// Encode directly into the final gRPC envelope; no intermediate payload copy.
pub fn encode_grpc_frame(frame: &wire::SessionFrame) -> Result<Vec<u8>, StreamError> {
    use prost::Message as _;
    let length = frame.encoded_len();
    if length > crate::MAX_COMMAND_BYTES {
        return Err(
            StreamError::LimitExceeded.with_source(FramingFailure::Length {
                length,
                maximum: crate::MAX_COMMAND_BYTES,
            }),
        );
    }
    let mut bytes = Vec::with_capacity(5 + length);
    bytes.push(0);
    bytes.extend_from_slice(
        &u32::try_from(length)
            .map_err(|source| StreamError::LimitExceeded.with_source(source))?
            .to_be_bytes(),
    );
    frame
        .encode(&mut bytes)
        .map_err(|source| StreamError::InvalidResponse.with_source(source))?;
    Ok(bytes)
}
