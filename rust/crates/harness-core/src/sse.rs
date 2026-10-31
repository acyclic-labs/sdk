use crate::Result;
use crate::{Error, ModelTransportFailure};
use acyclic_stream::BoxProviderStream;
use bytes::{Buf as _, Bytes};
use futures::{Stream, StreamExt as _};

/// One complete data event and the original funding for its buffer.
pub struct Frame {
    data: String,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    _custody: Option<acyclic_fs::NativeAllocation>,
}

impl Frame {
    /// Borrows the complete event while its original buffer custody is retained.
    pub fn data(&self) -> &str {
        &self.data
    }
}

/// Incremental data-event decoder. Protocol terminals remain the caller's concern.
#[derive(Default)]
pub struct Decoder {
    buffer: Vec<u8>,
    line: Line,
    line_has_bytes: bool,
    utf8: [u8; 4],
    utf8_len: usize,
    bytes: usize,
    cr: bool,
    started: bool,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    custody: Option<acyclic_fs::NativeAllocation>,
}

#[derive(Clone, Copy)]
enum Line {
    Field(usize),
    Bom(usize),
    Data { leading: bool },
    Ignore,
}

impl Default for Line {
    fn default() -> Self {
        Self::Field(0)
    }
}

impl Decoder {
    fn failure(&self, reason: &'static str) -> Error {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if self.custody.is_some() {
            return Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason,
            };
        }
        Error::Invalid(reason.into())
    }
    /// The body ended inside an event: the connection was cut off, not the stream malformed.
    fn truncated(&self) -> Error {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let native = self.custody.is_some();
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let native = false;
        Error::ModelUnavailable {
            failure: ModelTransportFailure::Interrupted,
            excerpt: if native {
                String::new()
            } else {
                "truncated SSE event".into()
            },
        }
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    /// Retains the supplied original work and admits capacity before allocation.
    pub fn native(work: &acyclic_fs::NativeMaterializationWork) -> Result<Self> {
        Ok(Self {
            custody: Some(work.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)?),
            ..Self::default()
        })
    }

    fn append(&mut self, bytes: &[u8], copy: bool) -> Result<()> {
        let target = self
            .buffer
            .len()
            .checked_add(bytes.len())
            .ok_or_else(|| self.failure("SSE size overflow"))?;
        let old = self.buffer.capacity();
        if target > old {
            let capacity = target.max(old.saturating_mul(2)).max(8);
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let mut allocation = self
                .custody
                .as_ref()
                .map(|owner| {
                    owner.work().claim(
                        capacity as u64,
                        1,
                        acyclic_fs::WorkCounters {
                            bytes_copied: (self.buffer.len() + if copy { bytes.len() } else { 0 })
                                as u64,
                            ..Default::default()
                        },
                    )
                })
                .transpose()?;
            self.buffer
                .try_reserve_exact(capacity - self.buffer.len())
                .map_err(|_| self.failure("SSE buffer allocation failed"))?;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let (Some(owner), Some(allocation)) = (&mut self.custody, &mut allocation) {
                allocation.reconcile_capacity(self.buffer.capacity() as u64)?;
                owner.release(old as u64)?;
                owner.absorb(allocation)?;
            }
        } else if copy && !bytes.is_empty() {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let Some(owner) = &self.custody {
                owner.work().charge(acyclic_fs::WorkCounters {
                    bytes_copied: bytes.len() as u64,
                    ..Default::default()
                })?;
            }
        }
        self.buffer.extend_from_slice(bytes);
        Ok(())
    }

    fn ignored_byte(&mut self, byte: u8) -> Result<()> {
        if self.utf8_len == 0 && byte.is_ascii() {
            return Ok(());
        }
        if let Some(slot) = self.utf8.get_mut(self.utf8_len) {
            *slot = byte;
        } else {
            return Err(self.failure("SSE is not UTF-8"));
        }
        self.utf8_len += 1;
        let bytes = self
            .utf8
            .get(..self.utf8_len)
            .ok_or_else(|| self.failure("SSE is not UTF-8"))?;
        match std::str::from_utf8(bytes) {
            Ok(_) => self.utf8_len = 0,
            Err(error) if error.error_len().is_none() => {}
            Err(_) => return Err(self.failure("SSE is not UTF-8")),
        }
        Ok(())
    }

    fn field_byte(&mut self, byte: u8) -> Result<()> {
        self.line_has_bytes = true;
        if !self.started {
            self.started = true;
            if byte == 0xef {
                self.line = Line::Bom(1);
                return Ok(());
            }
        }
        match self.line {
            Line::Field(index) if b"data:".get(index) == Some(&byte) => {
                self.line = if index == 4 {
                    Line::Data { leading: true }
                } else {
                    Line::Field(index + 1)
                };
            }
            Line::Bom(index) if b"\xef\xbb\xbf".get(index) == Some(&byte) => {
                self.line = if index == 2 {
                    Line::Field(0)
                } else {
                    Line::Bom(index + 1)
                };
            }
            Line::Bom(index) => {
                self.line = Line::Ignore;
                for prefix in b"\xef\xbb\xbf".iter().take(index) {
                    self.ignored_byte(*prefix)?;
                }
                self.ignored_byte(byte)?;
            }
            Line::Field(_) | Line::Ignore => {
                self.line = Line::Ignore;
                self.ignored_byte(byte)?;
            }
            Line::Data { .. } => return Err(self.failure("SSE data bypassed its span decoder")),
        }
        Ok(())
    }

    fn finish_line(&mut self) -> Result<()> {
        if self.utf8_len != 0 || matches!(self.line, Line::Bom(_)) {
            return Err(self.failure("SSE is not UTF-8"));
        }
        if matches!(self.line, Line::Field(4) | Line::Data { .. }) {
            self.append(b"\n", false)?;
        }
        self.line = Line::default();
        self.line_has_bytes = false;
        Ok(())
    }

    fn add_bytes(&mut self, bytes: usize, maximum: usize) -> Result<()> {
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .filter(|bytes| *bytes <= maximum)
            .ok_or_else(|| self.failure("SSE event exceeds configured byte limit"))?;
        Ok(())
    }
}

/// Pulls complete events without reading ahead past the current caller's demand.
/// EOF inside an event refuses; a pending body retains its original producer wait.
pub fn frames<'a, S>(
    body: S,
    maximum: usize,
    decoder: Decoder,
) -> impl Stream<Item = Result<Frame>> + acyclic_stream::ProviderTask + 'a
where
    S: Stream<Item = Result<Bytes>> + acyclic_stream::ProviderTask + Unpin + 'a,
{
    futures::stream::try_unfold(
        (body, Bytes::new(), decoder),
        move |(mut body, mut chunk, mut decoder)| async move {
            loop {
                if chunk.is_empty() {
                    let Some(next) = body.next().await else {
                        if decoder.line_has_bytes || !decoder.buffer.is_empty() {
                            return Err(decoder.truncated());
                        }
                        return Ok(None);
                    };
                    chunk = next?;
                }
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                if let Some(owner) = &decoder.custody {
                    owner.work().remaining()?;
                }
                while chunk.has_remaining() {
                    if let Line::Data { leading } = decoder.line {
                        let length = chunk
                            .iter()
                            .position(|byte| matches!(byte, b'\r' | b'\n'))
                            .unwrap_or(chunk.len());
                        if length != 0 {
                            decoder.add_bytes(length, maximum)?;
                            let data = chunk
                                .get(..length)
                                .ok_or_else(|| decoder.failure("SSE span is outside its source"))?;
                            let data = if leading {
                                data.strip_prefix(b" ").unwrap_or(data)
                            } else {
                                data
                            };
                            decoder.append(data, true)?;
                            decoder.line = Line::Data { leading: false };
                            chunk.advance(length);
                            continue;
                        }
                    }
                    let byte = chunk.get_u8();
                    if byte == b'\n' && decoder.cr {
                        decoder.cr = false;
                        continue;
                    }
                    decoder.cr = byte == b'\r';
                    if byte != b'\n' && byte != b'\r' {
                        decoder.add_bytes(1, maximum)?;
                        decoder.field_byte(byte)?;
                    } else if !decoder.line_has_bytes {
                        decoder.started = true;
                        decoder.bytes = 0;
                        if !decoder.buffer.is_empty() {
                            decoder.buffer.pop();
                            let data = String::from_utf8(std::mem::take(&mut decoder.buffer))
                                .map_err(|_| decoder.failure("SSE is not UTF-8"))?;
                            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                            let custody = decoder
                                .custody
                                .as_mut()
                                .map(|owner| owner.split_off(data.capacity() as u64))
                                .transpose()?;
                            let frame = Frame {
                                data,
                                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                                _custody: custody,
                            };
                            return Ok(Some((frame, (body, chunk, decoder))));
                        }
                    } else {
                        decoder.finish_line()?;
                    }
                }
            }
        },
    )
}

/// Ordinary data-only projection of the same complete event decoder.
pub fn decode<'a>(
    body: BoxProviderStream<'a, Result<Bytes>>,
    maximum: usize,
) -> BoxProviderStream<'a, Result<String>> {
    Box::pin(frames(body, maximum, Decoder::default()).map(|frame| frame.map(|frame| frame.data)))
}

#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use acyclic_fs::{CancellationToken, NativeMaterializationWork};
    use futures::FutureExt as _;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn split_events_transfer_one_funded_buffer_and_retain_source_failure(
            lines in prop::collection::vec("[^\\r\\n]{0,64}", 1..8),
            ignored in "[^\\r\\n]{0,64}",
            width in 1usize..64,
        ) {
            // Generated framing input, independent of any provider's event schema.
            let input = format!("\u{feff}: generated {ignored}\r\n{}\r\n", lines.iter()
                .map(|line| format!("data: {line}\r\n")).collect::<String>());
            let expected = lines.join("\n");
            for width in [1, width, input.len()] {
                let cancel = CancellationToken::new();
                let work = NativeMaterializationWork::new(
                    acyclic_fs::performance::work_budget(65_536, 64 * 1024 * 1024),
                    &cancel,
                ).map_err(|(cause, _)| cause).unwrap();
                let baseline = work.live_allocation_bytes();
                let (producer, receiver) = futures::channel::oneshot::channel::<Result<Bytes>>();
                let chunks = input.as_bytes().chunks(width).map(Bytes::copy_from_slice)
                    .map(Ok).collect::<Vec<_>>();
                let body = futures::stream::iter(chunks).chain(futures::stream::once(async {
                    receiver.await.unwrap_or(Err(Error::Indeterminate(crate::OperationId::from_bytes([7; 16]))))
                }));
                futures::executor::block_on(async {
                    let events = frames(Box::pin(body), input.len(), Decoder::native(&work).unwrap());
                    futures::pin_mut!(events);
                    let frame = events.next().await.unwrap().unwrap();
                    assert_eq!(frame.data(), expected);
                    // The old line+event decoder retains an extra line allocation here.
                    assert_eq!(work.live_allocation_bytes() - baseline, frame.data.capacity() as u64);
                    drop(frame);
                    assert_eq!(work.live_allocation_bytes(), baseline);
                    assert!(events.next().now_or_never().is_none());
                    drop(producer);
                    assert!(matches!(events.next().await, Some(Err(Error::Indeterminate(operation))) if operation == crate::OperationId::from_bytes([7; 16])));
                    assert!(events.next().await.is_none());
                });
                assert_eq!(work.live_allocation_bytes(), baseline);
            }
            let work = NativeMaterializationWork::new(
                acyclic_fs::performance::work_budget(65_536, 64 * 1024 * 1024),
                &CancellationToken::new(),
            ).map_err(|(cause, _)| cause).unwrap();
            let baseline = work.live_allocation_bytes();
            // Malformed generated framing never exposes a partial event.
            for input in [b"data: \xff\n\n".as_slice(), b": \xff\n", b"data: unfinished"] {
                futures::executor::block_on(async {
                    let body = futures::stream::iter([Ok(Bytes::from_static(input))]);
                    let events = frames(Box::pin(body), input.len(), Decoder::native(&work).unwrap());
                    futures::pin_mut!(events);
                    assert!(matches!(events.next().await,
                        Some(Err(Error::NativeBoundary { .. } | Error::ModelUnavailable { failure: ModelTransportFailure::Interrupted, .. }))));
                    assert!(events.next().await.is_none());
                });
                assert_eq!(work.live_allocation_bytes(), baseline);
            }
        }
    }
}
