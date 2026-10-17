use crate::invalid;
use acyclic_harness::Result;
use acyclic_stream::BoxProviderStream;
use bytes::Bytes;
use futures::{Stream, StreamExt as _};

pub(crate) struct Frame {
    pub(crate) data: String,
    #[cfg(not(target_arch = "wasm32"))]
    _custody: Option<acyclic_fs::NativeAllocation>,
}

#[derive(Default)]
pub(crate) struct Decoder {
    line: Vec<u8>,
    data: String,
    bytes: usize,
    cr: bool,
    #[cfg(not(target_arch = "wasm32"))]
    custody: Option<acyclic_fs::NativeAllocation>,
}

impl Decoder {
    fn failure(&self, storage: bool, reason: &'static str) -> acyclic_harness::Error {
        #[cfg(not(target_arch = "wasm32"))]
        if self.custody.is_some() {
            return acyclic_harness::Error::NativeBoundary {
                code: if storage {
                    acyclic_harness::wire::ErrorCode::Storage
                } else {
                    acyclic_harness::wire::ErrorCode::Invalid
                },
                reason,
            };
        }
        if storage {
            acyclic_harness::Error::Storage(reason.into())
        } else {
            invalid(reason)
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn native(work: &acyclic_fs::NativeMaterializationWork) -> Result<Self> {
        Ok(Self {
            custody: Some(work.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)?),
            ..Self::default()
        })
    }

    fn line_byte(&mut self, byte: u8) -> Result<()> {
        let target = self.line.len() + 1;
        let old = self.line.capacity();
        if target > old {
            let capacity = target.max(old.saturating_mul(2)).max(8);
            #[cfg(not(target_arch = "wasm32"))]
            let mut allocation = self
                .custody
                .as_ref()
                .map(|owner| {
                    owner.work().claim(
                        capacity as u64,
                        1,
                        acyclic_fs::WorkCounters {
                            bytes_copied: self.line.len() as u64,
                            ..Default::default()
                        },
                    )
                })
                .transpose()?;
            self.line
                .try_reserve_exact(capacity - self.line.len())
                .map_err(|_| self.failure(false, "SSE buffer allocation failed"))?;
            #[cfg(not(target_arch = "wasm32"))]
            if let (Some(owner), Some(allocation)) = (&mut self.custody, &mut allocation) {
                allocation.reconcile_capacity(self.line.capacity() as u64)?;
                owner.release(old as u64)?;
                owner.absorb(allocation)?;
            }
        }
        self.line.push(byte);
        Ok(())
    }

    fn finish_line(&mut self) -> Result<()> {
        let text =
            std::str::from_utf8(&self.line).map_err(|_| self.failure(false, "SSE is not UTF-8"))?;
        if let Some(value) = text.strip_prefix("data:") {
            let value = value.strip_prefix(' ').unwrap_or(value);
            let target = self
                .data
                .len()
                .checked_add(value.len())
                .and_then(|n| n.checked_add(1))
                .ok_or_else(|| self.failure(false, "SSE size overflow"))?;
            let old = self.data.capacity();
            if target > old {
                let capacity = target.max(old.saturating_mul(2)).max(8);
                #[cfg(not(target_arch = "wasm32"))]
                let mut allocation = self
                    .custody
                    .as_ref()
                    .map(|owner| {
                        owner.work().claim(
                            capacity as u64,
                            1,
                            acyclic_fs::WorkCounters {
                                bytes_copied: self.data.len() as u64,
                                ..Default::default()
                            },
                        )
                    })
                    .transpose()?;
                self.data
                    .try_reserve_exact(capacity - self.data.len())
                    .map_err(|_| self.failure(false, "SSE buffer allocation failed"))?;
                #[cfg(not(target_arch = "wasm32"))]
                if let (Some(owner), Some(allocation)) = (&mut self.custody, &mut allocation) {
                    allocation.reconcile_capacity(self.data.capacity() as u64)?;
                    owner.release(old as u64)?;
                    owner.absorb(allocation)?;
                }
            }
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(owner) = &self.custody {
                owner.work().charge(acyclic_fs::WorkCounters {
                    bytes_copied: value.len() as u64,
                    ..Default::default()
                })?;
            }
            self.data.push_str(value);
            self.data.push('\n');
        }
        self.line.clear();
        Ok(())
    }
}

pub(crate) fn frames<'a>(
    mut body: BoxProviderStream<'a, Result<Bytes>>,
    maximum: usize,
    mut decoder: Decoder,
) -> impl Stream<Item = Result<Frame>> + acyclic_stream::ProviderTask + 'a {
    async_stream::try_stream! {
        while let Some(chunk) = body.next().await {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(owner) = &decoder.custody { owner.work().remaining()?; }
            for byte in chunk? {
                if byte == b'\n' && decoder.cr { decoder.cr = false; continue; }
                decoder.cr = byte == b'\r';
                if byte != b'\n' && byte != b'\r' {
                    decoder.bytes = decoder.bytes.checked_add(1).ok_or_else(|| decoder.failure(false, "SSE size overflow"))?;
                    if decoder.bytes > maximum { Err(decoder.failure(false, "SSE event exceeds configured byte limit"))?; }
                    decoder.line_byte(byte)?;
                } else if decoder.line.is_empty() {
                    decoder.bytes = 0;
                    if !decoder.data.is_empty() {
                        decoder.data.pop();
                        #[cfg(not(target_arch = "wasm32"))]
                        let custody = decoder.custody.as_mut().map(|owner| owner.split_off(decoder.data.capacity() as u64)).transpose()?;
                        yield Frame { data: std::mem::take(&mut decoder.data), #[cfg(not(target_arch = "wasm32"))] _custody: custody };
                    }
                } else {
                    decoder.finish_line()?;
                }
            }
        }
        if !decoder.line.is_empty() || !decoder.data.is_empty() {
            Err(decoder.failure(true, "truncated SSE event"))?;
        }
    }
}

pub(crate) fn decode<'a>(
    body: BoxProviderStream<'a, Result<Bytes>>,
    maximum: usize,
) -> BoxProviderStream<'a, Result<String>> {
    Box::pin(frames(body, maximum, Decoder::default()).map(|frame| frame.map(|frame| frame.data)))
}
