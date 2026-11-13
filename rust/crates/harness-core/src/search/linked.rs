//! Original linked references retain their decoder's allocation owner.
use super::*;
use crate::filesystem::PreparationIo;
use acyclic_fs::kernel::{KernelAccounting, MetadataDecodeError, Retained};

/// Match record bytes and original immutable file references through one reader.
pub struct ContentMatcher<'a> {
    reader: &'a dyn ContentResidencyVerifier,
    ceiling: Option<&'a Capabilities>,
    io: Option<PreparationIo>,
}

struct Position {
    file: usize,
    offset: u64,
}

struct Inspection {
    step: Step<Retained<FileRef>>,
    #[cfg(not(target_arch = "wasm32"))]
    allocation: Option<acyclic_native_runtime::ReadAllocation>,
}

impl Inspection {
    fn done(source: Option<Retained<FileRef>>, matched: bool) -> Self {
        Self {
            step: Step {
                matched,
                source,
                next: None,
            },
            #[cfg(not(target_arch = "wasm32"))]
            allocation: None,
        }
    }
}

fn invalid(io: Option<&PreparationIo>, message: &'static str) -> Error {
    crate::executor::stock_boundary(io, crate::wire::ErrorCode::Invalid, message)
}

fn decode_error<E>(io: Option<&PreparationIo>, error: MetadataDecodeError<E>) -> Error {
    match error {
        MetadataDecodeError::Work(error) => error.into(),
        MetadataDecodeError::Codec(_) => invalid(io, "search source record is malformed"),
    }
}

struct Reference {
    file: Option<Retained<FileRef>>,
    more: bool,
}

fn reference(
    record: &Record,
    selected: usize,
    io: Option<&PreparationIo>,
    ctx: &mut KernelAccounting<'_>,
) -> Result<Reference> {
    let value = if record
        .value
        .as_slice()
        .starts_with(&crate::agent::RECORD_VERSION.to_be_bytes())
    {
        crate::agent::decode_record_accounted::<serde_json::Value>(&record.value, ctx)
            .map_err(|error| decode_error(io, error))?
    } else {
        match ctx.decode_json::<serde_json::Value>(&record.value) {
            Ok(value) => value,
            Err(MetadataDecodeError::Codec(_)) => {
                return Ok(Reference {
                    file: None,
                    more: false,
                });
            }
            Err(error) => return Err(decode_error(io, error)),
        }
    };
    let mut pending = Retained::allocate(1, ctx)?;
    pending.push(&*value, ctx)?;
    let mut file = None;
    let mut count = 0;
    while let Some(value) = pending.pop(ctx)? {
        match value {
            serde_json::Value::Object(fields) => {
                // This required field is only a candidate filter; the canonical
                // reference decoder still validates the complete object.
                if fields.contains_key("descriptor") {
                    match ctx.deserialize::<FileRef, _>(value) {
                        Ok(found) => {
                            if count > selected {
                                return Ok(Reference { file, more: true });
                            }
                            if count == selected {
                                file = Some(found);
                            }
                            count += 1;
                            continue;
                        }
                        Err(MetadataDecodeError::Codec(_)) => {}
                        Err(error) => return Err(decode_error(io, error)),
                    }
                }
                for child in fields.values().rev() {
                    pending.push(child, ctx)?;
                }
            }
            serde_json::Value::Array(values) => {
                for child in values.iter().rev() {
                    pending.push(child, ctx)?;
                }
            }
            _ => {}
        }
    }
    Ok(Reference { file, more: false })
}

fn position(after: Option<&[u8]>, io: Option<&PreparationIo>) -> Result<Position> {
    let Some(after) = after else {
        return Ok(Position { file: 0, offset: 0 });
    };
    if after.len() != 17 || after[0] != 1 {
        return Err(invalid(io, "invalid content search continuation"));
    }
    let file = u64::from_be_bytes(
        after[1..9]
            .try_into()
            .map_err(|_| invalid(io, "invalid content search continuation"))?,
    );
    let offset = u64::from_be_bytes(
        after[9..]
            .try_into()
            .map_err(|_| invalid(io, "invalid content search continuation"))?,
    );
    Ok(Position {
        file: usize::try_from(file)
            .map_err(|_| invalid(io, "content search position exceeds this platform"))?,
        offset,
    })
}

fn progress(position: Position, more: bool, io: Option<&PreparationIo>) -> Result<Inspection> {
    if !more {
        return Ok(Inspection::done(None, false));
    }
    let mut frame = [0; 17];
    frame[0] = 1;
    frame[1..9].copy_from_slice(&(position.file as u64).to_be_bytes());
    frame[9..].copy_from_slice(&position.offset.to_be_bytes());
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(io) = io {
        let work = io
            .original_native_work()
            .ok_or_else(|| invalid(Some(io), "native search has no original work"))?;
        let meter = work.runtime_meter();
        let result = (|| {
            let mut allocation = acyclic_native_runtime::ReadAllocation::new(&meter, frame.len())?;
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(frame.len())
                .map_err(|error| meter.allocation_failed(error))?;
            allocation.reconcile_capacity(bytes.capacity())?;
            meter.admit_codec(0, frame.len() as u64, 0)?;
            bytes.extend_from_slice(&frame);
            meter.record_codec(0, frame.len() as u64, 0)?;
            Ok::<_, std::io::Error>((bytes, allocation))
        })()
        .map_err(|error| Error::from(work.runtime_error(error)))?;
        return Ok(Inspection {
            step: Step {
                matched: false,
                source: None,
                next: Some(result.0),
            },
            allocation: Some(result.1),
        });
    }
    let _ = io;
    Ok(Inspection {
        step: Step {
            matched: false,
            source: None,
            next: Some(frame.to_vec()),
        },
        #[cfg(not(target_arch = "wasm32"))]
        allocation: None,
    })
}

impl<'a> ContentMatcher<'a> {
    /// Retain the existing reader and current capability ceiling.
    pub fn new(
        reader: &'a dyn ContentResidencyVerifier,
        ceiling: Option<&'a Capabilities>,
    ) -> Self {
        Self {
            reader,
            ceiling,
            io: None,
        }
    }

    /// Bind the exact original invocation; no replacement ledger is constructed.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn with_native_work(
        reader: &'a dyn ContentResidencyVerifier,
        ceiling: Option<&'a Capabilities>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        Ok(Self {
            reader,
            ceiling,
            io: Some(PreparationIo::from_native_work(work)?),
        })
    }

    async fn inspect(
        &self,
        record: &Record,
        pattern: &[u8],
        after: Option<&[u8]>,
        io: Option<&PreparationIo>,
    ) -> Result<Inspection> {
        if let Some(io) = io {
            io.charge(acyclic_fs::WorkCounters {
                items_examined: record.value.len() as u64,
                ..Default::default()
            })?;
        }
        if after.is_none() && contains(record.value.as_slice(), pattern) {
            return Ok(Inspection::done(None, true));
        }
        let position = position(after, io)?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let standalone = || KernelAccounting::Standalone {
            allocations: Default::default(),
            work: acyclic_fs::WorkCounters::UNCHARGED,
            budget: acyclic_fs::WorkBudget::UNBOUNDED,
            cancellation: &cancellation,
        };
        #[cfg(not(target_arch = "wasm32"))]
        let mut ctx = match io.and_then(PreparationIo::original_native_work) {
            Some(work) => KernelAccounting::NativeOriginal {
                work,
                cancellation: work.cancellation(),
            },
            None => standalone(),
        };
        #[cfg(target_arch = "wasm32")]
        let mut ctx = standalone();
        let selected = reference(record, position.file, io, &mut ctx)?;
        let Some(file) = selected.file else {
            if after.is_some() {
                return Err(invalid(
                    io,
                    "content search continuation differs from record",
                ));
            }
            return Ok(Inspection::done(None, false));
        };
        let length = file.descriptor().byte_length();
        if position.offset > length {
            return Err(invalid(io, "content search continuation exceeds source"));
        }
        let next_file = Position {
            file: position
                .file
                .checked_add(1)
                .ok_or_else(|| invalid(io, "content reference count overflow"))?,
            offset: 0,
        };
        if position.offset == length {
            return progress(next_file, selected.more, io);
        }
        let mut range = source_window(position.offset, pattern);
        range.length = range.length.min(length - range.offset);
        let bytes = match self
            .reader
            .read_range(&file, range, self.ceiling, io)?
            .await
        {
            Ok(bytes) => bytes,
            Err(Error::Unauthorized(_)) => return progress(next_file, selected.more, io),
            Err(error) => return Err(error),
        };
        if let Some(io) = io {
            io.charge(acyclic_fs::WorkCounters {
                items_examined: bytes.len() as u64,
                ..Default::default()
            })?;
        }
        match inspect_window(length, position.offset, pattern, bytes.as_slice(), io)? {
            Window::Match(_) => Ok(Inspection::done(Some(file), true)),
            Window::More(offset) => progress(
                Position {
                    file: position.file,
                    offset,
                },
                true,
                io,
            ),
            Window::Done => progress(next_file, selected.more, io),
        }
    }
}

impl Matcher for ContentMatcher<'_> {
    type Source = Retained<FileRef>;
    type Error = Error;
    fn identity(&self) -> [u8; 32] {
        *b"acyclic:search:linked-content:v2"
    }
    fn step<'a>(
        &'a self,
        record: &'a Record,
        pattern: &'a [u8],
        after: Option<&'a [u8]>,
    ) -> impl std::future::Future<Output = Result<Step<Self::Source>>> + acyclic_stream::ProviderTask + 'a
    {
        async move {
            if self.io.is_some() {
                return Err(invalid(
                    self.io.as_ref(),
                    "native matcher requires native search",
                ));
            }
            self.inspect(record, pattern, after, None)
                .await
                .map(|inspection| inspection.step)
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_step<'a>(
        &'a self,
        record: &'a Record,
        pattern: &'a [u8],
        after: Option<&'a [u8]>,
        meter: &'a std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> acyclic_stream::NativeProviderFuture<'a, Step<Self::Source>, Error> {
        acyclic_stream::NativeProviderFuture::new(meter, async move {
            let result = async {
                let io = self.io.as_ref().ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unsupported,
                    reason: "linked matcher has no native work",
                })?;
                let work = io
                    .original_native_work()
                    .ok_or_else(|| invalid(Some(io), "linked matcher has no native work"))?;
                if !std::sync::Arc::ptr_eq(meter, &work.runtime_meter()) {
                    return Err(invalid(Some(io), "linked matcher native owner differs"));
                }
                let inspection = self.inspect(record, pattern, after, Some(io)).await?;
                Ok(acyclic_stream::NativeStreamOutput::new(
                    inspection.step,
                    inspection
                        .allocation
                        .unwrap_or_else(|| acyclic_native_runtime::ReadAllocation::empty(meter)),
                ))
            }
            .await;
            Ok(result)
        })
    }
}
