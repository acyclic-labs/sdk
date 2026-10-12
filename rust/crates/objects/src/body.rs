//! Private immutable body storage for the logical Objects recovery engine.
use futures::future::BoxFuture;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::v1::{Error, wire::ErrorCode::Unavailable};

#[derive(Clone)]
pub(crate) enum BodyReadMode {
    Ordinary,
    #[cfg(not(target_arch = "wasm32"))]
    Native(Arc<dyn acyclic_native_runtime::NativeReadMeter>),
}
#[cfg(not(target_arch = "wasm32"))]
pub(crate) type BodyReadError = crate::v1::NativeObjectsError;
#[cfg(target_arch = "wasm32")]
pub(crate) type BodyReadError = Error;
enum BodyReadBuffer {
    Ordinary(Vec<u8>),
    #[cfg(not(target_arch = "wasm32"))]
    Native {
        storage: acyclic_stream::PreparedOwnedBytes<acyclic_native_runtime::ReadAllocation>,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    },
}
impl BodyReadBuffer {
    fn new(length: usize, mode: &BodyReadMode) -> Result<Self, BodyReadError> {
        match mode {
            BodyReadMode::Ordinary => Ok(Self::Ordinary(Vec::with_capacity(length))),
            #[cfg(not(target_arch = "wasm32"))]
            BodyReadMode::Native(meter) => Ok(Self::Native {
                storage: prepare_native_bytes(length, meter)?,
                meter: Arc::clone(meter),
            }),
        }
    }
    fn append(&mut self, written: usize, bytes: &[u8]) -> Result<(), BodyReadError> {
        match self {
            Self::Ordinary(output) => output.extend_from_slice(bytes),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { storage, meter } => {
                let end = written
                    .checked_add(bytes.len())
                    .ok_or(Error::from(Unavailable))?;
                let target = storage
                    .writable()
                    .get_mut(written..end)
                    .ok_or(Error::from(Unavailable))?;
                meter
                    .admit_codec(0, bytes.len() as u64, 0)
                    .map_err(crate::v1::NativeObjectsError::from)?;
                target.copy_from_slice(bytes);
                meter
                    .record_codec(0, bytes.len() as u64, 0)
                    .map_err(crate::v1::NativeObjectsError::from)?;
            }
        }
        Ok(())
    }
    fn freeze(self, written: usize) -> Result<acyclic_stream::OwnedBytes, BodyReadError> {
        match self {
            Self::Ordinary(output) => Ok(output.into()),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { storage, .. } => storage
                .freeze(written)
                .map_err(|_| Error::from(Unavailable).into()),
        }
    }
}

fn box_body_read<'a, F>(
    future: F,
    mode: &BodyReadMode,
) -> Result<
    crate::v1::Guarded<BoxFuture<'a, Result<acyclic_stream::OwnedBytes, BodyReadError>>>,
    BodyReadError,
>
where
    F: Future<Output = Result<acyclic_stream::OwnedBytes, BodyReadError>> + Send + 'a,
{
    #[cfg(not(target_arch = "wasm32"))]
    let allocation = match mode {
        BodyReadMode::Ordinary => None,
        BodyReadMode::Native(meter) => Some(
            acyclic_native_runtime::ReadAllocation::new(meter, std::mem::size_of::<F>())
                .map_err(crate::v1::NativeObjectsError::from)?,
        ),
    };
    #[cfg(target_arch = "wasm32")]
    let allocation = {
        let _ = mode;
        ()
    };
    Ok(crate::v1::Guarded {
        value: Box::pin(future),
        _allocation: allocation,
    })
}

/// Equality is structural: the same bytes or the same physical references.
#[derive(Clone, PartialEq)]
pub(crate) enum StoredBody {
    Memory(acyclic_stream::OwnedBytes),
    Composite {
        parts: Arc<crate::v1::Guarded<Vec<StoredBody>>>,
        length: usize,
    },
    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    Local {
        root: Arc<crate::physical::LocalRoot>,
        digest: [u8; 32],
        length: usize,
        location: LocalBodyLocation,
    },
}

impl StoredBody {
    pub(crate) fn memory(body: acyclic_stream::OwnedBytes) -> Self {
        // Establish ordinary byte controls before native reads.
        drop(body.clone());
        Self::Memory(body)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn composite_measured(
        parts: Vec<StoredBody>,
        length: usize,
        meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        allocation: acyclic_native_runtime::ReadAllocation,
    ) -> Result<Self, crate::v1::NativeObjectsError> {
        let bytes = parts
            .capacity()
            .checked_mul(std::mem::size_of::<StoredBody>());
        if bytes.and_then(|bytes| u64::try_from(bytes).ok()) != Some(allocation.bytes()) {
            drop(parts);
            drop(allocation);
            return Err(std::io::Error::from(std::io::ErrorKind::InvalidData).into());
        }
        let mut data = crate::v1::Guarded {
            value: parts,
            _allocation: Some(allocation),
        };
        let mut metadata = acyclic_native_runtime::ReadAllocation::new(
            meter,
            std::mem::size_of::<crate::v1::Guarded<Vec<StoredBody>>>(),
        )?;
        data._allocation
            .as_mut()
            .expect("native parts retain their original allocation")
            .absorb(&mut metadata)?;
        Ok(Self::Composite {
            parts: Arc::new(data),
            length,
        })
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Memory(body) => body.len(),
            Self::Composite { length, .. } => *length,
            #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
            Self::Local { length, .. } => *length,
        }
    }

    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    pub(crate) fn local_references(&self, output: &mut BTreeSet<LocalBodyReference>) {
        match self {
            Self::Memory(_) => {}
            Self::Composite { parts, .. } => {
                for part in parts.value.iter() {
                    part.local_references(output);
                }
            }
            Self::Local {
                digest,
                length,
                location,
                ..
            } => {
                output.insert(LocalBodyReference {
                    digest: *digest,
                    length: *length,
                    location: location.clone(),
                });
            }
        }
    }

    pub(crate) fn read(
        &self,
        start: usize,
        end: usize,
        mode: BodyReadMode,
    ) -> impl Future<Output = Result<acyclic_stream::OwnedBytes, BodyReadError>> + Send {
        let span = crate::obs::span!(
            TRACE,
            "acyclic.objects.body.read",
            backend = self.backend(),
            bytes = end.saturating_sub(start),
        );
        crate::obs::traced(span, self.read_inner_async(start, end, mode))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn backend(&self) -> &'static str {
        match self {
            Self::Memory(_) => "memory",
            Self::Composite { .. } => "composite",
            #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
            Self::Local { location, .. } => match location {
                LocalBodyLocation::Segment { .. } => "segment",
                LocalBodyLocation::Journal { .. } => "journal",
            },
        }
    }

    // Composite recursion uses the outer read span.
    fn read_inner_async(
        &self,
        start: usize,
        end: usize,
        mode: BodyReadMode,
    ) -> impl Future<Output = Result<acyclic_stream::OwnedBytes, BodyReadError>> + Send {
        async move {
            #[cfg(not(target_arch = "wasm32"))]
            if let BodyReadMode::Native(meter) = &mode {
                meter.check().map_err(crate::v1::NativeObjectsError::from)?;
            }
            if start > end || end > self.len() {
                return Err(Error::from(Unavailable).into());
            }
            match self {
                Self::Memory(body) => Ok(body.slice(start..end)),
                Self::Composite { parts, .. } => {
                    let mut offset = 0usize;
                    for part in parts.value.iter() {
                        let part_end = offset
                            .checked_add(part.len())
                            .ok_or(Error::from(Unavailable))?;
                        if start >= offset && end <= part_end {
                            return box_body_read(
                                part.read_inner_async(start - offset, end - offset, mode.clone()),
                                &mode,
                            )?
                            .await;
                        }
                        offset = part_end;
                    }
                    let mut output = BodyReadBuffer::new(end - start, &mode)?;
                    let mut written = 0usize;
                    let mut offset = 0usize;
                    for part in parts.value.iter() {
                        let part_end = offset
                            .checked_add(part.len())
                            .ok_or(Error::from(Unavailable))?;
                        if part_end > start && offset < end {
                            let bytes = box_body_read(
                                part.read_inner_async(
                                    start.saturating_sub(offset).min(part.len()),
                                    end.saturating_sub(offset).min(part.len()),
                                    mode.clone(),
                                ),
                                &mode,
                            )?
                            .await?;
                            output.append(written, bytes.as_slice())?;
                            written = written
                                .checked_add(bytes.len())
                                .ok_or(Error::from(Unavailable))?;
                        }
                        offset = part_end;
                    }
                    if written != end - start {
                        return Err(Error::from(Unavailable).into());
                    }
                    output.freeze(written)
                }
                #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
                Self::Local {
                    root,
                    digest,
                    length,
                    location,
                } => {
                    crate::physical::read_body_at(root, digest, *length, location, start, end, mode)
                        .await
                }
            }
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn prepare_native_bytes(
    length: usize,
    meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
) -> Result<
    acyclic_stream::PreparedOwnedBytes<acyclic_native_runtime::ReadAllocation>,
    crate::v1::NativeObjectsError,
> {
    use acyclic_native_runtime::ReadAllocation;
    use acyclic_stream::PreparedOwnedBytes;
    let mut allocation = ReadAllocation::new(meter, length)?;
    let mut data = Vec::new();
    data.try_reserve_exact(length)
        .map_err(|error| meter.allocation_failed(error))?;
    allocation.reconcile_capacity(data.capacity())?;
    data.resize(data.capacity(), 0);
    PreparedOwnedBytes::new(data, allocation, Arc::clone(meter))
        .map_err(|failure| crate::v1::native_preparation_error(failure.into_error()))
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum LocalBodyLocation {
    /// A body record inside one immutable, content-addressed segment file.
    Segment { id: [u8; 32], offset: u64 },
    /// Bytes carried by the journal frame that committed the body.
    Journal { offset: u64 },
}

/// Physical moves of local bodies, keyed by current location and digest: an empty body shares its journal offset with the inline body that follows it.
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub(crate) type LocalBodyRelocations = BTreeMap<(LocalBodyLocation, [u8; 32]), LocalBodyLocation>;

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct LocalBodyReference {
    pub(crate) digest: [u8; 32],
    pub(crate) length: usize,
    pub(crate) location: LocalBodyLocation,
}

#[cfg(all(test, not(target_arch = "wasm32")))]
pub(crate) mod tests {
    use super::*;
    #[test]
    fn resident_ranges_share_original_known_capacity_even_when_empty() {
        let mut bytes = Vec::with_capacity(128);
        bytes.extend_from_slice(b"abcdef");
        let capacity = bytes.capacity();
        let body = StoredBody::memory(bytes.into());
        let full = futures::executor::block_on(body.read(0, 6, BodyReadMode::Ordinary))
            .unwrap_or_else(|_| unreachable!());
        let selected = futures::executor::block_on(body.read(2, 4, BodyReadMode::Ordinary))
            .unwrap_or_else(|_| unreachable!());
        let empty = futures::executor::block_on(body.read(3, 3, BodyReadMode::Ordinary))
            .unwrap_or_else(|_| unreachable!());
        assert_eq!(full.retained_capacity(), Some(capacity));
        assert_eq!(selected.retained_capacity(), Some(capacity));
        assert_eq!(empty.retained_capacity(), Some(capacity));
        assert_eq!(selected.as_slice().as_ptr(), full.as_slice()[2..].as_ptr());
        drop(body);
        drop(full);
        assert_eq!(selected.as_slice(), b"cd");
        assert!(empty.is_empty());
    }
}
