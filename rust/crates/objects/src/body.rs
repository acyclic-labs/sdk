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
        storage: NativeStorage,
        header: acyclic_native_runtime::ReadAllocationReservation,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    },
}
impl BodyReadBuffer {
    fn new(length: usize, mode: &BodyReadMode) -> Result<Self, BodyReadError> {
        match mode {
            BodyReadMode::Ordinary => Ok(Self::Ordinary(Vec::with_capacity(length))),
            #[cfg(not(target_arch = "wasm32"))]
            BodyReadMode::Native(meter) => {
                let (storage, header) = native_body_storage(length, meter)?;
                Ok(Self::Native {
                    storage,
                    header,
                    meter: Arc::clone(meter),
                })
            }
        }
    }
    fn append(&mut self, written: usize, bytes: &[u8]) -> Result<(), BodyReadError> {
        let _ = written;
        match self {
            Self::Ordinary(output) => output.extend_from_slice(bytes),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { storage, meter, .. } => {
                // Append within the admitted capacity: never reallocate or initialize ahead.
                let data = &mut storage.0;
                if written != data.len() || bytes.len() > data.capacity() - data.len() {
                    Err(Error::from(Unavailable))?;
                }
                meter
                    .admit_codec(0, bytes.len() as u64, 0)
                    .map_err(crate::v1::NativeObjectsError::from)?;
                data.extend_from_slice(bytes);
                meter
                    .record_codec(0, bytes.len() as u64, 0)
                    .map_err(crate::v1::NativeObjectsError::from)?;
            }
        }
        Ok(())
    }
    fn freeze(self, written: usize) -> Result<acyclic_stream::OwnedBytes, BodyReadError> {
        let _ = written;
        match self {
            Self::Ordinary(output) => Ok(output.into()),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native {
                storage, header, ..
            } => {
                if storage.0.len() != written {
                    Err(Error::from(Unavailable))?;
                }
                publish_native_storage(storage, header)
            }
        }
    }
}

fn box_body_read<'a, F>(
    mode: BodyReadMode,
    future: impl FnOnce(BodyReadMode) -> F,
) -> Result<
    crate::v1::Guarded<BoxFuture<'a, Result<acyclic_stream::OwnedBytes, BodyReadError>>>,
    BodyReadError,
>
where
    F: Future<Output = Result<acyclic_stream::OwnedBytes, BodyReadError>> + Send + 'a,
{
    #[cfg(not(target_arch = "wasm32"))]
    let allocation = match &mode {
        BodyReadMode::Ordinary => None,
        BodyReadMode::Native(meter) => Some(
            acyclic_native_runtime::ReadAllocation::new(meter, std::mem::size_of::<F>())
                .map_err(crate::v1::NativeObjectsError::from)?,
        ),
    };
    #[cfg(target_arch = "wasm32")]
    let allocation = None;
    Ok(crate::v1::Guarded {
        value: Box::pin(future(mode)),
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

    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
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
            .ok_or(std::io::Error::from(std::io::ErrorKind::InvalidData))?
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
        if let Self::Memory(body) = self {
            return futures::future::Either::Left(async move {
                #[cfg(not(target_arch = "wasm32"))]
                if let BodyReadMode::Native(meter) = &mode {
                    meter.check().map_err(crate::v1::NativeObjectsError::from)?;
                }
                if start > end || end > body.len() {
                    Err(Error::from(Unavailable))?;
                }
                Ok(body.slice(start..end))
            });
        }
        let read = box_body_read(mode, |mode| self.read_nonresident(start, end, mode));
        futures::future::Either::Right(async move { read?.await })
    }

    // Keep disk I/O state out of every resident read and its callers' frames.
    async fn read_nonresident(
        &self,
        start: usize,
        end: usize,
        mode: BodyReadMode,
    ) -> Result<acyclic_stream::OwnedBytes, BodyReadError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let BodyReadMode::Native(meter) = &mode {
            meter.check().map_err(crate::v1::NativeObjectsError::from)?;
        }
        if start > end || end > self.len() {
            Err(Error::from(Unavailable))?;
        }
        match self {
            Self::Memory(_) => unreachable!("memory reads use the direct path"),
            Self::Composite { parts, .. } => {
                let mut offset = 0usize;
                for part in parts.value.iter() {
                    let part_end = offset
                        .checked_add(part.len())
                        .ok_or(Error::from(Unavailable))?;
                    if start >= offset && end <= part_end {
                        return part
                            .read_inner_async(start - offset, end - offset, mode.clone())
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
                        let bytes = part
                            .read_inner_async(
                                start.saturating_sub(offset).min(part.len()),
                                end.saturating_sub(offset).min(part.len()),
                                mode.clone(),
                            )
                            .await?;
                        output.append(written, bytes.as_slice())?;
                        written = written
                            .checked_add(bytes.len())
                            .ok_or(Error::from(Unavailable))?;
                    }
                    offset = part_end;
                }
                if written != end - start {
                    Err(Error::from(Unavailable))?;
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

/// Admitted, uninitialized read capacity and the allocation that accounts for it.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) type NativeStorage = (Vec<u8>, acyclic_native_runtime::ReadAllocation);

/// The one constructor of native read storage: capacity is admitted and
/// reserved but never initialized, so the kernel writes each byte once.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn native_storage(
    length: usize,
    meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
) -> Result<NativeStorage, crate::v1::NativeObjectsError> {
    let mut allocation = acyclic_native_runtime::ReadAllocation::new(meter, length)?;
    let mut data = Vec::new();
    data.try_reserve_exact(length)
        .map_err(|error| meter.allocation_failed(error))?;
    allocation.reconcile_capacity(data.capacity())?;
    #[cfg(test)]
    work::storage(&mut data);
    Ok((data, allocation))
}

/// Test observation of native read work, recorded on the calling thread for the differential harness.
#[cfg(all(test, not(target_arch = "wasm32")))]
pub(crate) mod work {
    use std::cell::Cell;

    thread_local! {
        static RESTORE_OLD_WORK: Cell<bool> = const { Cell::new(false) };
    }

    /// Runs `run` with the removed work restored: the negative control.
    pub(crate) fn restoring_old_work<T>(run: impl FnOnce() -> T) -> T {
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                RESTORE_OLD_WORK.set(false);
            }
        }
        RESTORE_OLD_WORK.set(true);
        let _reset = Reset;
        run()
    }

    pub(crate) fn restores_old_work() -> bool {
        RESTORE_OLD_WORK.get()
    }

    pub(crate) fn record(operation: &'static str, offset: u64, length: usize) {
        acyclic_test_support::differential::record((operation, offset, length as u64));
    }

    /// Records new storage and the bytes initialized before any read fills it.
    pub(crate) fn storage(data: &mut Vec<u8>) {
        let initialized = if restores_old_work() {
            data.resize(data.capacity(), 0);
            let initialized = data.len();
            data.clear();
            initialized
        } else {
            0
        };
        record("storage", initialized as u64, data.capacity());
    }
}

/// Storage for bytes that will be published, with their ownership header reserved before I/O.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn native_body_storage(
    length: usize,
    meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
) -> Result<
    (
        NativeStorage,
        acyclic_native_runtime::ReadAllocationReservation,
    ),
    crate::v1::NativeObjectsError,
> {
    let storage = native_storage(length, meter)?;
    let header = acyclic_stream::OwnedBytes::reserve_vec_guard::<
        acyclic_native_runtime::ReadAllocation,
    >(meter)?;
    Ok((storage, header))
}

/// Publishes the initialized prefix of native storage without copying it.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn publish_native_storage(
    storage: NativeStorage,
    header: acyclic_native_runtime::ReadAllocationReservation,
) -> Result<acyclic_stream::OwnedBytes, crate::v1::NativeObjectsError> {
    acyclic_stream::OwnedBytes::from_vec_with_reserved_guard(storage, header)
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
    use proptest::prelude::*;

    fn compact_read(frame: usize) -> bool {
        frame <= 512
    }

    #[test]
    fn resident_reads_do_not_carry_disk_future_storage() {
        let body = StoredBody::memory(vec![1, 2, 3].into());
        let fast = body.read_inner_async(0, 3, BodyReadMode::Ordinary);
        // The unchanged disk/composite implementation is the inline negative control.
        let inline = body.read_nonresident(0, 3, BodyReadMode::Ordinary);
        let fast = std::mem::size_of_val(&fast);
        let inline = std::mem::size_of_val(&inline);
        eprintln!("body read frames: resident={fast}, inline-disk={inline}");
        assert!(
            compact_read(fast),
            "resident frame retains disk state: {fast}"
        );
        assert!(
            !compact_read(inline),
            "inline control no longer exceeds the bound: {inline}"
        );
    }

    proptest! {
        // Frame separation preserves every selected byte and both range rejections,
        // including reads crossing or lying within nested multipart boundaries.
        #[test]
        fn resident_and_nested_composite_ranges_equal_the_original_bytes(
            parts in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..24), 1..8),
            a in any::<usize>(), b in any::<usize>(),
        ) {
            let bytes: Vec<_> = parts.iter().flatten().copied().collect();
            let resident = StoredBody::memory(bytes.clone().into());
            let mut composite = StoredBody::Composite {
                parts: Arc::new(crate::v1::Guarded {
                    value: parts.into_iter().map(|part| StoredBody::memory(part.into())).collect(),
                    _allocation: None,
                }),
                length: bytes.len(),
            };
            composite = StoredBody::Composite {
                parts: Arc::new(crate::v1::Guarded {
                    value: vec![composite],
                    _allocation: None,
                }),
                length: bytes.len(),
            };
            let (a, b) = (a % (bytes.len() + 1), b % (bytes.len() + 1));
            let (start, end) = (a.min(b), a.max(b));
            for body in [&resident, &composite] {
                let selected = futures::executor::block_on(body.read(start, end, BodyReadMode::Ordinary))
                    .map_err(|error| TestCaseError::fail(error.to_string()))?;
                prop_assert_eq!(selected.as_slice(), &bytes[start..end]);
                for (start, end) in [(1, 0), (0, bytes.len() + 1)] {
                    prop_assert!(futures::executor::block_on(body.read(start, end, BodyReadMode::Ordinary)).is_err());
                }
            }
        }
    }

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
        assert_eq!(
            selected.as_slice().as_ptr(),
            full.as_slice()
                .get(2..)
                .unwrap_or_else(|| unreachable!())
                .as_ptr()
        );
        drop(body);
        drop(full);
        assert_eq!(selected.as_slice(), b"cd");
        assert!(empty.is_empty());
    }
}
