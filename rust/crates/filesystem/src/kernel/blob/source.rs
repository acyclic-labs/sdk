use super::*;

/// An owned source chunk together with its actual logical backing capacity.
#[derive(Debug)]
pub struct OwnedSourceChunk {
    /// Exact bytes supplied by this read.
    pub bytes: acyclic_stream::OwnedBytes,
    /// Payload capacity retained by these bytes, not their visible length.
    pub logical_owned_bytes: u64,
    pub(crate) intrinsic: bool,
}

impl OwnedSourceChunk {
    /// Bytes whose retained capacity is accounted by the ordinary caller.
    pub fn claimed(bytes: acyclic_stream::OwnedBytes, logical_owned_bytes: u64) -> Self {
        Self {
            bytes,
            logical_owned_bytes,
            intrinsic: false,
        }
    }

    /// Bytes already retained by their original native allocation owner.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn intrinsic(bytes: acyclic_stream::OwnedBytes, logical_owned_bytes: u64) -> Self {
        Self {
            bytes,
            logical_owned_bytes,
            intrinsic: true,
        }
    }
}

/// Nonblocking bounded byte source for browser, network, and native streams.
pub trait AsyncBlobSource {
    /// Reads bounded bytes into the destination.
    fn read<'a>(
        &'a mut self,
        destination: &'a mut [u8],
        budget: WorkBudget,
        cancellation: &'a CancellationToken,
    ) -> impl Future<Output = std::io::Result<usize>>;

    /// Returns an owned chunk when the source supports it.
    fn read_owned(
        &mut self,
        _maximum: usize,
        _budget: WorkBudget,
        _cancellation: &CancellationToken,
    ) -> impl Future<Output = std::io::Result<Option<OwnedSourceChunk>>> {
        async { Ok(None) }
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// Reads bytes under the original native invocation.
    fn read_with_native_work<'a>(
        &'a mut self,
        _destination: &'a mut [u8],
        _original: &'a crate::NativeMaterializationWork,
        _cancellation: &'a CancellationToken,
    ) -> impl Future<Output = Result<usize, FsError>> {
        async { Err(BlobBuildError::NativeSourceUnavailable.into()) }
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// Returns an owned chunk under the original native invocation.
    fn read_owned_with_native_work(
        &mut self,
        _maximum: usize,
        _original: &crate::NativeMaterializationWork,
        _cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<Option<OwnedSourceChunk>, FsError>> {
        async { Ok(None) }
    }

    /// Takes work accumulated by the source.
    fn take_work(&mut self) -> Option<WorkCounters> {
        None
    }
}

pub(super) fn read_source<T: Read>(
    source: &mut T,
    destination: &mut [u8],
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> std::io::Result<usize> {
    if cancellation.is_cancelled() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Interrupted,
            "blob source read cancelled",
        ));
    }
    let maximum = destination
        .len()
        .min(usize::try_from(budget.source_bytes_read).unwrap_or(usize::MAX));
    if maximum == 0 && !destination.is_empty() {
        return Err(std::io::Error::other("blob source byte budget exhausted"));
    }
    Read::read(
        source,
        destination
            .get_mut(..maximum)
            .ok_or_else(|| std::io::Error::other("invalid bounded source buffer"))?,
    )
}

impl<T: AsRef<[u8]>> AsyncBlobSource for std::io::Cursor<T> {
    async fn read<'a>(
        &'a mut self,
        destination: &'a mut [u8],
        budget: WorkBudget,
        cancellation: &'a CancellationToken,
    ) -> std::io::Result<usize> {
        read_source(self, destination, budget, cancellation)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn read_with_native_work<'a>(
        &'a mut self,
        destination: &'a mut [u8],
        original: &'a crate::NativeMaterializationWork,
        cancellation: &'a CancellationToken,
    ) -> Result<usize, FsError> {
        let available = self
            .get_ref()
            .as_ref()
            .len()
            .saturating_sub(usize::try_from(self.position()).unwrap_or(usize::MAX));
        read_memory_source(self, available, destination, original, cancellation)
    }
}

impl AsyncBlobSource for &[u8] {
    async fn read<'a>(
        &'a mut self,
        destination: &'a mut [u8],
        budget: WorkBudget,
        cancellation: &'a CancellationToken,
    ) -> std::io::Result<usize> {
        read_source(self, destination, budget, cancellation)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn read_with_native_work<'a>(
        &'a mut self,
        destination: &'a mut [u8],
        original: &'a crate::NativeMaterializationWork,
        cancellation: &'a CancellationToken,
    ) -> Result<usize, FsError> {
        read_memory_source(self, self.len(), destination, original, cancellation)
    }
}

impl AsyncBlobSource for std::fs::File {
    async fn read<'a>(
        &'a mut self,
        destination: &'a mut [u8],
        budget: WorkBudget,
        cancellation: &'a CancellationToken,
    ) -> std::io::Result<usize> {
        read_source(self, destination, budget, cancellation)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn read_with_native_work<'a>(
        &'a mut self,
        destination: &'a mut [u8],
        original: &'a crate::NativeMaterializationWork,
        cancellation: &'a CancellationToken,
    ) -> Result<usize, FsError> {
        cancellation.check()?;
        let maximum = destination
            .len()
            .min(usize::try_from(original.remaining()?.source_bytes_read).unwrap_or(usize::MAX));
        if maximum == 0 && !destination.is_empty() {
            original.admit(WorkCounters {
                source_bytes_read: 1,
                ..WorkCounters::UNCHARGED
            })?;
        }
        original.admit(WorkCounters {
            source_bytes_read: usize_to_u64(maximum),
            ..WorkCounters::UNCHARGED
        })?;
        let destination = destination
            .get_mut(..maximum)
            .ok_or(BlobBuildError::TooLarge)?;
        let read = Read::read(self, destination).map_err(BlobBuildError::Source)?;
        original.spent(WorkCounters {
            source_bytes_read: usize_to_u64(read),
            ..WorkCounters::UNCHARGED
        })?;
        Ok(read)
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn read_memory_source<T: Read>(
    source: &mut T,
    available: usize,
    destination: &mut [u8],
    original: &crate::NativeMaterializationWork,
    cancellation: &CancellationToken,
) -> Result<usize, FsError> {
    cancellation.check()?;
    let maximum = usize_to_u64(destination.len().min(available));
    original.admit(WorkCounters {
        source_bytes_read: maximum,
        bytes_copied: maximum,
        ..WorkCounters::UNCHARGED
    })?;
    let read = Read::read(source, destination).map_err(BlobBuildError::Source)?;
    original.spent(WorkCounters {
        source_bytes_read: usize_to_u64(read),
        bytes_copied: usize_to_u64(read),
        ..WorkCounters::UNCHARGED
    })?;
    Ok(read)
}
pub(super) async fn read_owned_blob_source<R: AsyncBlobSource>(
    source: &mut R,
    maximum: usize,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<Option<OwnedSourceChunk>, FsError> {
    accounting.check()?;
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = accounting.native_work() {
        return source
            .read_owned_with_native_work(maximum, original, cancellation)
            .await;
    }
    let result = source
        .read_owned(maximum, accounting.remaining()?, cancellation)
        .await;
    let measured = source.take_work();
    if let Some(measured) = measured {
        completed_blob_backend(measured, accounting)?;
    }
    let chunk = result.map_err(BlobBuildError::Source)?;
    if let Some(chunk) = chunk {
        // This is an explicit by-value transfer from the actual source producer.
        let retained = chunk.logical_owned_bytes;
        let (custody, capacity_result) = accounting.record_actual_owned(retained)?;
        let completed = if measured.is_none() {
            WorkCounters {
                source_bytes_read: usize_to_u64(chunk.bytes.len()),
                ..WorkCounters::UNCHARGED
            }
        } else {
            WorkCounters::UNCHARGED
        };
        let completed_result = accounting.record_completed(completed);
        if let Err(error) = capacity_result.and(completed_result) {
            drop(chunk);
            drop(custody);
            let _ = release_blob_bytes(retained, accounting);
            return Err(error);
        }
        return Ok(Some(chunk));
    }
    Ok(None)
}

pub(super) async fn read_blob_chunk<R: AsyncBlobSource>(
    source: &mut R,
    maximum: usize,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(OwnedSourceChunk, usize), FsError> {
    let mut buffer = Some(empty_blob_vec());
    #[cfg(not(target_arch = "wasm32"))]
    let mut prepared = None;
    let result = async {
        #[cfg(not(target_arch = "wasm32"))]
        let native = accounting.native_work().cloned();
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = &native {
            original.admit(WorkCounters {
                source_bytes_read: usize_to_u64(maximum),
                ..WorkCounters::UNCHARGED
            })?;
        }
        let owned = buffer.as_mut().expect("source storage is prepared once");
        reserve_blob_vec(owned, maximum, accounting)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = &native {
            let capacity = owned.value.capacity();
            let copied = original.reserve_work(crate::WorkCounters {
                bytes_copied: usize_to_u64(capacity),
                ..crate::WorkCounters::UNCHARGED
            })?;
            owned.value.resize(capacity, 0);
            {
                let copy = copied;
                copy.record_completed(crate::WorkCounters {
                    bytes_copied: usize_to_u64(capacity),
                    ..crate::WorkCounters::UNCHARGED
                })
            }?;
            let owned = buffer.take().expect("original source storage");
            prepared = Some(
                acyclic_stream::PreparedOwnedBytes::new(
                    owned.value,
                    owned.custody,
                    &original.runtime_meter(),
                )
                .map_err(|failure| match failure.into_error() {
                    acyclic_stream::PreparedOwnedBytesError::Admission(error) => {
                        original.runtime_error(error).into()
                    }
                    acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized => {
                        FsError::from(crate::NativeTreeMaterializationError::InvalidLayout)
                    }
                })?,
            );
        }
        if let Some(owned) = buffer.as_mut() {
            let zeroed = WorkCounters {
                bytes_copied: usize_to_u64(maximum),
                ..WorkCounters::UNCHARGED
            };
            accounting.admit(zeroed)?;
            owned.value.resize(maximum, 0);
            accounting.record_completed(zeroed)?;
        }
        let mut filled = 0_usize;
        while filled < maximum {
            accounting.check()?;
            #[cfg(not(target_arch = "wasm32"))]
            let destination = match prepared.as_mut() {
                Some(prepared) => &mut prepared.writable()[filled..maximum],
                None => buffer
                    .as_mut()
                    .expect("ordinary source storage")
                    .value
                    .get_mut(filled..)
                    .ok_or(BlobBuildError::TooLarge)?,
            };
            #[cfg(target_arch = "wasm32")]
            let destination = buffer
                .as_mut()
                .expect("ordinary source storage")
                .value
                .get_mut(filled..)
                .ok_or(BlobBuildError::TooLarge)?;
            #[cfg(not(target_arch = "wasm32"))]
            let read = if let Some(original) = &native {
                source
                    .read_with_native_work(destination, original, cancellation)
                    .await
            } else {
                read_standalone_blob_source(source, destination, accounting, cancellation).await
            };
            #[cfg(target_arch = "wasm32")]
            let read =
                read_standalone_blob_source(source, destination, accounting, cancellation).await;
            let read = read?;
            if read > maximum - filled {
                return Err(BlobBuildError::TooLarge.into());
            }
            if read == 0 {
                break;
            }
            filled += read;
        }
        Ok(filled)
    }
    .await;
    match result {
        Err(error) => {
            #[cfg(not(target_arch = "wasm32"))]
            drop(prepared);
            if let Some(buffer) = buffer {
                let _cleanup = release_blob_vec(buffer, accounting);
            }
            Err(error)
        }
        Ok(filled) => {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(prepared) = prepared {
                let logical_owned_bytes = usize_to_u64(prepared.maximum_len());
                let bytes = prepared.freeze(filled).map_err(|failed| {
                    drop(failed);
                    FsError::from(crate::NativeTreeMaterializationError::InvalidLayout)
                })?;
                return Ok((
                    OwnedSourceChunk::intrinsic(bytes, logical_owned_bytes),
                    filled,
                ));
            }
            let mut buffer = buffer.expect("ordinary source storage");
            buffer.value.truncate(filled);
            let logical_owned_bytes = usize_to_u64(buffer.value.capacity());
            let bytes = owned_read_bytes(buffer)?;
            Ok((
                OwnedSourceChunk::claimed(bytes, logical_owned_bytes),
                filled,
            ))
        }
    }
}

pub(super) async fn read_standalone_blob_source<R: AsyncBlobSource>(
    source: &mut R,
    destination: &mut [u8],
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<usize, FsError> {
    let result = source
        .read(destination, accounting.remaining()?, cancellation)
        .await;
    let measured = source.take_work();
    if let Some(measured) = measured {
        completed_blob_backend(measured, accounting)?;
    }
    let read = result.map_err(BlobBuildError::Source)?;
    if measured.is_none() {
        accounting.record_completed(WorkCounters {
            source_bytes_read: usize_to_u64(read),
            ..WorkCounters::UNCHARGED
        })?;
    }
    Ok(read)
}
