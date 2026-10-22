use super::*;
use crate::kernel::AsyncBlobSource;

// Retain tiny bodies directly from the admitted source read. Reading them back
// from CAS introduces another I/O admission and makes capture depend on cache
// residency after staging. Holes remain zero in the caller's fixed-size buffer.
pub(super) struct ObservedSource<'a, F> {
    inner: acyclic_native_runtime::AsyncRangeReader<F>,
    inline: Option<&'a mut [u8]>,
    filled: usize,
}

pub(super) fn observed_range<'a>(
    native: &'a acyclic_native_runtime::NativeFile,
    range: &HostDataRange,
    inline: &'a mut Option<[u8; crate::kernel::MAXIMUM_INLINE_FILE_BYTES]>,
) -> Result<ObservedSource<'a, &'a acyclic_native_runtime::NativeFile>, WorkError> {
    let bounded = acyclic_native_runtime::AsyncRangeReader::new(native, range.offset, range.length);
    let observed = inline
        .as_mut()
        .map(|bytes| {
            let start = usize::try_from(range.offset).map_err(|_| WorkError::Overflow)?;
            let length = usize::try_from(range.length).map_err(|_| WorkError::Overflow)?;
            let end = start.checked_add(length).ok_or(WorkError::Overflow)?;
            bytes.get_mut(start..end).ok_or(WorkError::Overflow)
        })
        .transpose()?;
    Ok(ObservedSource::new(bounded, observed))
}

impl<'a, F> ObservedSource<'a, F> {
    pub(super) fn new(
        inner: acyclic_native_runtime::AsyncRangeReader<F>,
        inline: Option<&'a mut [u8]>,
    ) -> Self {
        Self {
            inner,
            inline,
            filled: 0,
        }
    }

    fn observe(
        &mut self,
        bytes: &[u8],
        original: &NativeMaterializationWork,
    ) -> Result<(), crate::FsError> {
        let Some(inline) = self.inline.as_mut() else {
            return Ok(());
        };
        let end = self
            .filled
            .checked_add(bytes.len())
            .ok_or(WorkError::Overflow)?;
        let destination = inline
            .get_mut(self.filled..end)
            .ok_or(crate::FsError::SourceSnapshotStale)?;
        let delta = crate::materializer::NativeResourceCredit::Copy.delta(bytes.len() as u64);
        let copy = original.reserve_work(delta)?;
        destination.copy_from_slice(bytes);
        copy.record_completed(delta)?;
        self.filled = end;
        Ok(())
    }
}

impl<F: std::borrow::Borrow<acyclic_native_runtime::NativeFile>> AsyncBlobSource
    for ObservedSource<'_, F>
{
    async fn read<'a>(
        &'a mut self,
        destination: &'a mut [u8],
        budget: WorkBudget,
        cancellation: &'a CancellationToken,
    ) -> std::io::Result<usize> {
        // Native range sources reject this entry point without an original ledger.
        AsyncBlobSource::read(&mut self.inner, destination, budget, cancellation).await
    }

    async fn read_owned_with_native_work(
        &mut self,
        maximum: usize,
        original: &NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<Option<crate::kernel::OwnedSourceChunk>, crate::FsError> {
        let chunk = self
            .inner
            .read_owned_with_native_work(maximum, original, cancellation)
            .await?;
        if let Some(chunk) = &chunk {
            self.observe(&chunk.bytes, original)?;
        }
        Ok(chunk)
    }

    async fn read_with_native_work<'a>(
        &'a mut self,
        destination: &'a mut [u8],
        original: &'a NativeMaterializationWork,
        cancellation: &'a CancellationToken,
    ) -> Result<usize, crate::FsError> {
        let read = self
            .inner
            .read_with_native_work(destination, original, cancellation)
            .await?;
        self.observe(
            destination.get(..read).ok_or(WorkError::Overflow)?,
            original,
        )?;
        Ok(read)
    }
}
