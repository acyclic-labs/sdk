use crate::*;

/// A bounded group of original, per-file ordered flushes on the native service.
///
/// Success and failure both await every member's physical terminal. Dropping the
/// observer leaves accepted jobs with their original file, fence and meter.
pub struct NativeSyncBatch {
    pending: Vec<Option<NativeCompletion<()>>>,
    failure: Option<(usize, io::Error)>,
    // The vector is freed before its original allocation claim.
    _allocation: ReadAllocation,
}

impl NativeSyncBatch {
    /// Maximum group width, taken from the already initialized native service.
    pub fn capacity() -> io::Result<usize> {
        Ok(workers(&NATIVE_WORKERS, true)?.threads)
    }

    /// Prepares every flush before polling any of them. Admission failure starts
    /// no I/O. Each member uses `NativeFile::sync_async`, including its fence,
    /// original durability charge, fault boundary and platform implementation.
    pub fn new(
        files: &[NativeFile],
        durability: Durability,
        meter: &Arc<dyn NativeReadMeter>,
    ) -> io::Result<Self> {
        Self::prepare(files.len(), meter, |index| {
            files
                .get(index)
                .ok_or(io::ErrorKind::InvalidInput)?
                .sync_async(durability, Some(Arc::clone(meter)))
        })
    }

    fn prepare(
        count: usize,
        meter: &Arc<dyn NativeReadMeter>,
        mut member: impl FnMut(usize) -> io::Result<NativeCompletion<()>>,
    ) -> io::Result<Self> {
        if count > Self::capacity()? {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let bytes = count
            .checked_mul(std::mem::size_of::<Option<NativeCompletion<()>>>())
            .ok_or(io::ErrorKind::InvalidInput)?;
        let allocation = ReadAllocation::new(meter, bytes)?;
        let mut batch = Self {
            pending: Vec::new(),
            failure: None,
            _allocation: allocation,
        };
        batch
            .pending
            .try_reserve_exact(count)
            .map_err(|error| meter.allocation_failed(error))?;
        batch._allocation.reconcile_capacity(
            batch.pending.capacity() * std::mem::size_of::<Option<NativeCompletion<()>>>(),
        )?;
        for index in 0..count {
            meter.examine(1, 0)?;
            batch.pending.push(Some(member(index)?));
        }
        Ok(batch)
    }
}

impl Future for NativeSyncBatch {
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        // A caller's permission to block inline must not turn this independent
        // group back into serial blocking calls. Reuse the same native workers.
        let _defer = InlineBlocking::defer();
        let mut waiting = false;
        for (index, slot) in this.pending.iter_mut().enumerate() {
            let Some(member) = slot else { continue };
            match Pin::new(member).poll(context) {
                Poll::Pending => waiting = true,
                Poll::Ready(result) => {
                    *slot = None;
                    if let Err(error) = result
                        && this
                            .failure
                            .as_ref()
                            .is_none_or(|(first, _)| index < *first)
                    {
                        this.failure = Some((index, error));
                    }
                }
            }
        }
        if waiting {
            Poll::Pending
        } else {
            Poll::Ready(this.failure.take().map_or(Ok(()), |(_, error)| Err(error)))
        }
    }
}

#[cfg(test)]
mod tests;
