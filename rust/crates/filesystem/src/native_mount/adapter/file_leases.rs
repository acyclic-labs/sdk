#[derive(Default)]
struct FileMutationEntries {
    entries: std::collections::HashMap<FileId, Weak<FileMutation>>,
    allocation: Option<crate::NativeAllocation>,
}

// An owned mutex guard retains the allocation for its own storage.
type FileMutation = tokio::sync::Mutex<crate::NativeAllocation>;

struct FileMutationLease<'a> {
    entries: &'a StdMutex<FileMutationEntries>,
    file: FileId,
    entry: Arc<FileMutation>,
}

impl Drop for FileMutationLease<'_> {
    fn drop(&mut self) {
        let mut entries = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        if Arc::strong_count(&self.entry) == 1 {
            entries.entries.remove(&self.file);
        }
    }
}

fn file_mutation_map_bytes(capacity: usize) -> Result<u64, MountSourceError> {
    if capacity == 0 {
        return Ok(0);
    }
    capacity
        .checked_mul(2 * std::mem::size_of::<(FileId, Weak<FileMutation>)>() + 2)
        .and_then(|bytes| bytes.checked_add(32))
        .map(|bytes| bytes as u64)
        .ok_or(WorkError::Overflow.into())
}

impl FileMutationEntries {
    fn reserve(&mut self, work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        if self.entries.len() == self.entries.capacity() {
            let count = self
                .entries
                .capacity()
                .max(4)
                .checked_mul(2)
                .ok_or(WorkError::Overflow)?;
            let mut allocation = work
                .claim(
                    file_mutation_map_bytes(count)?,
                    1,
                    crate::WorkCounters::UNCHARGED,
                )
                .map_err(MountSourceError::NativeWork)?;
            let copied = crate::WorkCounters {
                bytes_copied: self
                    .entries
                    .len()
                    .checked_mul(std::mem::size_of::<(FileId, Weak<FileMutation>)>())
                    .ok_or(WorkError::Overflow)? as u64,
                ..crate::WorkCounters::UNCHARGED
            };
            let copying = work
                .reserve_work(copied)
                .map_err(MountSourceError::NativeWork)?;
            let mut next = std::collections::HashMap::new();
            next.try_reserve(count)
                .map_err(MountSourceError::Allocation)?;
            allocation
                .reconcile_capacity(file_mutation_map_bytes(next.capacity())?)
                .map_err(MountSourceError::NativeWork)?;
            next.extend(self.entries.drain());
            self.entries = next;
            self.allocation = Some(allocation);
            copying
                .record_completed(copied)
                .map_err(MountSourceError::NativeWork)?;
        }
        Ok(())
    }
}

impl<A, O> SharedCheckout<A, O> {
    fn file_mutation(
        &self,
        file: FileId,
        work: &crate::NativeMaterializationWork,
    ) -> Result<FileMutationLease<'_>, MountSourceError> {
        crate::native_host::NativeHostWork::check(&work).map_err(MountSourceError::NativeWork)?;
        let mut entries = self
            .file_mutations
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let inspected = work
            .reserve_work(crate::WorkCounters {
                items_examined: 1,
                ..crate::WorkCounters::UNCHARGED
            })
            .map_err(MountSourceError::NativeWork)?;
        let existing = entries.entries.get(&file).and_then(Weak::upgrade);
        inspected
            .record_completed(crate::WorkCounters {
                items_examined: 1,
                ..crate::WorkCounters::UNCHARGED
            })
            .map_err(MountSourceError::NativeWork)?;
        let mutation = match existing {
            Some(mutation) => mutation,
            None => {
                entries.reserve(work)?;
                let allocation = work
                    .claim(
                        (std::mem::size_of::<FileMutation>() + 2 * std::mem::size_of::<usize>())
                            as u64,
                        1,
                        crate::WorkCounters::UNCHARGED,
                    )
                    .map_err(MountSourceError::NativeWork)?;
                let mutation = Arc::new(tokio::sync::Mutex::new(allocation));
                entries.entries.insert(file, Arc::downgrade(&mutation));
                mutation
            }
        };
        Ok(FileMutationLease {
            entries: &self.file_mutations,
            file,
            entry: mutation,
        })
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> SharedCheckout<A, O> {
    async fn file_candidate(
        &self,
        file: FileId,
        carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
    ) -> Result<CheckoutCandidate<'_, A, O>, MountSourceError> {
        let work = carrier
            .original_native_work()
            .ok_or(MountSourceError::OriginalSnapshotUnavailable)?;
        let lease = self.file_mutation(file, &work)?;
        let guard = await_lock(
            Arc::clone(&lease.entry).lock_owned(),
            carrier,
            cancellation,
            lifetime,
        )
        .await?;
        let mut candidate = self
            .candidate(None, carrier, cancellation, lifetime)
            .await?;
        candidate._file_mutation = Some((guard, lease));
        Ok(candidate)
    }
}
