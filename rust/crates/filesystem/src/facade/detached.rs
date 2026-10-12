use super::*;

/// Ephemeral path-independent regular file retained by an open native handle.
pub struct DetachedFile<A, O> {
    pub(super) volume: Volume<A, O>,
    pub(super) record: FileRecord,
    /// Keeps what the record reaches from a running collection.
    pub(super) _hold: Option<crate::collection::Hold>,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {

    /// Captures one regular file as an ephemeral path-independent open view.
    pub async fn detach_regular_file(
        &mut self,
        path: &NamespacePath,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<DetachedFile<A, O>> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = async {
            let (lookup, allocation) = self.lookup_no_follow_with_accounting(
                path, &mut accounting, cancellation, true
            ).await?;
            drop(allocation);
            let record = lookup.record.ok_or(FsError::NotFound)?;
            if record.kind != FileKind::Regular {
                return Err(FsError::FileRead(FileRangeReadError::NotRegular));
            }
            accounting.check()?;
            let hold = crate::Collection::hold_record_with_accounting(
                self.volume.fs.inner.objects.collection(), record, self.volume.config, &mut accounting,
            )?;
            Ok(DetachedFile { volume: self.volume.clone(), record, _hold: hold })
        }.await;
        measured(result, &accounting)
    }

    pub(crate) fn detached_from_record<W: FsWork>(
        &self,
        record: FileRecord,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<DetachedFile<A, O>, FsError> {
        DetachedFile::from_record(self.volume.clone(), record, budget, cancellation)
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> DetachedFile<A, O> {

    pub(crate) fn from_record<W: FsWork>(
        volume: Volume<A, O>, record: FileRecord, budget: W, cancellation: &CancellationToken,
    ) -> W::Output<Self, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = (|| -> Result<Self, FsError> {
            accounting.check()?;
            let hold = crate::Collection::hold_record_with_accounting(
                volume.fs.inner.objects.collection(), record, volume.config, &mut accounting,
            )?;
            Ok(Self { volume, record, _hold: hold })
        })();
        budget.finish(result, &accounting)
    }

    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        self._hold.as_ref().map_or(0, crate::collection::Hold::logical_owned_bytes)
    }

    #[cfg(feature = "native-mount")]
    pub(crate) const fn record(&self) -> FileRecord {
        self.record
    }

    /// Stable file identity retained across last-link removal.
    #[must_use]
    pub const fn file_id(&self) -> FileId {
        self.record.file_id
    }

    /// Current detached logical file length.
    #[must_use]
    pub fn logical_bytes(&self) -> u64 {
        if matches!(self.record.payload, FilePayload::InlineRegular(_) | FilePayload::Regular { .. }) {
            record_logical_bytes(self.record)
        } else { 0 }
    }
}

