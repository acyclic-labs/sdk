use super::*;

impl MaterializationJournalStore for LocalCoreStateStore {
    type Error = LocalCoreStateStoreError;

    #[cfg(not(target_arch = "wasm32"))]
    type ActionCompletion<'a>
        = LocalMaterializationActionCompletion
    where
        Self: 'a;

    #[cfg(not(target_arch = "wasm32"))]
    async fn prepare_action<'a>(
        &'a self,
        mut journal: crate::materializer::MaterializationJournal,
    ) -> Result<Self::ActionCompletion<'a>, Self::Error> {
        let work = match (&self.journal_work, &journal.custody) {
            (Some(work), _) => work.clone(),
            (_, Some(allocation)) => allocation.work().clone(),
            _ => crate::materializer::NativeMaterializationWork::new(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .map_err(|(cause, _work)| LocalCoreStateStoreError::MaterializationWork(cause))?,
        };
        if journal.custody.is_none() {
            journal.custody = Some(work.claim(
                crate::materializer::journal_owned_bytes(&journal)?,
                0,
                crate::WorkCounters::UNCHARGED,
            )?);
        }
        self.prepare_journal_action(journal, &work).await
    }

    async fn load(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<crate::materializer::MaterializationJournal>, Self::Error> {
        if let Some(work) = &self.journal_work {
            return self.load_journal(operation_id, work).await;
        }
        self.load_record(MATERIALIZATION_FAMILY, operation_id.into_bytes())
            .await
    }

    async fn compare_and_swap(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
        replacement: crate::materializer::MaterializationJournal,
    ) -> Result<(bool, crate::materializer::MaterializationJournal), Self::Error> {
        if let Some(work) = &self.journal_work {
            return self
                .swap_journal(operation_id, expected_revision, replacement, work)
                .await;
        }
        self.transaction(move |namespace| {
            let stored = compare_and_swap(
                &namespace.record(MATERIALIZATION_FAMILY, &operation_id.into_bytes())?,
                expected_revision,
                &replacement,
            )?;
            Ok((stored, replacement))
        })
        .await
    }
}

/// The original materialization record's accepted, lock-held completion.
pub struct LocalMaterializationActionCompletion {
    pub(super) journal: Option<crate::materializer::MaterializationJournal>,
    pub(super) encoding: Vec<u8>,
    pub(super) encoding_credit: crate::materializer::NativeWorkReservation,
    pub(super) host: Option<crate::native_host::PreparedMaterializationRecord>,
    pub(super) record_lock: OwnerLock,
    pub(super) _admission: Admission,
    pub(super) work: crate::materializer::NativeMaterializationWork,
    pub(super) _body_custody: crate::materializer::NativeAllocation,
    pub(super) _encoding_allocation: crate::materializer::NativeAllocation,
}

impl Future for LocalMaterializationActionCompletion {
    type Output = Result<crate::materializer::MaterializationJournal, LocalCoreStateStoreError>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        _context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();
        let Some(mut journal) = this.journal.take() else {
            return std::task::Poll::Ready(Err(
                crate::materializer::NativeTreeMaterializationError::InvalidLayout.into(),
            ));
        };
        std::task::Poll::Ready((|| {
            journal.work = this.work.journal_work()?;
            let capacity = this.encoding.len();
            let mut writer = &mut this.encoding[..];
            let encoded = serde_json::to_writer(&mut writer, &journal);
            let length = capacity - writer.len();
            this.encoding_credit.record_completed(crate::WorkCounters {
                bytes_encoded: length as u64,
                bytes_copied: length as u64,
                ..crate::WorkCounters::UNCHARGED
            })?;
            encoded
                .map_err(|_| crate::materializer::NativeTreeMaterializationError::InvalidLayout)?;
            this.host
                .take()
                .ok_or(crate::materializer::NativeTreeMaterializationError::MissingCustody)?
                .complete(&this.encoding[..length])?;
            let cleanup = this
                .record_lock
                .1
                .take()
                .ok_or(crate::materializer::NativeTreeMaterializationError::MissingCustody)?;
            let (unlocked, recorded) = cleanup.attempt(|| FileExt::unlock(&this.record_lock.0));
            recorded?;
            unlocked?;
            Ok(journal)
        })())
    }
}

pub(super) struct MaterializationEncodedSize<'a> {
    pub(super) work: &'a crate::materializer::NativeMaterializationWork,
    pub(super) length: usize,
    pub(super) failure: Option<crate::materializer::NativeTreeMaterializationError>,
}

impl Write for MaterializationEncodedSize<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let result = (|| {
            self.work.charge(crate::WorkCounters {
                bytes_encoded: bytes.len() as u64,
                ..crate::WorkCounters::UNCHARGED
            })?;
            self.length = self
                .length
                .checked_add(bytes.len())
                .ok_or(crate::WorkError::Overflow)?;
            Ok::<_, crate::materializer::NativeTreeMaterializationError>(bytes.len())
        })();
        match result {
            Ok(length) => Ok(length),
            Err(error) => {
                self.failure = Some(error);
                Err(std::io::ErrorKind::Other.into())
            }
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(super) fn prepare_materialization_encoding(
    journal: &mut MaterializationJournal,
    work: &crate::materializer::NativeMaterializationWork,
) -> Result<
    (
        Vec<u8>,
        crate::materializer::NativeWorkReservation,
        crate::materializer::NativeAllocation,
    ),
    LocalCoreStateStoreError,
> {
    // Only the inline work scalars can change in this accepted window.
    // Serialize the actual immutable journal with maximal decimal widths
    // using its existing serializer, rather than a guessed schema estimate.
    let original_work = journal.work;
    journal.work = crate::WorkBudget::UNBOUNDED;
    let mut count = MaterializationEncodedSize {
        work,
        length: 0,
        failure: None,
    };
    let encoded = serde_json::to_writer(&mut count, journal);
    journal.work = original_work;
    if let Some(error) = count.failure {
        return Err(error.into());
    }
    encoded?;
    let mut allocation = work.claim(
        count.length as u64,
        u64::from((count.length as u64) != 0),
        crate::WorkCounters::UNCHARGED,
    )?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(count.length)
        .map_err(crate::materializer::NativeTreeMaterializationError::AllocationFailed)?;
    bytes.resize(count.length, 0);
    allocation.reconcile_capacity(bytes.capacity() as u64)?;
    let credit = work.reserve_work(crate::WorkCounters {
        bytes_encoded: count.length as u64,
        bytes_copied: count.length as u64,
        ..crate::WorkCounters::UNCHARGED
    })?;
    Ok((bytes, credit, allocation))
}

impl LocalCoreStateStore {
    /// Binds the existing journal family to the native execution accounting boundary.
    pub fn materialization(&self, work: crate::materializer::NativeMaterializationWork) -> Self {
        Self {
            journal_work: Some(work),
            ..self.clone()
        }
    }
}

impl LocalCoreStateStore {
    pub(super) async fn prepare_journal_action(
        &self,
        mut journal: MaterializationJournal,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<LocalMaterializationActionCompletion, LocalCoreStateStoreError> {
        let custody = journal
            .custody
            .as_ref()
            .ok_or(crate::materializer::NativeTreeMaterializationError::MissingCustody)?;
        if !custody.work().shares_allocation_context(work) {
            return Err(LocalCoreStateStoreError::NativeLedgerMismatch);
        }
        if journal.phase != crate::MaterializationPhase::Preparing
            || !journal.is_valid_for(journal.plan.operation_id)
        {
            return Err(crate::materializer::NativeTreeMaterializationError::InvalidLayout.into());
        }
        self.materialization(work.clone())
            .transaction(move |namespace| {
                let original = namespace
                    ._allocation
                    .as_ref()
                    .expect("bound native work")
                    .work()
                    .clone();
                let work = &original;
                let paths = namespace.record(
                    MATERIALIZATION_FAMILY,
                    &journal.plan.operation_id.into_bytes(),
                )?;
                let (paths, record_lock) = materialization_acquire_lock(paths, work)?;
                let current = measured_read_record::<MaterializationJournal>(&paths, work)?
                    .ok_or(crate::materializer::NativeTreeMaterializationError::InvalidLayout)?;
                if current.0 != journal {
                    return Err(
                        crate::materializer::NativeTreeMaterializationError::InvalidLayout.into(),
                    );
                }
                drop(current);
                let body_custody = work.claim(
                    std::mem::size_of::<LocalMaterializationActionCompletion>() as u64,
                    0,
                    crate::WorkCounters::UNCHARGED,
                )?;
                journal.revision = journal
                    .revision
                    .checked_add(1)
                    .ok_or(crate::WorkError::Overflow)
                    .map_err(crate::materializer::NativeTreeMaterializationError::from)?;
                let (encoding, encoding_credit, encoding_allocation) =
                    prepare_materialization_encoding(&mut journal, work)?;
                // Preserve the actual old image durably before the action. This is
                // the same previous/current family, not a second journal.
                materialization_copy(&paths.current, &paths.previous, work)?;
                materialization_sync(&paths.directory, work)?;
                let host = crate::native_host::PreparedMaterializationRecord::prepare(
                    &paths.directory,
                    Path::new(paths.current.file_name().ok_or(
                        crate::materializer::NativeTreeMaterializationError::InvalidLayout,
                    )?),
                    Path::new(paths.temporary.file_name().ok_or(
                        crate::materializer::NativeTreeMaterializationError::InvalidLayout,
                    )?),
                    work,
                    encoding.len(),
                )?;
                drop(paths);
                let Namespace {
                    root,
                    admission,
                    _allocation,
                    ..
                } = namespace;
                drop(root);
                drop(_allocation);
                Ok(LocalMaterializationActionCompletion {
                    journal: Some(journal),
                    encoding,
                    encoding_credit,
                    host: Some(host),
                    record_lock,
                    _admission: admission,
                    work: work.clone(),
                    _body_custody: body_custody,
                    _encoding_allocation: encoding_allocation,
                })
            })
            .await
    }

    pub(super) async fn load_journal(
        &self,
        operation_id: OperationId,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<Option<MaterializationJournal>, LocalCoreStateStoreError> {
        self.materialization(work.clone())
            .transaction(move |namespace| {
                let original = namespace
                    ._allocation
                    .as_ref()
                    .expect("bound native work")
                    .work()
                    .clone();
                let work = &original;
                let paths = namespace.record(MATERIALIZATION_FAMILY, &operation_id.into_bytes())?;
                materialization_lock(paths, work, |paths| {
                    let journal = measured_read_record::<MaterializationJournal>(&paths, work)?;
                    let journal = journal.map(|(journal, allocation)| {
                        let mut journal = journal;
                        journal.custody = Some(allocation);
                        journal
                    });
                    work.set_original_work(
                        journal
                            .as_ref()
                            .map_or_else(crate::WorkCounters::default, |record| record.work),
                    );
                    Ok(journal)
                })
            })
            .await
    }

    pub(super) async fn swap_journal(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
        mut replacement: MaterializationJournal,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<(bool, MaterializationJournal), LocalCoreStateStoreError> {
        let allocation = replacement
            .custody
            .as_ref()
            .ok_or(crate::materializer::NativeTreeMaterializationError::MissingCustody)?;
        if !allocation.work().shares_ledger(work) {
            return Err(LocalCoreStateStoreError::NativeLedgerMismatch);
        }
        self.materialization(work.clone())
            .transaction(move |namespace| {
                let original = namespace
                    ._allocation
                    .as_ref()
                    .expect("bound native work")
                    .work()
                    .clone();
                let work = &original;
                let paths = namespace.record(MATERIALIZATION_FAMILY, &operation_id.into_bytes())?;
                let stored =
                    materialization_lock(paths, work, |paths| {
                        let current = measured_read_record::<MaterializationJournal>(&paths, work)?;
                        if current
                            .as_ref()
                            .map_or(0, |(journal, _allocation)| journal.revision)
                            != expected_revision
                        {
                            return Ok(false);
                        }
                        work.set_original_work(current.as_ref().map_or_else(
                            crate::WorkCounters::default,
                            |(journal, _allocation)| journal.work,
                        ));
                        replacement.work = work.journal_work()?;
                        measured_write_record(&paths, &replacement, work)?;
                        Ok(true)
                    })?;
                Ok((stored, replacement))
            })
            .await
    }
}

impl LocalCoreStateStore {
    /// Retains terminal intent until the final record removal; partial cleanup remains observable and retryable through this same journal family.
    pub(super) async fn remove_journal(
        &self,
        operation_id: OperationId,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<(), LocalCoreStateStoreError> {
        self.materialization(work.clone())
            .transaction(move |namespace| {
                let original = namespace
                    ._allocation
                    .as_ref()
                    .expect("bound native work")
                    .work()
                    .clone();
                let work = &original;
                let paths = namespace.record(MATERIALIZATION_FAMILY, &operation_id.into_bytes())?;
                materialization_lock(paths, work, |paths| {
                    let current = measured_read_record::<MaterializationJournal>(&paths, work)?;
                    if current.as_ref().is_some_and(|(journal, _allocation)| {
                        !matches!(
                            journal.phase,
                            crate::MaterializationPhase::Applied
                                | crate::MaterializationPhase::RolledBack
                        )
                    }) {
                        return Err(LocalCoreStateStoreError::MaterializationInProgress);
                    }
                    materialization_remove(&paths.temporary, work)?;
                    materialization_remove(&paths.previous, work)?;
                    materialization_remove(&paths.current, work)?;
                    materialization_sync(&paths.directory, work)
                })
            })
            .await
    }
}
