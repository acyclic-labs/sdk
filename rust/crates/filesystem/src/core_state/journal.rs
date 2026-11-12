use super::*;
use crate::{NativeMaterializationWork, NativeTreeMaterializationError};

/// The append-only v1 progress record owns no plan or preimage bytes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Progress {
    version: u32,
    journal_version: u32,
    revision: u64,
    phase: crate::MaterializationPhase,
    applied: u32,
    restored: u32,
    work: crate::WorkCounters,
}

impl Progress {
    fn new(journal: &MaterializationJournal) -> Self {
        Self {
            version: 1,
            journal_version: journal.version,
            revision: journal.revision,
            phase: journal.phase,
            applied: journal.applied,
            restored: journal.restored,
            work: journal.work,
        }
    }

    pub(super) fn apply(
        &self,
        journal: &mut MaterializationJournal,
    ) -> Result<(), LocalCoreStateStoreError> {
        if self.version != 1 || self.revision < journal.revision {
            return Err(LocalCoreStateStoreError::Integrity);
        }
        journal.revision = self.revision;
        journal.version = self.journal_version;
        journal.phase = self.phase;
        journal.applied = self.applied;
        journal.restored = self.restored;
        journal.work = self.work;
        Ok(())
    }
}

pub(super) fn progress(
    namespace: &Namespace,
    log: &mut CoreLog,
    operation: OperationId,
) -> Result<Option<Progress>, LocalCoreStateStoreError> {
    Ok(log
        .get::<Progress>(namespace, operation, None, None)?
        .map(|(value, _)| value))
}

pub(super) fn append_progress(
    namespace: &Namespace,
    log: &mut CoreLog,
    operation: OperationId,
    value: Option<Progress>,
) -> Result<(), LocalCoreStateStoreError> {
    let mut change = match &namespace._allocation {
        Some(allocation) => Change::new(allocation.work())?,
        None => Change::default(),
    };
    change.put(
        (
            RecordKey::MaterializationProgress(operation),
            value.map(RecordValue::MaterializationProgress),
        )
            .into(),
    )?;
    log.commit(namespace, change)
}

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
        mut journal: MaterializationJournal,
    ) -> Result<Self::ActionCompletion<'a>, Self::Error> {
        let work = match (&self.journal_work, &journal.custody) {
            (Some(work), _) => work.clone(),
            (_, Some(allocation)) => allocation.work().clone(),
            _ => NativeMaterializationWork::new(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .map_err(|(cause, _)| LocalCoreStateStoreError::from(cause))?,
        };
        if let Some(custody) = &journal.custody {
            if !custody.work().shares_allocation_context(&work) {
                return Err(crate::LazyWorkspaceError::NativeLedgerMismatch.into());
            }
        } else {
            journal.custody = Some(work.claim(
                crate::materializer::journal_owned_bytes(&journal)?,
                0,
                crate::WorkCounters::UNCHARGED,
            )?);
        }
        if journal.phase != crate::MaterializationPhase::Preparing
            || !journal.is_valid_for(journal.plan.operation_id)
        {
            return Err(NativeTreeMaterializationError::InvalidLayout.into());
        }
        self.materialization(work)
            .transaction(move |namespace| {
                let original = namespace
                    ._allocation
                    .as_ref()
                    .ok_or(NativeTreeMaterializationError::MissingCustody)?
                    .work()
                    .clone();
                let work = &original;
                let mut paths = namespace.record(
                    MATERIALIZATION_FAMILY,
                    &journal.plan.operation_id.into_bytes(),
                )?;
                let record_lock = acquire_lock(&mut paths, LockMode::Exclusive)?;
                let current = read_recoverable::<MaterializationJournal>(&paths)?
                    .ok_or(NativeTreeMaterializationError::InvalidLayout)?;
                if current.0 != journal {
                    return Err(NativeTreeMaterializationError::InvalidLayout.into());
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
                    .map_err(NativeTreeMaterializationError::from)?;
                let (encoding, encoding_credit, encoding_allocation) =
                    prepare_materialization_encoding(&mut journal, work)?;
                // Completion writes and flushes a separate image before its
                // atomic replacement. The acknowledged current image remains
                // durable throughout preparation and every incomplete write.
                let host = crate::native_host::PreparedMaterializationRecord::prepare(
                    paths.directory(),
                    Path::new(
                        paths
                            .current
                            .file_name()
                            .ok_or(NativeTreeMaterializationError::InvalidLayout)?,
                    ),
                    Path::new(
                        paths
                            .temporary
                            .file_name()
                            .ok_or(NativeTreeMaterializationError::InvalidLayout)?,
                    ),
                    work,
                    encoding.len(),
                )?;
                drop(paths);
                let Namespace { admission, .. } = namespace;
                Ok(LocalMaterializationActionCompletion {
                    journal: Some(journal),
                    encoding,
                    encoding_credit,
                    host: Some(host),
                    record_lock,
                    _admission: admission,
                    _body_custody: body_custody,
                    _encoding_allocation: encoding_allocation,
                })
            })
            .await
    }

    async fn load(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<MaterializationJournal>, Self::Error> {
        self.load_materialization(operation_id).await
    }

    async fn compare_and_swap(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
        replacement: MaterializationJournal,
    ) -> Result<(bool, MaterializationJournal), Self::Error> {
        self.swap_materialization(operation_id, expected_revision, replacement, false)
            .await
    }

    async fn compare_and_swap_progress(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
        replacement: MaterializationJournal,
    ) -> Result<(bool, MaterializationJournal), Self::Error> {
        self.swap_materialization(operation_id, expected_revision, replacement, true)
            .await
    }
}

impl LocalCoreStateStore {
    async fn swap_materialization(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
        mut replacement: MaterializationJournal,
        progress_only: bool,
    ) -> Result<(bool, MaterializationJournal), LocalCoreStateStoreError> {
        if let Some(work) = &self.journal_work {
            let custody = replacement
                .custody
                .as_ref()
                .ok_or(NativeTreeMaterializationError::MissingCustody)?;
            if !custody.work().shares_ledger(work) {
                return Err(crate::LazyWorkspaceError::NativeLedgerMismatch.into());
            }
        }
        self.transaction(move |mut namespace| {
            namespace.durability = Durability::Flushed;
            namespace.with_log(|log| {
                let current_progress = progress(&namespace, log, operation_id)?;
                if progress_only && let Some(current) = &current_progress {
                    if current.revision != expected_revision {
                        return Ok((false, replacement));
                    }
                    if let Some(allocation) = &namespace._allocation {
                        let work = allocation.work();
                        work.set_original_work(current.work);
                        replacement.work = work.journal_work()?;
                    }
                    append_progress(
                        &namespace,
                        log,
                        operation_id,
                        Some(Progress::new(&replacement)),
                    )?;
                    return Ok((true, replacement));
                }
                let mut paths =
                    namespace.record(MATERIALIZATION_FAMILY, &operation_id.into_bytes())?;
                let stored = with_lock(&mut paths, |paths| {
                    let mut current = read_recoverable::<MaterializationJournal>(paths)?;
                    if let (Some((journal, _)), Some(progress)) = (&mut current, &current_progress)
                    {
                        progress.apply(journal)?;
                    }
                    if current.as_ref().map_or(0, |(journal, _)| journal.revision)
                        != expected_revision
                    {
                        return Ok(false);
                    }
                    if let Some(allocation) = &paths.allocation {
                        let work = allocation.work();
                        work.set_original_work(
                            current
                                .as_ref()
                                .map_or_else(crate::WorkCounters::default, |(journal, _)| {
                                    journal.work
                                }),
                        );
                        replacement.work = work.journal_work()?;
                    }
                    write_journaled(paths, &replacement)?;
                    Ok(true)
                })?;
                // Preparation can still discover directory identities and preimages.
                // Once captured, this fixed image is never rewritten by a step.
                if stored
                    && (current_progress.is_some()
                        || replacement.phase != crate::MaterializationPhase::Preparing)
                {
                    append_progress(
                        &namespace,
                        log,
                        operation_id,
                        (replacement.phase != crate::MaterializationPhase::Preparing)
                            .then(|| Progress::new(&replacement)),
                    )?;
                }
                Ok((stored, replacement))
            })
        })
        .await
    }
}

/// The original materialization record's accepted, lock-held completion.
pub struct LocalMaterializationActionCompletion {
    journal: Option<MaterializationJournal>,
    encoding: Vec<u8>,
    encoding_credit: crate::materializer::NativeWorkReservation,
    host: Option<crate::native_host::PreparedMaterializationRecord>,
    record_lock: OwnerLock,
    _admission: Admission,
    _body_custody: crate::NativeAllocation,
    _encoding_allocation: crate::NativeAllocation,
}

impl Future for LocalMaterializationActionCompletion {
    type Output = Result<MaterializationJournal, LocalCoreStateStoreError>;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        _context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();
        let Some(mut journal) = this.journal.take() else {
            return std::task::Poll::Ready(Err(
                NativeTreeMaterializationError::InvalidLayout.into()
            ));
        };
        std::task::Poll::Ready((|| {
            journal.work = this._body_custody.work().journal_work()?;
            let capacity = this.encoding.len();
            let mut writer = this.encoding.as_mut_slice();
            let encoded = serde_json::to_writer(&mut writer, &journal);
            let length = capacity - writer.len();
            this.encoding_credit.record_completed(crate::WorkCounters {
                bytes_encoded: length as u64,
                bytes_copied: length as u64,
                ..crate::WorkCounters::UNCHARGED
            })?;
            encoded.map_err(|_| NativeTreeMaterializationError::InvalidLayout)?;
            this.host
                .take()
                .ok_or(NativeTreeMaterializationError::MissingCustody)?
                .complete(
                    this.encoding
                        .get(..length)
                        .ok_or(NativeTreeMaterializationError::InvalidLayout)?,
                )?;
            this.record_lock.unlock()?;
            Ok(journal)
        })())
    }
}

pub(super) struct MaterializationEncodedSize<'a> {
    pub(super) work: &'a crate::materializer::NativeMaterializationWork,
    pub(super) length: usize,
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
        result.map_err(std::io::Error::other)
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
    let mut count = MaterializationEncodedSize { work, length: 0 };
    let encoded = serde_json::to_writer(&mut count, journal);
    journal.work = original_work;
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
    async fn load_materialization(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<MaterializationJournal>, LocalCoreStateStoreError> {
        self.transaction(move |mut namespace| {
            namespace.durability = Durability::Flushed;
            namespace.with_log(|log| {
                let mut paths =
                    namespace.record(MATERIALIZATION_FAMILY, &operation_id.into_bytes())?;
                if paths.allocation.is_none() {
                    let mut journal = read_locked::<MaterializationJournal>(&mut paths)?;
                    if let (Some(journal), Some(progress)) =
                        (&mut journal, progress(&namespace, log, operation_id)?)
                    {
                        progress.apply(journal)?;
                    }
                    return Ok(journal);
                }
                with_lock(&mut paths, |paths| {
                    let mut journal = read_recoverable::<MaterializationJournal>(paths)?.map(
                        |(mut journal, allocation)| {
                            journal.custody = allocation;
                            journal
                        },
                    );
                    if let (Some(journal), Some(progress)) =
                        (&mut journal, progress(&namespace, log, operation_id)?)
                    {
                        progress.apply(journal)?;
                    }
                    paths
                        .allocation
                        .as_ref()
                        .ok_or(NativeTreeMaterializationError::MissingCustody)?
                        .work()
                        .set_original_work(
                            journal
                                .as_ref()
                                .map_or_else(crate::WorkCounters::default, |record| record.work),
                        );
                    Ok(journal)
                })
            })
        })
        .await
    }

    /// Binds the existing journal family to the native execution accounting boundary.
    pub fn materialization(&self, work: crate::materializer::NativeMaterializationWork) -> Self {
        Self {
            journal_work: Some(work),
            ..self.clone()
        }
    }
}
