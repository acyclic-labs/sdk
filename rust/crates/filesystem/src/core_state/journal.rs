use super::*;
use crate::{NativeMaterializationWork, NativeTreeMaterializationError};

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
                return Err(LocalCoreStateStoreError::NativeLedgerMismatch);
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
                    .expect("bound native work")
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
                // Keep the prior image durable before accepting the action.
                paths.copy(&paths.current, &paths.previous)?;
                paths.sync()?;
                let host = crate::native_host::PreparedMaterializationRecord::prepare(
                    &paths.directory,
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
        self.transaction(move |namespace| {
            let mut paths = namespace.record(MATERIALIZATION_FAMILY, &operation_id.into_bytes())?;
            if paths.allocation.is_none() {
                return read_locked(&mut paths);
            }
            with_lock(&mut paths, |paths| {
                let journal = read_recoverable::<MaterializationJournal>(paths)?.map(
                    |(mut journal, allocation)| {
                        journal.custody = allocation;
                        journal
                    },
                );
                paths
                    .allocation
                    .as_ref()
                    .expect("native record paths")
                    .work()
                    .set_original_work(
                        journal
                            .as_ref()
                            .map_or_else(crate::WorkCounters::default, |record| record.work),
                    );
                Ok(journal)
            })
        })
        .await
    }

    async fn compare_and_swap(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
        mut replacement: MaterializationJournal,
    ) -> Result<(bool, MaterializationJournal), Self::Error> {
        if let Some(work) = &self.journal_work {
            let custody = replacement
                .custody
                .as_ref()
                .ok_or(NativeTreeMaterializationError::MissingCustody)?;
            if !custody.work().shares_ledger(work) {
                return Err(LocalCoreStateStoreError::NativeLedgerMismatch);
            }
        }
        self.transaction(move |namespace| {
            let mut paths = namespace.record(MATERIALIZATION_FAMILY, &operation_id.into_bytes())?;
            let stored = with_lock(&mut paths, |paths| {
                let current = read_recoverable::<MaterializationJournal>(paths)?;
                if current.as_ref().map_or(0, |(journal, _)| journal.revision) != expected_revision
                {
                    return Ok(false);
                }
                if let Some(allocation) = &paths.allocation {
                    let work = allocation.work();
                    work.set_original_work(
                        current
                            .as_ref()
                            .map_or_else(crate::WorkCounters::default, |(journal, _)| journal.work),
                    );
                    replacement.work = work.journal_work()?;
                }
                write_journaled(paths, &replacement)?;
                Ok(true)
            })?;
            Ok((stored, replacement))
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
            let mut writer = &mut this.encoding[..];
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
                .complete(&this.encoding[..length])?;
            this.record_lock.unlock()?;
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
