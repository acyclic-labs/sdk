use super::*;
use crate::{NativeMaterializationWork, NativeTreeMaterializationError};
mod receipt;
use receipt::{CachedCut, Receipt};

pub(super) type JournalCut = Arc<Mutex<Option<CachedCut>>>;

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
        let work = match (&self.journal_work, journal.custody()) {
            (Some(work), _) => work.clone(),
            (_, Some(allocation)) => allocation.work().clone(),
            _ => NativeMaterializationWork::new(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .map_err(|(cause, _)| LocalCoreStateStoreError::from(cause))?,
        };
        if let Some(custody) = journal.custody() {
            if !custody.work().shares_allocation_context(&work) {
                return Err(crate::LazyWorkspaceError::NativeLedgerMismatch.into());
            }
        } else {
            journal.fund(work.claim(0, 0, crate::WorkCounters::UNCHARGED)?)?;
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
                let operation = journal.plan.operation_id;
                let current =
                    receipt::Log::open(&paths, operation, retained_cut(&namespace.journal_cut))?
                        .ok_or(NativeTreeMaterializationError::InvalidLayout)?;
                if current.current != Some(Receipt::from(&journal)) {
                    return Err(NativeTreeMaterializationError::InvalidLayout.into());
                }

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
                let mut receipt = Receipt::from(&journal);
                let (encoding, encoding_credit, encoding_allocation) =
                    prepare_materialization_encoding(&mut receipt, work)?;
                current.cut_tail(Some(work))?;
                let host =
                    PreparedJournalAppend::new(current.file, current.end, encoding.len(), work)?;
                let identity = current.identity;
                drop(paths);
                let Namespace {
                    admission,
                    journal_cut,
                    ..
                } = namespace;
                Ok(LocalMaterializationActionCompletion {
                    journal: Some(journal),
                    receipt,
                    encoding,
                    encoding_credit,
                    host: Some(host),
                    journal_cut,
                    identity,
                    live_data: current.live_data,
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

    async fn matches(&self, journal: &MaterializationJournal) -> Result<bool, Self::Error> {
        let operation = journal.plan.operation_id;
        let expected = Receipt::from(journal);
        self.transaction(move |namespace| {
            let mut paths = namespace.record(MATERIALIZATION_FAMILY, &operation.into_bytes())?;
            with_lock(&mut paths, |paths| {
                Ok(
                    receipt::Log::open(paths, operation, retained_cut(&namespace.journal_cut))?
                        .and_then(|log| log.current)
                        .is_some_and(|current| current == expected),
                )
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
            match replacement.custody() {
                Some(custody) if !custody.work().shares_ledger(work) => {
                    return Err(crate::LazyWorkspaceError::NativeLedgerMismatch.into());
                }
                Some(_) => {}
                None => {
                    replacement.fund(work.claim(0, 0, crate::WorkCounters::UNCHARGED)?)?;
                }
            }
        }
        self.transaction(move |namespace| {
            let mut paths = namespace.record(MATERIALIZATION_FAMILY, &operation_id.into_bytes())?;
            let stored = with_lock(&mut paths, |paths| {
                let log =
                    receipt::Log::open(paths, operation_id, retained_cut(&namespace.journal_cut))?;
                let current = log.as_ref().and_then(|log| log.current);
                if current.as_ref().map_or(0, |journal| journal.revision) != expected_revision {
                    return Ok(false);
                }
                if current
                    .as_ref()
                    .is_some_and(|current| current.history_id != replacement.history_id)
                {
                    return Ok(false);
                }
                if let Some(allocation) = &paths.allocation {
                    let work = allocation.work();
                    work.set_original_work(
                        current
                            .as_ref()
                            .map_or_else(crate::WorkCounters::default, |journal| journal.work),
                    );
                    replacement.work = work.journal_work()?;
                }
                let work = paths
                    .allocation
                    .as_ref()
                    .map(|allocation| allocation.work());
                let (batch, new_data) = receipt::encode(current, &replacement, work)?;
                let old_data = log.as_ref().map_or(0, |log| log.live_data);
                let next_receipt = batch.bytes.len() as u64 - new_data;
                let live_data = old_data
                    .checked_add(new_data)
                    .ok_or(LocalCoreStateStoreError::Integrity)?;
                let live_size = live_data
                    .checked_add(next_receipt)
                    .ok_or(LocalCoreStateStoreError::Integrity)?;
                if log
                    .as_ref()
                    .is_some_and(|log| log.end - old_data > live_size)
                {
                    drop(log);
                    let cut = compact_journal(
                        paths,
                        operation_id,
                        live_data,
                        &batch.bytes,
                        Receipt::from(&replacement),
                        work,
                    )?;
                    publish_cut(&namespace.journal_cut, cut);
                    return Ok(true);
                }
                let (file, end, identity) = match log {
                    Some(log) => {
                        if current.is_none() {
                            paths.sync()?;
                        }
                        log.cut_tail(work)?;
                        (log.file, log.end, log.identity)
                    }
                    None => {
                        let relative = paths
                            .directory()
                            .strip_prefix(&namespace.root)
                            .map_err(|_| LocalCoreStateStoreError::Integrity)?;
                        namespace.durable_directory(relative)?;
                        if let Some(parent) = paths.directory().parent() {
                            if let Some(work) = work {
                                let parent =
                                    crate::native_host::HostRoot::open(parent, &mut &*work)?;
                                crate::native_host::sync_directory_held(
                                    &parent.directory(),
                                    &mut &*work,
                                )?;
                            } else {
                                sync_parent(parent, NativeDurability::Full)?;
                            }
                        }
                        let file = open_log_file(&namespace, paths, work)?;
                        let identity = receipt::journal_identity(&file, work)?;
                        (file, 0, identity)
                    }
                };
                let flush = work
                    .map(|work| {
                        work.reserve_work(crate::WorkCounters {
                            durability_operations: 1,
                            ..Default::default()
                        })
                    })
                    .transpose()?;
                write_append_batch(end, &batch.bytes, work, |position, bytes| {
                    acyclic_native_runtime::WriteAt::new(&file, position).write(bytes)
                })?;
                let cut = CachedCut::new(
                    operation_id,
                    identity,
                    end + batch.bytes.len() as u64,
                    live_data,
                    Receipt::from(&replacement),
                    &batch.bytes,
                )?;
                flush_journal(&file, flush)?;
                publish_cut(&namespace.journal_cut, cut);
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
    receipt: Receipt,
    encoding: Vec<u8>,
    encoding_credit: crate::ledger::NativeWorkReservation,
    host: Option<PreparedJournalAppend>,
    journal_cut: JournalCut,
    identity: acyclic_native_runtime::FileIdentity,
    live_data: u64,
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
            this.receipt.work = journal.work;
            let capacity = this.encoding.len();
            let prefix = receipt::FORMAT.prefix_bytes();
            let mut writer = this
                .encoding
                .get_mut(prefix..capacity - acyclic_log_frame::LogFrame::CHECKSUM_BYTES)
                .ok_or(NativeTreeMaterializationError::InvalidLayout)?;
            let body_capacity = writer.len();
            let encoded = serde_json::to_writer(&mut writer, &("r", &this.receipt));
            let length = body_capacity - writer.len();
            this.encoding_credit.record_completed(crate::WorkCounters {
                bytes_encoded: length as u64,
                bytes_copied: (prefix + length + acyclic_log_frame::LogFrame::CHECKSUM_BYTES)
                    as u64,
                bytes_hashed: (prefix + length + receipt::FORMAT.domain.len()) as u64,
                ..crate::WorkCounters::UNCHARGED
            })?;
            encoded.map_err(|_| NativeTreeMaterializationError::InvalidLayout)?;
            this.encoding.truncate(prefix + length);
            receipt::FORMAT.seal(&mut this.encoding, 0)?;
            let host = this
                .host
                .take()
                .ok_or(NativeTreeMaterializationError::MissingCustody)?;
            let end = host.end + this.encoding.len() as u64;
            let cut = CachedCut::new(
                journal.plan.operation_id,
                this.identity,
                end,
                this.live_data,
                this.receipt,
                &this.encoding,
            )?;
            host.complete(&this.encoding)?;
            publish_cut(&this.journal_cut, cut);
            this.record_lock.unlock()?;
            Ok(journal)
        })())
    }
}

pub(super) struct MaterializationEncodedSize<'a> {
    pub(super) work: &'a crate::ledger::NativeMaterializationWork,
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

fn prepare_materialization_encoding(
    journal: &mut Receipt,
    work: &crate::ledger::NativeMaterializationWork,
) -> Result<
    (
        Vec<u8>,
        crate::ledger::NativeWorkReservation,
        crate::ledger::NativeAllocation,
    ),
    LocalCoreStateStoreError,
> {
    // Only the inline work scalars can change in this accepted window.
    // Serialize the actual immutable journal with maximal decimal widths
    // using its existing serializer, rather than a guessed schema estimate.
    let original_work = journal.work;
    journal.work = crate::WorkBudget::UNBOUNDED;
    let mut count = MaterializationEncodedSize { work, length: 0 };
    let encoded = serde_json::to_writer(&mut count, &("r", &*journal));
    journal.work = original_work;
    encoded?;
    let maximum = usize::try_from(receipt::FORMAT.frame_bytes(count.length as u64).ok_or(
        NativeTreeMaterializationError::from(crate::WorkError::Overflow),
    )?)
    .map_err(|_| NativeTreeMaterializationError::from(crate::WorkError::Overflow))?;
    let mut allocation = work.claim(maximum as u64, 1, crate::WorkCounters::UNCHARGED)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(maximum)
        .map_err(crate::materializer::NativeTreeMaterializationError::AllocationFailed)?;
    bytes.resize(maximum, 0);
    allocation.reconcile_capacity(bytes.capacity() as u64)?;
    let credit = work.reserve_work(crate::WorkCounters {
        bytes_encoded: count.length as u64,
        bytes_copied: maximum as u64,
        bytes_hashed: (receipt::FORMAT.domain.len() + receipt::FORMAT.prefix_bytes() + count.length)
            as u64,
        ..crate::WorkCounters::UNCHARGED
    })?;
    Ok((bytes, credit, allocation))
}

impl LocalCoreStateStore {
    async fn load_materialization(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<MaterializationJournal>, LocalCoreStateStoreError> {
        self.transaction(move |namespace| {
            let mut paths = namespace.record(MATERIALIZATION_FAMILY, &operation_id.into_bytes())?;
            with_lock(&mut paths, |paths| {
                let work = paths
                    .allocation
                    .as_ref()
                    .map(|allocation| allocation.work());
                let journal = match receipt::Log::open(paths, operation_id, None)? {
                    Some(log) => log.load(operation_id, work)?,
                    None => None,
                };

                if let Some(allocation) = &paths.allocation {
                    allocation.work().set_original_work(
                        journal
                            .as_ref()
                            .map_or_else(crate::WorkCounters::default, |record| record.work),
                    );
                }
                Ok(journal)
            })
        })
        .await
    }

    /// Binds the existing journal family to the native execution accounting boundary.
    pub fn materialization(&self, work: crate::ledger::NativeMaterializationWork) -> Self {
        Self {
            journal_work: Some(work),
            ..self.clone()
        }
    }
}

pub(super) fn remove_materialization(
    namespace: &Namespace,
    operation: OperationId,
) -> Result<(), LocalCoreStateStoreError> {
    let mut paths = namespace.record(MATERIALIZATION_FAMILY, &operation.into_bytes())?;
    with_lock(&mut paths, |paths| {
        if let Some(receipt) =
            receipt::Log::open(paths, operation, retained_cut(&namespace.journal_cut))?
                .and_then(|log| log.current)
        {
            if !matches!(
                receipt.phase,
                crate::MaterializationPhase::Applied | crate::MaterializationPhase::RolledBack
            ) {
                return Err(LocalCoreStateStoreError::MaterializationInProgress);
            }
        }
        remove_record(paths)
    })
}

struct PreparedJournalAppend {
    file: File,
    end: u64,
    writes: crate::NativeWorkReservation,
    flush: crate::NativeWorkReservation,
}

impl PreparedJournalAppend {
    fn new(
        file: File,
        end: u64,
        maximum: usize,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Self, LocalCoreStateStoreError> {
        Ok(Self {
            file,
            end,
            writes: work.reserve_work(crate::WorkCounters {
                backend_write_operations: maximum as u64,
                object_bytes_written: maximum as u64,
                ..Default::default()
            })?,
            flush: work.reserve_work(crate::WorkCounters {
                durability_operations: 1,
                ..Default::default()
            })?,
        })
    }

    fn complete(self, bytes: &[u8]) -> Result<(), LocalCoreStateStoreError> {
        let Self {
            file,
            end,
            writes,
            flush,
        } = self;
        write_append_batch(end, bytes, None, |position, bytes| {
            writes
                .record_completed(crate::WorkCounters {
                    backend_write_operations: 1,
                    ..Default::default()
                })
                .map_err(std::io::Error::other)?;
            let length = acyclic_native_runtime::WriteAt::new(&file, position).write(bytes)?;
            writes
                .record_completed(crate::WorkCounters {
                    object_bytes_written: length as u64,
                    ..Default::default()
                })
                .map_err(std::io::Error::other)?;
            Ok(length)
        })?;
        flush_journal(&file, Some(flush))
    }
}

fn compact_journal(
    paths: &RecordPaths<'_>,
    operation: OperationId,
    live_data: u64,
    batch: &[u8],
    receipt: Receipt,
    work: Option<&crate::NativeMaterializationWork>,
) -> Result<CachedCut, LocalCoreStateStoreError> {
    let old =
        receipt::Log::open(paths, operation, None)?.ok_or(LocalCoreStateStoreError::Integrity)?;
    let next = match work {
        Some(work) => {
            let next = materialization_open_options(
                &paths.temporary,
                &crate::native_host::NativeOpenOptions::ReadWriteCreate,
                work,
            )?;
            crate::native_host::truncate_file(&next, 0, &mut &*work)?;
            next
        }
        None => OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&paths.temporary)?,
    };
    let identity = receipt::journal_identity(&next, work)?;
    old.copy_live_data(&next, work)?;
    write_append_batch(old.live_data, batch, work, |offset, bytes| {
        acyclic_native_runtime::WriteAt::new(&next, offset).write(bytes)
    })?;
    let flush = work
        .map(|work| {
            work.reserve_work(crate::WorkCounters {
                durability_operations: 1,
                ..Default::default()
            })
        })
        .transpose()?;
    flush_journal(&next, flush)?;
    drop(next);
    let end = old.live_data + batch.len() as u64;
    drop(old);
    paths.rename(
        &paths.temporary,
        &paths.current,
        None,
        acyclic_native_runtime::RenameMode::Replace,
    )?;
    paths.sync()?;
    CachedCut::new(operation, identity, end, live_data, receipt, batch)
}

fn flush_journal(
    file: &File,
    credit: Option<crate::NativeWorkReservation>,
) -> Result<(), LocalCoreStateStoreError> {
    if let Some(credit) = credit {
        credit.record_completed(crate::WorkCounters {
            durability_operations: 1,
            ..Default::default()
        })?;
    }
    acyclic_native_runtime::flush_file_data(file)?;
    Ok(())
}

fn retained_cut(cache: &JournalCut) -> Option<CachedCut> {
    *cache.lock().unwrap_or_else(PoisonError::into_inner)
}

fn publish_cut(cache: &JournalCut, cut: CachedCut) {
    *cache.lock().unwrap_or_else(PoisonError::into_inner) = Some(cut);
}
