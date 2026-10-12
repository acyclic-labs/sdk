//! Original native handles and publication ownership.
use super::*;

pub(super) type Journal =
    FilesystemExecutionJournal<LocalStream, LocalAuthorityBackend, LocalObjectBackend>;
pub(super) type Effects = ConversationEffectHost<LocalStream>;
pub(super) type Reservation = (
    acyclic_fs::PublicationPermit,
    NativePublicationCleanup<LocalAuthorityBackend, LocalObjectBackend>,
);

#[derive(Default)]
pub(super) struct NativeGate {
    pub(super) reservation: Option<Reservation>,
    // Readiness only. Failure capabilities belong to the returned SDK Root error.
    pub(super) requires_original_recovery: bool,
}

impl NativeGate {
    pub(super) async fn reserve(
        &mut self,
        owner: &TaskJournalOwner<LocalStream>,
        workspace: &acyclic_fs::Workspace<LocalAuthorityBackend, LocalObjectBackend>,
        volume: &acyclic_harness::conversation::VolumeRef,
        generation: GenerationId,
        command: OperationId,
        meter: &NativeMaterializationWork,
        cancellation: &CancellationToken,
        cleanup_cancellation: &CancellationToken,
        existing_only: bool,
    ) -> Result<PublicationPermit> {
        if let Some(reservation) = self.reservation.as_ref() {
            owner
                .verify(false, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(meter)?)))
                .await?;
            owner.require_volume_grant(volume, VolumeOperation::Write, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(meter)?)))?;
            return Ok(reservation.0);
        }
        let recover_original = existing_only || self.requires_original_recovery;
        self.requires_original_recovery = true;
        let original = acyclic_fs::OperationId::from_bytes(command.into_bytes());
        let acquired = owner
            .publication(
                workspace,
                volume,
                generation,
                original,
                meter,
                cancellation,
                cleanup_cancellation,
                recover_original,
            )
            .await?;
        let reservation = acquired.ok_or(Error::Indeterminate(command))?;
        let permit = reservation.0;
        self.reservation = Some(reservation);
        remaining(meter)?;
        Ok(permit)
    }

    pub(super) async fn release(
        &mut self,
        owner: &TaskJournalOwner<LocalStream>,
        meter: &NativeMaterializationWork,
        cancellation: &CancellationToken,
        command: OperationId,
    ) -> Result<()> {
        let Some(reservation) = self.reservation.take() else {
            return if self.requires_original_recovery {
                Err(Error::Indeterminate(command))
            } else {
                Ok(())
            };
        };
        reservation.1.release(owner, meter, cancellation).await?;
        self.requires_original_recovery = false;
        remaining(meter)?;
        Ok(())
    }
}

pub(super) struct BoundEffect {
    pub(super) staged: Option<WorkflowCommand>,
    pub(super) plan: (TaskEffectPlan, NativeAllocation),
    pub(super) provider: Arc<NativeProcessProvider<LocalStream>>,
    pub(super) work: NativeMaterializationWork,
    pub(super) effects: Arc<Effects>,
    pub(super) owner: Arc<TaskJournalOwner<LocalStream>>,
    pub(super) journal: Arc<Journal>,
    pub(super) context: Arc<TaskContext>,
    pub(super) command: OperationId,
}

#[derive(serde::Serialize)]
pub(super) struct BorrowedEffectPlan<'a> {
    pub(super) provider: &'a str,
    pub(super) guarantee: &'a acyclic_harness::core::EffectGuarantee,
    pub(super) effect_kind: &'a str,
    pub(super) request: &'a acyclic_harness::conversation::FileRef,
    pub(super) result_schema: &'a acyclic_harness::conversation::FileRef,
}

#[derive(Default)]
pub(super) struct LiveHandles {
    pub(super) root: Option<PiNativeRoot>,
    pub(super) session: Option<(
        acyclic_harness::conversation::FileRef,
        acyclic_fs::NativeAllocation,
    )>,
    pub(super) preparation_work: Option<NativeMaterializationWork>,
    pub(super) project: Option<Arc<LocalSingleMount>>,
    pub(super) project_allocation: Option<acyclic_fs::NativeAllocation>,
    pub(super) project_work: Option<NativeMaterializationWork>,
    pub(super) project_startup_owner: Option<NativeMountSession>,
    pub(super) effect: Option<Arc<BoundEffect>>,
    pub(super) scratch_gate: NativeGate,
    pub(super) project_gate: NativeGate,
    pub(super) preparation_owner: Option<(
        Arc<TaskJournalOwner<LocalStream>>,
        Arc<TaskContext>,
        OperationId,
    )>,
}

pub(super) struct NativeEntry {
    pub(super) prepared: RwLock<Option<Arc<PreparedPiFork>>>,
    pub(super) observed: RwLock<Option<Arc<PreparedPiFork>>>,
    pub(super) view: RwLock<Option<Arc<PiNativeLiveView>>>,
    pub(super) cold: RwLock<Option<Arc<PiNativeColdLocator>>>,
    pub(super) handles: tokio::sync::Mutex<LiveHandles>,
    pub(super) run: tokio::sync::Mutex<()>,
    pub(super) operation_cancellation: CancellationToken,
    pub(super) cleanup_cancellation: CancellationToken,
    // Every physical payload and retained SDK proof drops before its birth custody.
    pub(super) native_custody: NativeAllocation,
}

impl NativeEntry {
    pub(super) fn new(work: &NativeMaterializationWork) -> Result<Self> {
        let known_payload =
            std::mem::size_of::<Self>() as u64 + std::mem::size_of::<(TaskId, Arc<Self>)>() as u64;
        let custody = work.claim(
            known_payload,
            u64::from((known_payload) != 0),
            acyclic_fs::WorkCounters::UNCHARGED,
        )?;
        Ok(Self {
            prepared: RwLock::new(None),
            observed: RwLock::new(None),
            view: RwLock::new(None),
            cold: RwLock::new(None),
            handles: tokio::sync::Mutex::new(LiveHandles::default()),
            run: tokio::sync::Mutex::new(()),
            operation_cancellation: CancellationToken::new(),
            cleanup_cancellation: CancellationToken::new(),
            native_custody: custody,
        })
    }
}

impl PiNativeCommandHost {
    pub(super) fn entry(
        &self,
        task: TaskId,
        work: &NativeMaterializationWork,
    ) -> Result<Arc<NativeEntry>> {
        let mut entries = self
            .entries
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(entry) = entries.get(&task) {
            return Ok(Arc::clone(entry));
        }
        let entry = Arc::new(NativeEntry::new(work)?);
        entries.insert(task, Arc::clone(&entry));
        Ok(entry)
    }

    pub(super) fn registered_entry(&self, task: TaskId) -> Result<Arc<NativeEntry>> {
        self.find_entry(task).ok_or(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::NotFound,
            reason: "Pi native task has no authenticated registered entry",
        })
    }

    pub(super) fn find_entry(&self, task: TaskId) -> Option<Arc<NativeEntry>> {
        self.entries
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&task)
            .cloned()
    }

    pub(super) fn prepared(&self, task: TaskId) -> Option<Arc<PreparedPiFork>> {
        let entry = self.find_entry(task)?;
        let prepared = entry
            .prepared
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        prepared
    }

    pub(super) fn source_prepared(&self, task: TaskId) -> Option<Arc<PreparedPiFork>> {
        if let Some(prepared) = self.prepared(task) {
            return Some(prepared);
        }
        let entry = self.find_entry(task)?;
        let observed = entry
            .observed
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        observed
    }

    pub(super) async fn bind_invocation_work(
        &self,
        task: TaskId,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        let entry = self.registered_entry(task)?;
        let mut handles = entry.handles.lock().await;
        if let Some(original) = handles.preparation_work.as_ref() {
            if !original.shares_ledger(work) {
                return Err(Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Conflict,
                    reason: "Pi native command differs from its original invocation ledger",
                });
            }
        } else {
            handles.preparation_work = Some(work.clone());
        }
        Ok(())
    }

    pub(super) async fn preparation_work(
        &self,
        entry: &NativeEntry,
    ) -> Result<NativeMaterializationWork> {
        let mut handles = entry.handles.lock().await;
        if handles.preparation_work.is_none() {
            handles.preparation_work = Some(
                NativeMaterializationWork::new(
                    crate::pi_root_provider::allocation_budget(),
                    &entry.operation_cancellation,
                )
                .map_err(native_construction)?,
            );
        }
        handles
            .preparation_work
            .as_ref()
            .cloned()
            .ok_or_else(|| unauthorized("Pi native preparation has no original work ledger"))
    }
}
