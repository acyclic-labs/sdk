//! Original native handles and publication ownership.
use super::*;

pub(super) type Journal =
    FilesystemExecutionJournal<LocalStream, LocalAuthorityBackend, LocalObjectBackend>;
pub(super) type Effects = ConversationEffectHost<LocalStream>;
pub(super) type NativeReservation = Option<(
    crate::LocalWorkspace,
    acyclic_fs::OperationId,
    Option<acyclic_fs::PublicationReservation>,
)>;

fn publication_permit(reservation: acyclic_fs::PublicationReservation) -> PublicationPermit {
    PublicationPermit::Reservation {
        operation_id: reservation.operation_id.into_bytes(),
        gate_tail: reservation.gate_tail,
        expected: reservation.expected,
    }
}

pub(super) async fn reserve_publication(
    retained_reservation: &mut NativeReservation,
    owner: &TaskJournalOwner<LocalStream>,
    workspace_volume_generation: (
        &crate::LocalWorkspace,
        &acyclic_harness::conversation::VolumeRef,
        GenerationId,
    ),
    command: OperationId,
    meter_cancellation: (&NativeMaterializationWork, &CancellationToken),
    existing_only: bool,
) -> Result<PublicationPermit> {
    let (workspace, volume, generation) = workspace_volume_generation;
    let (meter, cancellation) = meter_cancellation;
    let io = PreparationIo::from_native_work(meter)?;
    owner.verify(false, &mut Some(&mut &io)).await?;
    owner.require_volume_grant(volume, VolumeOperation::Write, &mut Some(&mut &io))?;
    if volume.storage_name(&mut Some(&mut &io))? != workspace.name().as_str() {
        return Err(unauthorized("Pi publication selected another volume"));
    }
    let operation = acyclic_fs::OperationId::from_bytes(command.into_bytes());
    if let Some((actual, original, reservation)) = retained_reservation.as_ref() {
        if actual.id() != workspace.id() || *original != operation {
            return Err(unauthorized(
                "Pi publication substituted its retained reservation",
            ));
        }
        if let Some(reservation) = reservation {
            return Ok(publication_permit(*reservation));
        }
    }
    let recover = existing_only || retained_reservation.is_some();
    let retained = retained_reservation.insert((workspace.clone(), operation, None));
    let reservation = match workspace
        .publication_reservation(operation, meter, cancellation)
        .await
        .map_err(Error::from)?
    {
        Some(reservation) => reservation,
        None if recover => return Err(Error::Indeterminate(command)),
        None => workspace
            .reserve_publication(operation, meter, cancellation)
            .await
            .map_err(Error::from)?,
    };
    retained.2 = Some(reservation);
    if workspace
        .head(meter, cancellation)
        .await
        .map_err(Error::from)?
        .id()
        != generation
    {
        return Err(unauthorized(
            "Pi publication changed its selected generation",
        ));
    }
    owner.verify(false, &mut Some(&mut &io)).await?;
    Ok(publication_permit(reservation))
}

pub(super) async fn release_publication(
    retained_reservation: &mut NativeReservation,
    owner: &TaskJournalOwner<LocalStream>,
    meter: &NativeMaterializationWork,
    cancellation: &CancellationToken,
) -> Result<()> {
    let Some((workspace, operation, retained)) = retained_reservation.as_ref() else {
        return Ok(());
    };
    let io = PreparationIo::from_native_work(meter)?;
    owner.verify(true, &mut Some(&mut &io)).await?;
    let reservation = match retained {
        Some(reservation) => Some(*reservation),
        None => workspace
            .publication_reservation(*operation, meter, cancellation)
            .await
            .map_err(Error::from)?,
    };
    if let Some(reservation) = reservation {
        workspace
            .release_publication(reservation, meter, cancellation)
            .await
            .map_err(Error::from)?;
    }
    *retained_reservation = None;
    Ok(())
}

pub(super) struct BoundEffect {
    pub(super) staged: Option<WorkflowCommand>,
    pub(super) effects: Arc<Effects>,
    pub(super) owner: Arc<TaskJournalOwner<LocalStream>>,
    pub(super) journal: Arc<Journal>,
    pub(super) context: Arc<TaskContext>,
    pub(super) command: OperationId,
    pub(super) plan: (TaskEffectPlan, NativeAllocation),
}

pub(super) fn effect_plan(
    provider: &str,
    guarantee: acyclic_harness::core::EffectGuarantee,
    effect_kind: &str,
    request: &acyclic_harness::conversation::FileRef,
    result_schema: &acyclic_harness::conversation::FileRef,
    work: &NativeMaterializationWork,
) -> Result<(TaskEffectPlan, NativeAllocation)> {
    let mut io = acyclic_harness::store::HistoryWork::new(PreparationIo::from_native_work(work)?);
    let plan = TaskEffectPlan {
        provider: io.clone_text(provider)?,
        guarantee,
        effect_kind: io.clone_text(effect_kind)?,
        request: request.copy(&mut Some(&mut io))?,
        result_schema: result_schema.copy(&mut Some(&mut io))?,
    };
    Ok((plan, io.into_custody()?))
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
    pub(super) effect: Option<Arc<BoundEffect>>,
    pub(super) scratch_gate: NativeReservation,
    pub(super) project_gate: NativeReservation,
    pub(super) preparation_owner: Option<Arc<TaskJournalOwner<LocalStream>>>,
}

pub(super) struct NativeEntry {
    pub(super) prepared: RwLock<Option<Arc<PreparedPiFork>>>,
    pub(super) view: RwLock<Option<Arc<PiNativeLiveView>>>,
    pub(super) cold: RwLock<Option<Arc<PiNativeColdLocator>>>,
    pub(super) handles: tokio::sync::Mutex<LiveHandles>,
    pub(super) run: tokio::sync::Mutex<()>,
    pub(super) operation_cancellation: CancellationToken,
    // Every physical payload and retained SDK proof drops before its birth custody.
    pub(super) _native_custody: NativeAllocation,
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
            view: RwLock::new(None),
            cold: RwLock::new(None),
            handles: tokio::sync::Mutex::new(LiveHandles::default()),
            run: tokio::sync::Mutex::new(()),
            operation_cancellation: CancellationToken::new(),
            _native_custody: custody,
        })
    }
}

impl PiNativeCommandHost {
    pub(super) async fn bind_effect(
        &self,
        shared: Arc<TaskContext>,
        input: &PiTaskInput,
        owner: Arc<TaskJournalOwner<LocalStream>>,
        command: OperationId,
        mut plan: (TaskEffectPlan, NativeAllocation),
        journal: Arc<Journal>,
        view: Option<Arc<PiNativeLiveView>>,
        approval: acyclic_harness::InteractionId,
        staged: Option<WorkflowCommand>,
        work: &NativeMaterializationWork,
    ) -> Result<Arc<BoundEffect>> {
        let bindings = self.result_bindings(&shared, input, work)?;
        let authorization = self.protected.conversation_authority_with_native_work(
            input.binding.task_id,
            input.binding.agent_id,
            shared.scope().grants(),
            work,
        )?;
        let mut io =
            acyclic_harness::store::HistoryWork::new(PreparationIo::from_native_work(work)?);
        let verifier = authorization.0.1.verifier(&mut Some(&mut io))?;
        plan.1.absorb(&mut io.into_custody()?)?;
        let content = bindings.reader;
        let authority = NativeProcessAuthority {
            verifier,
            schemas: SchemaRegistry::new(),
            content: content.clone(),
            results: bindings
                .writer
                .ok_or_else(|| unauthorized("Pi sealed result has no original publisher"))?,
            approvals: journal.clone(),
        };
        let mut provider_plan = effect_plan(
            &plan.0.provider,
            plan.0.guarantee,
            &plan.0.effect_kind,
            &plan.0.request,
            &plan.0.result_schema,
            work,
        )?;
        plan.1.absorb(&mut provider_plan.1)?;
        plan.1.absorb(&mut work.claim(
            std::mem::size_of::<
                NativeProcessProvider<LocalStream, LocalAuthorityBackend, LocalObjectBackend>,
            >() as u64,
            1,
            WorkCounters {
                allocation_operations: 1,
                ..WorkCounters::UNCHARGED
            },
        )?)?;
        let provider = Arc::new(
            NativeProcessProvider::new(
                owner.clone(),
                command,
                provider_plan.0,
                view,
                approval,
                authority,
            )
            .await?,
        );
        let mut registry = EffectRegistry::new(Some(work), Some(content.clone()))?;
        registry.register(provider, Some(work))?;
        let ((audience, issuer, scope), custody) = authorization;
        let effects = Effects::new(
            self.stream.clone(),
            (audience, issuer, scope),
            SchemaRegistry::new(),
            content,
            registry,
            Some(custody),
            Some(work),
        )?;
        Ok(Arc::new(BoundEffect {
            effects,
            owner,
            journal,
            context: shared,
            command,
            staged,
            plan,
        }))
    }

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

        entry
            .prepared
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
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
            handles.preparation_work = Some(work.outside_window());
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
