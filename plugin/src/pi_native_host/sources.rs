//! Prepared and observed native project sources.
use super::*;

pub(crate) struct PiNativeLiveSource {
    pub(crate) view: Arc<PiNativeLiveView>,
    pub(crate) prepared: Arc<PreparedPiFork>,
    pub(crate) owner: Arc<TaskJournalOwner<LocalStream>>,
    pub(crate) context: Arc<TaskContext>,
    pub(crate) original_work: NativeMaterializationWork,
}

/// Catalog identity plus an already authenticated selected source capability.
pub(super) struct PiNativeColdLocator {
    pub(super) parent: TaskId,
    pub(super) sdk_operation: String,
    pub(super) selected_source: Arc<LocalLazyWorkspace>,
    _custody: NativeAllocation,
}

impl PiNativeCommandHost {
    /// Called only after the ordinary preparation succeeded and sealed admission was observed.
    pub(crate) async fn register_prepared(&self, prepared: PreparedPiFork) -> Result<()> {
        self.register_binding(prepared, false).await
    }

    /// Read-only sealed publication observation cannot satisfy current-child dispatch admission.
    pub(crate) async fn register_observed_source(&self, prepared: PreparedPiFork) -> Result<()> {
        self.register_binding(prepared, true).await
    }

    pub(super) async fn register_binding(
        &self,
        prepared: PreparedPiFork,
        observed: bool,
    ) -> Result<()> {
        let original_work = prepared._metadata_custody.work().clone();
        prepared.report.validate_with_native_work(&original_work)?;
        let binding = &prepared.binding;
        binding.validate_with_native_work(
            None,
            binding.project_volumes.keys().map(String::as_str),
            false,
            &original_work,
        )?;
        let admitted = self
            .child_admission(binding.task_id, &original_work)
            .await?;
        let actual = &admitted.0;
        let input = &admitted.1;
        if actual != &prepared.admission
            || prepared.report.request.child.id != binding.task_id.to_string()
            || prepared.report.request.child_agent != binding.agent_id
        {
            return Err(unauthorized(
                "Pi prepared child differs from its original sealed admission",
            ));
        }
        validate_child_binding(binding, &input.binding)?;
        if !matches!(input.execution, PiTaskExecution::Child { .. })
            || actual.parent.is_none()
            || binding.project_volumes.len() != 1
            || binding.project_volumes.values().next()
                != Some(&prepared.report.request.preparation.child_project_volume)
            || binding.scratch.volume != prepared.report.child_private_volume
            || binding.results.as_ref().map(|result| &result.volume)
                != prepared.report.child_result_volume.as_ref()
            || prepared.native_lazy.workspace_id != prepared.workspace.workspace().id()
        {
            return Err(unauthorized(
                "Pi prepared selection differs from the actual original fork report",
            ));
        }
        let PiScratchAllocation::Created(scratch) = &binding.scratch.allocation else {
            return Err(unauthorized(
                "Pi native scratch has no original filesystem creator",
            ));
        };
        let Some(result) = &binding.results else {
            return Err(unauthorized("Pi child has no unmounted result volume"));
        };
        let PiScratchAllocation::Created(result) = &result.allocation else {
            return Err(unauthorized(
                "Pi native result has no original filesystem creator",
            ));
        };
        if scratch.generation != prepared.report.child_private_generation
            || Some(&result.generation) != prepared.report.child_result_generation.as_ref()
        {
            return Err(unauthorized(
                "Pi private generations differ from their actual creator report",
            ));
        }
        let entry = self.entry(binding.task_id, &original_work)?;
        let destination = if observed {
            &entry.observed
        } else {
            &entry.prepared
        };
        let mut retained = destination
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(prior) = retained.as_ref() {
            if prior.binding != prepared.binding
                || prior.admission != prepared.admission
                || prior.report != prepared.report
                || prior.frozen_source != prepared.frozen_source
                || prior.native_lazy != prepared.native_lazy
            {
                return Err(Error::Conflict(
                    "Pi live capabilities already belong to another original preparation".into(),
                ));
            }
            return Ok(());
        }
        *retained = Some(prepared.into_native_shared()?);
        Ok(())
    }

    /// Retains only transient capabilities. The service remains the route catalog.
    pub(crate) async fn register_cold_locator_with_native_work(
        &self,
        task: TaskId,
        parent: TaskId,
        sdk_operation: &str,
        root_key: &str,
        selected_source: Arc<LocalLazyWorkspace>,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        let admitted = self.child_admission(task, work).await?;
        let input = &admitted.1;
        let original = pi_fork::original_operation(
            self.protected.policy().root_task,
            parent,
            sdk_operation,
            work,
        )?;
        let operations = PiForkOperations::new(original, work)?;
        let PiTaskExecution::Child { launch } = &input.execution else {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi cold locator cannot substitute a root",
            });
        };
        if sdk_operation.is_empty()
            || launch.operation_id != operations.native
            || task != acyclic_harness::runtime::task_id_for_admission(operations.child)
            || input.binding.project_volumes.len() != 1
            || !input.binding.project_volumes.contains_key(root_key)
            || admitted.0.parent != Some(parent)
        {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi cold locator differs from the original native child admission",
            });
        }
        let mut sdk_operation = work.display(sdk_operation)?;
        let custody = work.claim(
            std::mem::size_of::<PiNativeColdLocator>() as u64,
            u64::from((std::mem::size_of::<PiNativeColdLocator>() as u64) != 0),
            acyclic_fs::WorkCounters::UNCHARGED,
        )?;
        let mut locator = PiNativeColdLocator {
            parent,
            sdk_operation: sdk_operation.0,
            selected_source,
            _custody: custody,
        };
        locator._custody.absorb(&mut sdk_operation.1)?;
        let entry = self.entry(task, work)?;
        let mut retained = entry
            .cold
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(prior) = retained.as_ref() {
            if prior.parent != locator.parent
                || prior.sdk_operation != locator.sdk_operation
                || prior.selected_source.workspace().id()
                    != locator.selected_source.workspace().id()
            {
                return Err(Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Conflict,
                    reason: "Pi cold locator changed its original selected source",
                });
            }
            return Ok(());
        }
        *retained = Some(Arc::new(locator));
        Ok(())
    }

    /// Uses the current original child owner; it never reclaims the parent, creates resources, re-admits the child, or captures a new source head.
    pub(crate) async fn hydrate_child(
        &self,
        shared: &Arc<TaskContext>,
        owner: &Arc<TaskJournalOwner<LocalStream>>,
        fence: &LeaseFence,
        cancellation: &CancellationToken,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        if self.prepared(owner.task_id()).is_some() {
            return Ok(());
        }
        let entry = self.registered_entry(owner.task_id())?;
        let locator = entry
            .cold
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .ok_or_else(|| {
                unauthorized("Pi cold child has no authenticated original source capability")
            })?;
        self.bind_invocation_work(owner.task_id(), work).await?;
        let coordinator = PiForkCoordinator::new(
            self.runtime.clone(),
            self.filesystem.clone(),
            self.stream.clone(),
            &self.resources,
            Arc::clone(&self.protected),
            work,
        )?;
        let original = pi_fork::original_operation(
            self.protected.policy().root_task,
            locator.parent,
            &locator.sdk_operation,
            work,
        )?;
        let plan = coordinator
            .load_plan(shared.as_ref(), original)
            .await?
            .ok_or_else(|| {
                unauthorized("Pi cold child has no original immutable preparation plan")
            })?;
        if plan.commit.caller != locator.parent
            || plan.commit.sdk_operation != locator.sdk_operation
            || plan.request.child.task_id() != owner.task_id()
            || plan.frozen.workspace_id != locator.selected_source.workspace().id()
            || plan.commit.child.project_volumes.len() != 1
        {
            return Err(unauthorized(
                "Pi cold selected source differs from the original sealed preparation",
            ));
        }
        let prepared = coordinator
            .restore_admitted(
                shared,
                owner,
                fence,
                &plan,
                locator.selected_source.clone(),
                cancellation.clone(),
            )
            .await?;
        self.register_prepared(prepared).await
    }

    /// Reads only the SDK-authenticated original preparation journal.
    pub(crate) async fn observe_preparation_journal(
        &self,
        task: TaskId,
        owner: &TaskJournalOwner<LocalStream>,
        work: &NativeMaterializationWork,
    ) -> Result<Option<NativeRecordedPreparation>> {
        let _admission = self.child_admission(task, work).await?;
        if owner.task_id() != task {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Conflict,
                reason: "Pi native journal observer selected another original task",
            });
        }
        let witness = self
            .runtime
            .runtime
            .task_host()
            .task_admission_readonly_witness_with_native_work(task, work)
            .await?;
        acyclic_harness::filesystem::observe_preparation(&witness, work)?.await
    }

    pub(super) async fn original_allocation(
        &self,
        owner: &TaskJournalOwner<LocalStream>,
        command: OperationId,
        work: &NativeMaterializationWork,
    ) -> Result<(
        acyclic_harness::filesystem::NativePreparationRoot,
        acyclic_fs::NativeAllocation,
    )> {
        let observation = self
            .observe_preparation_journal(owner.task_id(), owner, work)
            .await?;
        let Some(recorded) = observation else {
            return Err(Error::Indeterminate(command));
        };
        let (allocation, custody) =
            acyclic_harness::filesystem::NativeVolumeView::<
                LocalAuthorityBackend,
                LocalObjectBackend,
            >::recover_allocation(owner, &recorded.manifest.options, work)?
            .await?;
        Ok((allocation.ok_or(Error::Indeterminate(command))?, custody))
    }

    pub(crate) fn live_view(&self, task: TaskId) -> Option<Arc<PiNativeLiveView>> {
        let entry = self.find_entry(task)?;
        let view = entry
            .view
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        view
    }

    pub(crate) fn binding(
        &self,
        task: TaskId,
        work: &NativeMaterializationWork,
    ) -> Result<Option<(PiTaskBinding, NativeAllocation)>> {
        let Some(prepared) = self.source_prepared(task) else {
            return Ok(None);
        };
        let maximum = self
            .protected
            .policy()
            .maximum_payload_bytes
            .min(prepared.admission.limits.file_bytes);
        let bytes = crate::pi_root_provider::canonical_pi_value(&prepared.binding, maximum, work)?;
        Ok(Some(crate::pi_root_provider::guarded_pi_decode::<
            PiTaskBinding,
        >(
            &bytes.0,
            maximum,
            self.protected.policy().maximum_payload_bytes,
            acyclic_harness::wire::ErrorCode::Invalid,
            JsonValueSchema::None,
            crate::pi_root_provider::ROOT_ARTIFACT_DIAGNOSTIC_LABEL_BYTES,
            work,
        )?))
    }

    /// The exact originally prepared child workspace, not a current-head reopen.
    pub(crate) fn live_workspace(&self, task: TaskId) -> Option<Arc<LocalLazyWorkspace>> {
        self.source_prepared(task)
            .map(|prepared| prepared.workspace.clone())
    }

    /// Original live source owner/context, available while its background run waits on native tool RPC.
    pub(crate) async fn live_source(
        &self,
        task: TaskId,
        work: &NativeMaterializationWork,
    ) -> Result<Option<PiNativeLiveSource>> {
        let Some(entry) = self.find_entry(task) else {
            return Ok(None);
        };
        let (effect, original_work) = {
            let handles = entry.handles.lock().await;
            let Some(effect) = handles.effect.as_ref() else {
                return Ok(None);
            };
            let original_work = handles
                .preparation_work
                .as_ref()
                .ok_or(Error::Indeterminate(effect.command))?
                .clone();
            (Arc::clone(effect), original_work)
        };
        let Some(view) = self.live_view(task) else {
            return Ok(None);
        };
        let Some(prepared) = self.prepared(task) else {
            return Ok(None);
        };
        effect
            .owner
            .verify(false, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(&original_work)?)))
            .await?;
        if effect.owner.task_id() != task
            || effect.context.durable_task_id() != Some(task)
        {
            return Err(unauthorized(
                "Pi live source differs from its original owner/context",
            ));
        }
        work.remaining()?;
        Ok(Some(PiNativeLiveSource {
            view,
            prepared,
            owner: effect.owner.clone(),
            context: effect.context.clone(),
            original_work,
        }))
    }

    /// The parent's immutable writeback baseline, never a recaptured head.
    pub(crate) fn frozen_source(&self, task: TaskId) -> Option<LazySnapshotRef> {
        self.source_prepared(task)
            .map(|prepared| prepared.frozen_source)
    }

    /// The child's original authenticated FIRST native companion, not its final process publication.
    pub(crate) fn source_frozen(&self, task: TaskId) -> Option<LazySnapshotRef> {
        self.source_prepared(task)
            .map(|prepared| prepared.native_lazy)
    }

    pub(crate) fn selected_source_task(&self, task: TaskId) -> Option<TaskId> {
        let prepared = self.source_prepared(task)?;
        prepared
            .report
            .request
            .authorized_source
            .as_ref()
            .map(|source| source.source_task)
            .or_else(|| TaskId::parse(&prepared.report.request.parent.id).ok())
    }

    /// Approval display metadata only; authority remains the selected FS volume.
    pub(crate) fn with_selected_project_path<T>(
        &self,
        task: TaskId,
        volume: &acyclic_harness::conversation::VolumeRef,
        display: impl FnOnce(&std::path::Path) -> T,
    ) -> Option<T> {
        let prepared = self.source_prepared(task)?;
        if !prepared
            .binding
            .project_volumes
            .values()
            .any(|selected| selected == volume)
        {
            return None;
        }
        let view = self.live_view(task)?;
        let (mount, _) = self.project_mount(task, volume)?;
        let mapping = view.manifest().volumes.iter().find(|mapping| {
            &mapping.volume == volume
                && matches!(&mapping.namespace, NativeVolumeNamespace::LazyMount { .. })
        })?;
        if mapping.path != mount.path() {
            return None;
        }
        Some(display(&mapping.path))
    }

    /// Captures an active source checkpoint or a terminal publication candidate.
    pub(crate) async fn selection_snapshot(
        &self,
        task: TaskId,
        meter: &NativeMaterializationWork,
    ) -> Result<(LazySnapshotRef, acyclic_fs::NativeAllocation)> {
        async {
            let prepared = self.prepared(task).ok_or_else(|| {
                unauthorized("Pi selected child has no authenticated creator companion")
            })?;
            let entry = self
                .find_entry(task)
                .ok_or_else(|| unauthorized("Pi selected child has no original native handles"))?;
            let effect = entry.handles.lock().await.effect.clone().ok_or_else(|| {
                unauthorized("Pi selected child has not entered its original native operation")
            })?;
            let outcome = effect.owner.outcome_with_native_work(&meter).await?;
            if outcome.0.is_none() {
                let original_work = entry
                    .handles
                    .lock()
                    .await
                    .preparation_work
                    .as_ref()
                    .ok_or(Error::Indeterminate(effect.command))?
                    .clone();
                effect
                    .owner
                    .verify(false, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(&original_work)?)))
                    .await?;
                let view = self
                    .live_view(task)
                    .ok_or_else(|| Error::Indeterminate(effect.command))?;
                let snapshots = view
                    .checkpoint_projects_with_native_work(effect.owner.as_ref(), &original_work)?
                    .await?;
                if snapshots.0.len() != 1 {
                    return Err(unauthorized(
                        "Pi selected child has no unique authenticated project checkpoint",
                    ));
                }
                let snapshot = snapshots.0[0].1;
                drop(snapshots.0);
                return Ok((snapshot, snapshots.1));
            }
            if prepared.binding.project_volumes.len() != 1 {
                return Err(unauthorized(
                    "Pi published child has no unique original project binding",
                ));
            }
            let project = prepared
                .binding
                .project_volumes
                .values()
                .next()
                .ok_or_else(|| {
                    unauthorized("Pi published child has no original project binding")
                })?;
            effect
                .provider
                .published_project_candidate_with_native_work(
                    effect.provider.original_attempt(),
                    project,
                    meter,
                )?
                .await
        }
        .await
    }

    /// Authorizes a terminal metadata candidate through the actual operation's Common Project reader, then rereads the same protected publication.
    pub(crate) async fn validate_published_selection_snapshot(
        &self,
        task: TaskId,
        inspection: &TaskSourceInspection<LocalStream, LocalAuthorityBackend, LocalObjectBackend>,
        project: &acyclic_harness::conversation::VolumeRef,
        frozen: LazySnapshotRef,
        meter: &NativeMaterializationWork,
    ) -> Result<acyclic_fs::NativeAllocation> {
        let prepared = self.prepared(task).ok_or_else(|| {
            unauthorized("Pi published selection has no authenticated creator companion")
        })?;
        if prepared.binding.project_volumes.len() != 1
            || prepared.binding.project_volumes.values().next() != Some(project)
        {
            return Err(unauthorized(
                "Pi published selection replaced its original project binding",
            ));
        }
        let entry = self.registered_entry(task)?;
        let effect = entry.handles.lock().await.effect.clone().ok_or_else(|| {
            unauthorized("Pi published selection has no original native operation")
        })?;
        let (snapshot, custody) = effect
            .provider
            .inspect_published_project_snapshot_with_native_work(
                effect.provider.original_attempt(),
                inspection,
                project,
                frozen,
                meter,
            )?
            .await?;
        meter.charge(WorkCounters {
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        })?;
        if snapshot != frozen {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Conflict,
                reason: "Pi qualified publication changed its selected snapshot",
            });
        }
        Ok(custody)
    }

    pub(crate) fn project_mount(
        &self,
        task: TaskId,
        volume: &acyclic_harness::conversation::VolumeRef,
    ) -> Option<(Arc<LocalSingleMount>, NativeMaterializationWork)> {
        let prepared = self.prepared(task)?;
        if !prepared
            .binding
            .project_volumes
            .values()
            .any(|bound| bound == volume)
        {
            return None;
        }
        let view = self.live_view(task)?;
        let entry = self.find_entry(task)?;
        let handles = entry.handles.try_lock().ok()?;
        let mount = handles.project.as_ref()?;
        let mapping = view.manifest().volumes.iter().find(|mapping| {
            &mapping.volume == volume
                && matches!(&mapping.namespace, NativeVolumeNamespace::LazyMount { .. })
        })?;
        if mapping.path != mount.path() {
            return None;
        }
        Some((Arc::clone(mount), handles.project_work.clone()?))
    }

    /// Borrow the original live invocation ledger; never initialize another cap for an IO event, prepared-only child, terminal task or selected source.
    pub(crate) async fn invocation_work(&self, task: TaskId) -> Result<NativeMaterializationWork> {
        let entry = self
            .find_entry(task)
            .ok_or_else(|| unauthorized("Pi invocation has no original native handles"))?;
        if self.live_view(task).is_none() {
            return Err(unauthorized("Pi invocation has no original live view"));
        }
        let (bound, work) = {
            let handles = entry.handles.lock().await;
            let bound = handles
                .effect
                .clone()
                .ok_or_else(|| unauthorized("Pi invocation has no original bound owner"))?;
            let work = handles.preparation_work.clone().ok_or_else(|| {
                unauthorized("Pi invocation has no original retained work ledger")
            })?;
            (bound, work)
        };
        if bound.owner.task_id() != task || bound.context.durable_task_id() != Some(task)
        {
            return Err(unauthorized(
                "Pi invocation differs from its original native owner",
            ));
        }
        work.cancellation().check().map_err(|_| {
            acyclic_harness::Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled)
        })?;
        self.child_admission(task, &work).await?;
        bound
            .owner
            .verify(false, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(&work)?)))
            .await?;
        Ok(work)
    }
}
