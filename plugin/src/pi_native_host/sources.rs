//! Prepared and observed native project sources.
use super::*;
use acyclic_harness::filesystem::PreparationIo;

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
    /// Retains the fork coordinator's authenticated result.
    pub(crate) fn register_prepared(&self, prepared: PreparedPiFork) -> Result<()> {
        let entry = self.entry(prepared.binding.task_id, prepared._metadata_custody.work())?;
        let mut retained = entry
            .prepared
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
        coordinator.require_input_read(shared.as_ref())?;
        let plan = coordinator
            .load_original_plan(original)
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
        self.register_prepared(prepared)
    }

    pub(crate) fn live_view(&self, task: TaskId) -> Option<Arc<PiNativeLiveView>> {
        let entry = self.find_entry(task)?;

        entry
            .view
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// The exact originally prepared child workspace, not a current-head reopen.
    pub(crate) fn live_workspace(&self, task: TaskId) -> Option<Arc<LocalLazyWorkspace>> {
        self.prepared(task)
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
            .verify(
                false,
                &mut Some(&mut &PreparationIo::from_native_work(&original_work)?),
            )
            .await?;
        if effect.owner.task_id() != task || effect.context.durable_task_id() != Some(task) {
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
        self.prepared(task).map(|prepared| prepared.frozen_source)
    }

    pub(crate) fn selected_source_task(&self, task: TaskId) -> Option<TaskId> {
        let prepared = self.prepared(task)?;
        prepared
            .report
            .request
            .authorized_source
            .as_ref()
            .map(|source| source.source_task)
            .or(prepared.admission.parent)
    }

    /// Captures an active source checkpoint or a terminal publication candidate.
    pub(crate) async fn selection_snapshot(
        &self,
        task: TaskId,
        meter: &NativeMaterializationWork,
    ) -> Result<(LazySnapshotRef, acyclic_fs::NativeAllocation)> {
        let prepared = self.prepared(task).ok_or_else(|| {
            unauthorized("Pi selected child has no authenticated creator companion")
        })?;
        let entry = self
            .find_entry(task)
            .ok_or_else(|| unauthorized("Pi selected child has no original native handles"))?;
        let effect = entry.handles.lock().await.effect.clone();
        let Some(effect) = effect else {
            let snapshot = self
                .prepared(task)
                .map(|prepared| prepared.native_lazy)
                .ok_or_else(|| {
                    unauthorized("Pi undispatched source lost its first native companion")
                })?;
            prepared
                .workspace
                .validate_snapshot(
                    snapshot,
                    &mut acyclic_fs::kernel::KernelAccounting::NativeOriginal {
                        work: meter,
                        cancellation: meter.cancellation(),
                    },
                )
                .await
                .map_err(crate::pi_fork_host::native_lazy_error)?;
            return Ok((snapshot, meter.claim(0, 0, WorkCounters::UNCHARGED)?));
        };
        let outcome = self
            .runtime
            .runtime
            .task_host()
            .outcome_with_native_work(task, meter)?
            .await?;
        if !outcome
            .0
            .as_ref()
            .is_some_and(|outcome| !matches!(outcome, Outcome::Indeterminate { .. }))
        {
            let original_work = entry
                .handles
                .lock()
                .await
                .preparation_work
                .as_ref()
                .ok_or(Error::Indeterminate(effect.command))?
                .clone();
            let view = self
                .live_view(task)
                .ok_or(Error::Indeterminate(effect.command))?;
            return crate::pi_fork_host::freeze_child_source(
                &effect.context,
                &effect.owner,
                &view,
                &prepared.workspace,
                &original_work,
            )
            .await;
        }
        let snapshot = prepared
            .workspace
            .snapshot(&mut acyclic_fs::kernel::KernelAccounting::NativeOriginal {
                work: meter,
                cancellation: meter.cancellation(),
            })
            .await
            .map_err(crate::pi_fork_host::native_lazy_error)?;
        Ok((snapshot, meter.claim(0, 0, WorkCounters::UNCHARGED)?))
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
        if bound.owner.task_id() != task || bound.context.durable_task_id() != Some(task) {
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
            .verify(
                false,
                &mut Some(&mut &PreparationIo::from_native_work(&work)?),
            )
            .await?;
        Ok(work)
    }
}
