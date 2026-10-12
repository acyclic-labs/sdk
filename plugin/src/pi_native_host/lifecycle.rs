//! Stop, drainage, rollback and scratch disposal.
use super::*;

impl PiNativeCommandHost {
    pub(crate) async fn request_scratch_disposal(
        &self,
        task: TaskId,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        self.child_admission(task, work).await?;
        self.registered_entry(task)?
            .handles
            .lock()
            .await
            .dispose_requested = true;
        Ok(())
    }

    /// Signal every child before awaiting any service job or collector join.
    pub(crate) async fn signal_stop_child(
        &self,
        task: TaskId,
        meter: &NativeMaterializationWork,
    ) -> Result<()> {
        let cleanup = meter.cancellation();
        async {
            self.child_admission(task, meter).await?;
            let entry = self.registered_entry(task)?;
            let effect = {
                let handles = entry.handles.lock().await;
                if handles.drained.is_some() && handles.mounts_drained {
                    return Ok(());
                }
                if handles.effect.is_none() && handles.preparation_owner.is_some() {
                    entry.operation_cancellation.cancel();
                    return Ok(());
                }
                handles.effect.clone()
            };
            if let Some(effect) = effect {
                if self
                    .retained_unstarted_preparation(&entry, &effect, &meter)
                    .await?
                {
                    entry.operation_cancellation.cancel();
                    return Ok(());
                }
                effect
                    .provider
                    .signal_stop(effect.provider.original_attempt(), &meter, cleanup)
                    .await?;
                return Ok(());
            }
            if self.original_never_started(task, &meter).await? {
                return Ok(());
            }
            Err(unauthorized(
                "Pi stop requires its original supervisor or SDK preparation owner",
            ))
        }
        .await
    }

    /// Background only: join the original SDK supervisor and its own collectors.
    pub(crate) async fn drain_stopped_child(
        &self,
        task: TaskId,
        deactivate: bool,
        meter: &NativeMaterializationWork,
    ) -> Result<()> {
        async {
            let entry = self.registered_entry(task)?;
            let (effect, drained, interrupted, dispose) = {
                let handles = entry.handles.lock().await;
                (
                    handles.effect.clone(),
                    handles.drained.is_some() && handles.mounts_drained,
                    handles.preparation_owner.is_some(),
                    deactivate || handles.dispose_requested,
                )
            };
            if !drained {
                if let Some(effect) = effect {
                    if interrupted
                        && self
                            .retained_unstarted_preparation(&entry, &effect, &meter)
                            .await?
                    {
                        return self.rollback_preparation(task, meter).await;
                    }
                    self.drain_original_process(&entry, &effect, meter).await?;
                } else if interrupted {
                    return self.rollback_preparation(task, meter).await;
                } else if self.original_never_started(task, &meter).await? {
                    return Ok(());
                } else {
                    return Err(unauthorized(
                        "Pi drainage has no authenticated original attempt",
                    ));
                }
            }
            if dispose {
                self.dispose_child_scratch(task, meter).await?;
            } else {
                self.cleanup_child(task, meter).await?;
            }
            let retained = entry.handles.lock().await.effect.clone();
            if !deactivate {
                if let Some(effect) = retained {
                    let outcome = effect.owner.outcome_with_native_work(&meter).await?;
                    if outcome.0.is_none() {
                        let workflow = self
                            .runtime
                            .runtime
                            .workflow_journal(task, effect.context.owned_task_fence()?)
                            .await?;
                        effect
                            .journal
                            .release_if_quiescent_with_native_work(&meter, &workflow)
                            .await?;
                    }
                }
            }
            Ok(())
        }
        .await
    }

    /// Durable subtree intent first.
    pub(crate) async fn cancel_child_with_native_work(
        &self,
        task: TaskId,
        work: &NativeMaterializationWork,
    ) -> Result<Option<(EffectStatus, NativeAllocation)>> {
        let _admission = self.child_admission(task, work).await?;
        self.runtime
            .runtime
            .task_host()
            .cancel(task, Some(work))
            .await?;
        let Some(entry) = self.find_entry(task) else {
            return Ok(None);
        };
        let effect = entry.handles.lock().await.effect.clone();
        let Some(effect) = effect else {
            return Ok(None);
        };
        let receipt = effect
            .provider
            .receipt(effect.provider.original_attempt(), work, None)
            .await?;
        match receipt {
            Some(receipt) => {
                let (effect, custody) = receipt.into_effect()?;
                Ok(Some((effect.status, custody)))
            }
            None => Ok(None),
        }
    }

    pub(super) async fn drain_original_process(
        &self,
        entry: &NativeEntry,
        effect: &BoundEffect,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        {
            let handles = entry.handles.lock().await;
            if handles.process_drained.is_some() || handles.drained.is_some() {
                return Ok(());
            }
        }
        let view = self
            .live_view(effect.owner.task_id())
            .ok_or(Error::Indeterminate(effect.command))?;
        let proof = effect
            .provider
            .drain_with_native_work(
                effect.provider.original_attempt(),
                &view,
                work,
                work.cancellation(),
            )?
            .await?;
        let mut handles = entry.handles.lock().await;
        if handles.process_drained.is_none() && handles.drained.is_none() {
            handles.process_drained = Some(proof);
        }
        Ok(())
    }

    /// Authenticates actual process drainage before abandoning its actual lazy drivers.
    pub(crate) async fn cleanup_child(
        &self,
        task: TaskId,
        meter: &NativeMaterializationWork,
    ) -> Result<()> {
        let cancellation = meter.cancellation();
        let entry = self.registered_entry(task)?;
        let effect = entry
            .handles
            .lock()
            .await
            .effect
            .clone()
            .ok_or_else(|| unauthorized("Pi cleanup has no original native attempt"))?;
        self.drain_original_process(&entry, &effect, meter).await?;
        let mut handles = entry.handles.lock().await;
        if !handles.mounts_drained {
            let view = self
                .live_view(task)
                .ok_or(Error::Indeterminate(effect.command))?;
            let original = handles
                .preparation_work
                .as_ref()
                .cloned()
                .ok_or_else(|| unauthorized("Pi cleanup has no original view work"))?;
            effect
                .owner
                .verify(true, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(meter)?)))
                .await?;
            handles.stop_startup_mount(effect.command)?;
            let LiveHandles {
                process_drained,
                drained,
                ..
            } = &mut *handles;
            match view
                .cleanup_mounts_with_native_work(
                    effect.owner.as_ref(),
                    process_drained,
                    drained,
                    &original,
                    meter,
                    cancellation,
                )?
                .await?
            {
                Ok(receipt) => *drained = Some(receipt),
                Err(failure) => {
                    *drained = Some(failure.partial_receipt);
                    return Err(failure.cause);
                }
            }
            handles.mounts_drained = true;
            handles.project = None;
            handles.project_allocation = None;
            *entry
                .view
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        }
        if handles.drained.is_none() {
            return Err(Error::Indeterminate(effect.command));
        }
        effect
            .owner
            .verify(true, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(meter)?)))
            .await?;
        handles
            .project_gate
            .release(&effect.owner, meter, cancellation, effect.command)
            .await?;
        handles
            .scratch_gate
            .release(&effect.owner, meter, cancellation, effect.command)
            .await?;
        Ok(())
    }

    /// Explicit disposal is separate from normal cleanup: it deletes only the original child's private session scratch, under the SDK's opaque creator proof.
    pub(crate) async fn dispose_child_scratch(
        &self,
        task: TaskId,
        meter: &NativeMaterializationWork,
    ) -> Result<()> {
        async {
            self.cleanup_child(task, meter).await?;
            let entry = self
                .find_entry(task)
                .ok_or_else(|| unauthorized("Pi disposal has no original live capabilities"))?;
            let mut handles = entry.handles.lock().await;
            if handles.scratch_released {
                return Ok(());
            }
            let effect = handles
                .effect
                .as_ref()
                .cloned()
                .ok_or_else(|| unauthorized("Pi disposal has no original native attempt"))?;
            let proof = effect.owner.child_scratch_creation_proof()?;
            let LiveHandles {
                root,
                drained,
                preparation_work,
                ..
            } = &mut *handles;
            let root = root.as_mut().ok_or(Error::Indeterminate(effect.command))?;
            let drained = drained
                .as_ref()
                .ok_or(Error::Indeterminate(effect.command))?;
            let original = preparation_work
                .as_ref()
                .ok_or(Error::Indeterminate(effect.command))?;
            root.release_scratch_with_work(
                &effect.owner,
                effect.context.as_ref(),
                effect.command,
                drained,
                &proof,
                original,
                meter,
            )
            .await?;
            handles.scratch_released = true;
            handles.root = None;
            Ok(())
        }
        .await
    }

    /// Cancel and join this host's original run before rolling back an interrupted prelaunch preparation.
    pub(crate) async fn rollback_preparation(
        &self,
        task: TaskId,
        meter: &NativeMaterializationWork,
    ) -> Result<()> {
        async {
            self.child_admission(task, meter).await?;
            self.runtime
                .runtime
                .task_host()
                .cancel(task, Some(meter))
                .await?;
            let entry = self
                .find_entry(task)
                .ok_or_else(|| unauthorized("Pi rollback has no original live preparation"))?;
            entry.operation_cancellation.cancel();
            let _run = entry.run.lock().await;
            self.rollback_preparation_owned(task, &entry, meter, meter.cancellation())
                .await
        }
        .await
    }

    /// The task driver already serializes this invocation.
    pub(super) async fn rollback_preparation_owned(
        &self,
        task: TaskId,
        entry: &NativeEntry,
        meter: &NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        let mut handles = entry.handles.lock().await;
        let (owner, context, command) =
            handles.preparation_owner.as_ref().cloned().ok_or_else(|| {
                unauthorized(
                    "Pi cold rollback cannot adopt original native allocation capabilities",
                )
            })?;
        owner
            .verify(true, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(meter)?)))
            .await?;
        let admission = self.child_admission(task, meter).await?;
        let input = &admission.1;
        let recorded_effect = self
            .original_effect(context.as_ref(), input, command, meter)
            .await?;
        if recorded_effect
            .0
            .as_ref()
            .is_some_and(|effect| effect.status != EffectStatus::Planned)
        {
            return Err(Error::Indeterminate(command));
        }
        if let Some(bound) = &handles.effect
            && bound
                .provider
                .receipt(bound.provider.original_attempt(), meter, None)
                .await?
                .is_some()
        {
            return Err(Error::Indeterminate(command));
        }
        let creator = owner.child_scratch_creation_proof()?;
        creator
            .authorize_settlement_with_native_work(&owner, meter)
            .await?;
        handles.stop_startup_mount(command)?;
        if let Some(project) = handles.project.as_ref() {
            creator
                .authorize_settlement_with_native_work(&owner, meter)
                .await?;
            let volume_work = handles
                .project_work
                .as_ref()
                .ok_or(Error::Indeterminate(command))?;
            project
                .abandon_with_work(&volume_work)
                .map_err(Error::from)?;
            handles.project = None;
        }
        let root = handles.root.as_mut().ok_or(Error::Indeterminate(command))?;
        if let Ok(allocation) = root.witnessed_allocation() {
            if PiNativeLiveView::inspect_preparation(&owner, allocation, meter, false)?
                .await?
                .is_some()
            {
                PiNativeLiveView::inspect_preparation(&owner, allocation, meter, true)?.await?;
            }
        }
        root.rollback(&owner, context.as_ref(), &creator, meter)
            .await?;
        *entry
            .view
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        handles.project_allocation = None;
        handles.root = None;
        handles
            .project_gate
            .release(&owner, meter, cancellation, command)
            .await?;
        handles
            .scratch_gate
            .release(&owner, meter, cancellation, command)
            .await?;
        Ok(())
    }

    pub(super) async fn retained_unstarted_preparation(
        &self,
        entry: &NativeEntry,
        effect: &BoundEffect,
        meter: &NativeMaterializationWork,
    ) -> Result<bool> {
        if entry.handles.lock().await.preparation_owner.is_none() {
            return Ok(false);
        }
        if effect
            .provider
            .receipt(effect.provider.original_attempt(), meter, None)
            .await?
            .is_some()
        {
            return Ok(false);
        }
        let admission = self.child_admission(effect.owner.task_id(), meter).await?;
        let input = &admission.1;
        let recorded_effect = self
            .original_effect(effect.context.as_ref(), input, effect.command, meter)
            .await?;
        Ok(recorded_effect
            .0
            .as_ref()
            .is_none_or(|record| record.status == EffectStatus::Planned))
    }

    /// Settle native resources while this original command still owns its SDK lease.
    pub(super) async fn settle_progress(
        &self,
        task: TaskId,
        progress: TaskCommandProgress,
    ) -> Result<TaskCommandProgress> {
        if matches!(
            &progress,
            TaskCommandProgress::Ready(_) | TaskCommandProgress::ReadyWithNativeCustody { .. }
        ) {
            let entry = self
                .find_entry(task)
                .ok_or_else(|| unauthorized("Pi ready command has no original native handles"))?;
            if entry
                .handles
                .lock()
                .await
                .root
                .as_ref()
                .is_some_and(|root| root.view.is_some())
            {
                let work = entry
                    .handles
                    .lock()
                    .await
                    .preparation_work
                    .as_ref()
                    .cloned()
                    .ok_or_else(|| {
                        unauthorized("Pi ready cleanup has no original invocation ledger")
                    })?;
                self.dispose_child_scratch(task, &work).await?;
            }
        }
        Ok(progress)
    }
}
