use super::*;
use crate::filesystem::native_execution::NativeVolumeView;
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, NativeMaterializationWork, WorkCounters};
use std::any::Any;

/// SDK-issued physical process quiescence for one original accepted invocation.
pub struct NativeProcessDrainProof {
    view: Arc<dyn Any + Send + Sync>,
    receipt: NativeProcessReceipt,
}

fn same_manifest(
    actual: &NativeViewManifest, expected: &NativeViewManifest,
    work: &NativeMaterializationWork,
) -> Result<bool> {
    work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() }).map_err(Error::from)?;
    if actual.options != expected.options || actual.volumes.len() != expected.volumes.len() {
        return Ok(false);
    }
    for (actual, expected) in actual.volumes.iter().zip(&expected.volumes) {
        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() }).map_err(Error::from)?;
        if actual != expected { return Ok(false); }
    }
    Ok(true)
}

impl NativeProcessDrainProof {
    pub(crate) fn require_view_binding<P, A, O>(
        &self, view: &NativeVolumeView<A, O>, owner: &TaskJournalOwner<P>,
        original: &NativeMaterializationWork,
    ) -> Result<()>
    where
        P: StreamProvider,
        A: AsyncAuthorityStore + 'static,
        O: AsyncObjectStore + 'static,
    {
        let held = self.view.as_ref().downcast_ref::<NativeVolumeView<A, O>>()
            .ok_or(boundary(crate::wire::ErrorCode::Conflict, "process drain proof belongs to another physical view type"))?;
        let preparation = view.preparation();
        if !std::ptr::eq(held, view)
            || !view.work().shares_ledger(original)
            || (self.receipt.task, &self.receipt.original_fence) != owner.task_binding()
            || self.receipt.task != preparation.task
            || self.receipt.original_fence != preparation.original_fence
            || self.receipt.effect.attempt_id != crate::EffectAttemptId::from_bytes(self.receipt.effect.effect_id.into_bytes())
        {
            return Err(boundary(crate::wire::ErrorCode::Conflict, "process drain proof differs from the original task, fence or physical namespace"));
        }
        Ok(())
    }

    pub(crate) fn require_command_binding(&self, command: OperationId) -> Result<()> {
        if self.receipt.command != command
            || self.receipt.effect.attempt_id != crate::EffectAttemptId::from_bytes(self.receipt.effect.effect_id.into_bytes())
        {
            return Err(boundary(crate::wire::ErrorCode::Conflict, "process drain proof differs from its original approved command"));
        }
        Ok(())
    }
}

impl<P: StreamProvider> NativeProcessProvider<P> {
    /// Drains the same owned process engine, or authenticates a physically quiet retained result.
    pub fn drain_with_native_work<'a, A, O>(
        &'a self, attempt: crate::EffectAttemptId,
        view: &'a Arc<NativeVolumeView<A, O>>,
        work: &'a NativeMaterializationWork,
        cleanup_cancellation: &'a acyclic_fs::CancellationToken,
    ) -> Result<crate::NativeFutureBundle<'a, NativeProcessDrainProof>>
    where
        A: AsyncAuthorityStore + 'static,
        O: AsyncObjectStore + 'static,
    {
        finite_dispatch::require_finite(work)?;
        let original = view.work();
        let preparation = view.preparation();
        if attempt != self.original_attempt()
            || (preparation.task, &preparation.original_fence) != self.owner.task_binding()
        {
            return Err(boundary(crate::wire::ErrorCode::Conflict, "process drain differs from the original attempt or prepared task fence"));
        }
        if let Some(invocation) = &self.invocation {
            if !std::ptr::addr_eq(Arc::as_ptr(&invocation.view), Arc::as_ptr(view))
                || !self._custody.work().shares_ledger(original)
            {
                return Err(boundary(crate::wire::ErrorCode::Conflict, "process drain must retain the same originally dispatched physical view"));
            }
        }
        if self.invocation.is_none()
            && (!self.authority.content.supports_read(self.plan.request.volume(), Some(&crate::filesystem::PreparationIo::from_native_work(work)?))?
                || !self.authority.result_content.supports_read(self.authority.results.volume(), Some(&crate::filesystem::PreparationIo::from_native_work(work)?))?)
        {
            return Err(boundary(crate::wire::ErrorCode::Unsupported,
                "cold process drain requires actual finite request and result readers"));
        }
        crate::NativeFutureBundle::new(work, || async move {
            let mut custody = work.claim(std::mem::size_of::<NativeProcessDrainProof>() as u64, 0, acyclic_fs::WorkCounters::UNCHARGED).map_err(Error::from)?;
            cleanup_cancellation.check().map_err(acyclic_fs::NativeTreeMaterializationError::from).map_err(Error::from)?;
            self.authenticate_cleanup_owner(work).await?;
            let (request, request_custody) = self.request(work).await?;
            if !same_manifest(&request.as_ref().view,
                NativeProcessView::<P>::manifest(view.as_ref()), work)?
            {
                return Err(boundary(crate::wire::ErrorCode::Conflict, "process drain request differs from the existing original namespace"));
            }
            let quiescent = match &self.invocation {
                Some(_) => {
                    Some(self.wait_for_original_collection(attempt, work, cleanup_cancellation)
                        .await?)
                }
                None => None,
            };
            let mut receipt = self.approved_receipt(
                attempt, request.as_ref(), work,
            ).await?;
            if (receipt.task, &receipt.original_fence) != self.owner.task_binding()
                || receipt.command != self.command
            {
                return Err(boundary(crate::wire::ErrorCode::Conflict, "process drain receipt differs from its original task fence or command"));
            }
            match quiescent {
                Some(true) => {},
                Some(false) => return Err(Error::Indeterminate(self.command)),
                None => {
                    if !receipt.observed { return Err(Error::Indeterminate(self.command)); }
                    let reference = match &receipt.effect.status {
                        EffectStatus::Succeeded { result } => Some(result),
                        _ => receipt.diagnostic.as_ref(),
                    }.ok_or(Error::Indeterminate(self.command))?;
                    if reference.volume() != self.authority.results.volume()
                        || reference.descriptor().byte_length() > u64::from(request.as_ref().maximum_result_bytes)
                        || reference.descriptor().media_type() != "application/json"
                    {
                        return Err(boundary(crate::wire::ErrorCode::Conflict, "process drain result differs from its original owner, bound or content type"));
                    }
                    self.owner.require_volume_grant_with_native_work(reference.volume(),
                        crate::conversation::VolumeOperation::Read, work)?;
                    let bytes = self.authority.result_content.read(reference, u64::from(request.as_ref().maximum_result_bytes), Some(&crate::filesystem::PreparationIo::from_native_work(work)?))?.await?;
                    let (result, custody) = NativeProcessResult::decode_with_native_work(bytes.as_ref(), work)?;
                    if result.stop.as_ref().is_some_and(|stop| !stop.cleanup_completed) {
                        return Err(Error::Indeterminate(self.command));
                    }
                    drop(result);
                    drop(custody);
                    drop(bytes);
                }
            }
            cleanup_cancellation.check().map_err(acyclic_fs::NativeTreeMaterializationError::from).map_err(Error::from)?;
            self.authenticate_cleanup_owner(work).await?;
            drop((request, request_custody));
            let held_view: Arc<dyn Any + Send + Sync> = view.clone();
            receipt._custody.absorb(&mut custody)?;
            let proof = NativeProcessDrainProof { view: held_view, receipt };
            proof.require_view_binding::<P, A, O>(
                view.as_ref(), &self.owner, original,
            )?;
            proof.require_command_binding(self.command)?;
            Ok(proof)
        })
    }

    pub(super) async fn authenticate_cleanup_owner(&self, work: &NativeMaterializationWork) -> Result<()> {
        let (outcome, custody) = self.owner.outcome_with_native_work(work).await?;
        if outcome.is_none() { self.owner.verify_with_native_work(true, work).await?; }
        drop(outcome);
        drop(custody);
        Ok(())
    }
}

