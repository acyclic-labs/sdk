use super::*;
use super::receipt_observation::OriginalNativeRequest;
use crate::filesystem::native_execution::NativeVolumeView;
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, NativeAllocation, NativeMaterializationWork, WorkCounters};
use std::any::Any;

/// SDK-issued physical process quiescence for one original accepted invocation.
/// Fields, construction and view matching remain private; this is not a grant,
/// an exhaustive-output claim, or authority to recreate a cold namespace.
pub struct NativeProcessDrainProof {
    view: Arc<dyn Any + Send + Sync>,
    request: OriginalNativeRequest,
    receipt: NativeProcessReceipt,
    plan: Arc<TaskEffectPlan>,
    // Derived by the canonical task/command binding before dispatch or recovery.
    effect_id: EffectId,
    _provider_custody: Arc<Mutex<NativeAllocation>>,
    _custody: NativeAllocation,
}

fn boundary(code: crate::wire::ErrorCode, reason: &'static str) -> Error {
    Error::NativeBoundary { code, reason }
}

fn native(cause: acyclic_fs::NativeTreeMaterializationError) -> Error {
    Error::NativeWork { cause, custody: None }
}

fn same_manifest(
    actual: &NativeViewManifest, expected: &NativeViewManifest,
    work: &NativeMaterializationWork,
) -> Result<bool> {
    work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() }).map_err(native)?;
    if actual.options != expected.options || actual.volumes.len() != expected.volumes.len() {
        return Ok(false);
    }
    for (actual, expected) in actual.volumes.iter().zip(&expected.volumes) {
        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() }).map_err(native)?;
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
        P: StreamProvider + 'static,
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        let held = self.view.as_ref().downcast_ref::<NativeVolumeView<A, O>>()
            .ok_or(boundary(crate::wire::ErrorCode::Conflict, "process drain proof belongs to another physical view type"))?;
        let preparation = view.preparation_receipts().first()
            .ok_or(boundary(crate::wire::ErrorCode::Conflict, "process drain proof requires original preparation evidence"))?;
        if !std::ptr::eq(held, view)
            || !view.original_native_work().is_some_and(|work| work.shares_ledger(original))
            || (self.receipt.task, &self.receipt.original_fence) != owner.task_binding()
            || self.receipt.task != preparation.task
            || self.receipt.original_fence != preparation.original_fence
            || self.receipt.effect.provider != self.plan.provider
            || self.receipt.effect.attempt_id != crate::EffectAttemptId::from_bytes(self.receipt.effect.effect_id.into_bytes())
            || self.request.request().view != preparation.manifest
        {
            return Err(boundary(crate::wire::ErrorCode::Conflict, "process drain proof differs from the original task, fence or physical namespace"));
        }
        Ok(())
    }

    pub(crate) fn require_command_binding(&self, command: OperationId) -> Result<()> {
        if self.receipt.command != command
            || self.receipt.effect.effect_id != self.effect_id
            || self.receipt.effect.attempt_id != crate::EffectAttemptId::from_bytes(self.effect_id.into_bytes())
            || self.receipt.effect.provider != self.plan.provider
        {
            return Err(boundary(crate::wire::ErrorCode::Conflict, "process drain proof differs from its original approved command"));
        }
        Ok(())
    }
}

/// A borrowed SDK control gate. Only the authenticated original provider issues
/// it after matching its held view, task/fence, request and process birth ledger.
/// Cleanup accounting is independent; neither work ledger confers authority.
pub(super) struct NativeCleanupControl<'a> {
    supervisor: &'a NativeSupervisorControl,
    original: &'a NativeMaterializationWork,
    cleanup: &'a NativeMaterializationWork,
}

impl NativeCleanupControl<'_> {
    pub(super) fn original_work_for(
        &self, supervisor: &NativeSupervisorControl, cleanup: &NativeMaterializationWork,
    ) -> Result<&NativeMaterializationWork> {
        if !std::ptr::eq(self.supervisor, supervisor) || !self.cleanup.shares_ledger(cleanup) {
            return Err(boundary(crate::wire::ErrorCode::Conflict, "cleanup control differs from its authenticated original supervisor or span"));
        }
        Ok(self.original)
    }
}

impl<P: StreamProvider + 'static> NativeProcessProvider<P> {
    /// Drains the same owned process engine, or authenticates a physically quiet
    /// retained result. Cold receipt absence/launch-only uncertainty is refused.
    /// The existing prepared view is mandatory; no namespace or PID is adopted.
    pub fn drain_with_native_work<'a, A, O>(
        &'a self, attempt: crate::EffectAttemptId,
        view: &'a Arc<NativeVolumeView<A, O>>,
        work: &'a NativeMaterializationWork,
        cleanup_cancellation: &'a acyclic_fs::CancellationToken,
    ) -> Result<crate::NativeFutureBundle<'a, NativeProcessDrainProof>>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        finite_dispatch::require_finite(work)?;
        let provider_custody = self._custody.as_ref()
            .ok_or(boundary(crate::wire::ErrorCode::Unsupported, "process drain requires original provider payload custody"))?;
        let original = view.original_native_work()
            .ok_or(boundary(crate::wire::ErrorCode::Unsupported, "process drain requires an existing original native view"))?;
        let preparation = view.preparation_receipts().first()
            .ok_or(boundary(crate::wire::ErrorCode::Conflict, "process drain requires original preparation evidence"))?;
        if attempt != self.original_attempt()
            || (preparation.task, &preparation.original_fence) != self.owner.task_binding()
        {
            return Err(boundary(crate::wire::ErrorCode::Conflict, "process drain differs from the original attempt or prepared task fence"));
        }
        if let Some(held) = &self.view {
            if !std::ptr::addr_eq(Arc::as_ptr(held), Arc::as_ptr(view))
                || !self._native_work.as_ref().is_some_and(|work| work.shares_ledger(original))
            {
                return Err(boundary(crate::wire::ErrorCode::Conflict, "process drain must retain the same originally dispatched physical view"));
            }
        }
        if self.view.is_none()
            && (!self.authority.content.supports_native_read(self.plan.request.volume(), work)?
                || !self.authority.result_content.supports_native_read(self.authority.results.volume(), work)?)
        {
            return Err(boundary(crate::wire::ErrorCode::Unsupported,
                "cold process drain requires actual finite request and result readers"));
        }
        crate::NativeFutureBundle::new(work, || async move {
            let custody = work.retain_owned(std::mem::size_of::<NativeProcessDrainProof>() as u64).map_err(native)?;
            cleanup_cancellation.check().map_err(acyclic_fs::NativeTreeMaterializationError::from).map_err(native)?;
            self.authenticate_cleanup_owner(work).await?;
            let request = self.original_request_with_native_work(work).await?;
            if !same_manifest(&request.request().view,
                NativeProcessView::<P>::manifest(view.as_ref()), work)?
            {
                return Err(boundary(crate::wire::ErrorCode::Conflict, "process drain request differs from the existing original namespace"));
            }
            let phase = match &self.view {
                Some(_) => {
                    let control = NativeCleanupControl { supervisor: &self.supervisor, original, cleanup: work };
                    Some(self.wait_for_original_collection(attempt, work, cleanup_cancellation, Some(&control))
                        .await.map_err(NativeReceiptError::into_root_error)?)
                }
                None => None,
            };
            let receipt = self.approved_original_receipt_with_native_work(
                attempt, request.request(), work,
            ).await?;
            if (receipt.task, &receipt.original_fence) != self.owner.task_binding()
                || receipt.command != self.command
            {
                return Err(boundary(crate::wire::ErrorCode::Conflict, "process drain receipt differs from its original task fence or command"));
            }
            match phase {
                Some(NativeCapturePhase::Pending | NativeCapturePhase::Drained | NativeCapturePhase::Recorded) => {},
                Some(_) => return Err(Error::Indeterminate(self.command)),
                None => {
                    if !receipt.observed { return Err(Error::Indeterminate(self.command)); }
                    let reference = match &receipt.effect.status {
                        EffectStatus::Succeeded { result } => Some(result),
                        _ => receipt.diagnostic.as_ref(),
                    }.ok_or(Error::Indeterminate(self.command))?;
                    if reference.volume() != self.authority.results.volume()
                        || reference.descriptor().byte_length() > u64::from(request.request().maximum_result_bytes)
                        || reference.descriptor().media_type() != "application/json"
                    {
                        return Err(boundary(crate::wire::ErrorCode::Conflict, "process drain result differs from its original owner, bound or content type"));
                    }
                    self.owner.require_volume_grant_with_native_work(reference.volume(),
                        crate::conversation::VolumeOperation::Read, work)?;
                    let bytes = self.authority.result_content.read_with_native_work(
                        reference, u64::from(request.request().maximum_result_bytes), work,
                    )?.await?;
                    let (result, custody) = NativeProcessResult::decode_with_native_work(bytes.as_ref(), work)?;
                    let custody = custody;
                    let result = result;
                    if result.stop.as_ref().is_some_and(|stop| !stop.cleanup_completed) {
                        return Err(Error::Indeterminate(self.command));
                    }
                    drop(result);
                    drop(custody);
                    drop(bytes);
                }
            }
            cleanup_cancellation.check().map_err(acyclic_fs::NativeTreeMaterializationError::from).map_err(native)?;
            self.authenticate_cleanup_owner(work).await?;
            let held_view: Arc<dyn Any + Send + Sync> = view.clone();
            let proof = NativeProcessDrainProof {
                view: held_view, request, receipt, plan: Arc::clone(&self.plan),
                effect_id: self.effect_id,
                _provider_custody: Arc::clone(provider_custody), _custody: custody,
            };
            proof.require_view_binding::<P, A, O>(
                view.as_ref(), &self.owner, original,
            )?;
            proof.require_command_binding(self.command)?;
            Ok(proof)
        })
    }

    pub(super) async fn authenticate_cleanup_owner(&self, work: &NativeMaterializationWork) -> Result<()> {
        let (outcome, custody) = self.owner.outcome_with_native_work(work).await?;
        let custody = custody;
        let outcome = outcome;
        if outcome.is_none() { self.owner.verify_with_native_work(true, work).await?; }
        drop(outcome);
        drop(custody);
        Ok(())
    }
}

