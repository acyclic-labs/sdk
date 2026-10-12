//! Approved one-shot effects in the task's fenced Stream.
use super::native_execution::boundary;
use super::{NATIVE_PROCESS_EFFECT_KIND, NativeProcessRequest, NativeProcessResult,
    NativeProcessStop, NativeProcessStopKind, NativeProcessView, NativePublicationFailure, NativeViewManifest};
use crate::{Error, InteractionId, OperationId, Result,
    conversation::{ContentPublisher, ContentResidencyVerifier},
    core::{AuthorityVerifier, EffectGuarantee, EffectStatus}, distributed::JournalWrite,
    durable_host::TaskJournalOwner, effect_host::TaskEffectPlan,
    effects::{EffectDispatch, EffectObservation, EffectProvider},
    store::{HistoryReadLimits, HistoryReader}, tool::ToolApprovalVerifier};
use acyclic_fs::{NativeAllocation, NativeMaterializationWork, WorkCounters};
use acyclic_stream::{BoxProviderFuture, StreamError, StreamPath, StreamProvider};
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, collections::BTreeSet, sync::{Arc, Mutex}, time::Duration};

mod drain;
mod finite_binding;
mod finite_dispatch;
mod finite_execution;
mod publication_observation;
mod receipt_observation;
mod records;
mod supervisor;
mod validation;
pub use drain::NativeProcessDrainProof;
pub use records::{NativeProcessPublishedVolume, NativeProcessReceipt};
use records::{LaunchReceipt, NativeObservation, Receipt};
use supervisor::{NativeSupervisorCompletion, NativeSupervisorControl};
use finite_execution::execute;

/// Capabilities for authenticating admission and retaining results.
pub struct NativeProcessAuthority {
    /// Original conversation verifier.
    pub verifier: AuthorityVerifier,
    /// Finite authenticated history bounds.
    pub history_limits: HistoryReadLimits,
    /// Immutable request and schema reader.
    pub content: Arc<dyn ContentResidencyVerifier>,
    /// Authenticated result reader.
    pub result_content: Arc<dyn ContentResidencyVerifier>,
    /// Result publisher.
    pub results: Arc<dyn ContentPublisher>,
    /// Resolved approval verifier.
    pub approvals: Arc<dyn ToolApprovalVerifier>,
    /// Allocation of operation-owned authority data.
    pub authority_custody: Option<NativeAllocation>,
}

/// One retained command and its process supervisor.
pub struct NativeProcessProvider<P: StreamProvider> {
    owner: Arc<TaskJournalOwner<P>>,
    command: OperationId,
    plan: Arc<TaskEffectPlan>,
    invocation: Option<Arc<Invocation<P>>>,
    approval: InteractionId,
    authority: Arc<NativeProcessAuthority>,
    supervisor: Arc<NativeSupervisorControl>,
    effect_id: crate::EffectId,
    history: Arc<HistoryReader<P>>,
    _custody: Arc<NativeAllocation>,
    _arc_custody: Option<NativeAllocation>,
}

struct Invocation<P: StreamProvider> {
    view: Arc<dyn NativeProcessView<P>>,
    request: NativeProcessRequest,
    result_schema: serde_json::Value,
    approval_digest: [u8; 32],
}

fn examine(work: &NativeMaterializationWork, compare: impl FnOnce() -> bool) -> Result<bool> {
    work.charge(WorkCounters { items_examined: 1, ..Default::default() })?;
    Ok(compare())
}
fn receipt_backend(source: StreamError) -> Error {
    Error::NativeStream { code: crate::wire::ErrorCode::Storage, source, custody: None }
}

impl<P: StreamProvider> NativeProcessProvider<P> {
    /// Original one-shot effect attempt.
    #[must_use]
    pub fn original_attempt(&self) -> crate::EffectAttemptId {
        crate::EffectAttemptId::from_bytes(self.effect_id.into_bytes())
    }

    /// Signals the original owned process supervisor.
    pub async fn signal_stop(&self, attempt: crate::EffectAttemptId,
        work: &NativeMaterializationWork, cancellation: &acyclic_fs::CancellationToken) -> Result<()> {
        finite_dispatch::require_finite(work)?;
        crate::NativeFutureBundle::new(work, || async {
            cancellation.check().map_err(|_| acyclic_fs::NativeTreeMaterializationError::Cancelled)?;
            work.charge(WorkCounters { items_examined: 1, ..Default::default() })?;
            let (completed, _) = self.supervisor.snapshot(attempt)?;
            self.authenticate_cleanup_owner(work).await?;
            cancellation.check().map_err(|_| acyclic_fs::NativeTreeMaterializationError::Cancelled)?;
            if completed { return self.supervisor.settle_process(self._custody.work(), work); }
            work.charge(WorkCounters { items_examined: 1, ..Default::default() })?;
            let mut state = self.supervisor.active.lock()
                .map_err(|_| boundary(crate::wire::ErrorCode::Storage, "original supervisor control is poisoned"))?;
            let state = state.as_mut().ok_or(boundary(crate::wire::ErrorCode::Storage, "original supervisor disappeared"))?;
            if !state.completed { state.stop_requested = true; }
            Ok(())
        })?.await
    }

    async fn wait_for_original_collection(&self, attempt: crate::EffectAttemptId,
        work: &NativeMaterializationWork, cancellation: &acyclic_fs::CancellationToken) -> Result<bool> {
        self.signal_stop(attempt, work, cancellation).await?;
        let interval = Duration::from_millis(u64::from(self.invocation()?.request.cancellation_poll_ms));
        loop {
            let notification = self.supervisor.completion.notified();
            tokio::pin!(notification);
            notification.as_mut().enable();
            cancellation.check().map_err(|_| acyclic_fs::NativeTreeMaterializationError::Cancelled)?;
            work.charge(WorkCounters { items_examined: 1, ..Default::default() })?;
            let (completed, quiescent) = self.supervisor.snapshot(attempt)?;
            if completed {
                if !quiescent {
                    self.authenticate_cleanup_owner(work).await?;
                    self.supervisor.settle_process(self._custody.work(), work)?;
                }
                return Ok(self.supervisor.snapshot(attempt)?.1);
            }
            tokio::select! {
                () = &mut notification => {},
                () = tokio::time::sleep(interval) => {},
            }
        }
    }

    /// Waits for physical collection and the durable observation.
    pub async fn request_stop(&self, attempt: crate::EffectAttemptId,
        work: &NativeMaterializationWork, cancellation: &acyclic_fs::CancellationToken) -> Result<NativeProcessReceipt> {
        finite_dispatch::require_finite(work)?;
        crate::NativeFutureBundle::new(work, || async {
            let quiescent = self.wait_for_original_collection(attempt, work, cancellation).await?;
            let receipt = self.receipt(attempt, work, None).await?
                .ok_or(boundary(crate::wire::ErrorCode::Storage, "original supervisor has no launch receipt"))?;
            if !receipt.observed || !quiescent {
                return Err(boundary(crate::wire::ErrorCode::Storage, "original process collection or observation remains uncertain"));
            }
            Ok(receipt)
        })?.await
    }

    /// Reads authenticated output with its retained allocation.
    pub async fn output(&self, attempt: crate::EffectAttemptId, work: &NativeMaterializationWork)
        -> Result<Option<(crate::conversation::FileRef, NativeAllocation)>> {
        let invocation = self.invocation()?;
        let Some(receipt) = self.receipt(attempt, work, None).await? else { return Ok(None); };
        if !receipt.observed { return Ok(None); }
        let NativeProcessReceipt { effect, diagnostic, original_fence, publication_failure,
            invocation_failure, published, _custody: mut custody, .. } = receipt;
        drop((original_fence, publication_failure, invocation_failure, published));
        let EffectObservation { provider, status, .. } = effect;
        drop(provider);
        let reference = match (status, diagnostic) {
            (EffectStatus::Succeeded { result }, _) => result,
            (_, Some(reference)) => reference,
            (_, None) => return Ok(None),
        };
        let io = super::PreparationIo::from_native_work(work)?;
        self.owner.validate_input_file(&reference, Some(&io))?;
        let maximum = u64::from(invocation.request.maximum_result_bytes);
        if reference.volume() != self.authority.results.volume() || reference.descriptor().byte_length() > maximum {
            return Err(boundary(crate::wire::ErrorCode::Unauthorized, "native output differs from admitted owner or bound"));
        }
        let bytes = self.authority.result_content.read(&reference, maximum, Some(&io))?.await?;
        let value = io.decode(std::marker::PhantomData::<serde_json::Value>, &bytes, crate::wire::ErrorCode::Invalid)?;
        crate::contract::validate_json_schema_value(&invocation.result_schema, &value.0, "native process result", Some(&io))?;
        drop(value);
        drop(bytes);
        drop(io);
        custody.substitute(reference.logical_owned_bytes()?)?;
        Ok(Some((reference, custody)))
    }

    /// Funds shared provider storage on the original ledger.
    pub fn into_arc_with_native_work(mut self, work: &NativeMaterializationWork) -> Result<Arc<Self>> {
        finite_binding::require_family(self._custody.work(), work)?;
        let bytes = std::mem::size_of::<Self>() as u64;
        self._arc_custody = Some(work.claim(bytes, u64::from(bytes != 0), WorkCounters::UNCHARGED)?);
        self._custody.work().remaining()?;
        Ok(Arc::new(self))
    }
    fn invocation(&self) -> Result<&Arc<Invocation<P>>> {
        self.invocation.as_ref().ok_or(boundary(crate::wire::ErrorCode::Unsupported, "receipt-only recovery owns no executable invocation"))
    }
}

impl<P: StreamProvider> EffectProvider for NativeProcessProvider<P> {
    fn id(&self) -> &str { &self.plan.provider }
    fn guarantees(&self, kind: &str) -> BTreeSet<EffectGuarantee> {
        if kind == NATIVE_PROCESS_EFFECT_KIND { BTreeSet::from([EffectGuarantee::AtMostOnce]) } else { BTreeSet::new() }
    }
    fn supports_guarantee_with_native_work(&self, kind: &str, guarantee: EffectGuarantee,
        work: &NativeMaterializationWork) -> Result<bool> {
        finite_binding::require_family(self._custody.work(), work)?;
        work.charge(WorkCounters { items_examined: 1, ..Default::default() })?;
        Ok(kind == NATIVE_PROCESS_EFFECT_KIND && guarantee == EffectGuarantee::AtMostOnce)
    }
    fn linearizable_reconciliation(&self) -> bool { false }
    fn dispatch<'a>(&'a self, _: EffectDispatch) -> BoxProviderFuture<'a, Result<EffectObservation>> {
        Box::pin(std::future::ready(Err(boundary(crate::wire::ErrorCode::Storage, "native dispatch requires original work"))))
    }
    fn reconcile<'a>(&'a self, _: crate::EffectAttemptId) -> BoxProviderFuture<'a, Result<Option<EffectObservation>>> {
        Box::pin(std::future::ready(Err(boundary(crate::wire::ErrorCode::Storage, "native reconciliation requires original work"))))
    }
    fn dispatch_with_native_work<'a>(&'a self, dispatch: EffectDispatch, work: &'a NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, (EffectObservation, NativeAllocation)>> {
        finite_dispatch::require_finite(work)?;
        finite_binding::require_family(self._custody.work(), work)?;
        self.invocation()?.view.operation_cancellation().check()
            .map_err(|_| acyclic_fs::NativeTreeMaterializationError::Cancelled)?;
        let bytes = receipt_observation::add(receipt_observation::add(dispatch.provider.capacity() as u64,
            dispatch.effect_kind.capacity() as u64)?, dispatch.request.logical_owned_bytes()?)?;
        let custody = work.claim(bytes, 0, WorkCounters::UNCHARGED)?;
        crate::NativeFutureBundle::new(work, || self.launch((dispatch, custody), work))
    }
    fn reconcile_with_native_work<'a>(&'a self, attempt: crate::EffectAttemptId, work: &'a NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, Option<(EffectObservation, NativeAllocation)>>> {
        finite_dispatch::require_finite(work)?;
        finite_binding::require_family(self._custody.work(), work)?;
        crate::NativeFutureBundle::new(work, move || async move {
            self.receipt(attempt, work, None).await?.map(NativeProcessReceipt::into_effect).transpose()
        })
    }
}
