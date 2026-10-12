//! Approved one-shot process effects, using the task's existing fenced Stream.

use super::native_execution::boundary;
use super::{
    NATIVE_PROCESS_EFFECT_KIND, NativeProcessRequest, NativeProcessResult, NativeProcessStop,
    NativeProcessStopKind, NativeProcessView, NativePublicationFailure, NativeViewManifest,
};
use crate::{
    Error, InteractionId, OperationId, Result,
    conversation::{ContentPublisher, ContentResidencyVerifier},
    core::{AuthorityVerifier, EffectGuarantee, EffectStatus},
    distributed::JournalWrite,
    durable_host::TaskJournalOwner,
    effect_host::TaskEffectPlan,
    effects::{EffectDispatch, EffectObservation, EffectProvider},
    store::{HistoryReadLimits, HistoryReader},
    tool::ToolApprovalVerifier,
};
use acyclic_stream::{BoxProviderFuture, StreamError, StreamPath, StreamProvider};
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    collections::BTreeSet,
    sync::{Arc, Mutex},
    time::Duration,
};

mod receipt_observation;
mod finite_dispatch;
mod finite_execution;
mod finite_binding;
mod drain;
mod publication_observation;
pub use drain::NativeProcessDrainProof;
use finite_execution::execute;

impl NativeProcessRequest {
    fn validate(&self, view: &NativeViewManifest, io: &super::PreparationIo) -> Result<()> {
        use super::native_contract::validation_error;
        if self.standard_input.is_some() && self.mcp_stdio.is_some() {
            return Err(validation_error(Some(io), crate::wire::ErrorCode::Invalid,
                "literal stdin and MCP exchange are mutually exclusive"));
        }
        if let Some(exchange) = &self.mcp_stdio {
            let work = io.original_native_work().ok_or(boundary(crate::wire::ErrorCode::Storage, "native invocation requires its original work ledger"))?;
            let (exchange, first, custody) = exchange.exchange(Some(work))?;
            drop((exchange, first, custody));
        }
        {
            let items = self.environment.len().checked_add(self.argv.len())
                .and_then(|items| items.checked_add(self.cwd.components().count()))
                .ok_or_else(|| validation_error(Some(io), crate::wire::ErrorCode::Invalid,
                    "native invocation item count overflow"))?;
            io.charge(acyclic_fs::WorkCounters { items_examined: items as u64, ..Default::default() })?;
        }
        if !self.executable.is_absolute()
            || !self.cwd.is_absolute()
            || !self.view.options.root.is_absolute()
            || !self.cwd.strip_prefix(&self.view.options.root).is_ok_and(|relative|
                relative.components().all(|part| matches!(part, std::path::Component::Normal(_))))
            || !self.has_valid_capture_allowances()
            || self.environment.iter().any(|(key, value)| {
                key.is_empty() || key.contains(['=', '\0']) || value.contains('\0')
            })
            || self.argv.iter().any(|arg| arg.contains('\0'))
        {
            return Err(validation_error(Some(io), crate::wire::ErrorCode::Invalid,
                "invalid or changed native process invocation"));
        }
        #[cfg(windows)]
        for (index, key) in self.environment.keys().enumerate() {
            for prior in self.environment.keys().take(index) {
                io.charge(acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() })?;
                if key.chars().flat_map(char::to_uppercase)
                    .eq(prior.chars().flat_map(char::to_uppercase)) {
                    return Err(validation_error(Some(io), crate::wire::ErrorCode::Invalid,
                        "duplicate native environment key under Windows case folding"));
                }
            }
        }
        if &self.view != view {
            return Err(validation_error(Some(io), crate::wire::ErrorCode::Conflict,
                "native command view changed"));
        }
        Ok(())
    }
}

/// Host-only capabilities for checking committed admission and staging results.
pub struct NativeProcessAuthority {
    /// Original conversation authority verifier.
    pub verifier: AuthorityVerifier,
    /// Explicit finite allowance for authenticated exact effect history.
    pub history_limits: HistoryReadLimits,
    /// Owner-authenticated immutable request/schema reader.
    pub content: Arc<dyn ContentResidencyVerifier>,
    /// Independently authenticated result reader, including unmounted results.
    pub result_content: Arc<dyn ContentResidencyVerifier>,
    /// Owner-bound result publisher; it retains the result before returning.
    pub results: Arc<dyn ContentPublisher>,
    /// Existing resolved-approval ledger verifier.
    pub approvals: Arc<dyn ToolApprovalVerifier>,
    /// Original custody of operation-owned authority data.
    pub authority_custody: Option<acyclic_fs::NativeAllocation>,
}

/// Optional provider for one retained task command.
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
    _custody: Arc<acyclic_fs::NativeAllocation>,
    _arc_custody: Option<acyclic_fs::NativeAllocation>,
}

struct Invocation<P: StreamProvider> {
    view: Arc<dyn NativeProcessView<P>>,
    request: NativeProcessRequest,
    result_schema: serde_json::Value,
    approval_digest: [u8; 32],
}

struct NativeActiveAttempt {
    attempt: crate::EffectAttemptId,
    stop_requested: bool,
    quiescent: bool,
    completed: bool,
    process: Option<(acyclic_native_runtime::ProcessTree,
        Vec<acyclic_fs::native_mount::OwnedNativeMountOriginalWorkScope>, acyclic_fs::NativeAllocation)>,
}

struct NativeSupervisorControl {
    active: Mutex<Option<NativeActiveAttempt>>,
    completion: tokio::sync::Notify,
}

impl NativeSupervisorControl {
    fn settle_process(
        &self, original: &acyclic_fs::NativeMaterializationWork,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        let mut state = self.active.lock().map_err(|_| boundary(crate::wire::ErrorCode::Storage, "original supervisor process custody is poisoned"))?;
        let Some(active) = state.as_mut() else { return Ok(()); };
        if let Some(retained) = active.process.as_mut() {
            if !retained.2.work().shares_ledger(original) {
                return Err(boundary(crate::wire::ErrorCode::Conflict, "original supervisor tree belongs to another work family"));
            }
            retained.0.terminate(Some(work.runtime_meter().as_ref()))
                .map_err(|error| Error::NativeWork {
                    cause: work.runtime_error(error), custody: None })?;
        } else {
            return Ok(());
        }
        active.process.take();
        active.quiescent = true;
        Ok(())
    }

    fn begin(&self, attempt: crate::EffectAttemptId) -> Result<()> {
        let mut active = self.active.lock()
            .map_err(|_| boundary(crate::wire::ErrorCode::Storage, "native supervisor control was poisoned"))?;
        if active.is_some() {
            return Err(boundary(crate::wire::ErrorCode::Storage, "native provider already owns an original supervisor"));
        }
        *active = Some(NativeActiveAttempt {
            attempt, stop_requested: false,
            quiescent: true, completed: false, process: None,
        });
        Ok(())
    }

    fn snapshot(
        &self, attempt: crate::EffectAttemptId,
    ) -> Result<(bool, bool)> {
        let active = self.active.lock()
            .map_err(|_| boundary(crate::wire::ErrorCode::Storage, "original supervisor control is poisoned"))?;
        let active = active.as_ref()
            .ok_or(boundary(crate::wire::ErrorCode::Storage, "original owned supervisor is unavailable; launch intent cannot hydrate process control"))?;
        if active.attempt != attempt {
            return Err(boundary(crate::wire::ErrorCode::Storage, "stop attempt differs from the original owned supervisor"));
        }
        Ok((active.completed, active.quiescent))
    }

    fn record_quiescence(&self, quiescent: bool) -> std::io::Result<()> {
        let mut active = self.active.lock()
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::Other))?;
        active.as_mut().ok_or_else(|| std::io::Error::from(std::io::ErrorKind::Other))?.quiescent = quiescent;
        Ok(())
    }

    fn poll_stop(&self) -> std::io::Result<()> {
        let active = self.active.lock()
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::Other))?;
        let active = active.as_ref().ok_or_else(|| std::io::Error::from(std::io::ErrorKind::Other))?;
        if active.stop_requested {
            return Err(std::io::ErrorKind::Interrupted.into());
        }
        Ok(())
    }
}

struct NativeSupervisorCompletion(Arc<NativeSupervisorControl>);

impl Drop for NativeSupervisorCompletion {
    fn drop(&mut self) {
        if let Ok(mut active) = self.0.active.lock()
            && let Some(active) = active.as_mut()
        {
            active.completed = true;
        }
        self.0.completion.notify_waiters();
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LaunchReceipt<'a> {
    dispatch: Cow<'a, EffectDispatch>,
    plan: Cow<'a, TaskEffectPlan>,
    conversation: Cow<'a, crate::core::Authority>,
    task: crate::TaskId,
    command: OperationId,
    fence: Cow<'a, crate::scheduler::LeaseFence>,
    approval: InteractionId,
    approval_digest: [u8; 32],
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Receipt<L = LaunchReceipt<'static>, O = NativeObservation> {
    Launch(L),
    Observed(O),
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeObservation {
    effect: EffectObservation,
    diagnostic: Option<crate::conversation::FileRef>,
    publication_failure: Option<NativePublicationFailure>,
    invocation_failure: Option<String>,
    #[serde(default)]
    published: Vec<NativeProcessPublishedVolume>,
    work: acyclic_fs::WorkCounters,
}

fn examine(work: &acyclic_fs::NativeMaterializationWork, compare: impl FnOnce() -> bool) -> Result<bool> {
    work.charge(acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() })?;
    Ok(compare())
}

fn receipt_backend(source: StreamError) -> Error {
    Error::NativeStream { code: crate::wire::ErrorCode::Storage, source, custody: None }
}

/// Actual SDK-issued final snapshot data for one original approved mapping.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeProcessPublishedVolume {
    /// Exact index in the original immutable request's view manifest.
    pub index: usize,
    /// Full SDK qualification and actual retained DATA operation, when changed.
    pub snapshot: acyclic_fs::NativePublicationSnapshot,
}

/// Authenticated original attempt and its durable native diagnostic.
#[derive(Debug, Serialize)]
pub struct NativeProcessReceipt {
    /// Original task, never reconstructed from native paths.
    pub task: crate::TaskId,
    /// Original admitted command.
    pub command: OperationId,
    /// Creation/dispatch fence; legitimate recovery verifies its current fence separately.
    pub original_fence: crate::scheduler::LeaseFence,
    /// Retained effect status, including a launch-only indeterminate outcome.
    pub effect: EffectObservation,
    /// Whether process observation durably committed.
    pub observed: bool,
    /// Bounded OS output/cleanup artifact, when retained.
    pub diagnostic: Option<crate::conversation::FileRef>,
    /// Exact destination failure and already completed independent publications.
    pub publication_failure: Option<NativePublicationFailure>,
    /// Original capture/staging failure, not silently flattened into uncertainty.
    pub invocation_failure: Option<String>,
    /// Actual final SDK snapshot data committed in the original observation.
    pub published: Vec<NativeProcessPublishedVolume>,
    /// Selected native view's exact filesystem capture/publication work.
    pub work: acyclic_fs::WorkCounters,
    #[serde(skip)]
    _custody: acyclic_fs::NativeAllocation,
}

impl NativeProcessReceipt {
    /// Moves the observed effect together with its original measured payload custody.
    pub fn into_effect(
        self,
    ) -> Result<(EffectObservation, acyclic_fs::NativeAllocation)> {
        let Self { original_fence, effect, diagnostic, publication_failure,
            invocation_failure, published, _custody, .. } = self;
        drop((original_fence, diagnostic, publication_failure, invocation_failure, published));
        let mut custody = _custody;
        custody.substitute(receipt_observation::effect_bytes(&effect)?)?;
        Ok((effect, custody))
    }
}

impl<P> NativeProcessProvider<P>
where
    P: StreamProvider,
{

    /// Original one-shot task-effect attempt selected by the existing effect engine.
    #[must_use]
    pub fn original_attempt(&self) -> crate::EffectAttemptId {
        crate::EffectAttemptId::from_bytes(self.effect_id.into_bytes())
    }

    /// Signal only this original owned supervisor.
    pub async fn signal_stop(
        &self, attempt: crate::EffectAttemptId,
        work: &acyclic_fs::NativeMaterializationWork,
        cleanup_cancellation: &acyclic_fs::CancellationToken,
    ) -> Result<()> {
        if !work.remaining()?.has_finite_allowances() {
            return Err(boundary(crate::wire::ErrorCode::Storage, "native stop requires a finite original work allowance"));
        }
        crate::NativeFutureBundle::new(work, ||
            self.signal_stop_original(attempt, work, cleanup_cancellation))?.await
    }

    async fn signal_stop_original(
        &self, attempt: crate::EffectAttemptId,
        work: &acyclic_fs::NativeMaterializationWork,
        cleanup_cancellation: &acyclic_fs::CancellationToken,
    ) -> Result<()> {
        cleanup_cancellation.check().map_err(acyclic_fs::NativeTreeMaterializationError::from)?;
        work.charge(acyclic_fs::WorkCounters {
            items_examined: 1, ..acyclic_fs::WorkCounters::default()
        })?;
        let (completed, _) = self.supervisor.snapshot(attempt)?;
        self.authenticate_cleanup_owner(work).await?;
        cleanup_cancellation.check().map_err(acyclic_fs::NativeTreeMaterializationError::from)?;
        if completed {
            self.supervisor.settle_process(self._custody.work(), work)?;
            return Ok(());
        }
        work.charge(acyclic_fs::WorkCounters {
            items_examined: 1, ..acyclic_fs::WorkCounters::default()
        })?;
        let mut active = self.supervisor.active.lock()
            .map_err(|_| boundary(crate::wire::ErrorCode::Storage, "original supervisor control is poisoned"))?;
        let active = active.as_mut()
            .ok_or(boundary(crate::wire::ErrorCode::Storage, "original owned supervisor disappeared"))?;
        if active.attempt != attempt {
            return Err(boundary(crate::wire::ErrorCode::Storage, "stop attempt differs from the original owned supervisor"));
        }
        if !active.completed { active.stop_requested = true; }
        Ok(())
    }

    /// Await actual collection and the original durable observed receipt after signalling.
    pub async fn request_stop(
        &self, attempt: crate::EffectAttemptId,
        work: &acyclic_fs::NativeMaterializationWork,
        cleanup_cancellation: &acyclic_fs::CancellationToken,
    ) -> Result<NativeProcessReceipt> {
        if !work.remaining()?.has_finite_allowances() {
            return Err(boundary(crate::wire::ErrorCode::Storage, "native stop wait requires a finite original work allowance"));
        }
        crate::NativeFutureBundle::new(work, || async {
            let quiescent = self.wait_for_original_collection(attempt, work, cleanup_cancellation).await?;
            let receipt = self.receipt(attempt, work, None).await?
                .ok_or(boundary(crate::wire::ErrorCode::Storage, "original supervisor completed without a retained launch receipt"))?;
            if !receipt.observed {
                return Err(boundary(crate::wire::ErrorCode::Storage, "original supervisor completion has no durable observed receipt"));
            }
            if !quiescent {
                return Err(boundary(crate::wire::ErrorCode::Storage, "original physical process collection remains uncertain"));
            }
            Ok(receipt)
        })?.await
    }

    async fn wait_for_original_collection(
        &self,
        attempt: crate::EffectAttemptId,
        work: &acyclic_fs::NativeMaterializationWork,
        cleanup_cancellation: &acyclic_fs::CancellationToken,
    ) -> Result<bool> {
        self.signal_stop_original(attempt, work, cleanup_cancellation).await?;
        let poll_ms = self.invocation()?.request.cancellation_poll_ms;
        loop {
            let notified = self.supervisor.completion.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            cleanup_cancellation.check().map_err(acyclic_fs::NativeTreeMaterializationError::from)?;
            work.charge(acyclic_fs::WorkCounters {
                items_examined: 1, ..acyclic_fs::WorkCounters::default()
            })?;
            let (completed, quiescent) = self.supervisor.snapshot(attempt)?;
            if completed {
                if !quiescent {
                    self.authenticate_cleanup_owner(work).await?;
                    self.supervisor.settle_process(self._custody.work(), work)?;
                }
                return Ok(self.supervisor.snapshot(attempt)?.1);
            }
            tokio::select! {
                () = &mut notified => {},
                () = tokio::time::sleep(Duration::from_millis(u64::from(poll_ms))) => {},
            }
        }
    }

    /// Reads authenticated output together with its retained allocation.
    pub async fn output(
        &self, attempt: crate::EffectAttemptId, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<(crate::conversation::FileRef, acyclic_fs::NativeAllocation)>> {
        let Invocation { request, result_schema, .. } = self.invocation()?.as_ref();
        let Some(receipt) = self.receipt(attempt, work, None).await? else { return Ok(None); };
        if !receipt.observed { return Ok(None); }
        let NativeProcessReceipt { effect, diagnostic, original_fence, publication_failure,
            invocation_failure, published, _custody: mut custody, .. } = receipt;
        drop((original_fence, publication_failure, invocation_failure, published));
        let EffectObservation { provider, status, .. } = effect;
        drop(provider);
        let reference = match (status, diagnostic) {
            (EffectStatus::Succeeded { result }, _) => result,
            (_, Some(diagnostic)) => diagnostic,
            (_, None) => return Ok(None),
        };
        {
            let io = super::PreparationIo::from_native_work(work)?;
            self.owner.validate_input_file(&reference, Some(&io))?;
            if reference.volume() != self.authority.results.volume()
                || reference.descriptor().byte_length() > u64::from(request.maximum_result_bytes)
            {
                return Err(boundary(crate::wire::ErrorCode::Unauthorized, "native output differs from admitted result owner or bound"));
            }
            let bytes = self.authority.result_content.read(&reference, u64::from(request.maximum_result_bytes), Some(&io))?.await?;
            let value = io.decode(std::marker::PhantomData::<serde_json::Value>, &bytes, crate::wire::ErrorCode::Invalid)?;
            crate::contract::validate_json_schema_value(result_schema, &value.0,
                "native process result", Some(&io))?;
        }
        custody.substitute(reference.logical_owned_bytes()?)?;
        Ok(Some((reference, custody)))
    }

    /// Keeps the registry's shared provider allocation in this original family.
    pub fn into_arc_with_native_work(
        mut self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<Self>> {
        finite_binding::require_family(self._custody.work(), work)?;
        let mut custody = work.claim(std::mem::size_of::<Self>() as u64, u64::from((std::mem::size_of::<Self>() as u64) != 0), acyclic_fs::WorkCounters::UNCHARGED)?;
        let mut original = self._custody.work().claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)?;
        if let Err(cause) = original.absorb(&mut custody) {
            return Err(Error::NativeWork { cause, custody: Some(custody) });
        }
        self._arc_custody = Some(original);
        Ok(Arc::new(self))
    }

    fn invocation(&self) -> Result<&Arc<Invocation<P>>> {
        self.invocation.as_ref().ok_or(boundary(crate::wire::ErrorCode::Unsupported, "receipt-only recovery owns no executable invocation"))
    }

}

impl<P> EffectProvider for NativeProcessProvider<P>
where
    P: StreamProvider,
{
    fn id(&self) -> &str {
        &self.plan.provider
    }
    fn guarantees(&self, kind: &str) -> BTreeSet<EffectGuarantee> {
        if kind == NATIVE_PROCESS_EFFECT_KIND {
            BTreeSet::from([EffectGuarantee::AtMostOnce])
        } else {
            BTreeSet::new()
        }
    }
    fn supports_guarantee_with_native_work(
        &self, kind: &str, guarantee: EffectGuarantee,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<bool> {
        finite_binding::require_family(self._custody.work(), work)?;
        work.charge(acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() })?;
        Ok(kind == NATIVE_PROCESS_EFFECT_KIND && guarantee == EffectGuarantee::AtMostOnce)
    }
    fn linearizable_reconciliation(&self) -> bool {
        false
    }
    fn dispatch<'a>(&'a self, _request: EffectDispatch) -> BoxProviderFuture<'a, Result<EffectObservation>> {
        Box::pin(std::future::ready(Err(boundary(crate::wire::ErrorCode::Storage, "native dispatch requires its original work ledger"))))
    }
    fn reconcile<'a>(&'a self, _attempt: crate::EffectAttemptId) -> BoxProviderFuture<'a, Result<Option<EffectObservation>>> {
        Box::pin(std::future::ready(Err(boundary(crate::wire::ErrorCode::Storage, "native reconciliation requires its original work ledger"))))
    }
    fn dispatch_with_native_work<'a>(
        &'a self, request: EffectDispatch,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (EffectObservation, acyclic_fs::NativeAllocation)>> {
        finite_dispatch::require_finite(work)?;
        finite_binding::require_family(self._custody.work(), work)?;
        let view = &self.invocation()?.view;
        view.operation_cancellation().check()
            .map_err(acyclic_fs::NativeTreeMaterializationError::from)?;
        let bytes = receipt_observation::add(
            receipt_observation::add(request.provider.capacity() as u64, request.effect_kind.capacity() as u64)?,
            request.request.logical_owned_bytes()?)?;
        let custody = work.claim(bytes, 0, acyclic_fs::WorkCounters::UNCHARGED)?;
        crate::NativeFutureBundle::new(work, || self.launch((request, custody), work))
    }

    fn reconcile_with_native_work<'a>(
        &'a self, attempt: crate::EffectAttemptId,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, Option<(EffectObservation, acyclic_fs::NativeAllocation)>>> {
        finite_dispatch::require_finite(work)?;
        finite_binding::require_family(self._custody.work(), work)?;
        crate::NativeFutureBundle::new(work, || async move {
            self.receipt(attempt, work, None).await?
                .map(NativeProcessReceipt::into_effect)
                .transpose()
        })
    }
}
