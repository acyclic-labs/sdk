//! Approved one-shot process effects, using the task's existing fenced Stream.

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
            let work = io.original_native_work().ok_or(receipt_error("native invocation requires its original work ledger"))?;
            let (exchange, first, custody) = exchange.exchange_with_native_work(work)?;
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
            || !self.cwd.starts_with(&self.view.options.root)
            || self
                .cwd
                .components()
                .any(|component| matches!(component, std::path::Component::ParentDir))
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
                {
                    io.charge(acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() })?;
                }
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
        let relative = self.cwd.strip_prefix(&view.options.root)
            .map_err(|_| validation_error(Some(io), crate::wire::ErrorCode::Conflict,
                "native cwd is outside the selected locator"))?;
        if relative.components().any(|part| !matches!(part, std::path::Component::Normal(_))) {
            return Err(validation_error(Some(io), crate::wire::ErrorCode::Invalid,
                "native cwd has a non-relative component"));
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
    invocation: Option<Invocation<P>>,
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
    request: Arc<NativeProcessRequest>,
    result_schema: Arc<serde_json::Value>,
    approval_digest: [u8; 32],
}

struct NativeActiveAttempt {
    attempt: crate::EffectAttemptId,
    stop_requested: bool,
    quiescent: bool,
    completed: bool,
}

struct NativeRetainedProcess {
    process: acyclic_native_runtime::ProcessTree,
    _scopes: (Vec<acyclic_fs::native_mount::OwnedNativeMountOriginalWorkScope>, acyclic_fs::NativeAllocation),
    _custody: acyclic_fs::NativeAllocation,
}

struct NativeSupervisorControl {
    active: Mutex<Option<NativeActiveAttempt>>,
    completion: tokio::sync::Notify,
    process: Mutex<Option<NativeRetainedProcess>>,
}

impl NativeSupervisorControl {
    fn settle_process(
        &self, original: &acyclic_fs::NativeMaterializationWork,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        let mut retained = self.process.lock().map_err(|_| Error::NativeBoundary {
            code: crate::wire::ErrorCode::Storage, reason: "original supervisor process custody is poisoned",
        })?;
        if let Some(retained) = retained.as_mut() {
            if !retained._custody.work().shares_ledger(original) {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                    reason: "original supervisor tree belongs to another work family" });
            }
            retained.process.terminate(Some(work.runtime_meter()))
                .map_err(|error| Error::NativeWork {
                    cause: work.runtime_error(error), custody: None })?;
        } else {
            return Ok(());
        }
        retained.take();
        self.record_quiescence(true)
            .map_err(|error| Error::NativeWork { cause: error.into(), custody: None })
    }

    fn begin(&self, attempt: crate::EffectAttemptId) -> std::result::Result<(), Error> {
        let mut active = self.active.lock()
            .map_err(|_| receipt_error("native supervisor control was poisoned"))?;
        if active.is_some() {
            return Err(receipt_error("native provider already owns an original supervisor"));
        }
        *active = Some(NativeActiveAttempt {
            attempt, stop_requested: false,
            quiescent: true, completed: false,
        });
        Ok(())
    }

    fn snapshot(
        &self, attempt: crate::EffectAttemptId,
    ) -> std::result::Result<(bool, bool), Error> {
        let active = self.active.lock()
            .map_err(|_| receipt_error("original supervisor control is poisoned"))?;
        let active = active.as_ref()
            .ok_or(receipt_error("original owned supervisor is unavailable; launch intent cannot hydrate process control"))?;
        if active.attempt != attempt {
            return Err(receipt_error("stop attempt differs from the original owned supervisor"));
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

#[derive(Debug, PartialEq, Serialize, Deserialize)]
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
enum Receipt<L = Box<LaunchReceipt<'static>>, O = Box<NativeObservation>> {
    Launch(L),
    Observed(O),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
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

struct NativeExecution {
    status: EffectStatus,
    diagnostic: Option<crate::conversation::FileRef>,
    published: Vec<NativeProcessPublishedVolume>,
}

fn check(work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
    work.remaining()?;
    Ok(())
}

fn examine(work: &acyclic_fs::NativeMaterializationWork, compare: impl FnOnce() -> bool) -> Result<bool> {
    work.charge(acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() })?;
    Ok(compare())
}

fn receipt_error(reason: &'static str) -> Error {
    Error::NativeBoundary { code: crate::wire::ErrorCode::Storage, reason }
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
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    #[serde(default)]
    pub published: Vec<NativeProcessPublishedVolume>,
    /// Selected native view's exact filesystem capture/publication work.
    pub work: acyclic_fs::WorkCounters,
    #[serde(skip)]
    _custody: Option<acyclic_fs::NativeAllocation>,
}

impl NativeProcessReceipt {
    /// Moves the observed effect together with its original measured payload custody.
    pub fn into_effect(
        self,
    ) -> std::result::Result<(EffectObservation, acyclic_fs::NativeAllocation), Error> {
        let Self { original_fence, effect, diagnostic, publication_failure,
            invocation_failure, published, _custody, .. } = self;
        drop((original_fence, diagnostic, publication_failure, invocation_failure, published));
        let mut custody = _custody.ok_or(receipt_error("native receipt has no original measured custody"))?;
        custody.substitute(receipt_observation::effect_bytes(&effect)?)?;
        Ok((effect, custody))
    }
}

impl PartialEq for NativeProcessReceipt {
    fn eq(&self, other: &Self) -> bool {
        self.task == other.task && self.command == other.command
            && self.original_fence == other.original_fence && self.effect == other.effect
            && self.observed == other.observed && self.diagnostic == other.diagnostic
            && self.publication_failure == other.publication_failure
            && self.invocation_failure == other.invocation_failure && self.work == other.work
            && self.published == other.published
    }
}

impl<P> NativeProcessProvider<P>
where
    P: StreamProvider + 'static,
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
    ) -> std::result::Result<(), Error> {
        if !work.remaining()?.has_finite_allowances() {
            return Err(receipt_error("native stop requires a finite original work allowance"));
        }
        crate::NativeFutureBundle::new(work, ||
            self.signal_stop_original(attempt, work, cleanup_cancellation, work))?.await
    }

    async fn signal_stop_original(
        &self, attempt: crate::EffectAttemptId,
        work: &acyclic_fs::NativeMaterializationWork,
        cleanup_cancellation: &acyclic_fs::CancellationToken,
        original: &acyclic_fs::NativeMaterializationWork,
    ) -> std::result::Result<(), Error> {
        cleanup_cancellation.check().map_err(acyclic_fs::NativeTreeMaterializationError::from)?;
        work.charge(acyclic_fs::WorkCounters {
            items_examined: 1, ..acyclic_fs::WorkCounters::default()
        })?;
        let (completed, _) = self.supervisor.snapshot(attempt)?;
        self.authenticate_cleanup_owner(work).await?;
        cleanup_cancellation.check().map_err(acyclic_fs::NativeTreeMaterializationError::from)?;
        if completed {
            self.supervisor.settle_process(original, work)?;
            return Ok(());
        }
        work.charge(acyclic_fs::WorkCounters {
            items_examined: 1, ..acyclic_fs::WorkCounters::default()
        })?;
        let mut active = self.supervisor.active.lock()
            .map_err(|_| receipt_error("original supervisor control is poisoned"))?;
        let active = active.as_mut()
            .ok_or(receipt_error("original owned supervisor disappeared"))?;
        if active.attempt != attempt {
            return Err(receipt_error("stop attempt differs from the original owned supervisor"));
        }
        if !active.completed { active.stop_requested = true; }
        Ok(())
    }

    /// Await actual collection and the original durable observed receipt after signalling.
    pub async fn request_stop(
        &self, attempt: crate::EffectAttemptId,
        work: &acyclic_fs::NativeMaterializationWork,
        cleanup_cancellation: &acyclic_fs::CancellationToken,
    ) -> std::result::Result<NativeProcessReceipt, Error> {
        if !work.remaining()?.has_finite_allowances() {
            return Err(receipt_error("native stop wait requires a finite original work allowance"));
        }
        crate::NativeFutureBundle::new(work, || async {
            let quiescent = self.wait_for_original_collection(attempt, work, cleanup_cancellation, work).await?;
            let receipt = self.receipt(attempt, work, None).await?
                .ok_or(receipt_error("original supervisor completed without a retained launch receipt"))?;
            if !receipt.observed {
                return Err(receipt_error("original supervisor completion has no durable observed receipt"));
            }
            if !quiescent {
                return Err(receipt_error("original physical process collection remains uncertain"));
            }
            Ok(receipt)
        })?.await
    }

    async fn wait_for_original_collection(
        &self,
        attempt: crate::EffectAttemptId,
        work: &acyclic_fs::NativeMaterializationWork,
        cleanup_cancellation: &acyclic_fs::CancellationToken,
        original: &acyclic_fs::NativeMaterializationWork,
    ) -> std::result::Result<bool, Error> {
        self.signal_stop_original(attempt, work, cleanup_cancellation, original).await?;
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
                    self.supervisor.settle_process(original, work)?;
                }
                return Ok(self.supervisor.snapshot(attempt)?.1);
            }
            tokio::select! {
                () = &mut notified => {},
                () = tokio::time::sleep(Duration::from_millis(u64::from(poll_ms))) => {},
            }
        }
    }

    /// Reads retained output after authenticating this exact attempt.
    pub async fn output(
        &self,
        attempt: crate::EffectAttemptId, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<crate::conversation::FileRef>> {
        let Invocation { request, result_schema, .. } = self.invocation()?;
        let Some(receipt) = self.receipt(attempt, work, None).await? else { return Ok(None); };
        if !receipt.observed { return Ok(None); }
        let reference = match receipt.effect.status {
            EffectStatus::Succeeded { result } => Some(result),
            _ => receipt.diagnostic,
        };
        if let Some(reference) = &reference {
            self.owner.validate_input_file(reference, Some(&super::PreparationIo::from_native_work(work)?))?;
            if reference.volume() != self.authority.results.volume()
                || reference.descriptor().byte_length()
                    > u64::from(request.maximum_result_bytes)
            {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "native output differs from admitted result owner or bound" });
            }
            let bytes = self.authority.result_content.read_with_native_work(
                reference, u64::from(request.maximum_result_bytes), work)?.await?;
            let io = super::PreparationIo::from_native_work(work)?;
            let value = io.decode_preparation::<serde_json::Value>(&bytes, crate::wire::ErrorCode::Invalid)?;
            crate::contract::validate_json_schema_value(result_schema, &value.0,
                "native process result", Some(&io))?;
        }
        Ok(reference)
    }



    /// Keeps the registry's shared provider allocation in this original family.
    pub fn into_arc_with_native_work(
        mut self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<Self>> {
        self.require_work_family(work)?;
        let mut custody = work.claim(std::mem::size_of::<Self>() as u64)?;
        let mut original = self._custody.work().retain_owned(0)?;
        if let Err(cause) = original.absorb(&mut custody) {
            return Err(Error::NativeWork { cause, custody: Some(custody) });
        }
        self._arc_custody = Some(original);
        Ok(Arc::new(self))
    }

    fn require_work_family(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        finite_binding::require_family(self._custody.work(), work)
    }

    fn invocation(&self) -> Result<&Invocation<P>> {
        self.invocation.as_ref().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "receipt-only recovery owns no executable invocation",
        })
    }

}

impl<P> EffectProvider for NativeProcessProvider<P>
where
    P: StreamProvider + 'static,
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
        self.require_work_family(work)?;
        work.charge(acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() })?;
        Ok(kind == NATIVE_PROCESS_EFFECT_KIND && guarantee == EffectGuarantee::AtMostOnce)
    }
    fn linearizable_reconciliation(&self) -> bool {
        false
    }
    fn dispatch<'a>(&'a self, _request: EffectDispatch) -> BoxProviderFuture<'a, Result<EffectObservation>> {
        Box::pin(std::future::ready(Err(receipt_error("native dispatch requires its original work ledger"))))
    }
    fn reconcile<'a>(&'a self, _attempt: crate::EffectAttemptId) -> BoxProviderFuture<'a, Result<Option<EffectObservation>>> {
        Box::pin(std::future::ready(Err(receipt_error("native reconciliation requires its original work ledger"))))
    }
    fn dispatch_with_native_work<'a>(
        &'a self, request: EffectDispatch,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (EffectObservation, acyclic_fs::NativeAllocation)>> {
        finite_dispatch::require_finite(work)?;
        self.require_work_family(work)?;
        let view = &self.invocation()?.view;
        view.operation_cancellation().check()
            .map_err(acyclic_fs::NativeTreeMaterializationError::from)?;
        let bytes = receipt_observation::add(
            receipt_observation::add(request.provider.capacity() as u64, request.effect_kind.capacity() as u64)?,
            request.request.logical_owned_bytes()?)?;
        let custody = work.retain_owned(bytes)?;
        crate::NativeFutureBundle::new(work, || self.launch((request, custody), work))
    }

    fn reconcile_with_native_work<'a>(
        &'a self, attempt: crate::EffectAttemptId,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, Option<(EffectObservation, acyclic_fs::NativeAllocation)>>> {
        finite_dispatch::require_finite(work)?;
        self.require_work_family(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            self.receipt(attempt, work, None).await?
                .map(NativeProcessReceipt::into_effect)
                .transpose()
        })
    }
}
