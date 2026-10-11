//! Approved one-shot process effects, using the task's existing fenced Stream.

use super::{
    NATIVE_PROCESS_EFFECT_KIND, NativeProcessRequest, NativeProcessResult, NativeProcessStop,
    NativeProcessStopKind, NativeProcessView, NativePublicationFailure, NativeViewManifest, NativeViewOwner,
};
use crate::{
    Error, InteractionId, OperationId, Result,
    conversation::{ContentPublisher, ContentResidencyVerifier},
    core::{AuthorityVerifier, EffectGuarantee, EffectStatus},
    distributed::JournalWrite,
    durable_host::TaskJournalOwner,
    effect_host::{TaskEffectPlan, task_effect_id},
    effects::{EffectDispatch, EffectObservation, EffectProvider},
    store::{HistoryReadLimits, HistoryReader},
    tool::ToolApprovalVerifier,
};
use acyclic_stream::{BoxProviderFuture, StreamError, StreamPath, StreamProvider};
use bytes::Bytes;
use futures::TryStreamExt as _;
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    collections::BTreeSet,
    sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}},
    time::Duration,
};

mod receipt_observation;
mod finite_dispatch;
mod finite_execution;
mod finite_binding;
use finite_execution::execute;
#[cfg(test)]
mod receipt_observation_tests;

impl NativeProcessRequest {
    fn validate(&self) -> Result<()> {
        self.validate_with_io(None)
    }

    fn validate_with_io(&self, io: Option<&super::PreparationIo>) -> Result<()> {
        use super::native_contract::validation_error;
        if self.standard_input.is_some() && self.mcp_stdio.is_some() {
            return Err(validation_error(io, crate::wire::ErrorCode::Invalid,
                "literal stdin and MCP exchange are mutually exclusive"));
        }
        if let Some(exchange) = &self.mcp_stdio {
            match io.and_then(super::PreparationIo::original_native_work) {
                Some(work) => {
                    let (exchange, first, custody) = exchange.exchange_with_native_work(work)?;
                    drop(exchange);
                    drop(first);
                    drop(custody);
                }
                None => exchange.validate()?,
            }
        }
        if let Some(io) = io {
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
            return Err(validation_error(io, crate::wire::ErrorCode::Invalid,
                "invalid or changed native process invocation"));
        }
        #[cfg(windows)]
        for (index, key) in self.environment.keys().enumerate() {
            for prior in self.environment.keys().take(index) {
                if let Some(io) = io {
                    io.charge(acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() })?;
                }
                // The original Unicode uppercase spelling, without constructing
                // a second owned key or an unmetered case-folding set.
                if key.chars().flat_map(char::to_uppercase)
                    .eq(prior.chars().flat_map(char::to_uppercase)) {
                    return Err(validation_error(io, crate::wire::ErrorCode::Invalid,
                        "duplicate native environment key under Windows case folding"));
                }
            }
        }
        Ok(())
    }

    fn validate_native_paths(&self, view: &NativeViewManifest) -> Result<()> {
        self.validate_native_paths_with_io(view, None)
    }

    fn validate_native_paths_with_io(
        &self, view: &NativeViewManifest, io: Option<&super::PreparationIo>,
    ) -> Result<()> {
        use super::native_contract::validation_error;
        self.validate_with_io(io)?;
        if &self.view != view {
            return Err(validation_error(io, crate::wire::ErrorCode::Conflict,
                "native command view changed"));
        }
        // This is only the locator grammar. The prepared view checks its
        // original held root and each no-follow directory before capture.
        let relative = self.cwd.strip_prefix(&view.options.root)
            .map_err(|_| validation_error(io, crate::wire::ErrorCode::Conflict,
                "native cwd is outside the selected locator"))?;
        if relative.components().any(|part| !matches!(part, std::path::Component::Normal(_))) {
            return Err(validation_error(io, crate::wire::ErrorCode::Invalid,
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
    /// Original custody of operation-owned authority data. Shared provider aliases
    /// retain it until the authority and all of its capability fields are dropped.
    /// Pre-existing ordinary service capabilities carry no operation-owned claim.
    pub authority_custody: Option<acyclic_fs::NativeAllocation>,
}

/// Optional provider for one retained task command. Construction launches nothing.
/// Ordinary host processes are trusted effects, not a confinement boundary.
pub struct NativeProcessProvider<P: StreamProvider> {
    owner: Arc<TaskJournalOwner<P>>,
    command: OperationId,
    plan: Arc<TaskEffectPlan>,
    admission: NativeAdmission,
    view: Option<Arc<dyn NativeProcessView<P>>>,
    approval: InteractionId,
    authority: Arc<NativeProcessAuthority>,
    supervisor: Arc<NativeSupervisorControl>,
    effect_id: crate::EffectId,
    history: Arc<HistoryReader<P>>,
    _native_work: Option<acyclic_fs::NativeMaterializationWork>,
    _custody: Option<Arc<Mutex<acyclic_fs::NativeAllocation>>>,
}

enum NativeAdmission {
    Executable {
        request: Arc<NativeProcessRequest>,
        result_schema: Arc<serde_json::Value>,
        approval_digest: [u8; 32],
    },
    /// A protected past attempt is observable but never executable.
    ReceiptOnly,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum NativeCapturePhase {
    Pending,
    Spawning,
    Running,
    Drained,
    Uncertain,
}

struct NativeActiveAttempt {
    attempt: crate::EffectAttemptId,
    stop_requested: AtomicBool,
    capture: NativeCapturePhase,
    completed: bool,
}

struct NativeRetainedProcess {
    process: acyclic_native_runtime::ProcessTree,
    _scopes: Option<crate::filesystem::native_execution::NativeProcessNamespaceScopes>,
    _custody: acyclic_fs::NativeAllocation,
}

struct NativeSupervisorControl {
    active: Mutex<Option<NativeActiveAttempt>>,
    completion: tokio::sync::Notify,
    process: Mutex<Option<NativeRetainedProcess>>,
}

impl NativeSupervisorControl {
    fn new() -> Self {
        Self { active: Mutex::new(None), completion: tokio::sync::Notify::new(),
            process: Mutex::new(None) }
    }

    fn settle_process_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        let mut retained = self.process.lock().map_err(|_| Error::NativeBoundary {
            code: crate::wire::ErrorCode::Storage, reason: "original supervisor process custody is poisoned",
        })?;
        if let Some(retained) = retained.as_mut() {
            if !retained._custody.work().shares_ledger(work) {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                    reason: "original supervisor tree belongs to another work family" });
            }
            retained.process.terminate_measured(work.runtime_meter())
                .map_err(|error| Error::NativeWork {
                    cause: work.runtime_error(error), custody: None })?;
        } else {
            // No owned tree is not evidence that an uncertain OS outcome settled.
            return Ok(());
        }
        // A successfully reaped tree physically drops before its original alias.
        retained.take();
        self.record_capture(NativeCapturePhase::Drained)
            .map_err(|error| Error::NativeWork { cause: error.into(), custody: None })
    }

    fn begin(&self, attempt: crate::EffectAttemptId) -> std::result::Result<(), NativeReceiptError> {
        let mut active = self.active.lock()
            .map_err(|_| NativeReceiptError::Record("native supervisor control was poisoned"))?;
        if active.is_some() {
            return Err(NativeReceiptError::Record("native provider already owns an original supervisor"));
        }
        *active = Some(NativeActiveAttempt {
            attempt, stop_requested: AtomicBool::new(false),
            capture: NativeCapturePhase::Pending, completed: false,
        });
        Ok(())
    }

    fn snapshot(
        &self, attempt: crate::EffectAttemptId,
    ) -> std::result::Result<(bool, NativeCapturePhase), NativeReceiptError> {
        let active = self.active.lock()
            .map_err(|_| NativeReceiptError::Record("original supervisor control is poisoned"))?;
        let active = active.as_ref()
            .ok_or(NativeReceiptError::Record("original owned supervisor is unavailable; launch intent cannot hydrate process control"))?;
        if active.attempt != attempt {
            return Err(NativeReceiptError::Record("stop attempt differs from the original owned supervisor"));
        }
        Ok((active.completed, active.capture))
    }

    fn record_capture(&self, phase: NativeCapturePhase) -> std::io::Result<()> {
        let mut active = self.active.lock()
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::Other))?;
        active.as_mut().ok_or_else(|| std::io::Error::from(std::io::ErrorKind::Other))?.capture = phase;
        Ok(())
    }

    fn poll_stop(&self) -> std::io::Result<()> {
        let active = self.active.lock()
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::Other))?;
        let active = active.as_ref().ok_or_else(|| std::io::Error::from(std::io::ErrorKind::Other))?;
        if active.stop_requested.load(Ordering::Acquire) {
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
enum Receipt<'a> {
    Launch(Box<LaunchReceipt<'a>>),
    Observed(Box<NativeObservation>),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeObservation {
    effect: EffectObservation,
    diagnostic: Option<crate::conversation::FileRef>,
    publication_failure: Option<NativePublicationFailure>,
    invocation_failure: Option<String>,
    work: acyclic_fs::WorkCounters,
}

struct NativeExecution {
    status: EffectStatus,
    diagnostic: Option<crate::conversation::FileRef>,
    publication_failure: Option<NativePublicationFailure>,
    invocation_failure: Option<String>,
    work: acyclic_fs::WorkCounters,
}

/// Original native receipt failure without allocating a quota diagnostic.
#[derive(Debug, thiserror::Error)]
pub enum NativeReceiptError {
    /// An already owned policy or authoritative journal error.
    #[error(transparent)]
    Owner(#[from] Error),
    /// Original native admission or cancellation failure.
    #[error(transparent)]
    Native(#[from] acyclic_fs::NativeTreeMaterializationError),
    /// Original typed Stream provider failure.
    #[error(transparent)]
    Backend(#[from] StreamError),
    /// Original typed JSON failure, retaining its admitted diagnostic storage.
    #[error("{source}")]
    Decode {
        /// Original deserializer failure.
        source: serde_json::Error,
        /// Original logical diagnostic admission until the cause is dropped.
        custody: Option<acyclic_fs::NativeAllocation>,
    },
    /// Original lexical JSON failure and its scoped logical admission.
    #[error("{source}")]
    Codec {
        /// Original canonical codec failure.
        source: Error,
        /// Original logical diagnostic admission until the cause is dropped.
        custody: Option<acyclic_fs::NativeAllocation>,
    },
    /// A malformed or unavailable original receipt.
    #[error("{0}")]
    Record(&'static str),
    /// The original owned supervisor task failed to finish.
    #[error(transparent)]
    Supervisor(tokio::task::JoinError),
}

impl NativeReceiptError {
    /// Transfers the original typed cause and its already admitted custody to
    /// the canonical Harness error without formatting or copying diagnostics.
    #[must_use]
    pub fn into_root_error(self) -> Error {
        finite_dispatch::root_error(self)
    }
}

/// Authenticated original attempt and its durable native diagnostic.
/// Launch-only receipts retain uncertainty; this observation conveys no authority.
#[derive(Serialize, Deserialize)]
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
    /// Selected native view's exact filesystem capture/publication work.
    pub work: acyclic_fs::WorkCounters,
    #[serde(skip)]
    _custody: Option<acyclic_fs::NativeAllocation>,
}

impl NativeProcessReceipt {
    /// Moves the observed effect together with its original measured payload custody.
    ///
    /// The remaining receipt fields are physically dropped before their logical
    /// retention is released. Ordinary or deserialized receipts have no measured
    /// custody and cannot manufacture one through this conversion.
    pub fn into_effect_with_native_custody(
        self,
    ) -> std::result::Result<(EffectObservation, acyclic_fs::NativeAllocation), NativeReceiptError> {
        let Self {
            task: _, command: _, original_fence, effect, observed: _,
            diagnostic, publication_failure, invocation_failure, work, _custody,
        } = self;
        drop(original_fence);
        let (effect, custody) = finite_dispatch::into_observed_effect(finite_dispatch::ObservedPayload {
            observed: NativeObservation { effect, diagnostic, publication_failure, invocation_failure, work },
            _custody,
        })?;
        let custody = custody
            .ok_or(NativeReceiptError::Record("native receipt has no original measured custody"))?;
        Ok((effect, custody))
    }
}

impl std::fmt::Debug for NativeProcessReceipt {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("NativeProcessReceipt")
            .field("task", &self.task).field("command", &self.command)
            .field("original_fence", &self.original_fence).field("effect", &self.effect)
            .field("observed", &self.observed).field("diagnostic", &self.diagnostic)
            .field("publication_failure", &self.publication_failure)
            .field("invocation_failure", &self.invocation_failure).field("work", &self.work)
            .finish()
    }
}

impl PartialEq for NativeProcessReceipt {
    fn eq(&self, other: &Self) -> bool {
        self.task == other.task && self.command == other.command
            && self.original_fence == other.original_fence && self.effect == other.effect
            && self.observed == other.observed && self.diagnostic == other.diagnostic
            && self.publication_failure == other.publication_failure
            && self.invocation_failure == other.invocation_failure && self.work == other.work
    }
}

impl<P> NativeProcessProvider<P>
where
    P: StreamProvider + 'static,
{
    /// Borrows the immutable plan retained for this original native operation.
    /// Observation and recovery do not duplicate its admitted payload custody.
    pub fn original_plan(&self) -> &TaskEffectPlan {
        &self.plan
    }

    /// Original one-shot task-effect attempt selected by the existing effect
    /// engine. This observation selector does not authorize dispatch or reads.
    #[must_use]
    pub fn original_attempt(&self) -> crate::EffectAttemptId {
        crate::EffectAttemptId::from_bytes(self.effect_id.into_bytes())
    }

    /// Signal only this original owned supervisor. No process joins occur here;
    /// lifecycle callers can signal every job before awaiting any one of them.
    pub async fn signal_stop(
        &self, attempt: crate::EffectAttemptId,
        work: &acyclic_fs::NativeMaterializationWork,
        cleanup_cancellation: &acyclic_fs::CancellationToken,
    ) -> std::result::Result<(), NativeReceiptError> {
        if !work.remaining().map_err(NativeReceiptError::Native)?.has_finite_allowances() {
            return Err(NativeReceiptError::Record("native stop requires a finite original work allowance"));
        }
        crate::NativeFutureBundle::new(work, || async { Ok(async {
            cleanup_cancellation.check().map_err(acyclic_fs::NativeTreeMaterializationError::from)
                .map_err(NativeReceiptError::Native)?;
            work.charge(acyclic_fs::WorkCounters {
                items_examined: 1, ..acyclic_fs::WorkCounters::default()
            }).map_err(NativeReceiptError::Native)?;
            let (completed, _) = self.supervisor.snapshot(attempt)?;
            // The original current fence may settle after task cancellation;
            // this check confers no new dispatch or filesystem write authority.
            self.owner.verify_with_native_work(true, work).await?;
            cleanup_cancellation.check().map_err(acyclic_fs::NativeTreeMaterializationError::from)
                .map_err(NativeReceiptError::Native)?;
            if completed {
                self.supervisor.settle_process_with_native_work(work)?;
                return Ok(());
            }
            work.charge(acyclic_fs::WorkCounters {
                items_examined: 1, ..acyclic_fs::WorkCounters::default()
            }).map_err(NativeReceiptError::Native)?;
            let active = self.supervisor.active.lock()
                .map_err(|_| NativeReceiptError::Record("original supervisor control is poisoned"))?;
            let active = active.as_ref()
                .ok_or(NativeReceiptError::Record("original owned supervisor disappeared"))?;
            if active.attempt != attempt {
                return Err(NativeReceiptError::Record("stop attempt differs from the original owned supervisor"));
            }
            if !active.completed { active.stop_requested.store(true, Ordering::Release); }
            Ok(())
        }.await) }).map_err(NativeReceiptError::Owner)?.await.map_err(NativeReceiptError::Owner)?
    }

    /// Await actual collection and the original durable observed receipt after
    /// signalling. This does not stop by PID, cancel the view/task, or relaunch.
    pub async fn request_stop(
        &self, attempt: crate::EffectAttemptId,
        work: &acyclic_fs::NativeMaterializationWork,
        cleanup_cancellation: &acyclic_fs::CancellationToken,
    ) -> std::result::Result<NativeProcessReceipt, NativeReceiptError> {
        if !work.remaining().map_err(NativeReceiptError::Native)?.has_finite_allowances() {
            return Err(NativeReceiptError::Record("native stop wait requires a finite original work allowance"));
        }
        crate::NativeFutureBundle::new(work, || async { Ok(async {
            self.signal_stop(attempt, work, cleanup_cancellation).await?;
            let poll_ms = match &self.admission {
                NativeAdmission::Executable { request, .. } => request.cancellation_poll_ms,
                NativeAdmission::ReceiptOnly => return Err(NativeReceiptError::Record("receipt-only recovery owns no original supervisor")),
            };
            let phase = loop {
                let notified = self.supervisor.completion.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                cleanup_cancellation.check().map_err(acyclic_fs::NativeTreeMaterializationError::from)
                    .map_err(NativeReceiptError::Native)?;
                work.charge(acyclic_fs::WorkCounters {
                    items_examined: 1, ..acyclic_fs::WorkCounters::default()
                }).map_err(NativeReceiptError::Native)?;
                let (completed, phase) = self.supervisor.snapshot(attempt)?;
                if completed {
                    if !matches!(phase, NativeCapturePhase::Pending | NativeCapturePhase::Drained | NativeCapturePhase::Recorded) {
                        self.owner.verify_with_native_work(true, work).await?;
                        self.supervisor.settle_process_with_native_work(work)?;
                    }
                    break self.supervisor.snapshot(attempt)?.1;
                }
                tokio::select! {
                    () = &mut notified => {},
                    () = tokio::time::sleep(Duration::from_millis(u64::from(poll_ms))) => {},
                }
            };
            let receipt = self.receipt_with_native_work(attempt, work).await?
                .ok_or(NativeReceiptError::Record("original supervisor completed without a retained launch receipt"))?;
            if !receipt.observed {
                return Err(NativeReceiptError::Record("original supervisor completion has no durable observed receipt"));
            }
            if !matches!(phase, NativeCapturePhase::Pending | NativeCapturePhase::Drained | NativeCapturePhase::Recorded) {
                return Err(NativeReceiptError::Record("original physical process collection remains uncertain"));
            }
            Ok(receipt)
        }.await) }).map_err(NativeReceiptError::Owner)?.await.map_err(NativeReceiptError::Owner)?
    }

    /// Queries the same fenced launch/observation journal used by reconciliation.
    pub async fn receipt(
        &self,
        attempt: crate::EffectAttemptId,
    ) -> Result<Option<NativeProcessReceipt>> {
        self.receipt_original(attempt, None).await.map_err(receipt_observation::ordinary_error)
    }

    /// Reads retained output after authenticating this exact attempt. Unknown
    /// cleanup may have a diagnostic artifact but remains indeterminate.
    pub async fn output(
        &self,
        attempt: crate::EffectAttemptId,
    ) -> Result<Option<crate::conversation::FileRef>> {
        let (request, result_schema, _) = self.executable_admission()?;
        let Some(receipt) = self.receipt(attempt).await? else { return Ok(None); };
        if !receipt.observed { return Ok(None); }
        let reference = match receipt.effect.status {
            EffectStatus::Succeeded { result } => Some(result),
            _ => receipt.diagnostic,
        };
        if let Some(reference) = &reference {
            self.owner.validate_input_file(reference)?;
            if reference.volume() != self.authority.results.volume()
                || reference.descriptor().byte_length()
                    > u64::from(request.maximum_result_bytes)
            {
                return Err(Error::Unauthorized(
                    "native output differs from admitted result owner or bound".into(),
                ));
            }
            crate::effects::validate_result_bytes(
                result_schema,
                reference,
                &self.authority.result_content.read(reference).await?,
            )?;
        }
        Ok(reference)
    }

    /// Loads the pinned request/schema and verifies exact approval and task grants.
    /// Consumers stage `NativeProcessRequest` as the plan's JSON request and use
    /// the ordinary conversation interaction approval before constructing this.
    pub async fn new(
        owner: Arc<TaskJournalOwner<P>>,
        command: OperationId,
        plan: TaskEffectPlan,
        view: Arc<dyn NativeProcessView<P>>,
        approval: InteractionId,
        authority: NativeProcessAuthority,
    ) -> Result<Self> {
        owner.verify(false).await?;
        finite_binding::bind_original(
            owner, command, Cow::Owned(plan), Some(view), approval, authority, None,
        ).await
    }

    /// Reopens receipt observation after provider drop or host restart, including
    /// cancellation. It needs no native directories and has no dispatch capability.
    pub async fn recover(
        owner: Arc<TaskJournalOwner<P>>,
        command: OperationId,
        plan: TaskEffectPlan,
        approval: InteractionId,
        authority: NativeProcessAuthority,
    ) -> Result<Self> {
        finite_binding::bind_original(
            owner, command, Cow::Owned(plan), None, approval, authority, None,
        ).await
    }


    /// Binds this exact prepared namespace to its original finite work family.
    /// Admission precedes construction of the binding future and content reads.
    pub fn new_with_native_work<'a>(
        owner: Arc<TaskJournalOwner<P>>, command: OperationId, plan: &'a TaskEffectPlan,
        view: Arc<dyn NativeProcessView<P>>, approval: InteractionId,
        authority: NativeProcessAuthority, work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, Self>> {
        finite_dispatch::require_finite(work)?;
        finite_binding::require_view_family(view.as_ref(), work)?;
        crate::NativeFutureBundle::new(work, || async move {
            finite_binding::bind_original(
                owner, command, Cow::Borrowed(plan), Some(view), approval, authority, Some(work),
            ).await
        })
    }

    /// Keeps the registry's shared provider allocation in this original family.
    pub fn into_arc_with_native_work(
        self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<Self>> {
        self.require_work_family(work)?;
        let custody = work.claim(std::mem::size_of::<Self>() as u64)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        finite_binding::absorb_shared(&self._custody, custody)?;
        Ok(Arc::new(self))
    }

    fn require_work_family(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        finite_binding::require_family(self._native_work.as_ref(), work)
    }

    fn executable_admission(&self) -> Result<(&Arc<NativeProcessRequest>, &Arc<serde_json::Value>, [u8; 32])> {
        match &self.admission {
            NativeAdmission::Executable { request, result_schema, approval_digest } =>
                Ok((request, result_schema, *approval_digest)),
            NativeAdmission::ReceiptOnly => Err(Error::Unsupported("receipt-only native recovery has no dispatch or result-loading capability".into())),
        }
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
        work.charge(acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() })
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok(kind == NATIVE_PROCESS_EFFECT_KIND && guarantee == EffectGuarantee::AtMostOnce)
    }
    fn linearizable_reconciliation(&self) -> bool {
        false
    }
    fn dispatch<'a>(
        &'a self,
        request: EffectDispatch,
    ) -> BoxProviderFuture<'a, Result<EffectObservation>> {
        Box::pin(async move {
            let payload = finite_dispatch::dispatch_payload(request, None)
                .map_err(receipt_observation::ordinary_error)?;
            self.launch_original(payload, None).await
                .map(|(effect, _)| effect)
                .map_err(receipt_observation::ordinary_error)
        })
    }
    fn reconcile<'a>(
        &'a self,
        attempt: crate::EffectAttemptId,
    ) -> BoxProviderFuture<'a, Result<Option<EffectObservation>>> {
        Box::pin(async move {
            self.receipt_original(attempt, None).await
                .map(|receipt| receipt.map(|receipt| receipt.effect))
                .map_err(receipt_observation::ordinary_error)
        })
    }
    fn dispatch_with_native_work<'a>(
        &'a self, request: EffectDispatch,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (EffectObservation, acyclic_fs::NativeAllocation)>> {
        finite_dispatch::require_finite(work)?;
        self.require_work_family(work)?;
        let view = self.view.as_ref().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "receipt-only native recovery has no dispatch capability",
        })?;
        view.operation_cancellation().check()
            .map_err(acyclic_fs::NativeTreeMaterializationError::from)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let payload = finite_dispatch::dispatch_payload(request, Some(work))
            .map_err(finite_dispatch::root_error)?;
        crate::NativeFutureBundle::new(work, || async move {
            let (effect, custody) = self.launch_original(payload, Some(work)).await
                .map_err(finite_dispatch::root_error)?;
            let custody = custody.ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Storage,
                reason: "finite native dispatch lost original effect custody",
            })?;
            Ok((effect, custody))
        })
    }

    fn reconcile_with_native_work<'a>(
        &'a self, attempt: crate::EffectAttemptId,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, Option<(EffectObservation, acyclic_fs::NativeAllocation)>>> {
        finite_dispatch::require_finite(work)?;
        self.require_work_family(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            self.receipt_original(attempt, Some(work)).await
                .map_err(finite_dispatch::root_error)?
                .map(NativeProcessReceipt::into_effect_with_native_custody)
                .transpose().map_err(finite_dispatch::root_error)
        })
    }
}


fn require_view_grants<P: StreamProvider>(
    owner: &TaskJournalOwner<P>,
    manifest: &NativeViewManifest,
) -> Result<()> {
    manifest.validate()?;
    for mapping in &manifest.volumes {
        owner.require_volume_grant(&mapping.volume, crate::conversation::VolumeOperation::Read)?;
        if mapping.writable {
            owner.require_volume_grant(&mapping.volume, crate::conversation::VolumeOperation::Write)?;
        }
    }
    Ok(())
}

async fn publish_view<P: StreamProvider>(
    owner: &TaskJournalOwner<P>,
    view: &dyn NativeProcessView<P>,
    admitted: &NativeViewManifest,
) -> std::result::Result<acyclic_fs::WorkCounters, NativePublicationFailure> {
    let failure = |error: Error| NativePublicationFailure {
        destination: None,
        completed: Vec::new(),
        message: error.to_string(),
        work: acyclic_fs::WorkCounters::default(),
    };
    owner.verify(false).await.map_err(failure)?;
    if view.manifest() != admitted {
        return Err(failure(Error::Conflict("native publication view changed".into())));
    }
    require_view_grants(owner, admitted).map_err(failure)?;
    let future = view.publish(NativeViewOwner::new(owner)).map_err(|(error, work)| {
        NativePublicationFailure { destination: None, completed: Vec::new(),
            message: error.to_string(), work }
    })?;
    let meter = future.work().clone();
    let work = future.await.map_err(|error| NativePublicationFailure {
        destination: None, completed: Vec::new(), message: error.to_string(), work: meter.receipt(),
    })??;
    if view.manifest() != admitted {
        return Err(NativePublicationFailure {
            destination: None,
            completed: admitted.volumes.iter().filter(|mapping| mapping.writable).cloned().collect(),
            message: "native publication view changed after publication".into(),
            work,
        });
    }
    owner.verify(false).await.map_err(|error| NativePublicationFailure {
        destination: None,
        completed: admitted.volumes.iter().filter(|mapping| mapping.writable).cloned().collect(),
        message: error.to_string(),
        work,
    })?;
    Ok(work)
}

fn native_work_error(error: acyclic_fs::WorkError) -> Error {
    Error::Storage(format!("native work accounting failed: {error}"))
}

fn retain_validation_result<T>(
    result: std::result::Result<acyclic_fs::OperationReceipt<T>, (Error, acyclic_fs::WorkCounters)>,
    work: &mut acyclic_fs::WorkCounters,
    frame_bytes: u64,
) -> Result<T> {
    let (value, spent) = match result {
        Ok(receipt) => (Ok(receipt.value), receipt.work),
        Err((error, work)) => (Err(error), work),
    };
    merge_execution_work(work, spent, frame_bytes)?;
    value
}

fn merge_execution_work(
    work: &mut acyclic_fs::WorkCounters,
    spent: acyclic_fs::WorkCounters,
    frame_bytes: u64,
) -> Result<()> {
    let overlap = frame_bytes.checked_add(spent.peak_allocation_bytes)
        .ok_or(acyclic_fs::WorkError::Overflow).map_err(native_work_error)?;
    *work = work.checked_add(spent).map_err(native_work_error)?;
    work.peak_allocation_bytes = work.peak_allocation_bytes.max(overlap);
    Ok(())
}

fn execution_remaining(
    work: acyclic_fs::WorkCounters,
    budget: acyclic_fs::WorkBudget,
    frame_bytes: u64,
) -> Result<acyclic_fs::WorkBudget> {
    let mut remaining = work.remaining(budget).map_err(native_work_error)?;
    remaining.peak_allocation_bytes = budget.peak_allocation_bytes.checked_sub(frame_bytes)
        .ok_or(acyclic_fs::WorkError::Overflow).map_err(native_work_error)?;
    Ok(remaining)
}

fn invocation_failure(error: Error, work: acyclic_fs::WorkCounters) -> NativeExecution {
    NativeExecution {
        status: EffectStatus::Indeterminate,
        diagnostic: None,
        publication_failure: None,
        invocation_failure: Some(error.to_string()),
        work,
    }
}

// Admission precedes construction: evaluating the large installed execution
// future on the caller's stack would defeat this necessary heap boundary.
#[inline(never)]
fn box_execution<F: Future>(
    build: impl FnOnce(acyclic_fs::WorkCounters) -> F,
    budget: acyclic_fs::WorkBudget,
    cancellation: &acyclic_fs::CancellationToken,
) -> std::result::Result<std::pin::Pin<Box<F>>, NativeExecution> {
    let mut work = acyclic_fs::WorkCounters::default();
    if let Err(error) = cancellation.check() {
        return Err(invocation_failure(Error::Storage(error.to_string()), work));
    }
    let bytes = std::mem::size_of::<F>() as u64;
    if let Err(error) = work.charge_allocation(u64::from(bytes != 0), bytes, &budget) {
        return Err(invocation_failure(native_work_error(error), work));
    }
    Ok(Box::pin(build(work)))
}


fn receipt_path(attempt: crate::EffectAttemptId) -> Result<StreamPath> {
    use std::fmt::Write as _;
    let mut path = String::new();
    // The ordinary path retains its exact String construction policy.
    // Native callers use the intrinsically guarded StreamPath constructor.
    path.try_reserve_exact("harness/v2/native-process/".len() + 36)
        .map_err(acyclic_fs::NativeTreeMaterializationError::AllocationFailed)
        .map_err(|cause| Error::NativeWork { cause, custody: None })?;
    write!(&mut path, "harness/v2/native-process/{attempt}")
        .map_err(|_| Error::NativeBoundary {
            code: crate::wire::ErrorCode::Storage, reason: "native receipt path formatting failed",
        })?;
    StreamPath::new(path).map_err(|source| Error::NativeStream {
        code: crate::wire::ErrorCode::Storage, source, custody: None,
    })
}



#[cfg(test)]
mod execution_admission_tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn installed_execution_frame_is_admitted_before_constructing_its_future() {
        let constructed = AtomicBool::new(false);
        let cancellation = acyclic_fs::CancellationToken::new();
        let failure = box_execution(
            |_| {
                constructed.store(true, Ordering::SeqCst);
                std::future::ready(invocation_failure(
                    Error::Storage("unreachable execution".into()),
                    acyclic_fs::WorkCounters::default(),
                ))
            },
            acyclic_fs::WorkBudget::UNCHARGED,
            &cancellation,
        ).err().expect("the real execution frame requires owned storage");
        assert!(!constructed.load(Ordering::SeqCst));
        assert_eq!(failure.work, acyclic_fs::WorkCounters::default());
        assert!(failure.invocation_failure.is_some());
    }

    #[test]
    fn original_execution_cancellation_prevents_future_construction() {
        let constructed = AtomicBool::new(false);
        let cancellation = acyclic_fs::CancellationToken::new();
        cancellation.cancel();
        let failure = box_execution(
            |_| {
                constructed.store(true, Ordering::SeqCst);
                std::future::ready(invocation_failure(
                    Error::Storage("unreachable execution".into()),
                    acyclic_fs::WorkCounters::default(),
                ))
            },
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
        ).err().expect("original cancellation must reject execution admission");
        assert!(!constructed.load(Ordering::SeqCst));
        assert_eq!(failure.work, acyclic_fs::WorkCounters::default());
        assert!(failure.invocation_failure.is_some());
    }
}
