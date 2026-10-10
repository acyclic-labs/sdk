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
    process::{Command, Stdio},
    sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}},
    time::Duration,
};

mod receipt_observation;
#[cfg(test)]
mod receipt_observation_tests;

impl NativeProcessRequest {
    fn validate(&self) -> Result<()> {
        if self.standard_input.is_some() && self.mcp_stdio.is_some() {
            return Err(Error::Invalid("literal stdin and MCP exchange are mutually exclusive".into()));
        }
        if let Some(exchange) = &self.mcp_stdio {
            exchange.validate()?;
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
            return Err(Error::Invalid(
                "invalid or changed native process invocation".into(),
            ));
        }
        #[cfg(windows)]
        if self
            .environment
            .keys()
            .map(|key| key.to_uppercase())
            .collect::<BTreeSet<_>>()
            .len()
            != self.environment.len()
        {
            return Err(Error::Invalid(
                "duplicate native environment key under Windows case folding".into(),
            ));
        }
        Ok(())
    }

    fn validate_native_paths(&self, view: &NativeViewManifest) -> Result<()> {
        self.validate()?;
        if &self.view != view {
            return Err(Error::Conflict("native command view changed".into()));
        }
        // This is only the locator grammar. The prepared view checks its
        // original held root and each no-follow directory before capture.
        let relative = self.cwd.strip_prefix(&view.options.root)
            .map_err(|_| Error::Conflict("native cwd is outside the selected locator".into()))?;
        if relative.components().any(|part| !matches!(part, std::path::Component::Normal(_))) {
            return Err(Error::Invalid("native cwd has a non-relative component".into()));
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
}

/// Optional provider for one retained task command. Construction launches nothing.
/// Ordinary host processes are trusted effects, not a confinement boundary.
pub struct NativeProcessProvider<P: StreamProvider> {
    owner: Arc<TaskJournalOwner<P>>,
    command: OperationId,
    plan: TaskEffectPlan,
    admission: NativeAdmission,
    view: Option<Arc<dyn NativeProcessView<P>>>,
    approval: InteractionId,
    authority: Arc<NativeProcessAuthority>,
    supervisor: Arc<NativeSupervisorControl>,
    effect_id: crate::EffectId,
    history: HistoryReader<P>,
    _custody: Option<acyclic_fs::NativeAllocation>,
}

enum NativeAdmission {
    Executable {
        request: Arc<NativeProcessRequest>,
        result_schema: serde_json::Value,
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

struct NativeSupervisorControl {
    active: Mutex<Option<NativeActiveAttempt>>,
    completion: tokio::sync::Notify,
}

impl NativeSupervisorControl {
    fn new() -> Self {
        Self { active: Mutex::new(None), completion: tokio::sync::Notify::new() }
    }

    fn begin(&self, attempt: crate::EffectAttemptId) -> Result<()> {
        let mut active = self.active.lock()
            .map_err(|_| Error::Storage("native supervisor control was poisoned".into()))?;
        if active.is_some() {
            return Err(Error::Conflict("native provider already owns an original supervisor".into()));
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
            diagnostic, publication_failure, invocation_failure, work: _, _custody,
        } = self;
        let mut custody = _custody
            .ok_or(NativeReceiptError::Record("native receipt has no original measured custody"))?;
        drop(original_fence);
        drop(diagnostic);
        drop(publication_failure);
        drop(invocation_failure);
        custody.resize_retained(receipt_observation::effect_bytes(&effect)?)
            .map_err(NativeReceiptError::Native)?;
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
        super::native_execution::native_future_typed(|| async {
            cleanup_cancellation.check().map_err(acyclic_fs::NativeTreeMaterializationError::from)
                .map_err(NativeReceiptError::Native)?;
            work.charge(acyclic_fs::WorkCounters {
                items_examined: 1, ..acyclic_fs::WorkCounters::default()
            }).map_err(NativeReceiptError::Native)?;
            let (completed, _) = self.supervisor.snapshot(attempt)?;
            if completed { return Ok(()); }
            // The original current fence may settle after task cancellation;
            // this check confers no new dispatch or filesystem write authority.
            self.owner.verify_with_native_work(true, work).await?;
            cleanup_cancellation.check().map_err(acyclic_fs::NativeTreeMaterializationError::from)
                .map_err(NativeReceiptError::Native)?;
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
        }, work).map_err(NativeReceiptError::Native)?.await
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
        super::native_execution::native_future_typed(|| async {
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
                if completed { break phase; }
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
            if !matches!(phase, NativeCapturePhase::Pending | NativeCapturePhase::Drained) {
                return Err(NativeReceiptError::Record("original physical process collection remains uncertain"));
            }
            Ok(receipt)
        }, work).map_err(NativeReceiptError::Native)?.await
    }

    /// Queries the same fenced launch/observation journal used by reconciliation.
    pub async fn receipt(
        &self,
        attempt: crate::EffectAttemptId,
    ) -> Result<Option<NativeProcessReceipt>> {
        self.receipt_original(attempt, None).await.map_err(receipt_observation::ordinary_error)
    }

    async fn effect_state(&self, effect: crate::EffectId) -> Result<Option<crate::core::EffectState>> {
        let cursor = self.history.pin(0).await?;
        self.history.effect(&cursor, effect, self.authority.history_limits).await
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
        Self::bind(owner, command, plan, Some(view), approval, authority).await
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
        Self::bind(owner, command, plan, None, approval, authority).await
    }

    async fn bind(
        owner: Arc<TaskJournalOwner<P>>,
        command: OperationId,
        plan: TaskEffectPlan,
        view: Option<Arc<dyn NativeProcessView<P>>>,
        approval: InteractionId,
        authority: NativeProcessAuthority,
    ) -> Result<Self> {
        owner.verify(true).await?;
        if authority.history_limits.maximum_events == 0 || authority.history_limits.maximum_bytes == 0 {
            return Err(Error::Invalid("native effect history allowances must be finite and positive".into()));
        }
        owner.require_effect_grants(&plan.provider, false)?;
        owner.validate_input_file(&plan.request)?;
        owner.validate_input_file(&plan.result_schema)?;
        if plan.effect_kind != NATIVE_PROCESS_EFFECT_KIND
            || plan.guarantee != EffectGuarantee::AtMostOnce
        {
            return Err(Error::Invalid(
                "native process requires its one-shot at-most-once contract".into(),
            ));
        }
        if plan.request.descriptor().media_type() != "application/json" {
            return Err(Error::Invalid("native request must be JSON content".into()));
        }
        let bytes = authority.content.read(&plan.request).await?;
        plan.request.descriptor().verify(&bytes)?;
        let request: NativeProcessRequest = crate::contract::json_from_slice(&bytes)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        require_view_grants(&owner, &request.view)?;
        request.validate()?;
        if request
            .view
            .volumes
            .iter()
            .any(|mapping| &mapping.volume == authority.results.volume())
        {
            return Err(Error::Unsupported(
                "native result storage must be separate from materialized volumes".into(),
            ));
        }
        if u64::from(request.maximum_result_bytes) > owner.input_limits().file_bytes {
            return Err(Error::Unauthorized(
                "native result bound exceeds retained task file allowance".into(),
            ));
        }
        if request.standard_input.as_ref().is_some_and(|input| input.len() as u64 > owner.input_limits().file_bytes) {
            return Err(Error::Unauthorized("native stdin exceeds retained task file allowance".into()));
        }
        if let Some(view) = &view {
            request.validate_native_paths(view.manifest())?;
        }
        let schema = authority.content.read(&plan.result_schema).await?;
        let result_schema = crate::effects::validate_schema_bytes(&plan.result_schema, &schema)?;
        owner.require_volume_grant(
            authority.results.volume(),
            crate::conversation::VolumeOperation::Write,
        )?;
        owner.require_volume_grant(
            authority.results.volume(),
            crate::conversation::VolumeOperation::Read,
        )?;
        let approval_digest = request.approval_digest(owner.task_binding().0, command)?;
        let effect_id = task_effect_id(owner.task_binding().0, command)?;
        let history = HistoryReader::new(
            &owner.stream(), authority.verifier.audience(), authority.verifier.clone(),
        )?;
        authority
            .approvals
            .verify(
                approval,
                command,
                approval_digest,
            )
            .await?;
        Ok(Self {
            owner,
            command,
            plan,
            admission: NativeAdmission::Executable {
                request: Arc::new(request), result_schema, approval_digest,
            },
            view,
            approval,
            authority: Arc::new(authority),
            // This actual host/provider control is constructed once at bind,
            // outside finite per-invocation/query budgets. It is not authority.
            supervisor: Arc::new(NativeSupervisorControl::new()),
            effect_id,
            history,
            _custody: None,
        })
    }

    fn executable_admission(&self) -> Result<(&Arc<NativeProcessRequest>, &serde_json::Value, [u8; 32])> {
        match &self.admission {
            NativeAdmission::Executable { request, result_schema, approval_digest } =>
                Ok((request, result_schema, *approval_digest)),
            NativeAdmission::ReceiptOnly => Err(Error::Unsupported("receipt-only native recovery has no dispatch or result-loading capability".into())),
        }
    }

    async fn require_dispatch(&self, dispatch: &EffectDispatch) -> Result<()> {
        self.owner.verify(false).await?;
        let effect_id = task_effect_id(self.owner.task_binding().0, self.command)?;
        let state = self.effect_state(effect_id).await?.ok_or_else(|| {
            Error::Unauthorized("native command has no committed effect admission".into())
        })?;
        if EffectDispatch::from_state(effect_id, &state)? != *dispatch
            || state.provider != self.plan.provider
            || state.effect_kind != self.plan.effect_kind
            || state.guarantee != self.plan.guarantee
            || state.request != self.plan.request
            || state.result_schema != self.plan.result_schema
        {
            return Err(Error::Conflict(
                "native dispatch differs from retained command".into(),
            ));
        }
        let view = self.view.as_ref().ok_or_else(|| {
            Error::Unsupported("recovered native provider can only reconcile receipts".into())
        })?;
        let (request, _, approval_digest) = self.executable_admission()?;
        request.validate_native_paths(view.manifest())?;
        self.authority
            .approvals
            .verify(
                self.approval,
                self.command,
                approval_digest,
            )
            .await?;
        require_view_grants(&self.owner, &request.view)?;
        // Physical checks follow the original durable launch receipt, so a
        // pre-capture failure retains its spent work in the same journal.
        if view.manifest() != &request.view {
            return Err(Error::Conflict("native dispatch view changed".into()));
        }
        self.owner.verify(false).await.map(|_| ())
    }

    async fn launch(&self, dispatch: EffectDispatch) -> Result<EffectObservation> {
        self.require_dispatch(&dispatch).await?;
        let (task, fence) = self.owner.task_binding();
        let launch = LaunchReceipt {
            dispatch: Cow::Borrowed(&dispatch),
            plan: Cow::Borrowed(&self.plan),
            conversation: Cow::Borrowed(self.authority.verifier.audience()),
            task,
            command: self.command,
            fence: Cow::Borrowed(fence),
            approval: self.approval,
            approval_digest: self.executable_admission()?.2,
        };
        let path = receipt_path(dispatch.attempt_id)?;
        let existing = read_receipt(&self.owner, &path).await?;
        if let Some((retained, observed)) = existing {
            require_same_launch(&retained, &launch)?;
            return Ok(observed.map_or_else(
                || observation(&dispatch, EffectStatus::Indeterminate),
                |observed| observed.effect,
            ));
        }
        if !append_receipt(
            &self.owner,
            path.clone(),
            0,
            &Receipt::Launch(Box::new(launch.clone())),
            JournalWrite::Fresh,
        )
        .await?
        {
            let (retained, observed) = read_receipt(&self.owner, &path)
                .await?
                .ok_or_else(|| Error::Conflict("native launch receipt changed".into()))?;
            require_same_launch(&retained, &launch)?;
            return Ok(observed.map_or_else(
                || observation(&dispatch, EffectStatus::Indeterminate),
                |observed| observed.effect,
            ));
        }
        // The supervisor owns task/view/process capabilities independently of
        // this provider object and the caller's dispatch future. Dropping either
        // does not discard process cleanup or observation. Host death can still
        // leave an unknown OS outcome, which receipt reconciliation never replays.
        let owner = Arc::clone(&self.owner);
        let view = Arc::clone(self.view.as_ref().ok_or_else(|| {
            Error::Unsupported("native dispatch requires a prepared view".into())
        })?);
        let authority = Arc::clone(&self.authority);
        let (request, schema, _) = self.executable_admission()?;
        let request = Arc::clone(request);
        let schema = schema.clone();
        self.supervisor.begin(dispatch.attempt)?;
        let supervisor = Arc::clone(&self.supervisor);
        tokio::spawn(async move {
            let _completion = NativeSupervisorCompletion(Arc::clone(&supervisor));
            let execution = execute(&owner, view.as_ref(), &authority, &request, &dispatch, &schema, &supervisor).await;
            let observed = NativeObservation {
                effect: observation(&dispatch, execution.status),
                diagnostic: execution.diagnostic,
                publication_failure: execution.publication_failure,
                invocation_failure: execution.invocation_failure,
                work: execution.work,
            };
            if !append_receipt(
                &owner,
                path.clone(),
                1,
                &Receipt::Observed(Box::new(observed.clone())),
                JournalWrite::Settlement,
            )
            .await?
                && read_receipt(&owner, &path)
                    .await?
                    .and_then(|(_, observed)| observed)
                    .as_ref()
                    != Some(&observed)
            {
                return Err(Error::Conflict(
                    "native observed outcome was not retained".into(),
                ));
            }
            Ok(observed.effect)
        })
        .await
        .map_err(|error| Error::Storage(format!("native supervisor interrupted: {error}")))?
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
    fn linearizable_reconciliation(&self) -> bool {
        false
    }
    fn dispatch<'a>(
        &'a self,
        request: EffectDispatch,
    ) -> BoxProviderFuture<'a, Result<EffectObservation>> {
        Box::pin(self.launch(request))
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
}

async fn capture<P: StreamProvider + 'static>(
    owner: &Arc<TaskJournalOwner<P>>,
    request: &Arc<NativeProcessRequest>,
    supervisor: &Arc<NativeSupervisorControl>,
) -> Result<Option<NativeProcessResult>> {
    owner.verify(false).await?;
    let check_owner = Arc::clone(owner);
    let invocation = Arc::clone(request);
    let capture_supervisor = Arc::clone(supervisor);
    let runtime = tokio::runtime::Handle::current();
    let output = tokio::task::spawn_blocking(move || {
        let mut command = Command::new(&invocation.executable);
        command
            .args(&invocation.argv)
            .current_dir(&invocation.cwd)
            .env_clear()
            .envs(&invocation.environment)
            .stdin(if invocation.mcp_stdio.is_some() || invocation.standard_input.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let check = || {
            runtime
                .block_on(tokio::time::timeout(
                    Duration::from_millis(u64::from(invocation.control_timeout_ms)),
                    check_owner.verify(false),
                ))
                .map_err(|error| {
                    std::io::Error::new(std::io::ErrorKind::Interrupted, error.to_string())
                })?
                .map_err(|error| {
                    std::io::Error::new(std::io::ErrorKind::Interrupted, error.to_string())
                })
        };
        check()?;
        capture_supervisor.record_capture(NativeCapturePhase::Spawning)?;
        let mut process = acyclic_native_runtime::spawn_process_tree(&mut command)?;
        capture_supervisor.record_capture(NativeCapturePhase::Running)?;
        let mut next_check = std::time::Instant::now();
        let poll = || {
            capture_supervisor.poll_stop()?;
            if std::time::Instant::now() >= next_check {
                check()?;
                next_check = std::time::Instant::now()
                    + Duration::from_millis(u64::from(invocation.cancellation_poll_ms));
            }
            Ok(())
        };
        let timeout = Duration::from_millis(u64::from(invocation.timeout_ms));
        let maximum_output = invocation.maximum_output_bytes as usize;
        let output = if let Some(request) = &invocation.mcp_stdio {
            let (mut exchange, first) = request.exchange().map_err(|error| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, error.to_string())
            })?;
            process.wait_with_exchange_observed(
                timeout,
                maximum_output,
                request.maximum_bytes as usize,
                &first,
                poll,
                |chunk| {
                    let bytes = exchange.push(chunk).map_err(|error| {
                        std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string())
                    })?;
                    Ok(acyclic_native_runtime::ProcessInput {
                        bytes,
                        complete: exchange.is_complete(),
                    })
                },
            )
        } else if let Some(input) = &invocation.standard_input {
            process.wait_with_input_observed(timeout, maximum_output, input, poll)
        } else {
            process.wait_with_output_observed(timeout, maximum_output, poll)
        };
        Ok::<_, std::io::Error>(output)
    })
    .await
    .map_err(|error| Error::Storage(format!("native process observer interrupted: {error}")))?;
    let capture_phase = match &output {
        Ok(Ok(_)) => NativeCapturePhase::Drained,
        Ok(Err(failure)) if failure.cleanup_completed => NativeCapturePhase::Drained,
        _ => NativeCapturePhase::Uncertain,
    };
    supervisor.record_capture(capture_phase)
        .map_err(|error| Error::Storage(error.to_string()))?;
    let result = match output {
        Ok(Ok(output)) => NativeProcessResult {
            success: output.status.success(),
            exit_code: output.status.code(),
            stdout: output.stdout,
            stderr: output.stderr,
            stop: None,
        },
        Ok(Err(failure)) => NativeProcessResult {
            success: false,
            exit_code: failure.status.and_then(|status| status.code()),
            stdout: failure.stdout,
            stderr: failure.stderr,
            stop: Some(NativeProcessStop {
                kind: match failure.error.kind() {
                    std::io::ErrorKind::TimedOut => NativeProcessStopKind::Timeout,
                    std::io::ErrorKind::FileTooLarge => NativeProcessStopKind::OutputLimit,
                    std::io::ErrorKind::Interrupted => NativeProcessStopKind::ControlStopped,
                    _ => NativeProcessStopKind::CaptureFailed,
                },
                message: failure.error.to_string().chars().take(128).collect(),
                cleanup_completed: failure.cleanup_completed,
            }),
        },
        // A failed spawn cannot establish the native outcome.
        Err(_) => return Ok(None),
    };
    Ok(Some(result))
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
    let work = view.publish(NativeViewOwner::new(owner))?.await?;
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
    result: acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<T>, Error>,
    work: &mut acyclic_fs::WorkCounters,
    frame_bytes: u64,
) -> Result<T> {
    let (value, spent) = match result {
        Ok(receipt) => (Ok(receipt.value), receipt.work),
        Err(failure) => (Err(failure.error), *failure.work),
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
fn box_execution<F: std::future::Future<Output = NativeExecution>>(
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

async fn execute<P>(
    owner: &Arc<TaskJournalOwner<P>>,
    view: &dyn NativeProcessView<P>,
    authority: &NativeProcessAuthority,
    request: &Arc<NativeProcessRequest>,
    dispatch: &EffectDispatch,
    schema: &serde_json::Value,
    supervisor: &Arc<NativeSupervisorControl>,
) -> NativeExecution
where
    P: StreamProvider + 'static,
{
    let budget = match request.view.validation_budget() {
        Ok(budget) => budget,
        Err(error) => return invocation_failure(error, acyclic_fs::WorkCounters::default()),
    };
    match box_execution(
        |work| execute_in_heap(owner, view, authority, request, dispatch, schema, work, budget, supervisor),
        budget,
        view.operation_cancellation(),
    ) {
        Ok(future) => future.await,
        Err(failure) => failure,
    }
}

async fn execute_in_heap<P>(
    owner: &Arc<TaskJournalOwner<P>>,
    view: &dyn NativeProcessView<P>,
    authority: &NativeProcessAuthority,
    request: &Arc<NativeProcessRequest>,
    dispatch: &EffectDispatch,
    schema: &serde_json::Value,
    mut work: acyclic_fs::WorkCounters,
    validation_budget: acyclic_fs::WorkBudget,
    supervisor: &Arc<NativeSupervisorControl>,
) -> NativeExecution
where
    P: StreamProvider + 'static,
{
    let frame_bytes = work.peak_allocation_bytes;
    let result: Result<NativeExecution> = async {
    owner.verify(false).await?;
    require_view_grants(owner, &request.view)?;
    let available = execution_remaining(work, validation_budget, frame_bytes)?;
    let cwd = match view.validate_working_directory(&request.cwd, available) {
        Ok(future) => future.await,
        Err(failure) => Err(failure),
    };
    retain_validation_result(cwd, &mut work, frame_bytes)?;
    let remaining = execution_remaining(work, validation_budget, frame_bytes)?;
    let validation = match view.validate_for_dispatch(remaining) {
        Ok(future) => future.await,
        Err(failure) => Err(failure),
    };
    retain_validation_result(validation, &mut work, frame_bytes)?;
    work.verify(validation_budget).map_err(native_work_error)?;
    if view.manifest() != &request.view {
        return Err(Error::Conflict("native dispatch view changed after observation".into()));
    }
    owner.verify(false).await?;
    let Some(result) = capture(owner, request, supervisor).await? else {
        return Ok(NativeExecution {
            status: EffectStatus::Indeterminate,
            diagnostic: None,
            publication_failure: None,
            invocation_failure: Some("native capture did not retain an OS outcome".into()),
            work,
        });
    };
    let bytes = crate::contract::canonical_json_bytes(&result)?;
    if bytes.len() > request.maximum_result_bytes as usize {
        return Ok(NativeExecution {
            status: EffectStatus::Indeterminate,
            diagnostic: None,
            publication_failure: None,
            invocation_failure: Some("native result exceeded its admitted byte allowance".into()),
            work,
        });
    }
    crate::contract::validate_json_schema_value(
        schema,
        &serde_json::to_value(&result).map_err(|error| Error::Invalid(error.to_string()))?,
        "native process result",
    )?;
    let publish = result.stop.is_none();
    let cleanup_completed = result
        .stop
        .as_ref()
        .is_none_or(|stop| stop.cleanup_completed);
    let result = authority
        .results
        .stage(
            OperationId::from_bytes(dispatch.attempt_id.into_bytes()),
            "native-process/result.json",
            &bytes,
            "application/json",
            "Native process result",
        )
        .await?;
    if result.volume() != authority.results.volume() {
        return Err(Error::Unauthorized(
            "native result publisher returned another owner volume".into(),
        ));
    }
    crate::effects::validate_result_bytes(schema, &result, &bytes)?;
    if publish {
        match publish_view(owner, view, &request.view).await {
            Ok(spent) => merge_execution_work(&mut work, spent, frame_bytes)?,
            Err(mut failure) => {
                merge_execution_work(&mut work, failure.work, frame_bytes)?;
                failure.work = work;
                return Ok(NativeExecution {
                    status: EffectStatus::Indeterminate,
                    diagnostic: Some(result),
                    work: failure.work,
                    publication_failure: Some(failure),
                    invocation_failure: None,
                });
            }
        }
    }
    Ok(if cleanup_completed {
        NativeExecution {
            status: EffectStatus::Succeeded { result },
            diagnostic: None,
            publication_failure: None,
            invocation_failure: None,
            work,
        }
    } else {
        NativeExecution {
            status: EffectStatus::Indeterminate,
            diagnostic: Some(result),
            publication_failure: None,
            invocation_failure: None,
            work,
        }
    })
    }.await;
    result.unwrap_or_else(|error| invocation_failure(error, work))
}

fn receipt_path(attempt: crate::EffectAttemptId) -> Result<StreamPath> {
    Ok(StreamPath::new(format!(
        "harness/v2/native-process/{attempt}"
    ))?)
}

fn observation(dispatch: &EffectDispatch, status: EffectStatus) -> EffectObservation {
    EffectObservation {
        provider: dispatch.provider.clone(),
        effect_id: dispatch.effect_id,
        attempt_id: dispatch.attempt_id,
        request_digest: dispatch.request_digest,
        guarantee: dispatch.guarantee,
        status,
    }
}

fn require_same_launch(retained: &LaunchReceipt<'_>, requested: &LaunchReceipt<'_>) -> Result<()> {
    // A recovered task may hold a new lease; the original launch fence remains
    // evidence rather than permission to start again under the new fence.
    if retained.dispatch != requested.dispatch
        || retained.plan != requested.plan
        || retained.conversation != requested.conversation
        || retained.task != requested.task
        || retained.command != requested.command
        || retained.approval != requested.approval
        || retained.approval_digest != requested.approval_digest
    {
        return Err(Error::Conflict("native launch identity was reused".into()));
    }
    Ok(())
}

async fn read_receipt<P: StreamProvider>(
    owner: &TaskJournalOwner<P>,
    path: &StreamPath,
) -> Result<Option<(LaunchReceipt<'static>, Option<NativeObservation>)>> {
    receipt_observation::read(
        owner, path, None, OperationId::from_bytes([0; 16]),
    ).await
        .map(|receipt| receipt.map(|receipt| (receipt.launch, receipt.observed)))
        .map_err(receipt_observation::ordinary_error)
}

async fn append_receipt<P: StreamProvider>(
    owner: &TaskJournalOwner<P>,
    path: StreamPath,
    tail: u64,
    receipt: &Receipt<'_>,
    write: JournalWrite,
) -> Result<bool> {
    let bytes = crate::contract::canonical_json_bytes(receipt)?;
    if bytes.len() > acyclic_stream::MAX_RECORD_BYTES {
        return Err(Error::Invalid("native receipt exceeds Stream limit".into()));
    }
    let key = acyclic_stream::IdempotencyKey::new(Bytes::copy_from_slice(
        blake3::hash(&bytes).as_bytes(),
    ))?;
    owner
        .append(path, tail, &key, Bytes::from(bytes), write)
        .await
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "owned I/O error callback used by map_err"
)]
fn io_error(error: std::io::Error) -> Error {
    Error::Storage(error.to_string())
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
