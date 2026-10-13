//! Approved one-shot process effects, using the task's existing fenced Stream.

use super::{
    NATIVE_PROCESS_EFFECT_KIND, NativeProcessRequest, NativeProcessResult, NativeProcessStop,
    NativeProcessStopKind, NativeViewManifest, NativeVolumeView,
};
use crate::{
    Error, InteractionId, OperationId, Result,
    conversation::{ContentPublisher, ContentResidencyVerifier},
    core::{AuthorityVerifier, EffectGuarantee, EffectStatus, SchemaRegistry},
    distributed::JournalWrite,
    durable_host::TaskJournalOwner,
    effect_host::{TaskEffectPlan, task_effect_id},
    effects::{EffectDispatch, EffectObservation, EffectProvider},
    store::StreamAggregate,
    tool::ToolApprovalVerifier,
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_stream::{BoxProviderFuture, StreamError, StreamPath, StreamProvider};
use bytes::Bytes;
use futures::TryStreamExt as _;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    process::{Command, Stdio},
    sync::Arc,
    time::Duration,
};

impl NativeProcessRequest {
    fn validate(&self) -> Result<()> {
        if let Some(exchange) = &self.mcp_stdio {
            drop(exchange.exchange(None)?);
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
        if &self.view != view
            || !std::fs::canonicalize(&self.cwd)
                .map_err(io_error)?
                .starts_with(&view.options.root)
        {
            return Err(Error::Conflict("native command view or cwd changed".into()));
        }
        if !std::fs::metadata(&self.executable)
            .map_err(io_error)?
            .is_file()
            || !std::fs::metadata(&self.cwd).map_err(io_error)?.is_dir()
        {
            return Err(Error::Invalid(
                "native executable or cwd is unavailable".into(),
            ));
        }
        Ok(())
    }
}

/// Host-only capabilities for checking committed admission and staging results.
pub struct NativeProcessAuthority {
    /// Original conversation authority verifier.
    pub verifier: AuthorityVerifier,
    /// Same core schemas used by the owning conversation.
    pub schemas: SchemaRegistry,
    /// Owner-authenticated immutable request/schema reader.
    pub content: Arc<dyn ContentResidencyVerifier>,
    /// Owner-bound result publisher; it retains the result before returning.
    pub results: Arc<dyn ContentPublisher>,
    /// Existing resolved-approval ledger verifier.
    pub approvals: Arc<dyn ToolApprovalVerifier>,
}

/// Optional provider for one retained task command. Construction launches nothing.
pub struct NativeProcessProvider<P, A, O> {
    owner: Arc<TaskJournalOwner<P>>,
    command: OperationId,
    plan: TaskEffectPlan,
    request: NativeProcessRequest,
    view: Option<Arc<NativeVolumeView<A, O>>>,
    approval: InteractionId,
    authority: Arc<NativeProcessAuthority>,
    result_schema: acyclic_stream::OwnedBytes,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LaunchReceipt {
    dispatch: EffectDispatch,
    plan: TaskEffectPlan,
    conversation: crate::core::Authority,
    task: crate::TaskId,
    command: OperationId,
    fence: crate::scheduler::LeaseFence,
    approval: InteractionId,
    approval_digest: [u8; 32],
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Receipt {
    Launch(Box<LaunchReceipt>),
    Observed(Box<NativeObservation>),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeObservation {
    effect: EffectObservation,
    diagnostic: Option<crate::conversation::FileRef>,
}

impl<P, A, O> NativeProcessProvider<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    /// Reads retained output after authenticating this exact attempt.
    pub async fn output(
        &self,
        attempt: crate::EffectAttemptId,
    ) -> Result<Option<crate::conversation::FileRef>> {
        if self.reconcile(attempt).await?.is_none() {
            return Ok(None);
        }
        let Some((_, Some(observed))) = read_receipt(&self.owner, &receipt_path(attempt)?).await?
        else {
            return Ok(None);
        };
        let reference = match observed.effect.status {
            EffectStatus::Succeeded { result } => Some(result),
            _ => observed.diagnostic,
        };
        if let Some(reference) = &reference {
            self.owner.validate_input_file(reference, None)?;
            if reference.volume() != self.authority.results.volume()
                || reference.descriptor().byte_length()
                    > u64::from(self.request.maximum_result_bytes)
            {
                return Err(Error::Unauthorized(
                    "native output differs from admitted result owner or bound".into(),
                ));
            }
            crate::effects::validate_result_bytes(
                &self.plan.result_schema,
                &self.result_schema,
                reference,
                &self
                    .authority
                    .content
                    .read(
                        reference,
                        u64::from(self.request.maximum_result_bytes),
                        None,
                    )?
                    .await?,
                &mut None,
            )?;
        }
        Ok(reference)
    }

    /// Binds one command, with its native view when dispatch is available.
    pub async fn new(
        owner: Arc<TaskJournalOwner<P>>,
        command: OperationId,
        plan: TaskEffectPlan,
        view: Option<Arc<NativeVolumeView<A, O>>>,
        approval: InteractionId,
        authority: NativeProcessAuthority,
    ) -> Result<Self> {
        if view.is_some() {
            owner.verify(false, &mut None).await?;
        }
        owner.verify(true, &mut None).await?;
        owner.require_effect_grants(&plan.provider, false, &mut None)?;
        owner.validate_input_file(&plan.request, None)?;
        owner.validate_input_file(&plan.result_schema, None)?;
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
        let bytes = authority
            .content
            .read(&plan.request, owner.input_limits().file_bytes, None)?
            .await?;
        plan.request.descriptor().verify(&bytes, &mut None)?;
        let request: NativeProcessRequest = crate::contract::json_from_slice(&bytes)
            .map_err(|error| Error::Invalid(error.to_string()))?;
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
        if let Some(view) = &view {
            request.validate_native_paths(view.manifest())?;
        }
        let result_schema = authority
            .content
            .read(&plan.result_schema, owner.input_limits().file_bytes, None)?
            .await?;
        crate::effects::validate_schema_bytes(&plan.result_schema, &result_schema, &mut None)?;
        owner.require_volume_grant(
            authority.results.volume(),
            crate::conversation::VolumeOperation::Write,
            &mut None,
        )?;
        owner.require_volume_grant(
            authority.results.volume(),
            crate::conversation::VolumeOperation::Read,
            &mut None,
        )?;
        authority
            .approvals
            .verify(
                approval,
                command,
                request.approval_digest(owner.task_binding().0, command, None)?,
            )
            .await?;
        Ok(Self {
            owner,
            command,
            plan,
            request,
            view,
            approval,
            authority: Arc::new(authority),
            result_schema,
        })
    }

    async fn aggregate(&self) -> Result<StreamAggregate<P>> {
        StreamAggregate::open(
            &self.owner.stream(),
            self.authority.verifier.audience().clone(),
            self.authority.verifier.clone(),
            self.authority.schemas.clone(),
            None,
            crate::store::default_projection_read_limits(),
            crate::store::DEFAULT_TERMINAL_EFFECTS,
            None,
            &mut None,
        )
        .await
    }

    async fn require_dispatch(&self, dispatch: &EffectDispatch) -> Result<()> {
        self.owner.verify(false, &mut None).await?;
        let effect_id = task_effect_id(self.owner.task_binding().0, self.command, &mut None)?;
        let aggregate = self.aggregate().await?;
        let state = aggregate.reducer().effect(effect_id).ok_or_else(|| {
            Error::Unauthorized("native command has no committed effect admission".into())
        })?;
        if EffectDispatch::from_state(effect_id, state)? != *dispatch
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
        self.request.validate_native_paths(view.manifest())?;
        self.authority
            .approvals
            .verify(
                self.approval,
                self.command,
                self.request
                    .approval_digest(self.owner.task_binding().0, self.command, None)?,
            )
            .await?;
        view.validate_for_dispatch().await
    }

    async fn launch(&self, dispatch: EffectDispatch) -> Result<EffectObservation> {
        self.require_dispatch(&dispatch).await?;
        let (task, fence) = self.owner.task_binding();
        let launch = LaunchReceipt {
            dispatch: dispatch.clone(),
            plan: self.plan.clone(),
            conversation: self.authority.verifier.audience().clone(),
            task,
            command: self.command,
            fence: fence.clone_with_work(&mut None)?,
            approval: self.approval,
            approval_digest: self.request.approval_digest(task, self.command, None)?,
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
        let owner = Arc::clone(&self.owner);
        let view = Arc::clone(self.view.as_ref().ok_or_else(|| {
            Error::Unsupported("native dispatch requires a prepared view".into())
        })?);
        let authority = Arc::clone(&self.authority);
        let request = self.request.clone();
        let schema = (self.plan.result_schema.clone(), self.result_schema.clone());
        tokio::spawn(async move {
            let (status, diagnostic) =
                execute(&owner, &view, &authority, &request, &dispatch, &schema)
                    .await
                    .unwrap_or((EffectStatus::Indeterminate, None));
            let observed = NativeObservation {
                effect: observation(&dispatch, status),
                diagnostic,
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

impl<P, A, O> EffectProvider for NativeProcessProvider<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
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
            self.owner.verify(true, &mut None).await?;
            let Some((launch, observed)) =
                read_receipt(&self.owner, &receipt_path(attempt)?).await?
            else {
                return Ok(None);
            };
            if launch.task != self.owner.task_binding().0
                || launch.command != self.command
                || launch.plan != self.plan
                || &launch.conversation != self.authority.verifier.audience()
                || launch.dispatch.effect_id
                    != task_effect_id(launch.task, self.command, &mut None)?
                || launch.dispatch.attempt_id != attempt
                || launch.dispatch.provider != self.plan.provider
                || launch.dispatch.request != self.plan.request
                || launch.dispatch.effect_kind != self.plan.effect_kind
                || launch.dispatch.guarantee != self.plan.guarantee
                || launch.approval != self.approval
                || launch.approval_digest
                    != self
                        .request
                        .approval_digest(launch.task, self.command, None)?
            {
                return Err(Error::Conflict(
                    "native receipt belongs to a different admission".into(),
                ));
            }
            let aggregate = self.aggregate().await?;
            let state = aggregate
                .reducer()
                .effect(launch.dispatch.effect_id)
                .ok_or_else(|| {
                    Error::Unauthorized("native receipt has no retained core effect".into())
                })?;
            if state.request_digest != launch.dispatch.request_digest
                || state.request != launch.plan.request
                || state.provider != launch.plan.provider
                || state.effect_kind != launch.plan.effect_kind
                || state.guarantee != launch.plan.guarantee
                || state.result_schema != launch.plan.result_schema
                || state.attempts.last() != Some(&attempt)
            {
                return Err(Error::Conflict(
                    "native receipt differs from its retained core effect".into(),
                ));
            }
            Ok(Some(observed.map_or_else(
                || observation(&launch.dispatch, EffectStatus::Indeterminate),
                |observed| observed.effect,
            )))
        })
    }
}

async fn capture<P: StreamProvider + 'static>(
    owner: &Arc<TaskJournalOwner<P>>,
    request: &NativeProcessRequest,
) -> Result<Option<NativeProcessResult>> {
    owner.verify(false, &mut None).await?;
    let check_owner = Arc::clone(owner);
    let invocation = request.clone();
    let runtime = tokio::runtime::Handle::current();
    let output = tokio::task::spawn_blocking(move || {
        let mut command = Command::new(&invocation.executable);
        command
            .args(&invocation.argv)
            .current_dir(&invocation.cwd)
            .env_clear()
            .envs(&invocation.environment)
            .stdin(if invocation.mcp_stdio.is_some() {
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
                    check_owner.verify(false, &mut None),
                ))
                .map_err(|error| {
                    std::io::Error::new(std::io::ErrorKind::Interrupted, error.to_string())
                })?
                .map_err(|error| {
                    std::io::Error::new(std::io::ErrorKind::Interrupted, error.to_string())
                })
        };
        check()?;
        let mut process = acyclic_native_runtime::ProcessTree::spawn(&mut command)?;
        let mut next_check = std::time::Instant::now();
        let poll = || {
            if std::time::Instant::now() >= next_check {
                check()?;
                next_check = std::time::Instant::now()
                    + Duration::from_millis(u64::from(invocation.cancellation_poll_ms));
            }
            Ok(())
        };
        let timeout = Duration::from_millis(u64::from(invocation.timeout_ms));
        let maximum_output = invocation.maximum_output_bytes as usize;
        let exchange = invocation
            .mcp_stdio
            .as_ref()
            .map(|request| request.exchange(None))
            .transpose()
            .map_err(|error| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, error.to_string())
            })?;
        let (mut exchange, first, _hold) = match exchange {
            Some((exchange, first, hold)) => (Some(exchange), first, hold),
            None => (None, Vec::new(), None),
        };
        let input = exchange
            .as_ref()
            .map(|_| acyclic_native_runtime::CaptureInput::Exchange {
                initial: &first,
                maximum: invocation.mcp_stdio.as_ref().unwrap().maximum_bytes as usize,
            });
        let output = process.wait_with_output(
            timeout,
            maximum_output,
            input,
            poll,
            |chunk| {
                let Some(exchange) = exchange.as_mut() else {
                    return Ok(Default::default());
                };
                let (bytes, _hold) = exchange.push(chunk, None).map_err(|error| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string())
                })?;
                Ok(acyclic_native_runtime::ProcessInput {
                    bytes,
                    complete: exchange.is_complete(),
                })
            },
            None,
            None,
        );
        Ok::<_, std::io::Error>(output)
    })
    .await
    .map_err(|error| Error::Storage(format!("native process observer interrupted: {error}")))?;
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
        Err(_) => return Ok(None),
    };
    Ok(Some(result))
}

async fn execute<P, A, O>(
    owner: &Arc<TaskJournalOwner<P>>,
    view: &NativeVolumeView<A, O>,
    authority: &NativeProcessAuthority,
    request: &NativeProcessRequest,
    dispatch: &EffectDispatch,
    schema: &(crate::conversation::FileRef, acyclic_stream::OwnedBytes),
) -> Result<(EffectStatus, Option<crate::conversation::FileRef>)>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    let Some(result) = capture(owner, request).await? else {
        return Ok((EffectStatus::Indeterminate, None));
    };
    let bytes = crate::contract::canonical_json_bytes(&result)?;
    if bytes.len() > request.maximum_result_bytes as usize {
        return Ok((EffectStatus::Indeterminate, None));
    }
    crate::executor::validate_json_schema_value(
        &crate::contract::json_from_slice(&schema.1)
            .map_err(|error| Error::Invalid(error.to_string()))?,
        &serde_json::to_value(&result).map_err(|error| Error::Invalid(error.to_string()))?,
        "native process result",
        &mut None,
    )?;
    let publish = result.stop.is_none();
    let cleanup_completed = result
        .stop
        .as_ref()
        .is_none_or(|stop| stop.cleanup_completed);
    let (result, _result_hold) = authority
        .results
        .stage(
            OperationId::from_bytes(dispatch.attempt_id.into_bytes()),
            "native-process/result.json",
            &bytes,
            "application/json",
            "Native process result",
            None,
        )?
        .await?;
    if result.volume() != authority.results.volume() {
        return Err(Error::Unauthorized(
            "native result publisher returned another owner volume".into(),
        ));
    }
    crate::effects::validate_result_bytes(&schema.0, &schema.1, &result, &bytes, &mut None)?;
    if publish && view.publish(owner).await.is_err() {
        return Ok((EffectStatus::Indeterminate, Some(result)));
    }
    Ok(if cleanup_completed {
        (EffectStatus::Succeeded { result }, None)
    } else {
        (EffectStatus::Indeterminate, Some(result))
    })
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

fn require_same_launch(retained: &LaunchReceipt, requested: &LaunchReceipt) -> Result<()> {
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
) -> Result<Option<(LaunchReceipt, Option<NativeObservation>)>> {
    let stream = acyclic_stream::Stream::new(owner.stream(), path.as_str())?;
    let bounds = match stream.bounds().await {
        Ok(bounds) => bounds,
        Err(StreamError::NotFound) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if !(1..=2).contains(&bounds.tail) {
        return Err(Error::Storage("invalid native receipt length".into()));
    }
    let limit = u32::try_from(bounds.tail).map_err(|error| Error::Storage(error.to_string()))?;
    let mut records = stream.read(0, limit).await?;
    let first = records
        .try_next()
        .await?
        .ok_or_else(|| Error::Storage("missing native launch receipt".into()))?;
    let Receipt::Launch(launch) = crate::contract::json_from_slice(&first.value)
        .map_err(|error| Error::Storage(error.to_string()))?
    else {
        return Err(Error::Storage("native launch receipt required".into()));
    };
    if first.sequence != 0 {
        return Err(Error::Storage("native receipt starts after launch".into()));
    }
    let observed = match records.try_next().await? {
        Some(record) => {
            let Receipt::Observed(observed) = crate::contract::json_from_slice(&record.value)
                .map_err(|error| Error::Storage(error.to_string()))?
            else {
                return Err(Error::Storage("native observed receipt required".into()));
            };
            if record.sequence != 1
                || observation(&launch.dispatch, observed.effect.status.clone()) != observed.effect
                || (observed.diagnostic.is_some()
                    && observed.effect.status != EffectStatus::Indeterminate)
                || matches!(
                    observed.effect.status,
                    EffectStatus::Planned | EffectStatus::Dispatched
                )
            {
                return Err(Error::Storage(
                    "native observation identity is invalid".into(),
                ));
            }
            Some(*observed)
        }
        None => None,
    };
    if bounds.tail != 1 + u64::from(observed.is_some()) || records.try_next().await?.is_some() {
        return Err(Error::Storage("native receipt sequence is invalid".into()));
    }
    Ok(Some((*launch, observed)))
}

async fn append_receipt<P: StreamProvider>(
    owner: &TaskJournalOwner<P>,
    path: StreamPath,
    tail: u64,
    receipt: &Receipt,
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
        .append(
            path,
            tail,
            &key,
            acyclic_stream::OwnedBytes::from(bytes),
            write,
        )
        .await
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "owned I/O error callback used by map_err"
)]
fn io_error(error: std::io::Error) -> Error {
    Error::Storage(error.to_string())
}
