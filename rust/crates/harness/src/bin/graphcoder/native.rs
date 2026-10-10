//! Native composition of existing local providers, task admission and wire handles.
//! No UI state is persisted; all admission, ownership and recovery are Harness-owned.
#![allow(missing_docs, reason = "private binary configuration and composition, not a public SDK contract")]

use acyclic_fs::{LocalAuthorityBackend, LocalObjectBackend};
use acyclic_harness::{
    Admission, Error, OperationId, Result, TaskId,
    conversation::{ContentGrant, FileRef, Limits, VolumeRef},
    core::{Authority, AuthorityIssuer, AuthorityVerifier, SchemaRegistry, Scope},
    distributed::{WorkLease, WorkPull, Worker},
    executor::TurnOutput,
    filesystem::{FilesystemContentVerifier, FilesystemForkVerifier, FilesystemHost,
        FilesystemProjectMergeVerifier, FilesystemTaskRuntime, StockTurnMachine,
        StreamHistoryForkVerifier, TaskWorkerAttempt, TaskWorkerOutcome},
    fork::CompositeForkVerifier,
    model::{Model, ModelContent},
    resources::ProviderRef,
    runtime::{DurableTaskHost as _, RuntimeScope, RuntimeTask, TaskRegistry, TaskRunLimits},
    scheduler::SessionLimits,
    store::{HistoryReadLimits, HistoryReader, StreamAggregate},
    wire,
    wire_api::{HarnessWireApi, OperationControlRequest, negotiate,
        operation_status, validate_cancel_request, validate_observe_request, validate_resume_protocol},
    wire_codec::{decode_command_envelope, encode_authority, encode_event_envelope},
    workflow::{MachineRegistry, WorkflowCommand},
};
use acyclic_stream::{BoxProviderFuture, BoxProviderStream, LocalStream, StreamClient};
use futures::stream;
use serde::Deserialize;
use std::{path::PathBuf, sync::Arc};

use super::terminal::{MAX_FRAME_BYTES, MAX_PAGE_EVENTS};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityBinding {
    pub authority: Authority,
    pub issuer: String,
    pub key: [u8; 32],
    pub scope: Scope,
}

impl AuthorityBinding {
    fn verifier(&self) -> Result<AuthorityVerifier> {
        let verifier = AuthorityIssuer::new(&self.issuer, self.key, self.authority.clone()).verifier();
        verifier.verify(&self.scope)?;
        Ok(verifier)
    }
}

/// A producer-owned explicit local composition. Never derived from environment
/// credentials, user-facing task labels, fork IDs or display cursor state.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub filesystem_root: PathBuf,
    pub stream_root: PathBuf,
    pub filesystem_provider: ProviderRef,
    pub stream_provider: ProviderRef,
    pub private_volume: VolumeRef,
    pub owner: AuthorityBinding,
    pub observed_authorities: Vec<AuthorityBinding>,
    pub generation: String,
    pub limits: Limits,
    pub run_limits: TaskRunLimits,
    pub session_limits: SessionLimits,
    pub concurrency: usize,
    #[serde(default)]
    pub deterministic_delay_ms: u32,
}

type Files = FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>;
type Runtime = FilesystemTaskRuntime<LocalStream, LocalAuthorityBackend, LocalObjectBackend>;

pub struct NativeSession {
    config: Config,
    stream: StreamClient<LocalStream>,
    files: Arc<Files>,
    runtime: Runtime,
    model: Model,
}

impl NativeSession {
    pub async fn open(config: Config) -> Result<Self> {
        if !config.filesystem_root.is_absolute() || !config.stream_root.is_absolute()
            || config.generation.is_empty() || config.generation.len() > 128
            || config.observed_authorities.len() > 64 || config.concurrency == 0
            || config.deterministic_delay_ms > 5000
        {
            return Err(Error::Invalid("native composition requires absolute paths and finite bounds".into()));
        }
        config.limits.validate()?;
        let verifier = config.owner.verifier()?;
        for binding in &config.observed_authorities {
            binding.verifier()?;
        }
        let files = Arc::new(FilesystemHost::new(
            acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(&config.filesystem_root)).await
                .map_err(|error| Error::Storage(error.to_string()))?,
            config.filesystem_provider.clone(),
        )?);
        let stream = StreamClient::new(Arc::new(
            LocalStream::open(&config.stream_root, Default::default()).await
                .map_err(|error| Error::Storage(error.to_string()))?,
        ));
        let machine = Arc::new(StockTurnMachine::new());
        let mut tasks = TaskRegistry::default();
        tasks.register(machine.definition()?)?;
        let mut machines = MachineRegistry::default();
        machines.register(machine)?;
        let scope = RuntimeScope::new(config.owner.scope.capabilities().clone(), config.limits)?
            .with_run_limits(config.run_limits)?;
        let runtime = Runtime::open(
            stream.clone(), files.clone(), config.private_volume.clone(), verifier,
            config.owner.scope.clone(), scope, tasks, machines, Default::default(),
            config.session_limits, config.concurrency, MAX_FRAME_BYTES as u64,
        ).await?;
        let model = super::provider::model(config.deterministic_delay_ms);
        Ok(Self {
            config, stream, files, runtime,
            model,
        })
    }

    pub fn config(&self) -> Config { self.config.clone() }

    pub fn start_cursor(&self) -> wire::ReplayCursor {
        wire::ReplayCursor {
            authority: Some(encode_authority(&self.config.owner.authority)),
            generation: self.config.generation.clone(), revision: 0,
        }
    }

    fn binding(&self, authority: &wire::Authority) -> Result<&AuthorityBinding> {
        std::iter::once(&self.config.owner).chain(&self.config.observed_authorities)
            .find(|binding| encode_authority(&binding.authority) == *authority)
            .ok_or_else(|| Error::Unauthorized("authority is not in this native composition".into()))
    }

    /// Queue one typed stock task, inheriting from the actual retained parent
    /// context when selected. Admission is separate from /run; no worker starts
    /// as a side effect of displaying a prompt or reopening a provider.
    pub async fn queue(&self, operation: OperationId, command: WorkflowCommand,
        parent: Option<TaskId>) -> Result<Admission<RuntimeTask<TurnOutput>>>
    {
        let definition = self.runtime.harness().task("acyclic.stock_turn@1")?;
        if let Some(parent) = parent {
            let admission = self.runtime.task_host().observe_admission(parent).await?;
            self.runtime.harness().durable_context(parent, admission.operation_id).await?
                .admit(operation, &definition, command).await
        } else {
            self.runtime.harness().admit(operation, &definition, command, None).await
        }
    }

    pub fn prompt_command(text: &str, operation: OperationId) -> Result<WorkflowCommand> {
        Ok(WorkflowCommand {
            operation_id: operation,
            kind: acyclic_harness::filesystem::MODEL_TASK_COMMAND_KIND.into(),
            payload: serde_json::to_value(acyclic_harness::filesystem::ModelTaskCommand {
                input: ModelContent::Text(text.into()), selected_context: None,
                max_steps: 1, max_output_tokens: Some(256),
            }).map_err(|error| Error::Invalid(error.to_string()))?,
        })
    }

    pub async fn claim(&self, task: TaskId, worker: &Worker) -> Result<WorkPull> {
        let admission = self.runtime.task_host().observe_admission(task).await?;
        self.runtime.poll_task_wake(task).await?;
        self.runtime.task_host().pull_operation(worker, admission.operation_id).await
    }

    pub async fn cancel_task(&self, task: TaskId) -> Result<String> {
        let admission = self.runtime.task_host().observe_admission(task).await?;
        self.runtime.task_host().cancel(task).await?;
        Ok(format!("cancellation requested task={task} admission-operation={}; completion requires authoritative observation\n", admission.operation_id))
    }

    #[cfg(test)]
    pub async fn run(&self, task: TaskId, worker: &Worker, output: &mut impl std::io::Write) -> Result<String> {
        let lease = match self.claim(task, worker).await? {
            WorkPull::Idle => return Ok(format!("worker idle task={task}\n")),
            WorkPull::Claimed(lease) => lease,
            WorkPull::Unresolved { lease, error } => return render_attempt(TaskWorkerAttempt::Unresolved { lease, error }),
        };
        // External callers can retain this exact original lease even if the
        // process dies during dispatch. Hex preserves typed JSON without ever
        // emitting provider-controlled terminal controls.
        writeln!(output, "worker-claim task={task} lease-json-hex={}", lease_hex(&lease)?)
            .and_then(|()| output.flush()).map_err(|error| Error::Storage(error.to_string()))?;
        let commands = self.runtime.commands().with_model(
            self.model.clone(), Arc::new(super::provider::Deterministic::new(self.config.deterministic_delay_ms)),
            Default::default(), Default::default(),
        );
        render_attempt(self.runtime.resume_task(lease, &commands, 16).await)
    }

    pub async fn resume(&self, lease: WorkLease) -> Result<String> {
        let commands = self.runtime.commands().with_model(
            self.model.clone(), Arc::new(super::provider::Deterministic::new(self.config.deterministic_delay_ms)),
            Default::default(), Default::default(),
        );
        let attempt = self.runtime.resume_task(lease, &commands, 16).await;
        render_attempt(attempt)
    }

    pub async fn reconcile(&self, operation: OperationId) -> Result<String> {
        let found = self.runtime.harness().reconcile_admission(operation).await?;
        Ok(super::terminal::text(&format!("admission operation={operation} retained={found:?}\n")))
    }

    pub async fn children(&self, task: TaskId, revision: Option<u64>, after: Option<String>) -> Result<String> {
        let page = self.runtime.harness().children(task, revision, after, MAX_PAGE_EVENTS).await?;
        Ok(super::terminal::text(&format!("children parent-task={task} {page:?}\n")))
    }

    pub async fn outcome(&self, task: TaskId) -> Result<String> {
        let admission = self.runtime.task_host().observe_admission(task).await?;
        let outcome = self.runtime.task_host().outcome(task).await?;
        Ok(super::terminal::text(&format!("task={task} admission-operation={} outcome={outcome:?}\n", admission.operation_id)))
    }

    pub async fn read(&self, file: &FileRef, maximum: u64) -> Result<bytes::Bytes> {
        if maximum == 0 || maximum > MAX_FRAME_BYTES as u64 {
            return Err(Error::Invalid("file display exceeds terminal bounds".into()));
        }
        let grant = ContentGrant::verify_read(&self.config.owner.verifier()?, &self.config.owner.scope, file)?;
        self.files.read_content(file, &grant, maximum).await
    }
}

pub fn lease_hex(lease: &WorkLease) -> Result<String> {
    Ok(hex::encode(serde_json::to_vec(lease)
        .map_err(|error| Error::Invalid(error.to_string()))?))
}

pub fn render_attempt(attempt: TaskWorkerAttempt) -> Result<String> {
    match attempt {
        TaskWorkerAttempt::Progress(TaskWorkerOutcome::Completed { task }) => {
            Ok(format!("worker completed task={task}; observe /outcome for the authoritative result\n"))
        }
        TaskWorkerAttempt::Progress(TaskWorkerOutcome::Suspended { task, revision }) => {
            Ok(format!("worker suspended task={task} checkpoint={revision}; no active future retained\n"))
        }
        TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { lease }) => {
            Ok(format!("worker yielded lease-json-hex={}\n", lease_hex(&lease)?))
        }
        TaskWorkerAttempt::Progress(TaskWorkerOutcome::Reconciling { lease }) => {
            Ok(format!("worker reconciling lease-json-hex={}; no new work pulled\n", lease_hex(&lease)?))
        }
        TaskWorkerAttempt::Unresolved { lease, error } => {
            Ok(format!("worker unresolved lease-json-hex={} error={}\n",
                lease_hex(&lease)?, super::terminal::text(&error.to_string())))
        }
    }
}

impl HarnessWireApi for NativeSession {
    fn handshake<'a>(&'a self, request: wire::HandshakeRequest) -> BoxProviderFuture<'a, Result<wire::HandshakeResponse>> {
        Box::pin(async move { negotiate(&request, &wire::CapabilitySet::default()) })
    }

    fn submit<'a>(&'a self, envelope: wire::CommandEnvelope) -> BoxProviderFuture<'a, Result<wire::Admission>> {
        Box::pin(async move {
            let identity = envelope.operation.clone();
            let (authority, command) = decode_command_envelope(envelope)?;
            let wire_authority = encode_authority(&authority);
            let binding = self.binding(&wire_authority)?;
            let verifier = binding.verifier()?;
            let reader = Arc::new(FilesystemContentVerifier::new(
                self.files.clone(), verifier.clone(), binding.scope.clone(), MAX_FRAME_BYTES as u64,
            )?);
            let mut aggregate = StreamAggregate::open(&self.stream, authority, verifier, SchemaRegistry::new()).await?
                .with_content_verifier(reader)
                .with_fork_verifier(Arc::new(CompositeForkVerifier::new(vec![
                    Arc::new(FilesystemForkVerifier::new(self.files.clone(), MAX_FRAME_BYTES as u64)?),
                    Arc::new(StreamHistoryForkVerifier::new(self.config.stream_provider.clone())?),
                ])?))
                .with_merge_verifier(Arc::new(FilesystemProjectMergeVerifier::new(self.files.clone())));
            let (state, error) = match aggregate.execute(command).await {
                Ok(_) => (wire::AdmissionState::Accepted, None),
                Err(error @ Error::Indeterminate(_)) => (wire::AdmissionState::Indeterminate, Some(acyclic_harness::encode_error(&error))),
                Err(error) => (wire::AdmissionState::Rejected, Some(acyclic_harness::encode_error(&error))),
            };
            Ok(wire::Admission { operation: identity, state: state as i32, error })
        })
    }

    fn replay<'a>(&'a self, request: wire::ResumeRequest) -> BoxProviderFuture<'a, Result<BoxProviderStream<'static, Result<wire::Delivery>>>> {
        Box::pin(async move {
            validate_resume_protocol(&request)?;
            if request.cursors.len() != 1 {
                return Err(Error::Invalid("terminal inspection requires one explicit aggregate cursor".into()));
            }
            let cursor = request.cursors.into_iter().next()
                .ok_or_else(|| Error::Invalid("cursor missing".into()))?;
            if cursor.generation != self.config.generation {
                return Err(Error::Conflict("native replay generation changed".into()));
            }
            let authority = cursor.authority.as_ref().ok_or_else(|| Error::Invalid("cursor authority missing".into()))?;
            let binding = self.binding(authority)?;
            let reader = HistoryReader::new(&self.stream, &binding.authority, binding.verifier()?)?;
            let pinned = reader.pin(cursor.revision).await?;
            let page = reader.read_page(&pinned, HistoryReadLimits {
                maximum_events: MAX_PAGE_EVENTS as u32,
                maximum_bytes: (MAX_FRAME_BYTES - 8192) as u64,
            }).await?;
            if page.events.is_empty() {
                return Ok(Box::pin(stream::empty()) as BoxProviderStream<'static, Result<wire::Delivery>>);
            }
            let events = page.events.iter().map(|event|
                encode_event_envelope(&binding.authority, event)
            ).collect::<Result<Vec<_>>>()?;
            let delivery = wire::Delivery {
                authority: cursor.authority, generation: cursor.generation,
                from_revision: cursor.revision, through_revision: page.cursor.after_revision,
                events, live: false,
            };
            Ok(Box::pin(stream::iter([Ok(delivery)])) as BoxProviderStream<'static, Result<wire::Delivery>>)
        })
    }

    fn authorize_operation_control<'a>(&'a self, request: &'a OperationControlRequest) -> BoxProviderFuture<'a, Result<()>> {
        Box::pin(async move { self.runtime.task_host().authorize_operation_control(request).await })
    }

    fn observe<'a>(&'a self, request: wire::ObserveRequest) -> BoxProviderFuture<'a, Result<wire::OperationStatus>> {
        Box::pin(async move {
            let control = validate_observe_request(&request)?;
            let state = self.runtime.task_host().observe_control(&control).await?;
            Ok(operation_status(&state))
        })
    }

    fn cancel<'a>(&'a self, request: wire::CancelRequest) -> BoxProviderFuture<'a, Result<wire::CancelResponse>> {
        Box::pin(async move {
            let control = validate_cancel_request(&request)?;
            let state = self.runtime.task_host().cancel_control(&control).await?;
            Ok(wire::CancelResponse {
                status: Some(operation_status(&state)),
                operation: Some(wire::OperationIdentity {
                    operation_id: control.control.operation_id.to_string(),
                    idempotency_key: control.idempotency_key.0,
                }),
            })
        })
    }
}

#[cfg(test)]
#[path = "native_tests.rs"]
mod tests;
