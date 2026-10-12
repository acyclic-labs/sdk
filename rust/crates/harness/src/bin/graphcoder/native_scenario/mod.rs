//! Explicit local providers and their qualification inputs.
#![allow(missing_docs, reason = "private installed-binary scenario")]
mod case;
mod producer;
use crate::native::{AuthorityBinding, Config, Files, ForkSourceConfig, Runtime};
use crate::native_tools::{NativeToolCaller, NativeToolFork};
use crate::native_writeback::NativePublicationConfig;
use acyclic_harness::{
    Admission, AgentId, Capabilities, Error, IdempotencyKey, OperationId, Outcome, Result, TaskId,
    conversation::{
        ContentGrant, ConversationMessage, FileRef, Limits, MessageKind, VolumeClass,
        VolumeOperation, VolumeOwner, VolumeRef,
    },
    core::{Action, AggregateKind, Authority, AuthorityIssuer, Command, SchemaRegistry},
    distributed::{WorkLease, WorkPull, Worker},
    executor::TurnOutput,
    filesystem::{
        ConfiguredRecursiveMerge, FORK_PREPARE_TASK_COMMAND_KIND, FilesystemContentVerifier,
        FilesystemForkVerifier, FilesystemHost, FilesystemProjectMergeVerifier,
        ForkPreparationLimits, ForkToolArguments, MODEL_TASK_COMMAND_KIND, ModelTaskCommand,
        RecursiveForkConfig, RecursiveSourceSnapshot, StreamHistoryForkVerifier,
        fork_preparation_request, recursive_fork_source, workspace_ref,
    },
    fork::{CompositeForkVerifier, ForkReport},
    model::ModelContent,
    resource_preparation::ResourcePreparationRequest,
    resources::ProviderRef,
    runtime::{RuntimeTask, TaskAdmissionRecord, TaskDefinition, TaskRunLimits, TaskStateProvider},
    scheduler::{LeaseFence, SessionLimits},
    store::{HistoryReadLimits, StreamAggregate},
    workflow::WorkflowCommand,
};
use acyclic_stream::LocalStream;
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};

const ISSUER: &str = "graphcoder-explicit-trusted-local-qualification";
// Public qualification authority.
const TEST_KEY: [u8; 32] = [91; 32];
const PAYLOAD_BYTES: u64 = crate::terminal::MAX_FRAME_BYTES as u64;
const COMMON_GRANTS: &[&str] = &[
    "operation:declare",
    "operation:observe",
    "operation:cancel",
    "operation:wake",
    "model:generate",
    "fork:publish",
    "project:merge",
    "task:spawn:acyclic.stock_turn@1",
    "task:spawn:acyclic.fork_preparation@1",
    "task:spawn:acyclic.mailbox_model@1",
    "mail:send",
    "mail:read",
    "interaction:open",
    "interaction:resolve",
    "interaction:route",
    "effect:plan",
    "effect:run",
    "effect:provider:graphcoder.native",
    "tool:call:acyclic.fork",
    "tool:call:acyclic.merge",
    "tool:call:acyclic.read_file",
    "tool:call:acyclic.write_file",
    "tool:call:acyclic.edit_file",
];
struct Caller {
    task: TaskId,
    binding: AuthorityBinding,
    project: VolumeRef,
    private: VolumeRef,
    result: Option<VolumeRef>,
    aggregate: StreamAggregate<LocalStream>,
    lease: WorkLease,
}
struct Producer {
    root: PathBuf,
    files: Arc<Files>,
    stream: Arc<LocalStream>,
    runtime: Arc<Runtime>,
    config: Config,
    template: TaskAdmissionRecord,
    worker: Worker,
    preparations: Vec<Value>,
    references: BTreeMap<String, FileRef>,
    planned_commands: BTreeMap<String, WorkflowCommand>,
    leases: BTreeMap<String, PathBuf>,
    cancellation: acyclic_fs::CancellationToken,
}
pub async fn prepare(root: &Path, cancellation: &acyclic_fs::CancellationToken) -> Result<PathBuf> {
    if !root.is_absolute() {
        return Err(Error::Invalid(
            "scenario root must be an absolute path".into(),
        ));
    }
    acyclic_native_runtime::initialize_native_io_services()
        .map_err(|error| Error::Storage(format!("native IO service startup: {error}")))?;
    acyclic_fs::initialize_native_mount_services()
        .map_err(|error| Error::Storage(format!("native mount service startup: {error}")))?;
    let negative =
        case::prepare_case(&root.join("one-below-actual"), 11, None, cancellation).await?;
    case::prepare_case(root, 12, Some(negative), cancellation).await
}
async fn aggregate(
    stream: &Arc<LocalStream>,
    files: Arc<Files>,
    owner: &AuthorityBinding,
    config: &Config,
) -> Result<StreamAggregate<LocalStream>> {
    let verifier = owner.verifier()?;
    let content = FilesystemContentVerifier::new(
        files.clone(),
        &verifier,
        &owner.scope,
        PAYLOAD_BYTES,
        None,
    )?;
    let forks = CompositeForkVerifier::new(
        &[
            FilesystemForkVerifier::new(files.clone(), PAYLOAD_BYTES, None, None)?,
            StreamHistoryForkVerifier::new(&config.stream_provider, &mut None)?,
        ],
        &mut None,
    )?;
    let mut aggregate = StreamAggregate::open(
        stream,
        owner.authority.clone(),
        verifier,
        SchemaRegistry::new(),
        None,
        acyclic_harness::store::default_projection_read_limits(),
        acyclic_harness::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?
    .with_content_verifier(content)
    .with_fork_verifier(forks)
    .with_merge_verifier(FilesystemProjectMergeVerifier::new(files, None)?);
    aggregate.set_limits(config.limits)?;
    Ok(aggregate)
}
async fn claim(runtime: &Runtime, task: TaskId, worker: &Worker) -> Result<WorkLease> {
    let admission = runtime.task_host().observe_admission(task).await?;
    let lease = match runtime
        .task_host()
        .pull_operation(worker, admission.operation_id, &mut None)
        .await?
    {
        WorkPull::Claimed(lease) => lease,
        WorkPull::Idle => {
            return Err(Error::Conflict(format!(
                "qualification task {task} has no owner lease"
            )));
        }
        WorkPull::Unresolved { error, .. } => return Err(error),
    };
    runtime.task_host().start_task(&lease, None).await?;
    Ok(lease)
}
fn admitted_task<T: Send + 'static>(admission: Admission<RuntimeTask<T>>) -> Result<TaskId> {
    match admission {
        Admission::Accepted(RuntimeTask::Durable { task_id, .. }) => Ok(task_id),
        Admission::Accepted(RuntimeTask::Live(_)) => Err(Error::Unsupported(
            "qualification requires genuine durable admission".into(),
        )),
        Admission::Rejected { reason } => Err(Error::Conflict(reason)),
        Admission::Indeterminate { operation_id } => Err(Error::Indeterminate(operation_id)),
    }
}
async fn persist(path: &Path, value: &impl Serialize) -> Result<()> {
    let bytes =
        serde_json::to_vec_pretty(value).map_err(|error| Error::Invalid(error.to_string()))?;
    tokio::fs::write(path, bytes)
        .await
        .map_err(|error| Error::Storage(error.to_string()))
}

async fn write(
    files: &Files,
    owner: &AuthorityBinding,
    volume: &VolumeRef,
    path: &str,
    bytes: &[u8],
    media: &str,
    display: &str,
    key: &str,
) -> Result<FileRef> {
    let issuer = AuthorityIssuer::new(ISSUER, TEST_KEY, owner.authority.clone());
    let writer = ContentGrant::verify(
        &issuer.verifier(&mut None)?,
        &owner.scope,
        volume,
        VolumeOperation::Write,
        u64::MAX,
        &mut None,
    )?;
    files
        .put_content(
            volume,
            &writer,
            path,
            bytes,
            media,
            display,
            PAYLOAD_BYTES,
            &IdempotencyKey::new(key)?,
            None,
            None,
        )
        .await
}
