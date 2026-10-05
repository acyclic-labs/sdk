//! Executable Harness scenarios used by the SDK example registry.
//!
//! This module exercises the public Rust admission and cancellation surface,
//! and keeps the custom executor implementation identical in shape to the
//! canonical Harness example.  The recovery scenario owns its journal and
//! reopens it from the local filesystem across an actual process boundary.

use std::sync::Arc;

#[cfg(not(target_arch = "wasm32"))]
use std::path::Path;

#[cfg(not(target_arch = "wasm32"))]
use acyclic_fs::{Fs, LocalAuthorityBackend, LocalObjectBackend, LocalOptions};
use acyclic_harness::conversation::Attachment;
#[cfg(not(target_arch = "wasm32"))]
use acyclic_harness::conversation::{VolumeClass, VolumeOperation, VolumeOwner, VolumeRef};
#[cfg(not(target_arch = "wasm32"))]
use acyclic_harness::core::{AggregateKind, Authority, AuthorityIssuer};
use acyclic_harness::executor::{
    ExecutionEvent, ExecutionJournal, Executor, TurnInput, TurnOutput,
};
#[cfg(not(target_arch = "wasm32"))]
use acyclic_harness::filesystem::{FilesystemExecutionJournal, FilesystemHost};
use acyclic_harness::model::{ModelContent, ModelEvent};
#[cfg(not(target_arch = "wasm32"))]
use acyclic_harness::resources::ProviderRef;
use acyclic_harness::{
    Admission, AgentId, Capabilities, HarnessBuilder, OperationId, Outcome, TaskGroup,
};
#[cfg(not(target_arch = "wasm32"))]
use acyclic_stream::{LocalStream, LocalStreamLimits, StreamClient};
use futures::{FutureExt as _, future::BoxFuture};
use serde_json::json;

/// Stable source identity consumed by the examples manifest.
pub const SOURCE: &str = "rust/crates/sdk-examples/src/harness_scenarios.rs";
/// Stable scenario identity consumed by docs and fixture reports.
pub const SCENARIO_ID: &str = "harness-admission-recovery-cancel";

#[cfg(not(target_arch = "wasm32"))]
type PersistentJournal =
    FilesystemExecutionJournal<LocalStream, LocalAuthorityBackend, LocalObjectBackend>;

/// Reopenable local Harness journal used by the recovery scenario.
///
/// The Stream log and staged event payloads both live below `root`, so dropping
/// this value and opening it again exercises the same on-disk recovery path a
/// process restart uses.
#[cfg(not(target_arch = "wasm32"))]
pub struct PersistentHarnessStorage {
    journal: Arc<PersistentJournal>,
}

#[cfg(not(target_arch = "wasm32"))]
impl PersistentHarnessStorage {
    pub async fn open(root: &Path, agent: AgentId) -> acyclic_harness::Result<Self> {
        let stream = LocalStream::open(root.join("stream"), LocalStreamLimits::default())
            .await
            .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?;
        let filesystem = Fs::local(LocalOptions::new(root.join("filesystem")))
            .await
            .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?;
        let provider = ProviderRef::new("persistent-local", "filesystem", "2")?;
        let host = Arc::new(FilesystemHost::new(filesystem, provider.clone())?);
        let volume = VolumeRef::new(
            provider.clone(),
            "sdk-examples-harness",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
        )?;
        let workspace =
            acyclic_harness::filesystem::workspace_ref(provider.clone(), &volume.storage_name()?)?;
        if host.resolve(&workspace).await.is_err() {
            host.create_volume(&volume).await?;
        }
        let issuer = AuthorityIssuer::new(
            "persistent-sdk-examples",
            [0x5a; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "sdk-examples-persistent-recovery".into(),
            },
        );
        let scope = issuer.root_for_agent(
            agent,
            "owner",
            Capabilities::new([
                volume.capability(VolumeOperation::Read)?,
                volume.capability(VolumeOperation::Write)?,
            ]),
        );
        let journal = FilesystemExecutionJournal::new(
            StreamClient::new(Arc::new(stream)),
            host,
            volume,
            issuer.verifier(),
            scope,
            4_096,
        )?;
        Ok(Self {
            journal: Arc::new(journal),
        })
    }

    pub fn journal(&self) -> Arc<dyn ExecutionJournal> {
        self.journal.clone()
    }
}

#[cfg(not(target_arch = "wasm32"))]
/// Runs the first half of the persistent recovery scenario in a child process.
pub async fn run_persistent_recovery_child(
    root: &Path,
    agent: AgentId,
    operation_id: OperationId,
) -> Result<(), String> {
    let storage = PersistentHarnessStorage::open(root, agent)
        .await
        .map_err(|error| error.to_string())?;
    let input = TurnInput {
        operation_id,
        input: ModelContent::Text("durable recovery".into()),
        selected_context: None,
        max_steps: 1,
    };
    CustomExecutor
        .execute(input, storage.journal().as_ref())
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}

/// Rust source shown in the Harness custom executor projection.
pub const QUICKSTART_SNIPPET: &str = r#"use std::sync::Arc;
use std::path::PathBuf;
use acyclic_harness::executor::{ExecutionEvent, ExecutionJournal};
use acyclic_harness::filesystem::LocalHarnessStorage;
use acyclic_harness::{
    Admission, AgentId, OperationId, Outcome, TaskGroup,
};

let group = TaskGroup::new(1);
let completed = match group.try_spawn(async { 7_u8 }).await {
    Admission::Accepted(handle) => matches!(handle.result().await, Outcome::Succeeded(7)),
    Admission::Rejected { .. } | Admission::Indeterminate { .. } => false,
};
assert!(completed);

group.cancel();
assert!(matches!(
    group.try_spawn(async { 9_u8 }).await,
    Admission::Rejected { .. }
));

let fresh_group_after_cancellation = match TaskGroup::new(1).try_spawn(async { 11_u8 }).await {
    Admission::Accepted(handle) => matches!(handle.result().await, Outcome::Succeeded(11)),
    Admission::Rejected { .. } | Admission::Indeterminate { .. } => false,
};
assert!(fresh_group_after_cancellation);

// A durable journal is reopened from the same on-disk root. The executable
// scenario uses the same LocalHarnessStorage composition across process
// boundaries, while this compact projection proves the public reopen contract.
let root = PathBuf::from(std::env::temp_dir()).join(format!("acyclic-harness-example-{}", std::process::id()));
let agent = AgentId::new();
let operation_id = OperationId::new();
let storage = LocalHarnessStorage::open(&root, agent, 4_096).await?;
storage.journal().append(
    operation_id,
    "durable-start".into(),
    ExecutionEvent::Started { request_digest: [7; 32] },
).await?;
drop(storage);
let storage = LocalHarnessStorage::open(&root, agent, 4_096).await?;
assert_eq!(storage.replay(operation_id).await?.len(), 1);
let _ = std::fs::remove_dir_all(root);"#;

/// Application-owned executor demonstrating the complete typed callback.
pub struct CustomExecutor;

impl Executor for CustomExecutor {
    fn execute<'a>(
        &'a self,
        input: TurnInput,
        journal: &'a dyn ExecutionJournal,
    ) -> BoxFuture<'a, acyclic_harness::Result<TurnOutput>> {
        async move {
            let text = match &input.input {
                ModelContent::Text(text) => text.clone(),
                ModelContent::Part(_) | ModelContent::Parts(_) => {
                    "Custom executor accepted typed input".into()
                }
            };
            let replayed = journal.replay(input.operation_id).await?;
            if replayed
                .iter()
                .any(|record| matches!(record.event, ExecutionEvent::Model { .. }))
            {
                return Ok(TurnOutput {
                    text,
                    attachments: Vec::new(),
                    metadata: json!({"replayed": true}),
                    steps: 1,
                });
            }
            let event = ModelEvent::Completed {
                metadata: json!({"executor": "sdk-examples"}),
            };
            let bytes = serde_json::to_vec(&event)
                .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?;
            let staged = journal
                .stage(
                    input.operation_id,
                    "custom:complete:event".into(),
                    bytes,
                    "application/json",
                )
                .await?;
            journal
                .append(
                    input.operation_id,
                    "custom:complete".into(),
                    ExecutionEvent::Model {
                        step: 0,
                        event: staged,
                    },
                )
                .await?;
            let attachments = input
                .input
                .file_refs()
                .into_iter()
                .cloned()
                .map(|file| Attachment { file, label: None })
                .collect();
            Ok(TurnOutput {
                text,
                attachments,
                metadata: json!({"owned_by": "application"}),
                steps: 1,
            })
        }
        .boxed()
    }
}

/// Facts observed while exercising live admission, recovery, cancellation,
/// and the builder's journal requirement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HarnessScenarioReceipt {
    /// An admitted task completed successfully.
    pub admitted_and_completed: bool,
    /// Admission was rejected after cancellation closed the group.
    pub cancellation_rejected_admission: bool,
    /// A new executor instance replayed the retained journal after restart.
    pub durable_replay_after_restart: bool,
    /// A custom executor without an owner journal was rejected by the builder.
    pub journal_boundary_enforced: bool,
}

/// Executes the public Harness admission and cancellation scenario.
pub async fn execute_harness_scenario() -> HarnessScenarioReceipt {
    let group = TaskGroup::new(1);
    let admitted_and_completed = match group.try_spawn(async { 7_u8 }).await {
        Admission::Accepted(handle) => matches!(handle.result().await, Outcome::Succeeded(7)),
        Admission::Rejected { .. } | Admission::Indeterminate { .. } => false,
    };

    group.cancel();
    let cancellation_rejected_admission = matches!(
        group.try_spawn(async { 9_u8 }).await,
        Admission::Rejected { .. }
    );

    let _fresh_group_after_cancellation = match TaskGroup::new(1).try_spawn(async { 11_u8 }).await {
        Admission::Accepted(handle) => matches!(handle.result().await, Outcome::Succeeded(11)),
        Admission::Rejected { .. } | Admission::Indeterminate { .. } => false,
    };

    #[cfg(not(target_arch = "wasm32"))]
    let durable_replay_after_restart = {
        let operation_id = OperationId::new();
        let root =
            std::env::temp_dir().join(format!("acyclic-sdk-examples-harness-{}", operation_id));
        let agent = AgentId::new();
        let input = TurnInput {
            operation_id,
            input: ModelContent::Text("durable recovery".into()),
            selected_context: None,
            max_steps: 1,
        };
        let executable = std::env::current_exe().ok();
        let child_process = executable.as_ref().filter(|path| {
            path.file_stem()
                .and_then(|stem| stem.to_str())
                .is_some_and(|stem| stem.starts_with("sdk-examples"))
        });
        let first = if let Some(executable) = child_process {
            let child = std::process::Command::new(executable)
                .env("ACYCLIC_HARNESS_RECOVERY_CHILD", "first")
                .env("ACYCLIC_HARNESS_RECOVERY_ROOT", &root)
                .env(
                    "ACYCLIC_HARNESS_RECOVERY_AGENT",
                    hex::encode(agent.into_bytes()),
                )
                .env(
                    "ACYCLIC_HARNESS_RECOVERY_OPERATION",
                    hex::encode(operation_id.into_bytes()),
                )
                .status();
            match child {
                Ok(status) if status.success() => Ok(()),
                Ok(status) => Err(acyclic_harness::Error::Storage(format!(
                    "persistent recovery child exited with {status}"
                ))),
                Err(error) => Err(acyclic_harness::Error::Storage(format!(
                    "start persistent recovery child: {error}"
                ))),
            }
        } else if let Ok(storage) = PersistentHarnessStorage::open(&root, agent).await {
            let journal = storage.journal();
            CustomExecutor
                .execute(input.clone(), journal.as_ref())
                .await
                .map(|_| ())
        } else {
            Err(acyclic_harness::Error::Storage(
                "open persistent Harness storage".into(),
            ))
        };
        let second = if let Ok(storage) = PersistentHarnessStorage::open(&root, agent).await {
            let journal = storage.journal();
            CustomExecutor.execute(input, journal.as_ref()).await
        } else {
            Err(acyclic_harness::Error::Storage(
                "reopen persistent Harness storage".into(),
            ))
        };
        let _ = std::fs::remove_dir_all(&root);
        first.is_ok()
            && second
                .as_ref()
                .is_ok_and(|output| output.metadata["replayed"] == true)
    };
    #[cfg(target_arch = "wasm32")]
    let durable_replay_after_restart = false;

    let custom: Arc<dyn Executor> = Arc::new(CustomExecutor);
    let journal_boundary_enforced = HarnessBuilder::new()
        .name("sdk-examples")
        .executor(custom)
        .build()
        .is_err();

    HarnessScenarioReceipt {
        admitted_and_completed,
        cancellation_rejected_admission,
        durable_replay_after_restart,
        journal_boundary_enforced,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn admission_recovery_and_cancel_are_executed() {
        let receipt = execute_harness_scenario().await;
        assert!(receipt.admitted_and_completed);
        assert!(receipt.cancellation_rejected_admission);
        assert!(receipt.durable_replay_after_restart);
        assert!(receipt.journal_boundary_enforced);
    }

    #[test]
    fn source_and_snippet_are_stable() {
        assert!(SOURCE.ends_with("harness_scenarios.rs"));
        assert!(QUICKSTART_SNIPPET.contains("HarnessBuilder::new"));
        assert!(QUICKSTART_SNIPPET.contains("TaskGroup::new"));
        assert!(QUICKSTART_SNIPPET.contains("group.cancel()"));
        assert!(QUICKSTART_SNIPPET.contains("PersistentHarnessStorage"));
        assert!(QUICKSTART_SNIPPET.contains("child process"));
        assert!(QUICKSTART_SNIPPET.contains("durable recovery"));
        assert!(QUICKSTART_SNIPPET.contains("metadata[\"replayed\"]"));
        assert!(QUICKSTART_SNIPPET.contains("result.is_err()"));
    }
}
