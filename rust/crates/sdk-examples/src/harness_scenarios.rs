//! Executable Harness scenarios used by the SDK example registry.
//!
//! This module exercises the public Rust admission and cancellation surface,
//! and keeps the custom executor implementation identical in shape to the
//! canonical Harness example.  Durable model turns still require an
//! application-owned journal, so the builder receipt records that admission
//! boundary instead of inventing a journal implementation here.

use std::sync::Arc;

use acyclic_harness::conversation::Attachment;
use acyclic_harness::executor::{
    ExecutionEvent, ExecutionJournal, Executor, TurnInput, TurnOutput,
};
use acyclic_harness::filesystem::MemoryHarnessStorage;
use acyclic_harness::model::{ModelContent, ModelEvent};
use acyclic_harness::{Admission, AgentId, HarnessBuilder, OperationId, Outcome, TaskGroup};
use futures::{FutureExt as _, future::BoxFuture};
use serde_json::json;

/// Stable source identity consumed by the examples manifest.
pub const SOURCE: &str = "rust/crates/sdk-examples/src/harness_scenarios.rs";
/// Stable scenario identity consumed by docs and fixture reports.
pub const SCENARIO_ID: &str = "harness-admission-recovery-cancel";

/// Rust source shown in the Harness custom executor projection.
pub const QUICKSTART_SNIPPET: &str = r#"use std::sync::Arc;
use acyclic_harness::executor::{Executor, TurnInput};
use acyclic_harness::filesystem::MemoryHarnessStorage;
use acyclic_harness::model::ModelContent;
use acyclic_harness::{
    Admission, AgentId, HarnessBuilder, OperationId, Outcome, TaskGroup,
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

// A durable custom executor must bind its journal explicitly and replay it
// after a process restart.
let storage = MemoryHarnessStorage::new(AgentId::new(), 4_096).await?;
let journal = storage.journal();
let input = TurnInput {
    operation_id: OperationId::new(),
    input: ModelContent::Text("durable recovery".into()),
    selected_context: None,
    max_steps: 1,
};
let _first = MyExecutor.execute(input.clone(), journal.as_ref()).await?;
let resumed = MyExecutor.execute(input, journal.as_ref()).await?;
assert_eq!(resumed.metadata["replayed"], true);

// A custom executor without an owner journal is rejected.
let result = HarnessBuilder::new()
    .name("example")
    .executor(Arc::new(MyExecutor))
    .build();
assert!(result.is_err());"#;

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

    let fresh_group_after_cancellation = match TaskGroup::new(1).try_spawn(async { 11_u8 }).await {
        Admission::Accepted(handle) => matches!(handle.result().await, Outcome::Succeeded(11)),
        Admission::Rejected { .. } | Admission::Indeterminate { .. } => false,
    };

    let durable_replay_after_restart =
        if let Ok(storage) = MemoryHarnessStorage::new(AgentId::new(), 4_096).await {
            let journal = storage.journal();
            let input = TurnInput {
                operation_id: OperationId::new(),
                input: ModelContent::Text("durable recovery".into()),
                selected_context: None,
                max_steps: 1,
            };
            let first = CustomExecutor
                .execute(input.clone(), journal.as_ref())
                .await;
            let second = CustomExecutor.execute(input, journal.as_ref()).await;
            first.is_ok()
                && second
                    .as_ref()
                    .is_ok_and(|output| output.metadata["replayed"] == true)
        } else {
            false
        };

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
        assert!(QUICKSTART_SNIPPET.contains("MemoryHarnessStorage"));
        assert!(QUICKSTART_SNIPPET.contains("durable recovery"));
        assert!(QUICKSTART_SNIPPET.contains("metadata[\"replayed\"]"));
        assert!(QUICKSTART_SNIPPET.contains("result.is_err()"));
    }
}
