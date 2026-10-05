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
use acyclic_harness::model::{ModelContent, ModelEvent};
use acyclic_harness::{Admission, HarnessBuilder, Outcome, TaskGroup};
use futures::{future::BoxFuture, FutureExt as _};
use serde_json::json;

/// Stable source identity consumed by the examples manifest.
pub const SOURCE: &str = "rust/crates/sdk-examples/src/harness_scenarios.rs";
/// Stable scenario identity consumed by docs and fixture reports.
pub const SCENARIO_ID: &str = "harness-admission-recovery-cancel";

/// Rust source shown in the Harness custom executor projection.
pub const QUICKSTART_SNIPPET: &str = r#"use std::sync::Arc;
use acyclic_harness::{Admission, HarnessBuilder, Outcome, TaskGroup};

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

let recovered = match TaskGroup::new(1).try_spawn(async { 11_u8 }).await {
    Admission::Accepted(handle) => matches!(handle.result().await, Outcome::Succeeded(11)),
    Admission::Rejected { .. } | Admission::Indeterminate { .. } => false,
};
assert!(recovered);

// A durable custom executor must bind its journal explicitly.
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
                text: match input.input {
                    ModelContent::Text(text) => text,
                    ModelContent::Part(_) | ModelContent::Parts(_) => {
                        "Custom executor accepted typed input".into()
                    }
                },
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
    /// A fresh group admitted work after the cancelled group was discarded.
    pub recovered_with_fresh_group: bool,
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

    let recovered_with_fresh_group = match TaskGroup::new(1).try_spawn(async { 11_u8 }).await {
        Admission::Accepted(handle) => matches!(handle.result().await, Outcome::Succeeded(11)),
        Admission::Rejected { .. } | Admission::Indeterminate { .. } => false,
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
        recovered_with_fresh_group,
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
        assert!(receipt.recovered_with_fresh_group);
        assert!(receipt.journal_boundary_enforced);
    }

    #[test]
    fn source_and_snippet_are_stable() {
        assert!(SOURCE.ends_with("harness_scenarios.rs"));
        assert!(QUICKSTART_SNIPPET.contains("HarnessBuilder::new"));
        assert!(QUICKSTART_SNIPPET.contains("TaskGroup::new"));
    }
}
