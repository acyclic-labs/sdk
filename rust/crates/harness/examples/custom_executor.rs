//! A complete custom loop can replace the stock executor without core changes.

use acyclic_harness::{
    Result,
    conversation::Attachment,
    executor::{ExecutionEvent, ExecutionJournal, Executor, TurnInput, TurnOutput},
    model::{ModelContent, ModelEvent},
};
use futures::{FutureExt as _, future::BoxFuture};
use serde_json::json;

struct FullControlExecutor;

impl Executor for FullControlExecutor {
    fn execute<'a>(
        &'a self,
        input: TurnInput,
        journal: &'a dyn ExecutionJournal,
    ) -> BoxFuture<'a, Result<TurnOutput>> {
        async move {
            // A real implementation may select prompts/models/tools/memory,
            // compact, open interactions, spawn tasks, or stop here.
            let event = ModelEvent::Completed {
                metadata: json!({"executor": "custom"}),
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
            // Inputs contain immutable refs rather than attachment bytes. A
            // custom loop may retain or publish those refs explicitly; it
            // must not copy their bytes into a durable event.
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

fn main() {
    let _executor: Box<dyn Executor> = Box::new(FullControlExecutor);
}
