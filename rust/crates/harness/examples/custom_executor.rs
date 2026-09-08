//! A complete custom loop can replace the stock executor without core changes.

use acyclic_harness::{
    Result,
    executor::{ExecutionEvent, ExecutionJournal, Executor, TurnInput, TurnOutput},
    model::ModelEvent,
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
            journal
                .append(
                    input.operation_id,
                    "custom:complete".into(),
                    ExecutionEvent::Model {
                        step: 0,
                        event: ModelEvent::Completed {
                            metadata: json!({"executor": "custom"}),
                        },
                    },
                )
                .await?;
            Ok(TurnOutput {
                text: input.input.to_string(),
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
