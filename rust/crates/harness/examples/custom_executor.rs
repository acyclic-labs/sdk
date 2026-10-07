//! A custom control loop reuses public durable model admission and recovery.

use acyclic_harness::{
    Error, Result,
    context::ContextPipeline,
    executor::{ExecutionJournal, Executor, StockExecutor, TurnInput, TurnOutput},
    model::{Model, ModelAttempt, ModelEvent, ModelProvider, PreparedModelRequest},
    tool::ToolRegistry,
};
use futures::{
    future::BoxFuture,
    stream::{self, BoxStream},
};
use serde_json::Value;
use std::sync::Arc;

struct CustomExecutor(StockExecutor);

impl Executor for CustomExecutor {
    fn execute<'a>(
        &'a self,
        input: TurnInput,
        journal: &'a dyn ExecutionJournal,
    ) -> BoxFuture<'a, Result<TurnOutput>> {
        Box::pin(async move {
            let mut text = String::new();
            let mut metadata = None;
            // Custom stopping policy, the same admitted step used by the stock loop.
            for event in self.0.model_step(journal, &input, 0, &[]).await? {
                match event {
                    ModelEvent::Content { delta } => text.push_str(&delta),
                    ModelEvent::Completed { metadata: value } => metadata = Some(value),
                    ModelEvent::Reasoning { .. } => {}
                    ModelEvent::ToolCall { .. } => {
                        return Err(Error::Unsupported("this custom loop has no tools".into()));
                    }
                }
            }
            Ok(TurnOutput {
                text,
                attachments: Vec::new(),
                metadata: metadata.ok_or(Error::Indeterminate(input.operation_id))?,
                steps: 1,
            })
        })
    }
}

struct MockModel;
impl ModelProvider for MockModel {
    fn generate<'a>(&'a self, _: PreparedModelRequest) -> BoxStream<'a, Result<ModelEvent>> {
        Box::pin(stream::iter([Ok(ModelEvent::Completed {
            metadata: Value::Null,
        })]))
    }

    fn reconcile<'a>(&'a self, _: ModelAttempt) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        Box::pin(async { Ok(None) })
    }
}

fn main() -> Result<()> {
    let _executor: Box<dyn Executor> = Box::new(CustomExecutor(StockExecutor::new(
        Model::new("example", "mock", "1", Value::Null)?,
        Arc::new(MockModel),
        ContextPipeline::default(),
        ToolRegistry::new(),
    )));
    Ok(())
}
