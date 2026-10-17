//! A custom control loop reuses public durable model admission and recovery.

use acyclic_harness::{
    Error, Result,
    context::ContextPipeline,
    executor::{ExecutionJournal, Executor, StockExecutor, TurnInput, TurnOutput},
    model::{Model, ModelEvent},
    test_support::{ScriptedEvent, ScriptedModelProvider, ScriptedTurn},
    tool::ToolRegistry,
};
use futures::future::BoxFuture;
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
            for event in self.0.model_step(journal, &input, 0, &[]).await? {
                match event {
                    ModelEvent::Content { delta } => text.push_str(&delta),
                    ModelEvent::Completed { metadata: value } => metadata = Some(value),
                    ModelEvent::Reasoning { .. } | ModelEvent::ReasoningBlock { .. } => {}
                    ModelEvent::ToolCall(_) => {
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

fn main() -> Result<()> {
    let _executor: Box<dyn Executor> = Box::new(CustomExecutor(
        StockExecutor::new(
            Model::new("example".into(), "mock".into(), "1".into(), Value::Null)?,
            Arc::new(ScriptedModelProvider::new([ScriptedTurn::Events(vec![
                ScriptedEvent::Event(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ])])),
            ContextPipeline::default(),
            ToolRegistry::default(),
        )
        .with_compaction_policy(acyclic_harness::context::CompactionPolicy::Disabled),
    ));
    Ok(())
}
