use acyclic_harness::{
    Error, IdempotencyKey, OperationId, Result,
    conversation::Limits,
    model::{Model, ModelContent, ModelEvent, ModelMessage, ModelProvider, ModelRole},
    model_input::PreparedModelInput,
    swarm_budget::{
        MeteredModelProvider, SwarmBudget, SwarmBudgetLimits, SwarmOwnerFence, SwarmUsage,
        SwarmUsageSource,
    },
};
use futures::{StreamExt, stream};
use serde_json::Value;
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

struct MeasuredSource {
    snapshots: Mutex<VecDeque<SwarmUsage>>,
}

impl MeasuredSource {
    fn new(snapshots: impl IntoIterator<Item = SwarmUsage>) -> Self {
        Self {
            snapshots: Mutex::new(snapshots.into_iter().collect()),
        }
    }
}

impl SwarmUsageSource for MeasuredSource {
    fn provider_identity(&self) -> &str {
        "metered-provider-contract"
    }

    fn cumulative_usage(
        &self,
        _operation_id: OperationId,
        _dispatch_id: &IdempotencyKey,
    ) -> Result<SwarmUsage> {
        self.snapshots
            .lock()
            .map_err(|_| Error::Storage("measurement lock poisoned".into()))?
            .pop_front()
            .ok_or_else(|| Error::Storage("measurement exhausted".into()))
    }
}

struct StreamingModel;

impl ModelProvider for StreamingModel {
    fn generate<'a>(
        &'a self,
        _: PreparedModelInput,
    ) -> futures::stream::BoxStream<'a, Result<ModelEvent>> {
        Box::pin(stream::iter([
            Ok(ModelEvent::Content {
                delta: "measured answer".into(),
            }),
            Ok(ModelEvent::Completed {
                metadata: Value::Null,
            }),
        ]))
    }

    fn reconcile<'a>(
        &'a self,
        _: acyclic_harness::model::ModelAttempt,
    ) -> futures::future::BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        Box::pin(async { Ok(None) })
    }
}

fn request() -> ModelRequest {
    ModelRequest {
        model: Model::new("test", "metered", "1", Value::Null).expect("model"),
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text("measure this".into()),
        }],
        tools: Vec::new(),
        max_output_tokens: None,
    }
}

fn limits() -> SwarmBudgetLimits {
    SwarmBudgetLimits {
        max_active_agents: 1,
        max_total_agents: 1,
        max_recursion_depth: 0,
        max_model_steps: 1,
        max_output_bytes: 1_024,
        max_execution_time_ms: 30_000,
    }
}

#[tokio::test]
async fn metered_provider_charges_real_stream_and_rejects_next_step_at_ceiling() -> Result<()> {
    let session = OperationId::new();
    let owner = SwarmOwnerFence::new("metered-provider-owner", 0)?;
    let dispatch = IdempotencyKey::new("metered-provider-root")?;
    let budget = SwarmBudget::new_with_root_dispatch(session, owner, limits(), Some(dispatch))?;
    let source = MeasuredSource::new([SwarmUsage {
        model_steps: 1,
        output_bytes: 1_024,
        execution_time_ms: 30_000,
    }]);
    let (metered, meter) = MeteredModelProvider::new_root(
        Arc::new(StreamingModel),
        budget.root_usage_context(source)?,
    );

    let prepared = PreparedModelInput::prepare(request(), Limits::default())?;
    let events = metered.generate(prepared).collect::<Vec<_>>().await;
    assert_eq!(events.len(), 2);
    assert!(events.iter().all(Result::is_ok));
    let usage = meter.usage()?;
    assert_eq!(usage.model_steps, 1);
    assert!(usage.output_bytes > 0);
    assert!(usage.execution_time_ms <= limits().max_execution_time_ms);
    let _receipt = meter.issue_usage_receipt()?;

    let prepared = PreparedModelInput::prepare(request(), Limits::default())?;
    let exhausted = metered.generate(prepared).next().await;
    assert!(matches!(exhausted, Some(Err(Error::Conflict(_)))));
    Ok(())
}

#[tokio::test]
async fn metered_provider_rejects_host_receipt_behind_measured_counters() -> Result<()> {
    let session = OperationId::new();
    let owner = SwarmOwnerFence::new("metered-provider-owner", 0)?;
    let dispatch = IdempotencyKey::new("metered-provider-root")?;
    let budget = SwarmBudget::new_with_root_dispatch(session, owner, limits(), Some(dispatch))?;
    let source = MeasuredSource::new([SwarmUsage::default()]);
    let (metered, meter) = MeteredModelProvider::new_root(
        Arc::new(StreamingModel),
        budget.root_usage_context(source)?,
    );

    let prepared = PreparedModelInput::prepare(request(), Limits::default())?;
    let _ = metered.generate(prepared).collect::<Vec<_>>().await;
    let cursor = meter.receipt_cursor()?;
    assert!(matches!(
        meter.issue_usage_receipt(),
        Err(Error::Conflict(_))
    ));
    assert_eq!(meter.receipt_cursor()?, cursor);
    Ok(())
}
