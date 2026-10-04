#![cfg(feature = "filesystem-local")]

use acyclic_harness::{
    swarm_budget::{
        SwarmBudgetLimits, SwarmForkRequest, SwarmOwnerFence, SwarmResourceRequest, SwarmUsage,
        SwarmUsageSource,
    },
    swarm_budget_journal::SwarmBudgetJournal,
    Error, IdempotencyKey, OperationId, Result,
};
use acyclic_stream::{LocalStream, LocalStreamLimits, StreamClient};
use futures::future::join_all;
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};
use tempfile::tempdir;

fn request(operation_id: OperationId, key: String) -> SwarmForkRequest {
    SwarmForkRequest {
        operation_id,
        idempotency_key: IdempotencyKey::new(key).expect("idempotency key"),
        parent_operation_id: None,
        depth: 1,
        resources: SwarmResourceRequest {
            model_steps: 2,
            output_bytes: 20,
            execution_time_ms: 200,
        },
        admission_digest: None,
    }
}

fn limits() -> SwarmBudgetLimits {
    SwarmBudgetLimits {
        // The root counts toward active capacity. The concurrency case below
        // intentionally admits the root plus three live children.
        max_active_agents: 4,
        max_total_agents: 4,
        max_recursion_depth: 1,
        max_model_steps: 8,
        max_output_bytes: 80,
        max_execution_time_ms: 800,
    }
}

struct MeasuredSequence {
    snapshots: Mutex<VecDeque<SwarmUsage>>,
}

impl MeasuredSequence {
    fn new(snapshots: impl IntoIterator<Item = SwarmUsage>) -> Self {
        Self {
            snapshots: Mutex::new(snapshots.into_iter().collect()),
        }
    }
}

impl SwarmUsageSource for &MeasuredSequence {
    fn provider_identity(&self) -> &str {
        "local-production-measurement"
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

async fn client() -> Result<(tempfile::TempDir, StreamClient<LocalStream>)> {
    let root = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let stream = LocalStream::open(root.path().join("stream"), LocalStreamLimits::default())
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    Ok((root, StreamClient::new(Arc::new(stream))))
}

#[tokio::test]
async fn production_root_context_uses_remaining_capacity_and_durable_cursor() -> Result<()> {
    let (_root, client) = client().await?;
    let session = OperationId::new();
    let owner = SwarmOwnerFence::new("production-owner", 0)?;
    let root_dispatch = IdempotencyKey::new("root-production-lease")?;
    let mut journal = SwarmBudgetJournal::start_with_root_dispatch(
        &client,
        session,
        owner.clone(),
        limits(),
        root_dispatch,
    )
    .await?;
    journal
        .reserve_child(request(OperationId::new(), "child-reservation".into()))
        .await?;

    let source = MeasuredSequence::new([
        SwarmUsage {
            model_steps: 3,
            output_bytes: 20,
            execution_time_ms: 200,
        },
        SwarmUsage {
            model_steps: 5,
            output_bytes: 40,
            execution_time_ms: 400,
        },
    ]);
    let mut context = journal.root_usage_context(&source)?;
    assert_eq!(context.limiter_mut().limits().model_steps, 6);
    assert_eq!(context.limiter_mut().limits().output_bytes, 60);
    assert_eq!(context.limiter_mut().limits().execution_time_ms, 600);
    context.limiter_mut().admit_model_step()?;
    context.limiter_mut().admit_model_step()?;
    context.limiter_mut().admit_output(20)?;
    context.limiter_mut().admit_execution_time(200)?;
    assert!(context.limiter_mut().admit_model_step().is_ok());
    let first = context.issue_usage_receipt()?;
    journal
        .report_root_usage_with_receipt(&owner, first)
        .await?;
    assert_eq!(journal.root_usage_cursor()?.sequence, 1);

    drop(context);
    drop(journal);
    let mut reopened = SwarmBudgetJournal::open(&client, session).await?;
    let mut resumed = reopened.root_usage_context(&source)?;
    assert_eq!(resumed.receipt_cursor().sequence, 1);
    assert_eq!(resumed.limiter_mut().limits().model_steps, 6);
    resumed.limiter_mut().admit_model_step()?;
    resumed.limiter_mut().admit_model_step()?;
    resumed.limiter_mut().admit_output(20)?;
    resumed.limiter_mut().admit_execution_time(200)?;
    let second = resumed.issue_usage_receipt()?;
    reopened
        .report_root_usage_with_receipt(&owner, second)
        .await?;
    let usage = reopened.usage()?;
    assert_eq!(usage.consumed.model_steps, 5);
    assert_eq!(usage.consumed.output_bytes, 40);
    assert_eq!(usage.consumed.execution_time_ms, 400);
    assert_eq!(usage.reserved.model_steps, 2);
    Ok(())
}

#[tokio::test]
async fn production_concurrent_admission_exhausts_total_capacity_atomically() -> Result<()> {
    let (_root, client) = client().await?;
    let session = OperationId::new();
    let owner = SwarmOwnerFence::new("production-owner", 0)?;
    SwarmBudgetJournal::start(&client, session, owner, limits()).await?;
    let journals = join_all((0..8).map(|_| SwarmBudgetJournal::open(&client, session)))
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    let results = join_all(
        journals
            .into_iter()
            .enumerate()
            .map(|(index, mut journal)| {
                let request = request(OperationId::new(), format!("concurrent-{index}"));
                async move { journal.reserve_child(request).await }
            }),
    )
    .await;
    let admitted = results.iter().filter(|result| result.is_ok()).count();
    assert_eq!(
        admitted, 3,
        "root plus three children exhausts total capacity"
    );
    let reopened = SwarmBudgetJournal::open(&client, session).await?;
    let usage = reopened.usage()?;
    assert_eq!(usage.total_agents, 4);
    assert_eq!(usage.active_agents, 4);
    assert_eq!(usage.reserved.model_steps, 6);
    Ok(())
}
