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
    request_with_resources(
        operation_id,
        key,
        SwarmResourceRequest {
            model_steps: 2,
            output_bytes: 20,
            execution_time_ms: 200,
        },
    )
}

fn request_with_resources(
    operation_id: OperationId,
    key: String,
    resources: SwarmResourceRequest,
) -> SwarmForkRequest {
    SwarmForkRequest {
        operation_id,
        idempotency_key: IdempotencyKey::new(key).expect("idempotency key"),
        parent_operation_id: None,
        depth: 1,
        resources,
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

    fn source_fingerprint(&self) -> [u8; 32] {
        *blake3::hash(b"local-production-measurement").as_bytes()
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
        .bind_root_provider_identity(
            &owner,
            "local-production-measurement",
            *blake3::hash(b"local-production-measurement").as_bytes(),
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

#[tokio::test]
async fn production_admission_enforces_active_then_total_limits() -> Result<()> {
    let (_root, client) = client().await?;
    let session = OperationId::new();
    let owner = SwarmOwnerFence::new("production-owner", 0)?;
    let limits = SwarmBudgetLimits {
        max_active_agents: 2,
        max_total_agents: 3,
        max_recursion_depth: 1,
        max_model_steps: 20,
        max_output_bytes: 200,
        max_execution_time_ms: 2_000,
    };
    let mut journal = SwarmBudgetJournal::start(&client, session, owner.clone(), limits).await?;

    let first = request(OperationId::new(), "active-first".into());
    journal.reserve_child(first.clone()).await?;
    assert!(matches!(
        journal
            .reserve_child(request(OperationId::new(), "active-rejected".into()))
            .await,
        Err(Error::Conflict(message)) if message.contains("agent limit")
    ));

    journal.cancel(first.operation_id, &owner).await?;
    journal
        .reserve_child(request(OperationId::new(), "total-second".into()))
        .await?;
    assert!(matches!(
        journal
            .reserve_child(request(OperationId::new(), "total-rejected".into()))
            .await,
        Err(Error::Conflict(message)) if message.contains("agent limit")
    ));
    let usage = journal.usage()?;
    assert_eq!(usage.active_agents, 2);
    assert_eq!(usage.total_agents, 3);
    Ok(())
}

#[tokio::test]
async fn production_admission_enforces_depth_and_each_session_resource_dimension() -> Result<()> {
    let (_root, client) = client().await?;
    let session = OperationId::new();
    let owner = SwarmOwnerFence::new("production-owner", 0)?;
    let limits = SwarmBudgetLimits {
        max_active_agents: 4,
        max_total_agents: 4,
        max_recursion_depth: 1,
        max_model_steps: 10,
        max_output_bytes: 100,
        max_execution_time_ms: 1_000,
    };
    let mut journal = SwarmBudgetJournal::start(&client, session, owner, limits).await?;
    let parent = request_with_resources(
        OperationId::new(),
        "depth-parent".into(),
        SwarmResourceRequest {
            model_steps: 2,
            output_bytes: 20,
            execution_time_ms: 200,
        },
    );
    let parent_id = parent.operation_id;
    journal.reserve_child(parent).await?;
    let mut descendant = request(OperationId::new(), "depth-rejected".into());
    descendant.parent_operation_id = Some(parent_id);
    descendant.depth = 2;
    assert!(matches!(
        journal.reserve_child(descendant).await,
        Err(Error::Conflict(message)) if message.contains("recursion depth")
    ));

    for (key, resources, dimension) in [
        (
            "session-model-rejected",
            SwarmResourceRequest {
                model_steps: 11,
                output_bytes: 1,
                execution_time_ms: 1,
            },
            "model",
        ),
        (
            "session-output-rejected",
            SwarmResourceRequest {
                model_steps: 1,
                output_bytes: 101,
                execution_time_ms: 1,
            },
            "output",
        ),
        (
            "session-time-rejected",
            SwarmResourceRequest {
                model_steps: 1,
                output_bytes: 1,
                execution_time_ms: 1_001,
            },
            "execution",
        ),
    ] {
        let result = journal
            .reserve_child(request_with_resources(OperationId::new(), key.into(), resources))
            .await;
        assert!(
            matches!(result, Err(Error::Conflict(message)) if message.contains("resource budget")),
            "{dimension} dimension must be rejected"
        );
    }
    Ok(())
}

#[tokio::test]
async fn production_descendant_reservations_share_parent_remaining_budget() -> Result<()> {
    let (_root, client) = client().await?;
    let session = OperationId::new();
    let owner = SwarmOwnerFence::new("production-owner", 0)?;
    let limits = SwarmBudgetLimits {
        max_active_agents: 8,
        max_total_agents: 8,
        max_recursion_depth: 2,
        max_model_steps: 32,
        max_output_bytes: 320,
        max_execution_time_ms: 3_200,
    };
    let mut journal = SwarmBudgetJournal::start(&client, session, owner, limits).await?;
    let parent = request_with_resources(
        OperationId::new(),
        "parent-allocation".into(),
        SwarmResourceRequest {
            model_steps: 8,
            output_bytes: 80,
            execution_time_ms: 800,
        },
    );
    let parent_id = parent.operation_id;
    journal.reserve_child(parent).await?;

    for (key, steps) in [("descendant-one", 5), ("descendant-two", 3)] {
        let mut child = request_with_resources(
            OperationId::new(),
            key.into(),
            SwarmResourceRequest {
                model_steps: steps,
                output_bytes: steps * 10,
                execution_time_ms: steps * 100,
            },
        );
        child.parent_operation_id = Some(parent_id);
        child.depth = 2;
        journal.reserve_child(child).await?;
    }
    let mut over = request_with_resources(
        OperationId::new(),
        "descendant-over-allocation".into(),
        SwarmResourceRequest {
            model_steps: 1,
            output_bytes: 10,
            execution_time_ms: 100,
        },
    );
    over.parent_operation_id = Some(parent_id);
    over.depth = 2;
    assert!(matches!(
        journal.reserve_child(over).await,
        Err(Error::Conflict(message)) if message.contains("parent remaining")
    ));
    Ok(())
}

#[tokio::test]
async fn production_concurrent_reservations_cannot_double_spend_resource_budget() -> Result<()> {
    let (_root, client) = client().await?;
    let session = OperationId::new();
    let owner = SwarmOwnerFence::new("production-owner", 0)?;
    let limits = SwarmBudgetLimits {
        max_active_agents: 10,
        max_total_agents: 10,
        max_recursion_depth: 1,
        max_model_steps: 6,
        max_output_bytes: 60,
        max_execution_time_ms: 600,
    };
    SwarmBudgetJournal::start(&client, session, owner, limits).await?;
    let journals = join_all((0..12).map(|_| SwarmBudgetJournal::open(&client, session)))
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    let results = join_all(journals.into_iter().enumerate().map(|(index, mut journal)| {
        let resources = SwarmResourceRequest {
            model_steps: 2,
            output_bytes: 20,
            execution_time_ms: 200,
        };
        async move {
            journal
                .reserve_child(request_with_resources(
                    OperationId::new(),
                    format!("resource-race-{index}"),
                    resources,
                ))
                .await
        }
    }))
    .await;
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 3);
    let reopened = SwarmBudgetJournal::open(&client, session).await?;
    let usage = reopened.usage()?;
    assert_eq!(usage.reserved.model_steps, limits.max_model_steps);
    assert_eq!(usage.reserved.output_bytes, limits.max_output_bytes);
    assert_eq!(
        usage.reserved.execution_time_ms,
        limits.max_execution_time_ms
    );
    Ok(())
}
