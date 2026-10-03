#![cfg(feature = "filesystem-local")]

use acyclic_fs::{LocalFs, LocalOptions};
use acyclic_harness::{
    Error, IdempotencyKey, OperationId,
    swarm_budget::{
        ForkPublication, SwarmBudgetLimits, SwarmForkRequest, SwarmOwnerFence,
        SwarmResourceRequest, SwarmUsage,
    },
    swarm_budget_journal::SwarmBudgetJournal,
};
use acyclic_stream::{LocalStream, LocalStreamLimits, StreamClient};
use std::sync::Arc;
use tempfile::tempdir;

fn request(operation_id: OperationId, key: &str) -> SwarmForkRequest {
    SwarmForkRequest {
        operation_id,
        idempotency_key: IdempotencyKey::new(key).expect("key"),
        parent_operation_id: None,
        depth: 1,
        resources: SwarmResourceRequest {
            model_steps: 4,
            output_bytes: 64,
            execution_time_ms: 100,
        },
    }
}

fn limits() -> SwarmBudgetLimits {
    SwarmBudgetLimits {
        max_active_agents: 3,
        max_total_agents: 4,
        max_recursion_depth: 2,
        max_model_steps: 12,
        max_output_bytes: 192,
        max_execution_time_ms: 300,
    }
}

fn tight_limits() -> SwarmBudgetLimits {
    SwarmBudgetLimits {
        max_active_agents: 2,
        max_total_agents: 2,
        max_recursion_depth: 1,
        max_model_steps: 8,
        max_output_bytes: 128,
        max_execution_time_ms: 200,
    }
}

#[tokio::test]
async fn local_stream_budget_restarts_and_fences_stale_owner() {
    let root = tempdir().expect("temporary root");
    let _filesystem = LocalFs::local(LocalOptions::new(root.path().join("filesystem")))
        .await
        .expect("local filesystem provider");
    let client = StreamClient::new(Arc::new(
        LocalStream::open(root.path().join("stream"), LocalStreamLimits::default())
            .await
            .expect("local stream provider"),
    ));
    let session = OperationId::new();
    let owner = SwarmOwnerFence::new("worker-a", 0).expect("owner");
    let mut journal = SwarmBudgetJournal::start(&client, session, owner.clone(), limits())
        .await
        .expect("start budget");
    let child = OperationId::new();
    let admission = journal
        .reserve_child(request(child, "child-1"))
        .await
        .expect("reserve child");
    let replay = journal
        .reserve_child(request(child, "child-1"))
        .await
        .expect("retry child");
    assert!(replay.replayed);
    assert_eq!(admission.reservation, replay.reservation);

    let publication = ForkPublication {
        operation_id: child,
        parent_operation_id: None,
        completed_boundary_digest: [1; 32],
        workspace_generation_digest: [2; 32],
    };
    journal
        .activate(child, owner.clone(), publication)
        .await
        .expect("activate child");
    journal
        .report_usage(
            child,
            &owner,
            SwarmUsage {
                model_steps: 2,
                output_bytes: 16,
                execution_time_ms: 20,
            },
        )
        .await
        .expect("report child usage");
    let consumed_before_cancel = journal.usage().expect("usage").consumed;

    drop(journal);
    let mut restarted = SwarmBudgetJournal::open(&client, session)
        .await
        .expect("reopen budget");
    assert_eq!(
        restarted.usage().expect("restarted usage").consumed,
        consumed_before_cancel
    );
    let new_owner = restarted
        .takeover("worker-b", 0)
        .await
        .expect("take over budget");
    assert_eq!(new_owner.generation, 1);
    let stale = restarted
        .activate(child, owner, publication)
        .await
        .expect_err("stale owner must be fenced");
    assert!(stale.to_string().contains("stale"));
    restarted
        .dispatch_after_publication(child, new_owner, publication, |_token| async {
            Err::<(), _>(Error::Conflict("mock dispatcher rejected child".into()))
        })
        .await
        .expect_err("dispatcher rejection must be returned");
    let usage = restarted.usage().expect("final usage");
    assert_eq!(usage.active_agents, 1);
    assert_eq!(usage.total_agents, 2);
    assert_eq!(usage.consumed, consumed_before_cancel);
    assert_eq!(usage.reserved, SwarmUsage::default());
}

#[tokio::test]
async fn local_stream_budget_start_race_reopens_exact_descriptor() {
    let root = tempdir().expect("temporary root");
    let client = StreamClient::new(Arc::new(
        LocalStream::open(root.path().join("stream"), LocalStreamLimits::default())
            .await
            .expect("local stream provider"),
    ));
    let session = OperationId::new();
    let owner = SwarmOwnerFence::new("worker-a", 0).expect("owner");
    let first = SwarmBudgetJournal::start(&client, session, owner.clone(), limits());
    let second = SwarmBudgetJournal::start(&client, session, owner, limits());
    let (first, second) = tokio::join!(first, second);
    first.expect("first start");
    second.expect("second exact start");
}

#[tokio::test]
async fn local_stream_budget_concurrent_reservations_are_tail_atomic() {
    let root = tempdir().expect("temporary root");
    let client = StreamClient::new(Arc::new(
        LocalStream::open(root.path().join("stream"), LocalStreamLimits::default())
            .await
            .expect("local stream provider"),
    ));
    let session = OperationId::new();
    let owner = SwarmOwnerFence::new("worker-a", 0).expect("owner");
    let mut first = SwarmBudgetJournal::start(&client, session, owner, tight_limits())
        .await
        .expect("start budget");
    let mut second = SwarmBudgetJournal::open(&client, session)
        .await
        .expect("reopen budget");
    let first_request = request(OperationId::new(), "race-1");
    let second_request = request(OperationId::new(), "race-2");
    let (first, second) = tokio::join!(
        first.reserve_child(first_request),
        second.reserve_child(second_request)
    );
    assert_ne!(first.is_ok(), second.is_ok());
}
