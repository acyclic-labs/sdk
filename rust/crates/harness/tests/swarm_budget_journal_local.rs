#![cfg(feature = "filesystem-local")]

use acyclic_fs::{LocalFs, LocalOptions};
use acyclic_harness::{
    IdempotencyKey, OperationId,
    swarm_budget::{
        SwarmBudgetLimits, SwarmForkRequest, SwarmOwnerFence, SwarmResourceRequest, SwarmUsage,
    },
    swarm_budget_journal::SwarmBudgetJournal,
};
use acyclic_stream::{LocalStream, LocalStreamLimits, StreamClient};
use futures::future::join_all;
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
        admission_digest: None,
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
    journal
        .report_root_usage(
            &owner,
            SwarmUsage {
                model_steps: 1,
                output_bytes: 8,
                execution_time_ms: 10,
            },
        )
        .await
        .expect("report root usage");
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
        .takeover(&owner, "worker-b")
        .await
        .expect("take over budget");
    assert_eq!(new_owner.generation, 1);
    let stale = restarted
        .cancel(child, &owner)
        .await
        .expect_err("stale owner must be fenced");
    assert!(stale.to_string().contains("stale"));
    restarted
        .cancel(child, &new_owner)
        .await
        .expect("cancel rejected dispatch");
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
async fn local_stream_budget_rejects_invalid_descriptor_before_append() {
    let root = tempdir().expect("temporary root");
    let client = StreamClient::new(Arc::new(
        LocalStream::open(root.path().join("stream"), LocalStreamLimits::default())
            .await
            .expect("local stream provider"),
    ));
    let session = OperationId::new();
    let owner = SwarmOwnerFence::new("worker-a", 0).expect("owner");
    let mut invalid = limits();
    invalid.max_model_steps = 0;
    assert!(
        SwarmBudgetJournal::start(&client, session, owner.clone(), invalid)
            .await
            .is_err()
    );
    SwarmBudgetJournal::start(&client, session, owner, limits())
        .await
        .expect("valid descriptor can append after rejected start");
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

#[tokio::test]
async fn local_stream_budget_same_operation_race_has_one_append_and_replays() {
    let root = tempdir().expect("temporary root");
    let client = StreamClient::new(Arc::new(
        LocalStream::open(root.path().join("stream"), LocalStreamLimits::default())
            .await
            .expect("local stream provider"),
    ));
    let session = OperationId::new();
    let owner = SwarmOwnerFence::new("worker-a", 0).expect("owner");
    let race_limits = SwarmBudgetLimits {
        max_active_agents: 17,
        max_total_agents: 17,
        max_recursion_depth: 1,
        max_model_steps: 64,
        max_output_bytes: 1_024,
        max_execution_time_ms: 1_600,
    };
    SwarmBudgetJournal::start(&client, session, owner, race_limits)
        .await
        .expect("start budget");
    let journals = join_all((0..16).map(|_| SwarmBudgetJournal::open(&client, session)))
        .await
        .into_iter()
        .map(|journal| journal.expect("open budget"));
    let journals = journals.collect::<Vec<_>>();
    let operation_id = OperationId::new();
    let request = request(operation_id, "same-operation-race");
    let results = join_all(journals.into_iter().map(|mut journal| {
        let request = request.clone();
        async move { journal.reserve_child(request).await }
    }))
    .await;
    let applied = results
        .iter()
        .filter(|result| matches!(result, Ok(receipt) if !receipt.replayed))
        .count();
    let replayed = results
        .iter()
        .filter(|result| matches!(result, Ok(receipt) if receipt.replayed))
        .count();
    assert_eq!(applied, 1);
    assert_eq!(replayed, 15);
    let journal = SwarmBudgetJournal::open(&client, session)
        .await
        .expect("reopen budget");
    let usage = journal.usage().expect("usage");
    assert_eq!(usage.active_agents, 2);
    assert_eq!(usage.total_agents, 2);
    assert_eq!(usage.reserved.model_steps, 4);
}
