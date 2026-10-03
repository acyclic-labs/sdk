#![cfg(feature = "filesystem-local")]

use acyclic_fs::{LocalFs, LocalOptions};
use acyclic_harness::{
    IdempotencyKey, OperationId,
    swarm_budget::{
        ForkPublication, SwarmBudget, SwarmBudgetEvent, SwarmBudgetLimits, SwarmForkRequest,
        SwarmOwnerFence, SwarmResourceRequest, SwarmUsage, SwarmUsageReceipt,
        SwarmUsageReceiptIssuer, SwarmUsageSource,
    },
    swarm_budget_journal::SwarmBudgetJournal,
};
use acyclic_stream::{LocalStream, LocalStreamLimits, StreamClient};
use futures::future::join_all;
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};
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

struct LocalMeasuredUsage;

impl SwarmUsageSource for LocalMeasuredUsage {
    fn provider_identity(&self) -> &str {
        "local-stream-test-provider"
    }

    fn cumulative_usage(
        &self,
        _operation_id: OperationId,
        _dispatch_id: &IdempotencyKey,
    ) -> acyclic_harness::Result<SwarmUsage> {
        Ok(SwarmUsage {
            model_steps: 1,
            output_bytes: 8,
            execution_time_ms: 10,
        })
    }
}

struct LocalMeasuredUsageSequence {
    provider: String,
    snapshots: Mutex<VecDeque<SwarmUsage>>,
}

impl LocalMeasuredUsageSequence {
    fn new(provider: impl Into<String>, snapshots: impl IntoIterator<Item = SwarmUsage>) -> Self {
        Self {
            provider: provider.into(),
            snapshots: Mutex::new(snapshots.into_iter().collect()),
        }
    }
}

impl SwarmUsageSource for LocalMeasuredUsageSequence {
    fn provider_identity(&self) -> &str {
        &self.provider
    }

    fn cumulative_usage(
        &self,
        _operation_id: OperationId,
        _dispatch_id: &IdempotencyKey,
    ) -> acyclic_harness::Result<SwarmUsage> {
        self.snapshots
            .lock()
            .map_err(|_| acyclic_harness::Error::Storage("measurement lock poisoned".into()))?
            .pop_front()
            .ok_or_else(|| acyclic_harness::Error::Storage("measurement exhausted".into()))
    }
}

fn ancestor_request(
    operation_id: OperationId,
    key: &str,
    parent_operation_id: Option<OperationId>,
    depth: u32,
    resources: SwarmResourceRequest,
) -> SwarmForkRequest {
    SwarmForkRequest {
        operation_id,
        idempotency_key: IdempotencyKey::new(key).expect("key"),
        parent_operation_id,
        depth,
        resources,
        admission_digest: None,
    }
}

fn publication(
    operation_id: OperationId,
    parent_operation_id: Option<OperationId>,
) -> ForkPublication {
    ForkPublication {
        operation_id,
        parent_operation_id,
        completed_boundary_digest: [1; 32],
        workspace_generation_digest: [2; 32],
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
    let mut root_usage = SwarmUsageReceiptIssuer::new(
        LocalMeasuredUsage,
        session,
        IdempotencyKey::new("root-lease").expect("root dispatch"),
    )
    .expect("root usage issuer");
    journal
        .report_root_usage_with_receipt(&owner, root_usage.issue().expect("root usage receipt"))
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

#[tokio::test]
async fn local_stream_wrong_dispatch_receipt_is_rejected_before_append_and_reopen() {
    let root = tempdir().expect("temporary root");
    let stream_path = root.path().join("stream");
    let client = StreamClient::new(Arc::new(
        LocalStream::open(&stream_path, LocalStreamLimits::default())
            .await
            .expect("local stream provider"),
    ));
    let session = OperationId::new();
    let owner = SwarmOwnerFence::new("worker-a", 0).expect("owner");
    let dispatch_id = IdempotencyKey::new("root-dispatch").expect("dispatch");
    let usage = SwarmUsage {
        model_steps: 1,
        output_bytes: 8,
        execution_time_ms: 10,
    };
    let mut journal = SwarmBudgetJournal::start(&client, session, owner.clone(), limits())
        .await
        .expect("start budget");
    let mut issuer = SwarmUsageReceiptIssuer::new(
        LocalMeasuredUsage,
        session,
        dispatch_id.clone(),
    )
    .expect("usage issuer");
    journal
        .report_root_usage_with_receipt(&owner, issuer.issue().expect("first receipt"))
        .await
        .expect("first root receipt");

    let stream = client
        .stream(format!("harness/v2/swarm-budget/{session}"))
        .expect("budget stream");
    let tail_before = stream.tail().await.expect("tail before forged receipt");
    let mut forged_issuer = SwarmUsageReceiptIssuer::resume(
        LocalMeasuredUsage,
        session,
        IdempotencyKey::new("forged-dispatch").expect("forged dispatch"),
        1,
        Some(usage),
    )
    .expect("forged issuer");
    let result = journal
        .report_root_usage_with_receipt(&owner, forged_issuer.issue().expect("forged receipt"))
        .await;
    assert!(result.is_err(), "a receipt for another dispatch must fail");
    assert_eq!(
        stream.tail().await.expect("tail after forged receipt"),
        tail_before,
        "rejected receipt must not poison the durable tail"
    );
    drop(journal);
    let reopened = SwarmBudgetJournal::open(&client, session)
        .await
        .expect("reopen after forged receipt");
    assert_eq!(reopened.usage().expect("reopened usage").consumed, usage);
}

#[tokio::test]
async fn local_stream_resumed_receipt_cursor_preserves_cumulative_usage() {
    let root = tempdir().expect("temporary root");
    let client = StreamClient::new(Arc::new(
        LocalStream::open(root.path().join("stream"), LocalStreamLimits::default())
            .await
            .expect("local stream provider"),
    ));
    let session = OperationId::new();
    let owner = SwarmOwnerFence::new("worker-a", 0).expect("owner");
    let dispatch_id = IdempotencyKey::new("resumed-root-dispatch").expect("dispatch");
    let first_usage = SwarmUsage {
        model_steps: 1,
        output_bytes: 8,
        execution_time_ms: 10,
    };
    let second_usage = SwarmUsage {
        model_steps: 3,
        output_bytes: 24,
        execution_time_ms: 30,
    };
    let mut journal = SwarmBudgetJournal::start(&client, session, owner.clone(), limits())
        .await
        .expect("start budget");
    let mut issuer = SwarmUsageReceiptIssuer::new(
        LocalMeasuredUsageSequence::new("provider-a", [first_usage]),
        session,
        dispatch_id.clone(),
    )
    .expect("usage issuer");
    let first_receipt = issuer.issue().expect("first receipt");
    journal
        .report_root_usage_with_receipt(&owner, first_receipt)
        .await
        .expect("first root receipt");
    drop(journal);

    let mut reopened = SwarmBudgetJournal::open(&client, session)
        .await
        .expect("reopen budget");
    let mut resumed = SwarmUsageReceiptIssuer::resume(
        LocalMeasuredUsageSequence::new("provider-a", [second_usage]),
        session,
        dispatch_id,
        1,
        Some(first_usage),
    )
    .expect("resume usage issuer");
    let second_receipt = resumed.issue().expect("resumed receipt");
    reopened
        .report_root_usage_with_receipt(&owner, second_receipt)
        .await
        .expect("resumed root receipt");
    drop(reopened);

    let final_journal = SwarmBudgetJournal::open(&client, session)
        .await
        .expect("final reopen budget");
    assert_eq!(
        final_journal.usage().expect("final usage").consumed,
        second_usage,
        "cumulative provider usage must be charged by delta after resume"
    );
}

#[tokio::test]
async fn local_stream_sixteen_independent_providers_have_one_receipt_winner() {
    let root = tempdir().expect("temporary root");
    let stream_path = root.path().join("stream");
    let client = StreamClient::new(Arc::new(
        LocalStream::open(&stream_path, LocalStreamLimits::default())
            .await
            .expect("local stream provider"),
    ));
    let session = OperationId::new();
    let owner = SwarmOwnerFence::new("worker-a", 0).expect("owner");
    let dispatch_id = IdempotencyKey::new("shared-root-dispatch").expect("dispatch");
    let first_usage = SwarmUsage {
        model_steps: 1,
        output_bytes: 8,
        execution_time_ms: 10,
    };
    let second_usage = SwarmUsage {
        model_steps: 2,
        output_bytes: 16,
        execution_time_ms: 20,
    };
    let mut journal = SwarmBudgetJournal::start(&client, session, owner.clone(), limits())
        .await
        .expect("start budget");
    let mut issuer = SwarmUsageReceiptIssuer::new(
        LocalMeasuredUsageSequence::new("bootstrap-provider", [first_usage]),
        session,
        dispatch_id.clone(),
    )
    .expect("bootstrap issuer");
    journal
        .report_root_usage_with_receipt(&owner, issuer.issue().expect("bootstrap receipt"))
        .await
        .expect("bootstrap root receipt");
    drop(journal);

    let results = join_all((0..16).map(|index| {
        let stream_path = stream_path.clone();
        let owner = owner.clone();
        let dispatch_id = dispatch_id.clone();
        async move {
            let provider = LocalStream::open(stream_path, LocalStreamLimits::default())
                .await
                .expect("independent local stream provider");
            let client = StreamClient::new(Arc::new(provider));
            let mut journal = SwarmBudgetJournal::open(&client, session)
                .await
                .expect("open independent journal");
            let mut issuer = SwarmUsageReceiptIssuer::resume(
                LocalMeasuredUsageSequence::new(
                    format!("independent-provider-{index}"),
                    [second_usage],
                ),
                session,
                dispatch_id,
                1,
                Some(first_usage),
            )
            .expect("independent issuer");
            journal
                .report_root_usage_with_receipt(&owner, issuer.issue().expect("receipt"))
                .await
        }
    }))
    .await;
    assert_eq!(
        results.iter().filter(|result| result.is_ok()).count(),
        1,
        "one CAS winner must publish the next cumulative receipt"
    );
    assert_eq!(
        results.iter().filter(|result| result.is_err()).count(),
        15,
        "losing independent providers must not append competing receipts"
    );

    let reopened = SwarmBudgetJournal::open(&client, session)
        .await
        .expect("reopen budget");
    assert_eq!(reopened.usage().expect("usage").consumed, second_usage);
}

#[test]
fn replay_rejects_live_usage_that_exceeds_an_ancestor_ceiling() -> acyclic_harness::Result<()> {
    let session_id = OperationId::new();
    let owner = SwarmOwnerFence::new("worker-a", 0)?;
    let limits = SwarmBudgetLimits {
        max_active_agents: 8,
        max_total_agents: 8,
        max_recursion_depth: 3,
        max_model_steps: 40,
        max_output_bytes: 400,
        max_execution_time_ms: 4_000,
    };
    let parent_resources = SwarmResourceRequest {
        model_steps: 10,
        output_bytes: 100,
        execution_time_ms: 1_000,
    };
    let child_resources = parent_resources;
    let grandchild_resources = SwarmResourceRequest {
        model_steps: 4,
        output_bytes: 40,
        execution_time_ms: 400,
    };
    let projection = SwarmBudget::new(session_id, owner.clone(), limits)?;
    let parent = projection
        .reserve_child(ancestor_request(
            OperationId::new(),
            "ancestor-parent",
            None,
            1,
            parent_resources,
        ))?
        .reservation;
    let child = projection
        .reserve_child(ancestor_request(
            OperationId::new(),
            "ancestor-child",
            Some(parent.operation_id),
            2,
            child_resources,
        ))?
        .reservation;
    let grandchild = projection
        .reserve_child(ancestor_request(
            OperationId::new(),
            "ancestor-grandchild",
            Some(child.operation_id),
            3,
            grandchild_resources,
        ))?
        .reservation;
    let parent_dispatch = IdempotencyKey::new("ancestor-parent-dispatch")?;
    let grandchild_dispatch = IdempotencyKey::new("ancestor-grandchild-dispatch")?;
    let parent_usage = SwarmUsage {
        model_steps: 10,
        output_bytes: 100,
        execution_time_ms: 1_000,
    };
    let grandchild_usage = SwarmUsage {
        model_steps: 4,
        output_bytes: 40,
        execution_time_ms: 400,
    };
    let parent_receipt = SwarmUsageReceipt::new(
        parent.operation_id,
        parent_dispatch.clone(),
        1,
        parent_usage,
        "parent-provider",
    )?;
    let grandchild_receipt = SwarmUsageReceipt::new(
        grandchild.operation_id,
        grandchild_dispatch.clone(),
        1,
        grandchild_usage,
        "grandchild-provider",
    )?;
    let events = vec![
        SwarmBudgetEvent::Started {
            session_id,
            owner: owner.clone(),
            limits,
        },
        SwarmBudgetEvent::ChildReserved {
            reservation: parent.clone(),
        },
        SwarmBudgetEvent::ChildReserved {
            reservation: child.clone(),
        },
        SwarmBudgetEvent::ChildReserved {
            reservation: grandchild.clone(),
        },
        SwarmBudgetEvent::ChildActivated {
            operation_id: parent.operation_id,
            owner: owner.clone(),
            publication: publication(parent.operation_id, None),
            dispatch_id: Some(parent_dispatch),
        },
        SwarmBudgetEvent::ChildActivated {
            operation_id: child.operation_id,
            owner: owner.clone(),
            publication: publication(child.operation_id, Some(parent.operation_id)),
            dispatch_id: Some(IdempotencyKey::new("ancestor-child-dispatch")?),
        },
        SwarmBudgetEvent::ChildActivated {
            operation_id: grandchild.operation_id,
            owner: owner.clone(),
            publication: publication(grandchild.operation_id, Some(child.operation_id)),
            dispatch_id: Some(grandchild_dispatch),
        },
        SwarmBudgetEvent::UsageReported {
            operation_id: parent.operation_id,
            owner: owner.clone(),
            usage: parent_usage,
            receipt: parent_receipt,
        },
        SwarmBudgetEvent::UsageReported {
            operation_id: grandchild.operation_id,
            owner,
            usage: grandchild_usage,
            receipt: grandchild_receipt,
        },
    ];
    assert!(
        SwarmBudget::replay(events).is_err(),
        "live parent plus descendant usage must stay within each ancestor ceiling"
    );
    Ok(())
}
