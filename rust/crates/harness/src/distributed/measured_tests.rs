use super::*;
use super::tests::{declare, spec};
use crate::store::history::HistoryWork;
use acyclic_fs::{CancellationToken, WorkBudget};
use acyclic_stream::MemoryStream;
use std::sync::atomic::{AtomicUsize, Ordering};

type FaultStream = crate::test_stream::LostSessionAck<MemoryStream>;

const BUDGET: WorkBudget = acyclic_fs::performance::work_budget(64 * 1024 * 1024, 64 * 1024 * 1024);

#[derive(Default)]
struct UnmeasuredContent(AtomicUsize);
impl ContentResidencyVerifier for UnmeasuredContent {
    fn verify<'a>(&'a self, reference: &'a FileRef) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            reference.descriptor().verify(b"null")
        })
    }
}

async fn append_source(
    client: &std::sync::Arc<FaultStream>, revision: u64, key: &str, event: SchedulerEvent, time: u64,
) -> Result<u64> {
    let canonical = crate::contract::canonical_json_bytes(&event)?;
    let bytes = encode(revision, event.operation_id(), key,
        *blake3::hash(&canonical).as_bytes(), canonical, time, &mut None)?;
    let length = bytes.len() as u64;
    acyclic_stream::Stream::new(std::sync::Arc::clone(&client), COORDINATOR_PATH)?.append_batch(
        vec![OwnedBytes::from(bytes)], Some(revision - 1), None,
    ).await?;
    Ok(length)
}

#[tokio::test]
async fn coordinator_quota_and_cancellation_stop_before_the_next_real_sdk_call() -> Result<()> {
    for mode in 0..3 {
        let provider = Arc::new(FaultStream::default());
        let client = provider.clone();
        let mut coordinator = DistributedCoordinator::open(&client, Arc::new(UnmeasuredContent::default())).await?;
        let bytes = append_source(&client, 1, "measured-quota", SchedulerEvent::Declared {
            spec: Box::new(spec(OperationId::from_bytes([104; 16]), 0)?),
        }, 1).await?;
        provider.reset_io();
        let cancellation = CancellationToken::new();
        if mode == 0 { cancellation.cancel(); }
        let mut allowance = BUDGET;
        if mode == 1 { allowance.source_bytes_read = 0; }
        if mode == 2 { allowance.backend_read_operations = 0; }
        let mut work = HistoryWork::new(allowance, &cancellation);
        let result = coordinator.refresh_with_work(&mut Some(&mut work)).await;
        let failure = work.finish(result).expect_err("admission must fail closed");
        assert_eq!(provider.io_snapshot().reads, u64::from(mode == 1));
        assert_eq!(failure.work.backend_read_operations, u64::from(mode == 1));
        assert_eq!(failure.work.source_bytes_read, if mode == 1 { bytes } else { 0 });
        assert_eq!(provider.io_snapshot().writes, 0);
        assert_eq!(coordinator.revision, 0);
    }
    Ok(())
}

#[tokio::test]
async fn declared_content_without_measured_capability_rejects_before_ordinary_content_io() -> Result<()> {
    let provider = Arc::new(FaultStream::default());
    let client = provider.clone();
    let content = Arc::new(UnmeasuredContent::default());
    let mut coordinator = DistributedCoordinator::open(&client, content.clone()).await?;
    append_source(&client, 1, "unmeasured-content", SchedulerEvent::Declared {
        spec: Box::new(spec(OperationId::from_bytes([105; 16]), 0)?),
    }, 1).await?;
    provider.reset_io();
    let cancellation = CancellationToken::new();
    let mut work = HistoryWork::new(BUDGET, &cancellation);
    let result = coordinator.refresh_with_work(&mut Some(&mut work)).await;
    let failure = work.finish(result).expect_err("ordinary verification is not a finite capability");
    assert!(matches!(failure.error, Error::Unsupported(_)));
    assert_eq!(content.0.load(Ordering::SeqCst), 0);
    assert_eq!(failure.work.object_bytes_read, 0);
    assert_eq!(failure.work.backend_write_operations, 0);
    assert_eq!(provider.io_snapshot().reads, 1);
    assert_eq!(coordinator.revision, 0);
    Ok(())
}

async fn admitted_fixture(
    client: &std::sync::Arc<FaultStream>,
    operation: OperationId,
) -> Result<(DistributedCoordinator<FaultStream>, WorkLease)> {
    let mut coordinator = DistributedCoordinator::open(client, Arc::new(UnmeasuredContent::default())).await?;
    declare(&mut coordinator, spec(operation, 0)?, "measured-seed-0").await?;
    let lease = coordinator.pull(&Worker {
        id: "measured-release-worker".into(),
        available: ResourceSnapshot::default(),
        labels: BTreeMap::new(),
    }).await?.ok_or_else(|| Error::NotFound("original admitted lease".into()))?;
    Ok((coordinator, lease))
}

#[tokio::test]
async fn measured_quiescent_release_requires_witness_and_the_same_owner_verified_revision() -> Result<()> {
    let provider = Arc::new(FaultStream::default());
    let client = provider.clone();
    let operation = OperationId::from_bytes([120; 16]);
    let execution = OperationId::from_bytes([121; 16]);
    let (mut coordinator, lease) = admitted_fixture(&client, operation).await?;
    let cancellation = CancellationToken::new();
    let mut work = HistoryWork::new(BUDGET, &cancellation);
    provider.reset_io();
    assert!(matches!(coordinator.apply_if_execution_quiescent(
        IdempotencyKey::new("no-workflow-witness")?,
        SchedulerEvent::LeaseReleased {
            operation_id:operation, fence:LeaseFence::from(&lease.reservation),
        }, execution, 0, None, &mut Some(&mut work),
    ).await, Err(Error::Invalid(_))));
    assert_eq!(provider.io_snapshot().reads, 0);
    assert_eq!(provider.io_snapshot().writes, 0);
    let mut remote = DistributedCoordinator::open(&client, Arc::new(UnmeasuredContent::default())).await?;
    remote.apply(operation, IdempotencyKey::new("remote-original-start")?,
        SchedulerEvent::Started {
            operation_id:operation, fence:LeaseFence::from(&lease.reservation),
        }).await?;
    provider.reset_io();
    let result = coordinator.apply_if_execution_quiescent(
        IdempotencyKey::new("stale-owner-suspension")?,
        SchedulerEvent::WorkflowSuspended {
            operation_id:operation, fence:LeaseFence::from(&lease.reservation),
            workflow_revision:1, waiting_command:Some(execution),
        }, execution, 0, None, &mut Some(&mut work),
    ).await;
    assert!(matches!(result, Err(Error::Conflict(_))));
    assert_eq!(provider.io_snapshot().writes, 0);
    assert_eq!(coordinator.revision, remote.revision);
    assert_eq!(coordinator.scheduler(), remote.scheduler());
    Ok(())
}

async fn historical_source(
    client: &std::sync::Arc<FaultStream>, operation: OperationId,
) -> Result<(Authority, crate::core::AuthorityIssuer, Scope, LeaseFence, u64)> {
    let declaration = spec(operation, 0)?;
    let owner = declaration.owner.authority().clone();
    let issuer = crate::core::AuthorityIssuer::new("test-runtime", [9; 32], owner.clone());
    let declaration_scope = declaration.owner_scope.clone();
    let mut coordinator = DistributedCoordinator::open(client, Arc::new(UnmeasuredContent::default())).await?;
    coordinator.declare_operation(&owner, &declaration_scope, &issuer.verifier(), declaration,
        IdempotencyKey::new(format!("task-admission:{operation}"))?,
    None,).await?;
    let time = coordinator.last_committed_at_ms;
    let fence = LeaseFence { reservation_id: "original-provider-lease".into(), placement: "original-worker".into() };
    append_source(client, 2, "original-admitted", SchedulerEvent::Admitted {
        operation_id: operation,
        reservation: Reservation { id: fence.reservation_id.clone(), placement: fence.placement.clone(),
            admitted: crate::scheduler::ResourceRequest::default() },
    }, time + 1).await?;
    append_source(client, 3, "original-started", SchedulerEvent::Started {
        operation_id: operation, fence: fence.clone(),
    }, time + 2).await?;
    let scope = issuer.root("original-readonly-proof", crate::Capabilities::new(["operation:observe"]));
    Ok((owner, issuer, scope, fence, time))
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_reservation_proof_keeps_the_selected_old_origin_without_current_write_authority() -> Result<()> {
    let provider = Arc::new(FaultStream::default());
    let client = provider.clone();
    let operation = OperationId::from_bytes([128; 16]);
    let (owner, issuer, scope, original, time) = historical_source(&client, operation).await?;
    append_source(&client, 4, "original-released", SchedulerEvent::LeaseReleased {
        operation_id: operation, fence: original.clone(),
    }, time + 3).await?;
    let current = LeaseFence { reservation_id: "current-provider-lease".into(), placement: "current-worker".into() };
    append_source(&client, 5, "current-admitted", SchedulerEvent::Admitted {
        operation_id: operation, reservation: Reservation { id: current.reservation_id.clone(),
            placement: current.placement.clone(), admitted: crate::scheduler::ResourceRequest::default() },
    }, time + 4).await?;
    append_source(&client, 6, "current-started", SchedulerEvent::Started {
        operation_id: operation, fence: current,
    }, time + 5).await?;
    provider.reset_io();
    provider.forbid_writes.store(true, Ordering::SeqCst);
    let cancellation = CancellationToken::new();
    let native = acyclic_fs::NativeMaterializationWork::new(BUDGET, &cancellation)
        .expect("original native ledger control");
    let mut observer = crate::store::history::native_work_io(&native)?;
    let proof = original_reservation_proof_with_work(
        &client, &owner, &scope, &issuer.verifier(), operation, &original, &mut Some(&mut observer),
    ).await?;
    let custody = observer.into_custody();
    assert_eq!((&proof.1), &original);
    assert_eq!(proof.0.operation_id, operation);
    assert_eq!(provider.io_snapshot().writes, 0);
    drop((proof, custody));
    Ok(())
}

#[tokio::test]
async fn historical_proof_rejects_ambiguous_started_origins_and_foreign_selectors_without_writes() -> Result<()> {
    for duplicate in [false, true] {
        let provider = Arc::new(FaultStream::default());
        let client = provider.clone();
        let operation = OperationId::from_bytes([129; 16]);
        let (owner, issuer, scope, mut candidate, time) = historical_source(&client, operation).await?;
        if duplicate {
            append_source(&client, 4, "another-distinct-start-origin", SchedulerEvent::Started {
                operation_id: operation, fence: candidate.clone(),
            }, time + 3).await?;
        } else {
            candidate.reservation_id.push_str("-foreign");
        }
        provider.reset_io();
        provider.forbid_writes.store(true, Ordering::SeqCst);
        let cancellation = CancellationToken::new();
        let mut work = HistoryWork::new(BUDGET, &cancellation);
        let result = original_reservation_proof_with_work(
            &client, &owner, &scope, &issuer.verifier(), operation, &candidate, &mut Some(&mut work),
        ).await;
        assert!(if duplicate { matches!(result, Err(Error::Conflict(_))) }
            else { matches!(result, Err(Error::NotFound(_))) });
        assert_eq!(provider.io_snapshot().writes, 0);
        assert_eq!(work.receipt().backend_write_operations, 0);
    }
    Ok(())
}

