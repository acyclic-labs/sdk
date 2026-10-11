use super::*;
use super::tests::spec;
use crate::store::history::{HistoryIo as _, HistoryWork};
use acyclic_fs::{CancellationToken, WorkBudget};
use acyclic_stream::{CommitCondition, MemoryStream, StreamPath};
use std::sync::atomic::{AtomicUsize, Ordering};

type FaultStream = crate::test_stream::LostSessionAck<MemoryStream>;

fn budget() -> WorkBudget {
    const LIMIT: u64 = 64 * 1024 * 1024;
    WorkBudget {
        authority_records_read: LIMIT, authority_records_appended: LIMIT,
        authority_bytes_read: LIMIT, authority_bytes_written: LIMIT,
        object_probes: LIMIT, backend_read_operations: LIMIT, backend_write_operations: LIMIT,
        durability_operations: LIMIT, page_reads: LIMIT, page_writes: LIMIT,
        object_bytes_read: LIMIT, object_bytes_written: LIMIT,
        bytes_hashed: LIMIT, bytes_copied: LIMIT, bytes_encoded: LIMIT,
        source_bytes_read: LIMIT, source_path_components: LIMIT, source_entries_visited: LIMIT,
        output_bytes: LIMIT, items_examined: LIMIT, items_returned: LIMIT,
        allocation_operations: LIMIT, peak_allocation_bytes: LIMIT, materializations: LIMIT,
    }
}

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

async fn append_source<P: StreamProvider>(
    client: &StreamClient<P>, revision: u64, key: &str, event: SchedulerEvent, time: u64,
) -> Result<u64> {
    let canonical = crate::contract::canonical_json_bytes(&event)?;
    let bytes = encode(revision, event.operation_id(), key,
        *blake3::hash(&canonical).as_bytes(), canonical, time)?;
    let length = bytes.len() as u64;
    client.stream(COORDINATOR_PATH)?.append_batch(
        vec![OwnedBytes::from(bytes)], Some(revision - 1), None,
    ).await?;
    Ok(length)
}

async fn seed(client: &StreamClient<FaultStream>, ids: &[OperationId]) -> Result<()> {
    let mut coordinator = DistributedCoordinator::open(client, Arc::new(UnmeasuredContent::default())).await?;
    for (index, id) in ids.iter().enumerate() {
        let declaration = spec(*id, 0)?;
        let owner = declaration.owner.authority().clone();
        let issuer = crate::core::AuthorityIssuer::new("test-runtime", [9; 32], owner.clone());
        coordinator.declare_operation(&owner, &declaration.owner_scope.clone(), &issuer.verifier(),
            declaration, IdempotencyKey::new(format!("measured-seed-{index}"))?,
    None,).await?;
    }
    Ok(())
}

#[tokio::test]
async fn coordinator_quota_and_cancellation_stop_before_the_next_real_sdk_call() -> Result<()> {
    for mode in 0..3 {
        let provider = Arc::new(FaultStream::default());
        let client = StreamClient::new(provider.clone());
        let mut coordinator = DistributedCoordinator::open(&client, Arc::new(UnmeasuredContent::default())).await?;
        let bytes = append_source(&client, 1, "measured-quota", SchedulerEvent::Declared {
            spec: Box::new(spec(OperationId::from_bytes([104; 16]), 0)?),
        }, 1).await?;
        provider.reset_io();
        let cancellation = CancellationToken::new();
        if mode == 0 { cancellation.cancel(); }
        let mut allowance = budget();
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
    let client = StreamClient::new(provider.clone());
    let content = Arc::new(UnmeasuredContent::default());
    let mut coordinator = DistributedCoordinator::open(&client, content.clone()).await?;
    append_source(&client, 1, "unmeasured-content", SchedulerEvent::Declared {
        spec: Box::new(spec(OperationId::from_bytes([105; 16]), 0)?),
    }, 1).await?;
    provider.reset_io();
    let cancellation = CancellationToken::new();
    let mut work = HistoryWork::new(budget(), &cancellation);
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

struct MeasuredContentControl {
    inner: Arc<dyn ContentResidencyVerifier>,
    cancel_after_verify: bool,
    ordinary_calls: AtomicUsize,
    measured_calls: AtomicUsize,
}

impl ContentResidencyVerifier for MeasuredContentControl {
    fn verify<'a>(&'a self, reference: &'a FileRef) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.ordinary_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.verify(reference).await
        })
    }

    fn verify_measured<'a>(
        &'a self, reference: &'a FileRef, maximum_bytes: u64,
        budget: WorkBudget, cancellation: &'a CancellationToken,
    ) -> crate::conversation::ContentFuture<'a,
        acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error>>
    {
        Box::pin(async move {
            self.measured_calls.fetch_add(1, Ordering::SeqCst);
            let receipt = self.inner.verify_measured(reference, maximum_bytes, budget, cancellation).await?;
            if self.cancel_after_verify { cancellation.cancel(); }
            Ok(receipt)
        })
    }
}

#[tokio::test]
async fn declared_content_uses_real_finite_provider_receipts_and_cancellation_before_index_io() -> Result<()> {
    use crate::{
        AgentId, Capabilities,
        conversation::{ContentGrant, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
        core::{AggregateKind, Authority, AuthorityIssuer},
        filesystem::{FilesystemContentVerifier, FilesystemHost},
        resources::ProviderRef,
    };
    for cancel_after_verify in [false, true] {
        let fs_provider = ProviderRef::new("measured-coordinator", "filesystem", "2")?;
        let host = Arc::new(FilesystemHost::new(acyclic_fs::Fs::memory(), fs_provider.clone())?);
        let agent = AgentId::from_bytes([108; 16]);
        let volume = VolumeRef::new(fs_provider, "measured-state", VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent))?;
        host.create_volume(&volume).await?;
        let issuer = AuthorityIssuer::new("test-runtime", [9; 32], Authority {
            kind: AggregateKind::Task, id: "owner".into(),
        });
        let scope = issuer.root_for_agent(agent, "content", Capabilities::new([
            volume.capability(VolumeOperation::Read)?, volume.capability(VolumeOperation::Write)?,
        ]));
        let grant = ContentGrant::verify(&issuer.verifier(), &scope, &volume, VolumeOperation::Write)?;
        let state = host.put_content(&volume, &grant, "state/initial.json", b"null",
            "application/json", "initial.json", 4096, &IdempotencyKey::new("measured-state")?).await?;
        let reader: Arc<dyn ContentResidencyVerifier> = Arc::new(FilesystemContentVerifier::new(
            host, issuer.verifier(), scope, 4096,
        )?);
        let content = Arc::new(MeasuredContentControl {
            inner: reader, cancel_after_verify,
            ordinary_calls: AtomicUsize::new(0), measured_calls: AtomicUsize::new(0),
        });
        let provider = Arc::new(FaultStream::default());
        let client = StreamClient::new(provider.clone());
        let mut coordinator = DistributedCoordinator::open(&client, content.clone()).await?;
        let mut declaration = spec(OperationId::from_bytes([109; 16]), 0)?;
        declaration.state = state;
        let source_bytes = append_source(&client, 1, "measured-real-content",
            SchedulerEvent::Declared { spec: Box::new(declaration) }, 1).await?;
        provider.reset_io();
        let cancellation = CancellationToken::new();
        let mut work = HistoryWork::new(budget(), &cancellation);
        work.preflight()?;
        let result = coordinator.refresh_with_work(&mut Some(&mut work)).await;
        let counters = work.receipt();
        assert_eq!(content.ordinary_calls.load(Ordering::SeqCst), 0);
        assert_eq!(content.measured_calls.load(Ordering::SeqCst), 1);
        assert!(counters.object_bytes_read >= 4);
        if cancel_after_verify {
            assert!(matches!(result, Err(Error::Invalid(_))));
            assert_eq!(provider.io_snapshot().reads, 1);
            assert_eq!(provider.io_snapshot().writes, 0);
            assert_eq!(coordinator.revision, 0);
        } else {
            result?;
            assert_eq!(coordinator.revision, 1);
            assert_eq!(provider.io_snapshot().writes, 1);
        }
    }
    Ok(())
}

async fn admitted_fixture(
    client: &StreamClient<FaultStream>,
    operation: OperationId,
) -> Result<(DistributedCoordinator<FaultStream>, WorkLease)> {
    seed(client, &[operation]).await?;
    let mut coordinator = DistributedCoordinator::open(client, Arc::new(UnmeasuredContent::default())).await?;
    let lease = coordinator.pull(&Worker {
        id: "measured-release-worker".into(),
        available: ResourceSnapshot::default(),
        labels: BTreeMap::new(),
    }).await?.ok_or_else(|| Error::NotFound("original admitted lease".into()))?;
    Ok((coordinator, lease))
}

#[tokio::test]
async fn measured_lease_release_quota_and_cancellation_reject_before_publication() -> Result<()> {
    for mode in 0..4 {
        let provider = Arc::new(FaultStream::default());
        let client = StreamClient::new(provider.clone());
        let operation = OperationId::from_bytes([117; 16]);
        let (mut coordinator, lease) = admitted_fixture(&client, operation).await?;
        let revision = coordinator.revision;
        let cancellation = CancellationToken::new();
        if mode == 0 { cancellation.cancel(); }
        let mut allowance = budget();
        if mode == 1 { allowance.backend_write_operations = 0; }
        if mode == 2 { allowance.bytes_encoded = 0; }
        if mode == 3 { allowance.allocation_operations = 0; }
        let mut work = HistoryWork::new(allowance, &cancellation);
        provider.reset_io();
        let result = coordinator.release_lease_with_work(&lease,
            IdempotencyKey::new("release-before-io")?, &mut Some(&mut work)).await;
        let failure = work.finish(result).expect_err("bounded source admission must stop publication");
        assert!(matches!(failure.error, Error::Invalid(_)));
        assert_eq!(failure.work.backend_write_operations, 0);
        assert_eq!(provider.io_snapshot().writes, 0);
        assert_eq!(coordinator.revision, revision);
        assert_eq!(client.stream(COORDINATOR_PATH)?.tail().await?, revision);
        assert_eq!(coordinator.scheduler().operation(operation)
            .and_then(|operation| operation.reservation.as_ref()), Some(&lease.reservation));
    }
    Ok(())
}

#[tokio::test]
async fn measured_quiescent_release_requires_witness_and_the_same_owner_verified_revision() -> Result<()> {
    let provider = Arc::new(FaultStream::default());
    let client = StreamClient::new(provider.clone());
    let operation = OperationId::from_bytes([120; 16]);
    let execution = OperationId::from_bytes([121; 16]);
    let (mut coordinator, lease) = admitted_fixture(&client, operation).await?;
    let cancellation = CancellationToken::new();
    let mut work = HistoryWork::new(budget(), &cancellation);
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

#[tokio::test]
async fn measured_declaration_preserves_original_scope_key_and_never_uses_unmeasured_content() -> Result<()> {
    for mode in 0..4 {
        let provider = Arc::new(FaultStream::default());
        let client = StreamClient::new(provider.clone());
        let content = Arc::new(UnmeasuredContent::default());
        let mut coordinator = DistributedCoordinator::open(&client, content.clone()).await?;
        let declaration = spec(OperationId::from_bytes([122; 16]), 0)?;
        let owner = declaration.owner.authority().clone();
        let issuer = crate::core::AuthorityIssuer::new("test-runtime", [9;32], owner.clone());
        let scope = issuer.root("measured-declaration", crate::Capabilities::new(
            if mode == 1 {vec!["operation:observe"]} else {vec!["operation:declare"]},
        ));
        let wrong = crate::core::AuthorityIssuer::new("test-runtime", [8;32], owner.clone());
        let cancellation = CancellationToken::new();
        let mut allowance = budget();
        if mode == 3 {allowance.bytes_hashed = 0;}
        let mut work = HistoryWork::new(allowance, &cancellation);
        provider.reset_io();
        let result = coordinator.declare_operation_with_work(&owner, &scope,
            &if mode == 2 {wrong.verifier()} else {issuer.verifier()},
            declaration, IdempotencyKey::new("measured-declaration")?,
            None, &mut Some(&mut work),
        ).await;
        if mode == 0 {
            assert!(matches!(result, Err(Error::Unsupported(_))));
            assert_eq!(provider.io_snapshot().reads, 2);
        } else {
            assert!(matches!(result, Err(Error::Unauthorized(_)) | Err(Error::Invalid(_))));
            assert_eq!(provider.io_snapshot().reads, 0);
        }
        assert_eq!(content.0.load(Ordering::SeqCst), 0);
        assert_eq!(provider.io_snapshot().writes, 0);
        assert_eq!(coordinator.revision, 0);
    }
    Ok(())
}

#[tokio::test]
async fn prepared_commit_cancellation_uses_the_original_ledger_before_sdk_publication() -> Result<()> {
    let provider = Arc::new(FaultStream::default());
    let client = StreamClient::new(provider.clone());
    let cancellation = CancellationToken::new();
    let mut work = HistoryWork::new(budget(), &cancellation);
    let path = StreamPath::new("harness/v2/execution/prepared-cancel")?;
    let result = {
        let mut observer = Some(&mut work as &mut dyn crate::store::history::HistoryIo);
        let mut prepared = PreparedCommitRequest::new_with_digest([23; 32], &mut observer)?;
        prepared.push_condition(CommitCondition::Absent { path: path.clone() })?;
        prepared.append(path, OwnedBytes::from_static(b"source-owned"))?;
        assert_eq!(prepared.custody.retained(), prepared.vectors + 32);
        cancellation.cancel();
        commit_prepared(&client, prepared, OperationId::from_bytes([127; 16]), None, None, None).await
    };
    assert!(matches!(result, Err(Error::Invalid(_))));
    assert_eq!(work.receipt().backend_write_operations, 0);
    assert_eq!(provider.io_snapshot().writes, 0);
    assert_eq!(provider.io_snapshot().inspections, 0);
    Ok(())
}

async fn historical_source(
    client: &StreamClient<FaultStream>, operation: OperationId,
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
    let client = StreamClient::new(provider.clone());
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
    let native = acyclic_fs::NativeMaterializationWork::new(budget(), &cancellation)
        .expect("original native ledger control");
    let mut observer = crate::store::history::native_work_io(&native)?;
    let proof = original_reservation_proof_with_work(
        &client, &owner, &scope, &issuer.verifier(), operation, &original, &mut Some(&mut observer),
    ).await?;
    let custody = observer.into_custody();
    assert_eq!(proof.original_fence(), &original);
    assert_eq!(proof.declaration().operation_id, operation);
    assert_eq!(provider.io_snapshot().writes, 0);
    drop((proof, custody));
    Ok(())
}

#[tokio::test]
async fn historical_proof_rejects_ambiguous_started_origins_and_foreign_selectors_without_writes() -> Result<()> {
    for duplicate in [false, true] {
        let provider = Arc::new(FaultStream::default());
        let client = StreamClient::new(provider.clone());
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
        let mut work = HistoryWork::new(budget(), &cancellation);
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

#[tokio::test]
async fn historical_proof_authenticates_before_io_and_preserves_raw_failure_and_cancellation_prefixes() -> Result<()> {
    for mode in 0..7 {
        let provider = Arc::new(FaultStream::default());
        let client = StreamClient::new(provider.clone());
        let operation = OperationId::from_bytes([130; 16]);
        let (owner, issuer, valid, candidate, _) = historical_source(&client, operation).await?;
        let scope = if mode == 0 {
            issuer.root("not-an-observation-grant", crate::Capabilities::new(["operation:declare"]))
        } else { valid };
        provider.reset_io();
        provider.forbid_writes.store(true, Ordering::SeqCst);
        let cancellation = CancellationToken::new();
        if mode == 1 { cancellation.cancel(); }
        if mode == 2 { *provider.history_cancel_after_tail.lock().expect("control") = Some(cancellation.clone()); }
        if mode >= 4 { provider.history_read_fault.store(match mode { 4 => 4, 5 => 5, _ => 6 }, Ordering::SeqCst); }
        let mut allowance = budget();
        if mode == 3 { allowance.backend_read_operations = 0; }
        let mut work = HistoryWork::new(allowance, &cancellation);
        let result = original_reservation_proof_with_work(
            &client, &owner, &scope, &issuer.verifier(), operation, &candidate, &mut Some(&mut work),
        ).await;
        assert!(result.is_err());
        let counters = work.receipt();
        if mode <= 1 || mode == 3 {
            assert_eq!(provider.io_snapshot().reads, 0);
        } else if mode >= 4 {
            assert_eq!(provider.io_snapshot().reads, 1);
            if mode == 4 {  }
            if mode == 5 {  }
        } else {
            assert_eq!(provider.io_snapshot().reads, 2);
            assert_eq!(provider.io_snapshot().tails, 1);
        }
        assert_eq!(provider.io_snapshot().writes, 0);
        assert_eq!(counters.backend_write_operations, 0);
    }
    Ok(())
}

#[tokio::test]
async fn historical_proof_rejects_an_original_declaration_signed_by_another_key_before_tail_io() -> Result<()> {
    let provider = Arc::new(FaultStream::default());
    let client = StreamClient::new(provider.clone());
    let operation = OperationId::from_bytes([132; 16]);
    let mut declaration = spec(operation, 0)?;
    let owner = declaration.owner.authority().clone();
    let issuer = crate::core::AuthorityIssuer::new("test-runtime", [9; 32], owner.clone());
    let foreign = crate::core::AuthorityIssuer::new("test-runtime", [8; 32], owner.clone());
    declaration.owner_scope = foreign.root("foreign-original-declaration",
        crate::Capabilities::new(["operation:declare"]));
    let key = format!("task-admission:{operation}");
    let coordinator = DistributedCoordinator::open(&client, Arc::new(UnmeasuredContent::default())).await?;
    append_source(&client, 1, &key, SchedulerEvent::Declared { spec: Box::new(declaration) }, 1).await?;
    let record = client.stream(COORDINATOR_PATH)?.read(0, 1).await?.try_next().await?
        .expect("actual canonical source record");
    let (_, _, _, event_digest, _, _) = decode(&record.value)?;
    let location = IntentLocation { key, revision: 1, event_digest, commit_id: *record.commit_id.as_bytes() };
    coordinator.ensure_intent_location_with_work(&location, &mut None).await?;
    provider.reset_io();
    provider.forbid_writes.store(true, Ordering::SeqCst);
    let scope = issuer.root("observe", crate::Capabilities::new(["operation:observe"]));
    let candidate = LeaseFence { reservation_id: "not-used".into(), placement: "not-used".into() };
    let cancellation = CancellationToken::new();
    let mut work = HistoryWork::new(budget(), &cancellation);
    let result = original_reservation_proof_with_work(
        &client, &owner, &scope, &issuer.verifier(), operation, &candidate, &mut Some(&mut work),
    ).await;
    assert!(matches!(result, Err(Error::Unauthorized(_))));
    assert_eq!(provider.io_snapshot().reads, 2);
    assert_eq!(provider.io_snapshot().tails, 0);
    assert_eq!(provider.io_snapshot().writes, 0);
    Ok(())
}

#[tokio::test]
async fn historical_proof_never_uses_an_unobserved_cold_prefix_as_a_finite_facade() -> Result<()> {
    let provider = Arc::new(FaultStream::default());
    let client = StreamClient::new(provider.clone());
    let operation = OperationId::from_bytes([133; 16]);
    let declaration = spec(operation, 0)?;
    let owner = declaration.owner.authority();
    let issuer = crate::core::AuthorityIssuer::new("test-runtime", [9; 32], owner.clone());
    let scope = issuer.root("observe", crate::Capabilities::new(["operation:observe"]));
    let candidate = LeaseFence { reservation_id: "selector".into(), placement: "placement".into() };
    let result = original_reservation_proof_with_work(
        &client, owner, &scope, &issuer.verifier(), operation, &candidate, &mut None,
    ).await;
    assert!(matches!(result, Err(Error::Unsupported(_))));
    assert_eq!(provider.io_snapshot().reads, 0);
    assert_eq!(provider.io_snapshot().tails, 0);
    assert_eq!(provider.io_snapshot().writes, 0);
    Ok(())
}
