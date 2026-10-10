use super::*;
use super::tests::spec;
use crate::store::history::HistoryWork;
use acyclic_fs::{CancellationToken, WorkBudget};
use acyclic_stream::{CommitCondition, CommitMutation, CommitOutcome, CommitRequest, MemoryStream, StreamPath};
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
        *blake3::hash(&canonical).as_bytes(), canonical, time);
    let length = bytes.len() as u64;
    client.stream(COORDINATOR_PATH)?.append_batch(
        vec![Bytes::from(bytes)], Some(revision - 1), None,
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
            declaration, IdempotencyKey::new(format!("measured-seed-{index}"))?).await?;
    }
    Ok(())
}

#[tokio::test]
async fn observed_refresh_uses_the_original_reducer_and_repairs_only_original_indexes() -> Result<()> {
    let provider = Arc::new(FaultStream::default());
    let client = StreamClient::new(provider.clone());
    let ids = [OperationId::from_bytes([101; 16]), OperationId::from_bytes([102; 16])];
    seed(&client, &ids).await?;
    let content: Arc<dyn ContentResidencyVerifier> = Arc::new(UnmeasuredContent::default());
    let mut ordinary = DistributedCoordinator::open(&client, content.clone()).await?;
    let mut observed = DistributedCoordinator::open(&client, content).await?;
    let mut source_bytes = 0;
    for (index, id) in ids.iter().enumerate() {
        source_bytes += append_source(&client, 3 + index as u64, &format!("measured-cancel-{index}"),
            SchedulerEvent::CancellationRequested { operation_id: *id, recursive: false },
            observed.last_committed_at_ms + 1 + index as u64).await?;
    }
    provider.reset_io();
    let cancellation = CancellationToken::new();
    let mut work = HistoryWork::new(budget(), &cancellation);
    work.preflight()?;
    let result = observed.refresh_with_work(&mut Some(&mut work)).await;
    let receipt = work.finish(result).map_err(|failure| failure.error)?;
    let io = provider.io_snapshot();
    assert_eq!(receipt.work.backend_read_operations, io.reads + io.inspections);
    assert_eq!(receipt.work.backend_write_operations, io.writes);
    assert_eq!(receipt.work.authority_records_read, 2);
    assert_eq!(receipt.work.authority_bytes_read, source_bytes);
    assert_eq!(receipt.work.source_bytes_read, source_bytes);
    assert_eq!(receipt.work.authority_records_appended, 2);
    assert_eq!(receipt.work.authority_bytes_written, io.write_bytes);
    assert_eq!(receipt.work.page_reads, 0);
    assert_eq!(receipt.work.durability_operations, 0);
    assert!(receipt.work.peak_allocation_bytes >= source_bytes);
    ordinary.refresh().await?;
    assert_eq!(ordinary.revision, observed.revision);
    assert_eq!(ordinary.scheduler(), observed.scheduler());
    assert_eq!(ordinary.intents, observed.intents);
    Ok(())
}

#[tokio::test]
async fn coordinator_faults_keep_actual_raw_prefixes_before_decode_or_projection() -> Result<()> {
    for fault in [1, 2, 4, 5, 6] {
        let provider = Arc::new(FaultStream::default());
        let client = StreamClient::new(provider.clone());
        let mut coordinator = DistributedCoordinator::open(&client, Arc::new(UnmeasuredContent::default())).await?;
        let bytes = append_source(&client, 1, "measured-fault", SchedulerEvent::Declared {
            spec: Box::new(spec(OperationId::from_bytes([103; 16]), 0)?),
        }, 1).await?;
        provider.reset_io();
        provider.history_read_fault.store(fault, Ordering::SeqCst);
        let cancellation = CancellationToken::new();
        let mut work = HistoryWork::new(budget(), &cancellation);
        work.preflight()?;
        let result = coordinator.refresh_with_work(&mut Some(&mut work)).await;
        let failure = work.finish(result).expect_err("malformed/partial SDK history must fail");
        let expected = match fault {
            2 => bytes * 2,
            4 => b"corrupt".len() as u64,
            5 => (acyclic_stream::MAX_RECORD_BYTES + 1) as u64,
            _ => bytes,
        };
        assert_eq!(failure.work.authority_bytes_read, expected);
        assert_eq!(failure.work.source_bytes_read, expected);
        assert_eq!(failure.work.authority_records_read, if fault == 2 { 2 } else { 1 });
        assert_eq!(failure.work.backend_read_operations, 1);
        assert!(failure.work.peak_allocation_bytes >= expected);
        assert_eq!(provider.io_snapshot().writes, 0);
        assert_eq!(coordinator.revision, 0);
        assert!(coordinator.scheduler().operation(OperationId::from_bytes([103; 16])).is_none());
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

fn commit_request(tail: u64, key: &str) -> Result<CommitRequest> {
    let path = StreamPath::new("harness/v2/execution/measured-commit")?;
    Ok(CommitRequest {
        conditions: vec![if tail == 0 { CommitCondition::Absent { path: path.clone() } }
            else { CommitCondition::Tail { path: path.clone(), expected: tail } }],
        mutations: vec![CommitMutation::Append { path,
            records: vec![Bytes::from_static(b"first"), Bytes::from_static(b"second")] }],
        idempotency_key: stream_key(key)?,
    })
}

#[tokio::test]
async fn commit_receipts_distinguish_direct_ack_recovered_hidden_and_quota_blocked_observation() -> Result<()> {
    let operation = OperationId::from_bytes([106; 16]);
    for mode in 0..5 {
        let provider = Arc::new(FaultStream::default());
        let client = StreamClient::new(provider.clone());
        provider.commit_lose_ack.store(mode != 0, Ordering::SeqCst);
        provider.hide_receipt.store(mode == 2, Ordering::SeqCst);
        let mut allowance = budget();
        if mode == 3 { allowance.backend_read_operations = 0; }
        if mode == 4 { allowance.source_bytes_read = 1; }
        let cancellation = CancellationToken::new();
        let mut work = HistoryWork::new(allowance, &cancellation);
        work.preflight()?;
        let result = commit_keyed_with_work(&client, commit_request(0, "measured-commit")?,
            operation, &mut Some(&mut work)).await;
        let counters = work.receipt();
        let io = provider.io_snapshot();
        assert_eq!(counters.backend_write_operations, 1);
        assert_eq!(counters.backend_read_operations, u64::from(mode != 0 && mode != 3));
        assert_eq!(io.inspections, counters.backend_read_operations);
        assert_eq!(counters.authority_records_appended, if mode == 0 { 2 } else { 0 });
        assert_eq!(counters.authority_bytes_written, if mode == 0 { 11 } else { 0 });
        assert_eq!(counters.authority_records_read, if matches!(mode, 0 | 1 | 4) { 2 } else { 0 });
        assert_eq!(counters.authority_bytes_read, if matches!(mode, 0 | 1 | 4) { 11 } else { 0 });
        assert_eq!(counters.source_bytes_read, counters.authority_bytes_read);
        if matches!(mode, 0 | 1) {
            assert!(matches!(result?, CommitOutcome::Committed(_)));
        } else if mode == 2 {
            assert!(matches!(result, Err(Error::Indeterminate(id)) if id == operation));
        } else {
            assert!(matches!(result, Err(Error::Invalid(_))));
        }
        assert_eq!(client.bounds("harness/v2/execution/measured-commit").await?.tail, 2);
    }
    Ok(())
}

#[tokio::test]
async fn repeated_commit_calls_share_one_cumulative_allowance_and_no_conflict_acknowledgment() -> Result<()> {
    let provider = Arc::new(FaultStream::default());
    let client = StreamClient::new(provider.clone());
    let cancellation = CancellationToken::new();
    let mut allowance = budget();
    allowance.backend_write_operations = 1;
    let mut work = HistoryWork::new(allowance, &cancellation);
    work.preflight()?;
    let operation = OperationId::from_bytes([107; 16]);
    let first = commit_keyed_with_work(&client, commit_request(0, "first-measured-call")?,
        operation, &mut Some(&mut work)).await?;
    assert!(matches!(first, CommitOutcome::Committed(_)));
    let second = commit_keyed_with_work(&client, commit_request(2, "second-measured-call")?,
        operation, &mut Some(&mut work)).await;
    assert!(matches!(second, Err(Error::Invalid(_))));
    assert_eq!(work.receipt().backend_write_operations, 1);
    assert_eq!(provider.io_snapshot().writes, 1);
    assert_eq!(work.receipt().authority_records_appended, 2);
    let mut conflict_work = HistoryWork::new(budget(), &cancellation);
    let conflict = commit_keyed_with_work(&client, commit_request(0, "conflict-measured-call")?,
        operation, &mut Some(&mut conflict_work)).await?;
    assert!(matches!(conflict, CommitOutcome::Conflict(_)));
    assert_eq!(conflict_work.receipt().authority_records_appended, 0);
    assert_eq!(conflict_work.receipt().authority_bytes_read, 0);
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
        assert!(counters.authority_bytes_read >= source_bytes);
        if cancel_after_verify {
            assert!(matches!(result, Err(Error::Invalid(_))));
            assert_eq!(provider.io_snapshot().reads, 1);
            assert_eq!(provider.io_snapshot().writes, 0);
            assert_eq!(coordinator.revision, 0);
        } else {
            result?;
            assert_eq!(coordinator.revision, 1);
            assert_eq!(counters.authority_records_appended, 1);
            assert_eq!(provider.io_snapshot().writes, 1);
        }
    }
    Ok(())
}

#[tokio::test]
async fn observed_journal_fence_keeps_original_owner_lease_and_task_cancellation_checks() -> Result<()> {
    let provider = Arc::new(FaultStream::default());
    let client = StreamClient::new(provider.clone());
    let operation = OperationId::from_bytes([110; 16]);
    seed(&client, &[operation]).await?;
    let content: Arc<dyn ContentResidencyVerifier> = Arc::new(UnmeasuredContent::default());
    let mut owner = DistributedCoordinator::open(&client, content.clone()).await?;
    let lease = owner.pull(&Worker {
        id: "measured-owner".into(), available: ResourceSnapshot::default(), labels: BTreeMap::new(),
    }).await?.ok_or_else(|| Error::NotFound("measured lease".into()))?;
    let fence = LeaseFence::from(&lease.reservation);
    owner.apply(operation, IdempotencyKey::new("measured-start")?, SchedulerEvent::Started {
        operation_id: operation, fence: fence.clone(),
    }).await?;
    let mut ordinary = DistributedCoordinator::open(&client, content.clone()).await?;
    let mut observed = DistributedCoordinator::open(&client, content).await?;
    owner.apply(operation, IdempotencyKey::new("measured-task-cancel")?,
        SchedulerEvent::CancellationRequested { operation_id: operation, recursive: false }).await?;
    let authority = lease.operation.owner.authority();
    let issuer = crate::core::AuthorityIssuer::new("test-runtime", [9; 32], authority.clone());
    let scope = issuer.root("observe-journal", crate::Capabilities::new(["operation:observe"]));
    let expected = ordinary.journal_condition(authority, &scope, &issuer.verifier(), operation,
        &fence, JournalWrite::Fresh).await.expect_err("cancelled task cannot publish fresh work");
    provider.reset_io();
    let cancellation = CancellationToken::new();
    let mut work = HistoryWork::new(budget(), &cancellation);
    work.preflight()?;
    let result = observed.journal_condition_with_work(authority, &scope, &issuer.verifier(), operation,
        &fence, JournalWrite::Fresh, &mut Some(&mut work)).await;
    let failure = work.finish(result).expect_err("observation cannot weaken the original fence");
    assert_eq!(failure.error.to_string(), expected.to_string());
    assert_eq!(observed.revision, ordinary.revision);
    assert_eq!(observed.scheduler(), ordinary.scheduler());
    assert_eq!(failure.work.backend_read_operations, provider.io_snapshot().reads);
    assert_eq!(provider.io_snapshot().writes, 0);
    Ok(())
}

#[tokio::test]
async fn index_canonical_codec_admits_encoded_copy_and_owned_growth_before_publication() -> Result<()> {
    for mode in 0..3 {
        let provider = Arc::new(FaultStream::default());
        let client = StreamClient::new(provider.clone());
        let coordinator = DistributedCoordinator::open(&client, Arc::new(UnmeasuredContent::default())).await?;
        append_source(&client, 1, "codec-source", SchedulerEvent::Declared {
            spec: Box::new(spec(OperationId::from_bytes([111; 16]), 0)?),
        }, 1).await?;
        let records = client.stream(COORDINATOR_PATH)?.read(0, 1).await?
            .try_collect::<Vec<_>>().await?;
        let record = records.first().ok_or_else(|| Error::NotFound("codec source".into()))?;
        let (revision, _, key, event_digest, _, _) = decode(&record.value)?;
        let location = IntentLocation {
            key, revision, event_digest, commit_id: *record.commit_id.as_bytes(),
        };
        let mut allowance = budget();
        match mode {
            0 => allowance.bytes_encoded = 0,
            // The original key validation copy is admitted. No capacity or
            // canonical-copy allowance is silently reset for the serializer.
            1 => allowance.bytes_copied = location.key.len() as u64,
            2 => allowance.peak_allocation_bytes = 2 * location.key.capacity() as u64,
            _ => unreachable!(),
        }
        provider.reset_io();
        let cancellation = CancellationToken::new();
        let mut work = HistoryWork::new(allowance, &cancellation);
        work.preflight()?;
        let result = coordinator.ensure_intent_location_with_work(location.clone(),
            &mut Some(&mut work)).await;
        let failure = work.finish(result).expect_err("canonical storage must be preadmitted");
        assert!(matches!(failure.error, Error::Invalid(_)));
        assert_eq!(failure.work.backend_read_operations, 1);
        assert_eq!(failure.work.backend_write_operations, 0);
        assert_eq!(failure.work.authority_records_appended, 0);
        assert_eq!(failure.work.authority_bytes_written, 0);
        assert!(failure.work.bytes_encoded <= allowance.bytes_encoded);
        assert!(failure.work.bytes_copied <= allowance.bytes_copied);
        assert!(failure.work.peak_allocation_bytes <= allowance.peak_allocation_bytes);
        assert_eq!(provider.io_snapshot().reads, 1);
        assert_eq!(provider.io_snapshot().writes, 0);
        assert!(client.bounds(&intent_location_path(&location.key)).await.is_err());
        // The exact original event/location is still publishable through the
        // same engine; quota rejection leaves no partial immutable index.
        coordinator.ensure_intent_location_with_work(location.clone(), &mut None).await?;
        assert_eq!(client.bounds(&intent_location_path(&location.key)).await?.tail, 1);
    }
    Ok(())
}

#[tokio::test]
async fn direct_idempotent_commit_receipts_acknowledge_without_claiming_new_durable_records() -> Result<()> {
    let provider = Arc::new(FaultStream::default());
    let client = StreamClient::new(provider.clone());
    let cancellation = CancellationToken::new();
    let mut work = HistoryWork::new(budget(), &cancellation);
    work.preflight()?;
    let operation = OperationId::from_bytes([112; 16]);
    let first = commit_keyed_with_work(&client, commit_request(0, "direct-repeated-ack")?,
        operation, &mut Some(&mut work)).await?;
    let second = commit_keyed_with_work(&client, commit_request(0, "direct-repeated-ack")?,
        operation, &mut Some(&mut work)).await?;
    assert_eq!(first, second);
    // These are SDK-confirmed acknowledgment records, not physical new appends.
    assert_eq!(work.receipt().authority_records_appended, 4);
    assert_eq!(work.receipt().authority_bytes_written, 22);
    assert_eq!(work.receipt().authority_records_read, 4);
    assert_eq!(work.receipt().authority_bytes_read, 22);
    assert_eq!(work.receipt().backend_write_operations, 2);
    assert_eq!(work.receipt().backend_read_operations, 0);
    assert_eq!(provider.io_snapshot().inspections, 0);
    assert_eq!(client.bounds("harness/v2/execution/measured-commit").await?.tail, 2);
    Ok(())
}
