use super::*;
use super::tests::spec;
use crate::store::history::{HistoryIo as _, HistoryWork};
use acyclic_fs::{CancellationToken, WorkBudget};
use acyclic_stream::{CommitCondition, CommitMutation, CommitOutcome, MemoryStream, StreamPath};
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

async fn commit_fixture_with_work(
    client: &StreamClient<FaultStream>, tail: u64, key: &str, operation: OperationId,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<(CommitOutcome, u64)> {
    let path = StreamPath::new("harness/v2/execution/measured-commit")?;
    if let Some(work) = work.as_deref_mut() {
        work.hashed_bytes((b"acyclic-harness-coordinator-v3".len() + key.len()) as u64)?;
    }
    let mut hash = blake3::Hasher::new();
    hash.update(b"acyclic-harness-coordinator-v3");
    hash.update(key.as_bytes());
    let mut prepared = PreparedCommitRequest::new_with_digest(*hash.finalize().as_bytes(), work)?;
    prepared.push_condition(if tail == 0 { CommitCondition::Absent { path: path.clone() } }
        else { CommitCondition::Tail { path: path.clone(), expected: tail } })?;
    prepared.append(path, OwnedBytes::from_static(b"first"))?;
    let CommitMutation::Append { records, .. } = &mut prepared.request.mutations[0] else {
        unreachable!("the exact original append fixture");
    };
    reserve_prepared_vector(records, 2, &mut prepared.custody, &mut prepared.vectors)?;
    records.push(OwnedBytes::from_static(b"second"));
    commit_prepared_with_work(client, prepared, operation).await
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
        let result = commit_fixture_with_work(&client, 0, "measured-commit",
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
            let (outcome, retained) = result?;
            assert!(matches!(&outcome, CommitOutcome::Committed(_)));
            drop(outcome);
            work.release_owned(retained)?;
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
    let (first, retained) = commit_fixture_with_work(&client, 0, "first-measured-call",
        operation, &mut Some(&mut work)).await?;
    assert!(matches!(first, CommitOutcome::Committed(_)));
    drop(first);
    work.release_owned(retained)?;
    let second = commit_fixture_with_work(&client, 2, "second-measured-call",
        operation, &mut Some(&mut work)).await;
    assert!(matches!(second, Err(Error::Invalid(_))));
    assert_eq!(work.receipt().backend_write_operations, 1);
    assert_eq!(provider.io_snapshot().writes, 1);
    assert_eq!(work.receipt().authority_records_appended, 2);
    let mut conflict_work = HistoryWork::new(budget(), &cancellation);
    let (conflict, retained) = commit_fixture_with_work(&client, 0, "conflict-measured-call",
        operation, &mut Some(&mut conflict_work)).await?;
    assert!(matches!(conflict, CommitOutcome::Conflict(_)));
    drop(conflict);
    conflict_work.release_owned(retained)?;
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
            1 => allowance.bytes_copied = location.key.len() as u64,
            2 => allowance.peak_allocation_bytes = 2 * location.key.capacity() as u64,
            _ => unreachable!(),
        }
        provider.reset_io();
        let cancellation = CancellationToken::new();
        let mut work = HistoryWork::new(allowance, &cancellation);
        work.preflight()?;
        let result = coordinator.ensure_intent_location_with_work(&location,
            &mut Some(&mut work)).await;
        let failure = work.finish(result).expect_err("canonical storage must be preadmitted");
        assert!(matches!(failure.error, Error::Invalid(_)));
        assert_eq!(failure.work.backend_read_operations, u64::from(mode == 0));
        assert_eq!(failure.work.backend_write_operations, 0);
        assert_eq!(failure.work.authority_records_appended, 0);
        assert_eq!(failure.work.authority_bytes_written, 0);
        assert!(failure.work.bytes_encoded <= allowance.bytes_encoded);
        assert!(failure.work.bytes_copied <= allowance.bytes_copied);
        assert!(failure.work.peak_allocation_bytes <= allowance.peak_allocation_bytes);
        assert_eq!(provider.io_snapshot().reads, u64::from(mode == 0));
        assert_eq!(provider.io_snapshot().writes, 0);
        assert!(client.bounds(&intent_location_path(&location.key)).await.is_err());
        coordinator.ensure_intent_location_with_work(&location, &mut None).await?;
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
    let first = commit_fixture_with_work(&client, 0, "direct-repeated-ack",
        operation, &mut Some(&mut work)).await?;
    let second = commit_fixture_with_work(&client, 0, "direct-repeated-ack",
        operation, &mut Some(&mut work)).await?;
    assert_eq!(first, second);
    let (first, first_retained) = first;
    let (second, second_retained) = second;
    drop(first);
    drop(second);
    work.release_owned(first_retained + second_retained)?;
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


#[tokio::test]
async fn borrowed_observation_preserves_current_and_original_signed_grant_denials() -> Result<()> {
    use crate::core::{AggregateKind, Authority, AuthorityIssuer};
    for mode in 0..4 {
        let provider = Arc::new(FaultStream::default());
        let client = StreamClient::new(provider.clone());
        let operation = OperationId::from_bytes([114; 16]);
        let mut coordinator = DistributedCoordinator::open(&client, Arc::new(UnmeasuredContent::default())).await?;
        let mut declaration = spec(operation, 0)?;
        let owner = declaration.owner.authority().clone();
        let issuer = AuthorityIssuer::new("test-runtime", [9; 32], owner.clone());
        if mode == 3 {
            declaration.owner_scope = issuer.root("original-without-declare",
                crate::Capabilities::new(["operation:observe"]));
        }
        append_source(&client, 1, "borrow-original-grant",
            SchedulerEvent::Declared { spec: Box::new(declaration) }, 1).await?;
        coordinator.refresh().await?;
        let caller_owner = if mode == 2 {
            Authority { kind: AggregateKind::Task, id: "foreign-owner".into() }
        } else { owner };
        let caller_issuer = AuthorityIssuer::new("test-runtime", [9; 32], caller_owner.clone());
        let scope = match mode {
            0 => caller_issuer.root("missing-observe", crate::Capabilities::new(["operation:declare"])),
            1 => AuthorityIssuer::new("test-runtime", [8; 32], caller_owner.clone())
                .root("foreign-key", crate::Capabilities::new(["operation:observe"])),
            _ => caller_issuer.root("valid-observe", crate::Capabilities::new(["operation:observe"])),
        };
        let expected = coordinator.observe_operation(&caller_owner, &scope, &caller_issuer.verifier(),
            operation).expect_err("original authorization must reject");
        provider.reset_io();
        let cancellation = CancellationToken::new();
        let mut work = HistoryWork::new(budget(), &cancellation);
        work.preflight()?;
        let actual = coordinator.observe_operation_ref_with_work(&caller_owner, &scope,
            &caller_issuer.verifier(), operation, &mut Some(&mut work))
            .expect_err("borrowed authorization must reject identically");
        assert_eq!(actual.to_string(), expected.to_string());
        assert_eq!(work.receipt().bytes_encoded, 0);
        assert_eq!(work.receipt().bytes_copied, 0);
        assert_eq!(provider.io_snapshot().reads, 0);
        assert_eq!(provider.io_snapshot().writes, 0);
    }
    Ok(())
}

#[tokio::test]
async fn borrowed_owner_proof_hashing_respects_shared_quota_and_cancellation() -> Result<()> {
    let provider = Arc::new(FaultStream::default());
    let client = StreamClient::new(provider.clone());
    let operation = OperationId::from_bytes([115; 16]);
    seed(&client, &[operation]).await?;
    let coordinator = DistributedCoordinator::open(&client, Arc::new(UnmeasuredContent::default())).await?;
    let declaration = spec(operation, 0)?;
    let owner = declaration.owner.authority();
    let issuer = crate::core::AuthorityIssuer::new("test-runtime", [9; 32], owner.clone());
    let scope = issuer.root("bounded-borrow", crate::Capabilities::new(["operation:observe"]));
    for cancel in [false, true] {
        let cancellation = CancellationToken::new();
        if cancel { cancellation.cancel(); }
        let mut allowance = budget();
        if !cancel { allowance.bytes_hashed = 0; }
        let mut work = HistoryWork::new(allowance, &cancellation);
        provider.reset_io();
        let result = coordinator.observe_operation_ref_with_work(owner, &scope, &issuer.verifier(),
            operation, &mut Some(&mut work));
        assert!(matches!(result, Err(Error::Invalid(_))));
        assert_eq!(work.receipt().bytes_hashed, 0);
        assert_eq!(work.receipt().bytes_encoded, 0);
        assert_eq!(work.receipt().bytes_copied, 0);
        assert_eq!(provider.io_snapshot().reads, 0);
        assert_eq!(provider.io_snapshot().writes, 0);
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
async fn measured_lease_release_keeps_original_append_recovery_identity_and_acknowledgments() -> Result<()> {
    for mode in 0..3 {
        let provider = Arc::new(FaultStream::default());
        let client = StreamClient::new(provider.clone());
        let operation = OperationId::from_bytes([116; 16]);
        let (mut coordinator, lease) = admitted_fixture(&client, operation).await?;
        let mut ordinary = DistributedCoordinator::open(&client, Arc::new(UnmeasuredContent::default())).await?;
        provider.lose_ack.store(mode != 0, Ordering::SeqCst);
        provider.hide_receipt.store(mode == 2, Ordering::SeqCst);
        provider.reset_io();
        let cancellation = CancellationToken::new();
        let mut work = HistoryWork::new(budget(), &cancellation);
        work.preflight()?;
        let result = coordinator.release_lease_with_work(&lease,
            IdempotencyKey::new("original-measured-release")?, &mut Some(&mut work)).await;
        let io = provider.io_snapshot();
        let counters = work.receipt();
        assert_eq!(counters.backend_read_operations, io.reads + io.inspections);
        assert_eq!(counters.backend_write_operations, io.writes);
        assert_eq!(counters.page_writes, 0);
        assert_eq!(counters.durability_operations, 0);
        if mode == 2 {
            assert!(matches!(result, Err(Error::Indeterminate(id)) if id == operation));
            assert_eq!(counters.authority_records_appended, 0);
            assert_eq!(counters.authority_bytes_written, 0);
        } else {
            assert_eq!(result?, CoordinatorApply::Applied);
            assert_eq!(counters.authority_records_appended, 1 + u64::from(mode == 0));
            assert_eq!(ordinary.release_lease(&lease,
                IdempotencyKey::new("original-measured-release")?).await?, CoordinatorApply::Replayed);
            assert_eq!(ordinary.scheduler(), coordinator.scheduler());
        }
        provider.hide_receipt.store(false, Ordering::SeqCst);
        let reopened = DistributedCoordinator::open(&client, Arc::new(UnmeasuredContent::default())).await?;
        let retained = reopened.scheduler().operation(operation)
            .ok_or_else(|| Error::NotFound("released original task".into()))?;
        assert_eq!(retained.phase, crate::scheduler::OperationPhase::WaitingForCapacity);
        assert!(retained.reservation.is_none());
    }
    Ok(())
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
        assert_eq!(failure.work.authority_records_appended, 0);
        assert_eq!(provider.io_snapshot().writes, 0);
        assert_eq!(coordinator.revision, revision);
        assert_eq!(client.stream(COORDINATOR_PATH)?.tail().await?, revision);
        assert_eq!(coordinator.scheduler().operation(operation)
            .and_then(|operation| operation.reservation.as_ref()), Some(&lease.reservation));
    }
    Ok(())
}

#[tokio::test]
async fn measured_quiescent_suspension_uses_original_atomic_execution_cas_and_commit_recovery() -> Result<()> {
    for mode in 0..4 {
        let provider = Arc::new(FaultStream::default());
        let client = StreamClient::new(provider.clone());
        let operation = OperationId::from_bytes([118; 16]);
        let execution = OperationId::from_bytes([119; 16]);
        let (mut coordinator, lease) = admitted_fixture(&client, operation).await?;
        let fence = LeaseFence::from(&lease.reservation);
        coordinator.apply(operation, IdempotencyKey::new("quiescent-start")?,
            SchedulerEvent::Started {operation_id:operation, fence:fence.clone()}).await?;
        let revision = coordinator.revision;
        provider.commit_lose_ack.store(mode == 1 || mode == 2, Ordering::SeqCst);
        provider.hide_receipt.store(mode == 2, Ordering::SeqCst);
        provider.execution_race.store(mode == 3, Ordering::SeqCst);
        provider.reset_io();
        let cancellation = CancellationToken::new();
        let mut work = HistoryWork::new(budget(), &cancellation);
        work.preflight()?;
        let result = coordinator.apply_if_execution_quiescent_with_work(
            IdempotencyKey::new("measured-quiescent")?,
            SchedulerEvent::WorkflowSuspended {
                operation_id:operation, fence, workflow_revision:1, waiting_command:Some(execution),
            }, execution, 0, None, &mut Some(&mut work),
        ).await;
        let io = provider.io_snapshot();
        assert_eq!(work.receipt().backend_read_operations, io.reads + io.inspections);
        assert_eq!(work.receipt().backend_write_operations, io.writes);
        if mode == 3 {
            assert!(matches!(result, Err(Error::Conflict(_))));
            assert_eq!(work.receipt().authority_records_appended, 0);
            assert_eq!(client.stream(COORDINATOR_PATH)?.tail().await?, revision);
        } else if mode == 2 {
            assert!(matches!(result, Err(Error::Indeterminate(id)) if id == operation));
            assert_eq!(work.receipt().authority_records_appended, 0);
        } else {
            assert_eq!(result?, CoordinatorApply::Applied);
            assert_eq!(work.receipt().authority_records_appended, 1 + u64::from(mode == 0));
            assert!(work.receipt().authority_records_read >= 1 + u64::from(mode == 1));
        }
        provider.hide_receipt.store(false, Ordering::SeqCst);
        let reopened = DistributedCoordinator::open(&client, Arc::new(UnmeasuredContent::default())).await?;
        assert_eq!(reopened.scheduler().operation(operation)
            .ok_or_else(|| Error::NotFound("quiescent task".into()))?.phase,
            if mode == 3 {crate::scheduler::OperationPhase::Running}
            else {crate::scheduler::OperationPhase::Suspended});
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
    assert!(matches!(coordinator.apply_if_execution_quiescent_with_work(
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
    let result = coordinator.apply_if_execution_quiescent_with_work(
        IdempotencyKey::new("stale-owner-suspension")?,
        SchedulerEvent::WorkflowSuspended {
            operation_id:operation, fence:LeaseFence::from(&lease.reservation),
            workflow_revision:1, waiting_command:Some(execution),
        }, execution, 0, None, &mut Some(&mut work),
    ).await;
    assert!(matches!(result, Err(Error::Conflict(_))));
    assert!(work.receipt().authority_records_read >= 1);
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
            &mut Some(&mut work),
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
        assert_eq!(work.receipt().authority_records_appended, 0);
    }
    Ok(())
}

#[tokio::test]
async fn measured_original_declaration_publishes_and_replays_with_real_filesystem_verification() -> Result<()> {
    use crate::{
        AgentId, Capabilities,
        conversation::{ContentGrant, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
        filesystem::{FilesystemContentVerifier, FilesystemHost},
        resources::ProviderRef,
    };
    let fs_provider = ProviderRef::new("declaration-sdk-source", "filesystem", "2")?;
    let host = Arc::new(FilesystemHost::new(acyclic_fs::Fs::memory(), fs_provider.clone())?);
    let agent = AgentId::from_bytes([123;16]);
    let volume = VolumeRef::new(fs_provider, "declaration-state", VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent))?;
    host.create_volume(&volume).await?;
    let owner = crate::core::Authority {
        kind:crate::core::AggregateKind::Task, id:"owner".into(),
    };
    let issuer = crate::core::AuthorityIssuer::new("test-runtime", [9;32], owner.clone());
    let file_scope = issuer.root_for_agent(agent, "declaration-content", Capabilities::new([
        volume.capability(VolumeOperation::Read)?, volume.capability(VolumeOperation::Write)?,
    ]));
    let grant = ContentGrant::verify(&issuer.verifier(), &file_scope, &volume, VolumeOperation::Write)?;
    let state = host.put_content(&volume, &grant, "state/initial.json", b"null",
        "application/json", "initial.json", 4096, &IdempotencyKey::new("declaration-state")?).await?;
    let content: Arc<dyn ContentResidencyVerifier> = Arc::new(FilesystemContentVerifier::new(
        host, issuer.verifier(), file_scope, 4096,
    )?);
    let provider = Arc::new(FaultStream::default());
    let client = StreamClient::new(provider.clone());
    let mut coordinator = DistributedCoordinator::open(&client, content.clone()).await?;
    let mut ordinary = DistributedCoordinator::open(&client, content).await?;
    let operation = OperationId::from_bytes([124;16]);
    let mut declaration = spec(operation, 0)?;
    declaration.state = state;
    let scope = issuer.root("declaration-authority", Capabilities::new(["operation:declare"]));
    let cancellation = CancellationToken::new();
    let mut work = HistoryWork::new(budget(), &cancellation);
    work.preflight()?;
    provider.reset_io();
    let result = coordinator.declare_operation_with_work(&owner, &scope, &issuer.verifier(),
        declaration.clone(), IdempotencyKey::new("original-measured-declaration")?,
        &mut Some(&mut work)).await;
    assert_eq!(result?, CoordinatorApply::Applied);
    assert_eq!(coordinator.revision, 1);
    assert_eq!(work.receipt().authority_records_appended, 2);
    assert!(work.receipt().object_bytes_read >= 8); // original verify before append and replay
    assert!(work.receipt().authority_bytes_read > 0);
    assert!(work.receipt().bytes_encoded > 0);
    assert!(work.receipt().bytes_hashed > 0);
    assert_eq!(ordinary.declare_operation(&owner, &scope, &issuer.verifier(),
        declaration, IdempotencyKey::new("original-measured-declaration")?).await?,
        CoordinatorApply::Replayed);
    assert_eq!(ordinary.scheduler(), coordinator.scheduler());
    Ok(())
}

#[tokio::test]
async fn scheduler_json_admission_guards_all_original_variants_without_second_decode_or_io() -> Result<()> {
    use crate::{resources::CheckpointRef, scheduler::{Orchestration, Reservation, SessionLimits}};
    let operation = OperationId::from_bytes([125;16]);
    let mut declaration = spec(operation, 0)?;
    declaration.entrypoint.result_schema = serde_json::json!({
        "type":"object", "properties":{"nested":{"type":"array","items":{"enum":["escape\n\"\\","x"]}}},
    });
    declaration.orchestration = Orchestration::Reduce {reducer:declaration.entrypoint.clone()};
    declaration.placement.insert("zone".into(),"placement".into());
    declaration.resources.0.insert("memory".into(),1);
    let file = declaration.state.clone();
    let entrypoint = declaration.entrypoint.clone();
    let fence = LeaseFence {reservation_id:"original-json-reservation".into(),placement:"worker".into()};
    let reservation = Reservation {id:fence.reservation_id.clone(),placement:fence.placement.clone(),
        admitted:declaration.resources.clone()};
    let checkpoint = CheckpointRef::new(file.volume().provider().clone(),
        b"original-checkpoint".to_vec(),Some("1".into()))?;
    let events = vec![
        SchedulerEvent::SessionConfigured {operation_id:operation,
            limits:SessionLimits {active_tasks:1,total_tasks:2,depth:1,model_steps:1}},
        SchedulerEvent::ModelDispatchClaimed {operation_id:operation,attempt_id:operation,
            step:1,request_digest:[3;32],fence:fence.clone(),ceiling:1},
        SchedulerEvent::Declared {spec:Box::new(declaration)},
        SchedulerEvent::WaitingForCapacity {operation_id:operation},
        SchedulerEvent::Admitted {operation_id:operation,reservation:reservation.clone()},
        SchedulerEvent::PartiallyAdmitted {operation_id:operation,reservation},
        SchedulerEvent::Rejected {operation_id:operation,reason:"original rejection".into()},
        SchedulerEvent::Started {operation_id:operation,fence:fence.clone()},
        SchedulerEvent::Checkpointed {operation_id:operation,checkpoint,fence:fence.clone()},
        SchedulerEvent::WaitingForChildren {operation_id:operation,fence:fence.clone()},
        SchedulerEvent::WorkflowSuspended {operation_id:operation,fence:fence.clone(),
            workflow_revision:1,waiting_command:Some(operation)},
        SchedulerEvent::WorkflowResumed {operation_id:operation,workflow_revision:1,input:file.clone()},
        SchedulerEvent::ReconciliationResumed {operation_id:operation,fence:fence.clone()},
        SchedulerEvent::LeaseReleased {operation_id:operation,fence:fence.clone()},
        SchedulerEvent::CancellationRequested {operation_id:operation,recursive:true},
        SchedulerEvent::Completed {operation_id:operation,outcome:crate::Outcome::Succeeded(file.clone()),
            fence:Some(fence),execution_duration_ns:Some(7)},
        SchedulerEvent::Orchestrated {operation_id:operation,expected_revision:1,
            outcome:crate::Outcome::Succeeded(file),cancel:vec![operation;17],
            reducer:Some(entrypoint),reduction_digest:Some([4;32])},
    ];
    assert_eq!(events.len(),17);
    for event in events {
        let bytes = crate::contract::canonical_json_bytes(&event)?;
        let cancellation = CancellationToken::new();
        let mut work = HistoryWork::new(budget(),&cancellation);
        work.preflight()?;
        let admission = SchedulerEventJsonAdmission::prepare(&bytes,&mut work)?;
        let maximum = admission.maximum_owned_bytes;
        let decoded = admission.decode()?;
        assert_eq!(decoded,event);
        assert!(maximum >= decoded.logical_owned_bytes()?);
        assert_eq!(crate::contract::canonical_json_bytes(&decoded)?,bytes);
        assert_eq!(work.receipt().backend_read_operations,0);
        assert_eq!(work.receipt().backend_write_operations,0);
        assert_eq!(work.receipt().authority_records_read,0);
        assert_eq!(work.receipt().allocation_operations,0);
        assert!(work.receipt().items_examined > 0);
        assert!(work.receipt().peak_allocation_bytes >= maximum);
    }
    Ok(())
}

#[tokio::test]
async fn scheduler_json_admission_quota_cancel_and_duplicate_faults_precede_original_serde() -> Result<()> {
    let duplicate = br#"{"kind":"lease_released","operation_id":"00000000-0000-0000-0000-000000000001","fence":{"reservation_id":"a","reservation_id":"b","placement":"worker"}}"#;
    for mode in 0..3 {
        let cancellation = CancellationToken::new();
        if mode == 0 {cancellation.cancel();}
        let mut allowance = budget();
        if mode == 1 {allowance.peak_allocation_bytes = 0;}
        let mut work = HistoryWork::new(allowance,&cancellation);
        let admitted = SchedulerEventJsonAdmission::prepare(duplicate,&mut work);
        if mode == 2 {
            let admission = admitted?;
            assert!(admission.maximum_owned_bytes > duplicate.len() as u64);
            let expected = crate::contract::json_from_slice::<SchedulerEvent>(duplicate)
                .expect_err("duplicate original fence field");
            let actual = admission.decode().expect_err("original serde must reject the same duplicate");
            assert_eq!(actual.to_string(),Error::Storage(expected.to_string()).to_string());
        } else {
            assert!(matches!(&admitted,Err(Error::Invalid(_))));
            drop(admitted);
        }
        assert_eq!(work.receipt().backend_read_operations,0);
        assert_eq!(work.receipt().backend_write_operations,0);
        assert_eq!(work.receipt().allocation_operations,0);
    }
    let deep = format!("{}0{}","[".repeat(257),"]".repeat(257));
    let cancellation = CancellationToken::new();
    let mut work = HistoryWork::new(budget(),&cancellation);
    assert!(SchedulerEventJsonAdmission::prepare(deep.as_bytes(),&mut work).is_err());
    Ok(())
}

#[tokio::test]
async fn scheduler_json_quota_guard_releases_unused_and_cancelled_reservations() -> Result<()> {
    let bytes = crate::contract::canonical_json_bytes(&SchedulerEvent::LeaseReleased {
        operation_id:OperationId::from_bytes([126;16]),
        fence:LeaseFence {reservation_id:"guard-owned".into(),placement:"worker".into()},
    })?;
    let cancellation = CancellationToken::new();
    let mut sizing = HistoryWork::new(budget(),&cancellation);
    let admission = SchedulerEventJsonAdmission::prepare(&bytes,&mut sizing)?;
    let exact = admission.maximum_owned_bytes;
    drop(admission);
    let mut allowance = budget();
    allowance.peak_allocation_bytes = exact;
    let mut work = HistoryWork::new(allowance,&cancellation);
    drop(SchedulerEventJsonAdmission::prepare(&bytes,&mut work)?);
    let admission = SchedulerEventJsonAdmission::prepare(&bytes,&mut work)?;
    cancellation.cancel();
    assert!(matches!(admission.decode(),Err(Error::Invalid(_))));
    assert_eq!(work.receipt().peak_allocation_bytes,exact);
    assert_eq!(work.receipt().backend_read_operations,0);
    assert_eq!(work.receipt().backend_write_operations,0);
    assert_eq!(work.receipt().allocation_operations,0);
    Ok(())
}

#[tokio::test]
async fn prepared_key_admits_before_copy_and_unused_request_releases_its_custody() -> Result<()> {
    let cancellation = CancellationToken::new();
    let mut allowance = budget();
    allowance.peak_allocation_bytes = 31;
    let mut denied = HistoryWork::new(allowance, &cancellation);
    {
        let mut observer = Some(&mut denied as &mut dyn crate::store::history::HistoryIo);
        assert!(matches!(PreparedCommitRequest::new_with_digest([19; 32], &mut observer),
            Err(Error::Invalid(_))));
    }
    assert_eq!(denied.receipt().allocation_operations, 0);
    assert_eq!(denied.receipt().bytes_copied, 0);
    allowance.peak_allocation_bytes = 32;
    let mut work = HistoryWork::new(allowance, &cancellation);
    for _ in 0..2 {
        let mut observer = Some(&mut work as &mut dyn crate::store::history::HistoryIo);
        let prepared = PreparedCommitRequest::new_with_digest([19; 32], &mut observer)?;
        assert_eq!(prepared.custody.retained(), 32);
        assert_eq!(prepared.request.conditions.capacity(), 0);
        assert_eq!(prepared.request.mutations.capacity(), 0);
        drop(prepared);
    }
    assert_eq!(work.receipt().allocation_operations, 2);
    assert_eq!(work.receipt().bytes_copied, 64);
    assert_eq!(work.receipt().peak_allocation_bytes, 32);
    assert_eq!(work.receipt().backend_write_operations, 0);
    Ok(())
}

#[tokio::test]
async fn prepared_append_partial_growth_releases_actual_vectors_and_prevents_record_allocation() -> Result<()> {
    let cancellation = CancellationToken::new();
    let key = stream_key_with_work("prepared-partial-growth", &mut None).map(|(key, _)| key)?;
    let path = StreamPath::new("harness/v2/execution/prepared-partial-growth")?;
    let mut allowance = budget();
    allowance.peak_allocation_bytes = std::mem::size_of::<CommitMutation>() as u64;
    let mut work = HistoryWork::new(allowance, &cancellation);
    for _ in 0..2 {
        let mut observer = Some(&mut work as &mut dyn crate::store::history::HistoryIo);
        let mut prepared = PreparedCommitRequest::new(key.clone(), &mut observer)?;
        assert!(matches!(prepared.append(path.clone(), OwnedBytes::from_static(b"source-owned")),
            Err(Error::Invalid(_))));
        assert!(prepared.request.mutations.is_empty());
        assert_eq!(prepared.vectors,
            (prepared.request.mutations.capacity() * std::mem::size_of::<CommitMutation>()) as u64);
        assert_eq!(prepared.custody.retained(), prepared.vectors);
        drop(prepared);
    }
    assert_eq!(work.receipt().allocation_operations, 2);
    assert_eq!(work.receipt().peak_allocation_bytes, allowance.peak_allocation_bytes);
    assert_eq!(work.receipt().backend_write_operations, 0);
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
        commit_prepared_with_work(&client, prepared, OperationId::from_bytes([127; 16])).await
    };
    assert!(matches!(result, Err(Error::Invalid(_))));
    assert_eq!(work.receipt().allocation_operations, 4);
    assert_eq!(work.receipt().bytes_copied, 32);
    assert_eq!(work.receipt().backend_write_operations, 0);
    assert_eq!(work.receipt().authority_records_appended, 0);
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
        IdempotencyKey::new(format!("task-admission:{operation}"))?).await?;
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
    let (proof, custody) = original_reservation_proof_with_native_work(
        &client, &owner, &scope, &issuer.verifier(), operation, &original, &native,
    ).await?;
    assert_eq!(proof.original_fence(), &original);
    assert_eq!(proof.declaration().operation_id, operation);
    let (declaration_source, started_source) = proof.source_positions();
    assert_eq!(declaration_source.0, 1);
    assert_eq!(started_source.0, 3);
    assert_ne!(declaration_source.1, started_source.1);
    assert!(custody.bytes() > 0);
    let declaration_pointer = proof.declaration().owner.authority().id.as_ptr();
    let fence_pointer = proof.original_fence().reservation_id.as_ptr();
    let (declaration, fence) = proof.into_parts();
    assert_eq!(declaration.owner.authority().id.as_ptr(), declaration_pointer);
    assert_eq!(fence.reservation_id.as_ptr(), fence_pointer);
    assert_eq!(native.receipt().backend_read_operations,
        provider.io_snapshot().reads + provider.io_snapshot().tails as u64);
    assert_eq!(native.receipt().authority_bytes_read, provider.io_snapshot().read_bytes);
    assert_eq!(native.receipt().backend_write_operations, 0);
    assert_eq!(native.receipt().object_bytes_read, 0);
    assert_eq!(provider.io_snapshot().writes, 0);
    drop((declaration, fence, custody));
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
            assert_eq!(counters.authority_records_read, 1);
            if mode == 4 { assert_eq!(counters.authority_bytes_read, 7); }
            if mode == 5 { assert_eq!(counters.authority_bytes_read, acyclic_stream::MAX_RECORD_BYTES as u64 + 1); }
        } else {
            assert_eq!(provider.io_snapshot().reads, 2);
            assert_eq!(provider.io_snapshot().tails, 1);
        }
        assert_eq!(provider.io_snapshot().writes, 0);
        assert_eq!(counters.backend_write_operations, 0);
        assert_eq!(counters.authority_records_appended, 0);
    }
    Ok(())
}

#[tokio::test]
async fn immutable_locator_closed_admission_guards_key_allocation_and_preserves_original_serde_error() -> Result<()> {
    let input = br#"{"key":"first","key":"duplicate","revision":1,"event_digest":[0],"commit_id":[0]}"#;
    let cancellation = CancellationToken::new();
    let mut allowance = budget();
    allowance.peak_allocation_bytes = 0;
    let mut denied = HistoryWork::new(allowance, &cancellation);
    assert!(matches!(decode_intent_location_with_work(input, &mut Some(&mut denied)), Err(Error::Invalid(_))));
    assert_eq!(denied.receipt().peak_allocation_bytes, 0);
    let original = decode_intent_location_with_work(input, &mut None).expect_err("original duplicate field");
    let mut work = HistoryWork::new(budget(), &cancellation);
    let observed = decode_intent_location_with_work(input, &mut Some(&mut work)).expect_err("same duplicate field");
    assert_eq!(original.to_string(), observed.to_string());
    assert_eq!(work.receipt().backend_read_operations, 0);
    assert_eq!(work.receipt().backend_write_operations, 0);
    Ok(())
}

#[tokio::test]
async fn observed_declaration_uses_the_same_immutable_source_without_readonly_bootstrap() -> Result<()> {
    let provider = Arc::new(FaultStream::default());
    let client = StreamClient::new(provider.clone());
    let operation = OperationId::from_bytes([131; 16]);
    let (owner, issuer, scope, _, _) = historical_source(&client, operation).await?;
    let key = IdempotencyKey::new(format!("task-admission:{operation}"))?;
    let ordinary = observe_declaration(&client, &owner, &scope, &issuer.verifier(), operation, &key)
        .await?.expect("original declaration");
    provider.reset_io();
    provider.forbid_writes.store(true, Ordering::SeqCst);
    let cancellation = CancellationToken::new();
    let mut work = HistoryWork::new(budget(), &cancellation);
    let observed = observe_declaration_with_work(
        &client, &owner, &scope, &issuer.verifier(), operation, &key, &mut Some(&mut work),
    ).await?.expect("same immutable indexed declaration");
    assert_eq!(observed, ordinary);
    assert_eq!(provider.io_snapshot().reads, 2);
    assert_eq!(work.receipt().authority_records_read, 2);
    assert_eq!(work.receipt().authority_bytes_read, provider.io_snapshot().read_bytes);
    assert_eq!(provider.io_snapshot().writes, 0);
    let missing = IdempotencyKey::new(format!("missing-task-admission:{operation}"))?;
    assert!(observe_declaration_with_work(&client, &owner, &scope, &issuer.verifier(),
        operation, &missing, &mut Some(&mut work)).await?.is_none());
    assert_eq!(provider.io_snapshot().writes, 0);
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
