use super::*;

#[tokio::test]
async fn native_denial_precedes_read_capture() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    let meter = Arc::new(Meter::local());
    meter.deny_allocation.store(true, Ordering::SeqCst);
    let error = provider.native_read(ReadRequest {
        path: StreamPath::new("uncaptured")?, from: 0, limit: 1,
    }, meter.original()).await.err().ok_or("native read unexpectedly captured")?;
    assert_original_denial(&error, "allocation");
    assert_eq!(meter.allocations.load(Ordering::SeqCst), 0);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    assert_eq!(meter.copied.load(Ordering::SeqCst), 0);
    assert_eq!(meter.read_operations.load(Ordering::SeqCst), 0);
    meter.assert_refunded();
    Ok(())
}

#[tokio::test]
async fn native_append_lost_ack_reopens_exact_append_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let limits = LocalStreamLimits::default();
    let provider = LocalStream::open(directory.path(), limits).await?;
    let request = keyed_append("native-append-lost-ack", 0)?;
    let key = request.idempotency_key.clone().ok_or("append fixture lacks a key")?;
    let meter = Arc::new(Meter::local());
    meter.cancel_on_write.store(true, Ordering::SeqCst);
    let error = provider.native_append(request.clone(), meter.original()).await.err()
        .ok_or("accepted cancelled append fabricated an acknowledgement")?;
    assert_original_denial(&error, "cancelled");
    let accepted = provider.inspect_idempotency(key.clone()).await?
        .ok_or("accepted append identity disappeared")?;
    let crate::IdempotencyOutcome::Append(outcome) = &accepted.outcome else {
        return Err("native append was rewritten into the Commit family".into());
    };
    assert!(matches!(outcome, AppendOutcome::Committed(_)));
    drop(provider);
    let reopened = LocalStream::open(directory.path(), limits).await?;
    assert_eq!(reopened.inspect_idempotency(key.clone()).await?, Some(accepted.clone()));
    assert_eq!(reopened.append(request.clone()).await?, *outcome);
    assert_eq!(reopened.tail(request.path.clone()).await?, 1);
    // Reusing the same bytes/key through Commit is a different operation.
    let wrong_family = CommitRequest {
        conditions: vec![crate::CommitCondition::Tail { path: request.path.clone(), expected: 1 }],
        mutations: vec![crate::CommitMutation::Append { path: request.path, records: request.records }],
        idempotency_key: key,
    };
    meter.cancelled.store(false, Ordering::SeqCst);
    let failure = reopened.native_commit(wrong_family, meter.original()).await?
        .err().ok_or("Commit incorrectly reused an Append identity")?;
    assert_eq!(&failure, &StreamError::IdempotencyMismatch);
    drop(failure);
    drop(reopened);
    meter.assert_refunded();
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn native_append_keeps_keyed_conflict_and_does_not_journal_unkeyed_conflict()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let limits = LocalStreamLimits::default();
    let provider = LocalStream::open(directory.path(), limits).await?;
    let path = StreamPath::new("native-append-conflict")?;
    provider.append(AppendRequest {
        path: path.clone(), records: vec![OwnedBytes::from_static(b"seed")],
        if_tail: Some(0), idempotency_key: None,
    }).await?;
    let request = keyed_append(path.as_str(), 0)?;
    let key = request.idempotency_key.clone().ok_or("append fixture lacks a key")?;
    let meter = Arc::new(Meter::local());
    let expected = AppendOutcome::TailConflict { actual_tail: 1 };
    let conflict = provider.native_append(request.clone(), meter.original()).await??;
    assert_eq!(conflict.value(), &expected);
    drop(conflict);
    let after_keyed = std::fs::metadata(directory.path().join(JOURNAL_FILE))?.len();
    let mut unkeyed = request.clone();
    unkeyed.idempotency_key = None;
    let conflict = provider.native_append(unkeyed, meter.original()).await??;
    assert_eq!(conflict.value(), &expected);
    drop(conflict);
    assert_eq!(std::fs::metadata(directory.path().join(JOURNAL_FILE))?.len(), after_keyed);
    let accepted = provider.inspect_idempotency(key.clone()).await?
        .ok_or("keyed conflict lost its original identity")?;
    assert_eq!(accepted.outcome, crate::IdempotencyOutcome::Append(expected.clone()));
    drop(provider);
    let reopened = LocalStream::open(directory.path(), limits).await?;
    assert_eq!(reopened.inspect_idempotency(key).await?, Some(accepted));
    assert_eq!(reopened.append(request).await?, expected);
    assert_eq!(reopened.tail(path).await?, 1);
    drop(reopened);
    meter.assert_refunded();
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn native_request_copy_cancellation_precedes_detached_publication()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    let request = keyed_commit("uncaptured-request", 0)?;
    let key = request.idempotency_key.clone();
    let meter = Arc::new(Meter::local());
    meter.cancel_on_copy.store(true, Ordering::SeqCst);
    let error = provider.native_commit(request, meter.original()).await.err()
        .ok_or("cancelled capture escaped into publication")?;
    assert_original_denial(&error, "cancelled");
    assert_eq!(meter.hashed.load(Ordering::SeqCst), 0);
    assert_eq!(meter.write_operations.load(Ordering::SeqCst), 0);
    assert_eq!(meter.durability_operations.load(Ordering::SeqCst), 0);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    assert!(provider.inspect_idempotency(key).await?.is_none());
    assert_eq!(std::fs::metadata(directory.path().join(JOURNAL_FILE))?.len(), HEADER_BYTES as u64);
    meter.assert_refunded();
    Ok(())
}

#[tokio::test]
async fn native_durability_denial_precedes_semantic_cas()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    let meter = Arc::new(Meter::local());
    meter.deny_durability.store(true, Ordering::SeqCst);
    let request = keyed_commit("not-published", 0)?;
    let key = request.idempotency_key.clone();
    let error = provider.native_commit(request, meter.original()).await.err()
        .ok_or("native commit unexpectedly passed the durability gate")?;
    assert_original_denial(&error, "durability");
    assert!(provider.inspect_idempotency(key).await?.is_none());
    assert_eq!(provider.tail(StreamPath::new("not-published")?).await, Err(StreamError::NotFound));
    assert_eq!(std::fs::metadata(directory.path().join(JOURNAL_FILE))?.len(), HEADER_BYTES as u64);
    assert_eq!(meter.write_operations.load(Ordering::SeqCst), 0);
    assert_eq!(meter.durability_operations.load(Ordering::SeqCst), 0);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    assert!(meter.hashed.load(Ordering::SeqCst) > 0);
    meter.assert_refunded();
    Ok(())
}

#[tokio::test]
async fn borrowed_persisted_outcomes_match_unchanged_generated_schema()
-> Result<(), Box<dyn std::error::Error>> {
    use prost::Message as _;
    use crate::wire_codec::CanonicalWire;
    let provider = MemoryStream::default();
    let append_key = IdempotencyKey::new(OwnedBytes::from_static(b"wire-append"))?;
    let fork_key = IdempotencyKey::new(OwnedBytes::from_static(b"wire-fork"))?;
    let commit_key = IdempotencyKey::new(OwnedBytes::from_static(b"wire-commit"))?;
    provider.append(AppendRequest {
        path: StreamPath::new("wire/source")?,
        records: vec![OwnedBytes::new(), OwnedBytes::from_static(b"body")],
        if_tail: Some(0), idempotency_key: Some(append_key.clone()),
    }).await?;
    provider.fork(ForkRequest {
        source: StreamPath::new("wire/source")?, destination: StreamPath::new("wire/fork")?,
        at_tail: Some(1), idempotency_key: Some(fork_key.clone()),
    }).await?;
    let outcome = provider.commit(CommitRequest {
        conditions: vec![
            crate::CommitCondition::Tail { path: StreamPath::new("wire/source")?, expected: 2 },
            crate::CommitCondition::Tail { path: StreamPath::new("wire/fork")?, expected: 1 },
            crate::CommitCondition::Absent { path: StreamPath::new("wire/new-fork")? },
        ],
        mutations: vec![
            crate::CommitMutation::Append { path: StreamPath::new("wire/fork")?,
                records: vec![OwnedBytes::from_static(b"append")] },
            crate::CommitMutation::Fork {
                source: StreamPath::new("wire/source")?, destination: StreamPath::new("wire/new-fork")?,
                at_tail: 2, records: vec![OwnedBytes::from_static(b"forked")],
            },
        ], idempotency_key: commit_key.clone(),
    }).await?;
    let CommitOutcome::Committed(envelope) = outcome else { return Err("fixture conflict".into()); };
    assert_eq!(CanonicalWire::Envelope(&envelope).encode_to_vec()?,
        crate::wire_codec::envelope_to_wire(envelope.clone()).encode_to_vec());
    for key in [append_key, fork_key, commit_key] {
        let observation = provider.inspect_idempotency(key).await?.ok_or("missing wire identity")?;
        assert_eq!(CanonicalWire::Observation(
            &observation.idempotency_key.0, &observation.request_digest, &observation.outcome,
        ).encode_to_vec()?, crate::wire_codec::observation_wire(observation).encode_to_vec());
    }
    let key = IdempotencyKey::new(OwnedBytes::from_static(b"wire-conflicts"))?;
    let observation = IdempotencyObservation {
        idempotency_key: key, request_digest: [7; 32],
        outcome: crate::IdempotencyOutcome::Commit(CommitOutcome::Conflict(vec![
            crate::CommitConflict::Tail {
                path: StreamPath::new("wire/source")?, expected: 0, actual: Some(0),
            },
            crate::CommitConflict::Tail {
                path: StreamPath::new("wire/missing")?, expected: u64::MAX, actual: None,
            },
            crate::CommitConflict::Exists { path: StreamPath::new("wire/source")? },
        ])),
    };
    assert_eq!(CanonicalWire::Observation(
        &observation.idempotency_key.0, &observation.request_digest, &observation.outcome,
    ).encode_to_vec()?, crate::wire_codec::observation_wire(observation).encode_to_vec());
    Ok(())
}

#[tokio::test]
async fn prepared_denial_precedes_worker_and_canonical_edit()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    let request = prepared_fixture(&provider).await?;
    let meter = Arc::new(Meter::local());
    meter.deny_allocation.store(true, Ordering::SeqCst);
    let error = match provider.native_prepare_commit(request, 64, meter.original()).await {
        Err(error) => error,
        _ => return Err("denied preparation produced an accepted packet".into()),
    };
    assert_original_denial(&error, "allocation");
    assert_eq!(provider.tail(StreamPath::new("prepared/target")?).await?, 1);
    assert_eq!(meter.write_operations.load(Ordering::SeqCst), 0);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    meter.assert_refunded();
    Ok(())
}

#[tokio::test]
async fn prepared_late_cancel_drains_original_frame_and_recovery_without_fresh_admission()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    let mut request = prepared_fixture(&provider).await?;
    let meter = Arc::new(Meter::local());
    let packet = provider.native_prepare_commit(request.clone(), 64, meter.original()).await??;
    let (mut packet, packet_guard) = packet.into_parts();
    let recovery = packet.take_recovery_future().ok_or("missing precreated recovery")?;
    assert!(packet.take_recovery_future().is_none());
    // A prepared worker waiting for its body holds no provider lock.
    assert_eq!(provider.tail(StreamPath::new("prepared/target")?).await?, 1);
    let allocations = meter.allocations.load(Ordering::SeqCst);
    meter.cancelled.store(true, Ordering::SeqCst);
    let error = packet.finish(OwnedBytes::from_static(b"actual-completion")).await
        .err().ok_or("cancelled completion invented an acknowledgement")?;
    assert_original_denial(&error, "cancelled");
    assert_eq!(meter.allocations.load(Ordering::SeqCst), allocations);
    assert!(meter.write_bytes.load(Ordering::SeqCst) > 0);
    let recovery_error = recovery.await.err().ok_or("late cancelled inspection fabricated success")?;
    assert_original_denial(&recovery_error, "cancelled");
    assert_eq!(meter.allocations.load(Ordering::SeqCst), allocations);
    drop(packet_guard);
    drop(provider);
    let reopened = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    let [crate::CommitMutation::Append { records, .. }] = request.mutations.as_mut_slice()
        else { return Err("fixture changed family".into()); };
    records[0] = OwnedBytes::from_static(b"actual-completion");
    let observed = reopened.inspect_idempotency(request.idempotency_key.clone()).await?
        .ok_or("original prepared identity disappeared on reopen")?;
    let crate::IdempotencyOutcome::Commit(expected) = observed.outcome
        else { return Err("prepared operation changed canonical family".into()); };
    assert_eq!(reopened.commit(request).await?, expected);
    meter.assert_refunded();
    Ok(())
}

#[test]
fn prepared_partial_write_stops_at_exact_accepted_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    struct Partial { bytes: Vec<u8> }
    impl Write for Partial {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let length = bytes.len().min(3);
            self.bytes.extend_from_slice(&bytes[..length]);
            Ok(length)
        }
        fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
    }
    let meter = Arc::new(Meter::local());
    let mut plan = AdmittedFrameWrite::new(&meter.original(), 6)?.close_plan();
    let mut writer = Partial { bytes: Vec::new() };
    let (physical, observed) = plan.write_all(&mut writer, b"abcdef");
    assert!(matches!(physical, Err(LocalStreamError::IncompleteFrame)));
    observed?;
    assert_eq!(writer.bytes, b"abc");
    assert_eq!(meter.write_bytes.load(Ordering::SeqCst), 3);
    drop(plan);
    meter.assert_refunded();
    Ok(())
}

#[tokio::test]
async fn abandoned_prepared_packet_releases_worker_capacity_without_edit()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    let request = prepared_fixture(&provider).await?;
    let meter = Arc::new(Meter::local());
    let packet = provider.native_prepare_commit(request, 64, meter.original()).await??;
    let (packet, guard) = packet.into_parts();
    drop(packet);
    drop(guard);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while meter.held.load(Ordering::SeqCst) != 0 { tokio::task::yield_now().await; }
    }).await?;
    assert_eq!(provider.tail(StreamPath::new("prepared/target")?).await?, 1);
    assert_eq!(meter.write_operations.load(Ordering::SeqCst), 0);
    meter.assert_refunded();
    Ok(())
}

#[tokio::test]
async fn prepared_reply_drop_keeps_accepted_durability_and_input_custody()
-> Result<(), Box<dyn std::error::Error>> {
    let _serial = PERSIST_BLOCKER_TESTS.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    let request = prepared_fixture(&provider).await?;
    let meter = Arc::new(Meter::local());
    let packet = provider.native_prepare_commit(request, 64, meter.original()).await??;
    let (packet, packet_guard) = packet.into_parts();
    let (started_send, started_receive) = std::sync::mpsc::sync_channel(1);
    let (release_send, release_receive) = std::sync::mpsc::sync_channel(1);
    *JOURNAL_PERSIST_BLOCKER.lock().map_err(|_| "poisoned fixture seam")? = Some((
        Arc::as_ptr(&provider.inner) as usize, started_send, release_receive,
    ));
    let future = packet.finish(OwnedBytes::from_static(b"accepted-without-reply"));
    drop(future);
    drop(packet_guard);
    tokio::task::spawn_blocking(move || started_receive.recv()).await??;
    assert!(meter.held.load(Ordering::SeqCst) > 0);
    assert!(meter.write_bytes.load(Ordering::SeqCst) > 0);
    assert_eq!(meter.durability_operations.load(Ordering::SeqCst), 0);
    release_send.send(())?;
    assert_eq!(provider.tail(StreamPath::new("prepared/target")?).await?, 2);
    drop(provider);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while meter.held.load(Ordering::SeqCst) != 0 { tokio::task::yield_now().await; }
    }).await?;
    meter.assert_refunded();
    Ok(())
}

#[tokio::test]
async fn native_lost_ack_drains_flush_and_reopens_original_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let limits = LocalStreamLimits::default();
    let provider = LocalStream::open(directory.path(), limits).await?;
    let request = keyed_commit("lost-native-ack", 0)?;
    let key = request.idempotency_key.clone();
    let meter = Arc::new(Meter::local());
    meter.cancel_on_write.store(true, Ordering::SeqCst);
    let error = provider.native_commit(request.clone(), meter.original()).await.err()
        .ok_or("cancelled accepted mutation fabricated an acknowledgement")?;
    assert_original_denial(&error, "cancelled");
    assert!(std::fs::metadata(directory.path().join(JOURNAL_FILE))?.len() > HEADER_BYTES as u64);
    let accepted = provider.inspect_idempotency(key.clone()).await?
        .ok_or("accepted original identity disappeared")?;
    drop(provider);
    let reopened = LocalStream::open(directory.path(), limits).await?;
    assert_eq!(reopened.inspect_idempotency(key).await?, Some(accepted.clone()));
    assert_eq!(reopened.tail(StreamPath::new("lost-native-ack")?).await?, 1);
    let crate::IdempotencyOutcome::Commit(outcome) = &accepted.outcome else {
        return Err("reopened an unrelated operation family".into());
    };
    assert_eq!(&reopened.commit_before(request, 0).await?, outcome);
    drop(accepted);
    drop(error);
    meter.assert_refunded();
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn native_compaction_drains_accepted_controls_and_actual_close()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let limits = LocalStreamLimits { journal_operations: 2, ..LocalStreamLimits::default() };
    let provider = LocalStream::open(directory.path(), limits).await?;
    let request = keyed_commit("compact-native-ack", 0)?;
    let key = request.idempotency_key.clone();
    let meter = Arc::new(Meter::local());
    meter.cancel_on_durability.store(true, Ordering::SeqCst);
    let error = provider.native_commit(request.clone(), meter.original()).await.err()
        .ok_or("late cancelled compaction fabricated an acknowledgement")?;
    assert_original_denial(&error, "cancelled");
    assert_eq!(std::fs::metadata(directory.path().join(JOURNAL_FILE))?.len(), HEADER_BYTES as u64);
    assert!(directory.path().join(SNAPSHOT_FILE).is_file());
    assert!(!directory.path().join(SNAPSHOT_TEMPORARY).exists());
    let accepted = provider.inspect_idempotency(key.clone()).await?
        .ok_or("compaction erased original identity")?;
    drop(provider);
    let reopened = LocalStream::open(directory.path(), limits).await?;
    assert_eq!(reopened.inspect_idempotency(key).await?, Some(accepted.clone()));
    assert_eq!(reopened.tail(StreamPath::new("compact-native-ack")?).await?, 1);
    let crate::IdempotencyOutcome::Commit(outcome) = &accepted.outcome else {
        return Err("snapshot retained an unrelated operation".into());
    };
    assert_eq!(&reopened.commit_before(request, 0).await?, outcome);
    drop(accepted);
    drop(error);
    meter.assert_refunded();
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn native_commit_before_checks_actual_cas_and_preserves_replay()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let clock = Arc::new(AtomicU64::default());
    clock.store(99, Ordering::SeqCst);
    let provider = LocalStream::open_with_clock(
        directory.path(), LocalStreamLimits::default(), clock.clone(),
    ).await?;
    let request = keyed_commit("native-expired", 0)?;
    let journal_before = std::fs::read(directory.path().join(JOURNAL_FILE))?;
    let meter = Arc::new(Meter::local());
    *meter.expire_after_hash.lock().map_err(|_| "native clock seam was poisoned")? =
        Some((clock.clone(), 100));
    let expired = provider.native_commit_before(request.clone(), 100, meter.original()).await?;
    assert!(matches!(expired.as_ref(), Err(StreamError::DeadlineElapsed)));
    assert_eq!(clock.load(Ordering::SeqCst), 100);
    assert_eq!(std::fs::read(directory.path().join(JOURNAL_FILE))?, journal_before);
    assert_eq!(meter.durability_operations.load(Ordering::SeqCst), 0);
    drop(expired);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    meter.assert_refunded();
    let accepted = provider.commit(request.clone()).await?;
    let replay = provider.native_commit_before(request, 0, meter.original()).await??;
    assert_eq!(replay.value(), &accepted);
    drop(replay);
    assert_eq!(provider.tail(StreamPath::new("native-expired")?).await?, 1);
    meter.assert_refunded();
    Ok(())
}

#[tokio::test]
async fn native_guard_denial_reads_same_provider_without_lock_and_preserves_typed_owner()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    provider.commit(keyed_commit("guard-readable", 0)?).await?;
    let before = std::fs::read(directory.path().join(JOURNAL_FILE))?;
    let request = keyed_commit("guard-denied", 0)?;
    let key = request.idempotency_key.clone();
    let meter = Arc::new(Meter::local());
    let callback_provider = provider.clone();
    let callback_meter = Arc::clone(&meter);
    let callback_path = StreamPath::new("guard-readable")?;
    let result = tokio::time::timeout(std::time::Duration::from_secs(5),
        provider.native_commit_guarded(request, None, meter.original(), move || {
            let provider = callback_provider.clone();
            let meter = Arc::clone(&callback_meter);
            let path = callback_path.clone();
            async move {
                let tail = provider.native_tail(path, meter.original()).await
                    .map_err(NativeGuardFailure::Io)??;
                assert_eq!(*tail.value(), 1);
                drop(tail);
                meter.cancelled.store(true, Ordering::SeqCst);
                Err(NativeGuardFailure::Owner(0x51))
            }
        }),
    ).await??;
    assert!(matches!(result, Err(NativeGuardFailure::Owner(0x51))));
    assert!(provider.inspect_idempotency(key).await?.is_none());
    assert_eq!(std::fs::read(directory.path().join(JOURNAL_FILE))?, before);
    drop(provider);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    meter.assert_refunded();
    Ok(())
}

#[tokio::test]
async fn native_guard_rechecks_actual_canonical_drift_before_retaining_owner_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let limits = LocalStreamLimits::default();
    let provider = LocalStream::open(directory.path(), limits).await?;
    let request = keyed_commit("guard-drift-target", 0)?;
    let key = request.idempotency_key.clone();
    let meter = Arc::new(Meter::local());
    let calls = Arc::new(AtomicU64::new(0));
    let callback_provider = provider.clone();
    let callback_meter = Arc::clone(&meter);
    let callback_calls = Arc::clone(&calls);
    let result = provider.native_commit_guarded(request, None, meter.original(), move || {
        let provider = callback_provider.clone();
        let meter = Arc::clone(&callback_meter);
        let calls = Arc::clone(&callback_calls);
        async move {
            if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                let intervening = keyed_commit("guard-drift-other", 0)
                    .map_err(|error| NativeGuardFailure::Provider(error))?;
                let result = provider.native_commit(intervening, meter.original()).await
                    .map_err(NativeGuardFailure::Io)??;
                assert!(matches!(result.value(), CommitOutcome::Committed(_)));
                drop(result);
                Ok(())
            } else {
                Err(NativeGuardFailure::Owner(0x52))
            }
        }
    }).await?;
    assert!(matches!(result, Err(NativeGuardFailure::Owner(0x52))));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(provider.inspect_idempotency(key.clone()).await?.is_none());
    assert_eq!(provider.tail(StreamPath::new("guard-drift-target")?).await, Err(StreamError::NotFound));
    assert_eq!(provider.tail(StreamPath::new("guard-drift-other")?).await?, 1);
    drop(provider);
    let reopened = LocalStream::open(directory.path(), limits).await?;
    assert!(reopened.inspect_idempotency(key).await?.is_none());
    assert_eq!(reopened.tail(StreamPath::new("guard-drift-other")?).await?, 1);
    drop(reopened);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    meter.assert_refunded();
    Ok(())
}
