use super::*;

mod headers;

#[tokio::test]
async fn prepared_memory_and_local_commits_fund_settlement_and_recovery_codec_work()
-> Result<(), Box<dyn std::error::Error>> {
    for local in [false, true] {
        let directory = tempfile::tempdir()?;
        let provider: Arc<dyn crate::NativeStreamProvider> = if local {
            Arc::new(LocalStream::open(directory.path(), LocalStreamLimits::default()).await?)
        } else {
            Arc::new(MemoryStream::new(MemoryLimits::default()))
        };
        let request = prepared_fixture(provider.as_ref()).await?;
        let maximum_hash = crate::request::commit_hash_bytes(&request)
            + 64
            + 8 * crate::MAX_RECORD_KEYS as u64
            + 16 * crate::MAX_RECORD_ATTRIBUTES as u64;
        use tracing_subscriber::prelude::*;
        let metrics = acyclic_observability::Metrics::default();
        let prepare = metrics.query("acyclic.stream.prepare_commit");
        let settle = metrics.query("acyclic.stream.commit");
        let inspect = metrics.query("acyclic.stream.inspect_idempotency");
        let _subscriber = tracing::subscriber::set_default(
            tracing_subscriber::registry().with(acyclic_observability::Operations(metrics)),
        );
        let meter = Arc::new(Meter::default());
        let packet = provider
            .native_prepare_commit(request, 64, meter.original())
            .await??;
        let (mut packet, guard) = packet.into_parts();
        assert!(
            meter.withheld_codec().hashed >= 2 * maximum_hash,
            "prepared settlement and recovery did not both retain codec credit"
        );
        meter.deny_codec.store(true, Ordering::SeqCst);
        assert!(
            acyclic_native_runtime::NativeReadMeter::admit_codec(meter.as_ref(), 0, 0, 1).is_err()
        );
        let recovery = packet.take_recovery_future().ok_or("missing recovery")?;
        let result = packet
            .finish(OwnedBytes::from_static(b"completed").into())
            .await??;
        assert!(matches!(result.value(), CommitOutcome::Committed(_)));
        assert_eq!(
            meter.withheld_codec().hashed,
            maximum_hash,
            "settlement consumed or leaked the independent recovery codec credit"
        );
        drop(result);
        let observed = recovery.await??;
        assert!(observed.value().is_some());
        drop(observed);
        for query in [&prepare, &settle, &inspect] {
            let snapshot = query.snapshot();
            assert_eq!(
                (snapshot.ok, snapshot.err, snapshot.incomplete),
                (1, 0, 0),
                "native preparation, settlement and recovery must each complete once"
            );
            assert_eq!(snapshot.latency_ns.iter().sum::<u64>(), 1);
        }

        drop(guard);
        drop(provider);
        meter.assert_refunded();
    }
    Ok(())
}

#[test]
fn prepared_frames_withhold_and_refund_codec_allowance() -> Result<(), Box<dyn std::error::Error>> {
    use acyclic_native_runtime::{NativeCodecWork, NativeReadMeter};
    let meter = Arc::new(Meter {
        codec_limit: NativeCodecWork {
            encoded: 128,
            copied: 128,
            hashed: 128 + b"acyclic-stream-local-frame-v1\0".len() as u64,
        },
        ..Meter::default()
    });
    let work = crate::native::Work::new(meter.original())?;
    let frame = PreparedFrame::reserve(128, Some(Arc::clone(&work)))?;
    for request in [(1, 0, 0), (0, 1, 0), (0, 0, 1)] {
        assert!(
            meter.admit_codec(request.0, request.1, request.2).is_err(),
            "prepared frame did not withhold codec credit"
        );
    }
    assert_eq!(meter.encoded.load(Ordering::SeqCst), 0);
    assert_eq!(meter.copied.load(Ordering::SeqCst), 0);
    assert_eq!(meter.hashed.load(Ordering::SeqCst), 0);
    drop(frame);
    meter.admit_codec(128, 128, meter.codec_limit.hashed)?;
    drop(work);
    meter.assert_refunded();
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn native_denial_precedes_read_capture() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    let meter = Arc::new(Meter::local());
    use tracing_subscriber::prelude::*;
    let metrics = acyclic_observability::Metrics::default();
    let query = metrics.query("acyclic.stream.read");
    let _subscriber = tracing::subscriber::set_default(
        tracing_subscriber::registry().with(acyclic_observability::Operations(metrics)),
    );
    meter.deny_allocation.store(true, Ordering::SeqCst);
    let error = provider
        .native_read(
            ReadRequest {
                key: None,
                path: StreamPath::new("uncaptured")?,
                from: 0,
                limit: 1,
                at_least: Default::default(),
            },
            meter.original(),
        )
        .await
        .err()
        .ok_or("native read unexpectedly captured")?;
    assert_original_denial(&error, "allocation");
    let snapshot = query.snapshot();
    assert_eq!((snapshot.ok, snapshot.err, snapshot.incomplete), (0, 1, 0));
    assert_eq!(snapshot.latency_ns.iter().sum::<u64>(), 1);

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
    let key = request.idempotency_key.clone();
    let meter = Arc::new(Meter::local());
    meter.cancel_on_write.store(true, Ordering::SeqCst);
    let error = provider
        .native_append(request.clone(), meter.original())
        .await
        .err()
        .ok_or("accepted cancelled append fabricated an acknowledgement")?;
    assert_original_denial(&error, "cancelled");
    let accepted = provider
        .inspect_idempotency(key.clone())
        .await?
        .ok_or("accepted append identity disappeared")?;
    let crate::IdempotencyOutcome::Append(outcome) = &accepted.outcome else {
        return Err("native append was rewritten into the Commit family".into());
    };
    assert!(matches!(outcome, AppendOutcome::Committed(_)));
    drop(provider);
    let reopened = LocalStream::open(directory.path(), limits).await?;
    assert_eq!(
        reopened.inspect_idempotency(key.clone()).await?,
        Some(accepted.clone())
    );
    assert_eq!(reopened.append(request.clone()).await?, *outcome);
    assert_eq!(reopened.tail(request.path.clone()).await?, 1);
    // Reusing the same bytes/key through Commit is a different operation.
    let wrong_family = CommitRequest {
        deadline_unix_millis: None,
        conditions: vec![crate::CommitCondition::Tail {
            path: request.path.clone(),
            expected: 1,
        }],
        mutations: vec![crate::CommitMutation::Append {
            path: request.path,
            records: request.records,
        }],
        idempotency_key: key,
    };
    meter.cancelled.store(false, Ordering::SeqCst);
    let failure = reopened
        .native_commit(wrong_family, None, meter.original())
        .await?
        .err()
        .ok_or("Commit incorrectly reused an Append identity")?;
    assert_eq!(&failure, &StreamError::IdempotencyMismatch);
    drop(reopened);
    meter.assert_refunded();
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn native_append_conflicts_leave_keys_free_and_journal_unchanged()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let limits = LocalStreamLimits::default();
    let provider = LocalStream::open(directory.path(), limits).await?;
    let path = StreamPath::new("native-append-conflict")?;
    provider
        .append(AppendRequest {
            path: path.clone(),
            records: vec![(OwnedBytes::from_static(b"seed")).into()],
            if_tail: Some(0),
            idempotency_key: crate::IdempotencyKey::unique(),
        })
        .await?;
    let request = keyed_append(path.as_str(), 0)?;
    let key = request.idempotency_key.clone();
    let before = std::fs::metadata(directory.path().join(JOURNAL_FILE))?.len();
    let meter = Arc::new(Meter::local());
    let expected = AppendOutcome::TailConflict { actual_tail: 1 };
    let conflict = provider
        .native_append(request.clone(), meter.original())
        .await??;
    assert_eq!(conflict.value(), &expected);
    drop(conflict);
    let after_keyed = std::fs::metadata(directory.path().join(JOURNAL_FILE))?.len();
    assert_eq!(after_keyed, before);
    assert!(provider.inspect_idempotency(key.clone()).await?.is_none());
    drop(provider);
    let reopened = LocalStream::open(directory.path(), limits).await?;
    assert!(reopened.inspect_idempotency(key.clone()).await?.is_none());
    assert_eq!(reopened.append(request.clone()).await?, expected);
    let mut retry = request;
    retry.if_tail = Some(1);
    assert!(matches!(
        reopened.append(retry.clone()).await?,
        AppendOutcome::Committed(_)
    ));
    let accepted = reopened
        .inspect_idempotency(key.clone())
        .await?
        .ok_or("missing success")?;
    assert_eq!(reopened.tail(path).await?, 2);
    drop(reopened);
    let reopened = LocalStream::open(directory.path(), limits).await?;
    assert_eq!(reopened.inspect_idempotency(key).await?, Some(accepted));
    assert!(matches!(
        reopened.append(retry).await?,
        AppendOutcome::Committed(_)
    ));
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
    let error = provider
        .native_commit(request, None, meter.original())
        .await
        .err()
        .ok_or("cancelled capture escaped into publication")?;
    assert_original_denial(&error, "cancelled");
    assert_eq!(meter.write_operations.load(Ordering::SeqCst), 0);
    assert_eq!(meter.durability_operations.load(Ordering::SeqCst), 0);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    assert!(provider.inspect_idempotency(key).await?.is_none());
    assert_eq!(
        std::fs::metadata(directory.path().join(JOURNAL_FILE))?.len(),
        HEADER_BYTES as u64
    );
    meter.assert_refunded();
    Ok(())
}

#[tokio::test]
async fn native_durability_denial_precedes_semantic_cas() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    let meter = Arc::new(Meter::local());
    meter.deny_durability.store(true, Ordering::SeqCst);
    let request = keyed_commit("not-published", 0)?;
    let key = request.idempotency_key.clone();
    let error = provider
        .native_commit(request, None, meter.original())
        .await
        .err()
        .ok_or("native commit unexpectedly passed the durability gate")?;
    assert_original_denial(&error, "durability");
    assert!(provider.inspect_idempotency(key).await?.is_none());
    assert_eq!(
        provider.tail(StreamPath::new("not-published")?).await,
        Err(StreamError::NotFound)
    );
    assert_eq!(
        std::fs::metadata(directory.path().join(JOURNAL_FILE))?.len(),
        HEADER_BYTES as u64
    );
    assert_eq!(meter.write_operations.load(Ordering::SeqCst), 0);
    assert_eq!(meter.durability_operations.load(Ordering::SeqCst), 0);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    meter.assert_refunded();
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
    let Err(error) = provider
        .native_prepare_commit(request, 64, meter.original())
        .await
    else {
        return Err("denied preparation produced an accepted packet".into());
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
    let packet = provider
        .native_prepare_commit(request.clone(), 64, meter.original())
        .await??;
    let (mut packet, packet_guard) = packet.into_parts();
    let recovery = packet
        .take_recovery_future()
        .ok_or("missing precreated recovery")?;
    assert!(packet.take_recovery_future().is_none());
    // A prepared worker waiting for its body holds no provider lock.
    assert_eq!(provider.tail(StreamPath::new("prepared/target")?).await?, 1);
    let allocations = meter.allocations.load(Ordering::SeqCst);
    meter.cancelled.store(true, Ordering::SeqCst);
    let error = packet
        .finish((OwnedBytes::from_static(b"actual-completion")).into())
        .await
        .err()
        .ok_or("cancelled completion invented an acknowledgement")?;
    assert_original_denial(&error, "cancelled");
    assert_eq!(meter.allocations.load(Ordering::SeqCst), allocations);
    assert!(meter.write_bytes.load(Ordering::SeqCst) > 0);
    let recovery_error = recovery
        .await
        .err()
        .ok_or("late cancelled inspection fabricated success")?;
    assert_original_denial(&recovery_error, "cancelled");
    assert_eq!(meter.allocations.load(Ordering::SeqCst), allocations);
    drop(packet_guard);
    drop(provider);
    let reopened = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    let [crate::CommitMutation::Append { records, .. }] = request.mutations.as_mut_slice() else {
        return Err("fixture changed family".into());
    };
    *records.first_mut().ok_or(StreamError::InvalidArgument)? =
        (OwnedBytes::from_static(b"actual-completion")).into();
    let observed = reopened
        .inspect_idempotency(request.idempotency_key.clone())
        .await?
        .ok_or("original prepared identity disappeared on reopen")?;
    let crate::IdempotencyOutcome::Commit(expected) = observed.outcome else {
        return Err("prepared operation changed canonical family".into());
    };
    let CommitOutcome::Committed(receipt) = reopened.commit(request).await? else {
        return Err("replay changed canonical family".into());
    };
    assert!(receipt.matches_envelope(&expected));
    assert_eq!(reopened.read_commit(receipt.commit_id).await?, expected);
    meter.assert_refunded();
    Ok(())
}

#[test]
fn prepared_partial_write_stops_at_exact_accepted_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    struct Partial {
        bytes: Vec<u8>,
    }
    impl Write for Partial {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let length = bytes.len().min(3);
            self.bytes.extend_from_slice(
                bytes
                    .get(..length)
                    .ok_or(std::io::ErrorKind::InvalidInput)?,
            );
            Ok(length)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let meter = Arc::new(Meter::local());
    let work = crate::native::Work::new(meter.original())?;
    let mut plan = PreparedFrame::encode(
        &WriteRequest::Commit(keyed_commit("partial", 0)?),
        0,
        Some(work),
    )?;
    let mut writer = Partial { bytes: Vec::new() };
    let physical = plan.write(&mut writer);
    assert!(matches!(physical, Err(LocalStreamError::IncompleteFrame)));
    assert_eq!(writer.bytes.len(), 3);
    assert_eq!(meter.write_bytes.load(Ordering::SeqCst), 3);
    drop(plan);
    meter.assert_refunded();
    Ok(())
}

#[tokio::test]
async fn abandoned_prepared_packet_releases_worker_capacity_without_edit()
-> Result<(), Box<dyn std::error::Error>> {
    let (schedule, mut boundaries) = acyclic_native_runtime::testing::Schedule::new();
    journal_lifecycle(&schedule, &mut boundaries, async {
        let directory = tempfile::tempdir()?;
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        let request = prepared_fixture(&provider).await?;
        let meter = Arc::new(Meter::local());
        let packet = provider
            .native_prepare_commit(request, 64, meter.original())
            .await??;
        let (packet, guard) = packet.into_parts();
        drop(packet);
        drop(guard);
        assert_eq!(provider.tail(StreamPath::new("prepared/target")?).await?, 1);
        assert_eq!(meter.write_operations.load(Ordering::SeqCst), 0);
        // Preparation may grow the provider's funded retained-entry buffer. Its
        // allocation belongs to the open provider, not the abandoned worker.
        let terminal = Arc::downgrade(&provider.inner);
        drop(provider);
        schedule.drained().await;
        assert!(
            terminal.upgrade().is_none(),
            "abandoned worker retained the provider"
        );
        assert_eq!(meter.write_operations.load(Ordering::SeqCst), 0);
        meter.assert_refunded();
        Ok(())
    })
    .await?
}

#[test]
fn prepared_reply_drop_keeps_accepted_durability_and_input_custody()
-> Result<(), Box<dyn std::error::Error>> {
    let _serial = PERSIST_BLOCKER_TESTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let (schedule, mut boundaries) = acyclic_native_runtime::testing::Schedule::new();
        journal_lifecycle(&schedule, &mut boundaries, async {
            let directory = tempfile::tempdir()?;
            let provider =
                LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
            let request = prepared_fixture(&provider).await?;
            let meter = Arc::new(Meter::local());
            let packet = provider
                .native_prepare_commit(request, 64, meter.original())
                .await??;
            let (packet, packet_guard) = packet.into_parts();
            let (started_send, started_receive) = tokio::sync::oneshot::channel();
            let (release_send, release_receive) = std::sync::mpsc::sync_channel(1);
            *JOURNAL_PERSIST_BLOCKER
                .lock()
                .map_err(|_| "poisoned fixture seam")? = Some((
                Arc::as_ptr(&provider.inner.journal) as usize,
                started_send,
                release_receive,
            ));
            let future = packet.finish((OwnedBytes::from_static(b"accepted-without-reply")).into());
            drop(future);
            drop(packet_guard);
            started_receive.await?;
            assert!(meter.held.load(Ordering::SeqCst) > 0);
            assert!(meter.write_bytes.load(Ordering::SeqCst) > 0);
            assert_eq!(meter.durability_operations.load(Ordering::SeqCst), 0);
            assert!(futures::poll!(std::pin::pin!(schedule.drained())).is_pending());
            release_send.send(())?;
            assert_eq!(provider.tail(StreamPath::new("prepared/target")?).await?, 2);
            drop(provider);
            schedule.drained().await;
            meter.assert_refunded();
            Ok(())
        })
        .await?
    })
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
    let error = provider
        .native_commit(request.clone(), None, meter.original())
        .await
        .err()
        .ok_or("cancelled accepted mutation fabricated an acknowledgement")?;
    assert_original_denial(&error, "cancelled");
    assert!(std::fs::metadata(directory.path().join(JOURNAL_FILE))?.len() > HEADER_BYTES as u64);
    let accepted = provider
        .inspect_idempotency(key.clone())
        .await?
        .ok_or("accepted original identity disappeared")?;
    drop(provider);
    let reopened = LocalStream::open(directory.path(), limits).await?;
    assert_eq!(
        reopened.inspect_idempotency(key).await?,
        Some(accepted.clone())
    );
    assert_eq!(reopened.tail(StreamPath::new("lost-native-ack")?).await?, 1);
    let crate::IdempotencyOutcome::Commit(outcome) = &accepted.outcome else {
        return Err("reopened an unrelated operation family".into());
    };
    let CommitOutcome::Committed(receipt) = reopened
        .commit(CommitRequest {
            deadline_unix_millis: Some(0),
            ..request
        })
        .await?
    else {
        return Err("replay changed canonical family".into());
    };
    assert!(receipt.matches_envelope(outcome));
    assert_eq!(&reopened.read_commit(receipt.commit_id).await?, outcome);
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
    let limits = LocalStreamLimits {
        compaction_operations: 2,
        ..LocalStreamLimits::default()
    };
    let provider = LocalStream::open(directory.path(), limits).await?;
    let request = keyed_commit("compact-native-ack", 0)?;
    let key = request.idempotency_key.clone();
    let meter = Arc::new(Meter::local());
    meter.cancel_on_durability.store(true, Ordering::SeqCst);
    let error = provider
        .native_commit(request.clone(), None, meter.original())
        .await
        .err()
        .ok_or("late cancelled compaction fabricated an acknowledgement")?;
    assert_original_denial(&error, "cancelled");
    assert_eq!(
        std::fs::metadata(directory.path().join(JOURNAL_FILE))?.len(),
        HEADER_BYTES as u64
    );
    assert!(directory.path().join(SNAPSHOT_FILE).is_file());
    assert!(!directory.path().join(SNAPSHOT_TEMPORARY).exists());
    let accepted = provider
        .inspect_idempotency(key.clone())
        .await?
        .ok_or("compaction erased original identity")?;
    drop(provider);
    let reopened = LocalStream::open(directory.path(), limits).await?;
    assert_eq!(
        reopened.inspect_idempotency(key).await?,
        Some(accepted.clone())
    );
    assert_eq!(
        reopened
            .tail(StreamPath::new("compact-native-ack")?)
            .await?,
        1
    );
    let crate::IdempotencyOutcome::Commit(outcome) = &accepted.outcome else {
        return Err("snapshot retained an unrelated operation".into());
    };
    let CommitOutcome::Committed(receipt) = reopened
        .commit(CommitRequest {
            deadline_unix_millis: Some(0),
            ..request
        })
        .await?
    else {
        return Err("replay changed canonical family".into());
    };
    assert!(receipt.matches_envelope(outcome));
    assert_eq!(&reopened.read_commit(receipt.commit_id).await?, outcome);
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
        directory.path(),
        LocalStreamLimits::default(),
        clock.clone(),
    )
    .await?;
    let request = keyed_commit("native-expired", 0)?;
    let journal_before = std::fs::read(directory.path().join(JOURNAL_FILE))?;
    let meter = Arc::new(Meter::local());
    *meter
        .expire_after_hash
        .lock()
        .map_err(|_| "native clock seam was poisoned")? = Some((clock.clone(), 100));
    let expired = provider
        .native_commit(request.clone(), Some(100), meter.original())
        .await?;
    assert!(matches!(
        expired.as_ref(),
        Err(StreamError::DeadlineElapsed)
    ));
    assert_eq!(clock.load(Ordering::SeqCst), 100);
    assert_eq!(
        std::fs::read(directory.path().join(JOURNAL_FILE))?,
        journal_before
    );
    assert_eq!(meter.durability_operations.load(Ordering::SeqCst), 0);
    drop(expired);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    meter.assert_refunded();
    let accepted = provider.commit(request.clone()).await?;
    let replay = provider
        .native_commit(request, Some(0), meter.original())
        .await??;
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
    let result = provider
        .native_commit_guarded(request, None, meter.original(), move || {
            let provider = callback_provider.clone();
            let meter = Arc::clone(&callback_meter);
            let path = callback_path.clone();
            async move {
                let tail = provider
                    .native_tail(path, meter.original())
                    .await
                    .map_err(NativeGuardFailure::Io)??;
                assert_eq!(*tail.value(), 1);
                drop(tail);
                meter.cancelled.store(true, Ordering::SeqCst);
                Err(NativeGuardFailure::Owner(0x51))
            }
        })
        .await?;
    assert!(matches!(result, Err(NativeGuardFailure::Owner(0x51))));
    assert!(provider.inspect_idempotency(key).await?.is_none());
    assert_eq!(std::fs::read(directory.path().join(JOURNAL_FILE))?, before);
    drop(provider);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    meter.assert_refunded();
    Ok(())
}

#[tokio::test]
async fn native_guard_returns_canonical_drift_before_an_explicit_new_request()
-> Result<(), Box<dyn std::error::Error>> {
    async fn check<P: crate::NativeStreamProvider + Clone + 'static>(
        provider: &P,
    ) -> Result<(IdempotencyKey, Arc<Meter>), Box<dyn std::error::Error>> {
        let request = keyed_commit("guard-drift-target", 0)?;
        let key = request.idempotency_key.clone();
        let meter = Arc::new(Meter::local());
        let calls = Arc::new(AtomicU64::new(0));
        let callback_provider = provider.clone();
        let callback_meter = Arc::clone(&meter);
        let callback_calls = Arc::clone(&calls);
        let before = move || {
            let provider = callback_provider.clone();
            let meter = Arc::clone(&callback_meter);
            let calls = Arc::clone(&callback_calls);
            async move {
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    let intervening = keyed_commit("guard-drift-other", 0)
                        .map_err(NativeGuardFailure::Provider)?;
                    let result = provider
                        .native_commit(intervening, None, meter.original())
                        .await
                        .map_err(NativeGuardFailure::Io)??;
                    assert!(matches!(result.value(), CommitOutcome::Committed(_)));
                    drop(result);
                    Ok(())
                } else {
                    Err(NativeGuardFailure::Owner(0x52))
                }
            }
        };
        let result = provider
            .native_commit_guarded(request.clone(), None, meter.original(), before.clone())
            .await?;
        assert!(matches!(
            result,
            Err(NativeGuardFailure::Provider(StreamError::PrefixNotRetained))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(provider.inspect_idempotency(key.clone()).await?.is_none());
        assert_eq!(
            provider.tail(StreamPath::new("guard-drift-target")?).await,
            Err(StreamError::NotFound)
        );
        assert_eq!(
            provider.tail(StreamPath::new("guard-drift-other")?).await?,
            1
        );
        let result = provider
            .native_commit_guarded(request.clone(), None, meter.original(), before)
            .await?;
        assert!(matches!(result, Err(NativeGuardFailure::Owner(0x52))));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(provider.inspect_idempotency(key.clone()).await?.is_none());
        let accepted = provider
            .native_commit(request.clone(), None, meter.original())
            .await??;
        assert!(matches!(accepted.value(), CommitOutcome::Committed(_)));
        let replay = provider
            .native_commit_guarded(request, None, meter.original(), || {
                std::future::ready(Err(NativeGuardFailure::Owner(0x53)))
            })
            .await??;
        assert_eq!(replay.value(), accepted.value());
        drop(replay);
        drop(accepted);
        assert_eq!(
            provider
                .tail(StreamPath::new("guard-drift-target")?)
                .await?,
            1
        );
        Ok((key, meter))
    }
    let memory = MemoryStream::default();
    let (_, memory_meter) = check(&memory).await?;
    drop(memory);
    memory_meter.assert_refunded();
    assert_eq!(memory_meter.held.load(Ordering::SeqCst), 0);
    let directory = tempfile::tempdir()?;
    let limits = LocalStreamLimits::default();
    let provider = LocalStream::open(directory.path(), limits).await?;
    let (key, meter) = check(&provider).await?;
    drop(provider);
    let reopened = LocalStream::open(directory.path(), limits).await?;
    assert!(reopened.inspect_idempotency(key).await?.is_some());
    assert_eq!(
        reopened.tail(StreamPath::new("guard-drift-other")?).await?,
        1
    );
    assert_eq!(
        reopened
            .tail(StreamPath::new("guard-drift-target")?)
            .await?,
        1
    );
    drop(reopened);
    meter.assert_refunded();
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn rejected_commits_do_not_grow_the_journal() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    let before = std::fs::metadata(directory.path().join(JOURNAL_FILE))?.len();
    let key = IdempotencyKey::new(b"stable-commit-intent".to_vec())?;
    for _ in 0..32 {
        assert!(matches!(
            provider
                .commit(CommitRequest {
                    deadline_unix_millis: None,
                    conditions: vec![crate::CommitCondition::Tail {
                        path: StreamPath::new("absent")?,
                        expected: 1
                    }],
                    mutations: vec![crate::CommitMutation::Append {
                        path: StreamPath::new("absent")?,
                        records: vec![OwnedBytes::from_static(b"intent").into()]
                    }],
                    idempotency_key: key.clone(),
                })
                .await?,
            CommitOutcome::Conflict(_)
        ));
    }
    assert!(provider.inspect_idempotency(key).await?.is_none());
    assert_eq!(
        std::fs::metadata(directory.path().join(JOURNAL_FILE))?.len(),
        before
    );
    Ok(())
}
