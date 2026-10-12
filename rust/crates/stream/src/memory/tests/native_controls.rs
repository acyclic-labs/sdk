use super::*;
use std::sync::atomic::AtomicBool;

use crate::test_meter::Meter;

async fn seeded() -> MemoryResult<MemoryStream> {
    let provider = MemoryStream::default();
    provider
        .append(AppendRequest {
            path: path("native/read")?,
            records: vec![
                OwnedBytes::from_static(b"aa"),
                OwnedBytes::from_static(b"bb"),
                OwnedBytes::from_static(b"cc"),
            ],
            if_tail: Some(0),
            idempotency_key: None,
        })
        .await?;
    Ok(provider)
}

async fn prepared_fixture(limits: MemoryLimits) -> MemoryResult<(MemoryStream, CommitRequest)> {
    let provider = MemoryStream::new(limits);
    for name in ["completion/owner", "completion/target"] {
        provider
            .append(AppendRequest {
                path: path(name)?,
                records: vec![OwnedBytes::from_static(b"seed")],
                if_tail: Some(0),
                idempotency_key: None,
            })
            .await?;
    }
    let request = CommitRequest {
        conditions: vec![
            CommitCondition::Tail {
                path: path("completion/owner")?,
                expected: 1,
            },
            CommitCondition::Tail {
                path: path("completion/target")?,
                expected: 1,
            },
        ],
        mutations: vec![CommitMutation::Append {
            path: path("completion/target")?,
            records: vec![OwnedBytes::new()],
        }],
        idempotency_key: key(b"prepared-completion")?,
    };
    Ok((provider, request))
}

fn fill_prepared_fixture(request: &mut CommitRequest, body: &'static [u8]) {
    let CommitMutation::Append { records, .. } = &mut request.mutations[0] else {
        unreachable!()
    };
    records[0] = OwnedBytes::from_static(body);
}

#[tokio::test]
async fn guarded_cas_drops_contended_continuation_before_wait_and_rechecks_original_guard()
-> MemoryResult<()> {
    let (provider, mut request) = prepared_fixture(MemoryLimits::default()).await?;
    fill_prepared_fixture(&mut request, b"contention-safe");
    let meter = Arc::new(Meter::default());
    let original = meter.original();
    let observer = Some(&original);
    let calls = Arc::new(AtomicU64::new(0));
    let mut before = || {
        let state = Arc::clone(&provider.state);
        let calls = Arc::clone(&calls);
        async move {
            let read = state.read_owned().await;
            let held = if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                Some(read)
            } else {
                drop(read);
                None
            };
            Ok::<_, MemoryFailure>(held)
        }
    };
    let (completion, _, continuation) = provider
        .apply_commit(&mut request, None, observer, Some(&mut before), None)
        .await??;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(matches!(continuation, Some(None)));
    assert!(matches!(completion.value, CommitOutcome::Committed(_)));
    assert_eq!(provider.tail(path("completion/target")?).await?, 2);
    Ok(())
}

#[tokio::test]
async fn public_guarded_commit_binds_current_clock_and_keeps_exact_cached_replay_time()
-> MemoryResult<()> {
    let clock = Arc::new(AtomicU64::new(100));
    let provider = MemoryStream::new_with_clock(MemoryLimits::default(), clock.clone());
    for name in ["completion/owner", "completion/target"] {
        provider
            .append(AppendRequest {
                path: path(name)?,
                records: vec![OwnedBytes::from_static(b"seed")],
                if_tail: Some(0),
                idempotency_key: None,
            })
            .await?;
    }
    let request = CommitRequest {
        conditions: vec![
            CommitCondition::Tail {
                path: path("completion/owner")?,
                expected: 1,
            },
            CommitCondition::Tail {
                path: path("completion/target")?,
                expected: 1,
            },
        ],
        mutations: vec![CommitMutation::Append {
            path: path("completion/target")?,
            records: vec![OwnedBytes::from_static(b"real-guarded-body")],
        }],
        idempotency_key: key(b"guarded-clock")?,
    };
    let meter = Arc::new(Meter::default());
    let guarded_provider = provider.clone();
    let guarded_meter = Arc::clone(&meter);
    let guarded_clock = Arc::clone(&clock);
    let before = move || {
        let provider = guarded_provider.clone();
        let meter = Arc::clone(&guarded_meter);
        let clock = Arc::clone(&guarded_clock);
        async move {
            let tail = provider
                .native_tail(path("completion/target")?, meter.original())
                .await??;
            assert_eq!(*tail.value(), 1);
            clock.store(125, Ordering::SeqCst);
            Ok::<(), MemoryFailure>(())
        }
    };
    let output = provider
        .native_commit_guarded(request.clone(), None, meter.original(), before)
        .await??;
    let CommitOutcome::Committed(envelope) = output.value() else {
        panic!("canonical Commit")
    };
    let [CommittedMutation::Append(append)] = envelope.mutations.as_slice() else {
        panic!("single append")
    };
    assert_eq!(append.records[0].committed_at_micros, 125_000);
    let retained = provider.read_commit(envelope.commit_id).await?;
    assert_eq!(&retained, envelope);
    let mut rows = provider
        .read(ReadRequest {
            path: path("completion/target")?,
            from: 1,
            limit: 1,
        })
        .await?;
    assert_eq!(
        rows.next()
            .await
            .ok_or(StreamError::Unavailable)??
            .committed_at_micros,
        125_000
    );
    let replay_clock = Arc::clone(&clock);
    let replay = provider
        .native_commit_guarded(request, None, meter.original(), move || {
            replay_clock.store(999, Ordering::SeqCst);
            std::future::ready(Ok::<(), MemoryFailure>(()))
        })
        .await??;
    assert_eq!(
        replay.value(),
        output.value(),
        "guarded replay preserves original record time"
    );
    assert_eq!(
        provider.state.read().await.last_committed_at_micros,
        125_000
    );
    Ok(())
}

#[tokio::test]
async fn public_prepared_future_keeps_original_payload_through_the_last_empty_alias()
-> MemoryResult<()> {
    struct PayloadGuard {
        retired: Arc<AtomicBool>,
        _allocation: ReadAllocation,
    }
    impl Drop for PayloadGuard {
        fn drop(&mut self) {
            self.retired.store(true, Ordering::SeqCst);
        }
    }
    let (provider, request) = prepared_fixture(MemoryLimits::default()).await?;
    let meter = Arc::new(Meter::default());
    let original = meter.original();
    let retired = Arc::new(AtomicBool::new(false));
    let mut allocation = ReadAllocation::new(&original, 64)?;
    let mut data = Vec::new();
    data.try_reserve_exact(64)
        .map_err(|error| original.allocation_failed(error))?;
    allocation.reconcile_capacity(data.capacity())?;
    data.resize(data.capacity(), 0);
    let mut builder = crate::PreparedOwnedBytes::new(
        data,
        PayloadGuard {
            retired: Arc::clone(&retired),
            _allocation: allocation,
        },
        Arc::clone(&original),
    )
    .map_err(|failure| match failure.into_error() {
        crate::PreparedOwnedBytesError::Admission(error) => MemoryFailure::Observer(error),
        crate::PreparedOwnedBytesError::NotFullyInitialized => {
            MemoryFailure::Provider(StreamError::InvalidArgument)
        }
    })?;
    let issued = provider
        .native_prepare_commit(request, 64, original)
        .await??;
    let (mut issued, packet_guard) = issued.into_parts();
    let recovery = issued
        .take_recovery_future()
        .ok_or(StreamError::Unavailable)?;
    assert!(issued.take_recovery_future().is_none());
    let allocations = meter.allocations.load(Ordering::SeqCst);
    builder.writable()[..9].copy_from_slice(b"root-body");
    meter.cancelled.store(true, Ordering::SeqCst);
    let body = builder
        .freeze(9)
        .map_err(|_| StreamError::InvalidArgument)?;
    let last_empty_alias = body.slice(..0);
    let output = issued.finish(body).await??;
    let CommitOutcome::Committed(envelope) = output.value() else {
        panic!("canonical Commit result")
    };
    let [CommittedMutation::Append(append)] = envelope.mutations.as_slice() else {
        panic!("single append")
    };
    assert_eq!(append.records[0].value.as_ref(), b"root-body");
    assert_eq!(meter.allocations.load(Ordering::SeqCst), allocations);
    assert_eq!(meter.pending_write.load(Ordering::SeqCst), 0);
    assert_eq!(meter.pending_write_operations.load(Ordering::SeqCst), 0);
    let observed = recovery.await??;
    let observation = observed.value().as_ref().ok_or(StreamError::Unavailable)?;
    let IdempotencyOutcome::Commit(recovered) = &observation.outcome else {
        panic!("original Commit family")
    };
    assert_eq!(recovered, output.value());
    assert_eq!(meter.allocations.load(Ordering::SeqCst), allocations);
    assert_eq!(meter.pending_read.load(Ordering::SeqCst), 0);
    assert_eq!(meter.pending_read_operations.load(Ordering::SeqCst), 0);
    drop(observed);
    assert!(!retired.load(Ordering::SeqCst));
    drop(output);
    drop(packet_guard);
    drop(provider);
    assert!(last_empty_alias.is_empty());
    assert!(
        !retired.load(Ordering::SeqCst),
        "the empty view still owns its full physical backing"
    );
    assert!(meter.held.load(Ordering::SeqCst) > 0);
    drop(last_empty_alias);
    assert!(retired.load(Ordering::SeqCst));
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn prebuilt_recovery_rejects_actual_foreign_family_and_larger_commit_without_growth()
-> MemoryResult<()> {
    for larger_commit in [false, true] {
        let (provider, request) = prepared_fixture(MemoryLimits::default()).await?;
        let foreign_key = request.idempotency_key.clone();
        let meter = Arc::new(Meter::default());
        let issued = provider
            .native_prepare_commit(request, 64, meter.original())
            .await??;
        let (mut issued, packet_guard) = issued.into_parts();
        let recovery = issued
            .take_recovery_future()
            .ok_or(StreamError::Unavailable)?;
        let published = if larger_commit {
            let outcome = provider
                .commit(CommitRequest {
                    // Each real Append mutation needs its own current-tail guard.
                    conditions: vec![
                        CommitCondition::Tail {
                            path: path("completion/owner")?,
                            expected: 1,
                        },
                        CommitCondition::Tail {
                            path: path("completion/target")?,
                            expected: 1,
                        },
                    ],
                    mutations: vec![
                        CommitMutation::Append {
                            path: path("completion/owner")?,
                            records: vec![OwnedBytes::from_static(b"foreign-owner")],
                        },
                        CommitMutation::Append {
                            path: path("completion/target")?,
                            records: vec![OwnedBytes::from_static(b"foreign-target")],
                        },
                    ],
                    idempotency_key: foreign_key.clone(),
                })
                .await?;
            let CommitOutcome::Committed(envelope) = &outcome else {
                panic!("the larger foreign Commit must actually publish")
            };
            assert_eq!(envelope.mutations.len(), 2);
            assert_eq!(provider.tail(path("completion/owner")?).await?, 2);
            IdempotencyOutcome::Commit(outcome)
        } else {
            let outcome = provider
                .append(AppendRequest {
                    path: path("completion/target")?,
                    records: vec![OwnedBytes::from_static(b"foreign-append")],
                    if_tail: Some(1),
                    idempotency_key: Some(foreign_key.clone()),
                })
                .await?;
            assert!(matches!(&outcome, AppendOutcome::Committed(_)));
            IdempotencyOutcome::Append(outcome)
        };
        assert_eq!(provider.tail(path("completion/target")?).await?, 2);
        let retained = provider
            .inspect_idempotency(foreign_key)
            .await?
            .ok_or(StreamError::Unavailable)?;
        assert_eq!(retained.outcome, published);
        drop(retained);
        drop(published);
        let allocations = meter.allocations.load(Ordering::SeqCst);
        meter.cancelled.store(true, Ordering::SeqCst);
        let failure = match recovery.await? {
            Err(failure) => failure,
            Ok(_) => panic!("a closed recovery plan cannot pretend this foreign outcome is absent"),
        };
        assert!(if larger_commit {
            matches!(&failure, StreamError::LimitExceeded)
        } else {
            matches!(&failure, StreamError::IdempotencyMismatch)
        });
        assert_eq!(meter.allocations.load(Ordering::SeqCst), allocations);
        assert_eq!(meter.pending_read.load(Ordering::SeqCst), 0);
        assert_eq!(meter.pending_read_operations.load(Ordering::SeqCst), 0);
        drop(failure);
        drop(issued);
        drop(packet_guard);
        assert_eq!(meter.pending_write.load(Ordering::SeqCst), 0);
        assert_eq!(meter.pending_write_operations.load(Ordering::SeqCst), 0);
        drop(provider);
        assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    }
    Ok(())
}

#[tokio::test]
async fn preaccepted_completion_survives_cancellation_and_lost_ack_with_exact_commit_replay()
-> MemoryResult<()> {
    let (provider, mut request) = prepared_fixture(MemoryLimits::default()).await?;
    let mut repeat = request.clone();
    let mut mismatch = request.clone();
    let meter = Arc::new(Meter {
        late_write_error: true,
        ..Meter::default()
    });
    let prepared = provider
        .prepare_commit(&mut request, 64, meter.original())
        .await??;
    let repeat_prepared = provider
        .prepare_commit(&mut repeat, 64, meter.original())
        .await??;
    let mismatch_prepared = provider
        .prepare_commit(&mut mismatch, 64, meter.original())
        .await??;
    let allocation_count = meter.allocations.load(Ordering::SeqCst);
    let input_pointer = request.mutations.as_ptr();
    fill_prepared_fixture(&mut request, b"actual-scalars");
    fill_prepared_fixture(&mut repeat, b"actual-scalars");
    fill_prepared_fixture(&mut mismatch, b"different-scalars");
    meter.cancelled.store(true, Ordering::SeqCst);
    let completion = provider.finish_commit(
        &mut *provider.state.write().await,
        prepared,
        &mut request,
        None,
    )??;
    assert!(
        matches!(completion.1.as_ref(), Some(error) if error.kind() == std::io::ErrorKind::Interrupted)
    );
    let CommitOutcome::Committed(envelope) = &completion.0.value else {
        panic!("real canonical publication")
    };
    let [CommittedMutation::Append(append)] = envelope.mutations.as_slice() else {
        panic!("single append")
    };
    assert_eq!(
        append.records[0].value,
        OwnedBytes::from_static(b"actual-scalars")
    );
    assert_eq!(append.start, 1);
    assert_eq!(append.end, 2);
    assert_eq!(input_pointer, request.mutations.as_ptr());
    let replay = provider.finish_commit(
        &mut *provider.state.write().await,
        repeat_prepared,
        &mut repeat,
        None,
    )??;
    assert_eq!(&replay.0.value, &completion.0.value);
    assert!(matches!(
        provider.finish_commit(
            &mut *provider.state.write().await,
            mismatch_prepared,
            &mut mismatch,
            None,
        )?,
        Err(StreamError::IdempotencyMismatch)
    ));
    assert_eq!(meter.allocations.load(Ordering::SeqCst), allocation_count);
    assert_eq!(
        provider
            .state
            .read()
            .await
            .replays
            .get(request.idempotency_key.as_bytes())
            .ok_or(StreamError::Unavailable)?
            .digest,
        commit_digest(&request, None, None)?
    );
    assert_eq!(meter.pending_write.load(Ordering::SeqCst), 0);
    assert_eq!(meter.pending_write_operations.load(Ordering::SeqCst), 0);
    drop(replay);
    drop(completion);
    assert!(
        meter.held.load(Ordering::SeqCst) > 0,
        "published controls still own original custody"
    );
    drop(provider);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn prepared_completion_competing_tail_keeps_input_and_canonical_conflict() -> MemoryResult<()>
{
    let (provider, mut request) = prepared_fixture(MemoryLimits::default()).await?;
    let meter = Arc::new(Meter::default());
    let prepared = provider
        .prepare_commit(&mut request, 64, meter.original())
        .await??;
    provider
        .append(AppendRequest {
            path: path("completion/owner")?,
            records: vec![OwnedBytes::from_static(b"competing")],
            if_tail: Some(1),
            idempotency_key: None,
        })
        .await?;
    fill_prepared_fixture(&mut request, b"settled-prefix");
    let input = request.clone();
    let allocations = meter.allocations.load(Ordering::SeqCst);
    meter.cancelled.store(true, Ordering::SeqCst);
    let completion = provider.finish_commit(
        &mut *provider.state.write().await,
        prepared,
        &mut request,
        None,
    )??;
    let CommitOutcome::Conflict(conflicts) = &completion.0.value else {
        panic!("truthful conflict")
    };
    assert!(matches!(
        conflicts.as_slice(),
        [CommitConflict::Tail {
            expected: 1,
            actual: Some(2),
            ..
        }]
    ));
    assert_eq!(request, input);
    assert_eq!(provider.tail(path("completion/target")?).await?, 1);
    assert_eq!(provider.commit(request).await?, completion.0.value);
    assert_eq!(meter.allocations.load(Ordering::SeqCst), allocations);
    Ok(())
}

#[tokio::test]
async fn unrelated_stock_mutations_cannot_consume_preaccepted_resident_capacity() -> MemoryResult<()>
{
    let limits = MemoryLimits {
        commits: 3,
        records: 3,
        payload_bytes: 72,
        idempotency_results: 1,
        ..MemoryLimits::default()
    };
    let (provider, mut request) = prepared_fixture(limits).await?;
    let meter = Arc::new(Meter::default());
    let prepared = provider
        .prepare_commit(&mut request, 64, meter.original())
        .await??;
    assert!(matches!(
        provider
            .append(AppendRequest {
                path: path("unrelated")?,
                records: vec![OwnedBytes::new()],
                if_tail: Some(0),
                idempotency_key: None,
            })
            .await,
        Err(StreamError::Capacity)
    ));
    assert!(matches!(
        provider
            .fork(ForkRequest {
                source: path("completion/owner")?,
                destination: path("unrelated")?,
                at_tail: Some(1),
                idempotency_key: None,
            })
            .await,
        Err(StreamError::Capacity)
    ));
    let unrelated = CommitRequest {
        conditions: vec![CommitCondition::Absent {
            path: path("unrelated")?,
        }],
        mutations: vec![CommitMutation::Append {
            path: path("unrelated")?,
            records: vec![OwnedBytes::new()],
        }],
        idempotency_key: key(b"unrelated")?,
    };
    assert!(matches!(
        provider.commit(unrelated).await,
        Err(StreamError::Capacity)
    ));
    fill_prepared_fixture(&mut request, b"settled");
    meter.cancelled.store(true, Ordering::SeqCst);
    let completion = provider.finish_commit(
        &mut *provider.state.write().await,
        prepared,
        &mut request,
        None,
    )??;
    assert!(matches!(&completion.0.value, CommitOutcome::Committed(_)));
    assert_eq!(provider.state.read().await.commits.len(), 3);
    Ok(())
}

fn read_request() -> Result<ReadRequest, StreamError> {
    Ok(ReadRequest {
        path: path("native/read")?,
        from: 0,
        limit: 3,
    })
}

#[tokio::test]
async fn partial_budget_and_cancellation_keep_the_exact_ordered_prefix() -> MemoryResult<()> {
    let provider = seeded().await?;
    for cancellation in [false, true] {
        let meter = Arc::new(Meter {
            read_limit: if cancellation { u64::MAX } else { 2 },
            cancel_after_read: if cancellation { 2 } else { u64::MAX },
            ..Meter::default()
        });
        let mut output = provider
            .native_read(read_request()?, meter.original())
            .await??;
        let first = output
            .value_mut()
            .next()
            .await
            .ok_or(StreamError::Unavailable)???;
        assert_eq!(first.value().sequence, 0);
        assert_eq!(first.value().value, OwnedBytes::from_static(b"aa"));
        let alias = first.value().value.clone();
        drop(first);
        let error = output
            .value_mut()
            .next()
            .await
            .ok_or(StreamError::Unavailable)?;
        assert!(
            matches!(error, Err(error) if error.kind() == if cancellation {
            std::io::ErrorKind::Interrupted
        } else { std::io::ErrorKind::OutOfMemory })
        );
        assert!(output.value_mut().next().await.is_none());
        assert_eq!(meter.read.load(Ordering::SeqCst), 2);
        assert_eq!(meter.pending_read.load(Ordering::SeqCst), 0);
        assert_eq!(meter.copied.load(Ordering::SeqCst), 0);
        assert!(meter.held.load(Ordering::SeqCst) > 0);
        drop(output);
        assert_eq!(meter.held.load(Ordering::SeqCst), 0);
        assert_eq!(alias, OwnedBytes::from_static(b"aa"));
    }
    Ok(())
}
