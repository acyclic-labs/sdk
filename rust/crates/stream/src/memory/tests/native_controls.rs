use super::*;
use std::sync::atomic::AtomicBool;

use crate::{OwnedBytes, test_meter::Meter};
use acyclic_native_runtime::ReadAllocation;
use std::sync::atomic::Ordering;
type MemoryResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn descending_replay_keys_keep_copy_work_and_lookup_depth_bounded() -> MemoryResult<()> {
    let provider = MemoryStream::default();
    let meter = Arc::new(Meter::default());
    let mut small = 0;
    for index in 0..256_u64 {
        provider
            .native_append(
                AppendRequest {
                    path: path("replay/growth")?,
                    records: vec![(OwnedBytes::from_static(b"value")).into()],
                    if_tail: Some(index),
                    idempotency_key: Some(IdempotencyKey::new(OwnedBytes::from(
                        (255 - index).to_be_bytes().to_vec(),
                    ))?),
                },
                meter.original(),
            )
            .await??;
        if index == 63 {
            small = meter.copied.load(Ordering::SeqCst);
        }
    }
    let copied = meter.copied.load(Ordering::SeqCst);
    assert!(small > 0);
    assert!(copied <= small * 5, "copy work: {small} -> {copied}");
    let examined = meter.examined.load(Ordering::SeqCst);
    assert!(examined > 256);
    assert!(examined <= 256 * (6 * u64::from(257_u16.ilog2()) + 2));
    let retry = AppendRequest {
        path: path("replay/growth")?,
        records: vec![(OwnedBytes::from_static(b"value")).into()],
        if_tail: Some(0),
        idempotency_key: Some(IdempotencyKey::new(OwnedBytes::from(
            255_u64.to_be_bytes().to_vec(),
        ))?),
    };
    let denied = Arc::new(Meter::default());
    denied.examination_limit.store(0, Ordering::SeqCst);
    assert!(
        provider
            .native_append(retry.clone(), denied.original())
            .await
            .is_err()
    );
    let retry_meter = Arc::new(Meter::default());
    assert!(
        matches!(provider.native_append(retry, retry_meter.original()).await??.value(),
        AppendOutcome::Committed(receipt) if receipt.start == 0 && receipt.end == 1)
    );
    assert!(retry_meter.examined.load(Ordering::SeqCst) > 0);
    assert!(retry_meter.examined.load(Ordering::SeqCst) <= 4 * u64::from(257_u16.ilog2()));
    assert_eq!(provider.tail(path("replay/growth")?).await?, 256);
    let insertion = Arc::new(Meter::default());
    let conflict = provider
        .native_append(
            AppendRequest {
                path: path("replay/growth")?,
                records: vec![(OwnedBytes::from_static(b"value")).into()],
                if_tail: Some(0),
                idempotency_key: Some(IdempotencyKey::new(OwnedBytes::from(
                    256_u64.to_be_bytes().to_vec(),
                ))?),
            },
            insertion.original(),
        )
        .await??;
    assert!(matches!(
        conflict.value(),
        AppendOutcome::TailConflict { actual_tail: 256 }
    ));
    // Both keys follow the same rightmost search path; only the new key is inserted.
    assert!(
        insertion.examined.load(Ordering::SeqCst) > retry_meter.examined.load(Ordering::SeqCst)
    );
    for index in 0..256_u64 {
        let observed = provider
            .inspect_idempotency(IdempotencyKey::new(OwnedBytes::from(
                index.to_be_bytes().to_vec(),
            ))?)
            .await?
            .ok_or(StreamError::Unavailable)?;
        assert!(matches!(observed.outcome,
            IdempotencyOutcome::Append(AppendOutcome::Committed(receipt))
            if receipt.start == 255 - index && receipt.end == 256 - index));
    }
    drop(provider);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn prepared_replays_share_reserved_inventory_through_interleaved_growth() -> MemoryResult<()>
{
    let (provider, request) = prepared_fixture(MemoryLimits::default()).await?;
    let meter = Arc::new(Meter::default());
    let mut pending = Vec::new();
    for index in 0..16 {
        let mut request = request.clone();
        request.idempotency_key =
            IdempotencyKey::new(OwnedBytes::from(format!("prepared/{index}")))?;
        let key = request.idempotency_key.clone();
        let (mut issued, guard) = provider
            .native_prepare_commit(request, 64, meter.original())
            .await??
            .into_parts();
        drop(issued.take_recovery_future());
        pending.push((key, issued, guard));
    }
    for index in 0..64 {
        provider
            .native_append(
                AppendRequest {
                    path: path("other")?,
                    records: vec![(OwnedBytes::from_static(b"other")).into()],
                    if_tail: Some(index),
                    idempotency_key: Some(IdempotencyKey::new(OwnedBytes::from(format!(
                        "other/{index}"
                    )))?),
                },
                meter.original(),
            )
            .await??;
    }
    let allocations = meter.allocations.load(Ordering::SeqCst);
    let examined = meter.examined.load(Ordering::SeqCst);
    let withheld = meter.pending_examination.load(Ordering::SeqCst);
    assert!(withheld > 0);
    meter
        .examination_limit
        .store(examined + withheld, Ordering::SeqCst);
    assert!(meter.original().examine(withheld + 1, 0).is_err());
    meter.deny_allocation.store(true, Ordering::SeqCst);
    meter.cancelled.store(true, Ordering::SeqCst);
    for (index, (key, issued, _guard)) in pending.into_iter().enumerate() {
        let output = issued
            .finish((OwnedBytes::from_static(b"body")).into())
            .await??;
        assert_eq!(
            matches!(output.value(), CommitOutcome::Committed(_)),
            index == 0
        );
        let cached = provider
            .inspect_idempotency(key)
            .await?
            .ok_or(StreamError::Unavailable)?;
        assert_eq!(
            cached.outcome,
            IdempotencyOutcome::Commit(output.value().clone())
        );
    }
    assert_eq!(provider.tail(path("completion/target")?).await?, 2);
    assert!(meter.examined.load(Ordering::SeqCst) > examined);
    assert!(meter.examined.load(Ordering::SeqCst) <= examined + withheld);
    assert_eq!(meter.allocations.load(Ordering::SeqCst), allocations);
    meter.assert_refunded();
    drop(provider);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn prepared_replay_probe_credit_is_required_before_acceptance() -> MemoryResult<()> {
    let (provider, request) = prepared_fixture(MemoryLimits::default()).await?;
    let meter = Arc::new(Meter::default());
    meter.examination_limit.store(1, Ordering::SeqCst);
    assert!(
        provider
            .native_prepare_commit(request, 64, meter.original())
            .await
            .is_err()
    );
    assert_eq!(provider.tail(path("completion/target")?).await?, 1);
    meter.assert_refunded();
    Ok(())
}

async fn prepared_fixture(limits: MemoryLimits) -> MemoryResult<(MemoryStream, CommitRequest)> {
    let provider = MemoryStream::new(limits);
    for name in ["completion/owner", "completion/target"] {
        provider
            .append(AppendRequest {
                path: path(name)?,
                records: vec![(OwnedBytes::from_static(b"seed")).into()],
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
            records: vec![(OwnedBytes::new()).into()],
        }],
        idempotency_key: key(b"prepared-completion")?,
    };
    Ok((provider, request))
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
        &original,
    )
    .map_err(|failure| match failure.into_error() {
        crate::PreparedOwnedBytesError::Admission(error) => {
            Box::<dyn std::error::Error + Send + Sync>::from(error)
        }
        crate::PreparedOwnedBytesError::NotFullyInitialized => {
            Box::<dyn std::error::Error + Send + Sync>::from(StreamError::InvalidArgument)
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
    builder
        .writable()
        .get_mut(..9)
        .ok_or(StreamError::InvalidArgument)?
        .copy_from_slice(b"root-body");
    meter.cancelled.store(true, Ordering::SeqCst);
    let body = builder
        .freeze(9)
        .map_err(|_| StreamError::InvalidArgument)?;
    let last_empty_alias = body.slice(..0);
    let output = issued.finish((body).into()).await??;
    let CommitOutcome::Committed(envelope) = output.value() else {
        panic!("canonical Commit result")
    };
    let [CommittedMutation::Append(append)] = envelope.mutations.as_slice() else {
        panic!("single append")
    };
    assert_eq!(
        append
            .records
            .first()
            .ok_or(StreamError::InvalidArgument)?
            .value
            .as_ref(),
        b"root-body"
    );
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
                            records: vec![(OwnedBytes::from_static(b"foreign-owner")).into()],
                        },
                        CommitMutation::Append {
                            path: path("completion/target")?,
                            records: vec![(OwnedBytes::from_static(b"foreign-target")).into()],
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
                    records: vec![(OwnedBytes::from_static(b"foreign-append")).into()],
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
        let Err(failure) = recovery.await? else {
            return Err(
                "a closed recovery plan cannot pretend this foreign outcome is absent".into(),
            );
        };
        assert!(if larger_commit {
            matches!(&failure, StreamError::LimitExceeded)
        } else {
            matches!(&failure, StreamError::IdempotencyMismatch)
        });
        assert_eq!(meter.allocations.load(Ordering::SeqCst), allocations);
        assert_eq!(meter.pending_read.load(Ordering::SeqCst), 0);
        assert_eq!(meter.pending_read_operations.load(Ordering::SeqCst), 0);
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
async fn preaccepted_completion_survives_cancellation_and_lost_ack_with_exact_replay()
-> MemoryResult<()> {
    let (provider, request) = prepared_fixture(MemoryLimits::default()).await?;
    let key = request.idempotency_key.clone();
    let meter = Arc::new(Meter {
        late_write_error: true,
        ..Meter::default()
    });
    let issued = provider
        .native_prepare_commit(request.clone(), 64, meter.original())
        .await??;
    let replay = provider
        .native_prepare_commit(request.clone(), 64, meter.original())
        .await??;
    let mismatch = provider
        .native_prepare_commit(request, 64, meter.original())
        .await??;
    let allocations = meter.allocations.load(Ordering::SeqCst);
    meter.cancelled.store(true, Ordering::SeqCst);
    let failed = issued
        .into_parts()
        .0
        .finish((OwnedBytes::from_static(b"actual-scalars")).into())
        .await;
    assert!(matches!(failed, Err(error) if error.kind() == std::io::ErrorKind::Interrupted));
    let accepted = provider
        .inspect_idempotency(key)
        .await?
        .ok_or(StreamError::Unavailable)?;
    let IdempotencyOutcome::Commit(CommitOutcome::Committed(envelope)) = &accepted.outcome else {
        panic!("canonical Commit family");
    };
    let [CommittedMutation::Append(append)] = envelope.mutations.as_slice() else {
        panic!("one append");
    };
    assert_eq!(
        append
            .records
            .first()
            .ok_or(StreamError::InvalidArgument)?
            .value
            .as_ref(),
        b"actual-scalars"
    );
    assert_eq!((append.start, append.end), (1, 2));
    assert_eq!(provider.read_commit(envelope.commit_id).await?, *envelope);
    let repeated = replay
        .into_parts()
        .0
        .finish((OwnedBytes::from_static(b"actual-scalars")).into())
        .await??;
    assert_eq!(
        IdempotencyOutcome::Commit(repeated.value().clone()),
        accepted.outcome
    );
    let (mut mismatch, mismatch_guard) = mismatch.into_parts();
    let recovery = mismatch
        .take_recovery_future()
        .ok_or(StreamError::Unavailable)?;
    let failed = mismatch
        .finish((OwnedBytes::from_static(b"different-scalars")).into())
        .await?
        .err();
    assert_eq!(failed, Some(StreamError::IdempotencyMismatch));
    assert_eq!(meter.pending_write.load(Ordering::SeqCst), 0);
    assert_eq!(meter.pending_write_operations.load(Ordering::SeqCst), 0);
    assert_eq!(
        recovery
            .await??
            .value()
            .as_ref()
            .map(|value| &value.outcome),
        Some(&accepted.outcome)
    );
    drop(mismatch_guard);
    assert_eq!(meter.allocations.load(Ordering::SeqCst), allocations);
    meter.assert_refunded();
    drop(repeated);
    drop(provider);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn competing_tail_keeps_canonical_conflict_and_target_unchanged() -> MemoryResult<()> {
    let (provider, request) = prepared_fixture(MemoryLimits::default()).await?;
    let key = request.idempotency_key.clone();
    let meter = Arc::new(Meter::default());
    let issued = provider
        .native_prepare_commit(request, 64, meter.original())
        .await??;
    provider
        .append(AppendRequest {
            path: path("completion/owner")?,
            records: vec![(OwnedBytes::from_static(b"competing")).into()],
            if_tail: Some(1),
            idempotency_key: None,
        })
        .await?;
    let allocations = meter.allocations.load(Ordering::SeqCst);
    meter.cancelled.store(true, Ordering::SeqCst);
    let completion = issued
        .into_parts()
        .0
        .finish((OwnedBytes::from_static(b"settled-prefix")).into())
        .await??;
    let CommitOutcome::Conflict(conflicts) = completion.value() else {
        panic!("canonical conflict");
    };
    assert!(matches!(
        conflicts.as_slice(),
        [CommitConflict::Tail {
            expected: 1,
            actual: Some(2),
            ..
        }]
    ));
    assert_eq!(provider.tail(path("completion/target")?).await?, 1);
    let cached = provider
        .inspect_idempotency(key)
        .await?
        .ok_or(StreamError::Unavailable)?;
    assert_eq!(
        cached.outcome,
        IdempotencyOutcome::Commit(completion.value().clone())
    );
    assert_eq!(meter.allocations.load(Ordering::SeqCst), allocations);
    drop(completion);
    drop(provider);
    meter.assert_refunded();
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn stock_mutations_cannot_consume_promised_capacity_and_drop_releases_it() -> MemoryResult<()>
{
    let limits = MemoryLimits {
        commits: 3,
        records: 3,
        payload_bytes: 72,
        idempotency_results: 1,
        ..MemoryLimits::default()
    };
    for abandon in [false, true] {
        let (provider, request) = prepared_fixture(limits).await?;
        let meter = Arc::new(Meter::default());
        let issued = provider
            .native_prepare_commit(request, 64, meter.original())
            .await??;
        let append = AppendRequest {
            path: path("unrelated")?,
            records: vec![(OwnedBytes::new()).into()],
            if_tail: Some(0),
            idempotency_key: None,
        };
        let fork = ForkRequest {
            source: path("completion/owner")?,
            destination: path("unrelated")?,
            at_tail: Some(1),
            idempotency_key: None,
        };
        let commit = CommitRequest {
            conditions: vec![CommitCondition::Absent {
                path: path("unrelated")?,
            }],
            mutations: vec![CommitMutation::Append {
                path: path("unrelated")?,
                records: vec![(OwnedBytes::new()).into()],
            }],
            idempotency_key: key(b"unrelated")?,
        };
        assert_eq!(
            provider.append(append.clone()).await,
            Err(StreamError::Capacity)
        );
        assert_eq!(provider.fork(fork).await, Err(StreamError::Capacity));
        assert_eq!(provider.commit(commit).await, Err(StreamError::Capacity));
        if abandon {
            drop(issued);
            assert!(matches!(
                provider.append(append).await?,
                AppendOutcome::Committed(_)
            ));
        } else {
            meter.cancelled.store(true, Ordering::SeqCst);
            let completion = issued
                .into_parts()
                .0
                .finish((OwnedBytes::from_static(b"settled")).into())
                .await??;
            assert!(matches!(completion.value(), CommitOutcome::Committed(_)));
            assert_eq!(provider.append(append).await, Err(StreamError::Capacity));
        }
        drop(provider);
        meter.assert_refunded();
        assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    }
    Ok(())
}
