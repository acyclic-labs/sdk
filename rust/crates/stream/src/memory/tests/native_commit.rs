use super::*;
use crate::{OwnedBytes, test_meter::Meter};
use std::sync::atomic::{AtomicU64, Ordering};
type MemoryResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn public_guarded_commit_binds_current_clock_and_keeps_exact_cached_replay_time()
-> MemoryResult<()> {
    let clock = Arc::new(AtomicU64::new(100));
    let provider = MemoryStream::new_with_clock(MemoryLimits::default(), clock.clone());
    for name in ["completion/owner", "completion/target"] {
        provider
            .append(AppendRequest {
                path: path(name)?,
                records: vec![(OwnedBytes::from_static(b"seed")).into()],
                if_tail: Some(0),
                idempotency_key: crate::IdempotencyKey::unique(),
            })
            .await?;
    }
    let request = CommitRequest {
        deadline_unix_millis: None,
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
            records: vec![(OwnedBytes::from_static(b"real-guarded-body")).into()],
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
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        }
    };
    let output = provider
        .native_commit_guarded(request.clone(), None, meter.original(), before)
        .await??;
    let CommitOutcome::Committed(envelope) = output.value() else {
        panic!("canonical Commit")
    };
    let retained = provider.read_commit(envelope.commit_id).await?;
    assert!(envelope.matches_envelope(&retained));
    assert_eq!(envelope.committed_at_micros, 125_000);
    let [CommittedMutation::Append(append)] = retained.mutations.as_slice() else {
        panic!("single append")
    };
    assert_eq!(
        append
            .records
            .first()
            .ok_or(StreamError::InvalidArgument)?
            .committed_at_micros,
        125_000
    );
    let mut rows = provider
        .read(ReadRequest {
            key: None,
            path: path("completion/target")?,
            from: 1,
            limit: 1,
            at_least: Default::default(),
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
            std::future::ready(Ok::<(), Box<dyn std::error::Error + Send + Sync>>(()))
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
async fn native_publication_projection_keeps_exact_frame_funding() -> MemoryResult<()> {
    fn capture<F>(build: impl FnOnce() -> F) -> (F, acyclic_test_support::AllocationInfo) {
        let mut future = None;
        let allocation = acyclic_test_support::measure(|| future = Some(build()));
        (future.unwrap(), allocation)
    }
    let provider = MemoryStream::default();
    let request = AppendRequest {
        path: path("native/projection")?,
        records: vec![OwnedBytes::from_static(b"opaque").into()],
        if_tail: Some(0),
        idempotency_key: IdempotencyKey::unique(),
    };
    let accepted = provider.append(request.clone()).await?;
    let mut measurements = Vec::new();
    for projected in [false, true] {
        let meter = Arc::new(Meter::default());
        let request = request.clone();
        let allocation = if projected {
            let (future, allocation) =
                capture(|| provider.native_append(request, meter.original()));
            assert_eq!(
                meter.allocations.load(Ordering::SeqCst),
                2,
                "projection deferred frame admission"
            );
            let output = future.await??;
            assert_eq!(output.value(), &accepted);
            drop(output);
            allocation
        } else {
            let (future, allocation) = capture(|| {
                provider.native_write(crate::WriteRequest::Append(request), None, meter.original())
            });
            assert_eq!(meter.allocations.load(Ordering::SeqCst), 2);
            let output = future.await??;
            assert!(
                matches!(output.value(), crate::WriteOutcome::Append(outcome) if outcome == &accepted)
            );
            drop(output);
            allocation
        };
        assert_eq!(allocation.count_total, 2);
        assert_eq!(meter.held.load(Ordering::SeqCst), 0);
        meter.assert_refunded();
        measurements.push((allocation.count_total, allocation.bytes_total));
    }
    assert_eq!(measurements[0], measurements[1]);
    // Reintroducing a boxed projection has identical records and native totals,
    // but adds a real frame outside the original admission ledger.
    let meter = Arc::new(Meter::default());
    let (future, allocation) =
        capture(|| Box::pin(provider.native_append(request, meter.original())));
    let admitted = meter.allocations.load(Ordering::SeqCst);
    assert_eq!(admitted, 2);
    assert_eq!(allocation.count_total, admitted + 1);
    assert_eq!(future.await??.value(), &accepted);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    meter.assert_refunded();
    Ok(())
}
