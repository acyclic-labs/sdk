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
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
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
