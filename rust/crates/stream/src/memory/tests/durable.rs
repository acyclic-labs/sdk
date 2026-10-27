use super::super::*;
use super::TestClock;
use crate::journal::{Command, Outcome};
use std::sync::atomic::{AtomicUsize, Ordering};

fn request(tail: u64) -> Result<AppendRequest, StreamError> {
    Ok(AppendRequest {
        path: StreamPath::new("durable/events")?,
        records: vec![Bytes::from_static(b"accepted").into()],
        if_tail: Some(tail),
        idempotency_key: Some(IdempotencyKey::new(b"durable-identity".to_vec())?),
    })
}

#[tokio::test]
async fn refusals_and_retries_never_enter_the_durable_boundary() -> Result<(), StreamError> {
    let core = MemoryStream::default();
    let publications = AtomicUsize::new(0);
    let persist = async || {
        publications.fetch_add(1, Ordering::SeqCst);
        Ok(())
    };
    let bad = request(1)?;
    assert!(matches!(
        core.apply_durable(Command::Append(bad.clone()), None, persist())
            .await?,
        Outcome::Append(AppendOutcome::TailConflict { actual_tail: 0 })
    ));
    assert_eq!(publications.load(Ordering::SeqCst), 0);
    assert!(
        core.inspect_idempotency(bad.idempotency_key.ok_or(StreamError::InvalidArgument)?)
            .await?
            .is_none()
    );
    let good = request(0)?;
    let Outcome::Append(receipt) = core
        .apply_durable(Command::Append(good.clone()), None, persist())
        .await?
    else {
        return Err(StreamError::InvalidResponse);
    };
    let Outcome::Append(replayed) = core
        .apply_durable(Command::Append(good), None, persist())
        .await?
    else {
        return Err(StreamError::InvalidResponse);
    };
    assert_eq!(receipt, replayed);
    assert_eq!(publications.load(Ordering::SeqCst), 1);
    let fork = ForkRequest {
        source: StreamPath::new("absent")?,
        destination: StreamPath::new("destination")?,
        at_tail: None,
        exclude: Vec::new(),
        idempotency_key: None,
    };
    assert!(matches!(
        core.apply_durable(Command::Fork(fork), None, persist())
            .await,
        Err(StreamError::NotFound)
    ));
    let commit = CommitRequest {
        conditions: vec![CommitCondition::Tail {
            path: request(0)?.path.clone(),
            expected: 0,
        }],
        mutations: vec![CommitMutation::Append {
            path: request(0)?.path,
            records: vec![Bytes::from_static(b"refused").into()],
        }],
        idempotency_key: IdempotencyKey::new(b"conflict-identity".to_vec())?,
    };
    assert!(matches!(
        core.apply_durable(Command::Commit(commit.clone()), None, persist())
            .await?,
        Outcome::Commit(CommitOutcome::Conflict(_))
    ));
    assert!(
        core.inspect_idempotency(commit.idempotency_key)
            .await?
            .is_none()
    );
    assert_eq!(publications.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn failed_or_cancelled_durability_never_installs_or_notifies() -> Result<(), StreamError> {
    let core = MemoryStream::default();
    let mut initial = request(0)?;
    initial.idempotency_key = None;
    core.append(initial).await?;
    let append = request(1)?;
    let mut follow = core
        .follow(append.path.clone(), 1, crate::FollowFilter::default())
        .await?;
    let refused = core
        .apply_durable(Command::Append(append.clone()), None, async {
            Err(StreamError::Unavailable)
        })
        .await;
    assert!(matches!(refused, Err(StreamError::Unavailable)));
    assert_eq!(core.tail(append.path.clone()).await?, 1);
    assert!(futures::poll!(follow.next()).is_pending());
    {
        let prepared = core.apply_durable(
            Command::Append(append.clone()),
            None,
            std::future::pending(),
        );
        let mut prepared = std::pin::pin!(prepared);
        assert!(futures::poll!(prepared.as_mut()).is_pending());
        let read = core.tail(append.path.clone());
        let mut read = std::pin::pin!(read);
        assert!(futures::poll!(read.as_mut()).is_pending());
    }
    assert!(
        core.inspect_idempotency(
            append
                .idempotency_key
                .clone()
                .ok_or(StreamError::InvalidArgument)?
        )
        .await?
        .is_none()
    );
    assert_eq!(core.tail(append.path.clone()).await?, 1);
    let outcome = core
        .apply_durable(Command::Append(append), None, async { Ok(()) })
        .await?;
    assert!(matches!(
        outcome,
        Outcome::Append(AppendOutcome::Committed(_))
    ));
    assert_eq!(
        follow
            .next()
            .await
            .ok_or(StreamError::Unavailable)??
            .sequence,
        1
    );
    Ok(())
}

#[tokio::test]
async fn accepted_time_binds_after_durability_without_a_second_deadline_refusal()
-> Result<(), StreamError> {
    let wall = Arc::new(TestClock::default());
    let accepted = Arc::new(TestClock::default());
    let core = MemoryStream::new_with_commit_clock(
        MemoryLimits::default(),
        wall.clone(),
        accepted.clone(),
    );
    let Outcome::Append(AppendOutcome::Committed(receipt)) = core
        .apply_durable(Command::Append(request(0)?), None, async {
            accepted.0.store(3, Ordering::SeqCst);
            Ok(())
        })
        .await?
    else {
        return Err(StreamError::InvalidResponse);
    };
    let envelope = core.read_commit(receipt.commit_id).await?;
    let CommittedMutation::Append(append) = &envelope.mutations[0] else {
        return Err(StreamError::InvalidResponse);
    };
    assert_eq!(append.records[0].committed_at_micros, 3_000);
    let request = CommitRequest {
        conditions: vec![CommitCondition::Tail {
            path: request(0)?.path.clone(),
            expected: 1,
        }],
        mutations: vec![CommitMutation::Append {
            path: request(0)?.path,
            records: vec![Bytes::from_static(b"durable").into()],
        }],
        idempotency_key: IdempotencyKey::new(b"accepted-before-deadline".to_vec())?,
    };
    // The transport admitted at 5 ms, then durability completed after 10 ms.
    // Its success must install the accepted decision, not refuse it afterward.
    let Outcome::Commit(CommitOutcome::Committed(envelope)) = core
        .apply_durable(Command::Commit(request), Some(10), async {
            accepted.0.store(5, Ordering::SeqCst);
            wall.0.store(20, Ordering::SeqCst);
            Ok(())
        })
        .await?
    else {
        return Err(StreamError::InvalidResponse);
    };
    let retained = core.read_commit(envelope.commit_id).await?;
    assert!(envelope.matches_envelope(&retained));
    assert_eq!(envelope.committed_at_micros, 5_000);
    let CommittedMutation::Append(append) = &retained.mutations[0] else {
        return Err(StreamError::InvalidResponse);
    };
    assert_eq!(append.records[0].committed_at_micros, 5_000);
    assert_eq!(core.tail(append.path.clone()).await?, 2);
    Ok(())
}
