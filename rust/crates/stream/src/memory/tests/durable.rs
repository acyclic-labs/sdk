use super::super::*;
use super::TestClock;
use crate::{WriteOutcome, WriteRequest};
use futures::TryFutureExt as _;
use std::sync::atomic::{AtomicUsize, Ordering};

async fn apply(
    core: &MemoryStream,
    command: WriteRequest,
    deadline: Option<u64>,
    persist: impl Future<Output = Result<(), StreamError>>,
) -> Result<WriteOutcome, StreamError> {
    let canonical = core.canonicalize(command)?;
    core.apply_durable(&canonical, deadline, persist)
        .await
        .map(|execution| execution.outcome)
}

fn request(tail: u64) -> Result<AppendRequest, StreamError> {
    Ok(AppendRequest {
        path: StreamPath::new("durable/events")?,
        records: vec![Bytes::from_static(b"accepted").into()],
        if_tail: Some(tail),
        idempotency_key: IdempotencyKey::new(b"durable-identity".to_vec())?,
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
        apply(&core, WriteRequest::Append(bad.clone()), None, persist()).await?,
        WriteOutcome::Append(AppendOutcome::TailConflict { actual_tail: 0 })
    ));
    assert_eq!(publications.load(Ordering::SeqCst), 0);
    assert!(
        core.inspect_idempotency(bad.idempotency_key.clone())
            .await?
            .is_none()
    );
    let good = request(0)?;
    let WriteOutcome::Append(receipt) =
        apply(&core, WriteRequest::Append(good.clone()), None, persist()).await?
    else {
        return Err(StreamError::InvalidResponse);
    };
    let WriteOutcome::Append(replayed) =
        apply(&core, WriteRequest::Append(good), None, persist()).await?
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
        idempotency_key: crate::IdempotencyKey::unique(),
    };
    assert!(matches!(
        apply(&core, WriteRequest::Fork(fork), None, persist()).await,
        Err(StreamError::NotFound)
    ));
    let commit = CommitRequest {
        deadline_unix_millis: None,
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
        apply(&core, WriteRequest::Commit(commit.clone()), None, persist()).await?,
        WriteOutcome::Commit(CommitOutcome::Conflict(_))
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
    initial.idempotency_key = IdempotencyKey::new(b"durable-initial".to_vec())?;
    core.append(initial).await?;
    let append = request(1)?;
    let mut follow = core
        .follow(crate::FollowRequest::new(append.path.clone(), 1))
        .map_ok(crate::records_only)
        .await?;
    let refused = apply(&core, WriteRequest::Append(append.clone()), None, async {
        Err(StreamError::Unavailable)
    })
    .await;
    assert!(matches!(refused, Err(StreamError::Unavailable)));
    assert_eq!(core.tail(append.path.clone()).await?, 1);
    assert!(futures::poll!(follow.next()).is_pending());
    {
        let command = WriteRequest::Append(append.clone());
        let prepared = apply(&core, command, None, std::future::pending());
        let mut prepared = std::pin::pin!(prepared);
        assert!(futures::poll!(prepared.as_mut()).is_pending());
        let read = core.tail(append.path.clone());
        let mut read = std::pin::pin!(read);
        assert!(futures::poll!(read.as_mut()).is_pending());
    }
    assert!(
        core.inspect_idempotency(append.idempotency_key.clone())
            .await?
            .is_none()
    );
    assert_eq!(core.tail(append.path.clone()).await?, 1);
    let outcome = apply(&core, WriteRequest::Append(append), None, async { Ok(()) }).await?;
    assert!(matches!(
        outcome,
        WriteOutcome::Append(AppendOutcome::Committed(_))
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
async fn durability_completion_cannot_change_the_stamp_or_refuse_an_accepted_deadline()
-> Result<(), StreamError> {
    let wall = Arc::new(TestClock::default());
    let accepted = Arc::new(TestClock::default());
    let core = MemoryStream::new_with_commit_clock(
        MemoryLimits::default(),
        wall.clone(),
        accepted.clone(),
    );
    accepted.0.store(3, Ordering::SeqCst);
    let WriteOutcome::Append(AppendOutcome::Committed(receipt)) =
        apply(&core, WriteRequest::Append(request(0)?), None, async {
            accepted.0.store(4, Ordering::SeqCst);
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
        deadline_unix_millis: Some(10),
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
    // The journal prepared its stamp at 5 ms, then durability completed after 10 ms.
    // Its success must install the accepted decision, not refuse it afterward.
    accepted.0.store(5, Ordering::SeqCst);
    let WriteOutcome::Commit(CommitOutcome::Committed(envelope)) =
        apply(&core, WriteRequest::Commit(request), None, async {
            accepted.0.store(9, Ordering::SeqCst);
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

#[tokio::test]
async fn funded_publications_need_no_allocation_admission_after_durability()
-> Result<(), Box<dyn std::error::Error>> {
    for family in 0..3 {
        let meter = Arc::new(crate::test_meter::Meter::default());
        let work = Work::new(meter.original())?;
        work.reserve_write(8)?;
        let core = MemoryStream::default();
        let source = request(0)?;
        core.append(source.clone()).await?;
        let destination = StreamPath::new("new/parents/destination")?;
        let command = match family {
            0 => WriteRequest::Append(AppendRequest {
                path: destination.clone(),
                idempotency_key: IdempotencyKey::unique(),
                ..request(0)?
            }),
            1 => WriteRequest::Fork(ForkRequest {
                source: source.path,
                destination: destination.clone(),
                at_tail: Some(1),
                exclude: Vec::new(),
                idempotency_key: IdempotencyKey::unique(),
            }),
            _ => WriteRequest::Commit(CommitRequest {
                deadline_unix_millis: None,
                conditions: Vec::new(),
                mutations: vec![CommitMutation::Append {
                    path: destination.clone(),
                    records: vec![Bytes::from_static(b"accepted").into()],
                }],
                idempotency_key: IdempotencyKey::unique(),
            }),
        };
        let core = core.with_context(work);
        apply(&core, command, None, async {
            // A completed durable action owns its completion. New allocation
            // admission must not determine whether its state can be installed.
            meter.deny_allocation.store(true, Ordering::SeqCst);
            Ok(())
        })
        .await?;
        assert_eq!(core.tail(destination).await?, 1);
    }
    Ok(())
}
