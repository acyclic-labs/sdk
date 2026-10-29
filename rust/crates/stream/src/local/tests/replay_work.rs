use super::super::*;
use proptest::prelude::*;

pub(in crate::local) fn request(body: Vec<u8>) -> CommitRequest {
    let path = StreamPath::new("replay-work").unwrap();
    CommitRequest {
        deadline_unix_millis: None,
        conditions: vec![crate::CommitCondition::Absent { path: path.clone() }],
        mutations: vec![crate::CommitMutation::Append {
            path,
            records: vec![body.into()],
        }],
        idempotency_key: IdempotencyKey::new("replay-work").unwrap(),
    }
}
fn unchanged_frames(before: u64, after: u64) -> proptest::test_runner::TestCaseResult {
    prop_assert_eq!(before, after, "replay appended another WAL frame");
    Ok(())
}
async fn replay<const ELIDE: bool>(count: usize, bytes: Vec<u8>) -> (u64, u64) {
    let root = tempfile::tempdir().unwrap();
    let stream = LocalStream::open(root.path(), LocalStreamLimits::default())
        .await
        .unwrap();
    let request = request(bytes);
    let first = stream.commit(request.clone()).await.unwrap();
    let before = stream.inner.required().unwrap();
    for _ in 0..count {
        assert_eq!(stream.commit(request.clone()).await.unwrap(), first);
    }
    if !ELIDE {
        let command = WriteRequest::Commit(request.clone());
        let span = obs::OperationSpan::new(tracing::info_span!("duplicate_replay_control"));
        stream
            .exclusive(&span, move |stream| async move {
                let frame = stream.prepare(&command)?;
                stream.persist(frame).await
            })
            .await
            .unwrap();
    }
    let after = stream.inner.required().unwrap();
    let mut mismatched = request.clone();
    if let Some(crate::CommitMutation::Append { records, .. }) = mismatched.mutations.first_mut() {
        records.push((Bytes::from_static(b"different")).into());
    }
    assert!(matches!(
        stream.commit(mismatched).await,
        Err(StreamError::IdempotencyMismatch)
    ));
    drop(stream);
    let reopened = LocalStream::open(root.path(), LocalStreamLimits::default())
        .await
        .unwrap();
    assert_eq!(reopened.commit(request).await.unwrap(), first);
    (before, after)
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(4))]
    #[test]
    fn replay_reuses_its_durable_frame_and_detects_the_old_write(
        count in 1usize..8, bytes in proptest::collection::vec(any::<u8>(), 0..32),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let (before, after) = runtime.block_on(replay::<true>(count, bytes.clone()));
        unchanged_frames(before, after)?;
        let (before, after) = runtime.block_on(replay::<false>(count, bytes));
        prop_assert!(unchanged_frames(before, after).is_err(), "replay-write control escaped the same bound");
    }
}

#[tokio::test]
async fn a_normal_replay_flushes_its_original_deferred_frame() {
    let root = tempfile::tempdir().unwrap();
    let stream = LocalStream::open(root.path(), LocalStreamLimits::default())
        .await
        .unwrap();
    let request = request(b"deferred".to_vec());
    let first = deferring_durability(stream.commit(request.clone()))
        .await
        .unwrap();
    assert_eq!(stream.inner.journal.lock().unwrap().synced, 0);
    assert_eq!(stream.commit(request).await.unwrap(), first);
    let journal = stream.inner.journal.lock().unwrap();
    assert_eq!(journal.appended, 1);
    assert_eq!(journal.synced, 1);
}

#[tokio::test]
async fn append_and_fork_retries_reuse_and_flush_their_original_frames()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let limits = LocalStreamLimits::default();
    let stream = LocalStream::open(root.path(), limits).await?;
    let append = super::keyed_append("retry-source", 0)?;
    let appended = deferring_durability(stream.append(append.clone())).await?;
    let journal_path = root.path().join(JOURNAL_FILE);
    let append_bytes = std::fs::metadata(&journal_path)?.len();
    assert_eq!(stream.inner.journal.lock().unwrap().synced, 0);
    assert_eq!(stream.append(append.clone()).await?, appended);
    assert_eq!(std::fs::metadata(&journal_path)?.len(), append_bytes);
    assert_eq!(stream.inner.journal.lock().unwrap().synced, 1);

    let fork = ForkRequest {
        exclude: Vec::new(),
        source: append.path.clone(),
        destination: StreamPath::new("retry-fork")?,
        at_tail: Some(1),
        idempotency_key: IdempotencyKey::new("retry-fork")?,
    };
    let forked = deferring_durability(stream.fork(fork.clone())).await?;
    let fork_bytes = std::fs::metadata(&journal_path)?.len();
    assert_eq!(stream.inner.journal.lock().unwrap().synced, 1);
    assert_eq!(stream.fork(fork.clone()).await?, forked);
    assert_eq!(std::fs::metadata(&journal_path)?.len(), fork_bytes);
    assert_eq!(stream.inner.journal.lock().unwrap().synced, 2);

    let mut changed_append = append.clone();
    changed_append
        .records
        .push(Bytes::from_static(b"changed").into());
    assert!(matches!(
        stream.append(changed_append).await,
        Err(StreamError::IdempotencyMismatch)
    ));
    let mut changed_fork = fork.clone();
    changed_fork.destination = StreamPath::new("different-fork")?;
    assert!(matches!(
        stream.fork(changed_fork).await,
        Err(StreamError::IdempotencyMismatch)
    ));
    assert_eq!(std::fs::metadata(&journal_path)?.len(), fork_bytes);

    drop(stream);
    let reopened = LocalStream::open(root.path(), limits).await?;
    let replayed_append = reopened.append(append).await?;
    let replayed_fork = reopened.fork(fork.clone()).await?;
    // A deferred run rotates its revision incarnation on reopen.
    let mut appended = appended;
    if let (AppendOutcome::Committed(original), AppendOutcome::Committed(replayed)) =
        (&mut appended, &replayed_append)
    {
        original.revision.clone_from(&replayed.revision);
    }
    let mut forked = forked;
    forked.revision.clone_from(&replayed_fork.revision);
    assert_eq!(replayed_append, appended);
    assert_eq!(replayed_fork, forked);
    assert_eq!(reopened.tail(fork.destination).await?, 1);
    assert_eq!(std::fs::metadata(&journal_path)?.len(), fork_bytes);
    Ok(())
}
