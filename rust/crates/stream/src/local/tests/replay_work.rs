use super::super::*;
use proptest::prelude::*;

pub(in crate::local) fn request(body: Vec<u8>) -> CommitRequest {
    let path = StreamPath::new("replay-work").unwrap();
    CommitRequest {
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
    let first = commit::<ELIDE>(&stream, request.clone(), None)
        .await
        .unwrap();
    let before = stream.inner.required().unwrap();
    for _ in 0..count {
        assert_eq!(
            commit::<ELIDE>(&stream, request.clone(), None)
                .await
                .unwrap(),
            first
        );
    }
    let after = stream.inner.required().unwrap();
    let mut mismatched = request.clone();
    if let crate::CommitMutation::Append { records, .. } = &mut mismatched.mutations[0] {
        records.push(Bytes::from_static(b"different"));
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
