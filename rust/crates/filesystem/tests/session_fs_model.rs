//! Exhaustive checked change-stream cut and wake schedules. Policy/publication
//! races use the production observation models in `access::projection::model`.
#![cfg(acyclic_loom)]
use acyclic_stream::{verification, *};
use bytes::Bytes;
use futures::{FutureExt, StreamExt};
use std::{mem::ManuallyDrop, sync::Arc};
fn append(stream: &MemoryStream, path: &StreamPath, value: &'static [u8], from: u64) -> CommitId {
    let AppendOutcome::Committed(receipt) = verification::park(stream.append(AppendRequest {
        path: path.clone(),
        records: vec![Bytes::from_static(value).into()],
        if_tail: Some(from),
        idempotency_key: None,
    }))
    .unwrap() else {
        panic!("unexpected conflict")
    };
    receipt.commit_id
}
fn control(run: impl FnOnce(), expected: &str) {
    let error = std::panic::catch_unwind(std::panic::AssertUnwindSafe(run))
        .expect_err("broken model escaped");
    let text = error
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| error.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(text.contains(expected), "unexpected model failure: {text}");
}
fn resume(omit_wake: bool) {
    verification::check(move || {
        let path = StreamPath::new(".acyclic/fs/07070707070707070707070707070707")?;
        let stream = Arc::new(MemoryStream::default());
        let anchor = append(&stream, &path, b"accepted-root", 0);
        let keeper = ManuallyDrop::new(stream.clone());
        let writer = loom::thread::spawn({
            let stream = stream.clone();
            let path = path.clone();
            move || {
                if omit_wake {
                    verification::omit_append_wake();
                }
                append(&stream, &path, b"change", 1);
            }
        });
        verification::park(async {
            let mut follow = stream
                .follow_updates(path, 1, Some(anchor), FollowFilter::default())
                .await
                .unwrap();
            let FollowUpdate::Record(record) = follow.next().await.unwrap().unwrap() else {
                panic!("change lost")
            };
            assert_eq!(record.sequence, 1);
            assert_eq!(record.value.as_ref(), b"change");
            writer.join().unwrap();
            assert!(follow.next().now_or_never().is_none(), "duplicate change");
        });
        drop(ManuallyDrop::into_inner(keeper));
        Ok(())
    });
}
#[test]
fn change_resume_exhausts_append_registration_and_wakeup() {
    resume(false);
    control(|| resume(true), "deadlock");
}
fn replacement(unchecked: bool) {
    verification::check(move || {
        let path = StreamPath::new(".acyclic/fs/07070707070707070707070707070707")?;
        let stream = Arc::new(MemoryStream::default());
        let anchor = append(&stream, &path, b"original", 0);
        let keeper = ManuallyDrop::new(stream.clone());
        let replacement = MemoryStream::default();
        append(&replacement, &path, b"replacement", 0);
        append(&replacement, &path, b"later", 1);
        let writer = loom::thread::spawn({
            let stream = stream.clone();
            move || verification::park(verification::recover(&stream, &replacement)).unwrap()
        });
        verification::park(async {
            let mut follow = stream
                .follow_updates(
                    path,
                    1,
                    if unchecked { None } else { Some(anchor) },
                    FollowFilter::default(),
                )
                .await;
            writer.join().unwrap();
            let rejected = match &mut follow {
                Err(StreamError::PrefixNotRetained) => true,
                Ok(records) => matches!(
                    records.next().await,
                    Some(Err(StreamError::PrefixNotRetained))
                ),
                _ => false,
            };
            assert!(rejected, "history was not rejected");
        });
        drop(ManuallyDrop::into_inner(keeper));
        Ok(())
    });
}
#[test]
fn change_resume_exhausts_independent_history_replacement() {
    replacement(false);
    control(|| replacement(true), "history was not rejected");
}
