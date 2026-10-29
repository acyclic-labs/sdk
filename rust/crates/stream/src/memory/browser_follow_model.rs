use super::*;
use crate::interest::Interests;
use futures::StreamExt as _;

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    loom::future::block_on(Box::pin(future))
}

fn check<const OMIT_ANCHOR: bool>() {
    // Two actors and one replacement bound the state; every interleaving runs.
    // Provider state and publication locks are disjoint. Interest.next releases
    // its lock before entering the provider; the publication shim checks this.
    loom::model(|| {
        let provider = MemoryStream::default();
        let path = StreamPath::new("model").unwrap();
        let AppendOutcome::Committed(receipt) = block_on(provider.append(AppendRequest {
            path: path.clone(),
            records: vec![Bytes::from_static(b"old").into()],
            if_tail: Some(0),
            idempotency_key: crate::IdempotencyKey::unique(),
        }))
        .unwrap() else {
            panic!("seed conflicted")
        };
        let replacement = MemoryStream::default();
        block_on(replacement.append(AppendRequest {
            path: path.clone(),
            records: vec![
                Bytes::from_static(b"replacement").into(),
                Bytes::from_static(b"next").into(),
            ],
            if_tail: Some(0),
            idempotency_key: crate::IdempotencyKey::unique(),
        }))
        .unwrap();
        let encoded = block_on(replacement.encode_state());
        let interests = Interests::new();
        let mut changed = interests.subscribe(path.clone()).unwrap();
        let writer = loom::thread::spawn({
            let provider = provider.clone();
            let path = path.clone();
            move || {
                block_on(provider.install_state(&encoded)).unwrap();
                interests.notify(&path).unwrap();
            }
        });
        let result = block_on(async {
            let mut anchor = if OMIT_ANCHOR {
                None
            } else {
                Some(receipt.commit_id)
            };
            let (_, mut cursor) = provider
                .prepare_follow(
                    &path,
                    1,
                    anchor,
                    crate::WhenAbsent::NotFound,
                    crate::FollowFilter::default(),
                )
                .await?;
            changed
                .next(async || {
                    provider
                        .next_checked_record(
                            &path,
                            &mut cursor,
                            &mut anchor,
                            &crate::FollowFilter::default(),
                        )
                        .await
                })
                .await
        });
        writer.join().unwrap();
        assert!(
            matches!(result, Err(StreamError::PrefixNotRetained)),
            "replaced snapshot was spliced into new history: {result:?}"
        );
    });
}

/// Exhausts checked cursor preparation and publication against history replacement.
pub fn rejects_replaced_history_without_lost_wakeup() {
    check::<false>();
}

/// Runs the same checker with its immutable-history anchor deliberately omitted.
pub fn negative_control_omitting_the_anchor_is_detected() {
    let failure = std::panic::catch_unwind(check::<true>)
        .expect_err("the broken follow unexpectedly rejected replaced history");
    let message = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains("replaced snapshot was spliced into new history"),
        "unexpected model failure: {message}"
    );
}

fn creation<const WAIT: bool>() {
    loom::model(|| {
        let provider = MemoryStream::default();
        let path = StreamPath::new("created/during-registration").unwrap();
        let writer = loom::thread::spawn({
            let provider = provider.clone();
            let path = path.clone();
            move || {
                block_on(provider.append(AppendRequest {
                    path,
                    records: vec!["created".into(), "appended".into()],
                    if_tail: Some(0),
                    idempotency_key: crate::IdempotencyKey::unique(),
                }))
                .unwrap();
            }
        });
        let result = block_on(async {
            let mut request = crate::FollowRequest::new(path, 0);
            if WAIT {
                request.when_absent = crate::WhenAbsent::Wait;
            }
            let mut follow = provider.follow(request).await?;
            let mut records = Vec::new();
            for _ in 0..2 {
                let Some(crate::FollowUpdate::Record(record)) = follow.next().await.transpose()?
                else {
                    return Err(StreamError::InvalidResponse);
                };
                records.push((record.sequence, record.value));
            }
            Ok::<_, StreamError>(records)
        });
        writer.join().unwrap();
        assert_eq!(
            result,
            Ok(vec![
                (0, Bytes::from_static(b"created")),
                (1, Bytes::from_static(b"appended")),
            ]),
            "registration lost creation and its append"
        );
    });
}

/// Schedules creation plus append against the real missing-follow registration.
pub fn creation_during_registration_retains_the_requested_cursor() {
    creation::<true>();
}

/// The former NotFound path fails the same creation/registration schedules.
pub fn negative_control_refusing_initial_absence_is_detected() {
    let failure = std::panic::catch_unwind(creation::<false>)
        .expect_err("the old absence refusal satisfied a waiting follow");
    let message = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains("registration lost creation and its append"),
        "unexpected model failure: {message}"
    );
}
