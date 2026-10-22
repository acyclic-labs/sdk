use super::*;
use crate::interest::Interests;

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
            records: vec![Bytes::from_static(b"old")],
            if_tail: Some(0),
            idempotency_key: None,
        }))
        .unwrap() else {
            panic!("seed conflicted")
        };
        let replacement = MemoryStream::default();
        block_on(replacement.append(AppendRequest {
            path: path.clone(),
            records: vec![
                Bytes::from_static(b"replacement"),
                Bytes::from_static(b"next"),
            ],
            if_tail: Some(0),
            idempotency_key: None,
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
                .prepare_follow(&path, 1, anchor, crate::FollowFilter::default())
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
