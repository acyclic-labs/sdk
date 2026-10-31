use super::*;

#[cfg(any(
    all(feature = "agents", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
fn skip(next: u64) -> acyclic_stream::wire::SessionFrame {
    use acyclic_stream::wire;
    wire::SessionFrame {
        body: Some(wire::session_frame::Body::Watermark(
            wire::SessionWatermark {
                accepted: next,
                durable: 0,
                after: Some(wire::SessionCursor {
                    next,
                    opaque: Default::default(),
                }),
                preceding_commit_id: Default::default(),
            },
        )),
        ..Default::default()
    }
}

#[test]
#[cfg(any(
    all(feature = "agents", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
fn only_a_filtered_fold_advances_past_skipped_records() {
    let policy = StreamViewPolicy::default();
    let mut filtered = StreamView::new(policy, 2, 0, true, None, Default::default());
    assert!(filtered.apply(&skip(7).into(), 1).is_ok());
    assert_eq!(filtered.next_sequence(), 7);
    assert!(
        filtered.apply(&skip(5).into(), 2).is_err(),
        "a cut never rewinds"
    );
    let mut full = StreamView::new(policy, 2, 0, false, None, Default::default());
    assert!(full.apply(&skip(7).into(), 1).is_err());
    assert!(full.apply(&skip(2).into(), 1).is_ok());
}
