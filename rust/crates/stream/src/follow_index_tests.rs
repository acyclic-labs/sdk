use super::*;
use crate::{CommitId, RecordHeaders};
use acyclic_stream_wire::wire;
use futures::FutureExt as _;
#[test]
fn unrelated_publications_never_notify_filtered_interests() {
    let index = Arc::new(SyncMutex::new(FollowIndex::default()));
    let mut followed = FollowIndex::subscribe(
        &index,
        FollowFilter::new(wire::SessionFilter {
            all_of: vec![],
            keys: vec![wire::KeyRange {
                start: b"a".as_slice().into(),
                end: b"c".as_slice().into(),
            }],
            attributes: vec![],
        })
        .unwrap(),
        0,
    )
    .unwrap();
    let record = |key: &'static str, sequence| Record {
        sequence,
        value: "body".into(),
        headers: RecordHeaders::new(vec![key.into()], vec![]).unwrap(),
        commit_id: CommitId::default(),
        committed_at_micros: 0,
        origin: crate::StreamPath::new("origin").unwrap(),
    };
    FollowIndex::publish(&index, &[record("z", 0)], 1);
    assert!(followed.changed.changed().now_or_never().is_none());
    FollowIndex::publish(&index, &[record("b", 1)], 2);
    assert!(matches!(
        followed.changed.changed().now_or_never(),
        Some(Ok(()))
    ));
    drop(followed);
    assert!(
        index
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .entries
            .is_empty()
    );
    assert!(
        index
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .ranges
            .is_none()
    );
}
#[test]
fn interval_index_stays_balanced_and_matches_a_reference_after_removal() {
    let mut tree = None;
    for id in 0..4096_u64 {
        let start = Bytes::copy_from_slice(&id.to_be_bytes());
        let end = Bytes::copy_from_slice(&(id + 1).to_be_bytes());
        tree = Node::insert(tree, (start, id, 0), end);
    }
    assert!(Node::height(&tree) <= 14);
    for id in (0..4096_u64).step_by(2) {
        tree = Node::remove(tree, &(Bytes::copy_from_slice(&id.to_be_bytes()), id, 0));
    }
    for id in 0..4096_u64 {
        VISITS.with(|visits| visits.set(0));
        let mut matches = vec![];
        Node::query(&tree, &id.to_be_bytes(), &mut |id| matches.push(id));
        assert_eq!(matches, if id % 2 == 1 { vec![id] } else { vec![] });
        assert!(VISITS.with(std::cell::Cell::get) <= 30);
    }
    for id in (1..4096_u64).step_by(2) {
        tree = Node::remove(tree, &(Bytes::copy_from_slice(&id.to_be_bytes()), id, 0));
    }
    assert!(tree.is_none());
}
