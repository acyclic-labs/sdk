//! Atomic follow preconditions and legacy-peer confirmation compatibility.
use acyclic_stream::{
    CommitId, StreamError, wire,
    wire_codec::session::{confirm_follow, preceding_follow},
};
use proptest::prelude::*;
use prost::Message;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    // A checked subscription preserves every anchor byte through the wire codec;
    // only an exact server confirmation can authorize granting data credit.
    #[test]
    fn checked_anchor_round_trip_and_confirmation(anchor in any::<[u8;32]>(), other in any::<[u8;32]>(), next in 1_u64..=u64::MAX) {
        let follow = wire::SessionFollow { path: "events".into(), delivery: wire::SessionDelivery::All as i32, preceding_commit_id: anchor.to_vec().into() };
        let decoded = wire::SessionFollow::decode(follow.encode_to_vec().as_slice())?;
        prop_assert_eq!(&decoded, &follow);
        let resume = wire::SessionCursor { next, ..Default::default() };
        let preceding = preceding_follow(&decoded, &resume)?;
        prop_assert_eq!(preceding, Some(CommitId::from_bytes(anchor)));
        let confirmed = wire::SessionOpened { resume: Some(resume.clone()), preceding_commit_id: anchor.to_vec().into() };
        prop_assert!(confirm_follow(preceding, &confirmed).is_ok());
        let opened = wire::SessionOpened { resume: Some(resume), preceding_commit_id: other.to_vec().into() };
        let decoded = wire::SessionOpened::decode(opened.encode_to_vec().as_slice())?;
        prop_assert_eq!(&decoded, &opened);
        prop_assert_eq!(confirm_follow(preceding, &decoded).is_ok(), anchor == other);
    }

    // Malformed nonempty IDs, missing predecessor positions and conflating
    // delivery cannot silently become an unchecked subscription.
    #[test]
    fn malformed_guards_are_rejected(length in 0_usize..65, next in 0_u64..3, latest in any::<bool>(), opaque in any::<bool>()) {
        let follow = wire::SessionFollow { path: "events".into(), delivery: if latest { wire::SessionDelivery::Latest } else { wire::SessionDelivery::All } as i32, preceding_commit_id: vec![7;length].into() };
        let resume = wire::SessionCursor { next, opaque: if opaque { vec![1].into() } else { Default::default() } };
        prop_assert_eq!(preceding_follow(&follow, &resume).is_ok(), length == 0 || length == 32 && next > 0 && !latest && !opaque);
    }
}

#[test]
fn an_older_peer_cannot_confirm_a_checked_follow() {
    let opened = wire::SessionOpened::default();
    assert_eq!(
        confirm_follow(Some(CommitId::from_bytes([7; 32])), &opened),
        Err(StreamError::Unsupported)
    );
    assert_eq!(confirm_follow(None, &opened), Ok(()));
}
