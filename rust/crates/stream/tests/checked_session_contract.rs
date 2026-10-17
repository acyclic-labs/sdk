//! Atomic follow preconditions and legacy-peer confirmation compatibility.
use acyclic_stream::{
    CommitId, StreamError, wire,
    wire_codec::session::{confirm_follow, preceding_follow, record_anchor, record_cursor},
};
use proptest::prelude::*;
use prost::Message;
mod checks {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/support/follow_contract.rs"
    ));
}
use checks::{Fault, check};

fn tagged(kind: u8, value: &impl Message) -> Vec<u8> {
    [vec![kind], value.encode_to_vec()].concat()
}
fn contract_inputs() -> impl Strategy<Value = Vec<u8>> {
    let guards =
        (any::<[u8; 32]>(), 1_u64..=u64::MAX, 31_usize..34).prop_map(|(anchor, next, length)| {
            let mut guard = anchor.to_vec();
            guard.resize(length, 0);
            tagged(
                0,
                &wire::SessionSubscription {
                    resume: Some(wire::SessionCursor {
                        next,
                        ..Default::default()
                    }),
                    kind: Some(wire::session_subscription::Kind::Follow(
                        wire::SessionFollow {
                            path: "events".into(),
                            delivery: wire::SessionDelivery::All as i32,
                            preceding_commit_id: guard.into(),
                        },
                    )),
                    ..Default::default()
                },
            )
        });
    let snapshots = (any::<[u8; 32]>(), 1_u64..=u64::MAX).prop_map(|(anchor, next)| {
        tagged(
            1,
            &wire::SessionCursor {
                next,
                opaque: anchor.to_vec().into(),
            },
        )
    });
    let watermarks = (any::<u64>(), prop::option::of(any::<u64>()))
        .prop_map(|(tail, durable)| tagged(2, &wire::TailResponse { tail, durable }));
    prop_oneof![guards, snapshots, watermarks]
}

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

    // Snapshot cursors preserve the complete immutable cut through protobuf and
    // canonicalization; malformed cursors cannot be normalized into unchecked ones.
    #[test]
    fn snapshot_cursors_round_trip_without_losing_the_anchor(
        next in any::<u64>(), opaque in prop::collection::vec(any::<u8>(), 0..65),
    ) {
        let cursor = wire::SessionCursor { next, opaque: opaque.into() };
        let decoded = wire::SessionCursor::decode(cursor.encode_to_vec().as_slice())?;
        prop_assert_eq!(&decoded, &cursor);
        let expected = match (next, cursor.opaque.as_ref().try_into()) {
            (0, _) if cursor.opaque.is_empty() => Ok(None),
            (1.., Ok(anchor)) => Ok(Some(anchor)),
            _ => Err(StreamError::InvalidArgument),
        };
        prop_assert_eq!(record_anchor(&decoded).map(|id| id.map(|id| *id.as_bytes())), expected);
    }

    #[test]
    fn every_nonzero_snapshot_cut_reencodes_exactly(next in 1_u64..=u64::MAX, anchor in any::<[u8;32]>()) {
        let cursor = record_cursor(next, CommitId::from_bytes(anchor))?;
        prop_assert_eq!(cursor.opaque.as_ref(), &anchor);
        let decoded = wire::SessionCursor::decode(cursor.encode_to_vec().as_slice())?;
        prop_assert_eq!(decoded.opaque.as_ref(), &anchor);
        prop_assert_eq!(record_anchor(&decoded).map(|id| id.map(|id| *id.as_bytes())), Ok(Some(anchor)));
        prop_assert_eq!(decoded.next, next);
        prop_assert_eq!(record_cursor(0, CommitId::from_bytes(anchor)), Err(StreamError::InvalidArgument));
    }

    #[test]
    fn the_fuzz_driver_preserves_the_independent_wire_contract(input in contract_inputs()) {
        check(&input, Fault::None).map_err(TestCaseError::fail)?;
    }

    // Malformed nonempty IDs, missing predecessor positions and conflating
    // delivery cannot silently become an unchecked subscription.
    #[test]
    fn malformed_guards_are_rejected(length in 0_usize..65, next in 0_u64..3, latest in any::<bool>(), opaque in any::<bool>()) {
        let follow = wire::SessionFollow { path: "events".into(), delivery: if latest { wire::SessionDelivery::Latest } else { wire::SessionDelivery::All } as i32, preceding_commit_id: vec![7;length].into() };
        let resume = wire::SessionCursor { next, opaque: if opaque { vec![1].into() } else { Default::default() } };
        let expected = if length == 0 {
            Ok(None)
        } else if length == 32 && next > 0 && !latest && !opaque {
            Ok(Some(CommitId::from_bytes([7; 32])))
        } else {
            Err(StreamError::InvalidArgument)
        };
        prop_assert_eq!(preceding_follow(&follow, &resume), expected);
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

fn reject_mutant(fault: Fault, invariant: &str) {
    use proptest::test_runner::{Config, RngSeed, TestError, TestRunner};
    let mut runner = TestRunner::new(Config {
        cases: 128,
        rng_seed: RngSeed::Fixed(705),
        failure_persistence: None,
        ..Default::default()
    });
    let result = runner.run(&contract_inputs(), |input| {
        check(&input, fault).map_err(TestCaseError::fail)
    });
    assert!(
        matches!(result, Err(TestError::Fail(..))),
        "mutant {fault:?} escaped the shared property/fuzz driver"
    );
    if let Err(TestError::Fail(reason, input)) = result {
        assert_eq!(reason.to_string(), invariant);
        assert_eq!(
            check(&input, Fault::None),
            Ok(()),
            "shrunk counterexample must pass the production codecs"
        );
        assert_eq!(check(&input, fault), Err(invariant));
    }
}

#[test]
fn negative_control_accepts_a_31_byte_guard() {
    reject_mutant(Fault::AcceptShortGuard, "checked guard");
}
#[test]
fn negative_control_drops_checked_state() {
    reject_mutant(Fault::DropChecked, "checked guard");
}
#[test]
fn negative_control_accepts_every_nonempty_confirmation() {
    reject_mutant(Fault::AcceptAnyConfirmation, "confirmation anchor");
}
#[test]
fn negative_control_reverses_snapshot_encoding() {
    reject_mutant(Fault::ReverseEncode, "snapshot encode bytes");
}
#[test]
fn negative_control_reverses_snapshot_decoding() {
    reject_mutant(Fault::ReverseDecode, "snapshot decode bytes");
}
#[test]
fn negative_control_reverses_both_snapshot_codecs() {
    reject_mutant(Fault::ReverseBoth, "snapshot decode bytes");
}
#[test]
fn negative_control_infers_durability_from_acceptance() {
    reject_mutant(Fault::InferDurability, "durability boundary");
}

#[test]
fn negative_control_drops_snapshot_checked_state() {
    reject_mutant(Fault::DropSnapshotAnchor, "snapshot decode bytes");
}

#[test]
fn watermark_corpus_seeds_reach_their_named_outcomes() {
    use acyclic_stream::{StreamWatermarks, wire_codec::watermarks_from_wire};
    for (seed, expected) in [
        (
            include_bytes!("../fuzz/corpus/follow_contract/watermarks-impossible").as_slice(),
            Err(StreamError::Unavailable),
        ),
        (
            include_bytes!("../fuzz/corpus/follow_contract/watermarks-held-flush").as_slice(),
            Ok(StreamWatermarks {
                accepted: u64::MAX,
                durable: 0,
            }),
        ),
        (
            include_bytes!("../fuzz/corpus/follow_contract/watermarks-legacy").as_slice(),
            Err(StreamError::Unsupported),
        ),
    ] {
        let (&selector, bytes) = seed.split_first().unwrap();
        assert_eq!(selector, 2);
        assert_eq!(
            watermarks_from_wire(wire::TailResponse::decode(bytes).unwrap()),
            expected
        );
        assert_eq!(check(seed, Fault::None), Ok(()));
    }
}
