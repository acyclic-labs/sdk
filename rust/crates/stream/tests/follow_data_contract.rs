//! ALL/LATEST wire admission has an independent shrinking and fuzz oracle.
use acyclic_stream::{StreamError, wire, wire_codec::session::follow_sequence};
use bytes::Bytes;
use proptest::prelude::*;
use prost::Message;
use wire::session_data::Item;
mod checks {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/support/follow_data.rs"
    ));
}
use checks::{Fault, check};

fn inputs() -> impl Strategy<Value = (i32, u64, wire::SessionData)> {
    (
        0_u8..12,
        0_u64..16,
        1_u64..16,
        any::<[u8; 32]>(),
        any::<bool>(),
    )
        .prop_map(|(case, next, jump, anchor, attention)| {
            let all = case % 2 == 0;
            let after = next + if all { 1 } else { jump };
            let mut data = wire::SessionData {
                after: Some(wire::SessionCursor {
                    next: after,
                    ..Default::default()
                }),
                accepted: after + 2,
                durable: after,
                item: Some(if all {
                    Item::Record(wire::Record {
                        sequence: next,
                        commit_id: anchor.to_vec().into(),
                        ..Default::default()
                    })
                } else {
                    Item::LatestValue(wire::SessionLatestValue {
                        key: Bytes::from_static(b"agent"),
                        value: anchor.to_vec().into(),
                        attention,
                    })
                }),
            };
            match case {
                2 => {
                    if let Some(Item::Record(record)) = &mut data.item {
                        record.sequence += 1;
                        if let Some(after) = data.after.as_mut() {
                            after.next += 1;
                        }
                    }
                }
                3 => {
                    if let Some(Item::LatestValue(value)) = &mut data.item {
                        value.key.clear();
                    }
                }
                4 | 5 => data.durable = data.accepted + 1,
                6 | 7 => {
                    if let Some(after) = data.after.as_mut() {
                        after.opaque = vec![1].into();
                    }
                }
                8 => {
                    if let Some(Item::Record(record)) = &mut data.item {
                        record.commit_id = anchor[..31].to_vec().into();
                    }
                }
                9 => return (1, next, data),
                10 | 11 => data.accepted = after - 1,
                _ => {}
            }
            (if all { 1 } else { 2 }, next, data)
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]
    #[test]
    fn admission_matches_the_independent_wire_contract((delivery, next, data) in inputs()) {
        prop_assert_eq!(wire::SessionData::decode(data.encode_to_vec().as_slice()).ok(), Some(data.clone()));
        check(delivery, next, &data, Fault::None).map_err(TestCaseError::fail)?;
        checks::exercise(next, 2, &data.encode_to_vec(), false);
    }
    #[test]
    fn untrusted_positions_and_watermarks_obey_the_same_contract(
        delivery in -1_i32..5, next in any::<u64>(), after in any::<u64>(),
        sequence in any::<u64>(), accepted in any::<u64>(), durable in any::<u64>(), latest in any::<bool>(),
    ) {
        let data = wire::SessionData { after: Some(wire::SessionCursor { next: after, ..Default::default() }), accepted, durable,
            item: Some(if latest { Item::LatestValue(wire::SessionLatestValue { key: vec![1].into(), ..Default::default() }) }
                else { Item::Record(wire::Record { sequence, commit_id: vec![7;32].into(), ..Default::default() }) }) };
        check(delivery, next, &data, Fault::None).map_err(TestCaseError::fail)?;
    }
}

#[test]
fn negative_controls_are_shrunk_by_the_same_property_and_fuzz_driver() {
    use proptest::test_runner::{Config, RngSeed, TestError, TestRunner};
    for fault in [
        Fault::StrictLatest,
        Fault::SkipAll,
        Fault::EmptyKey,
        Fault::Watermark,
        Fault::Opaque,
        Fault::Commit,
        Fault::WrongMode,
    ] {
        let mut runner = TestRunner::new(Config {
            cases: 256,
            rng_seed: RngSeed::Fixed(710),
            failure_persistence: None,
            ..Default::default()
        });
        let result = runner.run(&inputs(), |(delivery, next, data)| {
            check(delivery, next, &data, fault).map_err(TestCaseError::fail)
        });
        let Err(TestError::Fail(reason, (delivery, next, data))) = result else {
            panic!("mutant {fault:?} escaped the admission oracle");
        };
        assert_eq!(reason.to_string(), "follow data contract");
        assert_eq!(check(delivery, next, &data, Fault::None), Ok(()));
        assert_eq!(
            check(delivery, next, &data, fault),
            Err("follow data contract")
        );
    }
}

#[test]
fn corpus_delivery_seeds_reach_their_named_outcomes() {
    for (case, (seed, delivery, next, expected)) in [
        (
            include_bytes!("../fuzz/corpus/session/latest-jump").as_slice(),
            2,
            1,
            Ok(7),
        ),
        (
            include_bytes!("../fuzz/corpus/session/latest-empty-key").as_slice(),
            2,
            1,
            Err(StreamError::Unavailable),
        ),
        (
            include_bytes!("../fuzz/corpus/session/latest-impossible-durable").as_slice(),
            2,
            1,
            Err(StreamError::Unavailable),
        ),
        (
            include_bytes!("../fuzz/corpus/session/all-short-commit").as_slice(),
            1,
            1,
            Err(StreamError::Unavailable),
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let frame = wire::SessionFrame::decode(seed).unwrap();
        let Some(wire::session_frame::Body::Data(data)) = frame.body else {
            panic!("seed is not data");
        };
        let after = if case == 3 { 2 } else { 8 };
        assert_eq!(
            data.after,
            Some(wire::SessionCursor {
                next: after,
                ..Default::default()
            })
        );
        assert_eq!(
            (data.accepted, data.durable),
            (after, if case == 2 { 9 } else { 0 })
        );
        match &data.item {
            Some(Item::LatestValue(value)) if case < 3 => {
                assert_eq!(
                    value.key.as_ref(),
                    if case == 1 {
                        b"".as_slice()
                    } else {
                        b"agent".as_slice()
                    }
                );
                assert_eq!(value.value.as_ref(), b"value");
                assert!(!value.attention);
            }
            Some(Item::Record(record)) if case == 3 => {
                assert_eq!(record.sequence, 1);
                assert_eq!(record.commit_id.as_ref(), &[b'x'; 31]);
            }
            _ => panic!("seed item does not match its intended case"),
        }
        assert_eq!(follow_sequence(delivery, next, &data), expected);
        assert_eq!(check(delivery, next, &data, Fault::None), Ok(()));
    }
}

#[test]
fn browser_corpus_uses_the_shared_rust_codec_and_exact_outcomes() {
    use acyclic_stream::{CommitId, wire_codec::session::confirm_follow};
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("fuzz/corpus/session");
    for (name, delivery, after, accepted, durable, length, expected) in [
        ("latest-jump", 2, 9, 10, 8, 1, Ok(8)),
        (
            "latest-empty-key",
            2,
            9,
            10,
            8,
            0,
            Err(StreamError::Unavailable),
        ),
        (
            "latest-impossible-durable",
            2,
            9,
            10,
            11,
            1,
            Err(StreamError::Unavailable),
        ),
        ("all-held-flush", 1, 1, 4, 0, 32, Ok(0)),
        (
            "all-short-commit",
            1,
            1,
            4,
            0,
            31,
            Err(StreamError::Unavailable),
        ),
    ] {
        let seed = std::fs::read(root.join(format!("browser-{name}"))).unwrap();
        let frame = wire::SessionFrame::decode(seed.as_slice()).unwrap();
        let Some(wire::session_frame::Body::Data(data)) = frame.body else {
            panic!("{name} is not data")
        };
        assert_eq!(
            data.after.as_ref().map(|cut| cut.next),
            Some(after),
            "{name}"
        );
        assert_eq!((data.accepted, data.durable), (accepted, durable), "{name}");
        match &data.item {
            Some(Item::LatestValue(value)) if delivery == 2 => {
                assert_eq!(value.key.len(), length, "{name}");
            }
            Some(Item::Record(record)) if delivery == 1 => {
                assert_eq!(record.commit_id.len(), length, "{name}");
            }
            _ => panic!("{name} has the wrong item"),
        }
        assert_eq!(follow_sequence(delivery, 0, &data), expected, "{name}");
        assert_eq!(check(delivery, 0, &data, Fault::None), Ok(()));
    }
    for (name, confirmation, expected) in [
        ("opened-empty", vec![], Err(StreamError::Unsupported)),
        ("opened-foreign", vec![9; 32], Err(StreamError::Unsupported)),
        ("opened-matching", vec![7; 32], Ok(())),
    ] {
        let seed = std::fs::read(root.join(format!("browser-{name}"))).unwrap();
        let frame = wire::SessionFrame::decode(seed.as_slice()).unwrap();
        let Some(wire::session_frame::Body::Opened(opened)) = frame.body else {
            panic!("{name} is not opened")
        };
        assert_eq!(opened.preceding_commit_id.as_ref(), confirmation, "{name}");
        assert_eq!(
            confirm_follow(Some(CommitId::from_bytes([7; 32])), &opened),
            expected,
            "{name}"
        );
    }
}
