use crate::session::*;
use crate::{CommitId, MemoryStream, OwnedBytes, Record};
use proptest::prelude::*;

pub(super) fn empty_session(latest: bool) -> Session<MemoryStream> {
    Session {
        authority: testing::authority(),
        slots: testing::slots(),
        trust_revision: 0,
        deadline: Some(1000),
        provider: Arc::new(MemoryStream::default()),
        executor: Arc::new(runtime::PlatformExecutor),
        publications: None,
        limits: Limits::default(),
        node: "node".into(),
        hello: true,
        prefer_control: true,
        revision: 1,
        channels: BTreeMap::from([(
            1,
            Channel {
                binding: Some(testing::binding()),
                opaque_cursor: false,
                revalidate: None,
                generation: 1,
                latest,
                pending: VecDeque::new(),
                indexed: None,
                credit: wire::SessionWindow::default(),
                sent: cursor(0),
                applied: cursor(0),
                ending: None,
            },
        )]),
        sources: sources::Sources::new(),
        terminals: sources::Sources::new(),
        incoming: stream::empty().boxed(),
        credit: wire::SessionWindow::default(),
        controls: VecDeque::new(),
        pending_frames: 0,
        pending_bytes: 0,
        runnable: BTreeSet::new(),
        ready_after: None,
        head_visits: 0,
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    // Child resume bytes round-trip exactly and must name a direct child.
    #[test]
    fn child_resume_round_trip_checks_its_parent(name in "[a-z]{1,8}", nested in any::<bool>(), next in 1_u64..1024) {
        let request = crate::ChildrenRequest { parent: Some(StreamPath::new("agents").unwrap()), limit: 1 };
        let path = if nested { format!("agents/other/{name}") } else { format!("agents/{name}") };
        let resume = wire::SessionCursor { next, opaque: Bytes::from(path) };
        let decoded = wire::SessionCursor::decode(resume.encode_to_vec().as_slice()).unwrap();
        prop_assert_eq!(&decoded, &resume);
        prop_assert_eq!(children::validate_resume(&request, &decoded).is_ok(), !nested);
    }

    #[test]
    fn child_frames_fit_the_reserved_slot(length in 1_usize..=crate::MAX_PATH_BYTES) {
        let path = "a".repeat(length);
        let after = wire::SessionCursor { next: u64::MAX, opaque: Bytes::copy_from_slice(path.as_bytes()) };
        let frame = frame(u64::MAX, u64::MAX, Body::Data(wire::SessionData { after: Some(after), accepted: u64::MAX, durable: 0, item: Some(Item::Child(wire::Child { path })) }));
        let decoded = wire::SessionFrame::decode(frame.encode_to_vec().as_slice()).unwrap();
        prop_assert_eq!(&decoded, &frame);
        prop_assert!(frame.encoded_len() <= CHILD_FRAME_BYTES);
    }

    // Property: ALL preserves every offset; LATEST retains every attention and
    // exactly the final ordinary value for each key, in original cursor order.
    #[test]
    fn backpressure_matches_the_history_projection(
        history in prop::collection::vec((1_u8..5, any::<u8>(), any::<bool>()), 0..32),
        latest in any::<bool>(),
    ) {
        let mut session = empty_session(latest);
        for (index, (key, value, attention)) in history.iter().enumerate() {
            let value = wire::SessionLatestValue {
                key: Bytes::copy_from_slice(&[*key]), value: Bytes::copy_from_slice(&[*value]), attention: *attention,
            }.encode_to_vec();
            let record = Record { headers: Default::default(),
                sequence: index as u64, value: OwnedBytes::from(value),
                commit_id: CommitId::from_bytes([1; 32]), committed_at_micros: 0,
            };
            let identity = wire::SessionChannel { channel_id: 1, generation: 1 };
            prop_assert!(session.record(identity, Ok(Event::Record(record))).is_ok());
        }
        let expected: Vec<_> = history.iter().enumerate().filter(|(index, (key, _, attention))| {
            !latest || *attention || !history.iter().skip(index + 1).any(|(later_key, _, later_attention)| later_key == key && !later_attention)
        }).map(|(index, _)| index as u64 + 1).collect();
        let channel = session.channels.get(&1).unwrap();
        let actual: Vec<_> = channel.pending.iter().filter_map(|pending| match &pending.frame.body {
            Some(Body::Data(data)) => data.after.as_ref().map(|after| after.next), _ => None,
        }).collect();
        prop_assert_eq!(actual, expected);
        prop_assert_eq!(session.pending_bytes, channel.pending.iter().map(|pending| pending.frame.encoded_len()).sum::<usize>());
    }
}

fn check_admission(session: &Session<MemoryStream>) {
    let expected: BTreeSet<_> = session
        .channels
        .iter()
        .filter_map(|(id, channel)| {
            channel
                .pending
                .front()
                .filter(|p| {
                    channel.credit.frames > 0
                        && p.frame.encoded_len() as u64 <= channel.credit.bytes
                })
                .map(|p| (p.frame.encoded_len(), *id))
        })
        .collect();
    let frames: usize = session.channels.values().map(|c| c.pending.len()).sum();
    let bytes: usize = session
        .channels
        .values()
        .flat_map(|c| &c.pending)
        .map(|p| p.frame.encoded_len())
        .sum();
    assert_eq!(session.pending_frames, frames);
    assert_eq!(session.pending_bytes, bytes);
    for (id, channel) in &session.channels {
        assert_eq!(
            channel.indexed.map(|length| (length, *id)),
            expected
                .iter()
                .find(|(_, candidate)| candidate == id)
                .copied()
        );
    }
    assert_eq!(
        session.runnable, expected,
        "runnable index lost a credited channel"
    );
}

fn exercise_admission(operations: &[u8], broken: bool) {
    let mut session = empty_session(false);
    session.credit = wire::SessionWindow {
        frames: 1000,
        bytes: crate::MAX_COMMAND_BYTES as u64,
    };
    let mut sequence = 0;
    for operation in operations {
        match operation % 4 {
            0 => {
                session
                    .record(
                        wire::SessionChannel {
                            channel_id: 1,
                            generation: 1,
                        },
                        Ok(Event::Record(Record {
                            headers: Default::default(),
                            sequence,
                            value: vec![0].into(),
                            commit_id: CommitId::from_bytes([1; 32]),
                            committed_at_micros: 0,
                        })),
                    )
                    .unwrap();
                sequence += 1;
            }
            1 => session.receive(super::window(1)).unwrap(),
            2 => {
                session.ready().unwrap();
            }
            _ => {
                session.remove(1).unwrap();
                session = empty_session(false);
                sequence = 0;
            }
        }
        if broken {
            session.runnable.clear();
        }
        check_admission(&session);
    }
}
proptest! {
    // The cached runnable set equals the credit policy after every
    // append, credit grant, delivery and cancellation; no source is lost.
    #[test]
    fn runnable_channels_match_credit_policy(operations in prop::collection::vec(any::<u8>(), 0..48)) {
        exercise_admission(&operations, false);
    }
}
#[test]
fn negative_control_drops_the_runnable_index_update() {
    assert!(std::panic::catch_unwind(|| exercise_admission(&[0, 1], true)).is_err());
}
