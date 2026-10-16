use crate::session::*;
use crate::{CommitId, MemoryStream, OwnedBytes, Record};
use proptest::prelude::*;

fn empty_session(latest: bool) -> Session<MemoryStream> {
    let (cancel, _) = oneshot::channel();
    let enabled = Arc::new(gate::Gate::new());
    Session {
        provider: Arc::new(MemoryStream::default()),
        limits: Limits::default(),
        node: "node".into(),
        hello: true,
        prefer_control: true,
        revision: 1,
        channels: BTreeMap::from([(
            1,
            Channel {
                children: false,
                generation: 1,
                latest,
                enabled,
                pending: 0,
                credit: wire::SessionWindow::default(),
                sent: cursor(0),
                applied: cursor(0),
                ending: None,
                cancel: Some(cancel),
            },
        )]),
        sources: SelectAll::new(),
        incoming: stream::empty().boxed(),
        credit: wire::SessionWindow::default(),
        controls: VecDeque::new(),
        pending: VecDeque::new(),
        pending_bytes: 0,
        readiness: (false, false, false),
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
            let record = Record {
                sequence: index as u64, value: OwnedBytes::from(value),
                commit_id: CommitId::from_bytes([1; 32]), committed_at_micros: 0,
            };
            let identity = wire::SessionChannel { channel_id: 1, generation: 1 };
            prop_assert!(session.record(identity, Ok(Event::Record(record))).is_ok());
        }
        let expected: Vec<_> = history.iter().enumerate().filter(|(index, (key, _, attention))| {
            !latest || *attention || !history[index + 1..].iter().any(|(later_key, _, later_attention)| later_key == key && !later_attention)
        }).map(|(index, _)| index as u64 + 1).collect();
        let actual: Vec<_> = session.pending.iter().filter_map(|pending| match &pending.frame.body {
            Some(Body::Data(data)) => data.after.as_ref().map(|after| after.next), _ => None,
        }).collect();
        prop_assert_eq!(actual, expected);
        prop_assert_eq!(session.pending_bytes, session.pending.iter().map(|pending| pending.frame.encoded_len()).sum::<usize>());
    }
}
