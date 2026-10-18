use bytes::Bytes;
use prost::Message;

pub fn exercise(next: u64, gap: u16, payload: &[u8], broken: bool) {
    if let Ok(subscription) = wire::SessionSubscription::decode(payload) {
        crate::fuzz_support::round_trip(
            &subscription,
            wire::SessionSubscription::decode(subscription.encode_to_vec().as_slice()),
        );
        if let (Some(wire::session_subscription::Kind::Follow(follow)), Some(resume)) =
            (&subscription.kind, &subscription.resume)
        {
            let _ = validate_follow(follow, resume);
        }
    }
    let next = next % (u64::MAX - u16::MAX as u64 - 1);
    let gap = u64::from(gap) + 1;
    let mut follow = wire::SessionFollow {
        path: "agents/status".into(),
        delivery: wire::SessionDelivery::Latest as i32,
        ..Default::default()
    };
    let mut data = wire::SessionData {
        after: Some(wire::SessionCursor {
            next: next + gap,
            opaque: Bytes::new(),
        }),
        accepted: next + gap,
        durable: next,
        item: Some(wire::session_data::Item::LatestValue(
            wire::SessionLatestValue {
                key: Bytes::from_static(b"agent"),
                value: Bytes::copy_from_slice(payload),
                attention: true,
            },
        )),
    };
    check(
        follow.delivery,
        next,
        &data,
        if broken {
            Fault::StrictLatest
        } else {
            Fault::None
        },
    )
    .expect("LATEST accepts a conflated cursor");
    assert_eq!(
        follow_sequence(follow.delivery, next, &data).unwrap(),
        next + gap - 1
    );
    let canonical = data.encode_to_vec();
    crate::fuzz_support::round_trip(&data, wire::SessionData::decode(canonical.as_slice()));
    data.durable = data.accepted + 1;
    assert!(follow_sequence(follow.delivery, next, &data).is_err());
    data.durable = next;
    if let Some(wire::session_data::Item::LatestValue(value)) = data.item.as_mut() {
        value.key = Bytes::new();
    }
    assert!(follow_sequence(follow.delivery, next, &data).is_err());

    follow.delivery = wire::SessionDelivery::All as i32;
    follow.preceding_commit_id = Bytes::from_static(&[7; 32]);
    let resume = wire::SessionCursor {
        next: next + 1,
        opaque: Bytes::new(),
    };
    validate_follow(&follow, &resume).unwrap();
    follow.preceding_commit_id = Bytes::from_static(&[7; 31]);
    assert!(validate_follow(&follow, &resume).is_err());
    follow.preceding_commit_id = Bytes::from_static(&[7; 32]);
    follow.delivery = wire::SessionDelivery::Latest as i32;
    assert!(validate_follow(&follow, &resume).is_err());
    follow.preceding_commit_id = Bytes::new();
    validate_follow(&follow, &resume).unwrap();

    data.item = Some(wire::session_data::Item::Record(wire::Record {
        sequence: next,
        value: Bytes::copy_from_slice(payload),
        commit_id: Bytes::from_static(&[7; 32]),
        ..Default::default()
    }));
    data.after.as_mut().unwrap().next = next + 1;
    assert_eq!(
        follow_sequence(wire::SessionDelivery::All as i32, next, &data).unwrap(),
        next
    );
    data.after.as_mut().unwrap().next += 1;
    data.accepted = data.after.as_ref().unwrap().next;
    assert!(follow_sequence(wire::SessionDelivery::All as i32, next, &data).is_err());
    if let Some(Item::Record(record)) = &mut data.item {
        record.sequence += 1;
    }
    assert_eq!(check(1, next, &data, Fault::None), Ok(()));
    assert!(check(1, next, &data, Fault::SkipAll).is_err());
}
