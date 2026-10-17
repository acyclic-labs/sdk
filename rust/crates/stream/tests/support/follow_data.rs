use acyclic_stream::wire_codec::session::validate_follow;
use acyclic_stream::{StreamError, wire, wire_codec::session::follow_sequence};
use bytes::Bytes;
use prost::Message;
use wire::{SessionDelivery, session_data::Item};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Fault {
    None,
    StrictLatest,
    SkipAll,
    EmptyKey,
    Watermark,
    Opaque,
    Commit,
    WrongMode,
}

// This oracle describes received wire positions directly. It never encodes a
// cursor or calls the production inverse to derive the expected sequence.
fn expected(delivery: i32, next: u64, data: &wire::SessionData) -> Result<u64, StreamError> {
    let Some(after) = &data.after else {
        return Err(StreamError::Unavailable);
    };
    if data.durable > data.accepted || after.next > data.accepted || !after.opaque.is_empty() {
        return Err(StreamError::Unavailable);
    }
    match (&data.item, delivery) {
        (Some(Item::Record(record)), 1)
            if record.commit_id.len() == 32 && record.sequence == next =>
        {
            let successor = u128::from(record.sequence) + 1;
            if successor > u128::from(u64::MAX) {
                return Err(StreamError::OutOfRange);
            }
            if u128::from(after.next) != successor {
                return Err(StreamError::Unavailable);
            }
            Ok(record.sequence)
        }
        (Some(Item::LatestValue(value)), 2)
            if !value.key.is_empty()
                && after.next != 0
                && after.next >= next.saturating_add(1)
                && next != u64::MAX =>
        {
            Ok(after.next - 1)
        }
        _ => Err(StreamError::Unavailable),
    }
}

pub(super) fn check(
    delivery: i32,
    next: u64,
    data: &wire::SessionData,
    fault: Fault,
) -> Result<(), &'static str> {
    let wanted = expected(delivery, next, data);
    let mut changed = data.clone();
    if fault == Fault::Watermark {
        changed.accepted = u64::MAX;
        changed.durable = 0;
    }
    if fault == Fault::Opaque
        && let Some(after) = &mut changed.after
    {
        after.opaque.clear();
    }
    if fault == Fault::Commit
        && let Some(Item::Record(record)) = &mut changed.item
    {
        record.commit_id = vec![7; 32].into();
    }
    let mut actual = follow_sequence(delivery, next, &changed);
    if let Some(after) = &data.after {
        match fault {
            Fault::StrictLatest
                if delivery == SessionDelivery::Latest as i32
                    && after.next > next.saturating_add(1)
                    && actual.is_ok() =>
            {
                actual = Err(StreamError::Unavailable);
            }
            Fault::SkipAll if delivery == SessionDelivery::All as i32 => {
                if let Some(Item::Record(record)) = &data.item
                    && record.sequence != next
                    && record.sequence.checked_add(1) == Some(after.next)
                {
                    actual = Ok(record.sequence);
                }
            }
            Fault::EmptyKey | Fault::WrongMode if after.next > next => {
                if let Some(Item::LatestValue(value)) = &data.item
                    && ((fault == Fault::EmptyKey && value.key.is_empty() && delivery == 2)
                        || (fault == Fault::WrongMode && delivery == 1))
                {
                    actual = Ok(after.next - 1);
                }
            }
            _ => {}
        }
    }
    if actual == wanted {
        Ok(())
    } else {
        Err("follow data contract")
    }
}

pub fn exercise(next: u64, gap: u16, payload: &[u8], broken: bool) {
    if let Ok(subscription) = wire::SessionSubscription::decode(payload) {
        assert_eq!(
            wire::SessionSubscription::decode(subscription.encode_to_vec().as_slice()).as_ref(),
            Ok(&subscription)
        );
        if let (Some(wire::session_subscription::Kind::Follow(follow)), Some(resume)) =
            (&subscription.kind, &subscription.resume)
        {
            let _ = validate_follow(follow, resume);
        }
    }
    let next = next % (u64::MAX - u64::from(u16::MAX) - 1);
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
    let fault = if broken {
        Fault::StrictLatest
    } else {
        Fault::None
    };
    assert_eq!(
        check(follow.delivery, next, &data, fault),
        Ok(()),
        "LATEST accepts a conflated cursor"
    );
    assert_eq!(
        follow_sequence(follow.delivery, next, &data),
        Ok(next + gap - 1)
    );
    let canonical = data.encode_to_vec();
    assert_eq!(
        wire::SessionData::decode(canonical.as_slice()).as_ref(),
        Ok(&data)
    );
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
    assert_eq!(validate_follow(&follow, &resume), Ok(()));
    follow.preceding_commit_id = Bytes::from_static(&[7; 31]);
    assert!(validate_follow(&follow, &resume).is_err());
    follow.preceding_commit_id = Bytes::from_static(&[7; 32]);
    follow.delivery = wire::SessionDelivery::Latest as i32;
    assert!(validate_follow(&follow, &resume).is_err());
    follow.preceding_commit_id = Bytes::new();
    assert_eq!(validate_follow(&follow, &resume), Ok(()));

    data.item = Some(wire::session_data::Item::Record(wire::Record {
        sequence: next,
        value: Bytes::copy_from_slice(payload),
        commit_id: Bytes::from_static(&[7; 32]),
        ..Default::default()
    }));
    data.after = Some(wire::SessionCursor {
        next: next + 1,
        opaque: Bytes::new(),
    });
    assert_eq!(
        follow_sequence(wire::SessionDelivery::All as i32, next, &data),
        Ok(next)
    );
    data.after = Some(wire::SessionCursor {
        next: next + 2,
        opaque: Bytes::new(),
    });
    data.accepted = next + 2;
    assert!(follow_sequence(wire::SessionDelivery::All as i32, next, &data).is_err());
    if let Some(Item::Record(record)) = &mut data.item {
        record.sequence += 1;
    }
    assert_eq!(check(1, next, &data, Fault::None), Ok(()));
    assert!(check(1, next, &data, Fault::SkipAll).is_err());
}
