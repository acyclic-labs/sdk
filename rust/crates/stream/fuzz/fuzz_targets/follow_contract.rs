#![no_main]

use acyclic_stream::{
    StreamError, StreamWatermarks, wire,
    wire_codec::{
        session::{confirm_follow, preceding_follow, record_anchor, record_cursor},
        watermarks_from_wire,
    },
};
use libfuzzer_sys::fuzz_target;
use prost::Message;

fn round_trip<M: Message + Default + PartialEq + std::fmt::Debug>(value: &M) {
    assert_eq!(&M::decode(value.encode_to_vec().as_slice()).unwrap(), value);
}

fuzz_target!(|input: &[u8]| {
    let Some((&kind, input)) = input.split_first() else {
        return;
    };
    if input.len() > 4096 {
        return;
    }
    match kind % 3 {
        0 => {
            let Ok(subscription) = wire::SessionSubscription::decode(input) else {
                return;
            };
            round_trip(&subscription);
            if let (Some(wire::session_subscription::Kind::Follow(follow)), Some(resume)) =
                (&subscription.kind, &subscription.resume)
            {
                let result = preceding_follow(follow, resume);
                if follow.preceding_commit_id.is_empty() {
                    assert_eq!(result, Ok(None));
                } else if follow.delivery == wire::SessionDelivery::All as i32
                    && resume.next > 0
                    && resume.opaque.is_empty()
                    && follow.preceding_commit_id.len() == 32
                {
                    let preceding = result.unwrap().expect("checked guard omitted");
                    assert_eq!(
                        preceding.as_bytes().as_slice(),
                        follow.preceding_commit_id.as_ref()
                    );
                    let confirmed = wire::SessionOpened {
                        resume: Some(resume.clone()),
                        preceding_commit_id: follow.preceding_commit_id.clone(),
                    };
                    assert_eq!(confirm_follow(Some(preceding), &confirmed), Ok(()));
                    assert_eq!(
                        confirm_follow(Some(preceding), &wire::SessionOpened::default()),
                        Err(StreamError::Unsupported)
                    );
                } else {
                    assert_eq!(result, Err(StreamError::InvalidArgument));
                }
            }
        }
        1 => {
            let Ok(cursor) = wire::SessionCursor::decode(input) else {
                return;
            };
            round_trip(&cursor);
            match record_anchor(&cursor) {
                Ok(Some(anchor)) => assert_eq!(record_cursor(cursor.next, anchor).unwrap(), cursor),
                Ok(None) => {
                    assert_eq!(cursor.next, 0);
                    assert!(cursor.opaque.is_empty());
                }
                Err(error) => {
                    assert_eq!(error, StreamError::InvalidArgument);
                    assert!(
                        (cursor.next == 0 && !cursor.opaque.is_empty())
                            || (cursor.next > 0 && cursor.opaque.len() != 32)
                    );
                }
            }
        }
        _ => {
            let Ok(response) = wire::TailResponse::decode(input) else {
                return;
            };
            round_trip(&response);
            let expected = match response.durable {
                None => Err(StreamError::Unsupported),
                Some(durable) if durable > response.tail => Err(StreamError::Unavailable),
                Some(durable) => Ok(StreamWatermarks {
                    accepted: response.tail,
                    durable,
                }),
            };
            assert_eq!(watermarks_from_wire(response), expected);
        }
    }
});
