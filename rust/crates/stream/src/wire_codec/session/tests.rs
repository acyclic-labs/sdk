use super::*;
use bytes::Bytes;
use wire::{session_data::Item, session_frame::Body};

fn round_trip(frame: &wire::SessionFrame) -> Result<wire::SessionFrame, prost::DecodeError> {
    wire::SessionFrame::decode(frame.encode_to_vec().as_slice())
}

#[test]
fn latest_values_round_trip_with_additive_credit() -> Result<(), Box<dyn std::error::Error>> {
    let frame = round_trip(&wire::SessionFrame {
        channel_id: 1,
        generation: 1,
        body: Some(Body::Data(wire::SessionData {
            after: Some(wire::SessionCursor {
                next: 7,
                opaque: Bytes::new(),
            }),
            accepted: 7,
            durable: 6,
            item: Some(Item::LatestValue(wire::SessionLatestValue {
                key: Bytes::from_static(b"agent"),
                value: Bytes::from_static(b"status"),
                attention: true,
            })),
        })),
    })?;
    let Some(Body::Data(data)) = &frame.body else {
        return Err("missing data".into());
    };
    assert!(
        matches!(&data.item, Some(Item::LatestValue(value)) if value.attention && value.key.as_ref() == b"agent" && value.value.as_ref() == b"status")
    );
    let length = u64::try_from(frame.encoded_len())?;
    let grant = wire::SessionWindow {
        bytes: length,
        frames: 1,
    };
    let mut remaining = wire::SessionWindow::default();
    grant_credit(&mut remaining, &grant)?;
    grant_credit(&mut remaining, &grant)?;
    assert_eq!(remaining.bytes, length * 2);
    assert_eq!(remaining.frames, 2);
    consume_credit(&mut remaining, &frame)?;
    assert_eq!(remaining, grant);
    consume_credit(&mut remaining, &frame)?;
    assert_eq!(remaining, wire::SessionWindow::default());
    assert_eq!(
        consume_credit(&mut remaining, &frame),
        Err(StreamError::Capacity)
    );
    assert_eq!(remaining, wire::SessionWindow::default());
    Ok(())
}

#[test]
fn control_frames_are_exempt_from_data_credit() -> Result<(), prost::DecodeError> {
    let mut remaining = wire::SessionWindow::default();
    for body in [
        Body::Hello(wire::SessionHello::default()),
        Body::Welcome(wire::SessionWelcome::default()),
        Body::Interest(wire::SessionInterest::default()),
        Body::InterestApplied(wire::SessionInterestApplied::default()),
        Body::Opened(wire::SessionOpened::default()),
        Body::Ack(wire::SessionAck::default()),
        Body::Window(wire::SessionWindow {
            bytes: 100,
            frames: 1,
        }),
        Body::Closed(wire::SessionClosed::default()),
        Body::Moved(wire::SessionMoved::default()),
        Body::Watermark(wire::SessionWatermark::default()),
    ] {
        for channel_id in [0, 1] {
            let frame = round_trip(&wire::SessionFrame {
                channel_id,
                generation: 1,
                body: Some(body.clone()),
            })?;
            assert_eq!(consume_credit(&mut remaining, &frame), Ok(()));
            assert_eq!(remaining, wire::SessionWindow::default());
        }
    }
    Ok(())
}

#[test]
fn credit_failures_never_partially_change_balances() {
    for (mut remaining, grant) in [
        (
            wire::SessionWindow {
                bytes: u64::MAX,
                frames: 1,
            },
            wire::SessionWindow {
                bytes: 1,
                frames: 1,
            },
        ),
        (
            wire::SessionWindow {
                bytes: 1,
                frames: u32::MAX,
            },
            wire::SessionWindow {
                bytes: 1,
                frames: 1,
            },
        ),
    ] {
        let before = remaining;
        assert_eq!(
            grant_credit(&mut remaining, &grant),
            Err(StreamError::LimitExceeded)
        );
        assert_eq!(remaining, before);
    }
    let frame = wire::SessionFrame {
        channel_id: 1,
        generation: 1,
        body: Some(Body::Data(wire::SessionData::default())),
    };
    for mut remaining in [
        wire::SessionWindow {
            bytes: 0,
            frames: 1,
        },
        wire::SessionWindow {
            bytes: 100,
            frames: 0,
        },
    ] {
        let before = remaining;
        assert_eq!(
            consume_credit(&mut remaining, &frame),
            Err(StreamError::Capacity)
        );
        assert_eq!(remaining, before);
    }
}
