//! Bounded verification of the canonical Streams session.
use acyclic_stream::{
    wire,
    wire_codec::session::{consume_credit, grant_credit},
};
use bytes::Bytes;
use proptest::prelude::*;
use prost::Message;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Property: additive grants either update both balances exactly or leave
    // both unchanged on overflow, across the complete u64/u32 input domains.
    #[test]
    fn credit_grants_are_atomic_and_additive(
        bytes in any::<u64>(), frames in any::<u32>(),
        added_bytes in any::<u64>(), added_frames in any::<u32>(),
    ) {
        let before = wire::SessionWindow { bytes, frames };
        let mut remaining = before;
        let result = grant_credit(&mut remaining, &wire::SessionWindow { bytes: added_bytes, frames: added_frames });
        match (bytes.checked_add(added_bytes), frames.checked_add(added_frames)) {
            (Some(bytes), Some(frames)) => {
                prop_assert!(result.is_ok());
                prop_assert_eq!(remaining, wire::SessionWindow { bytes, frames });
            }
            _ => { prop_assert!(result.is_err()); prop_assert_eq!(remaining, before); }
        }
    }

    // Property: every data debit conserves encoded bytes and frames; controls
    // consume neither. Failed debits are atomic, including zero-frame credit.
    #[test]
    fn frame_debits_conserve_both_balances(
        bytes in 0_u64..1024, frames in 0_u32..4,
        channel in 0_u64..4, generation in any::<u64>(),
        value in prop::collection::vec(any::<u8>(), 0..256), data in any::<bool>(),
    ) {
        let frame = wire::SessionFrame {
            channel_id: channel, generation,
            body: Some(if data {
                wire::session_frame::Body::Data(wire::SessionData {
                    item: Some(wire::session_data::Item::LatestValue(wire::SessionLatestValue {
                        key: Bytes::from_static(b"key"), value: Bytes::from(value), attention: true,
                    })), ..Default::default()
                })
            } else { wire::session_frame::Body::Moved(wire::SessionMoved::default()) }),
        };
        let before = wire::SessionWindow { bytes, frames };
        let mut remaining = before;
        let result = consume_credit(&mut remaining, &frame);
        if !data { prop_assert!(result.is_ok()); prop_assert_eq!(remaining, before); }
        else if channel != 0 && bytes >= frame.encoded_len() as u64 && frames > 0 {
            prop_assert!(result.is_ok());
            prop_assert_eq!(remaining.bytes + frame.encoded_len() as u64, bytes);
            prop_assert_eq!(remaining.frames + 1, frames);
        } else { prop_assert!(result.is_err()); prop_assert_eq!(remaining, before); }
    }

    // Property: every decodable untrusted protobuf frame has a stable canonical
    // encode/decode round trip, and every latest envelope preserves all fields.
    #[test]
    fn untrusted_frames_and_latest_values_round_trip(
        input in prop::collection::vec(any::<u8>(), 0..512),
        key in prop::collection::vec(any::<u8>(), 0..32),
        value in prop::collection::vec(any::<u8>(), 0..128), attention in any::<bool>(),
    ) {
        if let Ok(frame) = wire::SessionFrame::decode(input.as_slice()) {
            let encoded = frame.encode_to_vec();
            prop_assert_eq!(wire::SessionFrame::decode(encoded.as_slice()).ok(), Some(frame));
        }
        let value = wire::SessionLatestValue { key: Bytes::from(key), value: Bytes::from(value), attention };
        prop_assert_eq!(wire::SessionLatestValue::decode(value.encode_to_vec().as_slice()).ok(), Some(value));
    }
}
