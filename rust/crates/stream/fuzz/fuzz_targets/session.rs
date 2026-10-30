#![no_main]

use acyclic_stream::{
    wire,
    wire_codec::session::{consume_credit, grant_credit},
};
use libfuzzer_sys::fuzz_target;
use prost::Message;

mod checks {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../tests/support/follow_data.rs"
    ));
}

fuzz_target!(|input: &[u8]| {
    checks::exercise(
        u64::from(input.first().copied().unwrap_or(0)),
        u16::from(input.get(1).copied().unwrap_or(0)),
        input,
        false,
    );
    let Ok(frame) = wire::SessionFrame::decode(input) else {
        return;
    };
    if let Some(wire::session_frame::Body::Data(data)) = &frame.body {
        let after = data.after.as_ref().map_or(0, |cursor| cursor.next);
        for next in [0, after.saturating_sub(1), after, u64::MAX] {
            for delivery in [0, 1, 2] {
                checks::check(delivery, next, data, checks::Fault::None)
                    .expect("follow data contract");
            }
        }
    }
    let encoded = frame.encode_to_vec();
    assert_eq!(
        wire::SessionFrame::decode(encoded.as_slice()).ok().as_ref(),
        Some(&frame)
    );
    let mut credit = wire::SessionWindow {
        bytes: 1024,
        frames: 2,
    };
    let before = credit;
    if consume_credit(&mut credit, &frame).is_err() {
        assert_eq!(credit, before);
    }
    if let Some(wire::session_frame::Body::Window(grant)) = &frame.body {
        let before = credit;
        if grant_credit(&mut credit, grant).is_err() {
            assert_eq!(credit, before);
        }
    }
    assert!(acyclic_stream::codec_fuzz::session(input));
});
