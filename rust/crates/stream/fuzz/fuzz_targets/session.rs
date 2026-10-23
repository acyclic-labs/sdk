#![no_main]

use acyclic_stream::{
    MemoryStream, session, wire,
    wire_codec::session::{consume_credit, grant_credit},
};
use bytes::Bytes;
use futures::{StreamExt, stream};
use libfuzzer_sys::fuzz_target;
use prost::Message;
use std::sync::Arc;

mod checks {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../tests/support/follow_data.rs"
    ));
}

fuzz_target!(|input: &[u8]| {
    if input.len() > 4096 {
        return;
    }
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
    let hello = wire::SessionFrame {
        body: Some(wire::session_frame::Body::Hello(wire::SessionHello {
            credentials: vec![session::testing::credential()],
            version: 1,
            client_id: Bytes::from_static(b"fuzz"),
            destination_node: "node".into(),
            receive_bytes: 1024,
            receive_frames: 2,
        })),
        ..Default::default()
    };
    // Finite input guarantees termination even for a valid live subscription.
    let incoming = stream::iter([Ok(hello), Ok(frame)]).boxed();
    if let Ok(output) = session::serve(
        Arc::new(MemoryStream::default()),
        session::testing::authority(),
        "node".into(),
        session::Limits::default(),
        incoming,
    ) {
        futures::executor::block_on(output.collect::<Vec<_>>());
    }
});
