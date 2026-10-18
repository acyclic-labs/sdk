#![no_main]

pub use acyclic_fuzz_support as fuzz_support;

use acyclic_stream::{
    MemoryStream, session, wire,
    wire_codec::session::{consume_credit, grant_credit},
};
use bytes::Bytes;
use futures::{StreamExt, stream};
use prost::Message;
use std::sync::Arc;

mod checks {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../tests/support/follow_data.rs"
    ));
}

fuzz_support::fuzz_target!(|input: &[u8], fuzz_input| {
    if input.len() > 4096 {
        return;
    }
    let Some(frame) = fuzz_support::FuzzInput::parse(fuzz_input, wire::SessionFrame::decode) else {
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
    let _ = fuzz_support::round_trip(
        &frame,
        wire::SessionFrame::decode(frame.encode_to_vec().as_slice()),
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
    if let Ok(output) = acyclic_stream::session::serve(
        Arc::new(MemoryStream::default()),
        "node".into(),
        session::Limits::default(),
        incoming,
    ) {
        futures::executor::block_on(output.collect::<Vec<_>>());
    }
});
