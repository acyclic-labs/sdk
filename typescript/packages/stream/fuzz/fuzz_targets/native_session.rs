#![no_main]
use libfuzzer_sys::fuzz_target;
use prost::Message;

#[path = "../../../../../rust/crates/stream/tests/support/follow_data.rs"]
mod follow_data;

fuzz_target!(|bytes: &[u8]| {
    let mut next = [0; 8];
    for (to, from) in next.iter_mut().zip(bytes.iter()) {
        *to = *from;
    }
    follow_data::exercise(u64::from_le_bytes(next), bytes.len() as u16, bytes, false);
    let limits = tonic_web_wasm_client::limits::UnaryResponseLimits::new(bytes.len()).unwrap();
    for text in [false, true] {
        assert_eq!(limits.body_bytes(true, text), None);
        assert_eq!(
            limits.body_bytes(false, text),
            Some((bytes.len() + 16_384 + 10) * if text { 4 } else { 1 })
        );
        assert_eq!(limits.message_bytes(), bytes.len());
    }
    let generated = acyclic_stream::wire::SessionFrame {
        channel_id: bytes.first().copied().unwrap_or_default() as u64,
        body: Some(acyclic_stream::wire::session_frame::Body::Window(
            acyclic_stream::wire::SessionWindow {
                bytes: bytes.len() as u64,
                frames: 1,
            },
        )),
        ..Default::default()
    };
    assert_eq!(
        acyclic_stream::wire_codec::session::decode_frame(&generated.encode_to_vec()).unwrap(),
        generated
    );
    if let Ok(frame) = acyclic_stream::wire_codec::session::decode_frame(bytes) {
        let canonical = frame.encode_to_vec();
        assert_eq!(
            acyclic_stream::wire_codec::session::decode_frame(&canonical).unwrap(),
            frame
        );
    }
});
