#![no_main]

use acyclic_harness_tui_core::key_event::{InputEvent, KeyCode};
use acyclic_harness_tui_terminal::{Decoder, PROBE_LIMIT, Probe, encode_key};
use libfuzzer_sys::fuzz_target;

/// Decode one transport chunk through the whole-stream decoder.
fn decode(bytes: &[u8]) -> (bool, Vec<InputEvent>) {
    let mut decoder = Decoder::default();
    let failed = decoder.feed(bytes).is_err() | decoder.finish_input().is_err();
    (failed, decoder.take_events())
}

fuzz_target!(|bytes: &[u8]| {
    let (failed, events) = decode(bytes);

    // Framing is chunk-independent: the same bytes delivered one at a time
    // must classify identically, including the reported error.
    let mut split = Decoder::default();
    let mut split_failed = false;
    for byte in bytes {
        split_failed |= split.feed(std::slice::from_ref(byte)).is_err();
    }
    split_failed |= split.finish_input().is_err();
    assert_eq!(failed, split_failed, "chunking changed the decoder error");
    assert_eq!(events, split.take_events(), "chunking changed the events");
    assert!(split.take_events().is_empty(), "take_events did not drain");

    // No input byte may produce more than one event.
    assert!(
        events.len() <= bytes.len(),
        "decoder amplified {} bytes into {} events",
        bytes.len(),
        events.len()
    );

    for event in &events {
        match event {
            InputEvent::Paste(text) => {
                assert!(text.len() <= PROBE_LIMIT, "paste exceeded the input limit");
            }
            InputEvent::Key(key) => {
                assert!(
                    !matches!(key.code, KeyCode::Char(c) if c.is_control()),
                    "control character decoded as text"
                );
                // Every representable key must survive the shipping encoder and
                // decode back to the same event.
                if let Ok(encoded) = encode_key(*key) {
                    let (encoded_failed, encoded_events) = decode(&encoded);
                    assert!(!encoded_failed, "encoded key was rejected");
                    assert_eq!(
                        encoded_events,
                        [InputEvent::Key(*key)],
                        "key round trip changed the event"
                    );
                }
            }
            _ => {}
        }
    }

    // A bare probe frames the same bytes without the decoder's paste and text
    // layers. It must consume every byte and classify identically in any chunking.
    let mut probe = Probe::default();
    assert_eq!(
        probe.feed(bytes),
        bytes.len(),
        "probe did not consume the chunk"
    );
    probe.finish_input();
    let mut split_probe = Probe::default();
    for byte in bytes {
        assert_eq!(split_probe.feed(std::slice::from_ref(byte)), 1);
    }
    split_probe.finish_input();
    assert_eq!(
        probe.replies, split_probe.replies,
        "probe chunking changed the replies"
    );
});
