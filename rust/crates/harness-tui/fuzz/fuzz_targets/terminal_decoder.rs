#![no_main]
use acyclic_harness_tui::key_event::{InputEvent, KeyCode, KeyEvent, KeyModifiers};
use acyclic_harness_tui::term::{Decoder, Environment, encode_key};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|bytes: &[u8]| {
    let mut whole = Decoder::default();
    let whole_ok = whole.feed(bytes).is_ok();
    let mut split = Decoder::default();
    let chunk = bytes.first().map_or(1, |byte| usize::from(*byte) + 1);
    let mut split_ok = true;
    for bytes in bytes.chunks(chunk) {
        if split.feed(bytes).is_err() {
            split_ok = false;
        }
    }
    assert_eq!(whole_ok, split_ok);
    assert_eq!(whole.replies(), split.replies());
    assert_eq!(whole.take_events(), split.take_events());
    assert_eq!(whole.finish_input().is_ok(), split.finish_input().is_ok());
    assert_eq!(whole.take_events(), split.take_events());
    let _ = whole.replies().capabilities(&Environment::default());
    let code = bytes.first().map_or('x', |byte| char::from(*byte).max(' '));
    let modifiers = KeyModifiers::from_bits_retain(bytes.get(1).copied().unwrap_or_default() & 63);
    let key = KeyEvent::new(KeyCode::Char(code), modifiers);
    if let Ok(encoded) = encode_key(key) {
        let mut decoder = Decoder::default();
        assert!(decoder.feed(&encoded).is_ok());
        assert_eq!(decoder.take_events(), vec![InputEvent::Key(key)]);
    }
});
