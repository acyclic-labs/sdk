#![no_main]

use acyclic_harness_tui::key_event::{InputEvent, KeyEvent, KeyModifiers};
use acyclic_harness_tui::term::{Decoder, PROBE_LIMIT};
use libfuzzer_sys::fuzz_target;

#[path = "../../tests/support/terminal_keys.rs"]
mod table;

fuzz_target!(|data: &[u8]| {
    let rows: Vec<_> = table::KITTY
        .iter()
        .chain(table::NUMBERED)
        .chain(table::FINAL)
        .collect();
    let mut stream = Vec::new();
    let mut expected = Vec::new();
    for index in data.iter().copied().take(32).chain(std::iter::once(0)) {
        let (code, body) = rows[usize::from(index) % rows.len()];
        stream.extend_from_slice(format!("\x1b[{body}").as_bytes());
        expected.push(InputEvent::Key(KeyEvent::new(*code, KeyModifiers::NONE)));
    }
    stream.extend_from_slice(b"\x1b[200~valid paste\x1b[201~\x1b]11;rgb:ffff/0000/0000\x07");
    expected.push(InputEvent::Paste("valid paste".into()));
    let mut whole = Decoder::default();
    assert!(whole.feed(&stream).is_ok());
    assert_eq!(whole.take_events(), expected);

    // Every bit independently selects a byte boundary; the fuzzer can express
    // any partition of this valid stream, not only fixed-size chunks.
    let mut split = Decoder::default();
    let mut start = 0;
    for end in 1..=stream.len() {
        let bit = end - 1;
        let cut = data
            .get(bit / 8)
            .is_some_and(|byte| byte & (1 << (bit % 8)) != 0);
        if cut || end == stream.len() {
            assert!(split.feed(&stream[start..end]).is_ok());
            start = end;
        }
    }
    assert_eq!(split.take_events(), expected);
    assert_eq!(split.replies(), whole.replies());

    // Reject both oversized and malformed UTF-8 pastes. Valid input following
    // the terminator in the same chunk, and in later chunks, must survive.
    let mut rejected = b"\x1b[200~".to_vec();
    if data.first().is_some_and(|byte| byte & 1 != 0) {
        rejected.resize(
            rejected.len() + PROBE_LIMIT + 1 + data.len().min(1024),
            b'p',
        );
    } else {
        rejected.push(0xff);
    }
    rejected.extend_from_slice(b"\x1b[201~");
    rejected.extend_from_slice(&stream);
    assert!(split.feed(&rejected).is_err());
    assert_eq!(split.take_events(), expected);
    assert!(split.feed(&stream).is_ok());
    assert_eq!(split.take_events(), expected);
});
