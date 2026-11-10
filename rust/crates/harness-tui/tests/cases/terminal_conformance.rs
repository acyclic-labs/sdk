//! Wire conformance, transport partition invariance and recovery after rejection.
#![cfg(feature = "tui")]

use acyclic_harness_tui::key_event::{
    InputEvent, KeyCode, KeyEvent, KeyEventKind, KeyEventState, KeyModifiers,
};
use acyclic_harness_tui::term::{Decoder, PROBE_LIMIT, encode_key};
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};
use std::io;

#[path = "../support/terminal_keys.rs"]
mod table;

fn event(code: KeyCode, modifiers: KeyModifiers) -> InputEvent {
    InputEvent::Key(KeyEvent::new(code, modifiers))
}

fn decoded(bytes: &[u8]) -> io::Result<Vec<InputEvent>> {
    let mut decoder = Decoder::default();
    decoder.feed(bytes)?;
    decoder.finish_input()?;
    Ok(decoder.take_events())
}

fn modified(body: &str, modifiers: u16, kind: Option<u8>) -> String {
    let (number, action) = body.split_at(body.len() - 1);
    let number = if number.is_empty() { "1" } else { number };
    match kind {
        Some(kind) => format!("\x1b[{number};{modifiers}:{kind}{action}"),
        None => format!("\x1b[{number};{modifiers}{action}"),
    }
}

#[test]
fn every_supported_xterm_numbered_and_cursor_sequence() -> io::Result<()> {
    for &(code, body) in table::NUMBERED.iter().chain(table::FINAL) {
        assert_eq!(
            decoded(format!("\x1b[{body}").as_bytes())?,
            vec![event(code, KeyModifiers::NONE)],
            "{body}"
        );
        for value in 1u16..=16 {
            let bits = value - 1;
            let mut modifiers = KeyModifiers::NONE;
            for (bit, flag) in [
                (1, KeyModifiers::SHIFT),
                (2, KeyModifiers::ALT),
                (4, KeyModifiers::CONTROL),
                (8, KeyModifiers::META),
            ] {
                if bits & bit != 0 {
                    modifiers.insert(flag);
                }
            }
            assert_eq!(
                decoded(modified(body, value, None).as_bytes())?,
                vec![event(code, modifiers)],
                "{body};{value}"
            );
        }
    }
    for &(code, body) in table::FINAL {
        assert_eq!(
            decoded(format!("\x1bO{body}").as_bytes())?,
            vec![event(code, KeyModifiers::NONE)]
        );
    }
    assert_eq!(
        decoded(b"\x1b[Z")?,
        vec![event(KeyCode::BackTab, KeyModifiers::SHIFT)]
    );
    Ok(())
}

#[test]
fn every_supported_kitty_functional_sequence_and_encoder_bytes() -> io::Result<()> {
    for &(code, body) in table::KITTY {
        for value in 1u16..=256 {
            for (kind_number, kind) in [
                (1, KeyEventKind::Press),
                (2, KeyEventKind::Repeat),
                (3, KeyEventKind::Release),
            ] {
                let bits = value - 1;
                let mut modifiers = KeyModifiers::NONE;
                for (bit, flag) in [
                    (1, KeyModifiers::SHIFT),
                    (2, KeyModifiers::ALT),
                    (4, KeyModifiers::CONTROL),
                    (8, KeyModifiers::SUPER),
                    (16, KeyModifiers::HYPER),
                    (32, KeyModifiers::META),
                ] {
                    if bits & bit != 0 {
                        modifiers.insert(flag);
                    }
                }
                let mut state = KeyEventState::NONE;
                if bits & 64 != 0 {
                    state.insert(KeyEventState::CAPS_LOCK);
                }
                if bits & 128 != 0 {
                    state.insert(KeyEventState::NUM_LOCK);
                }
                let key = KeyEvent {
                    code,
                    modifiers,
                    kind,
                    state,
                };
                let bytes = modified(body, value, Some(kind_number));
                assert_eq!(
                    encode_key(key)?,
                    bytes.as_bytes(),
                    "{body};{value}:{kind_number}"
                );
                assert_eq!(
                    decoded(bytes.as_bytes())?,
                    vec![InputEvent::Key(key)],
                    "{body};{value}:{kind_number}"
                );
            }
        }
    }
    Ok(())
}

#[test]
fn alternate_keys_modify_other_keys_and_legacy_text() -> io::Result<()> {
    for (bytes, code, modifiers) in [
        ("\x1b[97:65:113;2u", KeyCode::Char('a'), KeyModifiers::SHIFT),
        ("\x1b[27;5;13~", KeyCode::Enter, KeyModifiers::CONTROL),
        ("\x1b[27;9;120~", KeyCode::Char('x'), KeyModifiers::META),
        ("\x1bx", KeyCode::Char('x'), KeyModifiers::ALT),
        ("\x1b界", KeyCode::Char('界'), KeyModifiers::ALT),
        ("\t", KeyCode::Tab, KeyModifiers::NONE),
        ("\r\n", KeyCode::Enter, KeyModifiers::NONE),
        ("\x7f", KeyCode::Backspace, KeyModifiers::NONE),
        ("\x00", KeyCode::Null, KeyModifiers::NONE),
    ] {
        assert_eq!(
            decoded(bytes.as_bytes())?,
            vec![event(code, modifiers)],
            "{bytes:?}"
        );
    }
    Ok(())
}

#[test]
fn legacy_control_and_printable_key_table() -> io::Result<()> {
    for byte in 0u8..=127 {
        let (code, modifiers) = match byte {
            0 => (KeyCode::Null, KeyModifiers::NONE),
            8 | 127 => (KeyCode::Backspace, KeyModifiers::NONE),
            9 => (KeyCode::Tab, KeyModifiers::NONE),
            10 | 13 => (KeyCode::Enter, KeyModifiers::NONE),
            27 => (KeyCode::Esc, KeyModifiers::NONE),
            1..=26 => (KeyCode::Char(char::from(byte + 96)), KeyModifiers::CONTROL),
            28..=31 => (KeyCode::Char(char::from(byte + 64)), KeyModifiers::CONTROL),
            _ => (KeyCode::Char(char::from(byte)), KeyModifiers::NONE),
        };
        assert_eq!(
            decoded(&[byte])?,
            vec![event(code, modifiers)],
            "byte={byte}"
        );
    }
    let mut decoder = Decoder::default();
    assert!(decoder.feed(b"\xc3\xffx\x1b[15~").is_err());
    assert_eq!(decoder.take_events(), decoded(b"x\x1b[15~")?);
    decoder.feed(b"y")?;
    assert_eq!(decoder.take_events(), decoded(b"y")?);
    decoder.feed(b"\r")?;
    decoder.take_events();
    assert!(decoder.feed(b"\xff").is_err());
    decoder.feed(b"\n")?;
    assert_eq!(decoder.take_events(), decoded(b"\n")?);
    Ok(())
}

fn rejected_paste(length: usize) -> Vec<u8> {
    let mut bytes = b"\x1b[200~".to_vec();
    bytes.resize(bytes.len() + length, b'p');
    bytes.extend_from_slice(b"\x1b[201~");
    bytes
}

#[test]
fn rejected_pastes_drain_and_preserve_the_suffix() -> io::Result<()> {
    for length in [PROBE_LIMIT, PROBE_LIMIT + 1, 17 * 1024, PROBE_LIMIT * 4] {
        for chunk in [1, 5, 6, 1024, usize::MAX] {
            let mut bytes = rejected_paste(length);
            bytes.extend_from_slice(b"x\x1b[15;5~\x1b[200~ok\x1b[201~");
            let mut decoder = Decoder::default();
            let errors = bytes
                .chunks(chunk)
                .filter(|part| decoder.feed(part).is_err())
                .count();
            assert_eq!(errors, usize::from(length > PROBE_LIMIT));
            let mut expected = Vec::new();
            if length == PROBE_LIMIT {
                expected.push(InputEvent::Paste("p".repeat(length)));
            }
            expected.extend([
                event(KeyCode::Char('x'), KeyModifiers::NONE),
                event(KeyCode::F(5), KeyModifiers::CONTROL),
                InputEvent::Paste("ok".into()),
            ]);
            assert_eq!(decoder.take_events(), expected);
            decoder.feed(b"z")?;
            assert_eq!(
                decoder.take_events(),
                vec![event(KeyCode::Char('z'), KeyModifiers::NONE)]
            );
        }
    }
    let mut decoder = Decoder::default();
    assert!(decoder.feed(b"\x1b[200~\xff\x1b[201~x").is_err());
    assert_eq!(
        decoder.take_events(),
        vec![event(KeyCode::Char('x'), KeyModifiers::NONE)]
    );
    decoder.feed(b"y")?;
    assert_eq!(
        decoder.take_events(),
        vec![event(KeyCode::Char('y'), KeyModifiers::NONE)]
    );
    Ok(())
}

fn split_check(indices: &[usize], cuts: &[bool], broken: bool) -> Result<(), TestCaseError> {
    let mut bytes = Vec::new();
    for index in indices {
        if let Some((_, body)) = table::KITTY.get(index % table::KITTY.len()) {
            bytes.extend_from_slice(modified(body, 6, Some(1)).as_bytes());
        }
    }
    let expected = decoded(&bytes)?;
    let mut decoder = Decoder::default();
    let mut start = 0;
    for end in 1..=bytes.len() {
        if cuts.get(end - 1).copied().unwrap_or(false) || end == bytes.len() {
            decoder.feed(bytes.get(start..end).unwrap_or_default())?;
            if broken {
                decoder.finish_input()?;
            }
            start = end;
        }
    }
    decoder.finish_input()?;
    prop_assert_eq!(decoder.take_events(), expected);
    Ok(())
}

fn recovery_check(extra: usize, broken: bool) -> Result<(), TestCaseError> {
    let mut decoder = Decoder::default();
    let mut bytes = rejected_paste(PROBE_LIMIT + 1 + extra);
    bytes.extend_from_slice(b"x\x1b[15~");
    prop_assert!(decoder.feed(&bytes).is_err());
    if broken {
        decoder.feed(b"\x1b[200~")?;
    }
    let mut actual = decoder.take_events();
    decoder.feed(b"y\x1b[4~")?;
    actual.extend(decoder.take_events());
    prop_assert_eq!(actual, decoded(b"x\x1b[15~y\x1b[4~")?);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn arbitrary_partitions_preserve_valid_streams(indices in prop::collection::vec(any::<usize>(), 1..20), cuts in prop::collection::vec(any::<bool>(), 0..512)) {
        split_check(&indices, &cuts, false)?;
    }
    #[test]
    fn rejection_never_poisons_later_valid_input(extra in 0usize..1024) {
        recovery_check(extra, false)?;
    }
}

#[test]
fn negative_controls_detect_chunk_finalization_and_poisoned_recovery() {
    let config = || Config {
        cases: 16,
        failure_persistence: None,
        ..Config::default()
    };
    let split_witness = Just((vec![0usize], vec![true]));
    assert!(
        TestRunner::new(config())
            .run(&split_witness, |(indices, cuts)| split_check(
                &indices, &cuts, true
            ))
            .is_err()
    );
    assert!(
        TestRunner::new(config())
            .run(&(0usize..64), |extra| recovery_check(extra, true))
            .is_err()
    );
}
