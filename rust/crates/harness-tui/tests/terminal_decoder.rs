//! Production input/reply classification and key-codec verification.
#![cfg(feature = "tui")]

use acyclic_harness_tui::key_event::{
    InputEvent, KeyCode, KeyEvent, KeyEventKind, KeyEventState, KeyModifiers,
};
use acyclic_harness_tui::term::*;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};
use std::io;

fn key_events(text: &str) -> Vec<InputEvent> {
    text.chars()
        .map(|c| InputEvent::Key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE)))
        .collect()
}

#[test]
fn da1_only_does_not_trust_environment_features() -> io::Result<()> {
    let env = Environment(
        [
            ("TERM_PROGRAM".into(), "kitty".into()),
            ("COLORTERM".into(), "truecolor".into()),
        ]
        .into(),
    );
    let mut decoder = Decoder::default();
    decoder.feed(b"\x1b[?62;1c")?;
    let caps = decoder.replies().capabilities(&env);
    assert_eq!(caps.keyboard, KeyboardProtocol::Legacy);
    assert_eq!(caps.images, ImageProtocol::Placeholder);
    assert_eq!(caps.colour, ColourDepth::Ansi16);
    assert!(!caps.clipboard);
    let fallback = Decoder::default().replies().capabilities(&env);
    assert_eq!(fallback.keyboard, KeyboardProtocol::Kitty);
    assert_eq!(fallback.colour, ColourDepth::TrueColour);
    Ok(())
}

#[test]
fn late_and_partial_replies_never_become_input() -> io::Result<()> {
    for reply in [
        b"\x1b]11;rgb:ffff/0000/0000\x07".as_slice(),
        b"\x1bP1+r524742=31\x1b\\",
        b"\x1b_Gi=314159;OK\x1b\\",
        b"\x1b[?2004;2$y",
    ] {
        for split in 0..=reply.len() {
            let mut decoder = Decoder::default();
            decoder.feed(b"a\x1b[?62;1c")?;
            decoder.feed(reply.get(..split).ok_or(io::ErrorKind::InvalidInput)?)?;
            // Taking events at a startup deadline preserves partial framing.
            let mut events = decoder.take_events();
            decoder.feed(reply.get(split..).ok_or(io::ErrorKind::InvalidInput)?)?;
            decoder.feed(b"b")?;
            events.extend(decoder.take_events());
            assert_eq!(events, key_events("ab"), "split={split}");
        }
        let wrapped = Multiplexer::Tmux.wrap(reply);
        let mut decoder = Decoder::default();
        decoder.feed(b"a\x1b[?62;1c")?;
        for chunk in wrapped.chunks(1) {
            decoder.feed(chunk)?;
        }
        decoder.feed(b"b")?;
        assert_eq!(decoder.take_events(), key_events("ab"));
    }
    Ok(())
}

#[test]
fn input_deadline_only_resolves_an_isolated_escape() -> io::Result<()> {
    let mut decoder = Decoder::default();
    decoder.feed(b"\x1b")?;
    decoder.finish_input()?;
    assert_eq!(
        decoder.take_events(),
        vec![InputEvent::Key(KeyEvent::new(
            KeyCode::Esc,
            KeyModifiers::NONE
        ))]
    );
    decoder.feed(b"x")?;
    assert_eq!(decoder.take_events(), key_events("x"));
    for reply in [
        b"\x1b]11;rgb:ffff/0000/0000\x07".as_slice(),
        b"\x1bP1+r524742\x1b\\",
        b"\x1b[?2026;2$y",
    ] {
        for split in 2..reply.len() {
            let mut decoder = Decoder::default();
            decoder.feed(reply.get(..split).ok_or(io::ErrorKind::InvalidData)?)?;
            decoder.finish_input()?;
            assert!(decoder.take_events().is_empty());
            decoder.feed(reply.get(split..).ok_or(io::ErrorKind::InvalidData)?)?;
            decoder.feed(b"x")?;
            assert_eq!(decoder.take_events(), key_events("x"));
        }
    }
    Ok(())
}

#[test]
fn input_protocols_share_the_reply_decoder() -> io::Result<()> {
    let mut decoder = Decoder::default();
    decoder.feed(b"\x1b[?62;1c\x1b[13;6:3u\x1b[27;3;13~\x1b[I\x1b[O\x1b[<0;4;2M\r\n")?;
    let events = decoder.take_events();
    assert_eq!(events.len(), 6);
    assert!(
        matches!(events.first(), Some(InputEvent::Key(key)) if key.code == KeyCode::Enter
        && key.modifiers == (KeyModifiers::CONTROL | KeyModifiers::SHIFT) && key.kind == KeyEventKind::Release)
    );
    assert!(
        matches!(events.get(1), Some(InputEvent::Key(key)) if key.modifiers == KeyModifiers::ALT)
    );
    assert_eq!(events.get(2), Some(&InputEvent::Focus(true)));
    assert_eq!(events.get(3), Some(&InputEvent::Focus(false)));
    assert!(
        matches!(events.get(4), Some(InputEvent::Mouse(mouse)) if mouse.column == 3 && mouse.row == 1)
    );
    assert!(matches!(events.get(5), Some(InputEvent::Key(key)) if key.code == KeyCode::Enter));
    decoder.feed(b"\x1b[200~paste\x1b]11;?\x07\x1a\x1b[201~")?;
    assert_eq!(
        decoder.take_events(),
        vec![InputEvent::Paste("paste\x1b]11;?\x07\x1a".into())]
    );
    Ok(())
}

fn keys() -> impl Strategy<Value = KeyEvent> {
    let code = prop_oneof![
        any::<char>()
            .prop_filter("kitty literal text", |c| !c.is_control()
                && !(57344..=63743).contains(&u32::from(*c)))
            .prop_map(KeyCode::Char),
        prop::sample::select(vec![
            KeyCode::Enter,
            KeyCode::Tab,
            KeyCode::Esc,
            KeyCode::Backspace,
            KeyCode::Left,
            KeyCode::Right,
            KeyCode::Up,
            KeyCode::Down,
            KeyCode::Home,
            KeyCode::End,
            KeyCode::PageUp,
            KeyCode::PageDown,
            KeyCode::Insert,
            KeyCode::Delete,
            KeyCode::Null,
            KeyCode::CapsLock,
            KeyCode::ScrollLock,
            KeyCode::NumLock,
            KeyCode::PrintScreen,
            KeyCode::Pause,
            KeyCode::Menu
        ]),
        (1u8..=35).prop_map(KeyCode::F),
    ];
    (code, 0u8..64, 0u8..3, 0u8..4).prop_map(|(code, modifiers, kind, state)| KeyEvent {
        code,
        modifiers: KeyModifiers::from_bits_retain(modifiers),
        kind: match kind {
            0 => KeyEventKind::Press,
            1 => KeyEventKind::Repeat,
            _ => KeyEventKind::Release,
        },
        state: (if state & 1 != 0 {
            KeyEventState::CAPS_LOCK
        } else {
            KeyEventState::NONE
        }) | (if state & 2 != 0 {
            KeyEventState::NUM_LOCK
        } else {
            KeyEventState::NONE
        }),
    })
}

fn round_trip(key: KeyEvent, broken: u8, chunk: usize) -> Result<(), TestCaseError> {
    let mut encoded_key = key;
    if broken == 1 {
        encoded_key.modifiers.remove(KeyModifiers::CONTROL);
    }
    if broken == 2 {
        encoded_key.code = KeyCode::Enter;
    }
    let bytes = encode_key(encoded_key)?;
    let mut decoder = Decoder::default();
    for bytes in bytes.chunks(chunk) {
        decoder.feed(bytes)?;
    }
    prop_assert_eq!(decoder.take_events(), vec![InputEvent::Key(key)]);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    // Every representable key, all six modifiers, both lock states and all
    // three event kinds survive the production encoder and streaming decoder.
    #[test]
    fn decode_encode_identity(key in keys(), chunk in 1usize..32) {
        round_trip(key, 0, chunk)?;
    }
}

#[test]
fn negative_controls_detect_lost_modifiers_and_broken_key_mapping() {
    // Every generated input exposes both defects, independently of RNG seed.
    let witnesses = keys().prop_map(|mut key| {
        key.modifiers.insert(KeyModifiers::CONTROL);
        key.code = KeyCode::Char('x');
        key
    });
    for broken in [1, 2] {
        let mut runner = TestRunner::new(Config {
            cases: 64,
            failure_persistence: None,
            ..Config::default()
        });
        assert!(
            runner
                .run(&witnesses, |key| round_trip(key, broken, 1))
                .is_err()
        );
    }
}
