use acyclic_harness_tui::key_event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers as M};
use acyclic_harness_tui::{Action, DecodedKey, InputKeymap, Motion, decode_key, key_chord};

#[test]
fn repeat_policy_selection_modifiers_and_platform_keys() {
    for (code, mods, expected, select) in [
        (KeyCode::Left, M::SHIFT, Action::Move(Motion::Left), true),
        (
            KeyCode::Left,
            M::SHIFT | M::CONTROL,
            Action::Move(Motion::WordLeft),
            true,
        ),
        (
            KeyCode::Left,
            M::SUPER,
            Action::Move(Motion::LineStart),
            false,
        ),
        (KeyCode::Up, M::SUPER, Action::Move(Motion::Start), false),
        (KeyCode::Char('a'), M::SUPER, Action::SelectAll, false),
        (
            KeyCode::Char('Z'),
            M::SHIFT | M::CONTROL,
            Action::Redo,
            false,
        ),
        (KeyCode::BackTab, M::SHIFT, Action::PreviousDock, false),
        (KeyCode::Enter, M::ALT, Action::Newline, false),
    ] {
        assert_eq!(
            decode_key(KeyEvent::new(code, mods), &InputKeymap::default()),
            Some(DecodedKey::Action {
                action: expected,
                select
            })
        );
    }
    for code in [
        KeyCode::Char('c'),
        KeyCode::Char('v'),
        KeyCode::Char('x'),
        KeyCode::Char('r'),
    ] {
        let mut key = KeyEvent::new(code, M::CONTROL);
        key.kind = KeyEventKind::Repeat;
        assert_eq!(decode_key(key, &InputKeymap::default()), None);
    }
    let mut left = KeyEvent::new(KeyCode::Left, M::CONTROL);
    left.kind = KeyEventKind::Repeat;
    assert_eq!(
        decode_key(left, &InputKeymap::default()),
        Some(DecodedKey::Action {
            action: Action::Move(Motion::WordLeft),
            select: false
        })
    );
    left.kind = KeyEventKind::Release;
    assert_eq!(decode_key(left, &InputKeymap::default()), None);
    assert_eq!(
        key_chord(KeyEvent::new(KeyCode::BackTab, M::SHIFT)),
        Some("Shift+Tab".into())
    );
}

#[test]
fn user_overrides_drive_dispatch_help_and_disabling() -> Result<(), Box<dyn std::error::Error>> {
    let map =
        InputKeymap::parse(r#"{"bindings":{"Ctrl+l":"cursor.line-start","Ctrl+c":null,"a":null}}"#)
            .map_err(|errors| format!("{errors:?}"))?;
    assert_eq!(
        decode_key(KeyEvent::new(KeyCode::Char('l'), M::CONTROL), &map),
        Some(DecodedKey::Action {
            action: Action::Move(Motion::LineStart),
            select: false
        })
    );
    assert_eq!(
        decode_key(KeyEvent::new(KeyCode::Char('c'), M::CONTROL), &map),
        None
    );
    assert_eq!(
        decode_key(KeyEvent::new(KeyCode::Char('a'), M::NONE), &map),
        None
    );
    assert!(map.help().contains("Ctrl+l\tLine start\n"));
    assert!(!map.help().contains("Ctrl+c\t"));
    assert_eq!(InputKeymap::parse(&map.encode()), Ok(map));
    assert!(InputKeymap::parse(r#"{"bindings":{"Shift+Ctrl+l":"cursor.line-start"}}"#).is_err());
    assert!(InputKeymap::parse(r#"{"bindings":{"Ctrl+l":"typo"}}"#).is_err());
    Ok(())
}

#[test]
fn super_overrides_share_canonical_dispatch_and_help() -> Result<(), Box<dyn std::error::Error>> {
    let map = InputKeymap::parse(
        r#"{"bindings":{"Super+k":"cursor.line-start","Super+x Super+e":"external-editor"}}"#,
    )
    .map_err(|errors| format!("{errors:?}"))?;
    for modifiers in [M::SUPER, M::CONTROL] {
        assert_eq!(
            decode_key(KeyEvent::new(KeyCode::Char('k'), modifiers), &map),
            Some(DecodedKey::Action {
                action: Action::Move(Motion::LineStart),
                select: false
            })
        );
    }
    assert_eq!(
        map.resolve("Ctrl+k").map(|b| b.action),
        Some(Action::Move(Motion::LineStart))
    );
    assert!(map.help().contains("Ctrl+k\tLine start"));
    assert_eq!(
        map.resolve("Ctrl+x Ctrl+e").map(|b| b.action),
        Some(Action::ExternalEditor)
    );
    assert_eq!(InputKeymap::parse(&map.encode()), Ok(map));
    assert!(
        InputKeymap::parse(
            r#"{"bindings":{"Super+k":"cursor.line-start","Ctrl+k":"cursor.line-end"}}"#
        )
        .is_err()
    );
    Ok(())
}

fn assert_dispatch_allocates_nothing(broken: bool) -> Result<(), Box<dyn std::error::Error>> {
    let overrides = InputKeymap::parse(r#"{"bindings":{"Super+k":"cursor.line-start","a":null,"Ctrl+x Ctrl+e":"cursor.line-start"}}"#)
        .map_err(|errors| format!("{errors:?}"))?;
    for map in [InputKeymap::default(), overrides] {
        for code in [
            KeyCode::Char('a'),
            KeyCode::Char('Z'),
            KeyCode::Char('\u{754c}'),
            KeyCode::Char('\u{130}'),
            KeyCode::Char('\u{1f980}'),
            KeyCode::Left,
            KeyCode::Home,
            KeyCode::Backspace,
            KeyCode::Enter,
            KeyCode::BackTab,
        ] {
            for modifiers in [
                M::NONE,
                M::SHIFT,
                M::CONTROL,
                M::ALT,
                M::SUPER,
                M::SHIFT | M::CONTROL,
                M::CONTROL | M::ALT | M::SHIFT | M::SUPER,
            ] {
                for kind in [
                    KeyEventKind::Press,
                    KeyEventKind::Repeat,
                    KeyEventKind::Release,
                ] {
                    let mut event = KeyEvent::new(code, modifiers);
                    event.kind = kind;
                    let allocation = acyclic_test_support::measure(|| {
                        if broken {
                            // Negative control: recreate the old heap chord on the hot path.
                            std::hint::black_box(key_chord(event));
                        }
                        std::hint::black_box(decode_key(event, &map));
                        std::hint::black_box(map.resolve("Ctrl+x Ctrl+e"));
                    });
                    assert_eq!(
                        allocation.count_total, 0,
                        "keypress dispatch allocated: {event:?}"
                    );
                }
            }
        }
    }
    Ok(())
}

#[test]
fn default_and_override_keypress_dispatch_allocate_zero_times()
-> Result<(), Box<dyn std::error::Error>> {
    assert_dispatch_allocates_nothing(false)
}

#[test]
#[should_panic(expected = "keypress dispatch allocated")]
fn allocation_negative_control_rejects_heap_chord_dispatch() {
    let _ = assert_dispatch_allocates_nothing(true);
}
