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
    let map = InputKeymap::from_json(r#"{"Ctrl+l":"Home","Ctrl+c":"","a":""}"#)?;
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
    assert_eq!(InputKeymap::from_json(&map.to_json()?)?, map);
    assert!(InputKeymap::from_json(r#"{"Shift+Ctrl+l":"Home"}"#).is_err());
    assert!(InputKeymap::from_json(r#"{"Ctrl+l":"typo"}"#).is_err());
    Ok(())
}

#[test]
fn super_overrides_share_canonical_dispatch_and_help() -> Result<(), Box<dyn std::error::Error>> {
    let map = InputKeymap::from_json(r#"{"Super+k":"Home","Super+x Super+e":"Ctrl+x Ctrl+e"}"#)?;
    for modifiers in [M::SUPER, M::CONTROL] {
        assert_eq!(
            decode_key(KeyEvent::new(KeyCode::Char('k'), modifiers), &map),
            Some(DecodedKey::Action {
                action: Action::Move(Motion::LineStart),
                select: false
            })
        );
    }
    assert_eq!(map.resolve("Super+k"), map.resolve("Ctrl+k"));
    assert!(map.help().contains("Ctrl+k\tLine start"));
    assert_eq!(map.resolve("Super+x Super+e"), map.resolve("Ctrl+x Ctrl+e"));
    assert_eq!(InputKeymap::from_json(&map.to_json()?)?, map);
    assert!(InputKeymap::from_json(r#"{"Super+k":"Home","Ctrl+k":"End"}"#).is_err());
    Ok(())
}
