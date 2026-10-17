use crate::key_event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use crate::{Action, Binding, InputKeymap};
use std::fmt::Write as _;

/// A decoded editing key, or committed text from a terminal character event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DecodedKey {
    /// A binding from the declarative keymap.
    Action {
        /// Action selected by the keymap.
        action: Action,
        /// Whether a motion extends selection.
        select: bool,
    },
    /// One terminal character event; hosts may also deliver whole IME commits.
    Text(char),
}

/// Resolve an already decoded key through the declarative editor bindings.
/// Releases do not edit; repeats only invoke repeatable editing actions.
pub fn decode_key(mut key: KeyEvent, bindings: &InputKeymap) -> Option<DecodedKey> {
    if key.kind == KeyEventKind::Release {
        return None;
    }
    if key.code == KeyCode::BackTab {
        key.code = KeyCode::Tab;
        key.modifiers.insert(KeyModifiers::SHIFT);
    }
    // macOS Cmd is the action modifier for character shortcuts. Keep Cmd
    // arrows intact because those mean line/buffer motion in the keymap.
    if matches!(key.code, KeyCode::Char(_)) && key.modifiers.contains(KeyModifiers::SUPER) {
        key.modifiers.remove(KeyModifiers::SUPER);
        key.modifiers.insert(KeyModifiers::CONTROL);
    }
    if event_chord(key).is_some_and(|chord| bindings.disabled(&chord)) {
        return None;
    }
    if let Some(binding) = find_binding(bindings, key) {
        if key.kind == KeyEventKind::Repeat && !binding.repeat {
            return None;
        }
        return Some(DecodedKey::Action {
            action: binding.action,
            select: false,
        });
    }
    if key.modifiers.contains(KeyModifiers::SHIFT) {
        let mut unshifted = key;
        unshifted.modifiers.remove(KeyModifiers::SHIFT);
        if let Some(binding) = find_binding(bindings, unshifted) {
            if matches!(
                binding.action,
                Action::Move(_) | Action::Delete(_) | Action::DeleteWord(_)
            ) {
                if key.kind == KeyEventKind::Repeat && !binding.repeat {
                    return None;
                }
                return Some(DecodedKey::Action {
                    action: binding.action,
                    select: true,
                });
            }
        }
    }
    if key.modifiers.difference(KeyModifiers::SHIFT).is_empty() {
        if let KeyCode::Char(character) = key.code {
            if !character.is_control() {
                return Some(DecodedKey::Text(character));
            }
        }
    }
    None
}

fn find_binding(bindings: &InputKeymap, key: KeyEvent) -> Option<Binding<'_>> {
    let chord = event_chord(key)?;
    bindings.resolve(&chord)
}

/// Canonical chord spelling for user overrides and keymap help.
pub fn key_chord(key: KeyEvent) -> Option<String> {
    event_chord(key).map(|chord| chord.as_str().to_owned())
}

// Four modifier labels (21 bytes), then at most three lowercased Unicode
// scalars (12 bytes), or the longest named key (9 bytes), fit in 40 bytes.
pub(crate) fn event_chord(mut key: KeyEvent) -> Option<arrayvec::ArrayString<40>> {
    if key.code == KeyCode::BackTab {
        key.code = KeyCode::Tab;
        key.modifiers.insert(KeyModifiers::SHIFT);
    }
    if matches!(key.code, KeyCode::Char(_)) && key.modifiers.contains(KeyModifiers::SUPER) {
        key.modifiers.remove(KeyModifiers::SUPER);
        key.modifiers.insert(KeyModifiers::CONTROL);
    }
    let name = match key.code {
        KeyCode::Backspace => "Backspace",
        KeyCode::Enter => "Enter",
        KeyCode::Left => "Left",
        KeyCode::Right => "Right",
        KeyCode::Up => "Up",
        KeyCode::Down => "Down",
        KeyCode::Home => "Home",
        KeyCode::End => "End",
        KeyCode::PageUp => "PageUp",
        KeyCode::PageDown => "PageDown",
        KeyCode::Tab => "Tab",
        KeyCode::Delete => "Delete",
        KeyCode::Insert => "Insert",
        KeyCode::F(_) => "",
        KeyCode::Char(' ') => "Space",
        KeyCode::Esc => "Esc",
        KeyCode::Char(_) => "",
        _ => return None,
    };
    let mut chord = arrayvec::ArrayString::new();
    for (modifier, label) in [
        (KeyModifiers::CONTROL, "Ctrl+"),
        (KeyModifiers::ALT, "Alt+"),
        (KeyModifiers::SHIFT, "Shift+"),
        (KeyModifiers::SUPER, "Super+"),
    ] {
        if key.modifiers.contains(modifier) {
            chord.push_str(label);
        }
    }
    if key
        .modifiers
        .intersects(KeyModifiers::HYPER | KeyModifiers::META)
    {
        return None;
    }
    if let KeyCode::F(number) = key.code {
        write!(&mut chord, "F{number}").ok()?;
    } else if key.code == KeyCode::Char(' ') {
        chord.push_str("Space");
    } else if let KeyCode::Char(character) = key.code {
        for character in character.to_lowercase() {
            chord.push(character);
        }
    } else {
        chord.push_str(name);
    }
    Some(chord)
}
