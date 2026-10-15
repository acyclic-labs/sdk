use crate::key_event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use crate::{Action, Binding, InputKeymap};

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
    Text(String),
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
    if key_chord(key).is_some_and(|chord| bindings.disabled(&chord)) {
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
                return Some(DecodedKey::Text(character.to_string()));
            }
        }
    }
    None
}

fn find_binding(bindings: &InputKeymap, key: KeyEvent) -> Option<Binding> {
    let chord = key_chord(key)?;
    bindings.resolve(&chord)
}

/// Canonical chord spelling for user overrides and keymap help.
pub fn key_chord(mut key: KeyEvent) -> Option<String> {
    if key.code == KeyCode::BackTab {
        key.code = KeyCode::Tab;
        key.modifiers.insert(KeyModifiers::SHIFT);
    }
    if matches!(key.code, KeyCode::Char(_)) && key.modifiers.contains(KeyModifiers::SUPER) {
        key.modifiers.remove(KeyModifiers::SUPER);
        key.modifiers.insert(KeyModifiers::CONTROL);
    }
    let name = match key.code {
        KeyCode::Backspace => "Backspace".to_owned(),
        KeyCode::Enter => "Enter".to_owned(),
        KeyCode::Left => "Left".to_owned(),
        KeyCode::Right => "Right".to_owned(),
        KeyCode::Up => "Up".to_owned(),
        KeyCode::Down => "Down".to_owned(),
        KeyCode::Home => "Home".to_owned(),
        KeyCode::End => "End".to_owned(),
        KeyCode::PageUp => "PageUp".to_owned(),
        KeyCode::PageDown => "PageDown".to_owned(),
        KeyCode::Tab => "Tab".to_owned(),
        KeyCode::BackTab => "Shift+Tab".to_owned(),
        KeyCode::Delete => "Delete".to_owned(),
        KeyCode::Esc => "Esc".to_owned(),
        KeyCode::Char(character) => character.to_lowercase().collect(),
        _ => return None,
    };
    let mut chord = String::new();
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
    chord.push_str(&name);
    Some(chord)
}
