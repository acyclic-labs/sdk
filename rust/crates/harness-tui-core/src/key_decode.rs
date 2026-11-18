use crate::input_keymap::KeyBinding;
use crate::key_event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use crate::{Action, InputKeymap};
use arrayvec::ArrayString;
use std::fmt::Write as _;

// Four modifier labels (21 bytes), then at most three lowercased Unicode
// scalars (12 bytes), or the longest named key (9 bytes), fit in 40 bytes.
const CHORD_BYTES: usize = 40;

/// A decoded editing key, or committed text from a terminal character event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

/// A pending first key and the edit it would perform on its own.
#[derive(Clone, Debug)]
pub struct PendingChord {
    prefix: ArrayString<CHORD_BYTES>,
    started_ms: u64,
    fallback: Option<DecodedKey>,
}

/// How a key continues a pending two-key binding.
pub enum ChordContinuation {
    /// A repeat of the first key leaves the prefix pending.
    Repeat,
    /// The key completed a binding.
    Matched(DecodedKey),
    /// The pending first key must be handled before this key.
    Unmatched,
}

impl PendingChord {
    /// Start a prefix for a pressed key with a sequence or cut fallback.
    pub fn start(
        key: KeyEvent,
        bindings: &InputKeymap,
        started_ms: u64,
        fallback: Option<DecodedKey>,
    ) -> Option<Self> {
        if key.kind != KeyEventKind::Press {
            return None;
        }
        let prefix = event_chord(key)?;
        (bindings.has_prefix(&prefix)
            || matches!(
                fallback,
                Some(DecodedKey::Action {
                    action: Action::CutPrefix,
                    ..
                })
            ))
        .then_some(Self {
            prefix,
            started_ms,
            fallback,
        })
    }

    /// Whether the caller's monotonic clock has reached the prefix deadline.
    pub fn timed_out(&self, now_ms: u64, timeout_ms: u64) -> bool {
        now_ms.saturating_sub(self.started_ms) >= timeout_ms
    }

    /// Consume the pending key and recover its original edit and timestamp.
    pub fn into_fallback(self) -> Option<(u64, DecodedKey)> {
        self.fallback.map(|decoded| (self.started_ms, decoded))
    }

    /// Match a second key in a buffer sized for two canonical chords.
    pub fn continuation(&self, key: KeyEvent, bindings: &InputKeymap) -> ChordContinuation {
        let Some(next) = event_chord(key) else {
            return ChordContinuation::Unmatched;
        };
        match key.kind {
            KeyEventKind::Repeat if next == self.prefix => ChordContinuation::Repeat,
            KeyEventKind::Press => {
                let mut sequence = ArrayString::<{ 2 * CHORD_BYTES + 1 }>::new();
                if write!(sequence, "{} {next}", self.prefix).is_err() {
                    return ChordContinuation::Unmatched;
                }
                bindings
                    .resolve(&sequence)
                    .map_or(ChordContinuation::Unmatched, |binding| {
                        ChordContinuation::Matched(DecodedKey::Action {
                            action: binding.action,
                            select: false,
                        })
                    })
            }
            _ => ChordContinuation::Unmatched,
        }
    }
}

/// Resolve an already decoded key through the declarative editor bindings.
/// Releases do not edit; repeats only invoke repeatable editing actions.
pub fn decode_key(key: KeyEvent, bindings: &InputKeymap) -> Option<DecodedKey> {
    if key.kind == KeyEventKind::Release {
        return None;
    }
    let key = normalize_key(key);
    let chord = normalized_chord(key);
    let binding = match chord.as_ref().map(|chord| bindings.lookup(chord)) {
        Some(KeyBinding::Disabled) => return None,
        Some(KeyBinding::Action(binding)) => Some(binding),
        Some(KeyBinding::Unbound) | None => None,
    };
    if let Some(binding) = binding {
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
        if let Some(binding) = normalized_chord(unshifted)
            .as_ref()
            .and_then(|chord| bindings.resolve(chord))
            && matches!(
                binding.action,
                Action::Move(_) | Action::Delete(_) | Action::DeleteWord(_)
            )
        {
            if key.kind == KeyEventKind::Repeat && !binding.repeat {
                return None;
            }
            return Some(DecodedKey::Action {
                action: binding.action,
                select: true,
            });
        }
    }
    if key.modifiers.difference(KeyModifiers::SHIFT).is_empty()
        && let KeyCode::Char(character) = key.code
        && !character.is_control()
    {
        return Some(DecodedKey::Text(character));
    }
    None
}

/// Canonical chord spelling for user overrides and keymap help.
pub fn key_chord(key: KeyEvent) -> Option<String> {
    event_chord(key).map(|chord| chord.as_str().to_owned())
}

fn normalize_key(mut key: KeyEvent) -> KeyEvent {
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
    key
}

fn event_chord(key: KeyEvent) -> Option<ArrayString<CHORD_BYTES>> {
    normalized_chord(normalize_key(key))
}

fn normalized_chord(key: KeyEvent) -> Option<ArrayString<CHORD_BYTES>> {
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
        KeyCode::Char(' ') => "Space",
        KeyCode::Esc => "Esc",
        KeyCode::F(_) | KeyCode::Char(_) => "",
        _ => return None,
    };
    let mut chord = ArrayString::new();
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
