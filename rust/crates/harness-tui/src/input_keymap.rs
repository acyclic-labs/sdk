use crate::key_event::{KeyCode, KeyEvent, KeyModifiers};
use crate::{Binding, KEYMAP};
use std::{collections::BTreeMap, io, path::Path};

/// User remapping data. JSON maps canonical source chords to default action
/// chords, e.g. `{"Ctrl+l":"Home","Home":""}`. An empty target disables a key.
/// Super character shortcuts share Ctrl's canonical action chord. Conflicting
/// aliases are rejected, so generated help and dispatch always agree.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct InputKeymap {
    overrides: BTreeMap<String, String>,
}

impl InputKeymap {
    /// Parse and validate an override file without changing any bindings on error.
    pub fn from_json(json: &str) -> io::Result<Self> {
        let raw: BTreeMap<String, String> = serde_json::from_str(json)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        let invalid = || {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid or conflicting keymap chord",
            )
        };
        let mut overrides = BTreeMap::new();
        for (source, target) in raw {
            let source = canonical_chord(&source).ok_or_else(invalid)?;
            let target = if target.is_empty() {
                target
            } else {
                canonical_chord(&target).ok_or_else(invalid)?
            };
            if !target.is_empty() && !KEYMAP.iter().any(|b| b.chord == target) {
                return Err(invalid());
            }
            if overrides
                .insert(source, target.clone())
                .is_some_and(|old| old != target)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "conflicting canonical keymap chords",
                ));
            }
        }
        Ok(Self { overrides })
    }

    /// Read overrides from a caller-selected configuration file.
    pub fn load(path: &Path) -> io::Result<Self> {
        Self::from_json(&std::fs::read_to_string(path)?)
    }

    /// Write declarative overrides to the caller's config-volume path.
    pub fn save(&self, path: &Path) -> io::Result<()> {
        crate::prompt_files::write_atomic(path, self.to_json()?.as_bytes())
    }

    /// Serialize the overrides for editing or persistence.
    pub fn to_json(&self) -> io::Result<String> {
        serde_json::to_string(&self.overrides).map_err(io::Error::other)
    }

    /// Resolve a chord using the same action data as generated help.
    pub fn resolve(&self, chord: &str) -> Option<Binding> {
        let chord = canonical_chord(chord)?;
        let target = self
            .overrides
            .get(&chord)
            .map_or(chord.as_str(), String::as_str);
        KEYMAP
            .iter()
            .find(|binding| binding.chord == target)
            .copied()
    }

    pub(crate) fn disabled(&self, chord: &str) -> bool {
        canonical_chord(chord)
            .and_then(|chord| self.overrides.get(&chord))
            .is_some_and(String::is_empty)
    }

    pub(crate) fn has_prefix(&self, prefix: &str) -> bool {
        KEYMAP
            .iter()
            .map(|binding| binding.chord)
            .chain(self.overrides.keys().map(String::as_str))
            .any(|source| {
                source
                    .split_once(' ')
                    .is_some_and(|(first, _)| first == prefix)
                    && self.resolve(source).is_some()
            })
    }

    /// Generate help for the effective map, including overrides and disabled keys.
    pub fn help(&self) -> String {
        let mut sources: std::collections::BTreeSet<&str> =
            KEYMAP.iter().map(|b| b.chord).collect();
        sources.extend(self.overrides.keys().map(String::as_str));
        sources
            .into_iter()
            .filter_map(|source| {
                self.resolve(source)
                    .map(|b| format!("{source}\t{}\n", b.help))
            })
            .collect()
    }
}

fn canonical_chord(chord: &str) -> Option<String> {
    let keys: Vec<_> = chord.split(' ').collect();
    if keys.is_empty() || keys.len() > 2 {
        return None;
    }
    keys.into_iter()
        .map(|mut key| {
            let mut modifiers = KeyModifiers::NONE;
            for (prefix, modifier) in [
                ("Ctrl+", KeyModifiers::CONTROL),
                ("Alt+", KeyModifiers::ALT),
                ("Shift+", KeyModifiers::SHIFT),
                ("Super+", KeyModifiers::SUPER),
            ] {
                if let Some(rest) = key.strip_prefix(prefix) {
                    key = rest;
                    modifiers.insert(modifier);
                }
            }
            let code = match key {
                "Left" => KeyCode::Left,
                "Right" => KeyCode::Right,
                "Up" => KeyCode::Up,
                "Down" => KeyCode::Down,
                "Home" => KeyCode::Home,
                "End" => KeyCode::End,
                "Backspace" => KeyCode::Backspace,
                "Delete" => KeyCode::Delete,
                "Enter" => KeyCode::Enter,
                "Tab" => KeyCode::Tab,
                "Esc" => KeyCode::Esc,
                "PageUp" => KeyCode::PageUp,
                "PageDown" => KeyCode::PageDown,
                _ => {
                    let mut chars = key.chars();
                    let character = chars.next()?;
                    if chars.next().is_some() || character.is_control() || character.is_uppercase()
                    {
                        return None;
                    }
                    KeyCode::Char(character)
                }
            };
            crate::key_decode::key_chord(KeyEvent::new(code, modifiers))
        })
        .collect::<Option<Vec<_>>>()
        .map(|keys| keys.join(" "))
}
