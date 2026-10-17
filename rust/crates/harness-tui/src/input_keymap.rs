use crate::key_event::{KeyCode, KeyEvent, KeyModifiers};
use crate::{Binding, KEYMAP, named_action};
use std::{collections::BTreeMap, io, path::Path};

/// Canonical schema-validated `{"bindings":{"Ctrl+l":"cursor.line-start"}}`
/// preferences. A null action disables a key. Super character shortcuts share
/// Ctrl's canonical chord; conflicting aliases produce typed diagnostics.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct InputKeymap {
    overrides: BTreeMap<String, String>,
}

impl InputKeymap {
    /// Parse the same keymap format as the live config file follower.
    pub fn parse(json: &str) -> Result<Self, Vec<crate::config::Diagnostic>> {
        let value = crate::config::parse(
            crate::config::ConfigFile::Keymap,
            crate::config::Scope::User,
            json,
        )?;
        Ok(Self::from_bindings(&value))
    }

    // Accept only a value already validated and canonicalized by config::parse.
    pub(crate) fn from_bindings(value: &serde_json::Value) -> Self {
        let overrides = value["bindings"]
            .as_object()
            .into_iter()
            .flatten()
            .map(|(chord, action)| {
                (
                    chord.clone(),
                    action.as_str().unwrap_or_default().to_owned(),
                )
            })
            .collect();
        Self { overrides }
    }

    /// Read overrides from a caller-selected configuration file.
    pub fn load(path: &Path) -> io::Result<Self> {
        Self::parse(&std::fs::read_to_string(path)?)
            .map_err(|errors| io::Error::new(io::ErrorKind::InvalidData, format!("{errors:?}")))
    }

    /// Write declarative overrides to the caller's config-volume path.
    pub fn save(&self, path: &Path) -> io::Result<()> {
        crate::prompt_files::write_atomic(path, self.encode().as_bytes())
    }

    /// Encode the canonical file format, including null disabled bindings.
    pub fn encode(&self) -> String {
        let bindings: serde_json::Map<String, serde_json::Value> = self
            .overrides
            .iter()
            .map(|(chord, action)| {
                (
                    chord.clone(),
                    if action.is_empty() {
                        serde_json::Value::Null
                    } else {
                        action.clone().into()
                    },
                )
            })
            .collect();
        serde_json::json!({"bindings":bindings}).to_string()
    }

    /// Resolve an already canonical event chord using the generated help data.
    /// Override files are canonicalized once by `parse`.
    pub fn resolve(&self, chord: &str) -> Option<Binding<'_>> {
        if let Some((source, target)) = self.overrides.get_key_value(chord) {
            if let Some(binding) = named_action(target) {
                return Some(Binding {
                    chord: source,
                    ..binding
                });
            }
            return None;
        }
        KEYMAP
            .iter()
            .find(|binding| binding.chord == chord)
            .copied()
    }

    pub(crate) fn disabled(&self, chord: &str) -> bool {
        self.overrides.get(chord).is_some_and(String::is_empty)
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

pub(crate) fn canonical_chord(chord: &str) -> Option<String> {
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
                "Insert" => KeyCode::Insert,
                "Space" => KeyCode::Char(' '),
                key if key.starts_with('F') => {
                    let number = key[1..].parse::<u8>().ok()?;
                    if !(1..=24).contains(&number) {
                        return None;
                    }
                    KeyCode::F(number)
                }
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
