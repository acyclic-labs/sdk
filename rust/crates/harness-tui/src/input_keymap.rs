use crate::{Binding, KEYMAP};
use std::{collections::BTreeMap, io, path::Path};

/// User remapping data. JSON maps canonical source chords to default action
/// chords, e.g. `{"Ctrl+l":"Home","Home":""}`. An empty target disables a key.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct InputKeymap {
    overrides: BTreeMap<String, String>,
}

impl InputKeymap {
    /// Parse and validate an override file without changing any bindings on error.
    pub fn from_json(json: &str) -> io::Result<Self> {
        let overrides: BTreeMap<String, String> = serde_json::from_str(json)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        for (source, target) in &overrides {
            if !valid_chord(source)
                || !target.is_empty() && !KEYMAP.iter().any(|b| b.chord == target)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid keymap chord or action",
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
        let target = self.overrides.get(chord).map_or(chord, String::as_str);
        KEYMAP
            .iter()
            .find(|binding| binding.chord == target)
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

fn valid_chord(chord: &str) -> bool {
    let keys: Vec<_> = chord.split(' ').collect();
    !keys.is_empty()
        && keys.len() <= 2
        && keys.into_iter().all(|mut key| {
            for modifier in ["Ctrl+", "Alt+", "Shift+", "Super+"] {
                key = key.strip_prefix(modifier).unwrap_or(key);
            }
            matches!(
                key,
                "Left"
                    | "Right"
                    | "Up"
                    | "Down"
                    | "Home"
                    | "End"
                    | "Backspace"
                    | "Delete"
                    | "Enter"
                    | "Tab"
                    | "Esc"
                    | "PageUp"
                    | "PageDown"
            ) || key.chars().count() == 1
                && key
                    .chars()
                    .all(|c| !c.is_control() && !c.is_uppercase() && c != '+')
        })
}
