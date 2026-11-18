//! Declarative configuration files, their schema checks and precedence fold.
mod json;
pub mod theme;
mod validate;
use serde_json::Value;

/// Location determines precedence, independently of publication arrival order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scope {
    /// User config volume, above defaults.
    User,
    /// Workspace config volume, above user preferences.
    Workspace,
}
impl Scope {
    /// Stable source-array slot.
    pub fn index(self) -> usize {
        match self {
            Self::User => 0,
            Self::Workspace => 1,
        }
    }
}

/// The three ordinary UI configuration files.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfigFile {
    /// Editor and presentation preferences.
    Settings,
    /// Named action bindings.
    Keymap,
    /// Semantic colour tokens, dark and light.
    Theme,
}
impl ConfigFile {
    /// Files selected by the config live query.
    pub const ALL: [Self; 3] = [Self::Settings, Self::Keymap, Self::Theme];
    /// Stable source-array slot.
    pub fn index(self) -> usize {
        match self {
            Self::Settings => 0,
            Self::Keymap => 1,
            Self::Theme => 2,
        }
    }
    /// Canonical path relative to the selected volume.
    pub fn path(self) -> &'static str {
        match self {
            Self::Settings => "config/settings.json",
            Self::Keymap => "config/keymap.json",
            Self::Theme => "config/theme.json",
        }
    }
    /// Recognize a path emitted by the file query.
    pub fn from_path(path: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|f| f.path() == path)
    }
    /// Shipped declarative defaults.
    pub fn defaults(self) -> &'static str {
        match self {
            Self::Settings => include_str!("../../data/settings.json"),
            Self::Keymap => include_str!("../../data/keymap.json"),
            Self::Theme => include_str!("../../data/theme.json"),
        }
    }
    /// JSON schema for partial override files.
    pub fn schema(self) -> &'static str {
        match self {
            Self::Settings => include_str!("../../data/settings.schema.json"),
            Self::Keymap => include_str!("../../data/keymap.schema.json"),
            Self::Theme => include_str!("../../data/theme.schema.json"),
        }
    }
}

/// Stable diagnostic categories; file text never becomes a panic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DiagnosticKind {
    /// Invalid JSON or UTF-8.
    Syntax,
    /// A key is not part of the schema.
    UnknownKey,
    /// A field has the wrong JSON type.
    InvalidType,
    /// A number exceeds the declared bounds.
    OutOfRange,
    /// An enum or color is not recognized.
    InvalidValue,
    /// The chord grammar rejected a binding.
    InvalidChord,
    /// The binding names no registered action.
    UnknownAction,
    /// Two definitions claim the same key.
    Conflict,
}
/// One diagnostic displayed with its source and JSON path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Diagnostic {
    /// Source file.
    pub file: ConfigFile,
    /// Source volume precedence.
    pub scope: Scope,
    /// Field path within the JSON file.
    pub path: String,
    /// Typed failure category.
    pub kind: DiagnosticKind,
    /// Human-readable diagnostic, sanitized by the widget.
    pub detail: String,
}

/// One pure recursive fold: later objects override earlier objects per key;
/// scalars (including null disabling a key binding) replace whole values.
pub fn fold(defaults: &Value, user: Option<&Value>, workspace: Option<&Value>) -> Value {
    fn overlay(target: &mut Value, source: &Value) {
        if let (Some(target), Some(source)) = (target.as_object_mut(), source.as_object()) {
            for (key, value) in source {
                if let Some(old) = target.get_mut(key) {
                    overlay(old, value);
                } else {
                    target.insert(key.clone(), value.clone());
                }
            }
        } else {
            *target = source.clone();
        }
    }
    let mut result = defaults.clone();
    for layer in [user, workspace].into_iter().flatten() {
        overlay(&mut result, layer);
    }
    result
}

/// Parse and schema-check one file. Returned values have canonical key chords.
/// Invalid files do not supply an override; lower-precedence files remain usable.
pub fn parse(file: ConfigFile, scope: Scope, text: &str) -> Result<Value, Vec<Diagnostic>> {
    validate::parse(file, scope, text)
}

/// Decode untrusted file bytes without lossy replacement of configuration data.
pub fn parse_bytes(file: ConfigFile, scope: Scope, bytes: &[u8]) -> Result<Value, Vec<Diagnostic>> {
    let text = std::str::from_utf8(bytes).map_err(|error| {
        vec![Diagnostic {
            file,
            scope,
            path: "$".into(),
            kind: DiagnosticKind::Syntax,
            detail: error.to_string(),
        }]
    })?;
    parse(file, scope, text)
}
