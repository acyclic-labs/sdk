//! Files are authoritative. These values are disposable projections over their
//! snapshots; all three formats share the same defaults/user/workspace fold.
mod diagnostics;
mod json;
mod theme;
mod validate;
use crate::{InputKeymap, PromptInput, UiSettings};
pub use diagnostics::ConfigDiagnostics;
use serde_json::Value;
pub use theme::{ThemeRole, ThemeStyles, default_style};

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
    /// Semantic colors and attributes.
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

/// Disposable snapshot/follow fold. No watcher, background task or independent store.
#[derive(Clone, Debug, Default)]
pub struct ConfigView {
    layers: [[Option<Value>; 3]; 2],
    errors: [[Vec<Diagnostic>; 3]; 2],
}
impl ConfigView {
    /// Fold one file write, including malformed replacements. None is a deletion.
    pub fn set_file(&mut self, scope: Scope, file: ConfigFile, text: Option<&str>) {
        let slot = &mut self.layers[scope.index()][file.index()];
        let errors = &mut self.errors[scope.index()][file.index()];
        errors.clear();
        match text.map(|text| parse(file, scope, text)) {
            Some(Ok(value)) => *slot = Some(value),
            Some(Err(diagnostics)) => {
                *slot = None;
                *errors = diagnostics;
            }
            None => *slot = None,
        }
    }
    /// Fold raw file bytes, diagnosing invalid UTF-8.
    pub fn set_bytes(&mut self, scope: Scope, file: ConfigFile, bytes: &[u8]) {
        match std::str::from_utf8(bytes) {
            Ok(text) => self.set_file(scope, file, Some(text)),
            Err(error) => {
                self.layers[scope.index()][file.index()] = None;
                self.errors[scope.index()][file.index()] = vec![Diagnostic {
                    file,
                    scope,
                    path: "$".into(),
                    kind: DiagnosticKind::Syntax,
                    detail: error.to_string(),
                }];
            }
        }
    }
    /// Diagnostics from the current file cut.
    pub fn diagnostics(&self) -> impl Iterator<Item = &Diagnostic> {
        self.errors.iter().flatten().flatten()
    }
    /// Validated source-file projection before defaults or higher scopes are merged.
    /// Borrowing a source never reparses its JSON or schema.
    pub fn source(&self, scope: Scope, file: ConfigFile) -> Option<&Value> {
        self.layers[scope.index()][file.index()].as_ref()
    }

    /// Effective file value; defaults are also declarative files.
    pub fn effective(&self, file: ConfigFile) -> Result<Value, Vec<Diagnostic>> {
        let defaults = parse(file, Scope::User, file.defaults())?;
        Ok(fold(
            &defaults,
            self.layers[0][file.index()].as_ref(),
            self.layers[1][file.index()].as_ref(),
        ))
    }
    /// Typed effective preferences.
    pub fn settings(&self) -> Result<UiSettings, Vec<Diagnostic>> {
        serde_json::from_value(self.effective(ConfigFile::Settings)?).map_err(|error| {
            vec![Diagnostic {
                file: ConfigFile::Settings,
                scope: Scope::User,
                path: "$".into(),
                kind: DiagnosticKind::InvalidValue,
                detail: error.to_string(),
            }]
        })
    }
    /// Canonical file bytes for an ordinary UI action that changes one setting.
    /// The host publishes these bytes, and only the ensuing follow updates views.
    pub fn edit(
        &self,
        scope: Scope,
        file: ConfigFile,
        patch: &Value,
    ) -> Result<Vec<u8>, Vec<Diagnostic>> {
        let old = self.layers[scope.index()][file.index()]
            .as_ref()
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));
        let value = fold(&old, Some(patch), None);
        let text = value.to_string();
        parse(file, scope, &text).map(|value| value.to_string().into_bytes())
    }
    /// Resolve all semantic UI and Markdown roles from gc-terminal's profile.
    pub fn theme(
        &self,
        profile: &crate::term::Capabilities,
    ) -> Result<ThemeStyles, Vec<Diagnostic>> {
        Ok(theme::resolve(&self.effective(ConfigFile::Theme)?, profile))
    }
    /// Apply an accepted projection while retaining drafts, history and undo state.
    pub fn apply(
        &self,
        input: &mut PromptInput,
        profile: &crate::term::Capabilities,
    ) -> Result<(), Vec<Diagnostic>> {
        let settings = self.settings()?;
        let keymap = InputKeymap::from_bindings(&self.effective(ConfigFile::Keymap)?);
        let theme = self.theme(profile)?;
        input.set_settings(settings);
        input.set_keymap(keymap);
        input.theme = theme;
        Ok(())
    }
}
