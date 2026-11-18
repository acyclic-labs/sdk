//! Files are authoritative. These values are disposable projections over their
//! snapshots; all three formats share the same defaults/user/workspace fold.
mod diagnostics;
mod theme;
use acyclic_harness_tui_core::config::{
    ConfigFile, Diagnostic, DiagnosticKind, Scope, fold, parse,
};
use acyclic_harness_tui_core::{InputKeymap, UiSettings};
use crate::PromptInput;
pub use diagnostics::ConfigDiagnostics;
use serde_json::Value;
pub use theme::{ThemeRole, ThemeStyles, default_style};

/// Disposable snapshot/follow fold. No watcher, background task or independent store.
#[derive(Clone, Debug, Default)]
pub struct ConfigView {
    layers: Table<Option<Value>>,
    errors: Table<Vec<Diagnostic>>,
}

/// One value per scope and configuration file, addressed exhaustively.
#[derive(Clone, Debug, Default)]
struct Table<T>([[T; 3]; 2]);
impl<T> Table<T> {
    fn get(&self, scope: Scope, file: ConfigFile) -> &T {
        let [user, workspace] = &self.0;
        let [settings, keymap, theme] = match scope {
            Scope::User => user,
            Scope::Workspace => workspace,
        };
        match file {
            ConfigFile::Settings => settings,
            ConfigFile::Keymap => keymap,
            ConfigFile::Theme => theme,
        }
    }
    fn get_mut(&mut self, scope: Scope, file: ConfigFile) -> &mut T {
        let [user, workspace] = &mut self.0;
        let [settings, keymap, theme] = match scope {
            Scope::User => user,
            Scope::Workspace => workspace,
        };
        match file {
            ConfigFile::Settings => settings,
            ConfigFile::Keymap => keymap,
            ConfigFile::Theme => theme,
        }
    }
    fn iter(&self) -> impl Iterator<Item = &T> {
        self.0.iter().flatten()
    }
}
impl ConfigView {
    /// Fold one file write, including malformed replacements. None is a deletion.
    pub fn set_file(&mut self, scope: Scope, file: ConfigFile, text: Option<&str>) {
        let slot = self.layers.get_mut(scope, file);
        let errors = self.errors.get_mut(scope, file);
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
                *self.layers.get_mut(scope, file) = None;
                *self.errors.get_mut(scope, file) = vec![Diagnostic {
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
        self.errors.iter().flatten()
    }
    /// Validated source-file projection before defaults or higher scopes are merged.
    /// Borrowing a source never reparses its JSON or schema.
    pub fn source(&self, scope: Scope, file: ConfigFile) -> Option<&Value> {
        self.layers.get(scope, file).as_ref()
    }

    /// Effective file value; defaults are also declarative files.
    pub fn effective(&self, file: ConfigFile) -> Result<Value, Vec<Diagnostic>> {
        let defaults = parse(file, Scope::User, file.defaults())?;
        Ok(fold(
            &defaults,
            self.layers.get(Scope::User, file).as_ref(),
            self.layers.get(Scope::Workspace, file).as_ref(),
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
        let old = self
            .layers
            .get(scope, file)
            .as_ref()
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));
        let value = fold(&old, Some(patch), None);
        let text = value.to_string();
        parse(file, scope, &text).map(|value| value.to_string().into_bytes())
    }
    /// Resolve the theme's tokens for gc-terminal's profile: the light or dark
    /// set by the file's mode or the probed background, at its colour depth.
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
