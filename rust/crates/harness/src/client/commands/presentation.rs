use super::{CommandAction, CommandCatalog, CommandSpec};
use serde::{Deserialize, Serialize};

/// One completion replaces the supplied composer prefix, including its slash.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CommandCompletion {
    /// Display text; direct tools carry the `[tool]` suffix.
    pub label: String,
    /// Catalog-owned help text.
    pub description: String,
    /// Exact composer replacement; tools and theme append a space.
    pub replacement: String,
}

impl CommandCatalog {
    /// Composer completions, including aliases and configured theme names.
    /// Non-slash input and arguments other than theme names have no completions.
    pub fn complete_input(&self, input: &str) -> Vec<CommandCompletion> {
        let Some(command) = input.strip_prefix('/') else {
            return Vec::new();
        };
        if let Some(end) = command.find(char::is_whitespace) {
            let Some(spec) = self.lookup(&command[..end]) else {
                return Vec::new();
            };
            if !matches!(spec.action, CommandAction::Theme) {
                return Vec::new();
            }
            let prefix = command[end..].trim_start().to_lowercase();
            return self
                .themes
                .iter()
                .filter(|name| name.to_lowercase().starts_with(&prefix))
                .map(|name| CommandCompletion {
                    label: name.clone(),
                    description: "Switch theme".into(),
                    replacement: format!("/{} {name}", spec.name),
                })
                .collect();
        }
        self.complete(command)
            .map(|spec| {
                let tool = matches!(spec.action, CommandAction::Tool { .. });
                let space = tool || matches!(spec.action, CommandAction::Theme);
                CommandCompletion {
                    label: format!("/{}{}", spec.name, if tool { " [tool]" } else { "" }),
                    description: spec.description.clone(),
                    replacement: format!("/{}{}", spec.name, if space { " " } else { "" }),
                }
            })
            .collect()
    }

    /// Fold palette.json and the catalog query into borrowed display rows.
    pub fn palette<'a>(&'a self, state: &'a CommandPaletteState) -> CommandPalette<'a> {
        let filter = state.filter.to_lowercase();
        let entries = self
            .complete("")
            .filter(|spec| {
                spec.name.to_lowercase().contains(&filter)
                    || spec
                        .aliases
                        .iter()
                        .any(|alias| alias.to_lowercase().contains(&filter))
                    || spec.description.to_lowercase().contains(&filter)
            })
            .collect();
        CommandPalette { state, entries }
    }
}

/// Contents of palette.json in the config volume, followed by the UI.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandPaletteState {
    /// Substring matched against catalog names, aliases and descriptions.
    pub filter: String,
    /// Logical selection; projection wraps it to the current result count.
    pub selected: u64,
}

/// Renderer-neutral fold over the palette-state and command-file snapshots.
#[derive(Debug)]
pub struct CommandPalette<'a> {
    state: &'a CommandPaletteState,
    entries: Vec<&'a CommandSpec>,
}

impl<'a> CommandPalette<'a> {
    /// Ordered matches from the same catalog snapshot.
    pub fn entries(&self) -> &[&'a CommandSpec] {
        &self.entries
    }

    /// Selected row, absent for an empty result set.
    pub fn selected(&self) -> Option<&'a CommandSpec> {
        self.index().map(|index| self.entries[index])
    }

    /// New palette-file contents. Publish, then render the resulting query cut.
    pub fn next(&self) -> CommandPaletteState {
        let selected = self
            .index()
            .map_or(0, |index| (index + 1) % self.entries.len());
        CommandPaletteState {
            filter: self.state.filter.clone(),
            selected: selected as u64,
        }
    }

    /// New palette-file contents, with backward wrapping.
    pub fn previous(&self) -> CommandPaletteState {
        let selected = self.index().map_or(0, |index| {
            if index == 0 {
                self.entries.len() - 1
            } else {
                index - 1
            }
        });
        CommandPaletteState {
            filter: self.state.filter.clone(),
            selected: selected as u64,
        }
    }

    fn index(&self) -> Option<usize> {
        (!self.entries.is_empty())
            .then(|| (self.state.selected % self.entries.len() as u64) as usize)
    }
}
