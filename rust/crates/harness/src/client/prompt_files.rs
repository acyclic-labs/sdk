//! Disposable prompt projection over the command session's existing file query.
use super::{CommandFileRoot, CommandSession, restore_tui_prompt, save_tui_draft};
use crate::{
    Error, Result,
    task_volume::FileValue,
    tui::{InputKeymap, PromptHistory, PromptInput},
};
use acyclic_stream::StreamProvider;

/// Remembers the last applied file contents, so unrelated publications never
/// replace the editor or invalidate its ephemeral selection/history navigation.
#[derive(Default)]
pub struct PromptFiles {
    draft: Option<FileValue>,
    history: Option<FileValue>,
    keymap: Option<FileValue>,
    settings: Option<FileValue>,
}

fn history(file: Option<&FileValue>) -> Result<PromptHistory> {
    let text = file
        .map(|file| std::str::from_utf8(&file.bytes))
        .transpose()
        .map_err(|error| Error::Invalid(error.to_string()))?;
    crate::tui::parse_history(text.unwrap_or("[]"))
        .map_err(|error| Error::Invalid(error.to_string()))
}

// Keep the largest chronological suffix that fits the serialized file budget.
// Count encoded strings, including JSON escaping, commas and array brackets.
fn recent_history(history: &PromptHistory, maximum_bytes: usize) -> Result<PromptHistory> {
    if maximum_bytes < 2 {
        return Err(Error::Invalid(
            "history file cannot hold an empty array".into(),
        ));
    }
    let (mut bytes, mut count) = (2, 0);
    for text in history.entries().iter().rev() {
        let encoded =
            serde_json::to_vec(text).map_err(|error| Error::Invalid(error.to_string()))?;
        let additional = encoded.len() + usize::from(count > 0);
        if additional > maximum_bytes - bytes {
            break;
        }
        bytes += additional;
        count += 1;
    }
    let mut recent = PromptHistory::default();
    for text in &history.entries()[history.entries().len() - count..] {
        recent.push(text.clone());
    }
    Ok(recent)
}

impl PromptFiles {
    /// Apply changed files together, validating every file before mutating the editor.
    pub fn refresh<P: StreamProvider + ?Sized>(
        &mut self,
        files: &CommandSession<P>,
        input: &mut PromptInput,
    ) -> Result<()> {
        let draft = files.state_file(CommandFileRoot::Session, "draft.json")?;
        let entries = files.state_file(CommandFileRoot::Config, "history.json")?;
        let bindings = files.state_file(CommandFileRoot::Config, "keymap.json")?;
        let settings_file = files.state_file(CommandFileRoot::Config, "settings.json")?;
        let settings_changed = self.settings.as_ref() != settings_file;
        let draft_changed = self.draft.as_ref() != draft;
        let history_changed = self.history.as_ref() != entries;
        let keymap_changed = self.keymap.as_ref() != bindings;
        if !draft_changed && !history_changed && !keymap_changed && !settings_changed {
            return Ok(());
        }
        // Decode in isolation; malformed bytes must leave all caches and input
        // at their old cut so a repaired publication can be applied later.
        let mut decoded = PromptInput::default();
        restore_tui_prompt(files, &mut decoded)?;
        let keymap = bindings
            .map(|file| {
                let text = std::str::from_utf8(&file.bytes)
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                InputKeymap::parse(text).map_err(|errors| Error::Invalid(format!("{errors:?}")))
            })
            .transpose()?
            .unwrap_or_default();
        let settings: serde_json::Value = settings_file
            .map(|file| serde_json::from_slice(&file.bytes))
            .transpose()
            .map_err(|error| Error::Invalid(error.to_string()))?
            .unwrap_or_else(|| serde_json::json!({}));
        if !settings.is_object() {
            return Err(Error::Invalid("settings must be an object".into()));
        }
        let reduced_motion = settings
            .get("reduced_motion")
            .map(|value| {
                value
                    .as_bool()
                    .ok_or_else(|| Error::Invalid("reduced_motion must be a boolean".into()))
            })
            .transpose()?
            .unwrap_or(false);
        if draft_changed {
            input.editor.set_text(decoded.editor.text(), 0);
        }
        if history_changed {
            input.replace_history(history(entries)?);
        }
        if keymap_changed {
            input.set_keymap(keymap);
        }
        if settings_changed {
            let mut preferences = *input.editor.settings();
            preferences.reduced_motion = reduced_motion;
            input.set_settings(preferences);
        }
        self.settings = settings_file.cloned();
        self.keymap = bindings.cloned();
        self.draft = draft.cloned();
        self.history = entries.cloned();
        Ok(())
    }

    /// Save local submissions by appending to the latest followed history file,
    /// preserving entries published by other sessions since the previous frame.
    /// Unrelated owner publications may advance the cut; peer edits to the
    /// saved file still fail closed instead of overwriting their contents.
    pub async fn save<P: StreamProvider + ?Sized>(
        &mut self,
        files: &mut CommandSession<P>,
        input: &mut PromptInput,
    ) -> Result<()> {
        let before = history(self.history.as_ref())?;
        let appended = input
            .history()
            .entries()
            .strip_prefix(before.entries())
            .ok_or_else(|| {
                Error::Conflict("prompt history changed outside its file projection".into())
            })?;
        if !appended.is_empty() {
            let current = files.state_file(CommandFileRoot::Config, "history.json")?;
            let mut merged = history(current)?;
            for text in appended {
                merged.push(text.clone());
            }
            let merged = recent_history(
                &merged,
                files.state_byte_limit(CommandFileRoot::Config, "history.json")?,
            )?;
            let value = serde_json::to_value(merged.entries())
                .map_err(|error| Error::Invalid(error.to_string()))?;
            if files
                .state(CommandFileRoot::Config, "history.json")?
                .as_ref()
                != Some(&value)
            {
                files
                    .write_unchanged_state(CommandFileRoot::Config, "history.json", &value)
                    .await?;
            }
            input.replace_history(merged);
            self.history = files
                .state_file(CommandFileRoot::Config, "history.json")?
                .cloned();
        }
        save_tui_draft(files, input).await?;
        self.draft = files
            .state_file(CommandFileRoot::Session, "draft.json")?
            .cloned();
        Ok(())
    }
}
