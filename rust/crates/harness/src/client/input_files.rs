//! TUI intents and drafts use the command session's ordinary file publications.
use super::{
    CommandAction, CommandFileRoot, CommandQuery, CommandRequest, CommandResolution, CommandSession,
};
use crate::{
    Error, Result,
    conversation::ContentResidencyVerifier,
    tui::{InputEffect, PromptInput},
};
use acyclic_stream::StreamProvider;
use serde_json::Value;

/// Native and embedded clients share classification and file publication.
pub async fn apply_tui_input<P: StreamProvider + ?Sized>(
    files: &mut CommandSession<P>,
    reader: &dyn ContentResidencyVerifier,
    effect: InputEffect,
) -> Result<()> {
    match effect {
        InputEffect::FindStep { previous } => {
            let mut layout = files
                .state(CommandFileRoot::Session, "layout.json")?
                .unwrap_or_else(|| serde_json::json!({}));
            if layout["open_view"] != "find" || layout["view_input"] != "find" {
                return Err(Error::Invalid("transcript search is closed".into()));
            }
            let index = layout
                .get("search_index")
                .map(|value| {
                    value
                        .as_i64()
                        .ok_or_else(|| Error::Invalid("search index must be an integer".into()))
                })
                .transpose()?
                .unwrap_or(0);
            layout["search_index"] =
                serde_json::json!(index.saturating_add(if previous { -1 } else { 1 }));
            files
                .write_unchanged_state(CommandFileRoot::Session, "layout.json", &layout)
                .await?;
        }
        InputEffect::Submit {
            text, edit: None, ..
        } => {
            files.submit_input(&text, reader).await?;
        }
        InputEffect::Host(crate::tui::Action::App(action)) => {
            use crate::tui::AppAction;
            let query = match action {
                AppAction::Help => CommandQuery::Shortcuts,
                AppAction::Find => CommandQuery::Find,
                AppAction::Reasoning => CommandQuery::Reasoning,
                AppAction::Details => CommandQuery::Details,
                _ => {
                    return Err(Error::Unsupported(
                        "This shortcut is unavailable in this session".into(),
                    ));
                }
            };
            let command = files
                .catalog()
                .commands()
                .find(|spec| spec.action == CommandAction::Select { query })
                .map(|spec| format!("/{}", spec.name))
                .ok_or_else(|| {
                    Error::Unsupported(
                        "This shortcut has no enabled command in this session".into(),
                    )
                })?;
            files.submit_input(&command, reader).await?;
        }
        InputEffect::Host(crate::tui::Action::Escape) => {
            let mut layout = files
                .state(CommandFileRoot::Session, "layout.json")?
                .unwrap_or_else(|| serde_json::json!({}));
            let object = layout
                .as_object_mut()
                .ok_or_else(|| Error::Invalid("layout must be an object".into()))?;
            if object.remove("open_view").is_none() {
                // TODO(#495): route the remaining empty-input escape ladder to the owner.
                return Err(Error::Unsupported(
                    "This action is unavailable in this session".into(),
                ));
            }
            object.remove("view_input");
            object.remove("view_request");
            object.remove("view_back");
            files
                .write_state(CommandFileRoot::Session, "layout.json", &layout)
                .await?;
        }
        InputEffect::Submit {
            text,
            edit: Some(message),
            ..
        } => {
            files.replace_message(&message, text).await?;
        }
        InputEffect::Interrupt => files.cancel_active().await?,
        InputEffect::Quit => {
            files
                .publish_input_resolution(CommandResolution::Submit {
                    request: CommandRequest::Quit,
                })
                .await?;
        }
        // Clipboard and external editor effects require a separate platform binding.
        _ => {
            return Err(Error::Unsupported(
                "This action is unavailable in this session".into(),
            ));
        }
    }
    Ok(())
}

#[cfg(all(
    test,
    feature = "terminal",
    not(acyclic_harness_loom),
    not(target_arch = "wasm32")
))]
#[path = "input_files/shortcuts.rs"]
mod shortcuts;

/// Process a user action, retaining ordinary command errors as session feedback.
/// Contention and peer edits stay visible as feedback; authority, storage and
/// ambiguous publication failures still reach the caller.
pub async fn handle_tui_input<P: StreamProvider + ?Sized>(
    files: &mut CommandSession<P>,
    reader: &dyn ContentResidencyVerifier,
    effect: InputEffect,
) -> Result<()> {
    if files
        .state(CommandFileRoot::Config, "settings.json")?
        .is_some_and(|value| value["renderer"] == "ratatui")
    {
        crate::tui::catalog::configure(files).await?;
    }
    let messages = match apply_tui_input(files, reader, effect).await {
        Ok(()) => Vec::new(),
        Err(
            error @ (Error::Invalid(_)
            | Error::NotFound(_)
            | Error::Unsupported(_)
            | Error::Conflict(_)),
        ) => {
            vec![error.to_string()]
        }
        Err(error) => return Err(error),
    };
    let current = files.state(CommandFileRoot::Session, "notifications.json")?;
    let next = serde_json::json!(messages);
    if current.as_ref() != Some(&next) && (current.is_some() || !messages.is_empty()) {
        if let Err(error) = files
            .write_state(CommandFileRoot::Session, "notifications.json", &next)
            .await
        {
            // The input is already accepted. A newer peer notification wins;
            // losing this presentation-only CAS must not terminate the session.
            if !matches!(error, Error::Conflict(_)) {
                return Err(error);
            }
        }
    }
    Ok(())
}

/// Load one session's persisted prompt before accepting input.
pub fn restore_tui_prompt<P: StreamProvider + ?Sized>(
    files: &CommandSession<P>,
    input: &mut PromptInput,
) -> Result<()> {
    let draft = files.state(CommandFileRoot::Session, "draft.json")?;
    let text = draft
        .as_ref()
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| Error::Invalid("draft file must contain a string".into()))
        })
        .transpose()?;
    let history = files
        .state(CommandFileRoot::Config, "history.json")?
        .map(|value| {
            crate::tui::parse_history(&value.to_string())
                .map_err(|error| Error::Invalid(error.to_string()))
        })
        .transpose()?;
    if let Some(text) = text {
        input.editor.set_text(text, 0);
    }
    if let Some(history) = history {
        input.replace_history(history);
    }
    Ok(())
}

/// Persist only changed draft text, and render after the file's accepted cut.
pub async fn save_tui_draft<P: StreamProvider + ?Sized>(
    files: &mut CommandSession<P>,
    input: &PromptInput,
) -> Result<()> {
    let draft = Value::String(input.editor.text().to_owned());
    if files
        .state(CommandFileRoot::Session, "draft.json")?
        .as_ref()
        != Some(&draft)
    {
        files
            .write_unchanged_state(CommandFileRoot::Session, "draft.json", &draft)
            .await?;
    }
    Ok(())
}

/// Publish prompt actions and transcript offsets through their authorized files.
pub async fn file_tui_input<P: StreamProvider + ?Sized>(
    files: &mut CommandSession<P>,
    reader: &dyn ContentResidencyVerifier,
    effect: InputEffect,
    page_rows: usize,
    maximum_scroll: usize,
) -> Result<()> {
    if let Some(rows) = super::transcript_motion(&effect, page_rows) {
        return super::scroll_tui(files, [rows], maximum_scroll).await;
    }
    handle_tui_input(files, reader, effect).await
}

/// Publish a burst's effects in order. Each run of view motions folds against
/// the scroll bounds in memory and publishes once, exactly where publishing
/// each motion in turn would have ended.
pub async fn file_tui_inputs<P: StreamProvider + ?Sized>(
    files: &mut CommandSession<P>,
    reader: &dyn ContentResidencyVerifier,
    effects: impl IntoIterator<Item = InputEffect>,
    page_rows: usize,
    maximum_scroll: usize,
) -> Result<()> {
    let mut motions = Vec::new();
    for effect in effects {
        if let Some(rows) = super::transcript_motion(&effect, page_rows) {
            motions.push(rows);
            continue;
        }
        if !motions.is_empty() {
            super::scroll_tui(files, motions.drain(..), maximum_scroll).await?;
        }
        handle_tui_input(files, reader, effect).await?;
    }
    if motions.is_empty() {
        return Ok(());
    }
    super::scroll_tui(files, motions, maximum_scroll).await
}
