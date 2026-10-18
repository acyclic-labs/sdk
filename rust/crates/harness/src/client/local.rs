//! Local bindings shared by native clients; presentation never owns a host.
use crate::{OperationId, Result, local::LocalHarness, wire};
use std::sync::Arc;

/// Opens the authorized task-file client; opening starts no task.
pub async fn connect_local(
    local: Arc<LocalHarness>,
    operation: OperationId,
) -> Result<(super::LocalFileClient, OperationId)> {
    match super::LocalFileClient::open(local.clone(), operation).await {
        Ok(client) => Ok((client, operation)),
        Err(error) => {
            disconnect_local(local).await?;
            Err(error)
        }
    }
}

/// Releases an exclusively owned local connection without discarding its tasks.
/// Other clients retain a shared runtime until its final owner disconnects.
pub async fn disconnect_local(local: Arc<LocalHarness>) -> Result<()> {
    if let Some(local) = Arc::into_inner(local) {
        local.disconnect().await?;
    }
    Ok(())
}

/// Resolve one headless input exactly as the TUI would: from the same
/// `commands/catalog.json` file its CommandSession folds, read at one
/// snapshot, so saved commands, renames and removals apply to `-p` too. Plain text is a prompt;
/// slash input is a command or an error, never chat.
pub async fn local_input(
    local: &LocalHarness,
    operation: OperationId,
    input: &str,
) -> Result<crate::task_volume::FileValue> {
    let reader = local.client_reader()?;
    let snapshot = local
        .task_files()
        .snapshot(&super::CommandCatalog::saved_query()?, 1)
        .await?;
    let catalog =
        super::CommandCatalog::saved(&snapshot, super::local_headless::MAXIMUM_BYTES as u64)?;
    headless_input(&catalog, reader.as_ref(), operation, input).await
}

/// Selections, confirmations and renderer-only requests have no headless form.
async fn headless_input(
    catalog: &super::CommandCatalog,
    reader: &dyn crate::conversation::ContentResidencyVerifier,
    operation: OperationId,
    input: &str,
) -> Result<crate::task_volume::FileValue> {
    use super::{CommandRequest, CommandResolution};
    let resolution = catalog
        .resolve(input, reader, super::local_headless::MAXIMUM_BYTES as u64)
        .await?;
    match &resolution {
        CommandResolution::Select { .. }
        | CommandResolution::Confirm { .. }
        | CommandResolution::Sdk { .. }
        | CommandResolution::Submit {
            request: CommandRequest::Quit | CommandRequest::Theme { .. } | CommandRequest::CycleMode,
        } => Err(crate::Error::Unsupported(
            "this command has no headless form; run it in the terminal UI".into(),
        )),
        _ => super::local_headless::prepare_local_resolution(operation, &resolution),
    }
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;

/// Stages an immutable request for the client outbox, without admitting it.
pub async fn local_prompt(
    _local: &LocalHarness,
    operation: OperationId,
    prompt: &str,
) -> Result<crate::task_volume::FileValue> {
    super::prepare_local_prompt(operation, prompt)
}

/// Follow the local host's ordinary command/workspace files into the prompt.
#[cfg(feature = "tui")]
pub async fn local_completions(
    local: &LocalHarness,
    input: &mut crate::tui::PromptInput,
) -> Result<crate::tui_files::CompletionFiles> {
    // TODO(#495): take this resident query bound from the followed client settings.
    crate::tui_files::CompletionFiles::observe_authorized(local.task_files(), input, None, 4096)
        .await
}

/// Open the file-backed UI session. Its identity is supplied by the client binding.
#[cfg(feature = "tui")]
pub async fn local_ui(
    local: &LocalHarness,
    operation: &str,
    owner: &wire::Authority,
) -> Result<super::CommandSession<acyclic_stream::LocalStream>> {
    if owner.kind != wire::AggregateKind::Task as i32 {
        return Err(crate::Error::Invalid(
            "local UI requires a task owner".into(),
        ));
    }
    let session = uuid::Uuid::from_bytes(OperationId::parse(operation)?.into_bytes());
    // TODO(#495): obtain the stable UI-session identity and bounds from client settings.
    let mut files =
        super::CommandSession::open_authorized(local.task_files().clone(), session, 65_536, 4096)
            .await?;
    #[cfg(feature = "tui")]
    {
        let mut settings = files
            .state(super::CommandFileRoot::Config, "settings.json")?
            .unwrap_or_else(|| serde_json::json!({}));
        if settings["renderer"] != "ratatui" {
            settings["renderer"] = serde_json::json!("ratatui");
            files
                .write_state(super::CommandFileRoot::Config, "settings.json", &settings)
                .await?;
        }
        crate::tui::catalog::configure(&mut files).await?;
    }
    if files.selected_task()?.is_none() {
        let task = crate::TaskId::parse(&owner.id)?;
        let mut layout = files
            .state(super::CommandFileRoot::Session, "layout.json")?
            .unwrap_or_else(|| serde_json::json!({}));
        layout["selected_task"] = serde_json::Value::String(task.to_string());
        files
            .write_state(super::CommandFileRoot::Session, "layout.json", &layout)
            .await?;
    }
    Ok(files)
}

/// Follow the active local agent's goal/plans through its authorized file binding.
pub async fn local_documents(
    local: &LocalHarness,
    views: &mut super::ClientViews,
) -> Result<crate::task_volume::ChangeStream> {
    // TODO(#495): obtain document count/content/work bounds from followed settings.
    let files = local.scratch_files(acyclic_fs::performance::BOUNDARY_BUDGET)?;
    super::document_files(&files, views, 4096, 65_536).await
}
