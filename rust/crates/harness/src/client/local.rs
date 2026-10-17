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
