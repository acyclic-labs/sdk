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

/// Project prompt roots: `.claude/commands` and `.codex/prompts` below the
/// local workspace. TODO(#495): add the user-scope roots from client settings.
fn command_roots(local: &LocalHarness) -> Vec<super::CommandRoot> {
    vec![super::CommandRoot {
        scope: super::CommandScope::Project,
        root: crate::context::ContextRoot {
            volume: local.project().clone(),
            directory: String::new(),
        },
    }]
}

/// Resolve one headless input exactly as the TUI catalog would: plain text is a
/// prompt, slash input is a command or an error, never chat. Selections,
/// confirmations and renderer-only requests have no headless form.
pub async fn local_input(
    local: &LocalHarness,
    operation: OperationId,
    input: &str,
) -> Result<crate::task_volume::FileValue> {
    use super::{CommandRequest, CommandResolution};
    let reader = local.client_reader()?;
    let catalog = super::CommandCatalog::capture(
        reader.as_ref(),
        &command_roots(local),
        &[],
        crate::context::ContextDiscoveryLimits::default(),
    )
    .await?;
    let resolution = catalog
        .resolve(
            input,
            reader.as_ref(),
            super::local_headless::MAXIMUM_BYTES as u64,
        )
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
        // One startup capture of project prompts into the session catalog, the
        // same discovery headless input resolves with. A malformed prompt file
        // is session feedback, never a failed start.
        let captured = super::CommandSources::observe(
            &mut files,
            super::DEFAULT_COMMANDS_JSON.to_vec(),
            command_roots(local),
            Vec::new(),
            local.client_reader()?,
            crate::context::ContextDiscoveryLimits::default(),
            65_536,
            4096,
        )
        .await;
        match captured {
            Ok(_) => {}
            Err(
                error @ (crate::Error::Invalid(_)
                | crate::Error::Conflict(_)
                | crate::Error::NotFound(_)),
            ) => {
                files
                    .write_state(
                        super::CommandFileRoot::Session,
                        "notifications.json",
                        &serde_json::json!([format!("project commands: {error}")]),
                    )
                    .await?;
            }
            Err(error) => return Err(error),
        }
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
    authority: &wire::Authority,
) -> Result<acyclic_stream::BoxProviderStream<'static, Result<super::ViewUpdate>>> {
    // TODO(#495): obtain document count/content/work bounds from followed settings.
    let files = local.scratch_files(acyclic_fs::performance::BOUNDARY_BUDGET)?;
    super::document_files(&files, authority, 4096, 65_536).await
}
