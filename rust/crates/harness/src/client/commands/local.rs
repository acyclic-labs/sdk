//! Request-file dispatch belongs to the harness owner, never to a renderer.
use super::*;
use crate::{OperationId, local::LocalHarness, task_volume::FileValue};
use std::sync::Arc;

/// Consume an immutable request through the local owner. Both TUI and headless
/// clients use the same claim/result files and request-bound task selection.
pub async fn consume_local_command<P: acyclic_stream::StreamProvider + ?Sized>(
    session: &mut CommandSession<P>,
    local: &LocalHarness,
    request: &FileValue,
) -> Result<Option<serde_json::Value>> {
    let settings = session.request_settings(request).await?;
    let result = session
        .consume(request, |operation, target, resolution| {
            let future: acyclic_stream::BoxProviderFuture<'_, Result<serde_json::Value>> = Box::pin(
                local.dispatch_configured_command(operation, target, resolution, &settings),
            );
            future
        })
        .await?;
    Ok(result)
}

impl CommandSession<dyn acyclic_stream::StreamProvider> {
    /// Observe command files in the local owner's already-authorized private volume.
    pub async fn open_local(
        local: &LocalHarness,
        session: uuid::Uuid,
        maximum_bytes: u64,
        maximum_files: usize,
    ) -> Result<Self> {
        let maximum_bytes = maximum_bytes.min(local.task_files().maximum_bytes());
        let volume = local
            .task_files()
            .inner
            .reopen_erased()
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        let mut files = Self::open(
            std::sync::Arc::new(volume),
            session,
            maximum_bytes,
            maximum_files,
        )
        .await?;
        local.bind_client_files(&mut files).await?;
        Ok(files)
    }
}

/// Execute an admitted command through the merged owner dispatch and stock runner.
/// Admission-only clients can continue to use `consume_local_command`.
pub async fn run_local_command<P: acyclic_stream::StreamProvider + ?Sized>(
    session: &mut CommandSession<P>,
    local: &LocalHarness,
    request: &FileValue,
) -> Result<Option<serde_json::Value>> {
    let settings = session.request_settings(request).await?;
    session
        .consume_with_recovery::<true, _, _, _, _>(
            request,
            |operation, target, resolution| {
                Box::pin(execute(local, operation, target, resolution, &settings))
            },
            |operation, target, resolution| {
                let settings = &settings;
                async move {
                    if !matches!(
                        &resolution,
                        CommandResolution::Prompt { .. }
                            | CommandResolution::Tool {
                                tool: CommandTool::Spawn,
                                ..
                            }
                            | CommandResolution::Submit {
                                request: CommandRequest::Replace { .. }
                            }
                    ) {
                        return Err(Error::Indeterminate(operation));
                    }
                    let task = crate::runtime::task_id_for_admission(operation);
                    match local.resume(task).await {
                        // No turn or admission was saved after the claim. Replay
                        // only these idempotent admissions from their request.
                        Err(Error::NotFound(_)) => {
                            Box::pin(execute(local, operation, target, resolution, settings)).await
                        }
                        output => {
                            let mut value = serde_json::to_value(output?)
                                .map_err(|error| Error::Invalid(error.to_string()))?;
                            value["task"] = serde_json::json!(task);
                            value["selected_task"] = serde_json::json!(task);
                            value["admission_operation"] = serde_json::json!(operation);
                            Ok(value)
                        }
                    }
                }
            },
        )
        .await
}

async fn execute(
    local: &LocalHarness,
    operation: OperationId,
    target: Option<crate::TaskId>,
    resolution: CommandResolution,
    settings: &serde_json::Value,
) -> Result<serde_json::Value> {
    let target = target.filter(|task| task.to_string() != local.client_owner().id);
    let mut result = local
        .dispatch_configured_command(operation, target, resolution, settings)
        .await?;
    if result.get("admission_operation").is_some() {
        let task = result
            .get("task")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| Error::Invalid("command admission has no task".into()))?;
        let output = serde_json::to_value(local.resume(crate::TaskId::parse(task)?).await?)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        result
            .as_object_mut()
            .ok_or_else(|| Error::Invalid("command result must be an object".into()))?
            .extend(
                output
                    .as_object()
                    .ok_or_else(|| Error::Invalid("turn output must be an object".into()))?
                    .clone(),
            );
    }
    Ok(result)
}

/// Follows ordinary command files and drives their durable local owner.
/// Poll this future alongside input/follows; dropping it never resends a claim.
pub async fn run_local_commands(local: Arc<LocalHarness>, session: OperationId) -> Result<()> {
    Box::pin(run_local_command_files(local, session, false)).await
}

pub(crate) async fn run_local_command_files(
    local: Arc<LocalHarness>,
    session: OperationId,
    only_session: bool,
) -> Result<()> {
    let mut commands = CommandSession::open(
        Arc::new(
            local
                .task_files()
                .inner
                .reopen()
                .await
                .map_err(|error| Error::Storage(error.to_string()))?,
        ),
        uuid::Uuid::from_bytes(session.into_bytes()),
        65_536,
        4096,
    )
    .await?;
    loop {
        let request = commands.next_request().await?;
        if only_session && !matches!(request.key, crate::task_volume::FileKey::Session { .. }) {
            continue;
        }
        if let Err(error) = run_local_command(&mut commands, &local, &request).await {
            if !only_session && commands.failure_is_saved(&request, &error).await? {
                continue;
            }
            // Only a failed claim CAS was requeued by the shared consumer.
            // Headless failures retain their existing terminal error contract.
            if matches!(&error, Error::Conflict(message)
                if message == "command query advanced; retry from the new cut")
            {
                continue;
            }
            return Err(error);
        }
        if only_session {
            return Ok(());
        }
    }
}

/// Drive the local owner throughout presentation, including awaited input writes.
/// Polling it only while waiting for events can strand a file lock held by an
/// owner future while presentation waits for that same file operation.
pub async fn with_local_commands<T, E: From<Error>>(
    local: Arc<LocalHarness>,
    session: OperationId,
    presentation: impl std::future::Future<Output = std::result::Result<T, E>>,
) -> std::result::Result<T, E> {
    super::scheduling::drive_command_owner(run_local_commands(local, session), presentation).await
}
