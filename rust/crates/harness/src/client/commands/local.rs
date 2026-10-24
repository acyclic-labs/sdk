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
            local.dispatch_configured_command(operation, target, resolution, &settings)
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
        Self::open(
            std::sync::Arc::new(volume),
            session,
            maximum_bytes,
            maximum_files,
        )
        .await
    }
}

/// Observe a runtime-owned command subscription while presenting its files.
/// Closing presentation stops admission and joins admitted work before returning.
pub async fn with_local_commands<T, E: From<Error>>(
    local: Arc<LocalHarness>,
    session: OperationId,
    presentation: impl std::future::Future<Output = std::result::Result<T, E>>,
) -> std::result::Result<T, E> {
    let mut subscription = crate::package::harness::CommandSubscription::start(
        local,
        &acyclic_stream::runtime::PlatformExecutor,
        session,
        false,
    );
    let attachment = subscription.take_attachment();
    super::scheduling::drive_command_owner(subscription, presentation, || drop(attachment)).await
}
