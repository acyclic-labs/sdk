//! Request-file dispatch belongs to the harness owner, never to a renderer.
use super::*;
use crate::{local::LocalHarness, task_volume::FileValue};

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
