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
    let result = session
        .consume(request, |operation, target, resolution| {
            local.dispatch_command(operation, target, resolution)
        })
        .await?;
    if let Some(task) = result.as_ref().and_then(|value| value.get("selected_task")) {
        let mut layout = session
            .state(CommandFileRoot::Session, "layout.json")?
            .unwrap_or_else(|| serde_json::json!({}));
        layout["selected_task"] = task.clone();
        session
            .write_state(CommandFileRoot::Session, "layout.json", &layout)
            .await?;
    }
    Ok(result)
}
