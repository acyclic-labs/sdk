//! Lifecycle effects retain protected immutable dispatch and context records.
use super::*;
use crate::OperationId;

pub(crate) fn phase_operation(tool: OperationId, event: HookEvent) -> OperationId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"harness:tool-hook:v1\0");
    hasher.update(&tool.into_bytes());
    hasher.update(event.name().as_bytes());
    let mut id = [0; 16];
    id.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    OperationId::from_bytes(id)
}

pub(crate) fn approval_identity(manifest: [u8; 32]) -> crate::registry::ComponentIdentity {
    crate::registry::ComponentIdentity {
        name: "hooks.pre-tool".into(),
        version: "1".into(),
        digest: manifest,
    }
}

/// Runs one event's hooks at most once per call operation. The outcome is
/// staged on the agent's private volume at `hooks/{turn}/{operation}.json`
/// before it is used, so a rebuilt turn reads it back and never runs the hooks
/// again. A reconciling turn that finds no outcome returns `None`.
pub async fn run_once<F, Fut>(
    context: &crate::runtime::ToolContext,
    event: HookEvent,
    reconcile_only: bool,
    run: F,
) -> crate::Result<Option<HookOutput>>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = crate::Result<HookOutput>>,
{
    let operation = phase_operation(context.operation_id(), event);
    if let (Some(file), _) = context.observe_staged_file_once(operation, None).await? {
        let bytes = context.task().read_file(&file, None).await?;
        return serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|error| crate::Error::Invalid(format!("retained hook outcome: {error}")));
    }
    if reconcile_only {
        return Ok(None);
    }
    let output = run().await?;
    let bytes = serde_json::to_vec(&output)
        .map_err(|error| crate::Error::Invalid(format!("hook outcome: {error}")))?;
    let path = format!("hooks/{}/{operation}.json", context.turn_id());
    context
        .stage_file_once(
            operation,
            &path,
            &bytes,
            "application/json",
            event.name(),
            None,
        )
        .await?;
    Ok(Some(output))
}
