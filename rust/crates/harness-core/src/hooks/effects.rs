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

/// Runs one event's hooks at most once per call operation. A start marker
/// is staged on the agent's private volume before the hooks run, and the
/// outcome at `hooks/{turn}/{operation}.json` after, so a rebuilt turn reads
/// the outcome back and never runs the hooks again. A turn that finds the
/// marker but no outcome was interrupted mid-run: the hooks may have acted,
/// so they do not run again, and the `interrupted` outcome (each hook's
/// failure policy) is recorded in their place. A reconciling turn that finds
/// no outcome returns `None`.
pub async fn run_once<F, Fut>(
    context: &crate::runtime::ToolContext,
    event: HookEvent,
    reconcile_only: bool,
    (run, interrupted): (F, impl FnOnce() -> crate::Result<HookOutput>),
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
    let started = start_operation(operation);
    let path = format!("hooks/{}/{operation}", context.turn_id());
    let output = if context
        .observe_staged_file_once(started, None)
        .await?
        .0
        .is_some()
    {
        interrupted()?
    } else {
        context
            .stage_file_once(
                started,
                &format!("{path}.started"),
                b"{}",
                "application/json",
                event.name(),
                None,
            )
            .await?;
        run().await?
    };
    let bytes = serde_json::to_vec(&output)
        .map_err(|error| crate::Error::Invalid(format!("hook outcome: {error}")))?;
    context
        .stage_file_once(
            operation,
            &format!("{path}.json"),
            &bytes,
            "application/json",
            event.name(),
            None,
        )
        .await?;
    Ok(Some(output))
}

/// The marker operation staged before `operation`'s hooks run.
fn start_operation(operation: OperationId) -> OperationId {
    let mut hasher = blake3::Hasher::new_derive_key("acyclic.hooks.started/v1");
    hasher.update(&operation.into_bytes());
    let mut id = [0; 16];
    hasher.finalize_xof().fill(&mut id);
    OperationId::from_bytes(id)
}
