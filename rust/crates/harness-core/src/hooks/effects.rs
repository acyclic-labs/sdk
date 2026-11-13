//! Lifecycle effects retain protected immutable dispatch and context records.
use super::*;
use crate::OperationId;

/// Stable delivery identity for one registered hook and one original record.
/// The record must come from the host's authorized read, including its original
/// account and selected stream. This identity grants no source or execution
/// authority. Program revisions are deliberately excluded: editing a program
/// cannot make an already delivered event execute again.
/// This is an identity, not a claim: the host serializes delivery through its
/// existing actor, and the ordinary hook outcome journal owns execution.
pub fn record_operation(
    account: &acyclic_auth::addressing::AccountId,
    selected: &acyclic_stream::StreamPath,
    record: &acyclic_stream::Record,
    instance: &str,
) -> OperationId {
    let mut hash = blake3::Hasher::new_derive_key("acyclic.hooks.record/v1");
    for bytes in [
        account.as_bytes(),
        selected.as_str().as_bytes(),
        record.origin.as_str().as_bytes(),
        instance.as_bytes(),
    ] {
        hash.update(&(bytes.len() as u64).to_be_bytes());
        hash.update(bytes);
    }
    hash.update(&record.sequence.to_be_bytes());
    hash.update(record.commit_id.as_bytes());
    let mut operation = [0; 16];
    hash.finalize_xof().fill(&mut operation);
    OperationId::from_bytes(operation)
}

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
/// is staged on the host journal before the hooks run, and the
/// outcome at `hooks/{turn}/{operation}.json` after, so a rebuilt turn reads
/// the outcome back and never runs the hooks again. A turn that finds the
/// marker but no outcome was interrupted mid-run: the hooks may have acted,
/// so they do not run again, and the `interrupted` outcome (each hook's
/// failure policy) is recorded in their place. A reconciling turn that finds
/// no outcome returns `None`.
/// The original execution journal's child-effect claim excludes concurrent
/// attempts before any replay read; a live duplicate refuses with conflict.

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
    let (content, publisher) = context.task().retained_content()?;
    let limits = context.effective_scope().limits();
    let operation = phase_operation(context.operation_id(), event);
    let owner = context.task().claim_effect(operation, None)?;
    if let Some(file) = content
        .observe_retained(publisher, operation, limits)
        .await?
    {
        let (bytes, _hold) = crate::conversation::verified_content_bytes(
            content.reader.as_ref(),
            &file,
            limits.file_bytes,
            None,
            None,
        )
        .await?;
        let output = serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|error| crate::Error::Invalid(format!("retained hook outcome: {error}")))?;
        owner.release();
        return Ok(output);
    }
    if reconcile_only {
        owner.release();
        return Ok(None);
    }
    let started = start_operation(operation);
    let path = format!("hooks/{}/{operation}", context.turn_id());
    let output = if content
        .observe_retained(publisher, started, limits)
        .await?
        .is_some()
    {
        interrupted()?
    } else {
        content
            .retain_once(
                publisher,
                started,
                (
                    &format!("{path}.started"),
                    b"{}",
                    "application/json",
                    event.name(),
                ),
                limits,
            )
            .await?;
        run().await?
    };
    let bytes = serde_json::to_vec(&output)
        .map_err(|error| crate::Error::Invalid(format!("hook outcome: {error}")))?;
    content
        .retain_once(
            publisher,
            operation,
            (
                &format!("{path}.json"),
                &bytes,
                "application/json",
                event.name(),
            ),
            limits,
        )
        .await?;
    owner.release();
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

#[cfg(all(test, not(loom), not(target_arch = "wasm32")))]
mod tests;
