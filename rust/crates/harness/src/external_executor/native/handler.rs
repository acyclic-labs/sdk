//! Hook handling over an existing, authenticated provider binding.
use crate::{
    Error, Result,
    external_executor::{ExternalAction, ExternalHost, ExternalRequest, request},
    task_volume::{Component, TaskVolume},
};
use acyclic_stream::StreamProvider;
use serde_json::Value;

/// Handles a native event using the caller's already-bound task volumes.
/// The same normalization and response substitution serve the process adapter.
pub async fn handle_hook<P: StreamProvider + ?Sized, R: StreamProvider + ?Sized>(
    host: ExternalHost,
    event: &str,
    input: Value,
    requests: &TaskVolume<P>,
    results: &TaskVolume<R>,
    task: Component,
) -> Result<Value> {
    let (request, mcp) = prepare(host, event, input)?;
    let result = request
        .send(requests, results, task.clone())
        .await
        .map_err(super::storage)?;
    answer(host, event, request, result, task, mcp)
}

pub(crate) fn prepare(
    host: ExternalHost,
    event: &str,
    mut input: Value,
) -> Result<(ExternalRequest, bool)> {
    let mcp = super::super::handoff::normalize_tool(host, event, &mut input)?;
    if host != ExternalHost::Pi && event == "PreToolUse" {
        input
            .as_object_mut()
            .ok_or_else(|| Error::Invalid("native hook input is not an object".into()))?
            .insert("hostExecuted".into(), Value::Bool(!mcp));
    }
    Ok((request(host, event, input)?, mcp))
}

pub(super) fn answer(
    host: ExternalHost,
    event: &str,
    request: ExternalRequest,
    answer: Value,
    task: Component,
    mcp: bool,
) -> Result<Value> {
    if matches!(request.action, ExternalAction::HostTool { .. }) {
        super::super::answer::host_tool(host, event, &request, answer)
    } else if mcp {
        super::super::handoff::answer(request, task, &answer)
    } else if host == ExternalHost::Pi && matches!(request.action, ExternalAction::RunTool { .. }) {
        super::super::tool_result(answer)
    } else {
        super::super::answer::validate(host, event, answer)
    }
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn bound_hook_future_stays_below_sixteen_kibibytes() -> Result<()> {
        let provider = Arc::new(acyclic_stream::MemoryStream::default());
        let requests = TaskVolume::open(
            provider.clone(),
            acyclic_stream::StreamPath::new("requests").unwrap(),
        )
        .await
        .map_err(super::super::storage)?;
        let results = TaskVolume::open(
            provider,
            acyclic_stream::StreamPath::new("results").unwrap(),
        )
        .await
        .map_err(super::super::storage)?;
        let hook = handle_hook(
            ExternalHost::Pi,
            "session_start",
            serde_json::json!({"sessionId":"size","cwd":"project"}),
            &requests,
            &results,
            Component::new("task").unwrap(),
        );
        let size = std::mem::size_of_val(&hook);
        eprintln!("bound native hook future: {size} bytes");
        assert!(size < 16 * 1024, "bound hook future grew to {size} bytes");
        Ok(())
    }
}
