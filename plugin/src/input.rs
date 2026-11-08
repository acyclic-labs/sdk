//! Native stdin and selected-task admission, before any workspace operation.
use acyclic_harness::agent::AgentPath;
use serde_json::{Value, json};
use std::io::{self, Read as _};

pub(super) type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub(super) fn parse_task(value: &str) -> Result<AgentPath> {
    Ok(serde_json::from_value(json!(value))?)
}

pub(super) fn read_json(input: impl io::Read) -> Result<Value> {
    let mut bytes = Vec::new();
    input.take(65_537).read_to_end(&mut bytes)?;
    if bytes.len() > 65_536 {
        return Err("native hook exceeds shared command file bound".into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}

/// Map native hook fields once, before the shared workspace boundary.
/// A non-idle Stop still belongs to Antigravity's running background work.
pub(super) fn agy_hook(
    event: &str,
    input: &mut Value,
    workspace: Option<&str>,
) -> Result<Option<&'static str>> {
    let mapped = match event {
        "SessionStart" => "SessionStart",
        // The awaited admission must retry the shared capture even if the
        // native host ignored an earlier PostToolUse failure.
        "PreInvocation" | "PostToolUse" => "ToolBoundary",
        "Stop" => match input.get("fullyIdle").and_then(Value::as_bool) {
            Some(false) => return Ok(None),
            Some(true) => "Stop",
            None => return Err("step=admit Antigravity retirement: fullyIdle absent; recovery=update agy to a version reporting native background retirement".into()),
        },
        _ => return Err("step=admit Antigravity hook: unknown event; recovery=acyclic install agy".into()),
    };
    let cwd = workspace.filter(|path| !path.is_empty()).or_else(|| {
        let paths = input.get("workspacePaths")?.as_array()?;
        if paths.len() != 1 { return None; }
        paths.first()?.as_str().filter(|path| !path.is_empty())
    }).ok_or("step=admit Antigravity workspace: exact workspace absent; recovery=acyclic host agy -- --sandbox --mode plan")?.to_owned();
    let session = input.get("conversationId").and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .ok_or("step=admit Antigravity session: conversationId absent; recovery=acyclic host agy -- --sandbox --mode plan")?.to_owned();
    let object = input
        .as_object_mut()
        .ok_or("native hook input must be an object")?;
    object.insert("cwd".into(), json!(cwd));
    object.insert("session_id".into(), json!(session));
    Ok(Some(mapped))
}
