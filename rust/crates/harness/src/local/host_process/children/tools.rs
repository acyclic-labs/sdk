//! The coordination tools a native host's model calls, served through the
//! shared stdio MCP tool server. Native CLIs own agent loops, tools and
//! policy; these tools only map onto [`NativeChildren`], and a wait parks
//! until its child is idle.
use super::*;
use crate::mcp::stdio::{ToolCall, ToolProvider, ToolServer};
use serde_json::json;
use std::collections::BTreeSet;
use std::time::Duration;

/// Waits parked at once, bounded like the host's request concurrency.
const WAITS: usize = 16;

/// The tools of one native host's parent, opening its children on first use.
struct Coordination {
    host: String,
    children: Option<NativeChildren>,
}

/// Serves `host`'s coordination tools on stdio until its client exits, then
/// interrupts every child still running.
pub fn serve(host: &str) -> Result<()> {
    let mut coordination = Coordination {
        host: host.into(),
        children: None,
    };
    let served = ToolServer {
        name: "acyclic-sessions",
        version: env!("CARGO_PKG_VERSION"),
        parked: WAITS,
    }
    .serve(&mut coordination, std::io::stdin(), std::io::stdout());
    let stopped = coordination
        .children
        .as_mut()
        .map_or(Ok(()), NativeChildren::shutdown);
    served.and(stopped)
}

impl ToolProvider for Coordination {
    fn tools(&self) -> Value {
        json!({"tools":(["spawn","send","wait","archive","cancel","resume"].map(|name| {
            let prompt=matches!(name,"spawn"|"send"|"resume");
            json!({"name":name,"description":format!("{name} a separate native session in an admitted harness view. Native tools and policy are preserved. Native subagents are unmanaged."),"inputSchema":{"type":"object","properties":{"reference":{"type":"string"},"prompt":{"type":"string"}},"required":if prompt {vec!["reference","prompt"]} else {vec!["reference"]},"additionalProperties":false}})
        }))})
    }

    fn call(&mut self, name: &str, arguments: &Value) -> Result<ToolCall> {
        let object = arguments
            .as_object()
            .ok_or_else(|| invalid("tool arguments must be an object"))?;
        if object
            .keys()
            .any(|key| !matches!(key.as_str(), "reference" | "prompt"))
        {
            return Err(invalid("unsupported coordination argument"));
        }
        let reference = object
            .get("reference")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("reference is required"))?;
        let prompt = || {
            object
                .get("prompt")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| invalid("prompt is required"))
        };
        if self.children.is_none() {
            self.children = Some(NativeChildren::open(&self.host)?);
        }
        let children = self
            .children
            .as_mut()
            .ok_or_else(|| invalid("native children absent"))?;
        let answer = match name {
            "spawn" => children.spawn(reference, prompt()?)?,
            "send" => children.send(reference, prompt()?)?,
            "resume" => children.resume(reference, prompt()?)?,
            "archive" => children.archive(reference)?,
            "cancel" => children.cancel(reference)?,
            "wait" => match children.wait(reference)? {
                Some(answer) => answer,
                None => return Ok(ToolCall::Park(reference.into())),
            },
            _ => return Err(invalid("unknown coordination tool")),
        };
        Ok(ToolCall::Answer(answer))
    }

    fn progress(
        &mut self,
        parked: &BTreeSet<String>,
        settled: &mut Vec<(String, Value)>,
    ) -> Result<Option<Duration>> {
        let Some(children) = self.children.as_mut() else {
            return Ok(None);
        };
        let (flowed, _) = children.pump()?;
        // A parked wait settles once its child is idle: its own process
        // exited, or a cancel or archive stopped it.
        for reference in parked {
            if let Some(answer) = children.wait(reference)? {
                settled.push((reference.clone(), answer));
            }
        }
        // Only this owner's own processes can settle a wait, so with none
        // running it blocks on its host. Ready output schedules another
        // bounded pass; idle waiting cannot throttle a backpressured pipe.
        Ok(children.running().then_some(if flowed {
            Duration::ZERO
        } else {
            Duration::from_millis(20)
        }))
    }
}
