//! The coordination tools a native host's model calls, served through the
//! shared stdio MCP tool server. Native CLIs own agent loops, tools and
//! policy; these tools only map onto [`NativeChildren`], and a wait parks
//! until its child is idle. Between passes the server sleeps on its client
//! and the children's ready queue together: no thread, timer or poll per
//! child, and no pass rebuilds or probes a quiet child's readiness.
use super::*;
use crate::mcp::stdio::{Settled, ToolCall, ToolProvider, ToolServer, Wake};
use serde_json::json;
use std::collections::BTreeSet;

/// Waits parked at once, bounded like the host's request concurrency.
const WAITS: usize = 16;
/// Bound of one request line.
const FRAME_BYTES: u32 = 65_536;

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
        frame_bytes: FRAME_BYTES,
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
        settled: &mut Vec<(String, Settled)>,
    ) -> Result<Option<Wake>> {
        let Some(children) = self.children.as_mut() else {
            return Ok(None);
        };
        // Only children whose readiness fired are pumped.
        children.pump()?;
        // A parked wait settles once its child is idle: its own process
        // exited, or a cancel or archive stopped it.
        for reference in parked {
            if let Some(answer) = children.wait(reference)? {
                settled.push((reference.clone(), Ok(answer)));
            }
        }
        // Only this owner's own processes can settle a wait, so with none
        // running it blocks on its host. Otherwise the next pass runs once a
        // child's pipe, stdin room or exit fires its own registration, or a
        // bounded pass left output unread and queued it again at once.
        if !children.running() {
            return Ok(None);
        }
        Ok(Some(Box::pin(children.woken())))
    }
}
