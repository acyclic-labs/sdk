//! Codex's unlisted lifecycle MCP hook. No model-visible tools or execution.
use super::*;
use std::io::{BufRead as _, Write as _};

pub(super) fn serve() -> Result<()> {
    let stdin = io::stdin();
    let mut input = stdin.lock();
    loop {
        let mut line = Vec::new();
        if input.by_ref().take(65_537).read_until(b'\n', &mut line)? == 0 {
            return Ok(());
        }
        if line.len() > 65_536 {
            return Err("MCP hook exceeds shared command file bound".into());
        }
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let response = match serde_json::from_slice(&line) {
            Ok(message) => answer(&message),
            Err(_) => Some(
                json!({"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"invalid JSON"}}),
            ),
        };
        if let Some(response) = response {
            let mut output = io::stdout().lock();
            serde_json::to_writer(&mut output, &response)?;
            writeln!(output)?;
            output.flush()?;
        }
    }
}

fn answer(message: &Value) -> Option<Value> {
    let id = message.get("id")?;
    let params = message.get("params").unwrap_or(&Value::Null);
    let result = match message.get("method").and_then(Value::as_str) {
        Some("initialize") => {
            json!({"protocolVersion":params.get("protocolVersion").and_then(Value::as_str).unwrap_or("2025-06-18"),"capabilities":{"tools":{}},"serverInfo":{"name":"acyclic-hooks","version":env!("CARGO_PKG_VERSION")}})
        }
        Some("ping") => json!({}),
        Some("tools/list") => json!({"tools":[]}),
        Some("tools/call") if params.get("name").and_then(Value::as_str) == Some("hook") => {
            let event = params
                .pointer("/arguments/event")
                .and_then(Value::as_str)
                .unwrap_or("PreToolUse");
            let mut input = params
                .pointer("/arguments/hook")
                .cloned()
                .unwrap_or(Value::Null);
            if let Some(thread) = params.pointer("/_meta/threadId").and_then(Value::as_str)
                && let Some(object) = input.as_object_mut()
            {
                object.insert("thread_id".into(), json!(thread));
            }
            let response = hook("codex", event, input);
            json!({"content":[{"type":"text","text":response.to_string()}]})
        }
        _ => {
            return Some(
                json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"method not found"}}),
            );
        }
    };
    Some(json!({"jsonrpc":"2.0","id":id,"result":result}))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_bridge_exposes_no_agent_or_tool_catalog() {
        assert_eq!(
            answer(&json!({"id":1,"method":"tools/list"}))
                .unwrap()
                .pointer("/result/tools")
                .cloned()
                .unwrap(),
            json!([])
        );
        assert_eq!(
            answer(&json!({"id":2,"method":"tools/call","params":{"name":"run_tool"}}))
                .unwrap()
                .pointer("/error/code")
                .cloned()
                .unwrap(),
            -32601
        );
        assert!(answer(&json!({"method":"notifications/initialized"})).is_none());
    }
}
