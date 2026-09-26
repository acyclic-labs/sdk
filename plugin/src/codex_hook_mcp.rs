//! Codex hook transport over MCP: `acyclic __mcp`.
//!
//! Codex runs every command hook through the session shell, which on Windows
//! is `PowerShell` and costs far more than the hook itself. Codex can instead
//! call a tool on an MCP server it keeps connected for the whole thread, and
//! the plugin declares this server for exactly that purpose. It lists no
//! tools, so nothing is exposed to the model; Codex's hook runner calls the
//! unlisted `hook` tool directly, without approval, and reads the tool's text
//! the way it reads a command hook's standard output.
//!
//! A call carries one hook event, rebuilt by Codex from the argument template
//! in `hooks/hooks.json`. It is answered by the same function that answers
//! `acyclic __hook codex EVENT`, so the text is exactly what that process
//! would print. The server is a pure request transport: the service's
//! lifecycle belongs to the `SessionStart` and `SessionEnd` command hooks
//! (see [`super::ServiceStart::Forbidden`]).

use super::{
    HookFailure, MAXIMUM_CONTROL_MESSAGE_BYTES, ServiceStart, display, forward_native_hook,
    local_native_hook_answer,
};
use serde_json::{Value, json};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _};
use tokio::sync::Mutex;

/// The server's name in the plugin's `.mcp.json` and in every hook's
/// `server` field.
pub(crate) const SERVER: &str = "acyclic-hooks";

/// The one tool the server answers.
pub(crate) const TOOL: &str = "hook";

/// The hook host every call is answered as.
const HOST: &str = "codex";

/// Every Codex event delivered over MCP, with the event fields its template
/// forwards: each one the service reads for that event. Codex fails a hook
/// whose template names a field the event lacks, so every field here is one
/// Codex's schema requires for that event (`agent_id` is required only by the
/// subagent lifecycle events; tool events carry the calling thread instead,
/// see `codex_thread_agent`).
///
/// `SessionStart` and `SessionEnd` stay command hooks. `SessionStart` is the
/// only event that starts or advances the service, which this server must
/// never parent; Codex does not run MCP hooks for `SessionEnd`.
pub(crate) const EVENTS: [(&str, &[&str]); 5] = [
    ("UserPromptSubmit", &["session_id", "turn_id", "cwd"]),
    (
        "PreToolUse",
        &[
            "session_id",
            "turn_id",
            "cwd",
            "tool_name",
            "tool_input",
            "tool_use_id",
        ],
    ),
    (
        "PostToolUse",
        &[
            "session_id",
            "turn_id",
            "cwd",
            "tool_name",
            "tool_input",
            "tool_use_id",
        ],
    ),
    (
        "SubagentStart",
        &["session_id", "turn_id", "cwd", "agent_id", "agent_type"],
    ),
    (
        "SubagentStop",
        &["session_id", "turn_id", "cwd", "agent_id"],
    ),
];

/// The `mcp_tool` hook handler for `event`: its argument template names every
/// forwarded field as a whole-string placeholder, which Codex replaces with
/// the field's JSON value.
pub(crate) fn hook_handler(event: &str, fields: &[&str]) -> Value {
    let hook = fields
        .iter()
        .map(|field| ((*field).to_owned(), Value::String(format!("${{{field}}}"))))
        .collect::<serde_json::Map<_, _>>();
    json!({
        "type": "mcp_tool",
        "server": SERVER,
        "tool": TOOL,
        "input": {"event": event, "hook": hook},
    })
}

/// The Codex events delivered to `acyclic __hook codex EVENT` as command
/// hooks (see [`EVENTS`]).
pub(crate) const COMMAND_EVENTS: [&str; 2] = ["SessionStart", "SessionEnd"];

/// The plugin's `.mcp.json`: this server, started by Codex from the plugin
/// root. `required` makes Codex connect it before a thread runs any turn, so
/// no hook can find it absent. Codex starts MCP servers with a minimal
/// environment; the one variable this server reads beyond it locates the
/// service's state on Linux.
pub(crate) fn server_declaration() -> Value {
    json!({"mcpServers": {SERVER: {
        "type": "stdio",
        "command": "./bin/acyclic",
        "args": ["__mcp"],
        "cwd": ".",
        "env_vars": ["XDG_STATE_HOME"],
        "required": true,
    }}})
}

/// Whether `document`, the plugin's `hooks/hooks.json`, delivers every Codex
/// event Acyclic handles through its transport.
pub(crate) fn hook_manifest_is_current(document: &Value) -> bool {
    COMMAND_EVENTS.iter().all(|event| {
        handlers(document, event).any(|hook| {
            hook.get("type").and_then(Value::as_str) == Some("command")
                && hook.get("command").and_then(Value::as_str)
                    == Some(
                        format!("\"${{PLUGIN_ROOT}}/bin/acyclic\" __hook codex {event}").as_str(),
                    )
                && hook.get("commandWindows").and_then(Value::as_str)
                    == Some(
                        format!("& \"$env:PLUGIN_ROOT\\bin\\acyclic.exe\" __hook codex {event}")
                            .as_str(),
                    )
        })
    }) && EVENTS.iter().all(|(event, fields)| {
        let expected = hook_handler(event, fields);
        handlers(document, event).any(|hook| {
            ["type", "server", "tool", "input"]
                .iter()
                .all(|key| hook.get(key) == expected.get(key))
        })
    })
}

fn handlers<'a>(document: &'a Value, event: &str) -> impl Iterator<Item = &'a Value> {
    document
        .pointer(&format!("/hooks/{event}"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .flat_map(|group| {
            group
                .get("hooks")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
        })
}

/// Serves MCP over standard input and output until Codex closes the stream.
pub(crate) fn serve() -> Result<(), String> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(display)?
        .block_on(serve_stream(tokio::io::stdin(), tokio::io::stdout()))
}

/// Answers every request line on `input`. Tool calls run concurrently, so a
/// slow hook (a subagent fork) never delays another thread's tool hook; each
/// response is written as one whole line.
async fn serve_stream(
    input: impl tokio::io::AsyncRead + Unpin,
    output: impl tokio::io::AsyncWrite + Unpin + Send + 'static,
) -> Result<(), String> {
    let output = Arc::new(Mutex::new(output));
    let mut input = tokio::io::BufReader::new(input);
    let mut calls = tokio::task::JoinSet::new();
    loop {
        // Every request reaps the calls that have finished, so a long
        // session holds only the hooks still running.
        while let Some(finished) = calls.try_join_next() {
            finished.map_err(display)??;
        }
        let mut line = Vec::new();
        let read = (&mut input)
            .take((MAXIMUM_CONTROL_MESSAGE_BYTES + 1) as u64)
            .read_until(b'\n', &mut line)
            .await
            .map_err(display)?;
        if read == 0 {
            break;
        }
        if line.len() > MAXIMUM_CONTROL_MESSAGE_BYTES {
            return Err("Acyclic MCP request exceeds the maximum frame size".to_owned());
        }
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let message = match serde_json::from_slice::<Value>(&line) {
            Ok(message) => message,
            Err(error) => {
                let response = error_response(&Value::Null, -32700, &error.to_string());
                write_line(&output, &response).await?;
                continue;
            }
        };
        // Notifications, including `notifications/initialized` and
        // cancellations, need no answer.
        let Some(id) = message.get("id").cloned() else {
            continue;
        };
        let method = message.get("method").and_then(Value::as_str).unwrap_or("");
        let params = message.get("params").cloned().unwrap_or(Value::Null);
        if method == "tools/call" {
            let output = Arc::clone(&output);
            calls.spawn(async move {
                let response = match call_result(&params).await {
                    Ok(result) => json!({"jsonrpc": "2.0", "id": id, "result": result}),
                    Err(error) => error_response(&id, -32602, &error),
                };
                write_line(&output, &response).await
            });
            continue;
        }
        let response = match method {
            "initialize" => {
                // Nothing here depends on the protocol revision, so the
                // client's own is accepted.
                let version = params
                    .get("protocolVersion")
                    .and_then(Value::as_str)
                    .unwrap_or("2025-06-18");
                json!({"jsonrpc": "2.0", "id": id, "result": {
                    "protocolVersion": version,
                    "capabilities": {"tools": {"listChanged": false}},
                    "serverInfo": {"name": SERVER, "version": env!("CARGO_PKG_VERSION")},
                }})
            }
            "ping" => json!({"jsonrpc": "2.0", "id": id, "result": {}}),
            // The hook tool is for Codex's hook runner only; listing it would
            // offer it to the model.
            "tools/list" => json!({"jsonrpc": "2.0", "id": id, "result": {"tools": []}}),
            _ => error_response(&id, -32601, "method not found"),
        };
        write_line(&output, &response).await?;
    }
    while let Some(finished) = calls.join_next().await {
        finished.map_err(display)??;
    }
    Ok(())
}

/// The result of one `tools/call`: the hook's answer as text content.
async fn call_result(params: &Value) -> Result<Value, String> {
    if params.get("name").and_then(Value::as_str) != Some(TOOL) {
        return Err(format!(
            "the {SERVER} MCP server answers only the `{TOOL}` tool"
        ));
    }
    let thread = params
        .pointer("/_meta/threadId")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let answer = match hook_request(params.get("arguments"), thread) {
        Ok((event, cwd, input)) => {
            let failure = HookFailure::classify(HOST, event, Some((&cwd, &input)));
            match local_native_hook_answer(HOST, event, &input) {
                Ok(Some(answer)) => answer,
                Ok(None) => forward_native_hook(HOST, event, cwd, input, ServiceStart::Forbidden)
                    .await
                    .unwrap_or_else(|error| failure.answer(&error)),
                Err(error) => failure.answer(&error),
            }
        }
        // A tool hook still gates its tool when its call cannot be read.
        Err(error) => match params.pointer("/arguments/event").and_then(Value::as_str) {
            Some(event @ "PreToolUse") => HookFailure::classify(HOST, event, None).answer(&error),
            _ => return Err(error),
        },
    };
    let text = serde_json::to_string(&answer).map_err(display)?;
    Ok(json!({"content": [{"type": "text", "text": text}]}))
}

/// The event, working directory and hook input of one call: the fields its
/// template forwarded, plus the calling Codex thread.
pub(crate) fn hook_request(
    arguments: Option<&Value>,
    thread: Option<String>,
) -> Result<(&'static str, PathBuf, Value), String> {
    let arguments = arguments.ok_or("hook call has no arguments")?;
    let named = arguments
        .get("event")
        .and_then(Value::as_str)
        .ok_or("hook call names no event")?;
    let (event, fields) = EVENTS
        .iter()
        .find(|(event, _)| *event == named)
        .ok_or_else(|| format!("Codex {named} hooks are not delivered over MCP"))?;
    let hook = arguments
        .get("hook")
        .and_then(Value::as_object)
        .ok_or("hook call carries no hook event")?;
    if hook.len() != fields.len() || fields.iter().any(|field| !hook.contains_key(*field)) {
        return Err(format!(
            "Codex {event} hook fields do not match the plugin's hook template"
        ));
    }
    let cwd = hook
        .get("cwd")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .ok_or("hook event cwd is not a path")?;
    let mut input = hook.clone();
    if let Some(thread) = thread {
        input.insert("thread_id".to_owned(), Value::String(thread));
    }
    Ok((event, cwd, Value::Object(input)))
}

fn error_response(id: &Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
}

async fn write_line(
    output: &Mutex<impl tokio::io::AsyncWrite + Unpin>,
    message: &Value,
) -> Result<(), String> {
    let mut line = serde_json::to_vec(message).map_err(display)?;
    line.push(b'\n');
    let mut output = output.lock().await;
    output.write_all(&line).await.map_err(display)?;
    output.flush().await.map_err(display)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::indexing_slicing, clippy::panic)]
mod tests {
    use super::*;

    /// Codex 0.155.1's generated input schema for `event`.
    fn codex_input_schema(event: &str) -> Value {
        let schema = match event {
            "UserPromptSubmit" => include_str!(
                "../tests/fixtures/codex-0.155.1-hook-input-schemas/user-prompt-submit.command.input.schema.json"
            ),
            "PreToolUse" => include_str!(
                "../tests/fixtures/codex-0.155.1-hook-input-schemas/pre-tool-use.command.input.schema.json"
            ),
            "PostToolUse" => include_str!(
                "../tests/fixtures/codex-0.155.1-hook-input-schemas/post-tool-use.command.input.schema.json"
            ),
            "SubagentStart" => include_str!(
                "../tests/fixtures/codex-0.155.1-hook-input-schemas/subagent-start.command.input.schema.json"
            ),
            "SubagentStop" => include_str!(
                "../tests/fixtures/codex-0.155.1-hook-input-schemas/subagent-stop.command.input.schema.json"
            ),
            _ => panic!("no Codex schema fixture for {event}"),
        };
        serde_json::from_str(schema).expect("Codex schema")
    }

    #[test]
    fn every_forwarded_field_is_one_codex_always_sends() {
        for (event, fields) in EVENTS {
            let schema = codex_input_schema(event);
            assert_eq!(
                schema.pointer("/properties/hook_event_name/const"),
                Some(&json!(event))
            );
            let required = schema["required"].as_array().expect("required fields");
            for field in fields {
                assert!(
                    required.contains(&json!(field)),
                    "Codex may omit {event}.{field}, which would fail the hook"
                );
            }
        }
    }

    #[test]
    fn every_field_the_service_reads_is_forwarded() {
        // The fields the service's Codex handling reads per event (see
        // `dispatch_native_session_hook` and `native_tool_identity`); the
        // service-level equivalence test proves nothing else is read.
        let tool = [
            "session_id",
            "turn_id",
            "cwd",
            "tool_name",
            "tool_input",
            "tool_use_id",
        ];
        let read: [(&str, &[&str]); 5] = [
            ("UserPromptSubmit", &["session_id", "turn_id", "cwd"]),
            ("PreToolUse", &tool),
            ("PostToolUse", &tool),
            (
                "SubagentStart",
                &["session_id", "turn_id", "agent_id", "agent_type"],
            ),
            ("SubagentStop", &["session_id", "turn_id", "agent_id"]),
        ];
        for (event, fields) in read {
            let (_, forwarded) = EVENTS
                .iter()
                .find(|(name, _)| *name == event)
                .expect("event is delivered over MCP");
            for field in fields {
                assert!(forwarded.contains(field), "{event} drops {field}");
            }
        }
    }

    #[test]
    fn the_plugin_manifests_declare_exactly_this_transport() {
        let hooks: Value =
            serde_json::from_str(include_str!("../hooks/hooks.json")).expect("hook manifest");
        assert!(hook_manifest_is_current(&hooks));
        let mut events = hooks["hooks"]
            .as_object()
            .expect("events")
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        events.sort();
        let mut expected = EVENTS
            .iter()
            .map(|(event, _)| (*event).to_owned())
            .chain(COMMAND_EVENTS.iter().map(|event| (*event).to_owned()))
            .collect::<Vec<_>>();
        expected.sort();
        assert_eq!(events, expected);
        for (event, fields) in EVENTS {
            let groups = hooks["hooks"][event].as_array().expect("groups");
            assert_eq!(groups.len(), 1);
            let handler = &groups[0]["hooks"][0];
            let mut generated = hook_handler(event, fields);
            for key in ["timeout", "statusMessage"] {
                if let Some(value) = handler.get(key) {
                    generated[key] = value.clone();
                }
            }
            assert_eq!(handler, &generated, "{event} handler");
        }
        let declaration: Value =
            serde_json::from_str(include_str!("../.mcp.json")).expect("MCP declaration");
        assert_eq!(declaration, server_declaration());
        let codex: Value = serde_json::from_str(include_str!("../.codex-plugin/plugin.json"))
            .expect("Codex manifest");
        assert_eq!(codex["mcpServers"], "./.mcp.json");
    }

    #[test]
    fn a_call_is_the_forwarded_fields_and_the_calling_thread() {
        let arguments = json!({"event": "SubagentStop", "hook": {
            "session_id": "s", "turn_id": "t", "cwd": "/w", "agent_id": "a"
        }});
        let (event, cwd, input) =
            hook_request(Some(&arguments), Some("a".to_owned())).expect("hook call");
        assert_eq!(event, "SubagentStop");
        assert_eq!(cwd, PathBuf::from("/w"));
        assert_eq!(
            input,
            json!({"session_id": "s", "turn_id": "t", "cwd": "/w", "agent_id": "a", "thread_id": "a"})
        );
        for rejected in [
            json!({"event": "SessionStart", "hook": {"session_id": "s", "cwd": "/w"}}),
            json!({"event": "SubagentStop", "hook": {"session_id": "s", "cwd": "/w"}}),
            json!({"event": "SubagentStop", "hook": {
                "session_id": "s", "turn_id": "t", "cwd": "/w", "agent_id": "a", "extra": 1
            }}),
            json!({"event": "SubagentStop"}),
        ] {
            assert!(hook_request(Some(&rejected), None).is_err(), "{rejected}");
        }
    }

    #[test]
    fn the_server_lists_no_tools_and_answers_hooks_as_text() {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
            .block_on(async {
                let requests = [
                    json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{
                        "protocolVersion":"2025-06-18","capabilities":{},
                        "clientInfo":{"name":"codex","version":"0"}
                    }}),
                    json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
                    json!({"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}),
                    // A non-filesystem tool is answered without the service.
                    json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{
                        "name":"hook",
                        "arguments":{"event":"PreToolUse","hook":{
                            "session_id":"s","turn_id":"t","cwd":"/w","tool_name":"web.run",
                            "tool_input":{},"tool_use_id":"u"
                        }},
                        "_meta":{"threadId":"s"}
                    }}),
                    json!({"jsonrpc":"2.0","id":4,"method":"tools/call","params":{
                        "name":"other","arguments":{}
                    }}),
                    json!({"jsonrpc":"2.0","id":5,"method":"resources/list"}),
                ];
                let mut input = Vec::new();
                for request in requests {
                    input.extend(serde_json::to_vec(&request).expect("request"));
                    input.push(b'\n');
                }
                let (client, server) = tokio::io::duplex(64 * 1024);
                serve_stream(input.as_slice(), server).await.expect("serve");
                let mut output = String::new();
                tokio::io::BufReader::new(client)
                    .read_to_string(&mut output)
                    .await
                    .expect("responses");
                let mut responses = output
                    .lines()
                    .map(|line| serde_json::from_str::<Value>(line).expect("response"))
                    .collect::<Vec<_>>();
                responses.sort_by_key(|response| response["id"].as_i64());
                assert_eq!(responses.len(), 5);
                assert_eq!(responses[0]["result"]["protocolVersion"], "2025-06-18");
                assert_eq!(responses[0]["result"]["serverInfo"]["name"], SERVER);
                assert_eq!(responses[1]["result"]["tools"], json!([]));
                assert_eq!(
                    responses[2]["result"],
                    json!({"content":[{"type":"text","text":"{}"}]})
                );
                assert_eq!(responses[3]["error"]["code"], -32602);
                assert_eq!(responses[4]["error"]["code"], -32601);
            });
    }
}
