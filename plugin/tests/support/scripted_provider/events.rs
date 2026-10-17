use super::{CLAUDE_FAILURE_MARKER, CLAUDE_SUCCESS_MARKER, CODEX_WORKFLOW_MARKER};
use serde_json::{Value, json};

pub(super) fn claude_child_tool_events(
    target: Option<&String>,
    offered_tools: &[String],
) -> Option<String> {
    let failed = target.is_none();
    let target = target.map_or("", String::as_str);
    let tool = if failed {
        offered_tools
            .iter()
            .find(|tool| matches!(tool.as_str(), "Bash" | "PowerShell"))?
    } else {
        offered_tools.iter().find(|tool| {
            matches!(
                tool.as_str(),
                "mcp__acyclic__bash" | "mcp__plugin_acyclic_acyclic__bash"
            )
        })?
    };
    let command = if failed {
        "exit 7".into()
    } else {
        workflow_command(target)
    };
    let input = json!({"command":command});
    Some(anthropic_named_tool_events_with_prefix(
        if failed {
            "failed_child"
        } else {
            "success_child"
        },
        tool,
        &input.to_string(),
    ))
}

pub(super) fn anthropic_lifecycle_events(root_target: &str) -> String {
    anthropic_named_tool_sequence_with_prefix(
        "lifecycle",
        [
            (
                "Write",
                json!({"file_path":root_target,"content":"qualified"}).to_string(),
            ),
            (
                "Agent",
                json!({
                    "description":"deterministic child isolation",
                    "prompt":format!("{CLAUDE_SUCCESS_MARKER}: write the requested relative file"),
                    "subagent_type":"general-purpose"
                })
                .to_string(),
            ),
        ],
    )
}

pub(super) fn anthropic_failed_agent_events() -> String {
    anthropic_named_tool_events_with_prefix(
        "failed_agent",
        "Agent",
        &json!({
            "description":"deterministic failed child",
            "prompt":format!("{CLAUDE_FAILURE_MARKER}: run the requested failing command"),
            "subagent_type":"general-purpose"
        })
        .to_string(),
    )
}

fn sse(events: impl IntoIterator<Item = Value>) -> String {
    events
        .into_iter()
        .map(|event| {
            let kind = event
                .get("type")
                .and_then(Value::as_str)
                .expect("scripted event type");
            format!("event: {kind}\ndata: {event}\n\n")
        })
        .collect()
}

pub(super) fn responses_root_tool_events(target: &str) -> String {
    let target = target.replace('\\', "/");
    assert!(
        !target.contains(['\r', '\n']),
        "scripted target must remain on one line"
    );
    let item = json!({
        "id":"ctc_1",
        "type":"custom_tool_call",
        "call_id":"call_1",
        "name":"exec",
        "input":format!(
            "const result = await tools.apply_patch({}); text(result);",
            serde_json::to_string(&format!(
                "*** Begin Patch\n*** Add File: {target}\n+qualified\n*** End Patch"
            ))
            .expect("scripted patch")
        ),
        "status":"completed"
    });
    sse([
        json!({"type":"response.created","response":{"id":"resp_tool","object":"response","status":"in_progress","model":"gpt-5.6-sol","output":[]}}),
        json!({"type":"response.output_item.done","output_index":0,"item":item}),
        json!({"type":"response.completed","response":{"id":"resp_tool","object":"response","status":"completed","model":"gpt-5.6-sol","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}),
    ])
}

pub(super) fn responses_spawn_events() -> String {
    let item = json!({
        "id":"fc_codex_spawn",
        "type":"function_call",
        "call_id":"call_codex_spawn",
        "name":"spawn_agent",
        "namespace":"collaboration",
        "arguments":serde_json::to_string(&json!({
            "task_name":"overlay_workflow",
            "message":format!("{CODEX_WORKFLOW_MARKER}: compile and run the supplied workflow")
        })).expect("spawn arguments"),
        "status":"completed"
    });
    responses_item_events("resp_spawn", &item)
}

pub(super) fn responses_search_events() -> String {
    responses_item_events(
        "resp_search",
        &json!({
            "id":"tsc_codex_shell","type":"tool_search_call",
            "call_id":"call_codex_shell","execution":"client",
            "arguments":{"query":"acyclic bash command","limit":1},"status":"completed"
        }),
    )
}

pub(super) fn codex_shell(request: &Value) -> Option<(String, Option<String>)> {
    fn find(tools: &[Value]) -> Option<(String, Option<String>)> {
        tools.iter().find_map(|tool| {
            let name = tool.get("name")?.as_str()?;
            if matches!(
                tool.get("type").and_then(Value::as_str),
                None | Some("namespace")
            ) && name.contains("acyclic")
            {
                tool.get("tools")?.as_array()?.iter().find_map(|nested| {
                    (nested.get("name")?.as_str()? == "bash")
                        .then(|| ("bash".into(), Some(name.into())))
                })
            } else {
                (name.contains("acyclic") && name.ends_with("__bash")).then(|| (name.into(), None))
            }
        })
    }
    request
        .get("tools")
        .and_then(Value::as_array)
        .and_then(|tools| find(tools))
        .or_else(|| {
            request
                .get("input")?
                .as_array()?
                .iter()
                .rev()
                .find_map(|item| {
                    (item.get("type")?.as_str()? == "tool_search_output")
                        .then(|| find(item.get("tools")?.as_array()?))?
                })
        })
}

pub(super) fn responses_workflow_tool_events(
    target: &str,
    tool: &(String, Option<String>),
) -> String {
    assert!(
        !target.contains(['\r', '\n', '\'', '"']),
        "scripted workflow target must be a simple relative path"
    );
    let command = workflow_command(target);
    let item = json!({
        "id":"fc_codex_workflow","type":"function_call",
        "call_id":"call_codex_workflow","name":tool.0,"namespace":tool.1,
        "arguments":json!({"command":command}).to_string(),"status":"completed"
    });
    responses_item_events("resp_workflow", &item)
}

fn workflow_command(target: &str) -> String {
    let suffix = if cfg!(windows) { ".exe" } else { "" };
    format!(
        "rustc --edition 2021 acyclic-workflow.rs -o acyclic-workflow-bin{suffix} && ./acyclic-workflow-bin{suffix} '{}'",
        target.replace('\'', "'\"'\"'")
    )
}

pub(super) fn responses_wait_events() -> String {
    responses_item_events(
        "resp_wait",
        &json!({
            "id":"fc_codex_wait","type":"function_call","call_id":"call_codex_wait",
            "name":"wait_agent","namespace":"collaboration",
            "arguments":json!({"timeout_ms":10000}).to_string(),"status":"completed"
        }),
    )
}

fn responses_item_events(response_id: &str, item: &Value) -> String {
    sse([
        json!({"type":"response.created","response":{"id":response_id,"object":"response","status":"in_progress","model":"gpt-5.6-sol","output":[]}}),
        json!({"type":"response.output_item.done","output_index":0,"item":item}),
        json!({"type":"response.completed","response":{"id":response_id,"object":"response","status":"completed","model":"gpt-5.6-sol","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}),
    ])
}

pub(super) fn responses_text_events() -> String {
    sse([
        json!({"type":"response.created","response":{"id":"resp_done","object":"response","status":"in_progress","model":"gpt-5.6-sol","output":[]}}),
        json!({"type":"response.output_item.done","output_index":0,"item":{"id":"msg_1","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"qualified","annotations":[]}]}}),
        json!({"type":"response.completed","response":{"id":"resp_done","object":"response","status":"completed","model":"gpt-5.6-sol","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}),
    ])
}

fn anthropic_named_tool_events_with_prefix(prefix: &str, tool: &str, input: &str) -> String {
    anthropic_named_tool_sequence_with_prefix(prefix, [(tool, input.to_owned())])
}

fn anthropic_named_tool_sequence_with_prefix<'a>(
    prefix: &str,
    tools: impl IntoIterator<Item = (&'a str, String)>,
) -> String {
    let mut events = vec![
        json!({"type":"message_start","message":{"id":"msg_tool","type":"message","role":"assistant","model":"claude-sonnet-4-5","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":1}}}),
    ];
    for (index, (tool, input)) in tools.into_iter().enumerate() {
        events.extend([
            json!({"type":"content_block_start","index":index,"content_block":{"type":"tool_use","id":format!("toolu_{prefix}_{}", index + 1),"name":tool,"input":{}}}),
            json!({"type":"content_block_delta","index":index,"delta":{"type":"input_json_delta","partial_json":input}}),
            json!({"type":"content_block_stop","index":index}),
        ]);
    }
    events.extend([
        json!({"type":"message_delta","delta":{"stop_reason":"tool_use","stop_sequence":null},"usage":{"output_tokens":1}}),
        json!({"type":"message_stop"}),
    ]);
    sse(events)
}

pub(super) fn anthropic_text_events() -> String {
    sse([
        json!({"type":"message_start","message":{"id":"msg_done","type":"message","role":"assistant","model":"claude-sonnet-4-5","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":1}}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"qualified"}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":1}}),
        json!({"type":"message_stop"}),
    ])
}
