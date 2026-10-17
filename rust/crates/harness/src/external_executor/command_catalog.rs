//! Native catalog entries for the existing stock task-command contracts.
use crate::tool::ToolDefinition;
use serde_json::json;

pub(super) fn definitions() -> Vec<ToolDefinition> {
    let operation = json!({"type":"string","format":"uuid"});
    [
        ("acyclic_spawn", "Admit a registered task, or launch a native child with id and prompt", json!({
            "name":{"type":"string"},"version":{"type":"string"},"input":{}
        }), vec!["name", "version", "input"]),
        ("acyclic_send", "Send a pinned readable file to an admitted task", json!({
            "recipient":operation,"payload":{"type":"object"}
        }), vec!["recipient", "payload"]),
        ("acyclic_steer", "Send a pinned steering file to an admitted task's mailbox", json!({
            "recipient":operation,"payload":{"type":"object"}
        }), vec!["recipient", "payload"]),
        ("acyclic_receive", "Receive the first mailbox file after the acknowledged sequence", json!({
            "after":{"type":"integer","minimum":0}
        }), vec!["after"]),
        ("acyclic_wait", "Wait for an admitted direct child by its returned task ID", json!({
            "child":operation
        }), vec!["child"]),
        ("acyclic_cancel", "Cancel a direct child and its subtree under the caller's fence", json!({
            "child":operation
        }), vec!["child"]),
        ("acyclic_inspect", "Inspect retained task metadata and outcome without adopting its grants", json!({
            "task":operation
        }), vec!["task"]),
    ].into_iter().map(|(name, description, properties, required)| {
        let output = json!({});
        let mut input_schema = json!({"type":"object","properties":properties,"required":required,"additionalProperties":false});
        if name == "acyclic_spawn" {
            let native = json!({"type":"object","required":["id","prompt"],"properties":{
                "id":{"type":"string","minLength":1},"prompt":{"type":"string","minLength":1},
                "context":{"type":"string","enum":["scratch","native_cut"],"description":"Start fresh, or inherit the exact caller branch before this spawn invocation"},
                "executor":{"type":"object","description":"Native host launch binding; supplied by the host adapter or an explicit caller"}
            },"additionalProperties":false});
            let mut all = input_schema["properties"].clone();
            all.as_object_mut().unwrap().extend(native["properties"].as_object().unwrap().clone());
            input_schema = json!({"type":"object","properties":all,"oneOf":[input_schema,native],"additionalProperties":false});
        }
        ToolDefinition {
            name:name.into(), revision:"1".into(), description:description.into(),
            input_schema,
            output_schema:output.clone(), projection_schema:crate::tool::json_projection_schema(output),
        }
    }).collect()
}

pub(super) fn catalog() -> crate::Result<Vec<ToolDefinition>> {
    let mut tools = crate::tools::defaults()?.definitions(None)?;
    tools.extend(definitions());
    Ok(tools)
}
