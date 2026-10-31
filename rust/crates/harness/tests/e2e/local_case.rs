include!("../support/schema/local.rs");

// Version arguments name this case's seeded immutable files. Bind before the
// provider runs; production receives the ordinary opaque version string.
pub(super) fn bind_seed_files(
    mut turns: Vec<ScriptedTurn>,
    references: &BTreeMap<String, Value>,
) -> Result<Vec<ScriptedTurn>, String> {
    use acyclic_harness::{model::ModelEvent, test_support::ScriptedEvent};
    for turn in &mut turns {
        if let ScriptedTurn::Events(events) = turn {
            for event in events {
                if let ScriptedEvent::Event(ModelEvent::ToolCall(call)) = event
                    && matches!(call.name.split('@').next(), Some("read" | "edit"))
                    && let Some(path) = call.arguments.get("version").and_then(Value::as_str)
                {
                    call.arguments["version"] = serde_json::to_string(
                        references
                            .get(path)
                            .ok_or_else(|| format!("unknown seed file: {path}"))?,
                    )
                    .map_err(|error| error.to_string())?
                    .into();
                }
            }
        }
    }
    Ok(turns)
}
