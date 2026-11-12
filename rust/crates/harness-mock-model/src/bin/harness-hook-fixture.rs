use std::fs::OpenOptions;
use std::io::{self, Write};

use serde_json::{Value, json};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let record = args.next().ok_or("missing hook record path")?;
    let action = args.next();
    let mut input: Value = serde_json::from_reader(io::stdin().lock())?;
    let pre_tool = input["hook_event_name"] == "PreToolUse";
    let response = match action.as_deref() {
        Some(decision @ ("ask" | "allow" | "deny")) if pre_tool => json!({
            "hookSpecificOutput": {
                "permissionDecision": decision,
                "permissionDecisionReason": "E2E approval policy"
            }
        }),
        Some("context") => json!({
            "hookSpecificOutput": { "additionalContext": "HOOK_E2E_CONTEXT" }
        }),
        _ => Value::Null,
    };
    input["fixture_action"] = json!(action);
    input["fixture_response"] = response.clone();
    let mut record = OpenOptions::new().create(true).append(true).open(record)?;
    serde_json::to_writer(&mut record, &input)?;
    record.write_all(b"\n")?;
    if !response.is_null() {
        let mut output = io::stdout().lock();
        serde_json::to_writer(&mut output, &response)?;
        output.write_all(b"\n")?;
    }
    if pre_tool && action.as_deref() == Some("fail") {
        io::stderr().write_all(b"E2E_HOOK_FAILURE\n")?;
        std::process::exit(1);
    }
    Ok(())
}
