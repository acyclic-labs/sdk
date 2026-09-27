#![allow(missing_docs)]

use acyclic_harness::conversation::{
    Limits, MAX_EXACT_JS_INTEGER, MAX_LABEL_BYTES, MAX_LIMIT_ATTACHMENTS,
    MAX_LIMIT_CONTEXT_MESSAGES, MAX_LIMIT_FILE_BYTES, MAX_LIMIT_MODEL_EVENTS_PER_STEP,
    MAX_LIMIT_MODEL_STEPS, MAX_LIMIT_RENDER_BYTES, MAX_LIMIT_TOOL_CALLS_PER_STEP, MAX_PATH_BYTES,
};
use acyclic_harness::fork::{
    MAX_FORK_AGENTS, MAX_FORK_ATTACHMENT_MANIFEST_BYTES, MAX_FORK_INHERITED_BYTES,
    MAX_FORK_INHERITED_MESSAGES, MAX_FORK_REFERENCE_BYTES, MAX_FORK_REFERENCES, MAX_FORK_RESOURCES,
};
use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let defaults = Limits::default();
    let maximum = Limits {
        file_bytes: MAX_LIMIT_FILE_BYTES,
        path_bytes: MAX_PATH_BYTES,
        attachments: MAX_LIMIT_ATTACHMENTS,
        render_bytes: MAX_LIMIT_RENDER_BYTES,
        model_steps: MAX_LIMIT_MODEL_STEPS,
        model_events_per_step: MAX_LIMIT_MODEL_EVENTS_PER_STEP,
        tool_calls_per_step: MAX_LIMIT_TOOL_CALLS_PER_STEP,
        context_messages: MAX_LIMIT_CONTEXT_MESSAGES,
    };
    defaults.validate()?;
    maximum.validate()?;
    println!(
        "{}",
        json!({
            "limits": {
                "default": defaults,
                "maximum": maximum,
                "exact_js_integer": MAX_EXACT_JS_INTEGER,
                "max_path_bytes": MAX_PATH_BYTES,
                "max_label_bytes": MAX_LABEL_BYTES,
            },
            "fork": {
                "agents": MAX_FORK_AGENTS,
                "resources": MAX_FORK_RESOURCES,
                "references": MAX_FORK_REFERENCES,
                "attachment_manifest_bytes": MAX_FORK_ATTACHMENT_MANIFEST_BYTES,
                "inherited_bytes": MAX_FORK_INHERITED_BYTES,
                "reference_bytes": MAX_FORK_REFERENCE_BYTES,
                "inherited_messages": MAX_FORK_INHERITED_MESSAGES,
            },
        })
    );
    Ok(())
}
