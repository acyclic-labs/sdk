#![allow(missing_docs, reason = "contract example binary, not public API")]

use acyclic_harness::conversation::{
    Limits, MAX_EXACT_JS_INTEGER, MAX_LABEL_BYTES, MAX_PATH_BYTES, MAX_PORTABLE_COUNT,
};
use acyclic_harness::fork::{
    MAX_FORK_AGENTS, MAX_FORK_ATTACHMENT_MANIFEST_BYTES, MAX_FORK_INHERITED_BYTES,
    MAX_FORK_INHERITED_MESSAGES, MAX_FORK_REFERENCE_BYTES, MAX_FORK_REFERENCES, MAX_FORK_RESOURCES,
};
use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let defaults = Limits::default();
    let maximum = Limits {
        file_bytes: MAX_EXACT_JS_INTEGER,
        path_bytes: MAX_PATH_BYTES,
        attachments: MAX_PORTABLE_COUNT,
        render_bytes: MAX_EXACT_JS_INTEGER,
        model_steps: MAX_PORTABLE_COUNT,
        model_events_per_step: MAX_PORTABLE_COUNT,
        tool_calls_per_step: MAX_PORTABLE_COUNT,
        context_messages: MAX_PORTABLE_COUNT,
    };
    defaults.validate(&mut None)?;
    maximum.validate(&mut None)?;
    let session = acyclic_harness::scheduler::SessionLimits {
        active_tasks: 1,
        total_tasks: 1,
        depth: 0,
        model_steps: 1,
    };
    session.validate(&mut None)?;
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
            "session": session,
            "fork": {
                "agents": MAX_FORK_AGENTS,
                "resources": MAX_FORK_RESOURCES,
                "references": MAX_FORK_REFERENCES,
                "attachment_manifest_bytes": MAX_FORK_ATTACHMENT_MANIFEST_BYTES.to_string(),
                "inherited_bytes": MAX_FORK_INHERITED_BYTES.to_string(),
                "reference_bytes": MAX_FORK_REFERENCE_BYTES.to_string(),
                "inherited_messages": MAX_FORK_INHERITED_MESSAGES.to_string(),
            },
            "projection": {
                "default_max_resolved_bytes": MAX_EXACT_JS_INTEGER,
                "default_max_manifest_bytes": MAX_EXACT_JS_INTEGER,
                "default_max_attachments": MAX_PORTABLE_COUNT,
                "default_max_messages": MAX_PORTABLE_COUNT,
                "default_max_render_bytes": MAX_EXACT_JS_INTEGER,
                "max_projected_attachments": acyclic_harness::conversation::MAX_PORTABLE_COUNT,
                "max_json_bytes": MAX_EXACT_JS_INTEGER,
            },
        })
    );
    Ok(())
}
