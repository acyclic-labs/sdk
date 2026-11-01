#![allow(missing_docs, reason = "contract example binary, not public API")]

use acyclic_harness::conversation::{Limits, MAX_EXACT_JS_INTEGER, MAX_PORTABLE_COUNT};
use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let defaults = Limits::default();
    let maximum = Limits {
        file_bytes: Some(MAX_EXACT_JS_INTEGER),
        path_bytes: std::num::NonZeroUsize::new(MAX_PORTABLE_COUNT),
        attachments: std::num::NonZeroUsize::new(MAX_PORTABLE_COUNT),
        render_bytes: Some(MAX_EXACT_JS_INTEGER),
        model_steps: std::num::NonZeroUsize::new(MAX_PORTABLE_COUNT),
        model_events_per_step: std::num::NonZeroUsize::new(MAX_PORTABLE_COUNT),
        tool_calls_per_step: std::num::NonZeroUsize::new(MAX_PORTABLE_COUNT),
        context_messages: std::num::NonZeroUsize::new(MAX_PORTABLE_COUNT),
    };
    defaults.validate(&mut None)?;
    maximum.validate(&mut None)?;
    println!(
        "{}",
        json!({
            "limits": {
                "default": defaults,
                "maximum": maximum,
                "exact_js_integer": MAX_EXACT_JS_INTEGER,
            },
            "projection": {
                "default_max_resolved_bytes": serde_json::Value::Null,
                "default_max_manifest_bytes": MAX_EXACT_JS_INTEGER,
                "default_max_attachments": serde_json::Value::Null,
                "default_max_messages": serde_json::Value::Null,
                "default_max_render_bytes": serde_json::Value::Null,
                "max_projected_attachments": acyclic_harness::conversation::MAX_PORTABLE_COUNT,
                "max_json_bytes": MAX_EXACT_JS_INTEGER,
            },
        })
    );
    Ok(())
}
