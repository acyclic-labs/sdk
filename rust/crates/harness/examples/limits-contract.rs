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

        })
    );
    Ok(())
}
