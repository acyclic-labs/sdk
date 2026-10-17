use serde_json::{Value, json};

/// Native denial when a tool hook cannot obtain an authenticated actor answer.
/// Lifecycle errors remain visible; the process also exits unsuccessfully.
#[must_use]
pub fn hook_failure(host: &str, event: &str, reason: &str) -> Value {
    match (host, event) {
        ("pi", "tool_call") => json!({"block":true,"reason":reason}),
        ("codex" | "claude-code", "PreToolUse") => json!({
            "hookSpecificOutput": {
                "hookEventName":"PreToolUse", "permissionDecision":"deny",
                "permissionDecisionReason":reason
            }
        }),
        _ => json!({"systemMessage":reason}),
    }
}
