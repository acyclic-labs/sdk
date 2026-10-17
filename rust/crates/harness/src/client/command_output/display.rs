//! The one mapping from a completed command's `ok` value to transcript text.
use std::borrow::Cow;

/// Text a completed command shows in the transcript: an explicit `text`, or a
/// process envelope's stdout then stderr, with a nonzero exit status line.
/// Other structured results (acknowledgements, selections) show nothing.
pub(in crate::client) fn display(ok: &serde_json::Value) -> Option<Cow<'_, str>> {
    if let Some(text) = ok.get("text").and_then(serde_json::Value::as_str) {
        return Some(Cow::Borrowed(text));
    }
    let stdout = ok.get("stdout").and_then(serde_json::Value::as_str);
    let stderr = ok.get("stderr").and_then(serde_json::Value::as_str);
    if stdout.is_none() && stderr.is_none() {
        return None;
    }
    let mut text = String::new();
    for stream in [stdout, stderr].into_iter().flatten() {
        if stream.is_empty() {
            continue;
        }
        text.push_str(stream);
        if !stream.ends_with('\n') {
            text.push('\n');
        }
    }
    match ok.get("exit_code") {
        Some(serde_json::Value::Number(code)) if code.as_i64() != Some(0) => {
            text.push_str(&format!("exit {code}\n"));
        }
        _ if ok.get("success") == Some(&serde_json::Value::Bool(false)) => {
            text.push_str("exit failed\n");
        }
        _ => {}
    }
    Some(Cow::Owned(text))
}

#[cfg(test)]
mod tests {
    use super::display;
    use serde_json::json;

    #[test]
    fn text_and_process_envelopes_display_and_acknowledgements_do_not() {
        assert_eq!(display(&json!({"text":"done"})).as_deref(), Some("done"));
        assert_eq!(
            display(&json!({"success":true,"exit_code":0,"stdout":"LPSHELL\n","stderr":""}))
                .as_deref(),
            Some("LPSHELL\n")
        );
        assert_eq!(
            display(&json!({"success":false,"exit_code":2,"stdout":"out","stderr":"err"}))
                .as_deref(),
            Some("out\nerr\nexit 2\n")
        );
        assert_eq!(
            display(
                &json!({"success":false,"exit_code":null,"stdout":"","stderr":"","stop":"timeout"})
            )
            .as_deref(),
            Some("exit failed\n")
        );
        assert_eq!(display(&json!({"task":"t"})), None);
        assert_eq!(display(&json!("plain")), None);
    }
}
