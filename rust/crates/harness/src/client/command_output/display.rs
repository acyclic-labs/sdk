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
    use proptest::prelude::*;
    use serde_json::{Value, json};
    use std::borrow::Cow;

    /// The checked property: a process result shows every nonempty stream and,
    /// when it failed, its exit status.
    fn assert_shows_process(
        display: impl Fn(&Value) -> Option<Cow<'_, str>>,
        stdout: &str,
        stderr: &str,
        code: i64,
    ) {
        let ok = json!({
            "success": code == 0,
            "exit_code": code,
            "stdout": stdout,
            "stderr": stderr,
            "stop": null,
        });
        let shown = display(&ok).map(Cow::into_owned).unwrap_or_default();
        for stream in [stdout, stderr] {
            assert!(
                shown.contains(stream),
                "process output missing from the transcript: {shown:?}"
            );
        }
        if code != 0 {
            assert!(
                shown.contains(&format!("exit {code}")),
                "exit status missing from the transcript: {shown:?}"
            );
        }
    }

    proptest! {
        #[test]
        fn every_process_stream_and_failure_is_displayed(
            stdout in "[a-z]{0,12}\n?",
            stderr in "[A-Z]{0,12}\n?",
            code in -3i64..4,
        ) {
            assert_shows_process(display, &stdout, &stderr, code);
        }
    }

    /// The mapping before this change showed only an explicit `text` field.
    #[test]
    #[should_panic(expected = "process output missing from the transcript")]
    fn a_text_only_mapping_is_detected_negative_control() {
        assert_shows_process(
            |ok| ok.get("text").and_then(Value::as_str).map(Cow::Borrowed),
            "LPSHELL\n",
            "",
            0,
        );
    }

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
