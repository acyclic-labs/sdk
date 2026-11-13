//! Workspace operand credit and the shell's retained-log presentation.
//! General tool output uses the shared lossless spill envelope.
use crate::{Error, Result, runtime::ToolContext, tool::ToolResult};
use serde_json::{Map, Value};

/// The call's scope limits that bound it: rendered bytes and path bytes.
pub(super) fn limits(context: &ToolContext) -> (Option<u64>, Option<std::num::NonZeroUsize>) {
    let limits = context.effective_scope().limits();
    (limits.render_bytes, limits.path_bytes)
}

/// Refuses an operand the model must correct: a command longer than one
/// rendered part, or a shell `cwd` longer than a path.
pub(super) fn operands(
    name: &str,
    arguments: &Value,
    (render, path): (Option<u64>, Option<std::num::NonZeroUsize>),
) -> Result<()> {
    let text = |field: &str| arguments.get(field).and_then(Value::as_str).unwrap_or("");
    if let Some(render) = render
        && text("command").len() as u64 > render
    {
        return Err(Error::Invalid(format!(
            "{name} command exceeds its {render}-byte bound"
        )));
    }
    if let Some(path) = path
        && text("cwd").len() > path.get()
    {
        return Err(Error::Invalid(format!(
            "{name} cwd exceeds its {path}-byte bound"
        )));
    }
    Ok(())
}

/// The shell's bounded view of its separately retained complete process log.
/// Other results proceed intact to the shared model projection and spill path.
pub(super) fn result(
    name: &str,
    result: ToolResult,
    maximum: impl Into<Option<u64>>,
) -> Result<ToolResult> {
    if name == super::shell::PLATFORM.name {
        // A prepared window carries original source coordinates. Later credit
        // reductions use the general lossless projection spill, never a second
        // interpretation of already shortened stdout/stderr fields.
        let maximum = crate::tool::output::view_bytes(maximum.into());
        if let Value::Object(fields) = result.value {
            if fields.get("text").and_then(Value::as_str).is_some() {
                return Ok(ToolResult {
                    value: Value::Object(fields),
                });
            }
            return shell(fields, usize::try_from(maximum).unwrap_or(usize::MAX))
                .map(|value| ToolResult { value });
        }
        return Err(Error::Invalid(
            "shell result is not an object; recovery: bind the shared shell executor".into(),
        ));
    }
    Ok(result)
}

fn shell(mut fields: Map<String, Value>, maximum: usize) -> Result<Value> {
    if fields
        .get("omitted_bytes")
        .and_then(Value::as_u64)
        .unwrap_or_default()
        > 0
    {
        return Err(Error::Invalid(
            "truncated shell output has no source window; bind the shared OutputWindow collector"
                .into(),
        ));
    }
    let mut take = |field: &str| match fields.get_mut(field) {
        Some(Value::String(text)) => std::mem::take(text),
        _ => String::new(),
    };
    let (stdout, stderr) = (take("stdout"), take("stderr"));
    let mut window = crate::tool::OutputWindow::new(stdout.len().saturating_add(stderr.len()));
    window
        .observe(stdout.as_bytes(), false)
        .map_err(Error::process_io)?;
    window
        .observe(stderr.as_bytes(), true)
        .map_err(Error::process_io)?;
    window
        .render(
            ToolResult {
                value: Value::Object(fields),
            },
            maximum as u64,
        )
        .map(|result| result.value)
}

pub(crate) fn shell_window(
    result: ToolResult,
    bytes: &[u8],
    prefix: usize,
    total: u64,
    maximum: u64,
) -> Result<(ToolResult, std::ops::Range<usize>)> {
    let Value::Object(mut fields) = result.value else {
        return Err(Error::Invalid("shell result is not an object".into()));
    };
    let maximum =
        usize::try_from(crate::tool::output::view_bytes(Some(maximum))).unwrap_or(usize::MAX);
    if total == bytes.len() as u64 {
        fields.insert("omitted_bytes".into(), 0.into());
        fields.insert(
            "omitted_range".into(),
            serde_json::json!({"start":total,"end":total}),
        );
        let text = super::shell::window_presentation(&fields, &String::from_utf8_lossy(bytes), "");
        if text_projection_len(&text) <= maximum {
            fields.insert("text".into(), text.into());
            fields.insert("truncated".into(), false.into());
            return Ok((
                ToolResult {
                    value: Value::Object(fields),
                },
                bytes.len()..bytes.len(),
            ));
        }
    }
    // Reserve the largest omission counter up front; shortening output cannot
    // grow its notice past this allowance. The result is bounded by its actual
    // text projection rather than a second JSON rendering of canonical fields.
    fields.insert("omitted_bytes".into(), Value::from(total));
    fields.insert(
        "omitted_range".into(),
        serde_json::json!({"start":total,"end":total}),
    );
    let base = super::shell::window_presentation(&fields, "", "");
    let overhead = text_projection_len(&base);
    let budget = maximum
        .checked_sub(overhead)
        .ok_or_else(|| bound_too_small(super::shell::PLATFORM.name))?;
    let selected = crate::tool::output::select_window(bytes, prefix, total, budget);
    let start = selected.start as u64;
    let end = total.saturating_sub((bytes.len() - selected.end) as u64);
    fields.insert("omitted_bytes".into(), Value::from(end - start));
    fields.insert(
        "omitted_range".into(),
        serde_json::json!({"start":start,"end":end}),
    );
    fields.insert("truncated".into(), Value::Bool(start != end));
    let head = String::from_utf8_lossy(&bytes[..selected.start]);
    let tail = String::from_utf8_lossy(&bytes[selected.end..]);
    let text = super::shell::window_presentation(&fields, &head, &tail);
    fields.insert("text".into(), Value::String(text));
    Ok((
        ToolResult {
            value: Value::Object(fields),
        },
        selected,
    ))
}

fn text_projection_len(text: &str) -> usize {
    br#"{"kind":"parts","parts":[{"kind":"text","text":""}]}"#.len() + escaped_len(text)
}

fn bound_too_small(name: &str) -> Error {
    Error::Invalid(format!(
        "{name} result bound cannot hold a truncated result"
    ))
}

#[cfg(test)]
fn encoded(value: &Value) -> Result<String> {
    serde_json::to_string(value).map_err(|error| Error::Invalid(error.to_string()))
}

#[cfg(test)]
fn encoded_len(value: &Value) -> Result<usize> {
    encoded(value).map(|text| text.len())
}

/// Bytes `character` takes inside an encoded JSON string.
pub(crate) fn escaped(character: char) -> usize {
    match character {
        '"' | '\\' | '\n' | '\r' | '\t' | '\u{8}' | '\u{c}' => 2,
        character if (character as u32) < 0x20 => 6,
        character => character.len_utf8(),
    }
}

pub(crate) fn escaped_len(text: &str) -> usize {
    text.chars().map(escaped).sum()
}

/// The longest prefix of `text` whose encoded string body fits `budget`.
pub(crate) fn prefix(text: &str, budget: usize) -> &str {
    let mut used = 0;
    for (index, character) in text.char_indices() {
        used += escaped(character);
        if used > budget {
            return &text[..index];
        }
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const BOUND: u64 = 256;

    /// Bytes the model sees: the result inside its full JSON projection.
    fn projected_len(result: &ToolResult) -> u64 {
        if let Some(text) = result
            .value
            .get("text")
            .filter(|_| result.value.get("exit_code").is_some())
        {
            encoded_len(&json!({"kind":"parts", "parts":[{"kind":"text", "text":text}]})).unwrap()
                as u64
        } else {
            encoded_len(&json!({"kind": "json", "value": result.value})).unwrap() as u64
        }
    }

    // ASCII output that would exactly fill the bound on its own still fits
    // once the projection wraps it.
    #[test]
    fn a_result_filling_the_bound_fits_its_projection() {
        for maximum in [BOUND, crate::tool::output::VIEW_BYTES * 2] {
            let flooded = ToolResult {
                value: json!({"success": true, "exit_code": 0, "stop": null,
                    "stdout": "o".repeat(32_768), "stderr": "", "spill":"shell-output.log"}),
            };
            let bounded = result(super::super::shell::PLATFORM.name, flooded, maximum).unwrap();
            let window = maximum.min(crate::tool::output::VIEW_BYTES);
            assert!(
                projected_len(&bounded) <= window,
                "{}",
                projected_len(&bounded)
            );
            assert!(projected_len(&bounded) > window - 24);
        }
    }

    // A shell that floods both streams still returns its exit status, within
    // the bound, marked truncated.
    #[test]
    fn flooded_shell_output_is_bounded_and_keeps_its_status() {
        let flooded = ToolResult {
            value: json!({
                "success": false, "exit_code": 3, "stop": null,
                "stdout": "\"o\n".repeat(4096), "stderr": "é".repeat(4096),
            }),
        };
        let bounded = result(super::super::shell::PLATFORM.name, flooded, BOUND).unwrap();
        assert!(projected_len(&bounded) <= BOUND);
        assert_eq!(bounded.value["exit_code"], 3);
        assert_eq!(bounded.value["truncated"], true);
        assert!(!bounded.value["stdout"].as_str().unwrap().is_empty());
        assert!(!bounded.value["stderr"].as_str().unwrap().is_empty());
    }

    // Operands are rejected before an effect, independently of output spill.
    #[test]
    fn oversized_operands_are_rejected() {
        assert!(
            operands(
                "fs",
                &json!({"command":"x".repeat(257)}),
                (Some(BOUND), std::num::NonZeroUsize::new(8))
            )
            .is_err()
        );
        assert!(
            operands(
                "shell",
                &json!({"command":"ls","cwd":"d".repeat(9)}),
                (Some(BOUND), std::num::NonZeroUsize::new(8))
            )
            .is_err()
        );
        assert!(
            operands(
                "shell",
                &json!({"command":"ls","cwd":"d".repeat(9)}),
                (Some(BOUND), None)
            )
            .is_ok()
        );
    }
}
