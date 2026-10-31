//! The one bound on every host's `shell` and `fs` calls, applied by the shared
//! workspace executor so each backend inherits it: operands within the
//! scope's limits before dispatch, and a text-only result within the bytes
//! one model context part renders.
use crate::{Error, Result, runtime::ToolContext, tool::ToolResult};
use serde_json::{Map, Value};

/// The call's scope limits that bound it: rendered bytes and path bytes.
pub(super) fn limits(context: &ToolContext) -> (u64, usize) {
    let limits = context.effective_scope().limits();
    (limits.render_bytes, limits.path_bytes)
}

/// Checks the task's path admission. Command intent is retained in the
/// original invocation; presentation demand does not limit executable input.
pub(super) fn operands(arguments: &Value, path: usize) -> Result<()> {
    let text = |field: &str| arguments.get(field).and_then(Value::as_str).unwrap_or("");
    if text("cwd").len() > path {
        return Err(Error::Invalid(format!(
            "shell cwd exceeds its {path}-byte bound"
        )));
    }
    Ok(())
}

/// The result within `maximum` encoded JSON bytes once the model projection
/// wraps it. A shell result keeps its fields and shortens `stdout` then
/// `stderr`; any other result becomes the leading text of its own encoding.
/// Either way it is marked `truncated`.
pub(super) fn result(name: &str, result: ToolResult, maximum: u64) -> Result<ToolResult> {
    if name == super::shell::PLATFORM.name {
        if let Value::Object(fields) = result.value {
            if let Some(text) = fields.get("text").and_then(Value::as_str)
                && text_projection_len(text) as u64 <= maximum
            {
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
    let maximum = maximum
        .checked_sub(crate::tool::schema::json_envelope_bytes()? as u64)
        .ok_or_else(|| bound_too_small(name))?;
    let encoded = encoded(&result.value)?;
    if encoded.len() as u64 <= maximum {
        return Ok(result);
    }
    let maximum = usize::try_from(maximum).unwrap_or(usize::MAX);
    let value = {
        let mut fields = Map::new();
        fields.insert("truncated".into(), Value::Bool(true));
        fields.insert("text".into(), Value::String(String::new()));
        let budget = maximum
            .checked_sub(encoded_len(&Value::Object(fields.clone()))?)
            .ok_or_else(|| bound_too_small(name))?;
        fields.insert(
            "text".into(),
            Value::String(prefix(&encoded, budget).to_owned()),
        );
        Value::Object(fields)
    };
    Ok(ToolResult { value })
}

fn shell(mut fields: Map<String, Value>, maximum: usize) -> Result<Value> {
    let mut take = |field: &str| match fields.get_mut(field) {
        Some(Value::String(text)) => std::mem::take(text),
        _ => String::new(),
    };
    let (stdout, stderr) = (take("stdout"), take("stderr"));
    let omitted = fields
        .get("omitted_bytes")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    // Reserve the largest omission counter up front; shortening output cannot
    // grow its footer past this allowance. The result is bounded by its actual
    // text projection rather than a second JSON rendering of canonical fields.
    fields.insert("omitted_bytes".into(), Value::from(u64::MAX));
    let base = super::shell::presentation(&fields);
    let overhead = text_projection_len(&base);
    let budget = maximum
        .checked_sub(overhead)
        .ok_or_else(|| bound_too_small(super::shell::PLATFORM.name))?;
    let error = escaped_len(&stderr);
    // Output keeps what stderr leaves, and at least half the budget.
    let budget = budget.min(30_000);
    let output = suffix(&stdout, budget.saturating_sub(error).max(budget / 2));
    let error = suffix(&stderr, budget - escaped_len(output));
    let omitted =
        omitted.saturating_add((stdout.len() - output.len() + stderr.len() - error.len()) as u64);
    let (output, error) = (output.to_owned(), error.to_owned());
    fields.insert("stdout".into(), Value::String(output));
    fields.insert("stderr".into(), Value::String(error));
    fields.insert("omitted_bytes".into(), Value::from(omitted));
    fields.insert("truncated".into(), Value::Bool(omitted > 0));
    let text = super::shell::presentation(&fields);
    fields.insert("text".into(), Value::String(text));
    Ok(Value::Object(fields))
}

fn suffix(text: &str, budget: usize) -> &str {
    let mut used = 0;
    for (index, character) in text.char_indices().rev() {
        used += escaped(character);
        if used > budget {
            return &text[index + character.len_utf8()..];
        }
    }
    text
}

fn text_projection_len(text: &str) -> usize {
    br#"{"kind":"parts","parts":[{"kind":"text","text":""}]}"#.len() + escaped_len(text)
}

fn bound_too_small(name: &str) -> Error {
    Error::Invalid(format!(
        "{name} result bound cannot hold a truncated result"
    ))
}

fn encoded(value: &Value) -> Result<String> {
    serde_json::to_string(value).map_err(|error| Error::Invalid(error.to_string()))
}

fn encoded_len(value: &Value) -> Result<usize> {
    encoded(value).map(|text| text.len())
}

/// Bytes `character` takes inside an encoded JSON string.
fn escaped(character: char) -> usize {
    match character {
        '"' | '\\' | '\n' | '\r' | '\t' | '\u{8}' | '\u{c}' => 2,
        character if (character as u32) < 0x20 => 6,
        character => character.len_utf8(),
    }
}

fn escaped_len(text: &str) -> usize {
    text.chars().map(escaped).sum()
}

/// The longest prefix of `text` whose encoded string body fits `budget`.
fn prefix(text: &str, budget: usize) -> &str {
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
        let flooded = ToolResult {
            value: json!({"success": true, "exit_code": 0, "stop": null,
                "stdout": "o".repeat(4096), "stderr": ""}),
        };
        let bounded = result(super::super::shell::PLATFORM.name, flooded, BOUND).unwrap();
        assert!(
            projected_len(&bounded) <= BOUND,
            "{}",
            projected_len(&bounded)
        );
        assert!(projected_len(&bounded) > BOUND - 24);
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

    // An fs result too large to render becomes bounded text, never an
    // oversized or non-text value.
    #[test]
    fn oversized_fs_result_becomes_bounded_text() {
        let listing = ToolResult {
            value: json!({"Paths": (0..512).map(|n| format!("dir/\"{n}\".bin")).collect::<Vec<_>>()}),
        };
        let bounded = result("fs", listing, BOUND).unwrap();
        assert!(projected_len(&bounded) <= BOUND);
        assert_eq!(bounded.value["truncated"], true);
        assert!(
            bounded.value["text"]
                .as_str()
                .unwrap()
                .starts_with("{\"Paths\":[")
        );
        assert!(operands(&json!({"command": "ls", "cwd": "d".repeat(9)}), 8).is_err());
    }
}
