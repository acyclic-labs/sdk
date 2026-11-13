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
        // The complete presentation, including its retained-log notice, fits
        // the shared window so it never needs a second output spill.
        let maximum = crate::tool::output::view_bytes(maximum.into());
        if let Value::Object(mut fields) = result.value {
            // Presentation belongs to the model projection, never beside the
            // canonical channels consumed by code-mode and result replay.
            if fields.contains_key("stdout") || fields.contains_key("stderr") {
                fields.remove("text");
            }
            let omitted = fields
                .get("omitted_bytes")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            fields.insert("omitted_bytes".into(), omitted.into());
            fields.insert("truncated".into(), (omitted > 0).into());
            if text_projection_len(super::shell::presentation(&fields)) as u64 <= maximum {
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
    // grow its notice past this allowance. The result is bounded by its actual
    // text projection rather than a second JSON rendering of canonical fields.
    fields.insert("omitted_bytes".into(), Value::from(u64::MAX));
    let base = super::shell::presentation(&fields);
    let overhead = text_projection_len(base);
    let budget = maximum
        .checked_sub(overhead)
        .ok_or_else(|| bound_too_small(super::shell::PLATFORM.name))?;
    let error = escaped_len(&stderr);
    // Output keeps what stderr leaves, and at least half the budget.
    let output = suffix(&stdout, budget.saturating_sub(error).max(budget / 2));
    let error = suffix(&stderr, budget - escaped_len(output));
    let omitted =
        omitted.saturating_add((stdout.len() - output.len() + stderr.len() - error.len()) as u64);
    let (output, error) = (output.to_owned(), error.to_owned());
    fields.insert("stdout".into(), Value::String(output));
    fields.insert("stderr".into(), Value::String(error));
    fields.insert("omitted_bytes".into(), Value::from(omitted));
    fields.insert("truncated".into(), Value::Bool(omitted > 0));
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

fn text_projection_len(text: impl std::fmt::Display) -> usize {
    struct EncodedBytes(usize);
    impl std::fmt::Write for EncodedBytes {
        fn write_str(&mut self, text: &str) -> std::fmt::Result {
            self.0 = self.0.saturating_add(escaped_len(text));
            Ok(())
        }
    }
    let mut bytes = EncodedBytes(br#"{"kind":"parts","parts":[{"kind":"text","text":""}]}"#.len());
    std::fmt::write(&mut bytes, format_args!("{text}")).expect("size writer accepts all text");
    bytes.0
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
        if result.value.get("exit_code").is_some() {
            let projection = super::super::shell::project_result(result, None).unwrap().0;
            encoded_len(&projection).unwrap() as u64
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

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn shell_channels_are_canonical_once_and_project_on_the_original_ledger() -> Result<()> {
        let raw = ToolResult {
            value: json!({
                "exit_code":7, "success":false, "stdout":"out 🦀\n\"quoted\"", "stderr":"err é",
                "stop":{"message":"interrupted"}, "spill":"volume:///output/original",
                "text":"out 🦀\n\"quoted\"err é",
            }),
        };
        let bounded = result(super::super::shell::PLATFORM.name, raw, 4096)?;
        assert!(bounded.value.get("text").is_none());
        assert_eq!(bounded.value["stdout"], "out 🦀\n\"quoted\"");
        assert_eq!(bounded.value["stderr"], "err é");
        assert_eq!(bounded.value["exit_code"], 7);
        assert_eq!(bounded.value["stop"]["message"], "interrupted");
        assert_eq!(bounded.value["omitted_bytes"], 0);
        assert_eq!(bounded.value["truncated"], false);
        assert_eq!(
            result(super::super::shell::PLATFORM.name, bounded.clone(), 4096)?,
            bounded
        );
        let encoded = serde_json::to_string(&bounded.value).unwrap();
        assert_eq!(encoded.matches("out 🦀").count(), 1);
        assert_eq!(encoded.matches("err é").count(), 1);
        let expected = super::super::shell::project_result(&bounded, None)?.0;
        assert_eq!(
            expected,
            json!({"kind":"parts","parts":[{"kind":"text","text":"Exit code: 7\nout 🦀\n\"quoted\"err é\ninterrupted"}]})
        );
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = work.live_allocation_bytes();
        let (native, custody) = super::super::shell::project_result(&bounded, Some(&work))?;
        assert_eq!(native, expected);
        drop(native);
        drop(custody);
        assert_eq!(work.live_allocation_bytes(), baseline);
        cancellation.cancel();
        assert!(super::super::shell::project_result(&bounded, Some(&work)).is_err());
        // A start acknowledgement has no captured channels to duplicate.
        // Refusals and cancellation metadata remain visible independently.
        for (value, text) in [
            (
                json!({"background":true,"text":"Started; read volume:///output/job/result.json","cancel":"volume:///output/job/cancel"}),
                "Started; read volume:///output/job/result.json",
            ),
            (
                json!({"error":"permission refused"}),
                "Tool failed: permission refused",
            ),
        ] {
            let bounded = result(
                super::super::shell::PLATFORM.name,
                ToolResult { value },
                4096,
            )?;
            assert_eq!(
                super::super::shell::project_result(&bounded, None)?.0["parts"][0]["text"],
                text
            );
        }
        Ok(())
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
