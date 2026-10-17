use super::{Line, Modifier, Span, Style};
use crate::task_volume::{Component, HeadFile};
use serde::Deserialize;
use serde_json::value::RawValue;
use std::borrow::Cow;

#[derive(Deserialize)]
struct Request<'a> {
    #[serde(borrow)]
    name: Cow<'a, str>,
}

#[derive(Default, Deserialize)]
#[cfg_attr(test, derive(Debug, PartialEq, serde::Serialize))]
struct Status {
    success: Option<bool>,
    exit_code: Option<i32>,
}

fn read<'a, T: Deserialize<'a>>(file: Option<&'a HeadFile>) -> Option<T> {
    let [bytes] = file?.segments.as_slice() else {
        return None;
    };
    serde_json::from_slice(bytes).ok()
}

#[derive(Deserialize)]
struct Process<'a> {
    #[serde(borrow)]
    stdout: Option<&'a RawValue>,
    #[serde(borrow)]
    stderr: Option<&'a RawValue>,
}

// Structured results remain visible for tools without output files. Process
// output is exclusively its stream files, including when a retained tail is empty.
pub(super) fn result_rows(result: Option<&HeadFile>) -> Vec<Line<'static>> {
    if read::<Process<'_>>(result)
        .is_some_and(|value| value.stdout.is_some() || value.stderr.is_some())
    {
        return Vec::new();
    }
    let Some(mut value) = read::<serde_json::Value>(result) else {
        return Vec::new();
    };
    if let Some(fields) = value.as_object_mut() {
        for name in ["success", "exit_code", "stop"] {
            fields.remove(name);
        }
        if fields.is_empty() {
            return Vec::new();
        }
    }
    if value.is_null() {
        return Vec::new();
    }
    let text = match value {
        serde_json::Value::String(text) => text,
        value => serde_json::to_string_pretty(&value).expect("JSON value"),
    };
    let mut parser = acyclic_markdown::Markdown::default();
    parser.push(&text.lines().take(20).collect::<Vec<_>>().join("\n"));
    parser.lines().iter().take(20).map(super::line).collect()
}

// The result contributes status only. Output has one source: the streamed
// stdout/stderr files, whose shared source-line budget is applied by TaskRows.
pub(super) fn heading(
    request: Option<&HeadFile>,
    result: Option<&HeadFile>,
    fallback: &Component,
) -> Line<'static> {
    let name = read::<Request<'_>>(request)
        .map(|request| request.name)
        .unwrap_or_else(|| Cow::Borrowed(fallback.as_str()));
    let mut spans = vec![Span::styled(
        name.escape_debug().to_string(),
        Style::default().add_modifier(Modifier::BOLD),
    )];
    if result.is_some() {
        let status = read::<Status>(result).unwrap_or_default();
        let state = match (status.success, status.exit_code) {
            (Some(false), _) | (_, Some(1..)) | (_, Some(..=-1)) => "failed",
            (Some(true), _) => "completed",
            _ => "finished",
        };
        spans.push(Span::raw(format!(" · {state}")));
        if let Some(code) = status.exit_code {
            spans.push(Span::raw(format!(" (exit {code})")));
        }
    }
    Line::from(spans)
}

#[cfg(test)]
mod tests;
