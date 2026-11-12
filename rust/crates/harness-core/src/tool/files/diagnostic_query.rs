//! Immutable host file queries over published agent state; ordinary read
//! arguments and byte continuations remain the model-visible interface.
use super::*;

/// A bounded projection of an immutable diagnostic file query.
#[derive(Serialize, Deserialize, schemars::JsonSchema, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticReadResult {
    /// Immutable query path; its digest never retargets after child-name reuse.
    pub path: String,
    /// Original failed agent, complete digest and total byte length.
    pub diagnostic: crate::agent::Diagnostic,
    /// Exact original UTF-8 text selected for this page.
    pub text: String,
    /// Original byte cursor and complete length.
    pub page: ReadPage,
}

pub(super) async fn read(
    context: &ToolContext,
    input: ReadFileInput,
    target: (crate::agent::AgentPath, [u8; 32]),
    work: acyclic_fs::NativeWork<'_>,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<(ToolResult, acyclic_fs::NativeCustody)> {
    validate_input(&input, work)?;
    let observer = context
        .agent_children()
        .ok_or_else(|| executor::rejection(work, "agent file queries are not bound"))?;
    let start = input.range.map_or(0, |r| r.start);
    let maximum = crate::tool::output::view_bytes(context.effective_scope().limits().render_bytes);
    let requested = input
        .range
        .map_or(u64::MAX, |r| r.end - r.start)
        .min(maximum);
    let (diagnostic, page, hold) = observer
        .diagnostic(&target.0, target.1, start, requested, io)
        .await?;
    // Retain original read backing until the one response encoding finishes.
    let mut result = DiagnosticReadResult {
        path: input.path,
        diagnostic,
        text: page.text,
        page: ReadPage {
            start: page.start,
            end: page.end,
            next: (page.end < page.total).then_some(page.end),
            total: page.total,
            eof: page.end == page.total,
            offset: 0,
        },
    };
    #[derive(Serialize)]
    struct Empty<'a> {
        path: &'a str,
        diagnostic: &'a crate::agent::Diagnostic,
        text: &'a str,
        page: ReadPage,
    }
    let overhead = crate::executor::with_history_io(io, |history| {
        crate::executor::json::json_byte_length(
            &Empty {
                path: &result.path,
                diagnostic: &result.diagnostic,
                text: "",
                page: ReadPage {
                    start: u64::MAX,
                    end: u64::MAX,
                    next: Some(u64::MAX),
                    total: u64::MAX,
                    eof: false,
                    offset: u64::MAX,
                },
            },
            None,
            history,
        )
    })? + schema::json_envelope_bytes()? as u64;
    let budget = maximum
        .checked_sub(overhead)
        .filter(|n| *n >= 6)
        .ok_or_else(|| {
            executor::rejection(work, "render credit cannot hold a diagnostic file page")
        })?;
    let shown = crate::tools::bounds::prefix(&result.text, budget as usize).len();
    if shown == 0 && !result.text.is_empty() {
        return Err(executor::rejection(
            work,
            "render credit cannot hold the next diagnostic character",
        ));
    }
    result.text.truncate(shown);
    result.page.end = result.page.start + shown as u64;
    result.page.next = (result.page.end < result.page.total).then_some(result.page.end);
    result.page.eof = result.page.end == result.page.total;
    let encoded = value(&result, u64::MAX, work)?;
    drop(result);
    drop(hold);
    Ok((ToolResult { value: encoded.0 }, encoded.1))
}

fn validate_input(input: &ReadFileInput, work: acyclic_fs::NativeWork<'_>) -> Result<()> {
    if input.wait
        || input.version.is_some()
        || !matches!(input.selection, None | Some(ReadSelection::Text))
        || input.range.is_some_and(|range| range.end < range.start)
    {
        return Err(executor::rejection(
            work,
            "diagnostic file queries use text byte ranges and their immutable path",
        ));
    }
    Ok(())
}

pub(super) fn project(
    input: &ReadFileInput,
    target: (crate::agent::AgentPath, [u8; 32]),
    result: &ToolResult,
    work: acyclic_fs::NativeWork<'_>,
) -> Result<(Value, acyclic_fs::NativeCustody)> {
    validate_input(input, work)?;
    let (result, _hold) = decode::<DiagnosticReadResult>(&result.value, work)?;
    let page = &result.page;
    if result.path != input.path
        || result.diagnostic.agent != target.0
        || result.diagnostic.digest != target.1
        || result.diagnostic.bytes != page.total
        || page.start != input.range.map_or(0, |range| range.start).min(page.total)
        || page.end.checked_sub(page.start) != Some(result.text.len() as u64)
        || page.end > page.total
        || input
            .range
            .is_some_and(|range| page.end > range.end.min(page.total))
        || page.next != (page.end < page.total).then_some(page.end)
        || page.eof != (page.end == page.total)
        || page.offset != 0
    {
        return Err(executor::rejection(
            work,
            "diagnostic page differs from its immutable query or source interval",
        ));
    }
    schema::project_json(&result, work)
}
