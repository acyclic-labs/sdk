//! Filesystem classes: path identity, fork/merge semantics, read paging,
//! omission visibility and the execution environment. Each world drives the
//! shipping filesystem, tool and environment seams; the exhaustive matrices
//! are model checks over those production functions.
use super::*;
use acyclic_harness::{
    AgentId, Capabilities, OperationId,
    conversation::Limits,
    filesystem::MemoryHarnessStorage,
    runtime::{Bindings, RuntimeScope, ToolContext},
    tool::{ToolInvocation, files::read},
};
use family::{Generator, Variant};
use serde_json::{Value, json};
use std::sync::Arc;

const SIZES: [&str; 3] = ["small", "multi_chunk", "above_stream_frame"];
const SELECTORS: [&str; 3] = ["byte_range", "line_selection", "none"];
const POSITIONS: [&str; 5] = ["start", "middle", "at_eof", "past_eof", "spanning_eof"];

/// One world per source size; each covers every selector and position.
pub(super) const READ_GENERATOR: Generator = Generator {
    class: "H4",
    seeds: every_read_selection_yields_a_page_or_terminal_eof::VALUES,
    variants: read_variants,
    world: read_world,
};

acyclic_test_support::cases! {
    module = every_read_selection_yields_a_page_or_terminal_eof,
    values: u64,
    runner = super::run,
    cases = [seed_0 => 0, seed_1 => 1, seed_2 => 2],
}

fn run(seed: u64) -> std::result::Result<(), String> {
    READ_GENERATOR.run(seed)
}

fn size(seed: u64) -> &'static str {
    SIZES
        .get(usize::try_from(seed % SIZES.len() as u64).unwrap_or(0))
        .copied()
        .unwrap_or("small")
}

fn read_variants(seed: u64) -> Vec<Variant> {
    let size = size(seed);
    let mut variants = Vec::new();
    for selector in SELECTORS {
        for position in POSITIONS {
            variants.push(Variant::from([
                ("selector", selector),
                ("position", position),
                ("size", size),
            ]));
        }
    }
    variants
}

const RENDER_BYTES: u64 = 40_000;

fn source(size: &str) -> String {
    let bytes = match size {
        "small" => 64,
        "multi_chunk" => 40_000,
        _ => acyclic_stream::MAX_RECORD_BYTES + 1,
    };
    let mut text = String::new();
    let mut index = 0;
    while text.len() < bytes {
        text.push_str(&format!("line {index:04} abcdefghij\n"));
        index += 1;
    }
    text.truncate(bytes);
    text
}

async fn task(
    storage: &MemoryHarnessStorage,
    render: u64,
) -> std::result::Result<Arc<acyclic_harness::runtime::TaskContext>, String> {
    let mut bindings = Bindings::local();
    bindings.scope = RuntimeScope::new(
        storage.owner_scope().capabilities().clone(),
        Limits {
            file_bytes: Some(1_000_000),
            render_bytes: Some(render),
            ..Limits::default()
        },
    )
    .map_err(oracle::failed)?;
    Ok(Arc::new(
        bindings
            .build()
            .map_err(oracle::failed)?
            .agent_context(&Capabilities::default(), storage.content_bindings())
            .map_err(oracle::failed)?,
    ))
}

async fn execute(
    task: &Arc<acyclic_harness::runtime::TaskContext>,
    name: &str,
    arguments: Value,
) -> std::result::Result<Value, String> {
    let invocation = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: name.into(),
        name: name.into(),
        arguments,
    };
    let context = ToolContext::new(
        Arc::clone(task),
        invocation.operation_id,
        &invocation.call_id,
    )
    .map_err(oracle::failed)?;
    read()
        .map_err(oracle::failed)?
        .executor
        .execute(invocation, Some(context))
        .await
        .map(|result| result.value)
        .map_err(oracle::failed)
}

fn page(value: &Value) -> std::result::Result<&Value, String> {
    value
        .get("page")
        .filter(|page| page.is_object())
        .ok_or_else(|| format!("read returned no page: {value}"))
}

fn text(value: &Value) -> std::result::Result<&str, String> {
    value
        .get("text")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("read returned no text: {value}"))
}

#[derive(Clone, Copy)]
struct Page {
    start: u64,
    end: u64,
    eof: bool,
    next: Option<u64>,
}

/// Check one page against the source bytes and, when the request fixes it, the
/// exact interval. A page that disagrees with the source is a failure even if
/// its own fields are internally consistent.
fn read_page(
    label: &str,
    value: &Value,
    source: &str,
    expected: Option<(u64, u64)>,
) -> std::result::Result<Page, String> {
    let page = page(value)?;
    let start = page
        .get("start")
        .and_then(Value::as_u64)
        .ok_or("page start")?;
    let end = page.get("end").and_then(Value::as_u64).ok_or("page end")?;
    let total = page
        .get("total")
        .and_then(Value::as_u64)
        .ok_or("page total")?;
    let eof = page.get("eof").and_then(Value::as_bool).ok_or("page eof")?;
    let next = page.get("next").and_then(Value::as_u64);
    let source_len = source.len() as u64;
    if total != source_len {
        return Err(format!("{label}: total {total} != source {source_len}"));
    }
    if start > end || end > total {
        return Err(format!("{label}: page interval {start}..{end} of {total}"));
    }
    if eof != (end == total) {
        return Err(format!("{label}: eof {eof} at {end} of {total}"));
    }
    if eof != next.is_none() {
        return Err(format!("{label}: next {next:?} with eof {eof}"));
    }
    if let Some((want_start, want_end)) = expected
        && (start, end) != (want_start, want_end)
    {
        return Err(format!(
            "{label}: page {start}..{end} != requested {want_start}..{want_end}"
        ));
    }
    let text = text(value)?;
    let want = source
        .get(
            usize::try_from(start).unwrap_or(usize::MAX)
                ..usize::try_from(end).unwrap_or(usize::MAX),
        )
        .ok_or_else(|| format!("{label}: page {start}..{end} outside the source"))?;
    if text != want {
        return Err(format!(
            "{label}: text differs from source {start}..{end}: {text:?} != {want:?}"
        ));
    }
    if !eof {
        if end <= start {
            return Err(format!("{label}: nonterminal page made no progress"));
        }
        if next != Some(end) {
            return Err(format!("{label}: next {next:?} != end {end}"));
        }
    }
    if text.starts_with("{\"file\"") || text.contains("\"descriptor\"") {
        return Err(format!(
            "{label}: read text is a machine envelope: {text:.80}"
        ));
    }
    Ok(Page {
        start,
        end,
        eof,
        next,
    })
}

fn line_offsets(source: &str) -> Vec<u64> {
    let mut offsets = vec![0];
    for (index, byte) in source.bytes().enumerate() {
        if byte == b'\n' {
            offsets.push(index as u64 + 1);
        }
    }
    offsets
}

fn byte_range(position: &str, total: u64) -> (u64, u64) {
    match position {
        "start" => (0, 32.min(total)),
        "middle" => (total / 2, (total / 2 + 32).min(total)),
        "at_eof" => (total, total),
        "past_eof" => (total + 10, total + 20),
        _ => (total.saturating_sub(10), total + 10),
    }
}

fn line_selection(position: &str, lines: usize) -> (u64, u64) {
    match position {
        "start" => (0, 2),
        "middle" => ((lines / 2) as u64, 2),
        "at_eof" => (lines as u64, 2),
        "past_eof" => (lines as u64 + 5, 2),
        _ => (lines.saturating_sub(1) as u64, 5),
    }
}

/// Follow the bare read's `next` cursor to EOF, checking every page against the
/// source. The first request is the selector under test; continuations use the
/// page cursor the model would use.
async fn none_pages(
    task: &Arc<acyclic_harness::runtime::TaskContext>,
    path: &str,
    source: &str,
) -> std::result::Result<Vec<Page>, String> {
    let total = source.len() as u64;
    let first = execute(task, "read", json!({"path": path})).await?;
    let first = read_page("none/start", &first, source, None)?;
    if first.start != 0 {
        return Err(format!("none/start: first page starts at {}", first.start));
    }
    let mut pages = vec![first];
    let mut cursor = first.next;
    while let Some(start) = cursor {
        let value = execute(
            task,
            "read",
            json!({"path": path, "range": {"start": start, "end": total}}),
        )
        .await?;
        let page = read_page(&format!("none/{}", pages.len()), &value, source, None)?;
        if page.start != start {
            return Err(format!(
                "none/{}: continuation starts at {} not {start}",
                pages.len(),
                page.start
            ));
        }
        cursor = page.next;
        pages.push(page);
    }
    Ok(pages)
}

fn read_world(seed: u64) -> std::result::Result<bool, String> {
    let size = size(seed);
    oracle::simulate(seed, move || {
        entry(async move {
            let storage = MemoryHarnessStorage::new(AgentId::new(), 1_000_000)
                .await
                .map_err(oracle::failed)?;
            let source = source(size);
            if size == "above_stream_frame" && source.len() <= acyclic_stream::MAX_RECORD_BYTES {
                return Err(format!(
                    "{size}: source {} does not exceed the stream frame {}",
                    source.len(),
                    acyclic_stream::MAX_RECORD_BYTES
                ));
            }
            storage
                .stage(
                    OperationId::new(),
                    "ui/turns/source.txt",
                    source.as_bytes(),
                    "text/plain",
                    "source.txt",
                )
                .await
                .map_err(oracle::failed)?;
            let task = task(&storage, RENDER_BYTES).await?;
            let total = source.len() as u64;
            let lines = line_offsets(&source);
            let path = "volume:///private/ui/turns/source.txt";
            for selector in SELECTORS {
                for position in POSITIONS {
                    let label = format!("{size}/{selector}/{position}");
                    match selector {
                        "byte_range" => {
                            let (start, end) = byte_range(position, total);
                            let value = execute(
                                &task,
                                "read",
                                json!({"path": path, "range": {"start": start, "end": end}}),
                            )
                            .await?;
                            read_page(
                                &label,
                                &value,
                                &source,
                                Some((start.min(total), end.min(total))),
                            )?;
                        }
                        "line_selection" => {
                            let (offset, limit) = line_selection(position, lines.len());
                            let want_start = lines
                                .get(usize::try_from(offset).unwrap_or(usize::MAX))
                                .copied();
                            let want_end = lines
                                .get(usize::try_from(offset + limit).unwrap_or(usize::MAX))
                                .copied();
                            let value = execute(
                                &task,
                                "read",
                                json!({"path": path, "selection": {"kind": "lines", "offset": offset, "limit": limit}}),
                            )
                            .await?;
                            read_page(
                                &label,
                                &value,
                                &source,
                                Some((want_start.unwrap_or(total), want_end.unwrap_or(total))),
                            )?;
                        }
                        _ => {
                            let pages = none_pages(&task, path, &source).await?;
                            let last = pages.last().ok_or_else(|| format!("{label}: no pages"))?;
                            match position {
                                "start" => {
                                    let first = pages
                                        .first()
                                        .ok_or_else(|| format!("{label}: no pages"))?;
                                    if first.start != 0 {
                                        return Err(format!(
                                            "{label}: first page starts at {}",
                                            first.start
                                        ));
                                    }
                                    if size != "small" && pages.len() < 2 {
                                        return Err(format!(
                                            "{label}: a {size} source did not need a continuation page"
                                        ));
                                    }
                                }
                                "middle" => {
                                    let middle = total / 2;
                                    if !pages
                                        .iter()
                                        .any(|page| page.start <= middle && middle < page.end)
                                    {
                                        return Err(format!(
                                            "{label}: no page covers the middle byte {middle}"
                                        ));
                                    }
                                }
                                "at_eof" => {
                                    if !last.eof || last.end != total {
                                        return Err(format!(
                                            "{label}: last page {}..{} eof {} is not EOF",
                                            last.start, last.end, last.eof
                                        ));
                                    }
                                }
                                "spanning_eof" => {
                                    if !(last.start < last.end && last.end == total) {
                                        return Err(format!(
                                            "{label}: last page {}..{} does not span EOF",
                                            last.start, last.end
                                        ));
                                    }
                                }
                                _ => {
                                    let value = execute(
                                        &task,
                                        "read",
                                        json!({"path": path, "range": {"start": total, "end": total + 1}}),
                                    )
                                    .await?;
                                    let page =
                                        read_page(&label, &value, &source, Some((total, total)))?;
                                    if !page.eof || !text(&value)?.is_empty() {
                                        return Err(format!(
                                            "{label}: past-EOF continuation is not terminal"
                                        ));
                                    }
                                }
                            }
                        }
                    }
                }
            }
            // Byte and line selectors agree on the same source interval.
            for position in ["start", "middle"] {
                let (offset, limit) = line_selection(position, lines.len());
                let start = *lines
                    .get(usize::try_from(offset).map_err(oracle::failed)?)
                    .ok_or_else(|| {
                        format!("{size}/{position}: line offset {offset} is outside the source")
                    })?;
                let end = lines
                    .get(usize::try_from(offset + limit).unwrap_or(0))
                    .copied()
                    .unwrap_or(total);
                let by_bytes = execute(
                    &task,
                    "read",
                    json!({"path": path, "range": {"start": start, "end": end}}),
                )
                .await?;
                let by_lines = execute(
                    &task,
                    "read",
                    json!({"path": path, "selection": {"kind": "lines", "offset": offset, "limit": limit}}),
                )
                .await?;
                if text(&by_bytes)? != text(&by_lines)? {
                    return Err(format!(
                        "{size}/{position}: byte and line selectors disagree: {:?} != {:?}",
                        text(&by_bytes)?,
                        text(&by_lines)?
                    ));
                }
            }
            Ok(true)
        })
    })
}
