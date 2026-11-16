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
        "multi_chunk" => 1_024,
        _ => 42_000,
    };
    let mut text = String::new();
    for index in 0..2_000 {
        text.push_str(&format!("line {index:04} abcdefghij\n"));
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

fn check_page(label: &str, value: &Value) -> std::result::Result<(), String> {
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
    let next = page.get("next");
    if start > end || end > total {
        return Err(format!("{label}: page interval {start}..{end} of {total}"));
    }
    if eof != (end == total) {
        return Err(format!("{label}: eof {eof} at {end} of {total}"));
    }
    if next.is_some() == eof {
        return Err(format!("{label}: next {next:?} with eof {eof}"));
    }
    let text = text(value)?;
    if text.starts_with("{\"file\"") || text.contains("\"descriptor\"") {
        return Err(format!(
            "{label}: read text is a machine envelope: {text:.80}"
        ));
    }
    Ok(())
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

fn read_world(seed: u64) -> std::result::Result<bool, String> {
    let size = size(seed);
    oracle::simulate(seed, move || {
        entry(async move {
            let storage = MemoryHarnessStorage::new(AgentId::new(), 1_000_000)
                .await
                .map_err(oracle::failed)?;
            let source = source(size);
            storage
                .stage(
                    OperationId::new(),
                    "ui/source.txt",
                    source.as_bytes(),
                    "text/plain",
                    "source.txt",
                )
                .await
                .map_err(oracle::failed)?;
            let task = task(&storage, RENDER_BYTES).await?;
            let total = source.len() as u64;
            let lines = line_offsets(&source);
            let path = "volume:///private/ui/source.txt";
            for selector in SELECTORS {
                for position in POSITIONS {
                    let label = format!("{size}/{selector}/{position}");
                    let arguments = match selector {
                        "byte_range" => {
                            let (start, end) = byte_range(position, total);
                            json!({"path": path, "range": {"start": start, "end": end}})
                        }
                        "line_selection" => {
                            let (offset, limit) = line_selection(position, lines.len());
                            json!({"path": path, "selection": {"kind": "lines", "offset": offset, "limit": limit}})
                        }
                        _ => json!({"path": path}),
                    };
                    let value = execute(&task, "read", arguments).await?;
                    check_page(&label, &value)?;
                    if selector == "byte_range" && position == "past_eof" {
                        if !text(&value)?.is_empty() {
                            return Err(format!("{label}: past-EOF byte range returned text"));
                        }
                    }
                    if selector == "line_selection" && position == "past_eof" {
                        if !text(&value)?.is_empty() {
                            return Err(format!("{label}: past-EOF line selection returned text"));
                        }
                    }
                    if selector == "none" && size == "above_stream_frame" && position == "start" {
                        let page = page(&value)?;
                        if page.get("next").is_none() {
                            return Err(format!("{label}: a large source did not page"));
                        }
                    }
                }
            }
            // Byte and line selectors agree on the same source interval.
            for position in ["start", "middle"] {
                let (offset, limit) = line_selection(position, lines.len());
                let start = lines[usize::try_from(offset).unwrap_or(0)];
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
