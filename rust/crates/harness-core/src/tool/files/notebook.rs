//! Cell operations publish back to the same versioned nbformat file.
use super::*;

fn parse(source: &str, work: crate::NativeWork<'_>) -> Result<(Value, crate::NativeCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let value: Value = crate::filesystem::retain_decoded(io.decode(
            std::marker::PhantomData::<Value>,
            source.as_bytes(),
            crate::wire::ErrorCode::Invalid,
        )?)?;
        let bytes = crate::filesystem::UploadRecord::logical_owned_bytes(&value)?;
        return Ok((value, Some(io.into_native_custody(bytes)?)));
    }
    Ok((
        serde_json::from_str(source)
            .map_err(|_| executor::rejection(work, "invalid notebook JSON"))?,
        None,
    ))
}

fn cells(value: &Value) -> Result<&Vec<Value>> {
    if value.get("nbformat").and_then(Value::as_u64) != Some(4) {
        return Err(Error::tool_rejected("cell operations require nbformat v4"));
    }
    value
        .get("cells")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::tool_rejected("notebook cells must be an array"))
}

pub(super) fn read(
    source: &str,
    offset: u64,
    limit: Option<u64>,
    maximum: u64,
    work: crate::NativeWork<'_>,
) -> Result<(String, ReadPage, crate::NativeCustody)> {
    let (value, _hold) = parse(source, work)?;
    let cells = cells(&value)?;
    let start = usize::try_from(offset)
        .ok()
        .filter(|start| *start <= cells.len())
        .ok_or_else(|| executor::rejection(work, "notebook offset exceeds cell count"))?;
    let count = limit.unwrap_or(u64::MAX).min((cells.len() - start) as u64);
    let mut text = String::new();
    let mut allocation = edit::allocation(work, std::mem::size_of::<String>())?;
    let mut end = start;
    for cell in cells
        .iter()
        .skip(start)
        .take(usize::try_from(count).unwrap_or(usize::MAX))
    {
        // Keep the complete cell record, including metadata, source and outputs.
        // A single oversized cell is continued by byte range against the source.
        let encoded = super::value(cell, u64::MAX, work)?;
        let line = serde_json::to_string(&encoded.0)
            .map_err(|_| executor::rejection(work, "notebook cell encoding failed"))?;
        if (text.len() + line.len() + 1) as u64 > maximum {
            if end == start {
                return Err(executor::rejection(
                    work,
                    "cell exceeds view credit; select a byte range or increase admitted rendering credit",
                ));
            }
            break;
        }
        edit::reserve_string(&mut allocation, &mut text, line.len() + 1)?;
        edit::charge(&allocation, line.len() as u64 + 1, 1)?;
        text.push_str(&line);
        text.push('\n');
        end += 1;
    }
    Ok((
        text,
        ReadPage {
            start: offset,
            end: end as u64,
            next: (end < cells.len()).then_some(end as u64),
            offset,
        },
        allocation,
    ))
}

pub(super) fn edit(
    source: &str,
    operation: &NotebookEdit,
    work: crate::NativeWork<'_>,
) -> Result<(String, crate::NativeCustody)> {
    let (mut value, _hold) = parse(source, work)?;
    cells(&value)?;
    let cells = value
        .get_mut("cells")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| executor::rejection(work, "notebook cells must be an array"))?;
    let (index, id) = match operation {
        NotebookEdit::Replace { index, id, .. } | NotebookEdit::Delete { index, id } => {
            (*index, id.as_deref())
        }
        NotebookEdit::Insert { index, .. } => (*index, None),
    };
    let index = usize::try_from(index)
        .map_err(|_| executor::rejection(work, "cell index exceeds notebook"))?;
    if !matches!(operation, NotebookEdit::Insert { .. }) {
        let cell = cells
            .get(index)
            .ok_or_else(|| executor::rejection(work, "cell index exceeds notebook"))?;
        if id.is_some_and(|id| cell.get("id").and_then(Value::as_str) != Some(id)) {
            return Err(executor::rejection(
                work,
                "cell ID differs from the selected cell",
            ));
        }
    }
    match operation {
        NotebookEdit::Replace { source, .. } => {
            let cell = cells
                .get_mut(index)
                .and_then(Value::as_object_mut)
                .ok_or_else(|| executor::rejection(work, "notebook cell must be an object"))?;
            cell.insert("source".into(), Value::String(source.clone()));
            // Code outputs belong to the previous source generation.
            if cell.get("cell_type").and_then(Value::as_str) == Some("code") {
                cell.insert("outputs".into(), json!([]));
                cell.insert("execution_count".into(), Value::Null);
            }
        }
        NotebookEdit::Insert {
            id,
            cell_type,
            source,
            ..
        } => {
            if index > cells.len()
                || !matches!(cell_type.as_str(), "code" | "markdown" | "raw")
                || id.is_empty()
                || id.len() > 64
                || !id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
                || cells
                    .iter()
                    .any(|cell| cell.get("id").and_then(Value::as_str) == Some(id))
            {
                return Err(executor::rejection(
                    work,
                    "invalid cell insertion index, type or unique ID",
                ));
            }
            let mut cell = json!({"id":id,"cell_type":cell_type,"metadata":{},"source":source});
            if cell_type == "code" {
                cell["outputs"] = json!([]);
                cell["execution_count"] = Value::Null;
            }
            cells.insert(index, cell);
        }
        NotebookEdit::Delete { .. } => {
            cells.remove(index);
        }
    }
    let encoded = serde_json::to_string(&value)
        .map_err(|_| executor::rejection(work, "notebook encoding failed"))?;
    let allocation = edit::allocation(work, encoded.capacity())?;
    Ok((encoded, allocation))
}
