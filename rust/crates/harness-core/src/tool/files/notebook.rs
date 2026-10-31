//! Borrowed cell spans keep unselected outputs out of the allocation graph.
//! Edits splice those spans back into the same authoritative nbformat file.
use super::*;
use serde::de::{DeserializeSeed, SeqAccess, Visitor};
use serde_json::value::RawValue;
use std::{borrow::Cow, fmt};

#[cfg(test)]
#[path = "notebook_checks.rs"]
mod checks;

#[derive(Deserialize)]
struct Notebook<'a> {
    nbformat: u32,
    #[serde(borrow)]
    cells: &'a RawValue,
}

#[derive(Deserialize)]
struct Cell<'a> {
    #[serde(borrow)]
    id: Option<Cow<'a, str>>,
    #[serde(borrow)]
    cell_type: Cow<'a, str>,
    #[serde(borrow)]
    source: &'a RawValue,
    #[serde(borrow)]
    outputs: Option<&'a RawValue>,
    #[serde(borrow)]
    execution_count: Option<&'a RawValue>,
}

struct EachCell<F>(F);
impl<'de, F: FnMut(u64, &'de RawValue)> DeserializeSeed<'de> for EachCell<F> {
    type Value = u64;
    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<u64, D::Error> {
        deserializer.deserialize_seq(self)
    }
}
impl<'de, F: FnMut(u64, &'de RawValue)> Visitor<'de> for EachCell<F> {
    type Value = u64;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a notebook cell array")
    }
    fn visit_seq<A: SeqAccess<'de>>(
        mut self,
        mut sequence: A,
    ) -> std::result::Result<u64, A::Error> {
        let mut index = 0;
        while let Some(cell) = sequence.next_element::<&RawValue>()? {
            (self.0)(index, cell);
            index += 1;
        }
        Ok(index)
    }
}

fn notebook<'a>(source: &'a str, work: crate::NativeWork<'_>) -> Result<Notebook<'a>> {
    let charge = edit::allocation(work, 0)?;
    edit::charge(&charge, 0, source.len() as u64)?;
    let notebook: Notebook<'a> = serde_json::from_str(source)
        .map_err(|_| executor::rejection(work, "invalid notebook JSON"))?;
    if notebook.nbformat != 4 {
        return Err(executor::rejection(
            work,
            "cell operations require nbformat v4",
        ));
    }
    Ok(notebook)
}

fn each<'a>(
    notebook: &Notebook<'a>,
    callback: impl FnMut(u64, &'a RawValue),
    work: crate::NativeWork<'_>,
) -> Result<u64> {
    EachCell(callback)
        .deserialize(&mut serde_json::Deserializer::from_str(
            notebook.cells.get(),
        ))
        .map_err(|_| executor::rejection(work, "notebook cells must be an array"))
}

fn span(source: &str, raw: &RawValue) -> (usize, usize) {
    let start = raw.get().as_ptr() as usize - source.as_ptr() as usize;
    (start, start + raw.get().len())
}

pub(super) fn read(
    source: &str,
    offset: u64,
    limit: Option<u64>,
    maximum: u64,
    work: crate::NativeWork<'_>,
) -> Result<(String, ReadPage, crate::NativeCustody)> {
    let notebook = notebook(source, work)?;
    let mut interval = None;
    let mut empty_boundary = None;
    let mut next_cell = offset;
    let count = each(
        &notebook,
        |index, cell| {
            if index == offset {
                empty_boundary = Some(span(source, cell).0);
            }
            if index >= offset && index - offset < limit.unwrap_or(u64::MAX) {
                let (start, end) = span(source, cell);
                let first = interval.map_or(start, |(first, _)| first);
                if (end - first) as u64 <= maximum || interval.is_none() {
                    interval = Some((first, end));
                    next_cell = index + 1;
                }
            }
        },
        work,
    )?;
    if offset > count {
        return Err(executor::rejection(
            work,
            "notebook offset exceeds cell count",
        ));
    }
    let (start, end) = interval.unwrap_or_else(|| {
        let end = empty_boundary.unwrap_or_else(|| span(source, notebook.cells).1 - 1);
        (end, end)
    });
    let selected = source
        .get(start..end)
        .ok_or_else(|| executor::rejection(work, "invalid notebook span"))?;
    let shown =
        crate::tools::bounds::prefix(selected, usize::try_from(maximum).unwrap_or(usize::MAX));
    let mut text = String::new();
    let mut hold = edit::allocation(work, std::mem::size_of::<String>())?;
    edit::reserve_string(&mut hold, &mut text, shown.len())?;
    edit::charge(&hold, shown.len() as u64, 1)?;
    text.push_str(shown);
    let end = start + shown.len();
    Ok((
        text,
        ReadPage {
            start: start as u64,
            end: end as u64,
            next: (next_cell < count || shown.len() < selected.len()).then_some(end as u64),
            offset,
        },
        hold,
    ))
}

/// Encode borrowed fields on the original ledger, without a temporary Value graph.
fn encoded<T: Serialize>(
    value: &T,
    work: crate::NativeWork<'_>,
) -> Result<(String, crate::NativeCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let bytes = io.encode(value, io.remaining()?.bytes_encoded, false)?;
        let hold = io.guard_bytes(&bytes)?;
        let mut text = String::new();
        let mut allocation = edit::allocation(Some(work), std::mem::size_of::<String>())?;
        edit::reserve_string(&mut allocation, &mut text, bytes.len())?;
        edit::charge(&allocation, bytes.len() as u64, 0)?;
        text.push_str(
            std::str::from_utf8(&bytes)
                .map_err(|_| executor::rejection(Some(work), "invalid encoded JSON"))?,
        );
        drop(bytes);
        drop(hold);
        return Ok((text, allocation));
    }
    Ok((
        serde_json::to_string(value)
            .map_err(|_| executor::rejection(work, "notebook encoding failed"))?,
        None,
    ))
}

pub(super) fn edit(
    source: &str,
    operation: &NotebookEdit,
    work: crate::NativeWork<'_>,
) -> Result<(String, crate::NativeCustody)> {
    let notebook = notebook(source, work)?;
    let (index, expected_id) = match operation {
        NotebookEdit::Replace { index, id, .. } | NotebookEdit::Delete { index, id } => {
            (*index, id.as_deref())
        }
        NotebookEdit::Insert { index, .. } => (*index, None),
    };
    let mut selected = None;
    let mut previous = None;
    let mut next = None;
    let mut duplicate = false;
    let count = each(
        &notebook,
        |position, raw| {
            if position == index {
                selected = Some(raw);
            }
            if position.checked_add(1) == Some(index) {
                previous = Some(span(source, raw).1);
            }
            if position == index.saturating_add(1) {
                next = Some(span(source, raw).0);
            }
            if let NotebookEdit::Insert { id, .. } = operation {
                if let Ok(cell) = serde_json::from_str::<Cell<'_>>(raw.get()) {
                    duplicate |= cell.id.as_deref() == Some(id.as_str());
                }
            }
        },
        work,
    )?;
    if index > count || (index == count && !matches!(operation, NotebookEdit::Insert { .. })) {
        return Err(executor::rejection(work, "cell index exceeds notebook"));
    }
    let mut substitutions: [Option<(usize, usize, &str)>; 3] = [None; 3];
    let _replacement_hold;
    let replacement;
    match operation {
        NotebookEdit::Replace {
            source: new_source, ..
        } => {
            let raw = selected.ok_or_else(|| executor::rejection(work, "cell is missing"))?;
            let cell: Cell<'_> = serde_json::from_str(raw.get())
                .map_err(|_| executor::rejection(work, "invalid notebook cell"))?;
            if expected_id.is_some_and(|id| cell.id.as_deref() != Some(id)) {
                return Err(executor::rejection(
                    work,
                    "cell ID differs from the selected cell",
                ));
            }
            (replacement, _replacement_hold) = encoded(new_source, work)?;
            let (start, end) = span(source, cell.source);
            substitutions[0] = Some((start, end, &replacement));
            if cell.cell_type == "code" {
                if let Some(outputs) = cell.outputs {
                    let (start, end) = span(source, outputs);
                    substitutions[1] = Some((start, end, "[]"));
                }
                if let Some(execution) = cell.execution_count {
                    let (start, end) = span(source, execution);
                    substitutions[2] = Some((start, end, "null"));
                }
            }
        }
        NotebookEdit::Insert {
            id,
            cell_type,
            source: new_source,
            ..
        } => {
            if duplicate
                || !matches!(cell_type.as_str(), "code" | "markdown" | "raw")
                || id.is_empty()
                || id.len() > 64
                || !id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
            {
                return Err(executor::rejection(
                    work,
                    "invalid notebook cell type or unique ID",
                ));
            }
            #[derive(Serialize)]
            struct NewCell<'a> {
                id: &'a str,
                cell_type: &'a str,
                source: &'a str,
                metadata: std::collections::BTreeMap<(), ()>,
                #[serde(skip_serializing_if = "Option::is_none")]
                outputs: Option<[(); 0]>,
                #[serde(skip_serializing_if = "Option::is_none")]
                execution_count: Option<()>,
            }
            // execution_count must be null for code cells; serialization of Some(()) is null.
            (replacement, _replacement_hold) = encoded(
                &NewCell {
                    id,
                    cell_type,
                    source: new_source,
                    metadata: Default::default(),
                    outputs: (cell_type == "code").then_some([]),
                    execution_count: (cell_type == "code").then_some(()),
                },
                work,
            )?;
            let point = selected.map_or_else(
                || span(source, notebook.cells).1 - 1,
                |raw| span(source, raw).0,
            );
            substitutions[0] = Some((point, point, &replacement));
        }
        NotebookEdit::Delete { .. } => {
            let raw = selected.ok_or_else(|| executor::rejection(work, "cell is missing"))?;
            let cell: Cell<'_> = serde_json::from_str(raw.get())
                .map_err(|_| executor::rejection(work, "invalid notebook cell"))?;
            if expected_id.is_some_and(|id| cell.id.as_deref() != Some(id)) {
                return Err(executor::rejection(
                    work,
                    "cell ID differs from the selected cell",
                ));
            }
            let (start, end) = span(source, raw);
            substitutions[0] = Some(if let Some(previous) = previous {
                (previous, end, "")
            } else if let Some(next) = next {
                (start, next, "")
            } else {
                (start, end, "")
            });
            replacement = String::new();
            _replacement_hold = None;
        }
    }
    substitutions.sort_unstable_by_key(|entry| entry.map_or(usize::MAX, |entry| entry.0));
    let extra = substitutions
        .iter()
        .flatten()
        .try_fold(0usize, |total, (_, _, text)| total.checked_add(text.len()))
        .ok_or_else(|| executor::rejection(work, "notebook output size overflow"))?;
    let mut text = String::new();
    let mut hold = edit::allocation(work, std::mem::size_of::<String>())?;
    edit::reserve_string(
        &mut hold,
        &mut text,
        source
            .len()
            .checked_add(extra)
            .and_then(|n| n.checked_add(1))
            .ok_or_else(|| executor::rejection(work, "notebook output size overflow"))?,
    )?;
    edit::charge(&hold, (source.len() + extra) as u64, 1)?;
    let mut cursor = 0;
    for (start, end, replacement) in substitutions.into_iter().flatten() {
        text.push_str(
            source
                .get(cursor..start)
                .ok_or_else(|| executor::rejection(work, "overlapping notebook spans"))?,
        );
        if matches!(operation, NotebookEdit::Insert { .. }) && index == count && count > 0 {
            text.push(',');
        }
        text.push_str(replacement);
        if matches!(operation, NotebookEdit::Insert { .. }) && index < count {
            text.push(',');
        }
        cursor = end;
    }
    text.push_str(
        source
            .get(cursor..)
            .ok_or_else(|| executor::rejection(work, "invalid notebook span"))?,
    );
    Ok((text, hold))
}
