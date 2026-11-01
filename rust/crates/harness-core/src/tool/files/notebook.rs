//! Borrowed cell spans keep unselected outputs out of the allocation graph.
//! Edits splice those spans back into the same authoritative nbformat file.
use super::*;
use serde::de::{DeserializeSeed, SeqAccess, Visitor};
use serde_json::value::RawValue;
use std::{borrow::Cow, fmt};

/// The value is destroyed before the admission that owns its allocations.
pub(super) struct Decoded<T> {
    value: T,
    _hold: crate::NativeCustody,
}
impl<T> std::ops::Deref for Decoded<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}

pub(super) fn decode<'de, S: DeserializeSeed<'de>>(
    seed: S,
    source: &'de str,
    work: crate::NativeWork<'_>,
) -> Result<Decoded<S::Value>>
where
    S::Value: crate::HeapLayout,
{
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let decoded = io.decode_seed(
            seed,
            source.as_bytes(),
            crate::executor::json::JsonValueSchema::None,
            crate::tool::TOOL_SERDE_DIAGNOSTIC_LABEL_BYTES,
            crate::wire::ErrorCode::Invalid,
        )?;
        let (value, hold) = crate::filesystem::native_decoded(decoded)?;
        return Ok(Decoded {
            value,
            _hold: Some(hold),
        });
    }
    let _ = work;
    Ok(Decoded {
        value: crate::executor::json::json_from_slice_bounded(
            seed,
            source.as_bytes(),
            crate::executor::json::JsonBounds {
                maximum_bytes: source.len() as u64,
                maximum_items: u64::MAX,
                maximum_depth: crate::executor::json::RECORD_MAXIMUM_DEPTH,
            },
            None,
            Some(crate::executor::json::JsonValueSchema::None),
            &mut |_| Ok(()),
        )?,
        _hold: None,
    })
}

#[cfg(test)]
#[path = "notebook_checks.rs"]
mod checks;

#[derive(Deserialize, crate::HeapLayout)]
struct Notebook<'a> {
    nbformat: u32,
    #[serde(borrow)]
    cells: &'a RawValue,
}

#[derive(Deserialize, crate::HeapLayout)]
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
struct CellCount(u64);
impl crate::HeapLayout for CellCount {
    const LAYOUT: &'static crate::executor::json::layout::JsonLayout =
        &crate::executor::json::layout::JsonLayout::list(
            crate::executor::json::layout::Alloc::INLINE,
            &[<RawValue as crate::HeapLayout>::LAYOUT],
        );
}
impl<'de, F: FnMut(u64, &'de RawValue)> DeserializeSeed<'de> for EachCell<F> {
    type Value = CellCount;
    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<CellCount, D::Error> {
        deserializer.deserialize_seq(self)
    }
}
impl<'de, F: FnMut(u64, &'de RawValue)> Visitor<'de> for EachCell<F> {
    type Value = CellCount;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a notebook cell array")
    }
    fn visit_seq<A: SeqAccess<'de>>(
        mut self,
        mut sequence: A,
    ) -> std::result::Result<CellCount, A::Error> {
        let mut index = 0;
        while let Some(cell) = sequence.next_element::<&RawValue>()? {
            (self.0)(index, cell);
            index += 1;
        }
        Ok(CellCount(index))
    }
}

fn notebook<'a>(source: &'a str, work: crate::NativeWork<'_>) -> Result<Decoded<Notebook<'a>>> {
    let notebook = decode(std::marker::PhantomData::<Notebook<'a>>, source, work)?;
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
    Ok(decode(EachCell(callback), notebook.cells.get(), work)?
        .value
        .0)
}

fn span(source: &str, raw: &RawValue) -> (usize, usize) {
    let start = raw.get().as_ptr() as usize - source.as_ptr() as usize;
    (start, start + raw.get().len())
}

/// Encode borrowed fields on the original ledger, without a temporary Value graph.
fn encoded<T: Serialize>(
    value: &T,
    work: crate::NativeWork<'_>,
) -> Result<(String, crate::NativeCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let hold;
        let bytes = io.encode(value, io.remaining()?.bytes_encoded, false)?;
        hold = io.guard_bytes(&bytes)?;
        let mut allocation = edit::allocation(Some(work), std::mem::size_of::<String>())?;
        let mut text = String::new();
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
    let mut cell_error = None;
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
                if cell_error.is_none() {
                    match decode(std::marker::PhantomData::<Cell<'_>>, raw.get(), work) {
                        Ok(cell) => duplicate |= cell.id.as_deref() == Some(id.as_str()),
                        Err(error) => cell_error = Some(error),
                    }
                }
            }
        },
        work,
    )?;
    if let Some(error) = cell_error {
        return Err(error);
    }
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
            let cell = decode(std::marker::PhantomData::<Cell<'_>>, raw.get(), work)?;
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
            let cell = decode(std::marker::PhantomData::<Cell<'_>>, raw.get(), work)?;
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
        }
    }
    substitutions.sort_unstable_by_key(|entry| entry.map_or(usize::MAX, |entry| entry.0));
    let extra = substitutions
        .iter()
        .flatten()
        .try_fold(0usize, |total, (_, _, text)| total.checked_add(text.len()))
        .ok_or_else(|| executor::rejection(work, "notebook output size overflow"))?;
    let mut hold = edit::allocation(work, std::mem::size_of::<String>())?;
    let mut text = String::new();
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
