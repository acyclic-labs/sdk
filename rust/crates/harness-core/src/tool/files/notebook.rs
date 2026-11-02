//! Borrowed cell spans keep unselected outputs out of the allocation graph.
//! Edits splice those spans back into the same authoritative nbformat file.
use super::*;
use crate::executor::json::{BorrowedText, JsonObject};
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
                maximum_bytes: Some(source.len() as u64),
                maximum_items: None,
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
    #[serde(borrow)]
    metadata: Option<&'a RawValue>,
}

#[derive(Deserialize, crate::HeapLayout)]
struct NotebookMetadata<'a> {
    #[serde(borrow)]
    language_info: Option<&'a RawValue>,
    #[serde(borrow)]
    kernelspec: Option<&'a RawValue>,
}

#[derive(Deserialize, crate::HeapLayout)]
struct LanguageInfo<'a> {
    #[serde(borrow)]
    name: Option<BorrowedText<'a>>,
}

#[derive(Deserialize, crate::HeapLayout)]
struct KernelSpec<'a> {
    #[serde(borrow)]
    language: Option<BorrowedText<'a>>,
}

fn language<'a>(
    notebook: &Notebook<'a>,
    work: crate::NativeWork<'_>,
) -> Result<Option<Decoded<Cow<'a, str>>>> {
    let Some(raw) = notebook.metadata else {
        return Ok(None);
    };
    let metadata = decode(
        JsonObject(std::marker::PhantomData::<NotebookMetadata<'a>>),
        raw.get(),
        work,
    )?;
    // The declared language name takes precedence over the kernel specification.
    if let Some(raw) = metadata.language_info {
        let info = decode(
            JsonObject(std::marker::PhantomData::<LanguageInfo<'a>>),
            raw.get(),
            work,
        )?;
        if let Some(value) = info.value.name {
            return Ok(Some(Decoded {
                value: value.0,
                _hold: info._hold,
            }));
        }
    }
    if let Some(raw) = metadata.kernelspec {
        let spec = decode(
            JsonObject(std::marker::PhantomData::<KernelSpec<'a>>),
            raw.get(),
            work,
        )?;
        if let Some(value) = spec.value.language {
            return Ok(Some(Decoded {
                value: value.0,
                _hold: spec._hold,
            }));
        }
    }
    Ok(None)
}

#[derive(Deserialize, crate::HeapLayout)]
pub(super) struct Cell<'a> {
    #[serde(borrow)]
    pub(super) id: Option<Cow<'a, str>>,
    #[serde(borrow)]
    pub(super) cell_type: Cow<'a, str>,
    #[serde(borrow)]
    pub(super) source: &'a RawValue,
    #[serde(borrow)]
    outputs: Option<&'a RawValue>,
    #[serde(borrow)]
    execution_count: Option<&'a RawValue>,
}

/// Publication outcomes consume these facts while the original source is live.
/// Inserts have no prior cell; their index is the resolved insertion boundary.
pub(super) struct EditFacts<'a> {
    pub(super) index: u64,
    pub(super) prior: Option<Decoded<Cell<'a>>>,
    pub(super) language: Option<Decoded<Cow<'a, str>>>,
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
    let notebook = decode(
        JsonObject(std::marker::PhantomData::<Notebook<'a>>),
        source,
        work,
    )?;
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
        let bytes = io.encode(value, Some(io.remaining()?.bytes_encoded), false)?;
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

type Substitutions<'a> = [Option<(usize, usize, &'a str)>; 6];

#[derive(Clone, Copy)]
struct CellField {
    key: usize,
    start: usize,
    end: usize,
    previous: Option<usize>,
    next: Option<usize>,
}

/// Type conversion uses the codec's existing lexical cursor to retain field
/// boundaries. No cell values or unselected outputs become an owned JSON graph.
fn convert_cell(
    source: &str,
    raw: &RawValue,
    cell: &Cell<'_>,
    kind: &str,
    substitutions: &mut Substitutions<'_>,
    work: crate::NativeWork<'_>,
) -> Result<()> {
    use crate::executor::json::{JsonBounds, JsonCodecCharge, JsonSpans, SpanEvent};
    let encoded_kind = match kind {
        "code" => "\"code\"",
        "markdown" => "\"markdown\"",
        "raw" => "\"raw\"",
        _ => return Err(executor::rejection(work, "invalid notebook cell type")),
    };
    if cell.cell_type == kind {
        return Ok(());
    }
    let _hold = edit::allocation(
        work,
        std::mem::size_of::<JsonSpans>() + std::mem::size_of::<[Option<CellField>; 4]>(),
    )?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let owned_io = work
        .map(crate::executor::PreparationIo::from_native_work)
        .transpose()?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let io = owned_io.as_ref();
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let io = None;
    let origin = span(source, raw).0;
    let mut fields: [Option<CellField>; 4] = [None; 4];
    let mut key = None;
    let mut previous_end = None;
    let mut last_field: Option<usize> = None;
    let mut cursor = JsonSpans::default();
    let mut event = |event| -> Result<bool> {
        match event {
            SpanEvent::Key {
                depth: 1,
                start,
                end,
            } => {
                if let Some(last) = last_field.take()
                    && let Some(field) = fields[last].as_mut()
                {
                    field.next = Some(origin + start as usize);
                }
                let label = decode(
                    std::marker::PhantomData::<crate::executor::json::BorrowedText<'_>>,
                    &raw.get()[start as usize..end as usize],
                    work,
                )?;
                let field = match label.0.as_ref() {
                    "cell_type" => Some(0),
                    "outputs" => Some(1),
                    "execution_count" => Some(2),
                    "attachments" => Some(3),
                    _ => None,
                };
                key = field.map(|field| (field, origin + start as usize));
            }
            SpanEvent::End {
                depth: 1,
                start,
                end,
                ..
            } => {
                if let Some((field, key)) = key.take() {
                    if fields[field].is_some() {
                        return Err(executor::rejection(work, "duplicate notebook cell field"));
                    }
                    fields[field] = Some(CellField {
                        key,
                        start: origin + start as usize,
                        end: origin + end as usize,
                        previous: previous_end,
                        next: None,
                    });
                    last_field = Some(field);
                }
                previous_end = Some(origin + end as usize);
            }
            _ => {}
        }
        Ok(false)
    };
    crate::executor::with_history_io(io, |history| -> Result<()> {
        if let Some(history) = history.as_deref_mut() {
            history.codec_charge(JsonCodecCharge::Read {
                bytes: raw.get().len() as u64,
            })?;
        }
        cursor.feed(
            raw.get().as_bytes(),
            JsonBounds {
                maximum_bytes: Some(raw.get().len() as u64),
                maximum_items: None,
                maximum_depth: crate::executor::json::RECORD_MAXIMUM_DEPTH,
            },
            Some(crate::wire::ErrorCode::Invalid),
            &mut |charge| {
                if let Some(history) = history.as_deref_mut() {
                    history.codec_charge(charge)?;
                }
                Ok(())
            },
            &mut event,
        )?;
        cursor.finish(Some(crate::wire::ErrorCode::Invalid), &mut event)
    })?;
    let field =
        fields[0].ok_or_else(|| executor::rejection(work, "notebook cell type is missing"))?;
    substitutions[3] = Some((field.start, field.end, encoded_kind));
    let remove = |field: CellField| {
        field.next.map_or(
            (field.previous.unwrap_or(field.key), field.end, ""),
            |next| (field.key, next, ""),
        )
    };
    if kind == "code" {
        for (slot, field, replacement) in [(1, fields[1], "[]"), (2, fields[2], "null")] {
            if let Some(field) = field {
                substitutions[slot] = Some((field.start, field.end, replacement));
            }
        }
        let missing = match (fields[1].is_none(), fields[2].is_none()) {
            (true, true) => ",\"outputs\":[],\"execution_count\":null",
            (true, false) => ",\"outputs\":[]",
            (false, true) => ",\"execution_count\":null",
            (false, false) => "",
        };
        if !missing.is_empty() {
            let end = span(source, raw).1 - 1;
            substitutions[5] = Some((end, end, missing));
        }
        substitutions[4] = fields[3].map(remove);
    } else {
        let mut removed = [fields[1], fields[2]];
        removed.sort_unstable_by_key(|field| field.map_or(usize::MAX, |field| field.key));
        if let [Some(first), Some(last)] = removed
            && first.next == Some(last.key)
        {
            // One run owns both the leading and trailing comma decision. In
            // particular, a removed final run consumes its preceding comma.
            removed = [
                Some(CellField {
                    end: last.end,
                    next: last.next,
                    ..first
                }),
                None,
            ];
        }
        substitutions[1] = removed[0].map(remove);
        substitutions[2] = removed[1].map(remove);
    }
    Ok(())
}

pub(super) fn edit<'a>(
    source: &'a str,
    operation: &NotebookEdit,
    work: crate::NativeWork<'_>,
) -> Result<(String, crate::NativeCustody, EditFacts<'a>)> {
    let notebook = notebook(source, work)?;
    let (target, after) = match operation {
        NotebookEdit::Replace { cell, .. } | NotebookEdit::Delete { cell } => (cell, false),
        NotebookEdit::Insert { cell, after, .. } => (cell, *after),
    };
    let mut index = None;
    let mut selected = None;
    let mut selected_cell = None;
    let mut previous = None;
    let mut next = None;
    let mut previous_end = None;
    let mut duplicate = false;
    let mut cell_error = None;
    let count = each(
        &notebook,
        |position, raw| {
            if cell_error.is_some() {
                return;
            }
            let cell = if matches!(target, NotebookCell::Id { .. })
                || matches!(operation, NotebookEdit::Insert { .. })
                || matches!(target, NotebookCell::Index { index, id: Some(_) } if *index == position)
            {
                match decode(
                    JsonObject(std::marker::PhantomData::<Cell<'_>>),
                    raw.get(),
                    work,
                ) {
                    Ok(cell) => Some(cell),
                    Err(error) => {
                        cell_error = Some(error);
                        return;
                    }
                }
            } else {
                None
            };
            let matches = match target {
                NotebookCell::Index { index, id } if *index == position => {
                    if id.as_deref().is_some_and(|id| {
                        cell.as_ref().and_then(|cell| cell.id.as_deref()) != Some(id)
                    }) {
                        cell_error = Some(executor::rejection(
                            work,
                            "cell ID differs from the selected cell",
                        ));
                        return;
                    }
                    true
                }
                NotebookCell::Id { id } => {
                    cell.as_ref().and_then(|cell| cell.id.as_deref()) == Some(id.as_str())
                }
                _ => false,
            };
            if matches {
                if index.is_some() {
                    cell_error = Some(executor::rejection(work, "cell ID is ambiguous"));
                    return;
                }
                index = position.checked_add(u64::from(after));
            }
            if let NotebookEdit::Insert { id, .. } = operation {
                duplicate |= cell.as_ref().and_then(|cell| cell.id.as_deref()) == Some(id.as_str());
            }
            if Some(position) == index {
                selected = Some(raw);
                if !matches!(operation, NotebookEdit::Insert { .. }) {
                    selected_cell = cell;
                }
                previous = previous_end;
            }
            if index.and_then(|index| index.checked_add(1)) == Some(position) {
                next = Some(span(source, raw).0);
            }
            previous_end = Some(span(source, raw).1);
        },
        work,
    )?;
    if let Some(error) = cell_error {
        return Err(error);
    }
    let index = index
        .or_else(|| match target {
            NotebookCell::Index { index, id: None }
                if *index == count
                    && !after
                    && matches!(operation, NotebookEdit::Insert { .. }) =>
            {
                Some(count)
            }
            _ => None,
        })
        .ok_or_else(|| executor::rejection(work, "cell selection is absent from notebook"))?;
    let mut substitutions: Substitutions<'_> = [None; 6];
    let _replacement_hold;
    let replacement;
    let mut prior = None;
    match operation {
        NotebookEdit::Replace {
            source: new_source,
            cell_type,
            ..
        } => {
            let raw = selected.ok_or_else(|| executor::rejection(work, "cell is missing"))?;
            let cell = match selected_cell {
                Some(cell) => cell,
                None => decode(
                    JsonObject(std::marker::PhantomData::<Cell<'_>>),
                    raw.get(),
                    work,
                )?,
            };
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
            if let Some(kind) = cell_type {
                convert_cell(source, raw, &cell, kind, &mut substitutions, work)?;
            }
            prior = Some(cell);
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
            prior = Some(match selected_cell {
                Some(cell) => cell,
                None => decode(
                    JsonObject(std::marker::PhantomData::<Cell<'_>>),
                    raw.get(),
                    work,
                )?,
            });
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
    let language = language(&notebook, work)?;
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
    Ok((
        text,
        hold,
        EditFacts {
            index,
            prior,
            language,
        },
    ))
}
