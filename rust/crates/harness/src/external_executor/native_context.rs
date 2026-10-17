//! Views over Pi native session files; selections retain the original record bytes.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::{borrow::Cow, collections::BTreeMap};

mod header;
mod projection;
mod selection;
pub(crate) mod source;
pub(crate) use header::{fresh, rebase, seed};

/// A decoded append-only native session file. Rows borrow their source bytes.
pub struct SessionFile<'a> {
    header: header::Header,
    header_bytes: &'a [u8],
    rows: Vec<Row<'a>>,
    ids: BTreeMap<Cow<'a, str>, usize>,
}

/// Exact source branch and pre-invocation context selected from a native file.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextCut {
    /// Native source identity, independent of the child label.
    pub session: String,
    /// Workspace recorded by the source native SDK.
    pub workspace: String,
    /// Active source leaf at selection, including the invoking assistant.
    pub head: Option<String>,
    /// Last inherited raw entry; the invoking assistant is excluded in full.
    pub through: Option<String>,
    /// Original native tool invocation selecting this cut.
    pub invocation: String,
    /// Native tool name of the unique owning invocation.
    pub tool: String,
    /// Fingerprint of the original header and active source branch.
    pub digest: [u8; 32],
}

struct Row<'a> {
    bytes: &'a [u8],
    meta: Metadata<'a>,
    parent: Option<usize>,
}

#[derive(Deserialize)]
struct Metadata<'a> {
    #[serde(rename = "type", borrow)]
    kind: Cow<'a, str>,
    #[serde(borrow)]
    id: Cow<'a, str>,
    #[serde(
        rename = "parentId",
        default,
        borrow,
        deserialize_with = "present_parent"
    )]
    parent: Option<Option<Cow<'a, str>>>,
    #[serde(borrow)]
    timestamp: Cow<'a, str>,
    #[serde(default, borrow)]
    message: Option<&'a RawValue>,
    #[serde(rename = "firstKeptEntryId", default, borrow)]
    kept: Option<Cow<'a, str>>,
    #[serde(rename = "targetId", default, borrow)]
    target: Option<Cow<'a, str>>,
    #[serde(default, borrow, deserialize_with = "present_raw")]
    replacement: Option<&'a RawValue>,
}

fn present_raw<'de, D: serde::Deserializer<'de>>(
    decoder: D,
) -> std::result::Result<Option<&'de RawValue>, D::Error> {
    <&RawValue>::deserialize(decoder).map(Some)
}

fn present_parent<'de, D: serde::Deserializer<'de>>(
    decoder: D,
) -> std::result::Result<Option<Option<Cow<'de, str>>>, D::Error> {
    Option::<Cow<'de, str>>::deserialize(decoder).map(Some)
}

fn text(value: &str) -> bool {
    !value.is_empty() && !value.contains('\0')
}
fn invalid() -> Error {
    Error::Invalid("invalid native context file or cut".into())
}

fn decode<'a, T: Deserialize<'a>>(bytes: &'a [u8], maximum: u64) -> Result<T> {
    crate::json_from_slice_bounded(
        std::marker::PhantomData::<T>,
        bytes,
        crate::JsonBounds {
            maximum_bytes: maximum,
            maximum_items: maximum.saturating_add(1),
            maximum_depth: 256,
        },
        bytes.len() as u64,
        None,
        None,
        &mut |_| Ok(()),
    )
    .map_err(|_| invalid())
}

impl<'a> SessionFile<'a> {
    /// Decodes a bounded file, rejecting duplicate IDs, missing parents and cycles.
    /// `maximum` comes from the file provider's authorized read allowance.
    pub fn decode(bytes: &'a [u8], maximum: u64) -> Result<Self> {
        if bytes.len() as u64 > maximum {
            return Err(invalid());
        }
        let mut lines = bytes
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.iter().all(u8::is_ascii_whitespace));
        let header_bytes = lines.next().ok_or_else(invalid)?;
        let header: header::Header = decode(header_bytes, maximum)?;
        header.validate()?;
        let mut rows: Vec<Row<'a>> = Vec::new();
        let mut ids: BTreeMap<Cow<'a, str>, usize> = BTreeMap::new();
        for bytes in lines {
            let meta: Metadata<'a> = decode(bytes, maximum)?;
            if !text(&meta.id)
                || !text(&meta.timestamp)
                || ids.contains_key(meta.id.as_ref())
                || !matches!(
                    meta.kind.as_ref(),
                    "message"
                        | "thinking_level_change"
                        | "model_change"
                        | "usage"
                        | "compaction"
                        | "branch_summary"
                        | "custom"
                        | "custom_message"
                        | "context_edit"
                        | "label"
                        | "session_info"
                )
            {
                return Err(invalid());
            }
            // Append-only parent references are strictly earlier than the row.
            // This establishes acyclicity without recursion or a depth limit.
            let parent = meta
                .parent
                .as_ref()
                .ok_or_else(invalid)?
                .as_deref()
                .map(|id| ids.get(id).copied().ok_or_else(invalid))
                .transpose()?;
            if meta.kind == "message" && meta.message.is_none() {
                return Err(invalid());
            }
            if meta.kind == "context_edit" && (meta.target.is_none() || meta.replacement.is_none())
            {
                return Err(invalid());
            }
            if meta.kind == "compaction" && meta.kept.is_none() {
                return Err(invalid());
            }
            ids.insert(meta.id.clone(), rows.len());
            rows.push(Row {
                bytes,
                meta,
                parent,
            });
        }
        Ok(Self {
            header,
            header_bytes,
            rows,
            ids,
        })
    }

    fn branch(&self, head: Option<&str>) -> Result<Vec<usize>> {
        let mut current = head
            .map(|id| self.ids.get(id).copied().ok_or_else(invalid))
            .transpose()?;
        let mut branch = Vec::new();
        while let Some(index) = current {
            branch.push(index);
            current = self.rows.get(index).ok_or_else(invalid)?.parent;
        }
        branch.reverse();
        Ok(branch)
    }
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn fuzz(bytes: &[u8]) {
    source::fuzz(bytes);
    if let Ok(file) = SessionFile::decode(bytes, acyclic_stream::MAX_RECORD_BYTES as u64) {
        if let Some(row) = file.rows.last() {
            if let Ok(cut) = file.cut(Some(&row.meta.id), "spawn") {
                let encoded = serde_json::to_vec(&cut).unwrap();
                assert_eq!(serde_json::from_slice::<ContextCut>(&encoded).unwrap(), cut);
                file.require_cut(Some(&row.meta.id), &cut).unwrap();
            }
        }
    }
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;
