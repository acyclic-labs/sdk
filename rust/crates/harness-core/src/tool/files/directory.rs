//! Directory selections use the same authenticated reader and version codec as files.
use super::*;

/// A name's kind at the selected immutable generation.
#[derive(
    Clone,
    Copy,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    schemars::JsonSchema,
    crate::HeapLayout,
)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum DirectoryEntryKind {
    /// Regular file.
    File,
    /// Child directory.
    Dir,
}

/// One selected name; its version does not confer authority.
#[derive(
    Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema, crate::HeapLayout,
)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct DirectoryEntry {
    /// Child name.
    pub name: String,
    /// Public tool path.
    pub path: String,
    /// Selected namespace kind.
    pub kind: DirectoryEntryKind,
    /// Opaque namespace version, also the exclusive continuation cursor.
    pub version: String,
}

/// A bounded directory page, without invented regular-file metadata.
#[derive(
    Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema, crate::HeapLayout,
)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct DirectoryReadResult {
    /// Ordered children.
    pub entries: Vec<DirectoryEntry>,
    /// Continue from the last returned entry's version.
    pub has_more: bool,
}

/// Canonical results for file and directory selections.
#[derive(Serialize, schemars::JsonSchema)]
#[serde(untagged)]
pub enum ReadResult {
    /// File bytes and their original source.
    File(ReadFileResult),
    /// Namespace entries at one immutable generation.
    Directory(DirectoryReadResult),
}

pub(super) fn validate_input(
    input: &ReadFileInput,
    limit: u32,
    work: crate::NativeWork<'_>,
) -> Result<()> {
    if input.range.is_some() || input.version.is_some() || limit == 0 {
        return Err(executor::rejection(
            work,
            "directory selection requires a positive page size and only its own continuation",
        ));
    }
    Ok(())
}

fn child_path(
    parent: &str,
    name: &str,
    work: crate::NativeWork<'_>,
) -> Result<(String, crate::NativeCustody)> {
    let separator = if parent.is_empty() { "" } else { "/" };
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        let (path, hold) = work.display(&format_args!("{parent}{separator}{name}"))?;
        return Ok((path, Some(hold)));
    }
    let _ = work;
    Ok((format!("{parent}{separator}{name}"), None))
}

fn public_parent(path: &str) -> &str {
    if path == "." {
        ""
    } else {
        path.trim_end_matches('/')
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Page<'a> {
    entries: Vec<Entry<'a>>,
    has_more: bool,
}

#[derive(Serialize)]
struct Entry<'a> {
    name: &'a str,
    path: String,
    kind: DirectoryEntryKind,
    version: String,
    #[serde(skip)]
    _holds: [crate::NativeCustody; 2],
}

pub(super) async fn read(
    context: &ToolContext,
    input: &ReadFileInput,
    after: Option<&str>,
    limit: u32,
    work: crate::NativeWork<'_>,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<(ToolResult, crate::NativeCustody)> {
    validate_input(input, limit, work)?;
    let (volume, path) = context.authorize_tool_directory_read(&input.path, io)?;
    let selected = after
        .map(|cursor| version::decode_selection(cursor, work))
        .transpose()?;
    let cursor = match &selected {
        Some((version::SelectedVersion::Authored(selected), _)) => {
            let (parent, name) = selected
                .path
                .rsplit_once('/')
                .unwrap_or(("", &selected.path));
            if &selected.volume != volume || parent != path || name.is_empty() {
                return Err(executor::rejection(
                    work,
                    "directory cursor belongs to another selection",
                ));
            }
            Some((&selected.generation, name))
        }
        Some(_) => {
            return Err(executor::rejection(
                work,
                "directory cursor requires a namespace selection",
            ));
        }
        None => None,
    };
    let (page, _page_hold) = context
        .list_tool_directory(
            crate::conversation::DirectoryRead {
                volume,
                granted_prefix: &path,
                path: &path,
                generation: cursor.map(|c| c.0),
                after: cursor.map(|c| c.1),
                maximum_entries: limit,
            },
            io,
        )
        .await?;
    let mut vector_hold = edit::allocation(work, std::mem::size_of::<Page<'_>>())?;
    let mut entries = Vec::new();
    edit::reserve_vec(&mut vector_hold, &mut entries, page.entries.len())?;
    for entry in &page.entries {
        let (relative, _relative_hold) = child_path(&path, &entry.name, work)?;
        let (version, version_hold) =
            encode_path_version(volume, &relative, &page.generation, work)?;
        let (public, public_hold) = child_path(public_parent(&input.path), &entry.name, work)?;
        entries.push(Entry {
            name: &entry.name,
            path: public,
            version,
            kind: match entry.kind {
                crate::conversation::PrivateDirectoryEntryKind::File => DirectoryEntryKind::File,
                crate::conversation::PrivateDirectoryEntryKind::Directory => {
                    DirectoryEntryKind::Dir
                }
            },
            _holds: [public_hold, version_hold],
        });
    }
    let encoded = value(
        &Page {
            entries,
            has_more: page.has_more,
        },
        context.effective_scope().limits().file_bytes,
        work,
    )?;
    Ok((ToolResult { value: encoded.0 }, encoded.1))
}

pub(super) fn project(
    input: &ReadFileInput,
    after: Option<&str>,
    limit: u32,
    result: &ToolResult,
    work: crate::NativeWork<'_>,
) -> Result<(Value, crate::NativeCustody)> {
    validate_input(input, limit, work)?;
    let (result, _hold) = decode::<DirectoryReadResult>(&result.value, work)?;
    if result.entries.len() > limit as usize || (result.has_more && result.entries.is_empty()) {
        return Err(executor::rejection(
            work,
            "directory result exceeds its selection or cannot advance",
        ));
    }
    let cursor = after
        .map(|cursor| version::decode_selection(cursor, work))
        .transpose()?;
    let mut previous = cursor
        .as_ref()
        .map(|(selected, _)| selected.path().rsplit('/').next().unwrap_or(""));
    for entry in &result.entries {
        if entry.name.is_empty()
            || entry.name.contains('/')
            || matches!(entry.name.as_str(), "." | "..")
            || previous.is_some_and(|previous| entry.name.as_bytes() <= previous.as_bytes())
        {
            return Err(executor::rejection(
                work,
                "directory result names are not ordered children",
            ));
        }
        let (expected, _hold) = child_path(public_parent(&input.path), &entry.name, work)?;
        if entry.path != expected {
            return Err(executor::rejection(
                work,
                "directory result belongs to another path",
            ));
        }
        previous = Some(&entry.name);
    }
    schema::project_json(&result, work)
}
