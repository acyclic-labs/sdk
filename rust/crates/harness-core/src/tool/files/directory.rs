//! Directory selections preserve provider continuations on the authenticated reader.
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
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum DirectoryEntryKind {
    /// Regular file.
    File,
    /// Child directory.
    Dir,
    /// Symbolic link, without following its target.
    Symlink,
    Fifo,
    Socket,
    CharacterDevice,
    BlockDevice,
    ReparsePoint,
    MountBoundary,
    Unsupported,
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
    /// Opaque namespace version for reading this entry.
    pub version: String,
}

/// A bounded directory page, without invented regular-file metadata.
#[derive(
    Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema, crate::HeapLayout,
)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct DirectoryReadResult {
    /// Physical candidates consumed before policy filtering.
    pub examined: u32,
    /// Ordered children.
    pub entries: Vec<DirectoryEntry>,
    /// Opaque continuation for this directory at the same immutable selection.
    pub next: Option<String>,
}

/// Canonical results for file and directory selections.
#[derive(Serialize, schemars::JsonSchema)]
#[serde(untagged)]
pub enum ReadResult {
    /// A bounded immutable host file query.
    Diagnostic(DiagnosticReadResult),
    /// File bytes and their original source.
    File(ReadFileResult),
    /// Namespace entries at one immutable generation.
    Directory(DirectoryReadResult),
}

pub(super) fn validate_input(
    input: &ReadFileInput,
    limit: u32,
    work: acyclic_fs::NativeWork<'_>,
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
    work: acyclic_fs::NativeWork<'_>,
) -> Result<(String, acyclic_fs::NativeCustody)> {
    let separator = if parent.is_empty() { "" } else { "/" };
    #[cfg(not(target_arch = "wasm32"))]
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
    examined: u32,
    entries: Vec<Entry<'a>>,
    next: Option<String>,
    #[serde(skip)]
    _next_hold: acyclic_fs::NativeCustody,
}

#[derive(Serialize)]
struct Entry<'a> {
    name: &'a str,
    path: String,
    kind: DirectoryEntryKind,
    version: String,
    #[serde(skip)]
    _holds: [acyclic_fs::NativeCustody; 2],
}

pub(super) async fn read(
    context: &ToolContext,
    input: &ReadFileInput,
    after: Option<&str>,
    limit: u32,
    work: acyclic_fs::NativeWork<'_>,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<(ToolResult, acyclic_fs::NativeCustody)> {
    validate_input(input, limit, work)?;
    let (volume, path) = context.authorize_tool_directory_read(&input.path, io)?;
    let selected = after
        .map(|cursor| decode_cursor(cursor, work))
        .transpose()?;
    if let Some((cursor, _)) = &selected {
        if &cursor.volume != volume || cursor.path != path {
            return Err(executor::rejection(
                work,
                "directory cursor belongs to another selection",
            ));
        }
    }
    let (page, _page_hold) = context
        .list_tool_directory(
            crate::conversation::DirectoryRead {
                volume,
                granted_prefix: &path,
                path: &path,
                generation: selected.as_ref().map(|(cursor, _)| &cursor.generation),
                after: selected.as_ref().map(|(cursor, _)| cursor.after.as_slice()),
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
                crate::conversation::PrivateDirectoryEntryKind::SymbolicLink => {
                    DirectoryEntryKind::Symlink
                }
                crate::conversation::PrivateDirectoryEntryKind::Fifo => DirectoryEntryKind::Fifo,
                crate::conversation::PrivateDirectoryEntryKind::Socket => {
                    DirectoryEntryKind::Socket
                }
                crate::conversation::PrivateDirectoryEntryKind::CharacterDevice => {
                    DirectoryEntryKind::CharacterDevice
                }
                crate::conversation::PrivateDirectoryEntryKind::BlockDevice => {
                    DirectoryEntryKind::BlockDevice
                }
                crate::conversation::PrivateDirectoryEntryKind::ReparsePoint => {
                    DirectoryEntryKind::ReparsePoint
                }
                crate::conversation::PrivateDirectoryEntryKind::MountBoundary => {
                    DirectoryEntryKind::MountBoundary
                }
                crate::conversation::PrivateDirectoryEntryKind::Unsupported => {
                    DirectoryEntryKind::Unsupported
                }
            },
            _holds: [public_hold, version_hold],
        });
    }
    let (next, next_hold) = match &page.next {
        Some(after) => {
            let (text, hold) = version::encode_token(
                &DirectoryCursorView {
                    selection: crate::conversation::PathSelectionView {
                        volume,
                        path: &path,
                        generation: &page.generation,
                    },
                    after,
                },
                DIRECTORY,
                work,
            )?;
            (Some(text), hold)
        }
        None => (None, None),
    };
    let encoded = value(
        &Page {
            examined: page.examined,
            entries,
            next,
            _next_hold: next_hold,
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
    work: acyclic_fs::NativeWork<'_>,
) -> Result<(Value, acyclic_fs::NativeCustody)> {
    validate_input(input, limit, work)?;
    let (result, _hold) = decode::<DirectoryReadResult>(&result.value, work)?;
    if result.examined > limit || result.entries.len() > result.examined as usize {
        return Err(executor::rejection(
            work,
            "directory result exceeds its selection or cannot advance",
        ));
    }
    if let Some(after) = after {
        decode_cursor(after, work)?;
    }
    if let Some(next) = &result.next {
        decode_cursor(next, work)?;
    }
    let cancellation = acyclic_fs::CancellationToken::new();
    #[cfg(not(target_arch = "wasm32"))]
    let ledger = match work {
        Some(work) => crate::executor::PreparationIo::from_native_work(work)?,
        None => crate::executor::PreparationIo::for_unmeasured(&cancellation)?,
    };
    #[cfg(target_arch = "wasm32")]
    let ledger = crate::executor::PreparationIo::for_unmeasured(&cancellation)?;
    if !ledger.account(|ctx| {
        acyclic_fs::kernel::all_unique_by_with_accounting(
            result.entries.iter().map(|entry| entry.name.as_str()),
            ctx,
            |left, right| left.cmp(right),
        )
    })? {
        return Err(executor::rejection(
            work,
            "directory result contains duplicate names",
        ));
    }
    for entry in &result.entries {
        if entry.name.is_empty()
            || entry.name.contains('/')
            || matches!(entry.name.as_str(), "." | "..")
        {
            return Err(executor::rejection(
                work,
                "directory result names are not children",
            ));
        }
        let (expected, _hold) = child_path(public_parent(&input.path), &entry.name, work)?;
        if entry.path != expected {
            return Err(executor::rejection(
                work,
                "directory result belongs to another path",
            ));
        }
    }
    schema::project_json(&result, work)
}

const DIRECTORY: &str = "directory-v1:";

#[derive(Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
struct DirectoryCursor {
    volume: crate::conversation::VolumeRef,
    path: String,
    generation: crate::resources::GenerationRef,
    after: Vec<u8>,
}

#[derive(Serialize)]
struct DirectoryCursorView<'a> {
    #[serde(flatten)]
    selection: crate::conversation::PathSelectionView<'a>,
    after: &'a [u8],
}

impl crate::filesystem::PreparationRecord for DirectoryCursor {
    fn diagnostic_label_bytes() -> u64 {
        crate::tool::TOOL_SERDE_DIAGNOSTIC_LABEL_BYTES
    }
}

fn decode_cursor(
    text: &str,
    work: acyclic_fs::NativeWork<'_>,
) -> Result<(DirectoryCursor, acyclic_fs::NativeCustody)> {
    let bytes = text
        .strip_prefix(DIRECTORY)
        .ok_or_else(|| executor::rejection(work, "invalid directory continuation"))?;
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(work) = work {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let (cursor, mut reservation) = io.decode::<DirectoryCursor>(
            std::marker::PhantomData,
            bytes.as_bytes(),
            crate::wire::ErrorCode::Invalid,
        )?;
        let mut history = io.history_io();
        cursor.validate(&mut Some(&mut history))?;
        let retained = crate::core::owned_sum([
            std::mem::size_of::<DirectoryCursor>() as u64,
            cursor.volume.logical_owned_bytes()?,
            cursor.path.capacity() as u64,
            cursor.generation.logical_owned_bytes()?,
            cursor.after.capacity() as u64,
        ])?;
        reservation.substitute(retained)?;
        reservation.into_retained()?;
        return Ok((cursor, Some(io.into_native_custody(retained)?)));
    }
    let cursor: DirectoryCursor =
        serde_json::from_str(bytes).map_err(|error| Error::Invalid(error.to_string()))?;
    cursor.validate(&mut None)?;
    Ok((cursor, None))
}

impl DirectoryCursor {
    fn validate(&self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>) -> Result<()> {
        self.volume.validate(work)?;
        self.generation.validate(work)?;
        if !self.path.is_empty() {
            crate::conversation::validate_content_path(&self.path, work)?;
        }
        if self.after.is_empty()
            || self.generation.as_resource().provider() != self.volume.provider()
        {
            return Err(crate::store::guarded_boundary(
                work,
                crate::wire::ErrorCode::Invalid,
                "directory continuation belongs to an invalid namespace",
            ));
        }
        Ok(())
    }
}
