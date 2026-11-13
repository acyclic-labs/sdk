//! The file-tree adapter over the existing generation-bound content reader.
use super::*;
use crate::{
    context::ContextRoot,
    conversation::{ContentAccess, FilePathSelection, PrivateDirectoryEntryKind},
    resources::GenerationRef,
};
use std::num::NonZeroU32;

pub use crate::tool::ReadPathKind as FileRootKind;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Directory {
    path: String,
    after: Option<String>,
    done: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Position {
    path: String,
    offset: u64,
}

/// A replayable file search at one immutable generation, retaining only a
/// depth-first frontier and one file position. It conveys no read authority.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileCursor {
    version: u32,
    root: ContextRoot,
    path: String,
    kind: FileRootKind,
    pattern: Vec<u8>,
    generation: Option<GenerationRef>,
    directories: Vec<Directory>,
    pending: Option<Position>,
}

/// An address in the original file, without hashing or copying its whole body.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileMatch {
    /// Exact source generation, path and original length, accepted by the reader.
    pub file: FilePathSelection,
    /// First matching byte in this file.
    pub offset: u64,
}

/// One bounded unit of file search work, including nonmatching source ranges.
pub struct FilePage {
    /// Original file addresses, at most one per matching file.
    pub matches: Vec<FileMatch>,
    /// Directory observations and source ranges consumed.
    pub work: u32,
    /// Absent only after the selected tree is exhausted.
    pub next: Option<FileCursor>,
}

fn within(root: &str, path: &str) -> bool {
    root.is_empty()
        || path == root
        || path
            .strip_prefix(root)
            .is_some_and(|rest| rest.starts_with('/'))
}

impl FileCursor {
    /// Select a declared root and literal bytes without reading the namespace.
    pub fn new(
        root: ContextRoot,
        path: String,
        kind: FileRootKind,
        pattern: Vec<u8>,
    ) -> Result<Self> {
        let mut cursor = Self {
            version: 1,
            root,
            path: path.clone(),
            kind,
            pattern,
            generation: None,
            directories: Vec::new(),
            pending: None,
        };
        match kind {
            FileRootKind::Directory => cursor.directories.push(Directory {
                path,
                after: None,
                done: false,
            }),
            FileRootKind::File => cursor.pending = Some(Position { path, offset: 0 }),
        }
        cursor.validate()?;
        Ok(cursor)
    }

    /// Selection bound into this cursor, for the original facade to recheck.
    pub fn selection(&self) -> (&ContextRoot, &str, FileRootKind, &[u8]) {
        (&self.root, &self.path, self.kind, &self.pattern)
    }

    fn validate(&self) -> Result<()> {
        self.root.validate(&mut None)?;
        let valid = |path: &str| {
            within(&self.root.directory, path)
                && within(&self.path, path)
                && (path.is_empty()
                    || crate::conversation::validate_content_path(path, &mut None).is_ok())
        };
        if self.version != 1
            || (self.kind == FileRootKind::File && self.path.is_empty())
            || !valid(&self.path)
            || self.directories.iter().any(|directory| {
                !valid(&directory.path)
                    || directory
                        .after
                        .as_ref()
                        .is_some_and(|token| token.is_empty())
            })
            || self
                .pending
                .as_ref()
                .is_some_and(|pending| !valid(&pending.path))
            || (self.kind == FileRootKind::File
                && (!self.directories.is_empty()
                    || self
                        .pending
                        .as_ref()
                        .is_some_and(|pending| pending.path != self.path)))
        {
            return Err(Error::Invalid(
                "file search cursor differs from its root".into(),
            ));
        }
        Ok(())
    }

    /// Read only one caller-budgeted page through the original content reader.
    /// All source data and namespace observations share the opening generation;
    /// current authorization still guards every range. Cancellation leaves the
    /// input cursor reusable and acknowledges no partial page.
    pub async fn page(
        &self,
        reader: &dyn ContentResidencyVerifier,
        ceiling: Option<&Capabilities>,
        budget: NonZeroU32,
    ) -> Result<FilePage> {
        self.validate()?;
        let mut cursor = self.clone();
        let mut matches = Vec::new();
        let mut work = 0;
        while work < budget.get() {
            if let Some(position) = cursor.pending.take() {
                cursor
                    .range(reader, ceiling, position, &mut matches)
                    .await?;
                work += 1;
                continue;
            }
            let Some(directory) = cursor.directories.last_mut() else {
                break;
            };
            if directory.done {
                cursor.directories.pop();
                continue;
            }
            let page = reader
                .list_private_directory(
                    &cursor.root.volume,
                    &cursor.root.directory,
                    &directory.path,
                    cursor.generation.as_ref(),
                    directory.after.as_deref(),
                    1,
                    ceiling,
                    None,
                )?
                .await?;
            work += 1;
            let Some((page, _hold)) = page else {
                cursor.directories.pop();
                continue;
            };
            page.validate()?;
            if page.examined > 1 || page.entries.len() > 1 {
                return Err(Error::Storage(
                    "file search directory exceeded its page".into(),
                ));
            }
            if cursor
                .generation
                .as_ref()
                .is_some_and(|generation| *generation != page.generation)
            {
                return Err(Error::Conflict("file search generation changed".into()));
            }
            cursor.generation = Some(page.generation);
            directory.done = !page.has_more;
            directory.after = page.next;
            if let Some(entry) = page.entries.into_iter().next() {
                let path = if directory.path.is_empty() {
                    entry.name
                } else {
                    format!("{}/{}", directory.path, entry.name)
                };
                match entry.kind {
                    PrivateDirectoryEntryKind::Directory => cursor.directories.push(Directory {
                        path,
                        after: None,
                        done: false,
                    }),
                    PrivateDirectoryEntryKind::File => {
                        cursor.pending = Some(Position { path, offset: 0 })
                    }
                }
            }
        }
        let next = (!cursor.directories.is_empty() || cursor.pending.is_some()).then_some(cursor);
        Ok(FilePage {
            matches,
            work,
            next,
        })
    }

    async fn range(
        &mut self,
        reader: &dyn ContentResidencyVerifier,
        ceiling: Option<&Capabilities>,
        position: Position,
        matches: &mut Vec<FileMatch>,
    ) -> Result<()> {
        let requested = source_window(position.offset, &self.pattern);
        let read = reader.read_private_range(
            &self.root.volume,
            &self.root.directory,
            &position.path,
            self.generation.as_ref(),
            requested,
            ContentAccess { ceiling, io: None },
        )?;
        let (file, bytes, _hold) = match read.await {
            Ok(read) => read,
            Err(error) if error.code() == crate::wire::ErrorCode::Unauthorized => return Ok(()),
            Err(error) => return Err(error),
        };
        if self
            .generation
            .as_ref()
            .is_some_and(|generation| *generation != file.generation)
        {
            return Err(Error::Conflict("file search generation changed".into()));
        }
        self.generation = Some(file.generation.clone());
        match inspect_window(
            file.byte_length,
            position.offset,
            &self.pattern,
            bytes.as_slice(),
            None,
        )? {
            Window::Match(offset) => matches.push(FileMatch { file, offset }),
            Window::More(offset) => {
                self.pending = Some(Position {
                    path: position.path,
                    offset,
                })
            }
            Window::Done => {}
        }
        Ok(())
    }
}
