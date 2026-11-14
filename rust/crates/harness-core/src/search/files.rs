//! The file-tree adapter over the existing generation-bound content reader.
use super::*;
use crate::executor::{
    PreparationIo, stock_boundary, stock_string_copy, stock_string_push, stock_vec_push,
};
use crate::{
    context::ContextRoot,
    conversation::{ContentAccess, FilePathSelection, PrivateDirectoryEntryKind},
    resources::GenerationRef,
};
use std::num::NonZeroU32;
mod ownership;
use ownership::{generation, retire};

pub use crate::tool::ReadPathKind as FileRootKind;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Directory {
    path: String,
    after: Option<Vec<u8>>,
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
        Self::new_using(root, path, kind, pattern, None)
    }

    fn new_using(
        root: ContextRoot,
        path: String,
        kind: FileRootKind,
        pattern: Vec<u8>,
        io: Option<&PreparationIo>,
    ) -> Result<Self> {
        let mut cursor = Self {
            version: 1,
            root,
            path: stock_string_copy(&path, io)?,
            kind,
            pattern,
            generation: None,
            directories: Vec::new(),
            pending: None,
        };
        match kind {
            FileRootKind::Directory => stock_vec_push(
                &mut cursor.directories,
                Directory {
                    path,
                    after: None,
                    done: false,
                },
                io,
            )?,
            FileRootKind::File => cursor.pending = Some(Position { path, offset: 0 }),
        }
        cursor.validate_with_io(io)?;
        Ok(cursor)
    }

    /// Selection bound into this cursor, for the original facade to recheck.
    pub fn selection(&self) -> (&ContextRoot, &str, FileRootKind, &[u8]) {
        (&self.root, &self.path, self.kind, &self.pattern)
    }

    fn validate_with_io(&self, io: Option<&PreparationIo>) -> Result<()> {
        crate::executor::with_history_io(io, |work| self.root.validate(work))?;
        let valid = |path: &str| -> Result<bool> {
            if !within(&self.root.directory, path) || !within(&self.path, path) {
                return Ok(false);
            }
            if !path.is_empty() {
                crate::executor::with_history_io(io, |work| {
                    crate::conversation::validate_content_path(path, work)
                })?;
            }
            Ok(true)
        };
        let invalid = || {
            stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "file search cursor differs from its root",
            )
        };
        if self.version != 1
            || (self.kind == FileRootKind::File && self.path.is_empty())
            || !valid(&self.path)?
        {
            return Err(invalid());
        }
        for directory in &self.directories {
            if !valid(&directory.path)? || directory.after.as_ref().is_some_and(Vec::is_empty) {
                return Err(invalid());
            }
        }
        if let Some(pending) = &self.pending
            && !valid(&pending.path)?
        {
            return Err(invalid());
        }
        if self.kind == FileRootKind::File
            && (!self.directories.is_empty()
                || self
                    .pending
                    .as_ref()
                    .is_some_and(|pending| pending.path != self.path))
        {
            return Err(invalid());
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
        self.page_using(reader, ceiling, budget, None).await
    }

    async fn page_using(
        &self,
        reader: &dyn ContentResidencyVerifier,
        ceiling: Option<&Capabilities>,
        budget: NonZeroU32,
        io: Option<&PreparationIo>,
    ) -> Result<FilePage> {
        self.validate_with_io(io)?;
        let mut cursor = self.copy_with_io(io)?;
        let mut matches = Vec::new();
        let mut work = 0;
        while work < budget.get() {
            if let Some(position) = cursor.pending.take() {
                cursor
                    .range(reader, ceiling, position, &mut matches, io)
                    .await?;
                work += 1;
                continue;
            }
            let Some(directory) = cursor.directories.last_mut() else {
                break;
            };
            if directory.done {
                retire(cursor.directories.pop(), io)?;
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
                    io,
                )?
                .await?;
            work += 1;
            let Some(read) = page else {
                retire(cursor.directories.pop(), io)?;
                continue;
            };
            let (page, _) = &read;
            page.validate_with_io(io)?;
            if page.examined > 1 || page.entries.len() > 1 {
                return Err(stock_boundary(
                    io,
                    crate::wire::ErrorCode::Storage,
                    "file search directory exceeded its page",
                ));
            }
            if cursor
                .generation
                .as_ref()
                .is_some_and(|generation| *generation != page.generation)
            {
                return Err(stock_boundary(
                    io,
                    crate::wire::ErrorCode::Conflict,
                    "file search generation changed",
                ));
            }
            if cursor.generation.is_none() {
                cursor.generation = Some(generation(&page.generation, io)?);
            }
            directory.done = page.next.is_none();
            retire(directory.after.take(), io)?;
            directory.after = page
                .next
                .as_ref()
                .map(|token| {
                    crate::executor::with_history_io(io, |work| {
                        crate::wire_codec::CodecQuota::produce(work, |quota| {
                            crate::core::copy_copy_vector(token, quota)
                        })
                        .map(|(value, _)| value)
                    })
                })
                .transpose()?;
            if let Some(entry) = page.entries.first() {
                let mut path = stock_string_copy(&directory.path, io)?;
                if !path.is_empty() {
                    stock_string_push(&mut path, "/", None, "search path size overflow", io)?;
                }
                stock_string_push(
                    &mut path,
                    &entry.name,
                    None,
                    "search path size overflow",
                    io,
                )?;
                match entry.kind {
                    PrivateDirectoryEntryKind::Directory => stock_vec_push(
                        &mut cursor.directories,
                        Directory {
                            path,
                            after: None,
                            done: false,
                        },
                        io,
                    )?,
                    PrivateDirectoryEntryKind::File => {
                        cursor.pending = Some(Position { path, offset: 0 })
                    }
                }
            }
        }
        let next = if !cursor.directories.is_empty() || cursor.pending.is_some() {
            Some(cursor)
        } else {
            retire(cursor, io)?;
            None
        };
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
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let requested = source_window(position.offset, &self.pattern);
        let read = reader.read_private_range(
            &self.root.volume,
            &self.root.directory,
            &position.path,
            self.generation.as_ref(),
            requested,
            ContentAccess { ceiling, io },
        )?;
        let read = match read.await {
            Ok(read) => read,
            Err(error) if error.code() == crate::wire::ErrorCode::Unauthorized => {
                retire(position, io)?;
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let (file, bytes, _) = &read;
        if self
            .generation
            .as_ref()
            .is_some_and(|generation| *generation != file.generation)
        {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "file search generation changed",
            ));
        }
        if self.generation.is_none() {
            self.generation = Some(generation(&file.generation, io)?);
        }
        match inspect_window(
            file.byte_length,
            position.offset,
            &self.pattern,
            bytes.as_slice(),
            io,
        )? {
            Window::Match(offset) => {
                let (file, bytes, hold) = read;
                if let Some(hold) = hold {
                    hold.into_retained()?;
                }
                stock_vec_push(matches, FileMatch { file, offset }, io)?;
                drop(bytes);
                retire(position, io)?;
            }
            Window::More(offset) => {
                drop(read);
                self.pending = Some(Position {
                    path: position.path,
                    offset,
                })
            }
            Window::Done => {
                drop(read);
                retire(position, io)?;
            }
        }
        Ok(())
    }
}
