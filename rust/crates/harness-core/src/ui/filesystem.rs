//! The shared UI View served by the filesystem's existing Computed adapter.
use super::{Affected, Cut, Error, View};
use acyclic_fs::{WorkCounters, WorkError, demand::*, kernel::*};
use std::{io::Write, marker::PhantomData};

/// Immutable UI inputs plus the host's authorized navigation namespace.
/// Implementations prepare only the requested page and retain their original
/// decoded record/file owners. No method here performs IO or scans children.
pub trait FilesystemCut: Cut + Send + Sync + 'static {
    /// Stable identity of this agent's projection namespace.
    fn identity(&self) -> [u8; 16];
    /// Exact child lookup. A present target names that child's UI directory in
    /// the exported namespace, outside the project cwd. None is known absence.
    /// Target bytes use the bound volume's link encoding.
    fn child_ui(&self, id: &str) -> super::Result<Option<&[u8]>>;
}

/// Thin filesystem representation of the same renderer used by reader clients.
pub struct FilesystemView<C> {
    view: View,
    cut: PhantomData<fn() -> C>,
}
impl<C: FilesystemCut> FilesystemView<C> {
    /// Construction reads nothing and starts no follow or background work.
    pub fn new(view: View) -> Self {
        Self {
            view,
            cut: PhantomData,
        }
    }

    fn node(
        &self,
        cut: &C,
        path: &str,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<SourceNode>, DemandError> {
        let diagnostic_directory = path
            .strip_prefix("diagnostics/")
            .filter(|name| !name.contains('/'));
        if let Some(digest) = diagnostic_directory {
            let source = match super::project::diagnostic_source(cut) {
                Ok(source) => source,
                Err(Error::Invalid(_)) => return Ok(None),
                Err(cause) => return Err(error(cause)),
            };
            if digest != blake3::Hash::from_bytes(source.digest).to_hex().as_str() {
                return Ok(None);
            }
        }
        let (kind, length) = if directory(path) || diagnostic_directory.is_some() {
            (SourceNodeKind::Directory, None)
        } else if let Some(id) = child_id(path) {
            if cut.child_ui(id).map_err(error)?.is_none() {
                return Ok(None);
            }
            (SourceNodeKind::SymbolicLink, None)
        } else {
            let mut count = Count {
                length: 0,
                ctx,
                failure: None,
            };
            let result = self.view.write(cut, &path, &mut count);
            if let Some(error) = count.failure {
                return Err(error);
            }
            match result {
                Ok(()) => (SourceNodeKind::RegularFile, Some(count.length)),
                Err(Error::Invalid(_)) => return Ok(None),
                Err(cause) => return Err(error(cause)),
            }
        };
        ctx.charge(WorkCounters {
            bytes_hashed: (48 + path.len()) as u64,
            ..WorkCounters::UNCHARGED
        })?;
        let mut hash = blake3::Hasher::new();
        hash.update(&self.view.identity());
        hash.update(&cut.identity());
        hash.update(path.as_bytes());
        Ok(Some(SourceNode {
            kind,
            logical_bytes: length,
            file_identity: *hash.finalize().as_bytes(),
            link_count: Some(1),
            device: None,
            version: SourceVersion([0; 32]),
            metadata: SourceMetadata::default(),
        }))
    }
}

impl<C: FilesystemCut> acyclic_fs::demand::View for FilesystemView<C> {
    type Snapshot = C;
    type Record = Affected;
    fn identity(&self) -> [u8; 32] {
        self.view.identity()
    }
    fn lookup(
        &self,
        _: ViewCut,
        cut: &C,
        path: &NamespacePath,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<SourceNode>, DemandError> {
        let text = path.unicode_text(ctx)?;
        self.node(cut, &text[1..], ctx)
    }
    fn nodes(
        &self,
        _: ViewCut,
        cut: &C,
        directory: &NamespacePath,
        cursor: Option<SourceCursor>,
        maximum_entries: u32,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<SourceDirectoryPage, DemandError> {
        let text = directory.unicode_text(ctx)?;
        let dir = &text[1..];
        let mut page = SourceDirectoryPageBuilder::new(ctx)?;
        let next = self.view.visit_nodes::<DemandError>(
            cut,
            &dir,
            cursor.as_ref().map_or(0, SourceCursor::offset),
            maximum_entries,
            |entry| {
                let text = OwnedSourceChunk::produce(ctx, |sink| {
                    if dir.is_empty() {
                        write!(sink, "/{}", entry.name)
                    } else {
                        write!(sink, "/{dir}/{}", entry.name)
                    }
                    .map_err(DemandError::from)
                })?;
                let text = std::str::from_utf8(text.bytes.as_slice())
                    .map_err(|_| DemandError::InvalidRequest)?;
                let node = self
                    .node(cut, &text[1..], ctx)?
                    .ok_or(DemandError::StaleSource)?;
                page.push_utf8(entry.name, node, ctx)
            },
        )?;
        page.finish(
            cursor.as_ref().map_or(0, SourceCursor::offset),
            next.map(SourceCursor::position),
            SourceVersion([0; 32]),
            ctx,
        )
    }
    fn bytes(
        &self,
        _: ViewCut,
        cut: &C,
        path: &NamespacePath,
        offset: u64,
        length: u64,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<OwnedSourceChunk, DemandError> {
        let text = path.unicode_text(ctx)?;
        if directory(&text[1..]) || child_id(&text[1..]).is_some() {
            return Err(DemandError::NotRegularFile);
        }
        let start = usize::try_from(offset).unwrap_or(usize::MAX);
        let end = usize::try_from(offset.saturating_add(length)).unwrap_or(usize::MAX);
        OwnedSourceChunk::produce(ctx, |mut sink| {
            self.view
                .write_range(cut, &&text[1..], start..end, &mut sink)
                .map_err(error)
        })
    }
    fn link(
        &self,
        _: ViewCut,
        cut: &C,
        path: &NamespacePath,
        maximum_bytes: u64,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<OwnedSourceChunk, DemandError> {
        let text = path.unicode_text(ctx)?;
        let id = child_id(&text[1..]).ok_or(DemandError::InvalidRequest)?;
        let target = cut
            .child_ui(id)
            .map_err(error)?
            .ok_or(DemandError::Absent)?;
        if target.len() as u64 > maximum_bytes {
            return Err(DemandError::InvalidRequest);
        }
        OwnedSourceChunk::produce(ctx, |sink| {
            sink.write_all(target).map_err(DemandError::from)
        })
    }
    fn affected(
        &self,
        record: &Affected,
        sink: &dyn SourceChangeSink,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), DemandError> {
        for relative in self.view.affected(*record) {
            let text = OwnedSourceChunk::produce(ctx, |sink| {
                write!(sink, "/{relative}").map_err(DemandError::from)
            })?;
            let text = std::str::from_utf8(text.bytes.as_slice())
                .map_err(|_| DemandError::InvalidRequest)?;
            let cancel = ctx.cancellation();
            let (path, mut allocation) = NamespacePath::parse_in_profile(
                text,
                acyclic_fs::FilesystemProfile::Portable,
                acyclic_fs::VolumeLimits::default(),
                ctx,
                cancel,
            )?;
            let bytes = path.logical_owned_bytes();
            let changes = [if directory(relative) {
                SourceChange::Name(path)
            } else {
                SourceChange::Entry(path)
            }];
            let result = sink.source_changed(&changes, ctx);
            drop(changes);
            ctx.release_owned(&mut allocation, bytes)?;
            result?;
        }
        Ok(())
    }
}

fn child_id(path: &str) -> Option<&str> {
    path.strip_prefix("children/")
        .filter(|id| !id.contains('/') && *id != "pages")
}
fn directory(path: &str) -> bool {
    matches!(
        path,
        "" | "turns" | "children" | "children/pages" | "diagnostics"
    )
}
fn error(cause: Error) -> DemandError {
    match cause {
        Error::Pending => DemandError::SourceUnavailable,
        Error::Output(error) => DemandError::Io(error),
        Error::Capacity | Error::Invalid(_) | Error::Json(_) | Error::Agent(_) => {
            DemandError::InvalidRequest
        }
    }
}
impl From<Error> for DemandError {
    fn from(cause: Error) -> Self {
        error(cause)
    }
}

struct Count<'a, 'w> {
    length: u64,
    ctx: &'a mut KernelAccounting<'w>,
    failure: Option<DemandError>,
}
impl Write for Count<'_, '_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.failure.is_none() {
            let result = self
                .ctx
                .charge(WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                })
                .map_err(DemandError::from)
                .and_then(|()| {
                    self.length = self
                        .length
                        .checked_add(bytes.len() as u64)
                        .ok_or(WorkError::Overflow)?;
                    Ok(())
                });
            self.failure = result.err();
        }
        if self.failure.is_some() {
            return Err(std::io::ErrorKind::Other.into());
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(all(test, feature = "test-support", not(target_arch = "wasm32")))]
mod tests;
