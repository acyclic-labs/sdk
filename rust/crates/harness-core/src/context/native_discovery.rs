//! Thin lazy-view directory adapter for the shared discovery traversal.
use crate::{
    Result,
    context::{DiscoveryEntry, DiscoveryKind, DiscoveryPage, DiscoverySource},
};
use acyclic_fs::{
    LazyDirectoryCursor, LazySnapshotRef, NativeMaterializationWork, kernel::KernelAccounting,
};
use acyclic_stream::BoxProviderFuture;

#[doc(hidden)]
pub type DiscoveryView = acyclic_fs::LazyWorkspace<
    acyclic_fs::LocalAuthorityBackend,
    acyclic_fs::LocalObjectBackend,
    acyclic_fs::demand::native::NativeDemandSource,
    acyclic_fs::LocalCoreStateStore,
>;

/// The cut and original work are owned by the admission, never ambient host state.
#[doc(hidden)]
pub struct LazyDiscovery<'a> {
    pub view: &'a DiscoveryView,
    pub cut: LazySnapshotRef,
    pub work: &'a NativeMaterializationWork,
}

impl DiscoverySource for LazyDiscovery<'_> {
    type Revision = LazySnapshotRef;
    type Cursor = LazyDirectoryCursor;
    fn page<'a>(
        &'a mut self,
        directory: &'a str,
        revision: Option<&'a LazySnapshotRef>,
        cursor: Option<LazyDirectoryCursor>,
        maximum: u32,
    ) -> BoxProviderFuture<'a, Result<DiscoveryPage<LazySnapshotRef, LazyDirectoryCursor>>> {
        Box::pin(async move {
            let cut = revision.copied().unwrap_or(self.cut);
            let mut accounting = KernelAccounting::NativeOriginal {
                work: self.work,
                cancellation: self.work.cancellation(),
            };
            let (page, _, _allocation) = self
                .view
                .list_directory_at_snapshot(
                    cut,
                    &format!("/{directory}"),
                    cursor,
                    maximum,
                    &mut accounting,
                )
                .await
                .map_err(|error| match error {
                    acyclic_fs::LazyWorkspaceError::NotFound
                    | acyclic_fs::LazyWorkspaceError::Filesystem(acyclic_fs::FsError::NotFound) => {
                        crate::Error::NotFound(directory.into())
                    }
                    error => error.into(),
                })?;
            let mut entries = Vec::with_capacity(page.entries.len());
            for entry in page.entries {
                let (name, _allocation) = entry
                    .name
                    .unicode_text(self.work, self.work.cancellation())
                    .map_err(|e| crate::Error::Storage(e.to_string()))?;
                entries.push(DiscoveryEntry {
                    name,
                    kind: entry.kind.into(),
                });
            }
            Ok(DiscoveryPage {
                revision: cut,
                entries,
                next: page.next,
            })
        })
    }
}

impl From<acyclic_fs::demand::SourceNodeKind> for DiscoveryKind {
    fn from(kind: acyclic_fs::demand::SourceNodeKind) -> Self {
        match kind {
            acyclic_fs::demand::SourceNodeKind::RegularFile => Self::File,
            acyclic_fs::demand::SourceNodeKind::Directory => Self::Directory,
            _ => Self::Other,
        }
    }
}
