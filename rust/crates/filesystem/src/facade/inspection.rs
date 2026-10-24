//! Exact host inspection with disposable objects and no publication authority.
use super::*;
use crate::{CaptureOptions, CapturePolicy, Generation, GitDiffCounts, GitIgnorePolicy};
use std::collections::BTreeSet;

mod objects;
use objects::{ReadAuthority, TransientObjects};
mod tree;
pub use tree::ReadOnlyGeneration;

/// Physical view inspected without publishing its contents.
#[derive(Clone, Copy)]
pub struct NativeGitView<'a> {
    /// Held checkout identity and bounded capture inputs.
    pub options: &'a CaptureOptions,
    /// Paths owned by the host rather than the repository.
    pub policy: &'a CapturePolicy,
    /// Whether SDK materialization supplied the physical view's metadata.
    pub materialized: bool,
}

/// Read-only native inspection failure.
#[derive(Debug, thiserror::Error)]
pub enum NativeGitInspectionError {
    /// Physical source changed or could not be captured.
    #[error(transparent)]
    Capture(#[from] crate::CaptureError),
    /// Authenticated workspace could not be read.
    #[error(transparent)]
    Workspace(#[from] crate::WorkspaceError),
    /// Private candidate could not be constructed.
    #[error(transparent)]
    Filesystem(#[from] crate::FsError),
    /// Git policy could not be evaluated.
    #[error(transparent)]
    Git(#[from] crate::GitCaptureError),
}

fn rebind<A, O, R, T>(
    workspace: &crate::Workspace<A, O>,
    filesystem: &Fs<R, T>,
) -> crate::Workspace<R, T> {
    crate::Workspace {
        name: workspace.name.clone(),
        id: workspace.id,
        access: workspace.access.clone(),
        volume: Volume {
            fs: filesystem.clone(),
            id: workspace.volume.id,
            config: workspace.volume.config,
        },
        #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
        source: None,
    }
}

fn private_fs<'a, A: AsyncAuthorityStore, O: AsyncObjectStore>(
    original: &'a Fs<A, O>,
    work: &crate::NativeMaterializationWork,
) -> Result<Fs<ReadAuthority<'a, A>, TransientObjects<'a, O>>, crate::FsError> {
    let layout = std::alloc::Layout::new::<[std::sync::atomic::AtomicUsize; 2]>()
        .extend(std::alloc::Layout::new::<
            super::fs::FsInner<ReadAuthority<'a, A>, TransientObjects<'a, O>>,
        >())
        .map_err(|_| WorkError::Overflow)?
        .0
        .pad_to_align();
    let allocation = work.claim(layout.size() as u64, 1, WorkCounters::UNCHARGED)?;
    Ok(Fs::new(
        ReadAuthority(original.authority()),
        TransientObjects::new(original.objects(), allocation),
        EmbeddedCapabilities::MEMORY,
    ))
}
