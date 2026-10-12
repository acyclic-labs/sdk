//! Native projection of one demand-backed lazy workspace.

use super::{adapter, NativeMountError, LazyMount};
use super::adapter::{CallbackRuntime, CheckoutCandidate, CheckoutDetachedFile, InstallScope}

;

use super::view_gate::{
    ViewGate as SourceViewGate, ViewReadLease as SourceViewLease,
    ViewWriteLease as SourceMutationLease,
}

;

use super::view_ledger::{ViewChange, ViewEffect, ViewStamp}

;

use super::{
    CheckoutMountSource, ContentSink, MountAttributePage, MountAttributeWriteMode, MountContentPin,
    MountDirectoryEntry, MountDirectoryPage, MountFilesystem, MountLookup, MountNode,
    MountNodeKind, MountOpenFile, MountPath, MountRangeAllocation, MountSeekTarget,
    MountSourceError, ViewObserver, capture_root_identity,
}

;

use crate::LazySeekTarget;

use crate::demand::{
    DemandError, DemandFile, DemandSource, SourceChange, SourceChangeSink, SourceNode,
    SourceNodeKind, SourceReference, SourceWatch,
}

;

use crate::heap_future::in_heap;

use crate::kernel::{
    FileKind, FileMetadata, FileMutation, FilePayload, FileRecord, MetadataField, Mutation,
}

;

use crate::lazy_workspace::logical_child_path;

use crate::{
    AsyncAuthorityStore, AsyncObjectStore, FileId, IdempotencyKey, LazyDirectoryCursor, LazyLookup,
    LazyWorkspace, LazyWorkspaceError, LazyWorkspaceStore, NativeRootIdentity, WorkspaceMetadata,
}

;

use bytes::Bytes;

use std::collections::{BTreeMap, HashMap}

;

use std::path::Path;

use std::sync::atomic::{AtomicU64, Ordering}

;

use std::sync::{Arc, Mutex, PoisonError, Weak}

;

mod state;
mod budget;
pub(super) use budget::*;

mod watch;
mod watch_owner;
pub use watch_owner::NativeMountSourceWatchOwner;

mod init;

mod publication;

mod promotion;

mod queries;

mod directory;

mod mutations;

mod callbacks;

mod content;

mod metadata;

mod handles;

use state::*;

use watch::*;

use metadata::*;

use promotion::*;

use content::*;

use handles::*;

const MAXIMUM_PROMOTION_BYTES: u64 = u64::MAX;

const MAXIMUM_LAZY_DIRECTORY_CURSORS: usize = 1_024;

/// Entries listed per page while promoting a subtree.
const DIRECTORY_PROMOTION_PAGE: u32 = 256;

const MAXIMUM_REMEMBERED_RESOLUTIONS: usize = 65_536;

const MAXIMUM_PROMOTION_RETRIES: usize = 3;

/// First-page listings of one directory before an interrupting change is
const MAXIMUM_INTERRUPTED_LISTINGS: usize = 3;

/// One native callback adapter over a source-backed sparse workspace.
pub struct LazyMountSource<A, O, D, S> {
    lazy: Arc<LazyWorkspace<A, O, D, S>>,
    authored: Arc<CheckoutMountSource<A, O>>,
    root: String,
    runtime: Arc<CallbackRuntime>,
    resolutions: Resolutions,
    /// Directories the source was found without, each with the stamp
    source_absent: SourceAbsentDirectories,
    /// Directories the checkout was found to hold, each with the stamp
    authored_directories: Mutex<HashMap<MountPath, ViewStamp>>,
    unstaged: UnstagedIdentities,
    cursors: CursorTable<(ViewStamp, LazyDirectoryCursor)>,
    source_view: Arc<SourceViewGate>,
    removals: Mutex<MountedRemovals>,
    detached: DetachedIdentities<A, O>,
    open_sources: OpenIdentityHandles,
    /// The source's reports of its own changes; `None` when it cannot make
    source_watch: Option<Box<dyn SourceWatch>>,
    /// Source nodes seen with more than one name, which a host reports a
    linked: Mutex<std::collections::HashSet<FileId>>,
}

#[cfg(test)]
mod tests;
