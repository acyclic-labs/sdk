//! Native projection of one demand-backed lazy workspace.

use super::adapter::{CallbackRuntime, CheckoutCandidate, CheckoutDetachedFile, InstallScope};
use super::view_gate::{
    ViewGate as SourceViewGate, ViewMutationPhase as SourceMutationLease,
    ViewReadLease as SourceViewLease,
};
use super::view_ledger::{ViewChange, ViewEffect, ViewStamp};
use super::{
    CheckoutMountSource, ContentSink, MountAttributePage, MountAttributeWriteMode, MountContentPin,
    MountDirectoryEntry, MountDirectoryPage, MountFilesystem, MountLookup, MountNode,
    MountNodeKind, MountOpenFile, MountPath, MountRangeAllocation, MountSeekTarget,
    MountSourceError, ViewObserver, capture_root_identity,
};
use super::{LazyMount, NativeMountError, adapter};
use crate::LazySeekTarget;
use crate::demand::{
    DemandError, DemandFile, DemandSource, SourceChange, SourceChangeSink, SourceNode,
    SourceNodeKind, SourceReference, SourceWatch,
};
use crate::heap_future::in_heap;
use crate::kernel::{
    FileKind, FileMetadata, FileMutation, FilePayload, FileRecord, MetadataField, Mutation,
};
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, FileId, IdempotencyKey, LazyDirectoryCursor, LazyLookup,
    LazyWorkspace, LazyWorkspaceError, LazyWorkspaceStore, NativeRootIdentity, WorkspaceMetadata,
};
use crate::{CancellationToken, WorkBudget};
use acyclic_stream::OwnedBytes;
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};

const MAXIMUM_PROMOTION_BYTES: u64 = u64::MAX;

const MAXIMUM_LAZY_DIRECTORY_CURSORS: usize = 1_024;

const DIRECTORY_PROMOTION_PAGE: u32 = 256;

const MAXIMUM_PROMOTION_RETRIES: usize = 3;

const MAXIMUM_INTERRUPTED_LISTINGS: usize = 3;

struct StampedCursor<T> {
    generation: (u64, u64),
    cursor: T,
}

struct CursorTable<T> {
    entries: Mutex<BTreeMap<u64, StampedCursor<T>>>,
    next: AtomicU64,
    maximum: usize,
}

enum Resolution {
    Authored(MountLookup),
    Absent,
    Unauthored(
        LazyLookup,
        Option<SourceReference>,
        Option<crate::NativeAllocation>,
    ),
}

#[derive(Default)]
struct MountedRemovals {
    paths: BTreeMap<String, FileId>,
    records: BTreeMap<FileId, (FileRecord, FileMetadata)>,
    epoch: u64,
}

impl MountedRemovals {
    fn rebound(&mut self, path: &str) -> bool {
        let Some(file_id) = self.paths.remove(path) else {
            return false;
        };
        if !self.paths.values().any(|pending| *pending == file_id) {
            self.records.remove(&file_id);
        }
        self.epoch = self.epoch.wrapping_add(1);
        true
    }
}

struct DetachedIdentity<A, O> {
    file: Arc<CheckoutDetachedFile<A, O>>,
    ready: bool,
    published_epoch: u64,
    pending_epoch: Option<u64>,
}

type DetachedIdentities<A, O> = Arc<Mutex<BTreeMap<FileId, DetachedIdentity<A, O>>>>;

type OpenIdentityHandles = Arc<Mutex<BTreeMap<FileId, Vec<Weak<dyn MountOpenFile>>>>>;

type DirtyDetachedRecord = (FileId, FileRecord, FileMetadata, u64);

struct SourceChangeRecorder<A, O, D, S> {
    lazy: Weak<LazyWorkspace<A, O, D, S>>,
    authored: Weak<CheckoutMountSource<A, O>>,
    constructor_inputs: Mutex<
        Option<(
            Arc<LazyWorkspace<A, O, D, S>>,
            String,
            Option<crate::NativeAllocation>,
        )>,
    >,
}

fn linked(node: &SourceNode) -> bool {
    node.kind != SourceNodeKind::Directory && node.link_count.is_some_and(|count| count > 1)
}

/// One native callback adapter over a source-backed sparse workspace.
pub struct LazyMountSource<A, O, D, S> {
    pub(in crate::native_mount) lazy: Arc<LazyWorkspace<A, O, D, S>>,
    authored: Arc<CheckoutMountSource<A, O>>,
    root: String,
    runtime: Arc<CallbackRuntime>,
    cursors: CursorTable<(ViewStamp, LazyDirectoryCursor)>,
    source_view: Arc<SourceViewGate>,
    removals: Mutex<MountedRemovals>,
    detached: DetachedIdentities<A, O>,
    open_sources: OpenIdentityHandles,
    source_watch: Option<NativeMountSourceWatchOwner>,
    linked: Mutex<std::collections::HashSet<FileId>>,
    native_allocation: Option<crate::NativeAllocation>,
}

struct LazyOpenFile<A, O, D, S> {
    lazy: Arc<LazyWorkspace<A, O, D, S>>,
    authored: Arc<CheckoutMountSource<A, O>>,
    runtime: Arc<CallbackRuntime>,
    path: String,
    expected_source: FileId,
    source_file: Box<dyn DemandFile>,
    source_node: SourceNode,
    source_generation: u64,
    source_view: Arc<SourceViewGate>,
    detached: DetachedIdentities<A, O>,
    open_sources: OpenIdentityHandles,
    _allocation: crate::NativeAllocation,
}

struct ViewBoundOpenFile<A, O> {
    inner: Arc<dyn MountOpenFile>,
    runtime: Arc<CallbackRuntime>,
    source_view: Arc<SourceViewGate>,
    detached: DetachedIdentities<A, O>,
    open_sources: OpenIdentityHandles,
    file_id: Option<FileId>,
    generation: u64,
    cancellation: CancellationToken,
    _allocation: crate::NativeAllocation,
}

mod budget;
mod callbacks;
mod content;
mod cursors;
mod directory;
mod handles;
mod identities;
mod mutations;
mod source_file;
pub(super) use budget::*;
mod watch_owner;
pub use watch_owner::NativeMountSourceWatchOwner;
mod callback_wait;
pub(super) use callback_wait::{
    is_current_capture_observer_dispatch, with_capture_observer_dispatch,
    with_native_callback_carrier,
};
mod init;
mod promotion;
mod queries;
mod snapshots;
mod sync;
mod watch;
use content::*;
mod metadata;
use metadata::*;
use promotion::*;

#[cfg(test)]
mod tests;
