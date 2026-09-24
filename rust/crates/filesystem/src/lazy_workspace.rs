//! Source-backed sparse workspaces.
//!
//! A lazy workspace keeps unresolved host state outside the authenticated
//! authored checkout. Forking copies only one immutable overlay identifier;
//! paths are observed individually through [`DemandSource`].

use crate::demand::{
    DemandError, DemandSource, SourceCursor, SourceDirectoryEntry, SourceNode, SourceNodeKind,
    SourceReference,
};
use crate::kernel::{
    FileKind, FileMetadata, FilePayload, FileRecord, LogicalName, MetadataField, NameEncoding,
    NamespacePath,
};
use crate::path::PortablePath;
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, FileId, ForkOptions, Fs,
    IdempotencyKey, OperationReceipt, TransactionCommit, WorkBudget, WorkCounters, Workspace,
    WorkspaceDirectoryEntry, WorkspaceError, WorkspaceExtentKind, WorkspaceId, WorkspaceStat,
};
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use thiserror::Error;

#[cfg(target_arch = "wasm32")]
type LazyFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;
#[cfg(not(target_arch = "wasm32"))]
type LazyFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

const LAZY_STATE_SCHEMA: u32 = 5;
const MAXIMUM_STATE_RETRIES: usize = 32;

const LAZY_SNAPSHOT_DOMAIN: &[u8] = b"acyclic-fs-lazy-snapshot-v1\0";

/// Stable logical identity of one constant-size lazy tree snapshot.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct LazySnapshotId([u8; 32]);

impl LazySnapshotId {
    /// Canonical digest bytes.
    #[must_use]
    pub const fn into_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// Constant-size reference to an authored generation plus unresolved source state.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LazySnapshotRef {
    /// Stable logical identity over every represented component.
    pub id: LazySnapshotId,
    /// Workspace authenticating authored copy-on-write state.
    pub workspace_id: WorkspaceId,
    /// Immutable authored generation.
    pub authored_generation: crate::GenerationId,
    /// Live source identity and invalidation epoch.
    pub source: SourceReference,
    /// Immutable first-observation/tombstone overlay root.
    pub overlay: LazyOverlayId,
    /// Immutable source-identity shadow root.
    pub shadows: LazyShadowId,
}

impl LazySnapshotRef {
    fn new(
        workspace_id: WorkspaceId,
        authored_generation: crate::GenerationId,
        source: SourceReference,
        overlay: LazyOverlayId,
        shadows: LazyShadowId,
    ) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(LAZY_SNAPSHOT_DOMAIN);
        hasher.update(&workspace_id.into_bytes());
        hasher.update(authored_generation.digest().as_bytes());
        hasher.update(&source.identity);
        hasher.update(&source.epoch.to_le_bytes());
        hasher.update(&overlay.into_bytes());
        hasher.update(&shadows.into_bytes());
        Self {
            id: LazySnapshotId(*hasher.finalize().as_bytes()),
            workspace_id,
            authored_generation,
            source,
            overlay,
            shadows,
        }
    }
}

/// Content address of one immutable observed/tombstone overlay.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct LazyOverlayId([u8; 32]);

impl LazyOverlayId {
    /// Reconstructs a persisted overlay identifier.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Stable raw identifier.
    #[must_use]
    pub const fn into_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// Durable constant-size binding for one sparse workspace.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LazyWorkspaceState {
    /// Serialization schema.
    pub schema_version: u32,
    /// Monotonic CAS revision.
    pub revision: u64,
    /// Workspace whose authored generation supplies the writable overlay.
    pub workspace_id: WorkspaceId,
    /// Direct sparse parent, when this state was produced by a fork.
    pub parent_workspace_id: Option<WorkspaceId>,
    /// Provider identity and invalidation epoch pinned at attach time.
    pub source: SourceReference,
    /// Immutable observations and tombstones visible to this context.
    pub overlay: LazyOverlayId,
    /// Persistent source identity to latest authored record index.
    pub shadows: LazyShadowId,
    /// Prepared authored removal recovered atomically when the workspace reopens.
    pub pending_remove: Option<PendingLazyRemove>,
}

/// Content address of one immutable source-identity shadow index node.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct LazyShadowId([u8; 32]);

impl LazyShadowId {
    /// Stable raw identifier.
    #[must_use]
    pub const fn into_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// Persistent treap mapping a source-derived identity to its latest authored record.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub enum LazyShadow {
    /// Shared empty index root.
    #[default]
    Empty,
    /// One identity record and immutable child indexes.
    Node {
        /// Source-derived stable identity used as the search key.
        file_id: FileId,
        /// Deterministic treap priority.
        priority: u64,
        /// Canonically encoded immutable file record.
        record: Vec<u8>,
        /// Projected scalar metadata authenticated with the record.
        metadata: Box<crate::WorkspaceMetadata>,
        /// Identities ordered before this one.
        left: LazyShadowId,
        /// Identities ordered after this one.
        right: LazyShadowId,
    },
}

impl LazyShadow {
    fn id(&self) -> Result<LazyShadowId, LazyWorkspaceError> {
        let encoded = serde_json::to_vec(self)
            .map_err(|error| LazyWorkspaceError::Store(error.to_string()))?;
        Ok(LazyShadowId(*blake3::hash(&encoded).as_bytes()))
    }
}

/// Durable intent that makes authored removal and source tombstoning recoverable.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PendingLazyRemove {
    /// Canonical path being removed.
    pub path: String,
    /// Overlay visible before the removal began.
    pub prior_overlay: LazyOverlayId,
    /// Overlay containing the prepared source tombstone.
    pub tombstone_overlay: LazyOverlayId,
    /// Whether recovery must consult a durable authored operation.
    pub kind: PendingLazyRemoveKind,
}

/// Durable decision needed to recover a prepared lazy removal.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum PendingLazyRemoveKind {
    /// The removed binding existed only in the lazy source.
    SourceOnly,
    /// An authored removal is identified by this stable publication key.
    Authored {
        /// Idempotency key persisted before the authored transaction begins.
        idempotency_key: IdempotencyKey,
    },
}

/// One immutable node in the persistent source-knowledge index.
///
/// The deterministic treap keeps attach and fork O(1), mutations O(log n),
/// and exact lookups O(log n) without rewriting the complete observed set.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub enum LazyOverlay {
    /// Shared empty index root.
    #[default]
    Empty,
    /// One path fact and its immutable child indexes.
    Node {
        /// Portable absolute path used as the search key.
        path: String,
        /// Deterministic heap priority derived from `path`.
        priority: u64,
        /// Source observation or authored source tombstone.
        change: LazyOverlayChange,
        /// Keys ordered before this path.
        left: LazyOverlayId,
        /// Keys ordered after this path.
        right: LazyOverlayId,
    },
}

/// One fact introduced by a lazy overlay delta.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum LazyOverlayChange {
    /// First exact observation of one demanded path.
    Observe {
        /// Source generation against which this fact was authenticated.
        source: SourceReference,
        /// Immutable source fact.
        node: SourceNode,
    },
    /// Authored removal of a source path or subtree.
    Tombstone,
}

impl LazyOverlay {
    fn id(&self) -> Result<LazyOverlayId, LazyWorkspaceError> {
        let encoded = serde_json::to_vec(self)
            .map_err(|error| LazyWorkspaceError::Store(error.to_string()))?;
        Ok(LazyOverlayId(*blake3::hash(&encoded).as_bytes()))
    }
}

/// Durable companion storage for constant-size lazy bindings and immutable overlays.
#[async_trait]
pub trait LazyWorkspaceStore: Clone + Send + Sync + 'static {
    /// Backend error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Loads one workspace binding.
    async fn load_lazy_workspace(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Option<LazyWorkspaceState>, Self::Error>;

    /// Loads one binding under an explicit logical durable-read budget.
    async fn load_lazy_workspace_measured(
        &self,
        workspace_id: WorkspaceId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<Option<LazyWorkspaceState>>, LazyWorkspaceError> {
        measured_lazy_store_read(budget, cancellation)?;
        let value = self
            .load_lazy_workspace(workspace_id)
            .await
            .map_err(store_error)?;
        cancellation
            .check()
            .map_err(|_| LazyWorkspaceError::Cancelled)?;
        Ok(OperationReceipt {
            value,
            work: lazy_store_read_work(),
        })
    }

    /// Replaces one binding only at the expected revision (`0` means absent).
    async fn compare_and_swap_lazy_workspace(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: LazyWorkspaceState,
    ) -> Result<bool, Self::Error>;

    /// Replaces one binding under an explicit logical durable-write budget.
    async fn compare_and_swap_lazy_workspace_measured(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: LazyWorkspaceState,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<bool>, LazyWorkspaceError> {
        measured_lazy_store_write(budget, cancellation)?;
        let value = self
            .compare_and_swap_lazy_workspace(workspace_id, expected_revision, replacement)
            .await
            .map_err(store_error)?;
        cancellation
            .check()
            .map_err(|_| LazyWorkspaceError::Cancelled)?;
        Ok(OperationReceipt {
            value,
            work: lazy_store_write_work(),
        })
    }

    /// Loads an immutable content-addressed overlay.
    async fn load_lazy_overlay(
        &self,
        overlay: LazyOverlayId,
    ) -> Result<Option<LazyOverlay>, Self::Error>;

    /// Loads one immutable overlay under an explicit logical read budget.
    async fn load_lazy_overlay_measured(
        &self,
        overlay: LazyOverlayId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<Option<LazyOverlay>>, LazyWorkspaceError> {
        measured_lazy_store_read(budget, cancellation)?;
        let value = self.load_lazy_overlay(overlay).await.map_err(store_error)?;
        cancellation
            .check()
            .map_err(|_| LazyWorkspaceError::Cancelled)?;
        Ok(OperationReceipt {
            value,
            work: lazy_store_read_work(),
        })
    }

    /// Stores an immutable overlay, idempotently rejecting digest mismatches.
    async fn put_lazy_overlay(
        &self,
        overlay: LazyOverlayId,
        value: LazyOverlay,
    ) -> Result<(), Self::Error>;

    /// Loads one immutable source-identity shadow node.
    async fn load_lazy_shadow(
        &self,
        shadow: LazyShadowId,
    ) -> Result<Option<LazyShadow>, Self::Error>;

    /// Loads one immutable shadow under an explicit logical read budget.
    async fn load_lazy_shadow_measured(
        &self,
        shadow: LazyShadowId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<Option<LazyShadow>>, LazyWorkspaceError> {
        measured_lazy_store_read(budget, cancellation)?;
        let value = self.load_lazy_shadow(shadow).await.map_err(store_error)?;
        cancellation
            .check()
            .map_err(|_| LazyWorkspaceError::Cancelled)?;
        Ok(OperationReceipt {
            value,
            work: lazy_store_read_work(),
        })
    }

    /// Stores one immutable source-identity shadow node.
    async fn put_lazy_shadow(
        &self,
        shadow: LazyShadowId,
        value: LazyShadow,
    ) -> Result<(), Self::Error>;
}

/// In-memory companion store for deterministic tests and embedded use.
#[derive(Clone, Default)]
pub struct MemoryLazyWorkspaceStore {
    inner: Arc<Mutex<MemoryLazyWorkspaceState>>,
}

#[derive(Default)]
struct MemoryLazyWorkspaceState {
    workspaces: BTreeMap<WorkspaceId, LazyWorkspaceState>,
    overlays: BTreeMap<LazyOverlayId, LazyOverlay>,
    shadows: BTreeMap<LazyShadowId, LazyShadow>,
}

#[async_trait]
impl LazyWorkspaceStore for MemoryLazyWorkspaceStore {
    type Error = Infallible;

    async fn load_lazy_workspace(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Option<LazyWorkspaceState>, Self::Error> {
        let state = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Ok(state.workspaces.get(&workspace_id).cloned())
    }

    async fn compare_and_swap_lazy_workspace(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: LazyWorkspaceState,
    ) -> Result<bool, Self::Error> {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let actual = state
            .workspaces
            .get(&workspace_id)
            .map_or(0, |current| current.revision);
        if actual != expected_revision {
            return Ok(false);
        }
        state.workspaces.insert(workspace_id, replacement);
        Ok(true)
    }

    async fn load_lazy_overlay(
        &self,
        overlay: LazyOverlayId,
    ) -> Result<Option<LazyOverlay>, Self::Error> {
        let state = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Ok(state.overlays.get(&overlay).cloned())
    }

    async fn put_lazy_overlay(
        &self,
        overlay: LazyOverlayId,
        value: LazyOverlay,
    ) -> Result<(), Self::Error> {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(existing) = state.overlays.get(&overlay) {
            assert_eq!(existing, &value, "content-addressed overlay collision");
        } else {
            state.overlays.insert(overlay, value);
        }
        Ok(())
    }

    async fn load_lazy_shadow(
        &self,
        shadow: LazyShadowId,
    ) -> Result<Option<LazyShadow>, Self::Error> {
        let state = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Ok(state.shadows.get(&shadow).cloned())
    }

    async fn put_lazy_shadow(
        &self,
        shadow: LazyShadowId,
        value: LazyShadow,
    ) -> Result<(), Self::Error> {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(existing) = state.shadows.get(&shadow) {
            assert_eq!(existing, &value, "content-addressed shadow collision");
        } else {
            state.shadows.insert(shadow, value);
        }
        Ok(())
    }
}

#[derive(Clone)]
struct ImmutableTreapNode<K, V, I> {
    key: K,
    priority: u64,
    value: V,
    left: I,
    right: I,
}

#[async_trait]
trait ImmutableTreap: Sync {
    type Id: Copy + Send + Sync;
    type Key: Clone + Ord + Send + Sync;
    type Value: Clone + Send + Sync;

    async fn load(
        &self,
        id: Self::Id,
    ) -> Result<Option<ImmutableTreapNode<Self::Key, Self::Value, Self::Id>>, LazyWorkspaceError>;

    async fn store(
        &self,
        node: ImmutableTreapNode<Self::Key, Self::Value, Self::Id>,
    ) -> Result<Self::Id, LazyWorkspaceError>;

    fn priority(key: &Self::Key) -> u64;
}

fn insert_immutable_treap<'a, T: ImmutableTreap>(
    treap: &'a T,
    root: T::Id,
    key: T::Key,
    value: T::Value,
) -> LazyFuture<'a, Result<T::Id, LazyWorkspaceError>> {
    Box::pin(async move {
        let Some(current) = treap.load(root).await? else {
            return treap
                .store(ImmutableTreapNode {
                    priority: T::priority(&key),
                    key,
                    value,
                    left: root,
                    right: root,
                })
                .await;
        };
        match key.cmp(&current.key) {
            std::cmp::Ordering::Equal => {
                treap
                    .store(ImmutableTreapNode {
                        key,
                        value,
                        ..current
                    })
                    .await
            }
            ordering => {
                let descend_left = ordering == std::cmp::Ordering::Less;
                let child_root = if descend_left {
                    current.left
                } else {
                    current.right
                };
                let inserted = insert_immutable_treap(treap, child_root, key, value).await?;
                let child = treap.load(inserted).await?.ok_or_else(|| {
                    LazyWorkspaceError::Store("inserted lazy index node is absent".to_owned())
                })?;
                if child.priority > current.priority {
                    let rotated = treap
                        .store(if descend_left {
                            ImmutableTreapNode {
                                left: child.right,
                                ..current
                            }
                        } else {
                            ImmutableTreapNode {
                                right: child.left,
                                ..current
                            }
                        })
                        .await?;
                    return treap
                        .store(if descend_left {
                            ImmutableTreapNode {
                                right: rotated,
                                ..child
                            }
                        } else {
                            ImmutableTreapNode {
                                left: rotated,
                                ..child
                            }
                        })
                        .await;
                }
                treap
                    .store(if descend_left {
                        ImmutableTreapNode {
                            left: inserted,
                            ..current
                        }
                    } else {
                        ImmutableTreapNode {
                            right: inserted,
                            ..current
                        }
                    })
                    .await
            }
        }
    })
}

async fn immutable_treap_value<T: ImmutableTreap>(
    treap: &T,
    mut root: T::Id,
    key: &T::Key,
) -> Result<Option<T::Value>, LazyWorkspaceError> {
    loop {
        let Some(node) = treap.load(root).await? else {
            return Ok(None);
        };
        match key.cmp(&node.key) {
            std::cmp::Ordering::Equal => return Ok(Some(node.value)),
            std::cmp::Ordering::Less => root = node.left,
            std::cmp::Ordering::Greater => root = node.right,
        }
    }
}

struct OverlayTreap<'a, S>(&'a S);

#[async_trait]
impl<S: LazyWorkspaceStore> ImmutableTreap for OverlayTreap<'_, S> {
    type Id = LazyOverlayId;
    type Key = String;
    type Value = LazyOverlayChange;

    async fn load(
        &self,
        id: Self::Id,
    ) -> Result<Option<ImmutableTreapNode<Self::Key, Self::Value, Self::Id>>, LazyWorkspaceError>
    {
        match self.0.load_lazy_overlay(id).await.map_err(store_error)? {
            Some(LazyOverlay::Empty) => Ok(None),
            Some(LazyOverlay::Node {
                path,
                priority,
                change,
                left,
                right,
            }) => Ok(Some(ImmutableTreapNode {
                key: path,
                priority,
                value: change,
                left,
                right,
            })),
            None => Err(LazyWorkspaceError::Store(
                "lazy overlay is absent".to_owned(),
            )),
        }
    }

    async fn store(
        &self,
        node: ImmutableTreapNode<Self::Key, Self::Value, Self::Id>,
    ) -> Result<Self::Id, LazyWorkspaceError> {
        let overlay = LazyOverlay::Node {
            path: node.key,
            priority: node.priority,
            change: node.value,
            left: node.left,
            right: node.right,
        };
        let id = overlay.id()?;
        self.0
            .put_lazy_overlay(id, overlay)
            .await
            .map_err(store_error)?;
        Ok(id)
    }

    fn priority(key: &Self::Key) -> u64 {
        path_priority(key)
    }
}

#[derive(Clone)]
struct ShadowValue {
    record: Vec<u8>,
    metadata: Box<crate::WorkspaceMetadata>,
}

struct ShadowTreap<'a, S>(&'a S);

#[async_trait]
impl<S: LazyWorkspaceStore> ImmutableTreap for ShadowTreap<'_, S> {
    type Id = LazyShadowId;
    type Key = FileId;
    type Value = ShadowValue;

    async fn load(
        &self,
        id: Self::Id,
    ) -> Result<Option<ImmutableTreapNode<Self::Key, Self::Value, Self::Id>>, LazyWorkspaceError>
    {
        match self.0.load_lazy_shadow(id).await.map_err(store_error)? {
            Some(LazyShadow::Empty) => Ok(None),
            Some(LazyShadow::Node {
                file_id,
                priority,
                record,
                metadata,
                left,
                right,
            }) => Ok(Some(ImmutableTreapNode {
                key: file_id,
                priority,
                value: ShadowValue { record, metadata },
                left,
                right,
            })),
            None => Err(LazyWorkspaceError::Store(
                "lazy identity shadow is absent".to_owned(),
            )),
        }
    }

    async fn store(
        &self,
        node: ImmutableTreapNode<Self::Key, Self::Value, Self::Id>,
    ) -> Result<Self::Id, LazyWorkspaceError> {
        let shadow = LazyShadow::Node {
            file_id: node.key,
            priority: node.priority,
            record: node.value.record,
            metadata: node.value.metadata,
            left: node.left,
            right: node.right,
        };
        let id = shadow.id()?;
        self.0
            .put_lazy_shadow(id, shadow)
            .await
            .map_err(store_error)?;
        Ok(id)
    }

    fn priority(key: &Self::Key) -> u64 {
        shadow_priority(*key)
    }
}

/// A source-backed workspace whose unresolved paths remain outside its authored generation.
pub struct LazyWorkspace<A, O, D, S> {
    workspace: Workspace<A, O>,
    source: Arc<D>,
    store: S,
}

impl<A, O, D, S> Clone for LazyWorkspace<A, O, D, S>
where
    S: Clone,
{
    fn clone(&self) -> Self {
        Self {
            workspace: self.workspace.clone(),
            source: Arc::clone(&self.source),
            store: self.store.clone(),
        }
    }
}

/// Path facts returned without forcing unrelated source state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LazyStat {
    /// Observed kind.
    pub kind: SourceNodeKind,
    /// Observed logical bytes when meaningful.
    pub logical_bytes: Option<u64>,
    /// Whether the authored checkout overrides the source.
    pub authored: bool,
}

/// Complete facts returned from either authored state or a source observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LazyLookup {
    /// Authenticated authored state overrides the source.
    Authored {
        /// Existing authored binding used to serve this path. This may differ
        /// from the requested source alias until that alias is promoted.
        path: String,
        /// Authenticated authored facts for the shared identity.
        stat: WorkspaceStat,
    },
    /// Latest authored state retained after the final promoted binding was removed.
    Shadow {
        /// Path-independent immutable file record.
        record: FileRecord,
        /// Projected scalar metadata retained with the record.
        metadata: crate::WorkspaceMetadata,
        /// Source-native alias count evidence.
        source_link_count: u64,
    },
    /// Immutable first-observation source state.
    Source(SourceNode),
}

struct ResolvedLazyLookup {
    lookup: LazyLookup,
    source: Option<SourceReference>,
}

struct PinnedSourceNode {
    source: SourceReference,
    node: SourceNode,
}

/// Sparse seek target for a lazily sourced regular file.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LazySeekTarget {
    /// First represented data byte.
    Data,
    /// First hole byte, including the logical end of a dense source file.
    Hole,
}

/// One merged directory entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LazyDirectoryEntry {
    /// Exact profile-aware child name.
    pub name: LogicalName,
    /// Observed or authored kind.
    pub kind: SourceNodeKind,
    /// Whether the authored checkout overrides the source.
    pub authored: bool,
}

/// Opaque bounded continuation for a merged source/authored directory scan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LazyDirectoryCursor {
    source: SourceReference,
    overlay: LazyOverlayId,
    directory: NamespacePath,
    phase: LazyDirectoryPhase,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum LazyDirectoryPhase {
    Source(Option<SourceCursor>),
    Authored(Option<LogicalName>),
}

/// One bounded page from the merged lazy directory view.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LazyDirectoryPage {
    /// At most the requested number of visible entries.
    pub entries: Vec<LazyDirectoryEntry>,
    /// Opaque continuation, absent after the authored phase is exhausted.
    pub next: Option<LazyDirectoryCursor>,
}

/// Fail-closed lazy workspace errors.
#[derive(Debug, Error)]
pub enum LazyWorkspaceError {
    /// Authenticated workspace operation failed.
    #[error("workspace operation failed: {0}")]
    Workspace(String),
    /// Demand provider operation failed.
    #[error("source demand failed: {0}")]
    Demand(#[from] DemandError),
    /// Companion state failed.
    #[error("lazy state failed: {0}")]
    Store(String),
    /// Durable state changed too frequently to complete a bounded retry.
    #[error("lazy state changed concurrently")]
    Concurrent,
    /// Provider identity or invalidation epoch no longer matches the binding.
    #[error("lazy source binding is stale")]
    StaleSource,
    /// Requested path is absent after applying authored tombstones.
    #[error("path is absent")]
    NotFound,
    /// Requested operation requires a regular file.
    #[error("path is not a regular file")]
    NotRegularFile,
    /// Requested read exceeds the explicit bound.
    #[error("file exceeds the requested read bound")]
    TooLarge,
    /// A directory page must request at least one entry.
    #[error("directory page bound must be positive")]
    InvalidPageBound,
    /// Directory view changed after the supplied cursor was issued.
    #[error("lazy directory cursor is stale")]
    StaleCursor,
    /// The path no longer names the object the caller resolved.
    #[error("lazy filesystem object identity is stale")]
    StaleIdentity,
    /// The source fact cannot be represented by this workspace profile.
    #[error("source node cannot be represented by the authored workspace")]
    UnsupportedNode,
    /// Removing the last promoted binding would resurrect stale source bytes
    /// through an unresolved hard-link alias.
    #[error("source hard-link aliases must be promoted before removing the final authored binding")]
    UnresolvedHardLinks,
    /// Exact traversal was cancelled before every path was captured.
    #[error("lazy exact scan was cancelled")]
    Cancelled,
    /// Exact traversal exceeded an explicit work budget.
    #[error("lazy exact scan exceeded its work budget: {0}")]
    Work(String),
}

#[derive(Clone, Copy)]
struct ExactificationSource(SourceReference);

impl LazySnapshotRef {
    fn exactification_source(
        self,
        live_source: SourceReference,
    ) -> Result<ExactificationSource, LazyWorkspaceError> {
        if self.source.identity != live_source.identity {
            return Err(LazyWorkspaceError::StaleSource);
        }
        Ok(ExactificationSource(live_source))
    }

    fn exactification_state(
        self,
        workspace_id: WorkspaceId,
        parent_workspace_id: WorkspaceId,
        source: ExactificationSource,
    ) -> LazyWorkspaceState {
        LazyWorkspaceState {
            schema_version: LAZY_STATE_SCHEMA,
            revision: 1,
            workspace_id,
            parent_workspace_id: Some(parent_workspace_id),
            // Observed overlay records retain their pinned source epochs. This
            // binding is deliberately live for paths unresolved at capture.
            source: source.0,
            overlay: self.overlay,
            shadows: self.shadows,
            pending_remove: None,
        }
    }
}

impl<A, O, D, S> LazyWorkspace<A, O, D, S>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    /// Creates an empty authored workspace and binds a source without demanding any path.
    pub async fn attach(
        fs: &Fs<A, O>,
        name: impl AsRef<str>,
        source: Arc<D>,
        store: S,
    ) -> Result<Self, LazyWorkspaceError> {
        let workspace = fs.create_workspace(name).await.map_err(workspace_error)?;
        let overlay = LazyOverlay::default();
        let overlay_id = overlay.id()?;
        store
            .put_lazy_overlay(overlay_id, overlay)
            .await
            .map_err(store_error)?;
        let shadows = LazyShadow::default();
        let shadow_id = shadows.id()?;
        store
            .put_lazy_shadow(shadow_id, shadows)
            .await
            .map_err(store_error)?;
        let state = LazyWorkspaceState {
            schema_version: LAZY_STATE_SCHEMA,
            revision: 1,
            workspace_id: workspace.id(),
            parent_workspace_id: None,
            source: source.reference(),
            overlay: overlay_id,
            shadows: shadow_id,
            pending_remove: None,
        };
        let existing = store
            .load_lazy_workspace(workspace.id())
            .await
            .map_err(store_error)?;
        match existing {
            Some(existing)
                if existing.source == state.source
                    && existing.schema_version == LAZY_STATE_SCHEMA => {}
            Some(_) => return Err(LazyWorkspaceError::StaleSource),
            None => {
                if !store
                    .compare_and_swap_lazy_workspace(workspace.id(), 0, state)
                    .await
                    .map_err(store_error)?
                {
                    return Err(LazyWorkspaceError::Concurrent);
                }
            }
        }
        let lazy = Self {
            workspace,
            source,
            store,
        };
        lazy.recover_pending_remove().await?;
        Ok(lazy)
    }

    /// Reopens an existing sparse workspace without demanding source state.
    pub async fn open(
        workspace: Workspace<A, O>,
        source: Arc<D>,
        store: S,
    ) -> Result<Self, LazyWorkspaceError> {
        let state = store
            .load_lazy_workspace(workspace.id())
            .await
            .map_err(store_error)?
            .ok_or_else(|| LazyWorkspaceError::Store("lazy binding is absent".to_owned()))?;
        if state.schema_version != LAZY_STATE_SCHEMA
            || state.workspace_id != workspace.id()
            || state.source.identity != source.reference().identity
        {
            return Err(LazyWorkspaceError::StaleSource);
        }
        if store
            .load_lazy_overlay(state.overlay)
            .await
            .map_err(store_error)?
            .is_none()
        {
            return Err(LazyWorkspaceError::Store(
                "lazy overlay is absent".to_owned(),
            ));
        }
        if store
            .load_lazy_shadow(state.shadows)
            .await
            .map_err(store_error)?
            .is_none()
        {
            return Err(LazyWorkspaceError::Store(
                "lazy identity shadow is absent".to_owned(),
            ));
        }
        let lazy = Self {
            workspace,
            source,
            store,
        };
        lazy.recover_pending_remove().await?;
        lazy.rebind_source().await?;
        Ok(lazy)
    }

    /// Authored SDK workspace used by merge, lineage, and publication primitives.
    #[must_use]
    pub const fn workspace(&self) -> &Workspace<A, O> {
        &self.workspace
    }

    /// Reopens another workspace bound to the same source capability.
    ///
    /// This is the constant-size branch/resume primitive for adapters that
    /// already resolved the destination workspace through durable lineage.
    pub async fn open_related(&self, workspace: Workspace<A, O>) -> Result<Self, LazyWorkspaceError>
    where
        S: Clone,
    {
        Self::open(workspace, Arc::clone(&self.source), self.store.clone()).await
    }

    /// Captures the complete logical lazy-tree identity without enumeration.
    pub async fn snapshot(&self) -> Result<LazySnapshotRef, LazyWorkspaceError> {
        let state = self.state().await?;
        let authored_generation = self.workspace.head().await.map_err(workspace_error)?.id();
        Ok(LazySnapshotRef::new(
            self.workspace.id(),
            authored_generation,
            state.source,
            state.overlay,
            state.shadows,
        ))
    }

    /// Loads the constant-size durable source/lineage binding.
    pub async fn binding(&self) -> Result<LazyWorkspaceState, LazyWorkspaceError> {
        self.state().await
    }

    /// Forks in constant-size metadata while retaining the exact overlay snapshot.
    pub async fn fork(
        &self,
        destination: impl AsRef<str>,
        idempotency_key: IdempotencyKey,
    ) -> Result<Self, LazyWorkspaceError> {
        let parent = self.state().await?;
        let generation = self.workspace.head().await.map_err(workspace_error)?;
        let workspace = self
            .workspace
            .fork(
                destination,
                ForkOptions {
                    generation,
                    idempotency_key,
                },
            )
            .await
            .map_err(workspace_error)?;
        let state = LazyWorkspaceState {
            schema_version: LAZY_STATE_SCHEMA,
            revision: 1,
            workspace_id: workspace.id(),
            parent_workspace_id: Some(self.workspace.id()),
            source: parent.source,
            overlay: parent.overlay,
            shadows: parent.shadows,
            pending_remove: None,
        };
        if !self
            .store
            .compare_and_swap_lazy_workspace(workspace.id(), 0, state)
            .await
            .map_err(store_error)?
        {
            let existing = self
                .store
                .load_lazy_workspace(workspace.id())
                .await
                .map_err(store_error)?
                .ok_or(LazyWorkspaceError::Concurrent)?;
            if existing.parent_workspace_id != Some(self.workspace.id())
                || existing.source != parent.source
                || existing.overlay != parent.overlay
                || existing.shadows != parent.shadows
            {
                return Err(LazyWorkspaceError::Concurrent);
            }
        }
        Ok(Self {
            workspace,
            source: Arc::clone(&self.source),
            store: self.store.clone(),
        })
    }

    /// Materializes an immutable lazy snapshot in a deterministic scratch fork.
    ///
    /// Previously observed records remain pinned to the snapshot's source epoch;
    /// unresolved paths are observed from the injected live source when first
    /// demanded. The owning workspace is never moved or rewritten.
    #[allow(clippy::too_many_lines)]
    pub async fn exactify_snapshot(
        &self,
        snapshot: LazySnapshotRef,
        destination: impl AsRef<str>,
        idempotency_key: IdempotencyKey,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<crate::Generation<A, O>>, LazyWorkspaceError> {
        cancellation
            .check()
            .map_err(|_| LazyWorkspaceError::Cancelled)?;
        if snapshot.workspace_id != self.workspace.id() {
            return Err(LazyWorkspaceError::StaleSource);
        }
        let exactification_source = snapshot.exactification_source(self.source.reference())?;
        let overlay = self
            .store
            .load_lazy_overlay_measured(snapshot.overlay, budget, cancellation)
            .await?;
        let mut work = overlay.work;
        let shadow = self
            .store
            .load_lazy_shadow_measured(
                snapshot.shadows,
                remaining_work(work, budget)?,
                cancellation,
            )
            .await?;
        work = account_work(work, shadow.work, budget)?;
        if overlay.value.is_none() || shadow.value.is_none() {
            return Err(LazyWorkspaceError::Store(
                "lazy snapshot index is absent".to_owned(),
            ));
        }
        let generation = self
            .workspace
            .generation_measured(
                snapshot.authored_generation,
                remaining_work(work, budget)?,
                cancellation,
            )
            .await
            .map_err(workspace_error)?;
        work = account_work(work, generation.work, budget)?;
        let workspace = self
            .workspace
            .fork_measured(
                destination,
                ForkOptions {
                    generation: generation.value,
                    idempotency_key,
                },
                remaining_work(work, budget)?,
                cancellation,
            )
            .await
            .map_err(workspace_error)?;
        work = account_work(work, workspace.work, budget)?;
        let workspace = workspace.value;
        let state = snapshot.exactification_state(
            workspace.id(),
            self.workspace.id(),
            exactification_source,
        );
        let inserted = self
            .store
            .compare_and_swap_lazy_workspace_measured(
                workspace.id(),
                0,
                state.clone(),
                remaining_work(work, budget)?,
                cancellation,
            )
            .await?;
        work = account_work(work, inserted.work, budget)?;
        if !inserted.value {
            let existing = self
                .store
                .load_lazy_workspace_measured(
                    workspace.id(),
                    remaining_work(work, budget)?,
                    cancellation,
                )
                .await?;
            work = account_work(work, existing.work, budget)?;
            let existing = existing.value.ok_or(LazyWorkspaceError::Concurrent)?;
            if existing.parent_workspace_id != state.parent_workspace_id
                || existing.source != state.source
                || existing.overlay != state.overlay
                || existing.shadows != state.shadows
            {
                return Err(LazyWorkspaceError::Concurrent);
            }
        }
        let exactified = Self {
            workspace,
            source: Arc::clone(&self.source),
            store: self.store.clone(),
        }
        .exactify(remaining_work(work, budget)?, cancellation)
        .await?;
        work = account_work(work, exactified.work, budget)?;
        Ok(OperationReceipt {
            value: exactified.value,
            work,
        })
    }

    /// Advances this binding to the provider's current invalidation epoch
    /// without enumerating the source. Prior overlay roots remain immutable;
    /// observations are refreshed on demand in the new epoch while authored
    /// state and tombstones remain shared.
    pub async fn rebind_source(&self) -> Result<LazyWorkspaceState, LazyWorkspaceError> {
        let source = self.source.reference();
        for _ in 0..MAXIMUM_STATE_RETRIES {
            let state = self
                .store
                .load_lazy_workspace(self.workspace.id())
                .await
                .map_err(store_error)?
                .ok_or_else(|| LazyWorkspaceError::Store("lazy binding is absent".to_owned()))?;
            if state.schema_version != LAZY_STATE_SCHEMA
                || state.workspace_id != self.workspace.id()
                || state.source.identity != source.identity
            {
                return Err(LazyWorkspaceError::StaleSource);
            }
            if state.pending_remove.is_some() {
                return Err(LazyWorkspaceError::Concurrent);
            }
            if state.source == source {
                return Ok(state);
            }
            let replacement = LazyWorkspaceState {
                revision: state.revision.saturating_add(1),
                source,
                ..state.clone()
            };
            if self
                .store
                .compare_and_swap_lazy_workspace(
                    self.workspace.id(),
                    state.revision,
                    replacement.clone(),
                )
                .await
                .map_err(store_error)?
            {
                return Ok(replacement);
            }
        }
        Err(LazyWorkspaceError::Concurrent)
    }

    /// Observes one path without reading file content.
    pub async fn stat(&self, path: &str) -> Result<LazyStat, LazyWorkspaceError> {
        let cancellation = CancellationToken::new();
        self.stat_measured(path, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map(|receipt| receipt.value)
    }

    async fn stat_measured(
        &self,
        path: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<LazyStat>, LazyWorkspaceError> {
        let receipt = self.lookup_measured(path, budget, cancellation).await?;
        let value = match receipt.value.lookup {
            LazyLookup::Authored { stat, .. } => LazyStat {
                kind: source_kind(stat.kind),
                logical_bytes: stat.logical_bytes,
                authored: true,
            },
            LazyLookup::Shadow { record, .. } => LazyStat {
                kind: source_kind(record.kind),
                logical_bytes: record_logical_bytes(record),
                authored: true,
            },
            LazyLookup::Source(node) => LazyStat {
                kind: node.kind,
                logical_bytes: node.logical_bytes,
                authored: false,
            },
        };
        Ok(OperationReceipt {
            value,
            work: receipt.work,
        })
    }

    /// Returns complete demanded facts without reading regular-file content.
    pub async fn lookup(&self, path: &str) -> Result<LazyLookup, LazyWorkspaceError> {
        let cancellation = CancellationToken::new();
        self.lookup_measured(path, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map(|receipt| receipt.value.lookup)
    }

    async fn lookup_measured(
        &self,
        path: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<ResolvedLazyLookup>, LazyWorkspaceError> {
        let authored = self
            .workspace
            .stat_optional_measured(path, budget, cancellation)
            .await
            .map_err(workspace_error)?;
        let mut work = authored.work;
        if let Some(stat) = authored.value {
            return Ok(OperationReceipt {
                value: ResolvedLazyLookup {
                    lookup: LazyLookup::Authored {
                        path: self.canonical_path(path)?,
                        stat,
                    },
                    source: None,
                },
                work,
            });
        }
        let state_receipt = self
            .state_measured(remaining_work(work, budget)?, cancellation)
            .await?;
        work = account_work(work, state_receipt.work, budget)?;
        let state = state_receipt.value;
        let tombstoned = self
            .tombstoned_measured(
                state.overlay,
                path,
                remaining_work(work, budget)?,
                cancellation,
            )
            .await?;
        work = account_work(work, tombstoned.work, budget)?;
        if tombstoned.value {
            return Err(LazyWorkspaceError::NotFound);
        }
        let observed = self.observe_measured(path, state, cancellation).await?;
        work = account_work(work, observed.work, budget)?;
        let (source, node) = observed.value;
        let authored = self
            .authored_alias_measured(&node, remaining_work(work, budget)?, cancellation)
            .await?;
        work = account_work(work, authored.work, budget)?;
        if let Some(authored) = authored.value {
            return Ok(OperationReceipt {
                value: ResolvedLazyLookup {
                    lookup: authored,
                    source: None,
                },
                work,
            });
        }
        Ok(OperationReceipt {
            value: ResolvedLazyLookup {
                lookup: LazyLookup::Source(node),
                source: Some(source),
            },
            work,
        })
    }

    /// Returns current facts without extending the immutable observation index.
    ///
    /// Directory projection uses this after receiving a source page so that
    /// gathering entry metadata cannot invalidate the page's continuation.
    pub async fn inspect(&self, path: &str) -> Result<LazyLookup, LazyWorkspaceError> {
        let cancellation = CancellationToken::new();
        self.inspect_measured(path, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map(|receipt| receipt.value.lookup)
    }

    async fn inspect_measured(
        &self,
        path: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<ResolvedLazyLookup>, LazyWorkspaceError> {
        self.inspect_with_observation_policy(path, budget, cancellation, false)
            .await
    }

    async fn inspect_snapshot_measured(
        &self,
        path: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<ResolvedLazyLookup>, LazyWorkspaceError> {
        self.inspect_with_observation_policy(path, budget, cancellation, true)
            .await
    }

    async fn inspect_with_observation_policy(
        &self,
        path: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        retain_pinned_observation: bool,
    ) -> Result<OperationReceipt<ResolvedLazyLookup>, LazyWorkspaceError> {
        let authored = self
            .workspace
            .stat_optional_measured(path, budget, cancellation)
            .await
            .map_err(workspace_error)?;
        let mut work = authored.work;
        if let Some(stat) = authored.value {
            return Ok(OperationReceipt {
                value: ResolvedLazyLookup {
                    lookup: LazyLookup::Authored {
                        path: self.canonical_path(path)?,
                        stat,
                    },
                    source: None,
                },
                work,
            });
        }
        let state_receipt = self
            .state_measured(remaining_work(work, budget)?, cancellation)
            .await?;
        work = account_work(work, state_receipt.work, budget)?;
        let state = state_receipt.value;
        let fact = self
            .overlay_fact_measured(
                state.overlay,
                path,
                remaining_work(work, budget)?,
                cancellation,
            )
            .await?;
        work = account_work(work, fact.work, budget)?;
        let observed = match fact.value {
            Some(LazyOverlayChange::Tombstone) => return Err(LazyWorkspaceError::NotFound),
            Some(LazyOverlayChange::Observe { source, node })
                if retain_pinned_observation || source == state.source =>
            {
                Some((source, node))
            }
            Some(LazyOverlayChange::Observe { .. }) | None => None,
        };
        let (source, node) = if let Some(observed) = observed {
            observed
        } else {
            let receipt = self
                .source
                .lookup(state.source, &self.namespace_path(path)?, cancellation)
                .await
                .map_err(|failure| LazyWorkspaceError::from(failure.error))?;
            work = account_work(work, receipt.work, budget)?;
            (
                state.source,
                receipt.value.ok_or(LazyWorkspaceError::NotFound)?,
            )
        };
        let tombstoned = self
            .tombstoned_measured(
                state.overlay,
                path,
                remaining_work(work, budget)?,
                cancellation,
            )
            .await?;
        work = account_work(work, tombstoned.work, budget)?;
        if tombstoned.value {
            return Err(LazyWorkspaceError::NotFound);
        }
        let authored = self
            .authored_alias_measured(&node, remaining_work(work, budget)?, cancellation)
            .await?;
        work = account_work(work, authored.work, budget)?;
        if let Some(authored) = authored.value {
            return Ok(OperationReceipt {
                value: ResolvedLazyLookup {
                    lookup: authored,
                    source: None,
                },
                work,
            });
        }
        Ok(OperationReceipt {
            value: ResolvedLazyLookup {
                lookup: LazyLookup::Source(node),
                source: Some(source),
            },
            work,
        })
    }

    /// Returns a stable SDK identity for an unresolved source object.
    ///
    /// Aliases carrying the same source-native identity map to one `FileId`,
    /// independent of path, so mount lookups preserve hard-link topology.
    pub fn source_file_id(&self, node: &SourceNode) -> FileId {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"acyclic-fs-lazy-source-file-id-v1\0");
        hasher.update(&self.source.reference().identity);
        hasher.update(&node.file_identity);
        let digest = hasher.finalize();
        let mut bytes = [0_u8; 16];
        bytes.copy_from_slice(&digest.as_bytes()[..16]);
        FileId::from_bytes(bytes)
    }

    async fn authored_alias_measured(
        &self,
        node: &SourceNode,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<Option<LazyLookup>>, LazyWorkspaceError> {
        let file_id = self.source_file_id(node);
        let state = self.state_measured(budget, cancellation).await?;
        let generation = self
            .workspace
            .head_measured(remaining_work(state.work, budget)?, cancellation)
            .await
            .map_err(workspace_error)?;
        let mut work = account_work(state.work, generation.work, budget)?;
        let state = state.value;
        let paths = generation
            .value
            .paths_for_file_id_measured(
                file_id,
                u32::MAX,
                remaining_work(work, budget)?,
                cancellation,
            )
            .await
            .map_err(workspace_error)?;
        work = account_work(work, paths.work, budget)?;
        if let Some(path) = paths.value.into_iter().next() {
            let stat = self
                .workspace
                .stat_optional_measured(&path, remaining_work(work, budget)?, cancellation)
                .await
                .map_err(workspace_error)?;
            work = account_work(work, stat.work, budget)?;
            let mut stat = stat.value.ok_or(LazyWorkspaceError::Concurrent)?;
            if let Some(source_links) = node.link_count {
                stat.link_count = stat.link_count.max(source_links);
            }
            return Ok(OperationReceipt {
                value: Some(LazyLookup::Authored { path, stat }),
                work,
            });
        }
        let shadow = self
            .shadow_record_measured(
                state.shadows,
                file_id,
                remaining_work(work, budget)?,
                cancellation,
            )
            .await?;
        work = account_work(work, shadow.work, budget)?;
        let Some((record, metadata)) = shadow.value else {
            return Ok(OperationReceipt { value: None, work });
        };
        Ok(OperationReceipt {
            value: Some(LazyLookup::Shadow {
                record,
                metadata,
                source_link_count: node.link_count.unwrap_or(1),
            }),
            work,
        })
    }

    /// Reads one exact range, demanding no unrelated content.
    pub async fn read_range(
        &self,
        path: &str,
        offset: u64,
        length: u64,
    ) -> Result<Bytes, LazyWorkspaceError> {
        let cancellation = CancellationToken::new();
        self.read_range_measured(path, offset, length, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map(|receipt| receipt.value)
    }

    async fn read_range_measured(
        &self,
        path: &str,
        offset: u64,
        length: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<Bytes>, LazyWorkspaceError> {
        let lookup = self.lookup_measured(path, budget, cancellation).await?;
        let prior = lookup.work;
        let ResolvedLazyLookup { lookup, source } = lookup.value;
        match lookup {
            LazyLookup::Authored {
                path: authored_path,
                ..
            } => {
                let value = self
                    .workspace
                    .read_range(&authored_path, offset, length)
                    .await
                    .map_err(workspace_error)?;
                Ok(OperationReceipt { value, work: prior })
            }
            LazyLookup::Shadow { record, .. } => {
                if record.kind != FileKind::Regular {
                    return Err(LazyWorkspaceError::NotRegularFile);
                }
                let receipt = self
                    .workspace
                    .detached_record(record)
                    .read_range(
                        crate::ByteRange { offset, length },
                        remaining_work(prior, budget)?,
                        cancellation,
                    )
                    .await
                    .map_err(|failure| LazyWorkspaceError::Workspace(failure.error.to_string()));
                let receipt = receipt?;
                Ok(OperationReceipt {
                    value: receipt.value.bytes,
                    work: account_work(prior, receipt.work, budget)?,
                })
            }
            LazyLookup::Source(node) => {
                if node.kind != SourceNodeKind::RegularFile {
                    return Err(LazyWorkspaceError::NotRegularFile);
                }
                let source = source.ok_or(LazyWorkspaceError::Concurrent)?;
                let receipt = self
                    .source
                    .read_range(
                        source,
                        &self.namespace_path(path)?,
                        node.version,
                        offset,
                        length,
                        cancellation,
                    )
                    .await
                    .map_err(|failure| LazyWorkspaceError::from(failure.error))?;
                Ok(OperationReceipt {
                    value: receipt.value,
                    work: account_work(prior, receipt.work, budget)?,
                })
            }
        }
    }

    /// Reads a complete regular file under an explicit byte bound.
    pub async fn read(&self, path: &str, maximum_bytes: u64) -> Result<Bytes, LazyWorkspaceError> {
        let stat = self.stat(path).await?;
        let length = stat
            .logical_bytes
            .ok_or(LazyWorkspaceError::NotRegularFile)?;
        if length > maximum_bytes {
            return Err(LazyWorkspaceError::TooLarge);
        }
        self.read_range(path, 0, length).await
    }

    /// Reads one exact symbolic-link target without following it.
    pub async fn read_link(&self, path: &str) -> Result<Bytes, LazyWorkspaceError> {
        let cancellation = CancellationToken::new();
        self.read_link_measured(path, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map(|receipt| receipt.value)
    }

    async fn read_link_measured(
        &self,
        path: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<Bytes>, LazyWorkspaceError> {
        let lookup = self.lookup_measured(path, budget, cancellation).await?;
        let prior = lookup.work;
        let ResolvedLazyLookup { lookup, source } = lookup.value;
        match lookup {
            LazyLookup::Authored {
                path: authored_path,
                stat,
            } => {
                if stat.kind != FileKind::SymbolicLink {
                    return Err(LazyWorkspaceError::NotRegularFile);
                }
                let value = self
                    .workspace
                    .read_symbolic_link(&authored_path)
                    .await
                    .map_err(workspace_error)?;
                Ok(OperationReceipt { value, work: prior })
            }
            LazyLookup::Shadow { record, .. } => {
                let receipt = self
                    .workspace
                    .detached_record(record)
                    .read_symbolic_link(remaining_work(prior, budget)?, cancellation)
                    .await
                    .map_err(|failure| LazyWorkspaceError::Workspace(failure.error.to_string()))?;
                Ok(OperationReceipt {
                    value: receipt.value,
                    work: account_work(prior, receipt.work, budget)?,
                })
            }
            LazyLookup::Source(node) => {
                if node.kind != SourceNodeKind::SymbolicLink {
                    return Err(LazyWorkspaceError::NotRegularFile);
                }
                let source = source.ok_or(LazyWorkspaceError::Concurrent)?;
                let receipt = self
                    .source
                    .read_link(
                        source,
                        &self.namespace_path(path)?,
                        node.version,
                        cancellation,
                    )
                    .await
                    .map_err(|failure| LazyWorkspaceError::from(failure.error))?;
                Ok(OperationReceipt {
                    value: receipt.value,
                    work: account_work(prior, receipt.work, budget)?,
                })
            }
        }
    }

    /// Finds a sparse boundary without demanding file contents.
    ///
    /// Authored files use their authenticated extent tree. Unresolved native
    /// sources currently expose only dense length/version facts, so their
    /// exact portable representation is data through EOF followed by a hole.
    pub async fn seek(
        &self,
        path: &str,
        offset: u64,
        target: LazySeekTarget,
    ) -> Result<Option<u64>, LazyWorkspaceError> {
        match self.lookup(path).await? {
            LazyLookup::Authored {
                path: authored_path,
                stat,
            } => {
                if stat.kind != FileKind::Regular {
                    return Err(LazyWorkspaceError::NotRegularFile);
                }
                let length = stat
                    .logical_bytes
                    .ok_or(LazyWorkspaceError::NotRegularFile)?;
                if offset >= length {
                    return Ok(None);
                }
                let plan = self
                    .workspace
                    .plan_extents(&authored_path, offset, length - offset, 65_536)
                    .await
                    .map_err(workspace_error)?;
                let found = plan.spans.into_iter().find(|span| match target {
                    LazySeekTarget::Data => span.kind != WorkspaceExtentKind::Hole,
                    LazySeekTarget::Hole => span.kind == WorkspaceExtentKind::Hole,
                });
                Ok(found
                    .map(|span| span.offset)
                    .or_else(|| (target == LazySeekTarget::Hole).then_some(length)))
            }
            LazyLookup::Shadow { record, .. } => {
                if record.kind != FileKind::Regular {
                    return Err(LazyWorkspaceError::NotRegularFile);
                }
                self.workspace
                    .detached_record(record)
                    .seek(
                        offset,
                        match target {
                            LazySeekTarget::Data => crate::kernel::ExtentSeekTarget::Data,
                            LazySeekTarget::Hole => crate::kernel::ExtentSeekTarget::Hole,
                        },
                        crate::WorkBudget::UNBOUNDED,
                        &crate::CancellationToken::new(),
                    )
                    .await
                    .map(|receipt| receipt.value)
                    .map_err(|failure| LazyWorkspaceError::Workspace(failure.error.to_string()))
            }
            LazyLookup::Source(node) => {
                if node.kind != SourceNodeKind::RegularFile {
                    return Err(LazyWorkspaceError::NotRegularFile);
                }
                let length = node
                    .logical_bytes
                    .ok_or(LazyWorkspaceError::NotRegularFile)?;
                if offset >= length {
                    return Ok(None);
                }
                Ok(Some(match target {
                    LazySeekTarget::Data => offset,
                    LazySeekTarget::Hole => length,
                }))
            }
        }
    }

    /// Promotes exactly one demanded source node into authored copy-on-write state.
    /// Directories remain shallow and regular files are fetched in bounded ranges.
    #[allow(clippy::too_many_lines)]
    pub async fn promote(
        &self,
        path: &str,
        maximum_bytes: u64,
        idempotency_key: IdempotencyKey,
    ) -> Result<TransactionCommit<A, O>, LazyWorkspaceError> {
        self.promote_with_permit(
            path,
            maximum_bytes,
            idempotency_key,
            crate::PublicationPermit::Unrestricted,
        )
        .await
    }

    #[allow(clippy::too_many_lines)]
    async fn populate_source_node_measured(
        &self,
        transaction: &mut crate::Transaction<A, O>,
        requested: &str,
        pinned: &PinnedSourceNode,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<WorkCounters, LazyWorkspaceError> {
        let node = &pinned.node;
        let source = pinned.source;
        let mut work = WorkCounters::default();
        match node.kind {
            SourceNodeKind::Directory => {
                let applied = transaction
                    .create_directory_measured(
                        requested,
                        remaining_work(work, budget)?,
                        cancellation,
                    )
                    .await
                    .map_err(workspace_error)?;
                work = account_work(work, applied, budget)?;
            }
            SourceNodeKind::RegularFile => {
                work = self
                    .populate_regular_source_node(
                        transaction,
                        requested,
                        pinned,
                        maximum_bytes,
                        budget,
                        cancellation,
                    )
                    .await?;
            }
            SourceNodeKind::SymbolicLink => {
                let receipt = self
                    .source
                    .read_link(
                        source,
                        &self.namespace_path(requested)?,
                        node.version,
                        cancellation,
                    )
                    .await
                    .map_err(|failure| LazyWorkspaceError::from(failure.error))?;
                work = account_work(work, receipt.work, budget)?;
                let applied = transaction
                    .create_symbolic_link_measured(
                        requested,
                        receipt.value,
                        remaining_work(work, budget)?,
                        cancellation,
                    )
                    .await
                    .map_err(workspace_error)?;
                work = account_work(work, applied, budget)?;
            }
            SourceNodeKind::Fifo
            | SourceNodeKind::Socket
            | SourceNodeKind::CharacterDevice
            | SourceNodeKind::BlockDevice => {
                let kind = match node.kind {
                    SourceNodeKind::Fifo => FileKind::Fifo,
                    SourceNodeKind::Socket => FileKind::Socket,
                    SourceNodeKind::CharacterDevice => FileKind::CharacterDevice,
                    SourceNodeKind::BlockDevice => FileKind::BlockDevice,
                    _ => unreachable!(),
                };
                let applied = transaction
                    .create_special_measured(
                        requested,
                        kind,
                        node.device,
                        remaining_work(work, budget)?,
                        cancellation,
                    )
                    .await
                    .map_err(workspace_error)?;
                work = account_work(work, applied, budget)?;
            }
            SourceNodeKind::Unsupported => return Err(LazyWorkspaceError::UnsupportedNode),
        }
        let applied = transaction
            .set_metadata_measured(
                requested,
                source_file_metadata(node.metadata),
                remaining_work(work, budget)?,
                cancellation,
            )
            .await
            .map_err(workspace_error)?;
        work = account_work(work, applied, budget)?;
        // Directories keep their source identity too: every fork that promotes
        // one shared source directory must name it identically, or siblings
        // adding entries to it conflict as independent additions.
        {
            let applied = transaction
                .preserve_file_identity_measured(
                    requested,
                    self.source_file_id(node),
                    remaining_work(work, budget)?,
                    cancellation,
                )
                .await
                .map_err(workspace_error)?;
            work = account_work(work, applied, budget)?;
        }
        Ok(work)
    }

    async fn populate_regular_source_node(
        &self,
        transaction: &mut crate::Transaction<A, O>,
        requested: &str,
        pinned: &PinnedSourceNode,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<WorkCounters, LazyWorkspaceError> {
        let node = &pinned.node;
        let source = pinned.source;
        let length = node
            .logical_bytes
            .ok_or(LazyWorkspaceError::NotRegularFile)?;
        if length > maximum_bytes {
            return Err(LazyWorkspaceError::TooLarge);
        }
        let applied = transaction
            .create_file_measured(requested, Bytes::new(), budget, cancellation)
            .await
            .map_err(workspace_error)?;
        let mut work = account_work(WorkCounters::default(), applied, budget)?;
        let mut offset = 0_u64;
        while offset < length {
            let requested_bytes = (length - offset).min(1024 * 1024);
            let receipt = self
                .source
                .read_range(
                    source,
                    &self.namespace_path(requested)?,
                    node.version,
                    offset,
                    requested_bytes,
                    cancellation,
                )
                .await
                .map_err(|failure| LazyWorkspaceError::from(failure.error))?;
            work = account_work(work, receipt.work, budget)?;
            let chunk = receipt.value;
            if chunk.is_empty() {
                return Err(LazyWorkspaceError::Demand(DemandError::StaleVersion));
            }
            let chunk_length =
                u64::try_from(chunk.len()).map_err(|_| LazyWorkspaceError::TooLarge)?;
            let applied = transaction
                .write_range_measured(
                    requested,
                    offset,
                    chunk,
                    remaining_work(work, budget)?,
                    cancellation,
                )
                .await
                .map_err(workspace_error)?;
            work = account_work(work, applied, budget)?;
            offset = offset
                .checked_add(chunk_length)
                .ok_or(LazyWorkspaceError::TooLarge)?;
        }
        Ok(work)
    }

    async fn commit_promotion(
        &self,
        mut transaction: crate::Transaction<A, O>,
        permit: crate::PublicationPermit,
        mut work: WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<TransactionCommit<A, O>>, LazyWorkspaceError> {
        let receipt = transaction
            .commit_with_permit_measured(permit, remaining_work(work, budget)?, cancellation)
            .await
            .map_err(workspace_error)?;
        work = account_work(work, receipt.work, budget)?;
        Ok(OperationReceipt {
            value: receipt.value,
            work,
        })
    }

    async fn existing_promotion(
        &self,
        mut work: WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<TransactionCommit<A, O>>, LazyWorkspaceError> {
        let head = self
            .workspace
            .head_measured(remaining_work(work, budget)?, cancellation)
            .await
            .map_err(workspace_error)?;
        work = account_work(work, head.work, budget)?;
        Ok(OperationReceipt {
            value: TransactionCommit::AlreadyCommitted(head.value),
            work,
        })
    }

    /// Promotes one demanded node under an authority-side publication permit.
    pub async fn promote_with_permit(
        &self,
        path: &str,
        maximum_bytes: u64,
        idempotency_key: IdempotencyKey,
        permit: crate::PublicationPermit,
    ) -> Result<TransactionCommit<A, O>, LazyWorkspaceError> {
        let cancellation = CancellationToken::new();
        self.promote_with_permit_measured(
            path,
            maximum_bytes,
            idempotency_key,
            permit,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map(|receipt| receipt.value)
    }

    #[allow(clippy::too_many_lines)]
    async fn promote_with_permit_measured(
        &self,
        path: &str,
        maximum_bytes: u64,
        idempotency_key: IdempotencyKey,
        permit: crate::PublicationPermit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<TransactionCommit<A, O>>, LazyWorkspaceError> {
        let requested = self.canonical_path(path)?;
        let lookup = self
            .lookup_measured(&requested, budget, cancellation)
            .await?;
        self.promote_resolved_with_permit_measured(
            &requested,
            lookup.value,
            maximum_bytes,
            idempotency_key,
            permit,
            lookup.work,
            budget,
            cancellation,
        )
        .await
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn promote_resolved_with_permit_measured(
        &self,
        requested: &str,
        lookup: ResolvedLazyLookup,
        maximum_bytes: u64,
        idempotency_key: IdempotencyKey,
        permit: crate::PublicationPermit,
        mut work: WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<TransactionCommit<A, O>>, LazyWorkspaceError> {
        work.verify(budget)
            .map_err(|error| LazyWorkspaceError::Work(error.to_string()))?;
        let transaction = self
            .workspace
            .begin_transaction_measured(
                idempotency_key,
                remaining_work(work, budget)?,
                cancellation,
            )
            .await
            .map_err(workspace_error)?;
        work = account_work(work, transaction.work, budget)?;
        let mut transaction = transaction.value;
        if let Some(parent) = parent_path(requested) {
            let applied = transaction
                .create_dir_all_measured(parent, remaining_work(work, budget)?, cancellation)
                .await
                .map_err(workspace_error)?;
            work = account_work(work, applied, budget)?;
        }
        let ResolvedLazyLookup { lookup, source } = lookup;
        let pinned = match lookup {
            LazyLookup::Source(node) => PinnedSourceNode {
                source: source.ok_or(LazyWorkspaceError::Concurrent)?,
                node,
            },
            LazyLookup::Authored {
                path: authored_path,
                ..
            } => {
                if authored_path == requested {
                    return self.existing_promotion(work, budget, cancellation).await;
                }
                let applied = transaction
                    .hard_link_measured(
                        &authored_path,
                        requested,
                        remaining_work(work, budget)?,
                        cancellation,
                    )
                    .await
                    .map_err(workspace_error)?;
                work = account_work(work, applied, budget)?;
                return self
                    .commit_promotion(transaction, permit, work, budget, cancellation)
                    .await;
            }
            LazyLookup::Shadow { record, .. } => {
                let applied = transaction
                    .restore_record_measured(
                        requested,
                        record,
                        remaining_work(work, budget)?,
                        cancellation,
                    )
                    .await
                    .map_err(workspace_error)?;
                work = account_work(work, applied, budget)?;
                return self
                    .commit_promotion(transaction, permit, work, budget, cancellation)
                    .await;
            }
        };
        let populated = self
            .populate_source_node_measured(
                &mut transaction,
                requested,
                &pinned,
                maximum_bytes,
                remaining_work(work, budget)?,
                cancellation,
            )
            .await?;
        work = account_work(work, populated, budget)?;
        self.commit_promotion(transaction, permit, work, budget, cancellation)
            .await
    }

    /// Promotes one source node and accepts only a durable successful outcome.
    ///
    /// Mount adapters use this boundary so a typed conflict can never advance
    /// their authored view as though promotion succeeded.
    pub async fn promote_exact(
        &self,
        path: &str,
        expected_source: FileId,
        maximum_bytes: u64,
        idempotency_key: IdempotencyKey,
    ) -> Result<crate::Generation<A, O>, LazyWorkspaceError> {
        let requested = self.canonical_path(path)?;
        match self.lookup(&requested).await? {
            LazyLookup::Source(node) if self.source_file_id(&node) == expected_source => {}
            LazyLookup::Authored {
                path: authored_path,
                stat,
            } if stat.file_id == expected_source => {
                if authored_path == requested {
                    return self.workspace.head().await.map_err(workspace_error);
                }
            }
            LazyLookup::Shadow { record, .. } if record.file_id == expected_source => {}
            LazyLookup::Source(_) | LazyLookup::Authored { .. } | LazyLookup::Shadow { .. } => {
                return Err(LazyWorkspaceError::StaleIdentity);
            }
        }
        match self
            .promote(&requested, maximum_bytes, idempotency_key)
            .await?
        {
            TransactionCommit::Committed(generation)
            | TransactionCommit::AlreadyCommitted(generation) => Ok(generation),
            TransactionCommit::Conflict { .. } | TransactionCommit::Fenced => {
                Err(LazyWorkspaceError::StaleIdentity)
            }
            TransactionCommit::IdempotencyConflict => Err(LazyWorkspaceError::Concurrent),
        }
    }

    /// Returns one bounded page combining unresolved source entries with the
    /// authored overlay. Source-native order is followed by authored order;
    /// authored entries replace same-name source entries and tombstones hide
    /// source entries without forcing unrelated subtrees.
    pub async fn list_directory(
        &self,
        path: &str,
        cursor: Option<LazyDirectoryCursor>,
        maximum_entries: u32,
    ) -> Result<LazyDirectoryPage, LazyWorkspaceError> {
        let cancellation = CancellationToken::new();
        self.list_directory_measured(
            path,
            cursor,
            maximum_entries,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map(|receipt| receipt.value)
    }

    #[allow(clippy::too_many_lines)]
    async fn list_directory_measured(
        &self,
        path: &str,
        cursor: Option<LazyDirectoryCursor>,
        maximum_entries: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<LazyDirectoryPage>, LazyWorkspaceError> {
        if maximum_entries == 0 {
            return Err(LazyWorkspaceError::InvalidPageBound);
        }
        let state = self.state_measured(budget, cancellation).await?;
        let mut work = state.work;
        let state = state.value;
        let directory = self.namespace_path(path)?;
        let phase = match cursor {
            Some(cursor)
                if cursor.source == state.source
                    && cursor.overlay == state.overlay
                    && cursor.directory == directory =>
            {
                cursor.phase
            }
            Some(_) => return Err(LazyWorkspaceError::StaleCursor),
            None => LazyDirectoryPhase::Source(None),
        };
        match phase {
            LazyDirectoryPhase::Source(source_cursor) => {
                let receipt = match self
                    .source
                    .list_page(
                        state.source,
                        &directory,
                        source_cursor,
                        maximum_entries,
                        cancellation,
                    )
                    .await
                {
                    Ok(receipt) => receipt,
                    // The directory exists only in the authored generation; the
                    // source contributes no entries and the authored phase follows.
                    Err(failure) if matches!(failure.error, DemandError::Absent) => {
                        work = account_work(work, *failure.work, budget)?;
                        return Box::pin(self.list_directory_measured(
                            path,
                            Some(LazyDirectoryCursor {
                                source: state.source,
                                overlay: state.overlay,
                                directory,
                                phase: LazyDirectoryPhase::Authored(None),
                            }),
                            maximum_entries,
                            remaining_work(work, budget)?,
                            cancellation,
                        ))
                        .await
                        .map(|receipt| OperationReceipt {
                            value: receipt.value,
                            work: account_work(work, receipt.work, budget).unwrap_or(receipt.work),
                        });
                    }
                    Err(failure) => return Err(failure.error.into()),
                };
                work = account_work(work, receipt.work, budget)?;
                let page = receipt.value;
                if page.entries.len() > usize::try_from(maximum_entries).unwrap_or(usize::MAX) {
                    return Err(LazyWorkspaceError::Work(
                        "source directory page exceeded its requested bound".to_owned(),
                    ));
                }
                let temporary_bytes = retained_directory_page_bytes(
                    page.entries.capacity(),
                    std::mem::size_of::<SourceDirectoryEntry>(),
                    page.entries.iter().map(|entry| entry.name.retained_bytes()),
                )?;
                let mut entries = Vec::new();
                let projection_bytes = reserve_lazy_directory_page(
                    &mut entries,
                    page.entries.len(),
                    temporary_bytes,
                    &mut work,
                    budget,
                )?;
                for entry in page.entries {
                    let child_path = logical_child_path(path, &entry.name);
                    if let Some(child) = child_path.as_ref() {
                        work = account_transient_string(work, child, projection_bytes, budget)?;
                        let tombstoned = self
                            .tombstoned_measured(
                                state.overlay,
                                child,
                                remaining_work(work, budget)?,
                                cancellation,
                            )
                            .await?;
                        work = account_work(work, tombstoned.work, budget)?;
                        if tombstoned.value {
                            continue;
                        }
                    }
                    if let Some(child_path) = child_path {
                        let receipt = self
                            .workspace
                            .stat_optional_measured(
                                &child_path,
                                remaining_work(work, budget)?,
                                cancellation,
                            )
                            .await
                            .map_err(workspace_error)?;
                        work = account_work(work, receipt.work, budget)?;
                        if receipt.value.is_some() {
                            continue;
                        }
                    }
                    entries.push(LazyDirectoryEntry {
                        name: entry.name,
                        kind: entry.kind,
                        authored: false,
                    });
                }
                let next = Some(LazyDirectoryCursor {
                    source: state.source,
                    overlay: state.overlay,
                    directory,
                    phase: page
                        .next
                        .map_or(LazyDirectoryPhase::Authored(None), |next| {
                            LazyDirectoryPhase::Source(Some(next))
                        }),
                });
                Ok(OperationReceipt {
                    value: LazyDirectoryPage { entries, next },
                    work,
                })
            }
            LazyDirectoryPhase::Authored(after) => {
                let page = match self
                    .workspace
                    .list_directory_measured(
                        path,
                        after.as_ref(),
                        maximum_entries,
                        remaining_work(work, budget)?,
                        cancellation,
                    )
                    .await
                {
                    Ok(page) => page,
                    Err(WorkspaceError::NotFound) => {
                        return Ok(OperationReceipt {
                            value: LazyDirectoryPage {
                                entries: Vec::new(),
                                next: None,
                            },
                            work,
                        });
                    }
                    Err(error) => return Err(workspace_error(error)),
                };
                work = account_work(work, page.work, budget)?;
                let page = page.value;
                if page.entries.len() > usize::try_from(maximum_entries).unwrap_or(usize::MAX) {
                    return Err(LazyWorkspaceError::Work(
                        "authored directory page exceeded its requested bound".to_owned(),
                    ));
                }
                let next = if page.has_more {
                    page.entries.last().map(|entry| LazyDirectoryCursor {
                        source: state.source,
                        overlay: state.overlay,
                        directory,
                        phase: LazyDirectoryPhase::Authored(Some(entry.name.clone())),
                    })
                } else {
                    None
                };
                let temporary_bytes = retained_directory_page_bytes(
                    page.entries.capacity(),
                    std::mem::size_of::<crate::WorkspaceDirectoryEntry>(),
                    page.entries.iter().map(|entry| entry.name.retained_bytes()),
                )?;
                let mut entries = Vec::new();
                let _projection_bytes = reserve_lazy_directory_page(
                    &mut entries,
                    page.entries.len(),
                    temporary_bytes,
                    &mut work,
                    budget,
                )?;
                entries.extend(page.entries.into_iter().map(authored_entry));
                Ok(OperationReceipt {
                    value: LazyDirectoryPage { entries, next },
                    work,
                })
            }
        }
    }

    /// Captures every currently visible path into an exact authored generation.
    ///
    /// Enumeration completes before promotion begins so authored overlay
    /// changes cannot invalidate source cursors. Budget exhaustion and
    /// cancellation are explicit errors; callers must never present a partial
    /// capture as exact. A retry safely continues from already promoted paths.
    pub async fn exactify(
        &self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<crate::Generation<A, O>>, LazyWorkspaceError> {
        self.exactify_with_permit(budget, cancellation, crate::PublicationPermit::Unrestricted)
            .await
    }

    /// Exactifies under the supplied operation-window or reservation permit.
    #[allow(clippy::too_many_lines)]
    pub async fn exactify_with_permit(
        &self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        permit: crate::PublicationPermit,
    ) -> Result<OperationReceipt<crate::Generation<A, O>>, LazyWorkspaceError> {
        cancellation
            .check()
            .map_err(|_| LazyWorkspaceError::Cancelled)?;
        let state = self.state_measured(budget, cancellation).await?;
        let mut work = state.work;
        let head = self
            .workspace
            .head_measured(remaining_work(work, budget)?, cancellation)
            .await
            .map_err(workspace_error)?;
        work = account_work(work, head.work, budget)?;
        let capture = LazySnapshotRef::new(
            self.workspace.id(),
            head.value.id(),
            state.value.source,
            state.value.overlay,
            state.value.shadows,
        )
        .id;
        let mut retained_bytes = 0_u64;
        let mut directories = Vec::new();
        let mut paths = Vec::new();
        reserve_exactify_path_slot(&mut directories, &mut retained_bytes, &mut work, budget)?;
        let root = "/".to_owned();
        retain_exactify_string(&root, &mut retained_bytes, &mut work, budget)?;
        directories.push(root);
        let mut next_directory = 0_usize;
        while next_directory < directories.len() {
            cancellation
                .check()
                .map_err(|_| LazyWorkspaceError::Cancelled)?;
            let directory = directories
                .get_mut(next_directory)
                .map(std::mem::take)
                .ok_or(LazyWorkspaceError::Concurrent)?;
            next_directory += 1;
            let mut cursor = None;
            loop {
                cancellation
                    .check()
                    .map_err(|_| LazyWorkspaceError::Cancelled)?;
                let receipt = self
                    .list_directory_measured(
                        &directory,
                        cursor,
                        1_024,
                        remaining_work(work, budget)?,
                        cancellation,
                    )
                    .await
                    .map_err(|error| exactify_error("enumerate", &directory, error))?;
                work = account_nested_with_live_memory(work, receipt.work, retained_bytes, budget)?;
                let page = receipt.value;
                for entry in page.entries {
                    let path = logical_child_path(&directory, &entry.name)
                        .ok_or(LazyWorkspaceError::UnsupportedNode)?;
                    work.items_examined = work
                        .items_examined
                        .checked_add(1)
                        .ok_or_else(|| LazyWorkspaceError::Work("counter overflow".to_owned()))?;
                    work.verify(budget)
                        .map_err(|error| LazyWorkspaceError::Work(error.to_string()))?;
                    if entry.kind == SourceNodeKind::Directory {
                        reserve_exactify_path_slot(
                            &mut directories,
                            &mut retained_bytes,
                            &mut work,
                            budget,
                        )?;
                        let child_directory = path.clone();
                        retain_exactify_string(
                            &child_directory,
                            &mut retained_bytes,
                            &mut work,
                            budget,
                        )?;
                        directories.push(child_directory);
                    }
                    reserve_exactify_path_slot(&mut paths, &mut retained_bytes, &mut work, budget)?;
                    retain_exactify_string(&path, &mut retained_bytes, &mut work, budget)?;
                    paths.push(path);
                }
                cursor = page.next;
                if cursor.is_none() {
                    break;
                }
            }
            retained_bytes = retained_bytes
                .checked_sub(u64::try_from(directory.capacity()).unwrap_or(u64::MAX))
                .ok_or_else(|| LazyWorkspaceError::Work("counter overflow".to_owned()))?;
        }
        let directory_vector_bytes = exactify_path_vector_bytes(&directories)?;
        retained_bytes = retained_bytes
            .checked_sub(directory_vector_bytes)
            .ok_or_else(|| LazyWorkspaceError::Work("counter overflow".to_owned()))?;
        drop(directories);
        let path_vector_bytes = exactify_path_vector_bytes(&paths)?;
        paths.sort_unstable_by(|left, right| exactify_path_order(left, right));
        for path in paths {
            cancellation
                .check()
                .map_err(|_| LazyWorkspaceError::Cancelled)?;
            let receipt = self
                .inspect_snapshot_measured(&path, remaining_work(work, budget)?, cancellation)
                .await
                .map_err(|error| exactify_error("inspect", &path, error))?;
            work = account_nested_with_live_memory(work, receipt.work, retained_bytes, budget)?;
            let lookup = receipt.value;
            if matches!(lookup.lookup, LazyLookup::Source(_)) {
                work.materializations = work
                    .materializations
                    .checked_add(1)
                    .ok_or_else(|| LazyWorkspaceError::Work("counter overflow".to_owned()))?;
                work.verify(budget)
                    .map_err(|error| LazyWorkspaceError::Work(error.to_string()))?;
            }
            let receipt = self
                .promote_resolved_with_permit_measured(
                    &path,
                    lookup,
                    budget.source_bytes_read,
                    exactify_key(capture, &path),
                    permit,
                    WorkCounters::default(),
                    remaining_work(work, budget)?,
                    cancellation,
                )
                .await
                .map_err(|error| exactify_error("promote", &path, error))?;
            work = account_nested_with_live_memory(work, receipt.work, retained_bytes, budget)?;
            retained_bytes = retained_bytes
                .checked_sub(u64::try_from(path.capacity()).unwrap_or(u64::MAX))
                .ok_or_else(|| LazyWorkspaceError::Work("counter overflow".to_owned()))?;
        }
        retained_bytes = retained_bytes
            .checked_sub(path_vector_bytes)
            .ok_or_else(|| LazyWorkspaceError::Work("counter overflow".to_owned()))?;
        debug_assert_eq!(retained_bytes, 0);
        let head = self
            .workspace
            .head_measured(remaining_work(work, budget)?, cancellation)
            .await
            .map_err(workspace_error)
            .map_err(|error| exactify_error("load head", "/", error))?;
        work = account_work(work, head.work, budget)?;
        Ok(OperationReceipt {
            value: head.value,
            work,
        })
    }

    /// Creates or replaces one complete authored file.
    ///
    /// Authored state always wins over the source overlay, so no companion
    /// metadata mutation is needed. An inherited tombstone deliberately stays
    /// attached to the underlying source fact: deleting the authored
    /// replacement later must not resurrect the source file.
    pub async fn write(
        &self,
        path: &str,
        bytes: Bytes,
    ) -> Result<TransactionCommit<A, O>, LazyWorkspaceError> {
        let path = self.canonical_path(path)?;
        let source_identity = match self.lookup(&path).await {
            Ok(LazyLookup::Source(node)) => Some(self.source_file_id(&node)),
            Ok(LazyLookup::Authored { stat, .. }) => {
                self.promote_exact(&path, stat.file_id, u64::MAX, IdempotencyKey::new())
                    .await?;
                None
            }
            Ok(LazyLookup::Shadow { record, .. }) => {
                self.promote_exact(&path, record.file_id, u64::MAX, IdempotencyKey::new())
                    .await?;
                None
            }
            Err(LazyWorkspaceError::NotFound) => None,
            Err(error) => return Err(error),
        };
        let mut transaction = self
            .workspace
            .begin_transaction(IdempotencyKey::new())
            .await
            .map_err(workspace_error)?;
        if let Some(parent) = parent_path(&path) {
            transaction
                .create_dir_all(parent)
                .await
                .map_err(workspace_error)?;
        }
        transaction
            .write(&path, bytes)
            .await
            .map_err(workspace_error)?;
        if let Some(file_id) = source_identity {
            transaction
                .preserve_file_identity(&path, file_id)
                .await
                .map_err(workspace_error)?;
        }
        transaction.commit().await.map_err(workspace_error)
    }

    /// Removes one path without confusing authored deletion with unresolved source state.
    pub async fn remove(&self, path: &str) -> Result<(), LazyWorkspaceError> {
        self.remove_if(path, None).await
    }

    /// Removes one path only if it still has the caller's resolved identity.
    #[allow(clippy::too_many_lines)]
    pub async fn remove_if(
        &self,
        path: &str,
        expected: Option<FileId>,
    ) -> Result<(), LazyWorkspaceError> {
        let path = self.canonical_path(path)?;
        let mut resolved = self.lookup(&path).await?;
        if let LazyLookup::Authored {
            path: authored_path,
            stat,
        } = &resolved
            && authored_path != &path
        {
            self.promote_exact(&path, stat.file_id, u64::MAX, IdempotencyKey::new())
                .await?;
            resolved = self.lookup(&path).await?;
        }
        let actual = match &resolved {
            LazyLookup::Authored { stat, .. } => stat.file_id,
            LazyLookup::Shadow { record, .. } => record.file_id,
            LazyLookup::Source(node) => self.source_file_id(node),
        };
        if let Some(expected) = expected
            && actual != expected
        {
            return Err(LazyWorkspaceError::StaleIdentity);
        }
        let removal_kind = if matches!(resolved, LazyLookup::Authored { .. }) {
            PendingLazyRemoveKind::Authored {
                idempotency_key: IdempotencyKey::new(),
            }
        } else {
            PendingLazyRemoveKind::SourceOnly
        };
        let mut prepared = None;
        for _ in 0..MAXIMUM_STATE_RETRIES {
            let state = self.state().await?;
            let shadows = if let LazyLookup::Authored { stat, .. } = &resolved {
                let record = self
                    .workspace
                    .record_by_id(stat.file_id)
                    .await
                    .map_err(workspace_error)?;
                self.insert_shadow(state.shadows, stat.file_id, record, stat.metadata)
                    .await?
            } else {
                state.shadows
            };
            let tombstone_overlay = self
                .insert_overlay(state.overlay, path.clone(), LazyOverlayChange::Tombstone)
                .await?;
            let pending_remove = PendingLazyRemove {
                path: path.clone(),
                prior_overlay: state.overlay,
                tombstone_overlay,
                kind: removal_kind,
            };
            let replacement = LazyWorkspaceState {
                revision: state.revision.saturating_add(1),
                overlay: tombstone_overlay,
                shadows,
                pending_remove: Some(pending_remove),
                ..state.clone()
            };
            if self
                .store
                .compare_and_swap_lazy_workspace(
                    self.workspace.id(),
                    state.revision,
                    replacement.clone(),
                )
                .await
                .map_err(store_error)?
            {
                prepared = Some(replacement);
                break;
            }
        }
        let prepared = prepared.ok_or(LazyWorkspaceError::Concurrent)?;

        let removal = if let PendingLazyRemoveKind::Authored { idempotency_key } = removal_kind {
            let mut transaction = self
                .workspace
                .begin_transaction(idempotency_key)
                .await
                .map_err(workspace_error)?;
            match transaction.remove_if(&path, actual).await {
                Ok(()) => transaction.commit().await,
                Err(error) => Err(error),
            }
        } else {
            Ok(TransactionCommit::AlreadyCommitted(
                self.workspace.head().await.map_err(workspace_error)?,
            ))
        };
        let prior_overlay = prepared
            .pending_remove
            .as_ref()
            .ok_or(LazyWorkspaceError::Concurrent)?
            .prior_overlay;
        let (overlay, result) = match removal {
            Ok(TransactionCommit::Committed(_))
            | Ok(TransactionCommit::AlreadyCommitted(_))
            | Err(WorkspaceError::NotFound) => (prepared.overlay, Ok(())),
            Ok(TransactionCommit::Conflict { .. }) | Ok(TransactionCommit::Fenced) => {
                (prior_overlay, Err(LazyWorkspaceError::StaleIdentity))
            }
            Ok(TransactionCommit::IdempotencyConflict) => {
                (prior_overlay, Err(LazyWorkspaceError::Concurrent))
            }
            Err(error) => (prior_overlay, Err(workspace_error(error))),
        };
        let replacement = LazyWorkspaceState {
            revision: prepared.revision.saturating_add(1),
            overlay,
            pending_remove: None,
            ..prepared.clone()
        };
        if !self
            .store
            .compare_and_swap_lazy_workspace(self.workspace.id(), prepared.revision, replacement)
            .await
            .map_err(store_error)?
        {
            return Err(LazyWorkspaceError::Concurrent);
        }
        result
    }

    async fn state(&self) -> Result<LazyWorkspaceState, LazyWorkspaceError> {
        let state = self
            .store
            .load_lazy_workspace(self.workspace.id())
            .await
            .map_err(store_error)?
            .ok_or_else(|| LazyWorkspaceError::Store("lazy binding is absent".to_owned()))?;
        let state = self.follow_source_epoch(state).await?;
        if state.pending_remove.is_some() {
            return Err(LazyWorkspaceError::Concurrent);
        }
        Ok(state)
    }

    /// Every view of one physical root shares its source, and a refresh of
    /// that root advances the source epoch for all of them at once. A view
    /// still bound to an earlier epoch of the same source adopts the current
    /// one (as [`Self::rebind_source`] does) instead of failing: otherwise a
    /// fork racing the refresh sees every unobserved path as stale.
    async fn follow_source_epoch(
        &self,
        state: LazyWorkspaceState,
    ) -> Result<LazyWorkspaceState, LazyWorkspaceError> {
        if state.schema_version != LAZY_STATE_SCHEMA || state.workspace_id != self.workspace.id() {
            return Err(LazyWorkspaceError::StaleSource);
        }
        let live = self.source.reference();
        if state.source == live {
            return Ok(state);
        }
        if state.source.identity != live.identity || state.source.epoch > live.epoch {
            return Err(LazyWorkspaceError::StaleSource);
        }
        crate::diag!(
            crate::diagnostics::Level::Debug,
            "lazy",
            "source_epoch_followed",
            from = state.source.epoch,
            to = live.epoch,
        );
        self.rebind_source().await
    }

    async fn state_measured(
        &self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<LazyWorkspaceState>, LazyWorkspaceError> {
        let receipt = self
            .store
            .load_lazy_workspace_measured(self.workspace.id(), budget, cancellation)
            .await?;
        let state = receipt
            .value
            .ok_or_else(|| LazyWorkspaceError::Store("lazy binding is absent".to_owned()))?;
        let state = self.follow_source_epoch(state).await?;
        if state.pending_remove.is_some() {
            return Err(LazyWorkspaceError::Concurrent);
        }
        Ok(OperationReceipt {
            value: state,
            work: receipt.work,
        })
    }

    async fn recover_pending_remove(&self) -> Result<(), LazyWorkspaceError> {
        for _ in 0..MAXIMUM_STATE_RETRIES {
            let state = self
                .store
                .load_lazy_workspace(self.workspace.id())
                .await
                .map_err(store_error)?
                .ok_or_else(|| LazyWorkspaceError::Store("lazy binding is absent".to_owned()))?;
            if state.schema_version != LAZY_STATE_SCHEMA
                || state.workspace_id != self.workspace.id()
                || state.source.identity != self.source.reference().identity
            {
                return Err(LazyWorkspaceError::StaleSource);
            }
            let Some(pending) = state.pending_remove.as_ref() else {
                return Ok(());
            };
            let overlay = match pending.kind {
                PendingLazyRemoveKind::SourceOnly => pending.tombstone_overlay,
                PendingLazyRemoveKind::Authored { idempotency_key } => {
                    if self
                        .workspace
                        .operation_generation(idempotency_key)
                        .await
                        .map_err(workspace_error)?
                        .is_some()
                    {
                        pending.tombstone_overlay
                    } else {
                        pending.prior_overlay
                    }
                }
            };
            let replacement = LazyWorkspaceState {
                revision: state.revision.saturating_add(1),
                overlay,
                pending_remove: None,
                ..state.clone()
            };
            if self
                .store
                .compare_and_swap_lazy_workspace(self.workspace.id(), state.revision, replacement)
                .await
                .map_err(store_error)?
            {
                return Ok(());
            }
        }
        Err(LazyWorkspaceError::Concurrent)
    }

    async fn observe_measured(
        &self,
        path: &str,
        state: LazyWorkspaceState,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<(SourceReference, SourceNode)>, LazyWorkspaceError> {
        let path = self.canonical_path(path)?;
        if let Some(change) = self.overlay_fact(state.overlay, &path).await? {
            return match change {
                LazyOverlayChange::Observe { source, node } if source == state.source => {
                    Ok(OperationReceipt {
                        value: (source, node),
                        work: WorkCounters::default(),
                    })
                }
                LazyOverlayChange::Observe { .. } => {
                    self.observe_live_measured(path, state, cancellation).await
                }
                LazyOverlayChange::Tombstone => Err(LazyWorkspaceError::NotFound),
            };
        }
        self.observe_live_measured(path, state, cancellation).await
    }

    async fn observe_live_measured(
        &self,
        path: String,
        state: LazyWorkspaceState,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<(SourceReference, SourceNode)>, LazyWorkspaceError> {
        let receipt = self
            .source
            .lookup(state.source, &self.namespace_path(&path)?, cancellation)
            .await
            .map_err(|failure| failure.error)?;
        let node = receipt.value.ok_or(LazyWorkspaceError::NotFound)?;
        self.append_overlay(
            path,
            LazyOverlayChange::Observe {
                source: state.source,
                node,
            },
        )
        .await?;
        Ok(OperationReceipt {
            value: (state.source, node),
            work: receipt.work,
        })
    }

    async fn append_overlay(
        &self,
        path: String,
        change: LazyOverlayChange,
    ) -> Result<(), LazyWorkspaceError> {
        for _ in 0..MAXIMUM_STATE_RETRIES {
            let state = self.state().await?;
            let overlay_id = self
                .insert_overlay(state.overlay, path.clone(), change.clone())
                .await?;
            let replacement = LazyWorkspaceState {
                revision: state.revision.saturating_add(1),
                overlay: overlay_id,
                ..state.clone()
            };
            if self
                .store
                .compare_and_swap_lazy_workspace(self.workspace.id(), state.revision, replacement)
                .await
                .map_err(store_error)?
            {
                return Ok(());
            }
        }
        Err(LazyWorkspaceError::Concurrent)
    }

    fn insert_overlay<'a>(
        &'a self,
        root: LazyOverlayId,
        path: String,
        change: LazyOverlayChange,
    ) -> LazyFuture<'a, Result<LazyOverlayId, LazyWorkspaceError>> {
        Box::pin(async move {
            insert_immutable_treap(&OverlayTreap(&self.store), root, path, change).await
        })
    }

    async fn overlay_fact(
        &self,
        root: LazyOverlayId,
        path: &str,
    ) -> Result<Option<LazyOverlayChange>, LazyWorkspaceError> {
        immutable_treap_value(&OverlayTreap(&self.store), root, &path.to_owned()).await
    }

    async fn overlay_fact_measured(
        &self,
        mut root: LazyOverlayId,
        path: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<Option<LazyOverlayChange>>, LazyWorkspaceError> {
        let mut work = WorkCounters::default();
        loop {
            let receipt = self
                .store
                .load_lazy_overlay_measured(root, remaining_work(work, budget)?, cancellation)
                .await?;
            work = account_work(work, receipt.work, budget)?;
            let Some(node) = receipt.value else {
                return Ok(OperationReceipt { value: None, work });
            };
            let LazyOverlay::Node {
                path: node_path,
                change,
                left,
                right,
                ..
            } = node
            else {
                return Ok(OperationReceipt { value: None, work });
            };
            match path.cmp(node_path.as_str()) {
                std::cmp::Ordering::Equal => {
                    return Ok(OperationReceipt {
                        value: Some(change),
                        work,
                    });
                }
                std::cmp::Ordering::Less => root = left,
                std::cmp::Ordering::Greater => root = right,
            }
        }
    }

    async fn shadow_record_measured(
        &self,
        mut root: LazyShadowId,
        file_id: FileId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<Option<(FileRecord, crate::WorkspaceMetadata)>>, LazyWorkspaceError>
    {
        let mut work = WorkCounters::default();
        loop {
            let receipt = self
                .store
                .load_lazy_shadow_measured(root, remaining_work(work, budget)?, cancellation)
                .await?;
            work = account_work(work, receipt.work, budget)?;
            let Some(node) = receipt.value else {
                return Ok(OperationReceipt { value: None, work });
            };
            let LazyShadow::Node {
                file_id: node_file_id,
                record,
                metadata,
                left,
                right,
                ..
            } = node
            else {
                return Ok(OperationReceipt { value: None, work });
            };
            match file_id.cmp(&node_file_id) {
                std::cmp::Ordering::Equal => {
                    let record = crate::kernel::decode_file_record(&record)
                        .map_err(|error| LazyWorkspaceError::Store(error.to_string()))?;
                    return Ok(OperationReceipt {
                        value: Some((record, *metadata)),
                        work,
                    });
                }
                std::cmp::Ordering::Less => root = left,
                std::cmp::Ordering::Greater => root = right,
            }
        }
    }

    fn insert_shadow<'a>(
        &'a self,
        root: LazyShadowId,
        file_id: FileId,
        record: FileRecord,
        metadata: crate::WorkspaceMetadata,
    ) -> LazyFuture<'a, Result<LazyShadowId, LazyWorkspaceError>> {
        Box::pin(async move {
            insert_immutable_treap(
                &ShadowTreap(&self.store),
                root,
                file_id,
                ShadowValue {
                    record: crate::kernel::encode_file_record(record),
                    metadata: Box::new(metadata),
                },
            )
            .await
        })
    }

    async fn tombstoned_measured(
        &self,
        overlay_id: LazyOverlayId,
        path: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<bool>, LazyWorkspaceError> {
        let path = self.canonical_path(path)?;
        let path_bytes = u64::try_from(path.capacity())
            .map_err(|_| LazyWorkspaceError::Work("counter overflow".to_owned()))?;
        let mut work = WorkCounters {
            bytes_copied: u64::try_from(path.len())
                .map_err(|_| LazyWorkspaceError::Work("counter overflow".to_owned()))?,
            allocation_operations: u64::from(path_bytes != 0),
            peak_allocation_bytes: path_bytes,
            ..WorkCounters::default()
        };
        work.verify(budget)
            .map_err(|error| LazyWorkspaceError::Work(error.to_string()))?;
        let ancestor_request = path.len();
        if ancestor_request != 0 {
            let requested = u64::try_from(ancestor_request)
                .map_err(|_| LazyWorkspaceError::Work("counter overflow".to_owned()))?;
            let mut attempted = work
                .checked_add(WorkCounters {
                    bytes_copied: requested,
                    allocation_operations: 1,
                    ..WorkCounters::default()
                })
                .map_err(|error| LazyWorkspaceError::Work(error.to_string()))?;
            attempted.peak_allocation_bytes = attempted.peak_allocation_bytes.max(
                path_bytes
                    .checked_add(requested)
                    .ok_or_else(|| LazyWorkspaceError::Work("counter overflow".to_owned()))?,
            );
            attempted
                .verify(budget)
                .map_err(|error| LazyWorkspaceError::Work(error.to_string()))?;
            work = attempted;
        }
        let root = self
            .overlay_fact_measured(overlay_id, "/", remaining_work(work, budget)?, cancellation)
            .await?;
        work = account_work(work, root.work, budget)?;
        if matches!(root.value, Some(LazyOverlayChange::Tombstone)) {
            return Ok(OperationReceipt { value: true, work });
        }
        let mut ancestor = String::new();
        ancestor
            .try_reserve_exact(ancestor_request)
            .map_err(|_| LazyWorkspaceError::Work("tombstone path allocation failed".to_owned()))?;
        for component in path.trim_start_matches('/').split('/') {
            if component.is_empty() {
                continue;
            }
            ancestor.push('/');
            ancestor.push_str(component);
            let fact = self
                .overlay_fact_measured(
                    overlay_id,
                    &ancestor,
                    remaining_work(work, budget)?,
                    cancellation,
                )
                .await?;
            work = account_work(work, fact.work, budget)?;
            if matches!(fact.value, Some(LazyOverlayChange::Tombstone)) {
                return Ok(OperationReceipt { value: true, work });
            }
        }
        Ok(OperationReceipt { value: false, work })
    }

    fn canonical_path(&self, path: &str) -> Result<String, LazyWorkspaceError> {
        PortablePath::parse(path, self.workspace.limits())
            .map(|path| path.as_str().to_owned())
            .map_err(|error| LazyWorkspaceError::Workspace(error.to_string()))
    }

    fn namespace_path(&self, path: &str) -> Result<NamespacePath, LazyWorkspaceError> {
        let portable = PortablePath::parse(path, self.workspace.limits())
            .map_err(|error| LazyWorkspaceError::Workspace(error.to_string()))?;
        NamespacePath::from_portable(&portable, self.workspace.limits())
            .map_err(|error| LazyWorkspaceError::Workspace(error.to_string()))
    }
}

fn store_error(error: impl std::fmt::Display) -> LazyWorkspaceError {
    LazyWorkspaceError::Store(error.to_string())
}

fn lazy_store_read_work() -> WorkCounters {
    WorkCounters {
        object_probes: 1,
        backend_read_operations: 1,
        items_examined: 1,
        ..WorkCounters::default()
    }
}

fn lazy_store_write_work() -> WorkCounters {
    WorkCounters {
        backend_write_operations: 1,
        items_examined: 1,
        ..WorkCounters::default()
    }
}

fn measured_lazy_store_read(
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(), LazyWorkspaceError> {
    cancellation
        .check()
        .map_err(|_| LazyWorkspaceError::Cancelled)?;
    lazy_store_read_work()
        .verify(budget)
        .map_err(|error| LazyWorkspaceError::Work(error.to_string()))
}

fn measured_lazy_store_write(
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(), LazyWorkspaceError> {
    cancellation
        .check()
        .map_err(|_| LazyWorkspaceError::Cancelled)?;
    lazy_store_write_work()
        .verify(budget)
        .map_err(|error| LazyWorkspaceError::Work(error.to_string()))
}

fn workspace_error(error: WorkspaceError) -> LazyWorkspaceError {
    match error {
        WorkspaceError::StaleIdentity => LazyWorkspaceError::StaleIdentity,
        WorkspaceError::Cancelled(_) => LazyWorkspaceError::Cancelled,
        WorkspaceError::Work(error) => LazyWorkspaceError::Work(error.to_string()),
        error => LazyWorkspaceError::Workspace(error.to_string()),
    }
}

fn source_kind(kind: crate::kernel::FileKind) -> SourceNodeKind {
    match kind {
        crate::kernel::FileKind::Directory => SourceNodeKind::Directory,
        crate::kernel::FileKind::Regular => SourceNodeKind::RegularFile,
        crate::kernel::FileKind::SymbolicLink => SourceNodeKind::SymbolicLink,
        crate::kernel::FileKind::Fifo => SourceNodeKind::Fifo,
        crate::kernel::FileKind::Socket => SourceNodeKind::Socket,
        crate::kernel::FileKind::CharacterDevice => SourceNodeKind::CharacterDevice,
        crate::kernel::FileKind::BlockDevice => SourceNodeKind::BlockDevice,
        crate::kernel::FileKind::ReparsePoint | crate::kernel::FileKind::MountBoundary => {
            SourceNodeKind::Unsupported
        }
    }
}

fn record_logical_bytes(record: FileRecord) -> Option<u64> {
    match record.payload {
        FilePayload::InlineRegular(bytes) => u64::try_from(bytes.as_bytes().len()).ok(),
        FilePayload::Regular { logical_bytes, .. }
        | FilePayload::SymbolicLink {
            target_bytes: logical_bytes,
            ..
        }
        | FilePayload::ReparsePoint {
            payload_bytes: logical_bytes,
            ..
        } => Some(logical_bytes),
        FilePayload::Directory { .. } | FilePayload::Empty | FilePayload::Device { .. } => None,
    }
}

fn source_file_metadata(source: crate::demand::SourceMetadata) -> FileMetadata {
    let time = |value: Option<i128>| {
        value
            .and_then(|value| i64::try_from(value).ok())
            .map_or(MetadataField::Unavailable, MetadataField::Value)
    };
    FileMetadata {
        posix_mode: metadata_u32(source.posix_mode),
        posix_uid: metadata_u32(source.posix_uid),
        posix_gid: metadata_u32(source.posix_gid),
        posix_flags: metadata_u64(source.posix_flags),
        windows_attributes: metadata_u32(source.windows_attributes),
        created_ns: time(source.created_ns),
        modified_ns: time(source.modified_ns),
        accessed_ns: time(source.accessed_ns),
        changed_ns: time(source.changed_ns),
        ..FileMetadata::default()
    }
}

fn metadata_u32(value: Option<u32>) -> MetadataField<u32> {
    value.map_or(MetadataField::Unavailable, MetadataField::Value)
}

fn metadata_u64(value: Option<u64>) -> MetadataField<u64> {
    value.map_or(MetadataField::Unavailable, MetadataField::Value)
}

fn path_priority(path: &str) -> u64 {
    let digest = blake3::hash(path.as_bytes());
    let mut bytes = [0_u8; 8];
    if let Some(prefix) = digest.as_bytes().get(..8) {
        bytes.copy_from_slice(prefix);
    }
    u64::from_be_bytes(bytes)
}

fn shadow_priority(file_id: FileId) -> u64 {
    let digest = blake3::hash(&file_id.into_bytes());
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&digest.as_bytes()[..8]);
    u64::from_le_bytes(bytes)
}

fn parent_path(path: &str) -> Option<&str> {
    let index = path.rfind('/')?;
    if index == 0 {
        Some("/")
    } else {
        path.get(..index)
    }
}

fn authored_entry(entry: WorkspaceDirectoryEntry) -> LazyDirectoryEntry {
    LazyDirectoryEntry {
        name: entry.name,
        kind: source_kind(entry.kind),
        authored: true,
    }
}

fn logical_child_path(parent: &str, name: &LogicalName) -> Option<String> {
    let component = match name.encoding() {
        NameEncoding::Utf8 | NameEncoding::PosixBytes => {
            std::str::from_utf8(name.as_bytes()).ok()?.to_owned()
        }
        NameEncoding::WindowsUtf16Le => {
            let units = name
                .as_bytes()
                .chunks_exact(2)
                .filter_map(|pair| pair.try_into().ok().map(u16::from_le_bytes));
            char::decode_utf16(units)
                .collect::<Result<String, _>>()
                .ok()?
        }
    };
    Some(if parent == "/" {
        format!("/{component}")
    } else {
        format!("{parent}/{component}")
    })
}

fn exactify_path_order(left: &str, right: &str) -> std::cmp::Ordering {
    left.matches('/')
        .count()
        .cmp(&right.matches('/').count())
        .then_with(|| left.cmp(right))
}

fn account_work(
    current: WorkCounters,
    additional: WorkCounters,
    budget: WorkBudget,
) -> Result<WorkCounters, LazyWorkspaceError> {
    let combined = current
        .checked_add(additional)
        .map_err(|error| LazyWorkspaceError::Work(error.to_string()))?;
    combined
        .verify(budget)
        .map_err(|error| LazyWorkspaceError::Work(error.to_string()))?;
    Ok(combined)
}

fn account_nested_with_live_memory(
    current: WorkCounters,
    mut nested: WorkCounters,
    live_bytes: u64,
    budget: WorkBudget,
) -> Result<WorkCounters, LazyWorkspaceError> {
    let simultaneous_peak = live_bytes
        .checked_add(nested.peak_allocation_bytes)
        .ok_or_else(|| LazyWorkspaceError::Work("counter overflow".to_owned()))?;
    nested.peak_allocation_bytes = 0;
    let mut combined = current
        .checked_add(nested)
        .map_err(|error| LazyWorkspaceError::Work(error.to_string()))?;
    combined.peak_allocation_bytes = combined.peak_allocation_bytes.max(simultaneous_peak);
    combined
        .verify(budget)
        .map_err(|error| LazyWorkspaceError::Work(error.to_string()))?;
    Ok(combined)
}

fn reserve_exactify_path_slot(
    paths: &mut Vec<String>,
    retained_bytes: &mut u64,
    work: &mut WorkCounters,
    budget: WorkBudget,
) -> Result<(), LazyWorkspaceError> {
    if paths.len() < paths.capacity() {
        return Ok(());
    }
    let item_bytes = u64::try_from(std::mem::size_of::<String>())
        .map_err(|_| LazyWorkspaceError::Work("counter overflow".to_owned()))?;
    let old_bytes = u64::try_from(paths.capacity())
        .ok()
        .and_then(|count| count.checked_mul(item_bytes))
        .ok_or_else(|| LazyWorkspaceError::Work("counter overflow".to_owned()))?;
    let target = paths
        .len()
        .checked_add(1)
        .ok_or_else(|| LazyWorkspaceError::Work("counter overflow".to_owned()))?;
    let requested_bytes = u64::try_from(target)
        .ok()
        .and_then(|count| count.checked_mul(item_bytes))
        .ok_or_else(|| LazyWorkspaceError::Work("counter overflow".to_owned()))?;
    let copied = u64::try_from(paths.len())
        .ok()
        .and_then(|count| count.checked_mul(item_bytes))
        .ok_or_else(|| LazyWorkspaceError::Work("counter overflow".to_owned()))?;
    let mut attempted = work
        .checked_add(WorkCounters {
            bytes_copied: copied,
            allocation_operations: 1,
            ..WorkCounters::default()
        })
        .map_err(|error| LazyWorkspaceError::Work(error.to_string()))?;
    attempted.peak_allocation_bytes = attempted.peak_allocation_bytes.max(
        retained_bytes
            .checked_add(requested_bytes)
            .ok_or_else(|| LazyWorkspaceError::Work("counter overflow".to_owned()))?,
    );
    attempted
        .verify(budget)
        .map_err(|error| LazyWorkspaceError::Work(error.to_string()))?;
    paths
        .try_reserve_exact(1)
        .map_err(|_| LazyWorkspaceError::Work("exactify path allocation failed".to_owned()))?;
    let new_bytes = u64::try_from(paths.capacity())
        .ok()
        .and_then(|count| count.checked_mul(item_bytes))
        .ok_or_else(|| LazyWorkspaceError::Work("counter overflow".to_owned()))?;
    *retained_bytes = retained_bytes
        .checked_sub(old_bytes)
        .and_then(|value| value.checked_add(new_bytes))
        .ok_or_else(|| LazyWorkspaceError::Work("counter overflow".to_owned()))?;
    attempted.peak_allocation_bytes = attempted.peak_allocation_bytes.max(*retained_bytes);
    attempted
        .verify(budget)
        .map_err(|error| LazyWorkspaceError::Work(error.to_string()))?;
    *work = attempted;
    Ok(())
}

fn account_transient_string(
    work: WorkCounters,
    value: &String,
    live_bytes: u64,
    budget: WorkBudget,
) -> Result<WorkCounters, LazyWorkspaceError> {
    let bytes = u64::try_from(value.capacity())
        .map_err(|_| LazyWorkspaceError::Work("counter overflow".to_owned()))?;
    let copied = u64::try_from(value.len())
        .map_err(|_| LazyWorkspaceError::Work("counter overflow".to_owned()))?;
    let mut delta = WorkCounters {
        bytes_copied: copied,
        allocation_operations: u64::from(bytes != 0),
        ..WorkCounters::default()
    };
    delta.peak_allocation_bytes = live_bytes
        .checked_add(bytes)
        .ok_or_else(|| LazyWorkspaceError::Work("counter overflow".to_owned()))?;
    account_work(work, delta, budget)
}

fn retained_directory_page_bytes(
    capacity: usize,
    entry_bytes: usize,
    mut names: impl Iterator<Item = usize>,
) -> Result<u64, LazyWorkspaceError> {
    let slots = capacity
        .checked_mul(entry_bytes)
        .ok_or_else(|| LazyWorkspaceError::Work("counter overflow".to_owned()))?;
    let names = names
        .try_fold(0_usize, |total, bytes| total.checked_add(bytes))
        .ok_or_else(|| LazyWorkspaceError::Work("counter overflow".to_owned()))?;
    u64::try_from(
        slots
            .checked_add(names)
            .ok_or_else(|| LazyWorkspaceError::Work("counter overflow".to_owned()))?,
    )
    .map_err(|_| LazyWorkspaceError::Work("counter overflow".to_owned()))
}

fn reserve_lazy_directory_page(
    entries: &mut Vec<LazyDirectoryEntry>,
    count: usize,
    temporary_bytes: u64,
    work: &mut WorkCounters,
    budget: WorkBudget,
) -> Result<u64, LazyWorkspaceError> {
    let requested = count
        .checked_mul(std::mem::size_of::<LazyDirectoryEntry>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| LazyWorkspaceError::Work("counter overflow".to_owned()))?;
    let mut attempted = work
        .checked_add(WorkCounters {
            allocation_operations: u64::from(count != 0),
            ..WorkCounters::default()
        })
        .map_err(|error| LazyWorkspaceError::Work(error.to_string()))?;
    attempted.peak_allocation_bytes = attempted.peak_allocation_bytes.max(
        temporary_bytes
            .checked_add(requested)
            .ok_or_else(|| LazyWorkspaceError::Work("counter overflow".to_owned()))?,
    );
    attempted
        .verify(budget)
        .map_err(|error| LazyWorkspaceError::Work(error.to_string()))?;
    entries
        .try_reserve_exact(count)
        .map_err(|_| LazyWorkspaceError::Work("directory page allocation failed".to_owned()))?;
    let allocated = entries
        .capacity()
        .checked_mul(std::mem::size_of::<LazyDirectoryEntry>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| LazyWorkspaceError::Work("counter overflow".to_owned()))?;
    attempted.peak_allocation_bytes = attempted.peak_allocation_bytes.max(
        temporary_bytes
            .checked_add(allocated)
            .ok_or_else(|| LazyWorkspaceError::Work("counter overflow".to_owned()))?,
    );
    attempted
        .verify(budget)
        .map_err(|error| LazyWorkspaceError::Work(error.to_string()))?;
    *work = attempted;
    temporary_bytes
        .checked_add(allocated)
        .ok_or_else(|| LazyWorkspaceError::Work("counter overflow".to_owned()))
}

fn exactify_path_vector_bytes(paths: &Vec<String>) -> Result<u64, LazyWorkspaceError> {
    u64::try_from(paths.capacity())
        .ok()
        .and_then(|count| count.checked_mul(u64::try_from(std::mem::size_of::<String>()).ok()?))
        .ok_or_else(|| LazyWorkspaceError::Work("counter overflow".to_owned()))
}

fn retain_exactify_string(
    value: &String,
    retained_bytes: &mut u64,
    work: &mut WorkCounters,
    budget: WorkBudget,
) -> Result<(), LazyWorkspaceError> {
    let capacity = u64::try_from(value.capacity())
        .map_err(|_| LazyWorkspaceError::Work("counter overflow".to_owned()))?;
    let mut delta = WorkCounters {
        bytes_copied: u64::try_from(value.len())
            .map_err(|_| LazyWorkspaceError::Work("counter overflow".to_owned()))?,
        allocation_operations: u64::from(capacity != 0),
        ..WorkCounters::default()
    };
    *retained_bytes = retained_bytes
        .checked_add(capacity)
        .ok_or_else(|| LazyWorkspaceError::Work("counter overflow".to_owned()))?;
    delta.peak_allocation_bytes = *retained_bytes;
    *work = account_work(*work, delta, budget)?;
    Ok(())
}

fn remaining_work(
    current: WorkCounters,
    budget: WorkBudget,
) -> Result<WorkBudget, LazyWorkspaceError> {
    current
        .remaining(budget)
        .map_err(|error| LazyWorkspaceError::Work(error.to_string()))
}

fn exactify_error(stage: &str, path: &str, error: LazyWorkspaceError) -> LazyWorkspaceError {
    match error {
        LazyWorkspaceError::Cancelled | LazyWorkspaceError::Demand(DemandError::Cancelled) => {
            LazyWorkspaceError::Cancelled
        }
        error @ LazyWorkspaceError::Work(_) => error,
        error => {
            LazyWorkspaceError::Workspace(format!("exactify could not {stage} {path}: {error}"))
        }
    }
}

fn exactify_key(snapshot_id: LazySnapshotId, path: &str) -> IdempotencyKey {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-fs-lazy-exactify-v1\0");
    hasher.update(&snapshot_id.into_bytes());
    hasher.update(path.as_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    IdempotencyKey::from_bytes(bytes)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::demand::{
        DemandResult, SourceCursor, SourceDirectoryEntry, SourceDirectoryPage, SourceMetadata,
        SourceVersion,
    };
    use crate::performance::{OperationFailure, OperationReceipt, WorkCounters};
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

    #[derive(Default)]
    struct SourceCounts {
        lookups: AtomicUsize,
        pages: AtomicUsize,
        ranges: AtomicUsize,
    }

    struct CountingSource {
        identity: [u8; 16],
        epoch: AtomicU64,
        node: SourceNode,
        versions: Mutex<BTreeMap<u64, Bytes>>,
        directory_entries: usize,
        counts: SourceCounts,
        cancel_on_page: AtomicBool,
    }

    impl CountingSource {
        fn new(bytes: Bytes) -> Self {
            let logical_bytes = bytes.len() as u64;
            let versions = BTreeMap::from([(3, bytes)]);
            Self {
                identity: [7; 16],
                epoch: AtomicU64::new(3),
                node: SourceNode {
                    kind: SourceNodeKind::RegularFile,
                    file_identity: [8; 32],
                    link_count: Some(1),
                    device: None,
                    logical_bytes: Some(logical_bytes),
                    version: SourceVersion([9; 32]),
                    metadata: SourceMetadata::default(),
                },
                versions: Mutex::new(versions),
                directory_entries: 1,
                counts: SourceCounts::default(),
                cancel_on_page: AtomicBool::new(false),
            }
        }

        fn receipt<T>(value: T) -> DemandResult<T> {
            Ok(OperationReceipt {
                value,
                work: WorkCounters::default(),
            })
        }

        fn invalidate(&self) {
            let prior = self.epoch.fetch_add(1, Ordering::Relaxed);
            let mut versions = self
                .versions
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let bytes = versions.get(&prior).cloned().unwrap_or_default();
            versions.insert(prior + 1, bytes);
        }

        fn replace(&self, bytes: Bytes) {
            let epoch = self.epoch.fetch_add(1, Ordering::Relaxed) + 1;
            self.versions
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert(epoch, bytes);
        }

        fn versioned_node(&self, source: SourceReference) -> Option<SourceNode> {
            if source.identity != self.identity {
                return None;
            }
            let versions = self
                .versions
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let bytes = versions.get(&source.epoch)?;
            Some(SourceNode {
                logical_bytes: Some(bytes.len() as u64),
                version: SourceVersion(*blake3::hash(bytes).as_bytes()),
                ..self.node
            })
        }

        fn with_directory_entries(mut self, entries: usize) -> Self {
            self.directory_entries = entries;
            self
        }

        fn cancel_next_page(&self) {
            self.cancel_on_page.store(true, Ordering::Relaxed);
        }
    }

    #[async_trait]
    impl DemandSource for CountingSource {
        fn reference(&self) -> SourceReference {
            SourceReference {
                identity: self.identity,
                epoch: self.epoch.load(Ordering::Relaxed),
            }
        }

        async fn lookup(
            &self,
            source: SourceReference,
            _path: &NamespacePath,
            _cancellation: &CancellationToken,
        ) -> DemandResult<Option<SourceNode>> {
            self.counts.lookups.fetch_add(1, Ordering::Relaxed);
            let Some(node) = self.versioned_node(source) else {
                return Err(OperationFailure::before_work(DemandError::StaleSource));
            };
            Self::receipt(Some(node))
        }

        async fn list_page(
            &self,
            source: SourceReference,
            _directory: &NamespacePath,
            cursor: Option<SourceCursor>,
            _maximum_entries: u32,
            cancellation: &CancellationToken,
        ) -> DemandResult<SourceDirectoryPage> {
            self.counts.pages.fetch_add(1, Ordering::Relaxed);
            if self.cancel_on_page.swap(false, Ordering::Relaxed) {
                cancellation.cancel();
                return Err(OperationFailure::before_work(DemandError::Cancelled));
            }
            if self.versioned_node(source).is_none() {
                return Err(OperationFailure::before_work(DemandError::StaleSource));
            }
            if cursor.is_some() {
                return Err(OperationFailure::before_work(DemandError::StaleCursor));
            }
            let entries = if self.directory_entries == 1 {
                vec![SourceDirectoryEntry {
                    name: LogicalName::new(NameEncoding::Utf8, b"file.txt".to_vec(), 255)
                        .expect("name"),
                    kind: SourceNodeKind::RegularFile,
                }]
            } else {
                (0..self.directory_entries)
                    .map(|index| SourceDirectoryEntry {
                        name: LogicalName::new(
                            NameEncoding::Utf8,
                            format!("file-{index:04}.txt").into_bytes(),
                            255,
                        )
                        .expect("name"),
                        kind: SourceNodeKind::RegularFile,
                    })
                    .collect()
            };
            Self::receipt(SourceDirectoryPage {
                entries,
                next: None,
                version: SourceVersion([4; 32]),
            })
        }

        async fn read_range(
            &self,
            source: SourceReference,
            _path: &NamespacePath,
            expected: SourceVersion,
            offset: u64,
            length: u64,
            _cancellation: &CancellationToken,
        ) -> DemandResult<Bytes> {
            self.counts.ranges.fetch_add(1, Ordering::Relaxed);
            let Some(node) = self.versioned_node(source) else {
                return Err(OperationFailure::before_work(DemandError::StaleSource));
            };
            if expected != node.version {
                return Err(OperationFailure::before_work(DemandError::StaleVersion));
            }
            let versions = self
                .versions
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let bytes = versions
                .get(&source.epoch)
                .ok_or_else(|| OperationFailure::before_work(DemandError::StaleSource))?;
            let start = usize::try_from(offset)
                .unwrap_or(usize::MAX)
                .min(bytes.len());
            let end = start
                .saturating_add(usize::try_from(length).unwrap_or(usize::MAX))
                .min(bytes.len());
            Self::receipt(bytes.slice(start..end))
        }

        async fn read_link(
            &self,
            _source: SourceReference,
            _path: &NamespacePath,
            _expected: SourceVersion,
            _cancellation: &CancellationToken,
        ) -> DemandResult<Bytes> {
            Err(OperationFailure::before_work(DemandError::InvalidRequest))
        }
    }

    #[tokio::test]
    async fn exactify_threads_cancellation_into_nested_source_work() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"content")));
        let root = LazyWorkspace::attach(
            &fs,
            "cancelled-exactify",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let cancellation = CancellationToken::new();
        source.cancel_next_page();

        assert!(matches!(
            root.exactify(WorkBudget::UNBOUNDED, &cancellation).await,
            Err(LazyWorkspaceError::Cancelled)
        ));
        assert!(cancellation.is_cancelled());
        assert_eq!(source.counts.pages.load(Ordering::Relaxed), 1);
        assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 0);
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn exactify_budget_exhaustion_cannot_publish_the_current_path() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"content")));
        let root = LazyWorkspace::attach(
            &fs,
            "budgeted-exactify",
            source,
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let before = root
            .workspace()
            .head()
            .await
            .expect("head before exactify")
            .id();
        let mut budget = WorkBudget::UNBOUNDED;
        budget.authority_records_appended = 0;

        assert!(matches!(
            root.exactify(budget, &CancellationToken::new()).await,
            Err(LazyWorkspaceError::Workspace(_))
        ));
        assert_eq!(
            root.workspace()
                .head()
                .await
                .expect("head after rejection")
                .id(),
            before
        );
        assert!(matches!(
            root.workspace().stat("/file.txt").await,
            Err(WorkspaceError::NotFound)
        ));
    }

    #[tokio::test]
    async fn exactify_snapshot_rejects_before_destination_publication() {
        let fs = Fs::memory();
        let root = LazyWorkspace::attach(
            &fs,
            "snapshot-source",
            Arc::new(CountingSource::new(Bytes::from_static(b"content"))),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let snapshot = root.snapshot().await.expect("snapshot");

        let cancelled = CancellationToken::new();
        cancelled.cancel();
        assert!(matches!(
            root.exactify_snapshot(
                snapshot,
                "cancelled-destination",
                IdempotencyKey::from_bytes([0x91; 16]),
                WorkBudget::UNBOUNDED,
                &cancelled,
            )
            .await,
            Err(LazyWorkspaceError::Cancelled)
        ));
        assert!(fs.open_workspace("cancelled-destination").await.is_err());

        assert!(matches!(
            root.exactify_snapshot(
                snapshot,
                "budget-destination",
                IdempotencyKey::from_bytes([0x92; 16]),
                WorkBudget::default(),
                &CancellationToken::new(),
            )
            .await,
            Err(LazyWorkspaceError::Work(_))
        ));
        assert!(fs.open_workspace("budget-destination").await.is_err());
    }

    #[tokio::test]
    async fn full_directory_page_peak_is_admitted_before_projection() {
        let fs = Fs::memory();
        let root = LazyWorkspace::attach(
            &fs,
            "page-memory",
            Arc::new(
                CountingSource::new(Bytes::from_static(b"content")).with_directory_entries(1_024),
            ),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let receipt = root
            .list_directory_measured(
                "/",
                None,
                1_024,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .expect("measured page");
        assert_eq!(receipt.value.entries.len(), 1_024);
        assert!(receipt.work.allocation_operations >= 1);

        let mut insufficient = WorkBudget::UNBOUNDED;
        insufficient.peak_allocation_bytes = receipt.work.peak_allocation_bytes - 1;
        assert!(matches!(
            root.list_directory_measured(
                "/",
                None,
                1_024,
                insufficient,
                &CancellationToken::new(),
            )
            .await,
            Err(LazyWorkspaceError::Work(_))
        ));
    }

    #[tokio::test]
    async fn attach_open_and_fork_demand_no_source_work() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"abcdef")));
        let store = MemoryLazyWorkspaceStore::default();
        let root = LazyWorkspace::attach(&fs, "root", Arc::clone(&source), store.clone())
            .await
            .expect("attach");
        let reopened = LazyWorkspace::open(
            fs.open_workspace("root").await.expect("workspace"),
            Arc::clone(&source),
            store.clone(),
        )
        .await
        .expect("open");
        let child = reopened
            .fork("child", IdempotencyKey::from_bytes([5; 16]))
            .await
            .expect("fork");
        let first_snapshot = child.snapshot().await.expect("snapshot");
        let second_snapshot = child.snapshot().await.expect("repeat snapshot");
        let resumed_child = root
            .open_related(fs.open_workspace("child").await.expect("child workspace"))
            .await
            .expect("resume related child");
        let sibling = root
            .fork("sibling", IdempotencyKey::from_bytes([6; 16]))
            .await
            .expect("sibling fork");
        let resumed_sibling = resumed_child
            .open_related(
                fs.open_workspace("sibling")
                    .await
                    .expect("sibling workspace"),
            )
            .await
            .expect("constant-work branch switch");
        assert_eq!(
            sibling.snapshot().await.expect("sibling snapshot"),
            resumed_sibling
                .snapshot()
                .await
                .expect("resumed sibling snapshot")
        );

        assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 0);
        assert_eq!(source.counts.pages.load(Ordering::Relaxed), 0);
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
        assert_eq!(
            child
                .state()
                .await
                .expect("child state")
                .parent_workspace_id,
            Some(root.workspace().id())
        );
        assert_eq!(first_snapshot, second_snapshot);
    }

    #[tokio::test]
    async fn exact_observation_is_shared_and_content_is_range_demanded() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"abcdef")));
        let store = MemoryLazyWorkspaceStore::default();
        let root = LazyWorkspace::attach(&fs, "root", Arc::clone(&source), store)
            .await
            .expect("attach");

        assert_eq!(
            root.stat("/deep/file.txt")
                .await
                .expect("stat")
                .logical_bytes,
            Some(6)
        );
        assert_eq!(
            root.stat("/deep/file.txt")
                .await
                .expect("cached stat")
                .logical_bytes,
            Some(6)
        );
        assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 1);
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
        assert_eq!(
            root.read_range("/deep/file.txt", 2, 3)
                .await
                .expect("range"),
            Bytes::from_static(b"cde")
        );
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 1);
        assert_eq!(source.counts.pages.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn independent_alias_promotions_preserve_source_hard_links() {
        let fs = Fs::memory();
        let mut source = CountingSource::new(Bytes::from_static(b"shared"));
        source.node.link_count = Some(2);
        source.node.metadata.posix_mode = Some(0o100640);
        source.node.metadata.modified_ns = Some(123_456);
        let source = Arc::new(source);
        let root = LazyWorkspace::attach(
            &fs,
            "hard-link-promotions",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");

        root.promote("/a", 64, IdempotencyKey::from_bytes([0x71; 16]))
            .await
            .expect("promote first alias");
        root.promote("/b", 64, IdempotencyKey::from_bytes([0x72; 16]))
            .await
            .expect("promote second alias");
        let first = root.workspace().stat("/a").await.expect("first stat");
        let second = root.workspace().stat("/b").await.expect("second stat");
        assert_eq!(first.file_id, second.file_id);
        assert_eq!(first.file_id, root.source_file_id(&source.node));
        assert_eq!(first.link_count, 2);
        assert_eq!(second.link_count, 2);
        assert_eq!(first.metadata.posix_mode, Some(0o100640));
        assert_eq!(second.metadata.modified_ns, Some(123_456));

        root.write("/a", Bytes::from_static(b"changed"))
            .await
            .expect("mutate alias");
        assert_eq!(
            root.workspace()
                .read("/b", 64)
                .await
                .expect("read other alias")
                .as_ref(),
            b"changed"
        );
    }

    #[tokio::test]
    async fn unresolved_alias_reads_and_promotes_the_latest_authored_identity() {
        let fs = Fs::memory();
        let mut source = CountingSource::new(Bytes::from_static(b"source"));
        source.node.link_count = Some(2);
        let source = Arc::new(source);
        let root = LazyWorkspace::attach(
            &fs,
            "hard-link-late-alias",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");

        root.promote("/a", 64, IdempotencyKey::from_bytes([0x73; 16]))
            .await
            .expect("promote first alias");
        root.write("/a", Bytes::from_static(b"changed"))
            .await
            .expect("mutate first alias");
        let source_reads = source.counts.ranges.load(Ordering::Relaxed);
        assert_eq!(
            root.read("/b", 64).await.expect("read alias").as_ref(),
            b"changed"
        );
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), source_reads);

        root.promote("/b", 64, IdempotencyKey::from_bytes([0x74; 16]))
            .await
            .expect("promote late alias");
        let first = root.workspace().stat("/a").await.expect("first stat");
        let second = root.workspace().stat("/b").await.expect("second stat");
        assert_eq!(first.file_id, second.file_id);
        assert_eq!(first.link_count, 2);
        assert_eq!(
            root.read("/b", 64)
                .await
                .expect("read promoted alias")
                .as_ref(),
            b"changed"
        );
    }

    #[tokio::test]
    async fn identity_shadow_survives_last_promoted_alias_removal() {
        let fs = Fs::memory();
        let mut source = CountingSource::new(Bytes::from_static(b"source"));
        source.node.link_count = Some(2);
        let source = Arc::new(source);
        let store = MemoryLazyWorkspaceStore::default();
        let root =
            LazyWorkspace::attach(&fs, "hard-link-shadow", Arc::clone(&source), store.clone())
                .await
                .expect("attach");

        root.write("/a", Bytes::from_static(b"changed"))
            .await
            .expect("direct identity-preserving write");
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
        root.remove("/a").await.expect("remove promoted alias");
        assert_eq!(
            root.read("/b", 64)
                .await
                .expect("read shadow alias")
                .as_ref(),
            b"changed"
        );

        let reopened = LazyWorkspace::open(
            fs.open_workspace("hard-link-shadow")
                .await
                .expect("workspace"),
            source,
            store,
        )
        .await
        .expect("reopen");
        assert_eq!(
            reopened
                .read("/b", 64)
                .await
                .expect("reopened shadow")
                .as_ref(),
            b"changed"
        );
    }

    #[tokio::test]
    async fn remove_if_rejects_a_stale_source_identity() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &fs,
            "stale-remove",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let stale = FileId::from_bytes([0xff; 16]);
        assert!(matches!(
            root.remove_if("/file.txt", Some(stale)).await,
            Err(LazyWorkspaceError::StaleIdentity)
        ));
        assert!(root.stat("/file.txt").await.is_ok());
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn metadata_inspection_does_not_stale_a_native_directory_cursor() {
        use crate::demand::native::NativeDemandSource;
        use crate::model::{FilesystemProfile, VolumeLimits};

        let directory = tempfile::tempdir().expect("temporary source");
        std::fs::write(directory.path().join("a.txt"), b"a").expect("first file");
        std::fs::write(directory.path().join("b.txt"), b"b").expect("second file");
        let source = Arc::new(
            NativeDemandSource::open(
                directory.path(),
                FilesystemProfile::Portable,
                VolumeLimits::default(),
            )
            .await
            .expect("source"),
        );
        let fs = Fs::memory();
        let root = LazyWorkspace::attach(
            &fs,
            "paged-source",
            source,
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let first = root.list_directory("/", None, 1).await.expect("first page");
        let name = std::str::from_utf8(first.entries[0].name.as_bytes()).expect("portable name");
        root.inspect(&format!("/{name}"))
            .await
            .expect("entry metadata");
        let second = root
            .list_directory("/", first.next, 1)
            .await
            .expect("second page remains valid");
        assert_eq!(second.entries.len(), 1);
    }

    #[tokio::test]
    async fn unresolved_aliases_have_one_stable_identity_and_dense_seek_needs_no_content() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"abcdef")));
        let root = LazyWorkspace::attach(
            &fs,
            "root",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let first = match root.lookup("/first").await.expect("first") {
            LazyLookup::Source(node) => node,
            LazyLookup::Authored { .. } | LazyLookup::Shadow { .. } => {
                panic!("source lookup expected")
            }
        };
        let second = match root.lookup("/second").await.expect("second") {
            LazyLookup::Source(node) => node,
            LazyLookup::Authored { .. } | LazyLookup::Shadow { .. } => {
                panic!("source lookup expected")
            }
        };
        assert_eq!(root.source_file_id(&first), root.source_file_id(&second));
        assert_eq!(
            root.seek("/first", 2, LazySeekTarget::Data)
                .await
                .expect("seek data"),
            Some(2)
        );
        assert_eq!(
            root.seek("/first", 2, LazySeekTarget::Hole)
                .await
                .expect("seek hole"),
            Some(6)
        );
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn promotion_demands_only_the_selected_file_and_is_idempotent() {
        let fs = Fs::memory();
        let bytes = Bytes::from(vec![b'x'; 1024 * 1024 + 17]);
        let source = Arc::new(CountingSource::new(bytes.clone()));
        let root = LazyWorkspace::attach(
            &fs,
            "root",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let key = IdempotencyKey::from_bytes([21; 16]);

        let first = root
            .promote("/deep/file.txt", bytes.len() as u64, key)
            .await
            .expect("promote");
        assert!(matches!(first, TransactionCommit::Committed(_)));
        assert_eq!(
            root.workspace()
                .read("/deep/file.txt", bytes.len() as u64)
                .await
                .expect("authored content"),
            bytes
        );
        assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 1);
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 2);
        assert_eq!(source.counts.pages.load(Ordering::Relaxed), 0);

        let second = root
            .promote("/deep/file.txt", 0, key)
            .await
            .expect("idempotent promotion");
        assert!(matches!(second, TransactionCommit::AlreadyCommitted(_)));
        assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 1);
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn exact_promotion_accepts_an_identity_preserving_direct_write() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &fs,
            "root",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let source_node = match root.lookup("/file.txt").await.expect("source lookup") {
            LazyLookup::Source(node) => node,
            LazyLookup::Authored { .. } | LazyLookup::Shadow { .. } => {
                panic!("expected source node")
            }
        };
        let expected_source = root.source_file_id(&source_node);
        root.write("/file.txt", Bytes::from_static(b"replacement"))
            .await
            .expect("authored replacement");

        root.promote_exact(
            "/file.txt",
            expected_source,
            64,
            IdempotencyKey::from_bytes([0x73; 16]),
        )
        .await
        .expect("identity-preserving write is already promoted");
        assert_eq!(
            root.read("/file.txt", 64)
                .await
                .expect("replacement remains"),
            Bytes::from_static(b"replacement")
        );
    }

    #[tokio::test]
    async fn authored_state_and_tombstones_mask_the_source() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &fs,
            "root",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");

        root.write("/file.txt", Bytes::from_static(b"authored"))
            .await
            .expect("write");
        assert_eq!(
            root.read("/file.txt", 32).await.expect("authored read"),
            Bytes::from_static(b"authored")
        );
        assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 1);
        root.remove("/file.txt").await.expect("remove");
        assert!(matches!(
            root.stat("/file.txt").await,
            Err(LazyWorkspaceError::NotFound)
        ));
        assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn source_epoch_can_rebind_without_enumeration() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &fs,
            "root",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        root.stat("/observed.txt").await.expect("observe");
        let before = root.snapshot().await.expect("snapshot before rebind");
        source.invalidate();
        // A view bound to an earlier epoch of the same source follows the
        // current one on its next access instead of failing as stale.
        root.stat("/new.txt").await.expect("follows the new epoch");
        let rebound = root.rebind_source().await.expect("rebind");
        assert_eq!(rebound.source, source.reference());
        assert_eq!(source.counts.pages.load(Ordering::Relaxed), 0);
        assert_eq!(
            root.stat("/observed.txt")
                .await
                .expect("cached")
                .logical_bytes,
            Some(6)
        );
        let after = root.snapshot().await.expect("snapshot after refresh");
        assert_ne!(after, before);
        // `/observed.txt` twice (once per epoch) and `/new.txt` once.
        assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn snapshot_exactification_uses_live_epoch_for_unresolved_paths() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &fs,
            "root",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let snapshot = root.snapshot().await.expect("snapshot before rebind");

        source.replace(Bytes::from_static(b"changed"));
        root.rebind_source().await.expect("rebind");
        let exact = root
            .exactify_snapshot(
                snapshot,
                "historical-exact",
                IdempotencyKey::from_bytes([23; 16]),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .expect("exactify historical snapshot");

        assert_eq!(
            exact
                .value
                .read("/file.txt", 32)
                .await
                .expect("unresolved file from live epoch"),
            Bytes::from_static(b"changed")
        );
    }

    #[tokio::test]
    async fn snapshot_exactification_keeps_observed_paths_on_the_pinned_epoch() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &fs,
            "root",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        root.stat("/file.txt").await.expect("observe old epoch");
        let snapshot = root.snapshot().await.expect("snapshot after observation");

        source.replace(Bytes::from_static(b"changed"));
        root.rebind_source().await.expect("rebind");
        let exact = root
            .exactify_snapshot(
                snapshot,
                "pinned-exact",
                IdempotencyKey::from_bytes([24; 16]),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .expect("exactify pinned observation");

        assert_eq!(
            exact
                .value
                .read("/file.txt", 32)
                .await
                .expect("observed file from pinned epoch"),
            Bytes::from_static(b"source")
        );
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn native_source_rebind_refreshes_an_observed_file() {
        use crate::demand::native::NativeDemandSource;
        use crate::model::{FilesystemProfile, VolumeLimits};

        let directory = tempfile::tempdir().expect("temporary source");
        let path = directory.path().join("file.txt");
        std::fs::write(&path, b"before").expect("initial file");
        let source = Arc::new(
            NativeDemandSource::open(
                directory.path(),
                FilesystemProfile::Portable,
                VolumeLimits::default(),
            )
            .await
            .expect("source"),
        );
        let fs = Fs::memory();
        let root = LazyWorkspace::attach(
            &fs,
            "native-rebind",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        assert_eq!(
            root.read("/file.txt", 64).await.expect("initial read"),
            Bytes::from_static(b"before")
        );
        let before = root.snapshot().await.expect("snapshot before change");

        std::fs::write(&path, b"after").expect("external write");
        source.invalidate();
        root.rebind_source().await.expect("rebind source");

        assert_eq!(
            root.read("/file.txt", 64).await.expect("refreshed read"),
            Bytes::from_static(b"after")
        );
        assert_ne!(
            root.snapshot().await.expect("snapshot after change"),
            before
        );
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn exactify_is_bounded_and_returns_only_a_complete_generation() {
        use crate::demand::native::NativeDemandSource;
        use crate::model::{FilesystemProfile, VolumeLimits};

        let directory = tempfile::tempdir().expect("temporary source");
        std::fs::create_dir(directory.path().join("nested")).expect("nested directory");
        std::fs::write(directory.path().join("a.txt"), b"alpha").expect("first file");
        std::fs::write(directory.path().join("nested").join("b.txt"), b"beta")
            .expect("second file");
        let source = Arc::new(
            NativeDemandSource::open(
                directory.path(),
                FilesystemProfile::Portable,
                VolumeLimits::default(),
            )
            .await
            .expect("source"),
        );
        let fs = Fs::memory();
        let root =
            LazyWorkspace::attach(&fs, "exactify", source, MemoryLazyWorkspaceStore::default())
                .await
                .expect("attach");
        let mut insufficient = WorkBudget::UNBOUNDED;
        insufficient.source_entries_visited = 1;
        assert!(matches!(
            root.exactify(insufficient, &CancellationToken::new()).await,
            Err(LazyWorkspaceError::Work(_))
        ));

        let exact = root
            .exactify(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .expect("exact capture");
        assert_eq!(
            exact
                .value
                .read("/nested/b.txt", 16)
                .await
                .expect("captured file"),
            Bytes::from_static(b"beta")
        );
        assert_eq!(exact.work.source_entries_visited, 3);
        assert_eq!(
            exact.work.source_bytes_read, 18,
            "source demand and authored ingestion are both accounted"
        );
    }

    #[tokio::test]
    async fn directory_pages_emit_authored_entries_once_after_source_phase() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &fs,
            "root",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        root.write("/file.txt", Bytes::from_static(b"authored"))
            .await
            .expect("write");

        let source_page = root
            .list_directory("/", None, 16)
            .await
            .expect("source phase");
        assert!(source_page.entries.is_empty());
        let authored_page = root
            .list_directory("/", source_page.next, 16)
            .await
            .expect("authored phase");
        assert_eq!(authored_page.entries.len(), 1);
        assert!(authored_page.entries[0].authored);
        assert!(authored_page.next.is_none());
    }

    #[tokio::test]
    async fn directory_cursor_rejects_overlay_changes_between_pages() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &fs,
            "root",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let first = root
            .list_directory("/", None, 16)
            .await
            .expect("first page");
        root.stat("/newly-observed.txt").await.expect("observe");

        assert!(matches!(
            root.list_directory("/", first.next, 16).await,
            Err(LazyWorkspaceError::StaleCursor)
        ));
    }

    #[tokio::test]
    async fn failed_authored_remove_rolls_back_source_tombstone() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &fs,
            "root",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        root.write("/directory/file.txt", Bytes::from_static(b"authored"))
            .await
            .expect("write");
        let before = root.state().await.expect("state").overlay;

        assert!(matches!(
            root.remove("/directory").await,
            Err(LazyWorkspaceError::Workspace(_))
        ));
        let after = root.state().await.expect("state after failure");
        assert_eq!(after.overlay, before);
        assert!(after.pending_remove.is_none());
        assert!(root.stat("/directory").await.expect("directory").authored);
    }

    #[tokio::test]
    async fn open_recovers_remove_intent_from_authored_state() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let store = MemoryLazyWorkspaceStore::default();
        let root = LazyWorkspace::attach(&fs, "root", Arc::clone(&source), store.clone())
            .await
            .expect("attach");
        root.write("/file.txt", Bytes::from_static(b"authored"))
            .await
            .expect("write");
        let prior = root.state().await.expect("state");
        let tombstone = root
            .insert_overlay(
                prior.overlay,
                "/file.txt".to_owned(),
                LazyOverlayChange::Tombstone,
            )
            .await
            .expect("tombstone");
        let first_key = IdempotencyKey::from_bytes([0x31; 16]);
        let pending = LazyWorkspaceState {
            revision: prior.revision + 1,
            overlay: tombstone,
            pending_remove: Some(PendingLazyRemove {
                path: "/file.txt".to_owned(),
                prior_overlay: prior.overlay,
                tombstone_overlay: tombstone,
                kind: PendingLazyRemoveKind::Authored {
                    idempotency_key: first_key,
                },
            }),
            ..prior.clone()
        };
        assert!(
            store
                .compare_and_swap_lazy_workspace(root.workspace().id(), prior.revision, pending)
                .await
                .expect("prepare")
        );

        let reopened = LazyWorkspace::open(
            fs.open_workspace("root").await.expect("workspace"),
            Arc::clone(&source),
            store.clone(),
        )
        .await
        .expect("recover before physical remove");
        let recovered = reopened.state().await.expect("recovered state");
        assert_eq!(recovered.overlay, prior.overlay);
        assert!(recovered.pending_remove.is_none());

        let committed_key = IdempotencyKey::from_bytes([0x32; 16]);
        let prepared = LazyWorkspaceState {
            revision: recovered.revision + 1,
            overlay: tombstone,
            pending_remove: Some(PendingLazyRemove {
                path: "/file.txt".to_owned(),
                prior_overlay: recovered.overlay,
                tombstone_overlay: tombstone,
                kind: PendingLazyRemoveKind::Authored {
                    idempotency_key: committed_key,
                },
            }),
            ..recovered.clone()
        };
        assert!(
            store
                .compare_and_swap_lazy_workspace(
                    reopened.workspace().id(),
                    recovered.revision,
                    prepared
                )
                .await
                .expect("prepare second")
        );
        let mut removal = reopened
            .workspace()
            .begin_transaction(committed_key)
            .await
            .expect("removal transaction");
        removal.remove("/file.txt").await.expect("stage remove");
        assert!(matches!(
            removal.commit().await.expect("physical remove"),
            TransactionCommit::Committed(_)
        ));

        let finalized = LazyWorkspace::open(
            fs.open_workspace("root").await.expect("workspace"),
            Arc::clone(&source),
            store,
        )
        .await
        .expect("recover after physical remove");
        let final_state = finalized.state().await.expect("final state");
        assert_eq!(final_state.overlay, tombstone);
        assert!(matches!(
            finalized.stat("/file.txt").await,
            Err(LazyWorkspaceError::NotFound)
        ));
    }

    #[tokio::test]
    async fn source_only_remove_recovery_keeps_tombstone_across_a_competing_create() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let store = MemoryLazyWorkspaceStore::default();
        let root = LazyWorkspace::attach(&fs, "root", Arc::clone(&source), store.clone())
            .await
            .expect("attach");
        root.stat("/file.txt").await.expect("observe source");
        let prior = root.state().await.expect("state");
        let tombstone = root
            .insert_overlay(
                prior.overlay,
                "/file.txt".to_owned(),
                LazyOverlayChange::Tombstone,
            )
            .await
            .expect("tombstone");
        let pending = LazyWorkspaceState {
            revision: prior.revision + 1,
            overlay: tombstone,
            pending_remove: Some(PendingLazyRemove {
                path: "/file.txt".to_owned(),
                prior_overlay: prior.overlay,
                tombstone_overlay: tombstone,
                kind: PendingLazyRemoveKind::SourceOnly,
            }),
            ..prior.clone()
        };
        assert!(
            store
                .compare_and_swap_lazy_workspace(root.workspace().id(), prior.revision, pending)
                .await
                .expect("prepare")
        );
        root.workspace()
            .write("/file.txt", Bytes::from_static(b"authored"))
            .await
            .expect("competing authored create");

        let reopened = LazyWorkspace::open(
            fs.open_workspace("root").await.expect("workspace"),
            Arc::clone(&source),
            store,
        )
        .await
        .expect("recover source removal");
        assert_eq!(reopened.state().await.expect("state").overlay, tombstone);
        assert_eq!(
            reopened
                .read("/file.txt", 64)
                .await
                .expect("authored replacement"),
            Bytes::from_static(b"authored")
        );
        reopened.remove("/file.txt").await.expect("remove authored");
        assert!(matches!(
            reopened.stat("/file.txt").await,
            Err(LazyWorkspaceError::NotFound)
        ));
    }

    #[tokio::test]
    async fn observation_index_stays_balanced_and_nodes_stay_constant_size() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let store = MemoryLazyWorkspaceStore::default();
        let root = LazyWorkspace::attach(&fs, "root", Arc::clone(&source), store.clone())
            .await
            .expect("attach");
        for index in 0..1_000 {
            root.stat(&format!("/paths/{index:04}.txt"))
                .await
                .expect("observe");
        }
        let root_id = root.state().await.expect("state").overlay;
        let state = store
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        fn depth(overlays: &BTreeMap<LazyOverlayId, LazyOverlay>, id: LazyOverlayId) -> usize {
            match &overlays[&id] {
                LazyOverlay::Empty => 0,
                LazyOverlay::Node { left, right, .. } => {
                    1 + depth(overlays, *left).max(depth(overlays, *right))
                }
            }
        }
        assert!(depth(&state.overlays, root_id) < 64);
        assert!(state.overlays.values().all(|overlay| {
            serde_json::to_vec(overlay)
                .expect("encode overlay node")
                .len()
                < 2_048
        }));
        assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 1_000);
    }

    #[test]
    fn million_path_exactify_capture_memory_is_exactly_admitted() {
        const PATHS: u64 = 1_000_000;

        fn capture(budget: WorkBudget) -> Result<(WorkCounters, u64), LazyWorkspaceError> {
            let mut paths = Vec::new();
            let mut retained = 0_u64;
            let mut work = WorkCounters::default();
            for index in 0..PATHS {
                let path = format!("/wide/{index:06}.txt");
                reserve_exactify_path_slot(&mut paths, &mut retained, &mut work, budget)?;
                retain_exactify_string(&path, &mut retained, &mut work, budget)?;
                paths.push(path);
            }
            let expected = exactify_path_vector_bytes(&paths)?
                + paths
                    .iter()
                    .map(|path| u64::try_from(path.capacity()).unwrap_or(u64::MAX))
                    .sum::<u64>();
            assert_eq!(retained, expected);
            Ok((work, retained))
        }

        let (work, retained) = capture(WorkBudget::UNBOUNDED).expect("unbounded capture");
        assert!(work.peak_allocation_bytes >= retained);
        assert!(work.allocation_operations >= PATHS);
        let mut insufficient = WorkBudget::UNBOUNDED;
        insufficient.peak_allocation_bytes = work.peak_allocation_bytes - 1;
        assert!(matches!(
            capture(insufficient),
            Err(LazyWorkspaceError::Work(_))
        ));
    }
}
