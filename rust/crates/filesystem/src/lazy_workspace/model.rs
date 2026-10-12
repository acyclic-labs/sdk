use super::*;

pub(crate) const LAZY_STATE_SCHEMA: u32 = 10;

pub(super) const LAZY_SNAPSHOT_DOMAIN: &[u8] = b"acyclic-fs-lazy-snapshot-v1\0";

/// Stable logical identity of one constant-size lazy tree snapshot.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct LazySnapshotId(pub(super) [u8; 32]);

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
    pub(super) fn new(
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
pub struct LazyOverlayId(pub(super) [u8; 32]);

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
#[serde(deny_unknown_fields)]
pub struct LazyWorkspaceState {
    /// Serialization schema.
    pub schema_version: u32,
    /// Monotonic CAS revision.
    pub revision: u64,
    /// Workspace whose authored generation supplies the writable overlay.
    pub workspace_id: WorkspaceId,
    /// Direct sparse parent, when this state was produced by a fork.
    pub parent_workspace_id: Option<WorkspaceId>,
    /// Original pre-creation source cut.
    #[serde(deserialize_with = "Option::<LazyForkOrigin>::deserialize")]
    pub fork_origin: Option<LazyForkOrigin>,
    /// Provider identity and invalidation epoch pinned at attach time.
    pub source: SourceReference,
    /// Immutable observations and tombstones visible to this context.
    pub overlay: LazyOverlayId,
    /// Persistent source identity to latest authored record index.
    pub shadows: LazyShadowId,
    /// Prepared authored removal recovered atomically when the workspace reopens.
    pub pending_remove: Option<PendingLazyRemove>,
    /// Latest committed selected import.
    #[serde(deserialize_with = "Option::<CompletedSelectedImport>::deserialize")]
    pub completed_selected_import: Option<CompletedSelectedImport>,
}

/// Content address of one immutable source-identity shadow index node.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct LazyShadowId(pub(super) [u8; 32]);

impl LazyShadowId {
    pub(crate) const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

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

/// Durable intent that makes authored removal and source tombstoning recoverable.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PendingLazyRemove {
    /// Overlay visible before the removal began.
    pub prior_overlay: LazyOverlayId,
    /// Identity records visible before the removal began.
    pub prior_shadows: LazyShadowId,
    /// Overlay prepared for the same durable publication decision.
    pub prepared_overlay: LazyOverlayId,
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
    /// Mounted removals staged with the checkout and published at its boundary.
    Mounted {
        /// The checkout publication whose result decides this overlay.
        idempotency_key: IdempotencyKey,
    },
}

/// One immutable node in the persistent source-knowledge index.
#[allow(
    clippy::large_enum_variant,
    reason = "boxing the Node payload would change this public enum's pattern-matching API"
)]
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
#[allow(
    clippy::large_enum_variant,
    reason = "boxing the Observe payload would change this public enum's pattern-matching API"
)]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum LazyOverlayChange {
    /// First exact observation of one demanded path.
    Observe {
        /// Source generation against which this fact was authenticated.
        source: SourceReference,
        /// Immutable source fact.
        node: SourceNode,
    },
    /// Authored removal of a source path or subtree. `removed` is the source
    Tombstone {
        /// Source version deleted, or `None` for a hidden path.
        removed: Option<SourceVersion>,
    },
}
