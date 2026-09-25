//! Durable operation windows for stable mounted-tool execution.
//!
//! A window is a short-lived coordination scope, not a filesystem lock. Any
//! number of tools may read and write the same mounted workspace while leases
//! are active. Parent advances are coalesced and reconciled exactly once after
//! the last lease closes. Durable stores make crash recovery and writer fencing
//! independent of the process that opened the window.

use crate::{
    AsyncAuthorityStore, AsyncObjectStore, GenerationId, IdempotencyKey, OperationId, Workspace,
    WorkspaceError, WorkspaceId, WorkspaceRebase,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use thiserror::Error;

#[cfg(feature = "distributed")]
use futures::StreamExt as _;

const STATE_VERSION: u32 = 1;
const MAXIMUM_CAS_ATTEMPTS: u8 = 32;

/// Stable identity of one tool lease.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OperationLeaseId(OperationId);

impl OperationLeaseId {
    /// Creates a fresh time-ordered lease identity.
    #[must_use]
    pub fn new() -> Self {
        Self(OperationId::new())
    }

    /// Restores a lease identity from its canonical bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(OperationId::from_bytes(bytes))
    }

    /// Returns the canonical bytes.
    #[must_use]
    pub const fn into_bytes(self) -> [u8; 16] {
        self.0.into_bytes()
    }
}

impl Default for OperationLeaseId {
    fn default() -> Self {
        Self::new()
    }
}

/// One active durable tool lease.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OperationLease {
    /// Stable lease identity.
    pub id: OperationLeaseId,
    /// Adapter-defined owner identity, normally a Codex agent and tool call.
    pub owner: String,
    /// Absolute Unix epoch expiry in milliseconds.
    pub expires_at_millis: u64,
}

/// Durable window phase.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum OperationWindowPhase {
    /// No tool currently pins the mounted view.
    Idle,
    /// One or more tools share a pinned view.
    Active {
        /// Parent generation visible when the first lease opened.
        pinned_parent: GenerationId,
        /// Active leases keyed by stable identity.
        leases: BTreeMap<OperationLeaseId, OperationLease>,
        /// Newest parent generation observed while the view was pinned.
        pending_parent: Option<GenerationId>,
    },
    /// The final lease closed and one owner must complete reconciliation.
    Reconciling {
        /// Stable retry identity for reconciliation.
        ticket: OperationId,
        /// Parent generation at window entry.
        pinned_parent: GenerationId,
        /// Newest coalesced parent, if the parent advanced.
        pending_parent: Option<GenerationId>,
        /// Parent observed after this reconciliation ticket was claimed.
        subsequent_parent: Option<GenerationId>,
    },
}

/// Complete versioned durable state for one workspace.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OperationWindowSnapshot {
    /// Serialization contract version.
    pub version: u32,
    /// Monotonic optimistic-concurrency revision.
    pub revision: u64,
    /// Workspace governed by this state.
    pub workspace_id: WorkspaceId,
    /// Current coordination phase.
    pub phase: OperationWindowPhase,
}

impl OperationWindowSnapshot {
    fn idle(workspace_id: WorkspaceId) -> Self {
        Self {
            version: STATE_VERSION,
            revision: 0,
            workspace_id,
            phase: OperationWindowPhase::Idle,
        }
    }
}

fn snapshot_shape_is_valid(snapshot: &OperationWindowSnapshot) -> bool {
    match &snapshot.phase {
        OperationWindowPhase::Idle => true,
        OperationWindowPhase::Reconciling { ticket, .. } => ticket.into_bytes() != [0; 16],
        OperationWindowPhase::Active { leases, .. } => {
            !leases.is_empty()
                && leases.iter().all(|(lease_id, lease)| {
                    *lease_id == lease.id
                        && lease_id.into_bytes() != [0; 16]
                        && !lease.owner.trim().is_empty()
                        && lease.expires_at_millis > 0
                })
        }
    }
}

/// Lease returned to one filesystem-touching tool.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationWindowLease {
    /// Workspace containing the shared mount.
    pub workspace_id: WorkspaceId,
    /// Stable lease identity.
    pub lease_id: OperationLeaseId,
    /// Parent generation pinned by the shared window.
    pub pinned_parent: GenerationId,
    /// Lease expiry copied from durable state.
    pub expires_at_millis: u64,
}

impl OperationWindowLease {
    /// Returns the authority permit that atomically admits publications made
    /// by this exact still-active lease.
    #[must_use]
    pub const fn publication_permit(&self) -> crate::PublicationPermit {
        crate::PublicationPermit::Lease {
            authority_id: crate::kernel::volume_authority_id(self.workspace_id.volume_id())
                .into_bytes(),
            workspace_id: self.workspace_id.into_bytes(),
            lease_id: self.lease_id.into_bytes(),
            expires_at_millis: self.expires_at_millis,
        }
    }
}

/// Reconciliation work claimed after a final close or expiry sweep.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OperationWindowReconcile {
    /// Stable retry identity.
    pub ticket: OperationId,
    /// Parent generation visible when the window opened.
    pub pinned_parent: GenerationId,
    /// Newest coalesced parent generation.
    pub pending_parent: Option<GenerationId>,
}

/// Result of closing or recovering a lease.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationWindowFinish {
    /// Other live leases still pin the view.
    StillActive {
        /// Number of unexpired leases that remain.
        remaining: u32,
    },
    /// The caller owns the one reconciliation attempt.
    Reconcile(OperationWindowReconcile),
    /// The lease was already closed or expired.
    AlreadyClosed,
}

/// Bounds applied when the last tool lease reconciles a fork with its parent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OperationReconcileLimits {
    /// Maximum immutable generations inspected while finding ancestry.
    pub maximum_generations: u32,
    /// Maximum semantic changes retained during planning.
    pub maximum_changes: u32,
    /// Maximum conflicts returned to the caller.
    pub maximum_conflicts: u32,
}

impl Default for OperationReconcileLimits {
    fn default() -> Self {
        Self {
            maximum_generations: 4_096,
            maximum_changes: 65_536,
            maximum_conflicts: 1_024,
        }
    }
}

/// Workspace-aware result of closing one tool lease.
pub enum WorkspaceOperationFinish<A, O> {
    /// Other live leases still pin the shared mount.
    StillActive {
        /// Number of unexpired leases that remain.
        remaining: u32,
    },
    /// The lease was already closed or expired.
    AlreadyClosed,
    /// The final close ran the core live-rebase operation.
    Reconciled(WorkspaceRebase<A, O>),
}

/// Durable optimistic-concurrency adapter for operation-window state.
pub trait OperationWindowStore: Send + Sync {
    /// Adapter error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Loads current state, returning `None` before first use.
    fn load(
        &self,
        workspace_id: WorkspaceId,
    ) -> impl Future<Output = Result<Option<OperationWindowSnapshot>, Self::Error>> + Send;

    /// Replaces `expected_revision` atomically. Revision zero creates state.
    fn compare_and_swap(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: OperationWindowSnapshot,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send;
}

/// Operation-window state stored beside generation authority in one Stream
/// provider. Each lease owns an immutable gate stream: opening creates tail 1,
/// while close/expiry atomically advances it to tail 2 with the window CAS.
/// A generation publication conditioned on tail 1 therefore linearizes
/// exactly against fencing.
#[derive(Clone)]
#[cfg(feature = "distributed")]
pub struct StreamOperationWindowStore<P> {
    provider: Arc<P>,
}

#[cfg(feature = "distributed")]
impl<P> StreamOperationWindowStore<P> {
    /// Binds operation windows to the exact provider used by filesystem
    /// authority publication.
    #[must_use]
    pub fn new(provider: Arc<P>) -> Self {
        Self { provider }
    }
}

/// Stream-backed operation-window failure.
#[cfg(feature = "distributed")]
#[derive(Debug, Error)]
pub enum StreamOperationWindowStoreError {
    /// The shared Stream provider rejected or could not durably resolve state.
    #[error("operation-window stream failed: {0}")]
    Stream(#[from] acyclic_stream::StreamError),
    /// Stored state is corrupt or incompatible.
    #[error("operation-window stream state is corrupt: {0}")]
    Corrupt(String),
}

#[cfg(feature = "distributed")]
impl<P: acyclic_stream::StreamProvider> OperationWindowStore for StreamOperationWindowStore<P> {
    type Error = StreamOperationWindowStoreError;

    async fn load(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Option<OperationWindowSnapshot>, Self::Error> {
        let path = stream_window_path(workspace_id)?;
        let tail = match self.provider.tail(path.clone()).await {
            Ok(tail) => tail,
            Err(acyclic_stream::StreamError::NotFound) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if tail == 0 {
            return Err(StreamOperationWindowStoreError::Corrupt(
                "window stream is empty".to_owned(),
            ));
        }
        let mut records = self
            .provider
            .read(acyclic_stream::ReadRequest {
                path,
                from: tail - 1,
                limit: 1,
            })
            .await?;
        let record = records.next().await.transpose()?.ok_or_else(|| {
            StreamOperationWindowStoreError::Corrupt("window stream tail has no record".to_owned())
        })?;
        let snapshot: OperationWindowSnapshot = serde_json::from_slice(&record.value)
            .map_err(|error| StreamOperationWindowStoreError::Corrupt(error.to_string()))?;
        if snapshot.revision != tail {
            return Err(StreamOperationWindowStoreError::Corrupt(
                "window revision does not match its stream tail".to_owned(),
            ));
        }
        if !snapshot_shape_is_valid(&snapshot) {
            return Err(StreamOperationWindowStoreError::Corrupt(
                "window phase shape is invalid".to_owned(),
            ));
        }
        Ok(Some(snapshot))
    }

    async fn compare_and_swap(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: OperationWindowSnapshot,
    ) -> Result<bool, Self::Error> {
        if replacement.workspace_id != workspace_id
            || replacement.revision != expected_revision.saturating_add(1)
            || !snapshot_shape_is_valid(&replacement)
        {
            return Err(StreamOperationWindowStoreError::Corrupt(
                "replacement identity or revision is invalid".to_owned(),
            ));
        }
        let current = self.load(workspace_id).await?;
        if current.as_ref().map_or(0, |snapshot| snapshot.revision) != expected_revision {
            return Ok(false);
        }
        let before = current
            .as_ref()
            .map_or_else(BTreeMap::new, |snapshot| phase_leases(&snapshot.phase));
        let after = phase_leases(&replacement.phase);
        let state_path = stream_window_path(workspace_id)?;
        let mut conditions = vec![if expected_revision == 0 {
            acyclic_stream::CommitCondition::Absent {
                path: state_path.clone(),
            }
        } else {
            acyclic_stream::CommitCondition::Tail {
                path: state_path.clone(),
                expected: expected_revision,
            }
        }];
        let encoded = serde_json::to_vec(&replacement)
            .map_err(|error| StreamOperationWindowStoreError::Corrupt(error.to_string()))?;
        let mut mutations = vec![acyclic_stream::CommitMutation::Append {
            path: state_path,
            records: vec![encoded.into()],
        }];
        for lease_id in after
            .keys()
            .filter(|lease_id| !before.contains_key(lease_id))
        {
            let path = stream_lease_path(workspace_id, *lease_id)?;
            conditions.push(acyclic_stream::CommitCondition::Absent { path: path.clone() });
            mutations.push(acyclic_stream::CommitMutation::Append {
                path,
                records: vec![bytes::Bytes::from_static(b"active")],
            });
        }
        for lease_id in before
            .keys()
            .filter(|lease_id| !after.contains_key(lease_id))
        {
            let path = stream_lease_path(workspace_id, *lease_id)?;
            conditions.push(acyclic_stream::CommitCondition::Tail {
                path: path.clone(),
                expected: 1,
            });
            mutations.push(acyclic_stream::CommitMutation::Append {
                path,
                records: vec![bytes::Bytes::from_static(b"fenced")],
            });
        }
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"acyclic-operation-window-cas-v2\0");
        hasher.update(&workspace_id.into_bytes());
        hasher.update(&expected_revision.to_le_bytes());
        hasher.update(
            &serde_json::to_vec(&replacement)
                .map_err(|error| StreamOperationWindowStoreError::Corrupt(error.to_string()))?,
        );
        let key = acyclic_stream::IdempotencyKey::new(hasher.finalize().as_bytes().to_vec())?;
        match self
            .provider
            .commit(acyclic_stream::CommitRequest {
                conditions,
                mutations,
                idempotency_key: key,
            })
            .await?
        {
            acyclic_stream::CommitOutcome::Committed(_) => Ok(true),
            acyclic_stream::CommitOutcome::Conflict(_) => Ok(false),
        }
    }
}

#[cfg(feature = "distributed")]
fn phase_leases(phase: &OperationWindowPhase) -> BTreeMap<OperationLeaseId, OperationLease> {
    match phase {
        OperationWindowPhase::Active { leases, .. } => leases.clone(),
        OperationWindowPhase::Idle | OperationWindowPhase::Reconciling { .. } => BTreeMap::new(),
    }
}

#[cfg(feature = "distributed")]
fn stream_window_path(
    workspace_id: WorkspaceId,
) -> Result<acyclic_stream::StreamPath, acyclic_stream::StreamError> {
    acyclic_stream::StreamPath::new(format!(
        "fs/operation-windows-v2/{}/state",
        hex::encode(workspace_id.into_bytes())
    ))
}

#[cfg(feature = "distributed")]
pub(crate) fn stream_lease_path(
    workspace_id: WorkspaceId,
    lease_id: OperationLeaseId,
) -> Result<acyclic_stream::StreamPath, acyclic_stream::StreamError> {
    acyclic_stream::StreamPath::new(format!(
        "fs/operation-windows-v2/{}/leases/{}",
        hex::encode(workspace_id.into_bytes()),
        hex::encode(lease_id.into_bytes())
    ))
}

/// Operation-window failure.
#[derive(Debug, Error)]
pub enum OperationWindowError<E: std::error::Error + 'static> {
    /// Durable adapter failed.
    #[error("operation-window store failed: {0}")]
    Store(E),
    /// Persisted state uses an unsupported version or identity.
    #[error("operation-window state is incompatible")]
    IncompatibleState,
    /// A reconciliation owner already fences new windows.
    #[error("workspace operation window is reconciling")]
    Reconciling,
    /// The supplied lease lifetime is empty or already expired.
    #[error("operation lease must expire after the current time")]
    InvalidExpiry,
    /// A deterministic lease identity was already bound to different inputs.
    #[error("operation lease identity is already bound to another owner or expiry")]
    LeaseIdentityConflict,
    /// The supplied lease was closed, expired, or superseded by a renewal.
    #[error("operation lease is no longer active")]
    StaleLease,
    /// Optimistic-concurrency contention exceeded the bounded retry policy.
    #[error("operation-window state remained contended")]
    Contended,
    /// Reconciliation completion used a stale ticket.
    #[error("operation-window reconciliation ticket is stale")]
    StaleTicket,
    /// The core workspace reconciliation failed.
    #[error("workspace reconciliation failed: {0}")]
    Workspace(#[source] WorkspaceError),
}

/// Core coordinator for overlapping tool leases and deferred parent advances.
pub struct OperationWindowCoordinator<S> {
    store: S,
}

impl<S> OperationWindowCoordinator<S> {
    /// Creates a coordinator over a durable state adapter.
    #[must_use]
    pub const fn new(store: S) -> Self {
        Self { store }
    }

    /// Borrows the underlying adapter.
    #[must_use]
    pub const fn store(&self) -> &S {
        &self.store
    }
}

impl<S: OperationWindowStore> OperationWindowCoordinator<S> {
    /// Opens an overlapping lease. The first lease pins `parent`.
    pub async fn begin(
        &self,
        workspace_id: WorkspaceId,
        parent: GenerationId,
        owner: impl Into<String>,
        now_millis: u64,
        expires_at_millis: u64,
    ) -> Result<OperationWindowLease, OperationWindowError<S::Error>> {
        self.begin_with_lease_id(
            workspace_id,
            parent,
            owner,
            now_millis,
            expires_at_millis,
            OperationLeaseId::new(),
        )
        .await
    }

    /// Opens a lease with a caller-supplied deterministic identity.
    pub async fn begin_with_lease_id(
        &self,
        workspace_id: WorkspaceId,
        parent: GenerationId,
        owner: impl Into<String>,
        now_millis: u64,
        expires_at_millis: u64,
        lease_id: OperationLeaseId,
    ) -> Result<OperationWindowLease, OperationWindowError<S::Error>> {
        if expires_at_millis <= now_millis {
            return Err(OperationWindowError::InvalidExpiry);
        }
        let owner = owner.into();
        if owner.trim().is_empty() || lease_id.into_bytes() == [0; 16] {
            return Err(OperationWindowError::LeaseIdentityConflict);
        }
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let mut current = self.snapshot(workspace_id).await?;
            if expire_to_reconcile(
                &mut current.phase,
                now_millis,
                OperationId::from_bytes(lease_id.into_bytes()),
            )
            .is_some()
            {
                let expected = current.revision;
                current.revision = expected.saturating_add(1);
                if self.cas(workspace_id, expected, current).await? {
                    return Err(OperationWindowError::Reconciling);
                }
                continue;
            }
            let pinned_parent = match &mut current.phase {
                OperationWindowPhase::Idle => {
                    let mut leases = BTreeMap::new();
                    leases.insert(
                        lease_id,
                        OperationLease {
                            id: lease_id,
                            owner: owner.clone(),
                            expires_at_millis,
                        },
                    );
                    current.phase = OperationWindowPhase::Active {
                        pinned_parent: parent,
                        leases,
                        pending_parent: None,
                    };
                    parent
                }
                OperationWindowPhase::Active {
                    pinned_parent,
                    leases,
                    ..
                } => {
                    if let Some(existing) = leases.get(&lease_id) {
                        if existing.owner == owner
                            && existing.expires_at_millis == expires_at_millis
                        {
                            return Ok(OperationWindowLease {
                                workspace_id,
                                lease_id,
                                pinned_parent: *pinned_parent,
                                expires_at_millis,
                            });
                        }
                        return Err(OperationWindowError::LeaseIdentityConflict);
                    }
                    leases.insert(
                        lease_id,
                        OperationLease {
                            id: lease_id,
                            owner: owner.clone(),
                            expires_at_millis,
                        },
                    );
                    *pinned_parent
                }
                OperationWindowPhase::Reconciling { .. } => {
                    return Err(OperationWindowError::Reconciling);
                }
            };
            let expected = current.revision;
            current.revision = expected.saturating_add(1);
            if self.cas(workspace_id, expected, current).await? {
                return Ok(OperationWindowLease {
                    workspace_id,
                    lease_id,
                    pinned_parent,
                    expires_at_millis,
                });
            }
        }
        Err(OperationWindowError::Contended)
    }

    /// Coalesces a newly authenticated parent generation without changing the mount.
    pub async fn observe_parent(
        &self,
        workspace_id: WorkspaceId,
        parent: GenerationId,
    ) -> Result<bool, OperationWindowError<S::Error>> {
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let mut current = self.snapshot(workspace_id).await?;
            match &mut current.phase {
                OperationWindowPhase::Idle => return Ok(false),
                OperationWindowPhase::Active { pending_parent, .. } => {
                    *pending_parent = Some(parent);
                }
                OperationWindowPhase::Reconciling {
                    subsequent_parent, ..
                } => *subsequent_parent = Some(parent),
            }
            let expected = current.revision;
            current.revision = expected.saturating_add(1);
            if self.cas(workspace_id, expected, current).await? {
                return Ok(true);
            }
        }
        Err(OperationWindowError::Contended)
    }

    /// Reconciles an idle child after its parent advances, or coalesces the
    /// advance into an active/reconciling operation window. An idle claim is
    /// durable and fences a new tool window until reconciliation completes.
    ///
    /// This also covers a parent tool that finishes after the child's last
    /// overlapping tool: the child need not open another tool to catch up.
    pub async fn parent_advanced_workspace<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        &self,
        workspace: &Workspace<A, O>,
        parent: GenerationId,
        limits: OperationReconcileLimits,
    ) -> Result<Option<WorkspaceRebase<A, O>>, OperationWindowError<S::Error>> {
        let workspace_id = workspace.id();
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let mut current = self.snapshot(workspace_id).await?;
            let reconcile = match &mut current.phase {
                OperationWindowPhase::Idle => {
                    let ticket = OperationId::new();
                    current.phase = OperationWindowPhase::Reconciling {
                        ticket,
                        pinned_parent: parent,
                        pending_parent: Some(parent),
                        subsequent_parent: None,
                    };
                    Some(OperationWindowReconcile {
                        ticket,
                        pinned_parent: parent,
                        pending_parent: Some(parent),
                    })
                }
                OperationWindowPhase::Active { pending_parent, .. } => {
                    *pending_parent = Some(parent);
                    None
                }
                OperationWindowPhase::Reconciling {
                    subsequent_parent, ..
                } => {
                    *subsequent_parent = Some(parent);
                    None
                }
            };
            let expected = current.revision;
            current.revision = expected.saturating_add(1);
            if self.cas(workspace_id, expected, current).await? {
                return match reconcile {
                    Some(reconcile) => self
                        .reconcile_workspace(workspace, reconcile, limits)
                        .await
                        .map(Some),
                    None => Ok(None),
                };
            }
        }
        Err(OperationWindowError::Contended)
    }

    /// Extends one still-active exact lease without changing the pinned mount.
    /// The returned lease replaces the caller's old publication permit; using
    /// the old expiry after renewal is fenced by durable state.
    pub async fn renew(
        &self,
        lease: &OperationWindowLease,
        now_millis: u64,
        expires_at_millis: u64,
    ) -> Result<OperationWindowLease, OperationWindowError<S::Error>> {
        if expires_at_millis <= now_millis || expires_at_millis < lease.expires_at_millis {
            return Err(OperationWindowError::InvalidExpiry);
        }
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let mut current = self.snapshot(lease.workspace_id).await?;
            let OperationWindowPhase::Active {
                pinned_parent,
                leases,
                ..
            } = &mut current.phase
            else {
                return Err(OperationWindowError::StaleLease);
            };
            let Some(existing) = leases.get(&lease.lease_id) else {
                return Err(OperationWindowError::StaleLease);
            };
            if existing.expires_at_millis != lease.expires_at_millis
                || existing.expires_at_millis <= now_millis
            {
                return Err(OperationWindowError::StaleLease);
            }
            if existing.expires_at_millis == expires_at_millis {
                return Ok(lease.clone());
            }
            let pinned_parent = *pinned_parent;
            leases
                .get_mut(&lease.lease_id)
                .ok_or(OperationWindowError::StaleLease)?
                .expires_at_millis = expires_at_millis;
            let expected = current.revision;
            current.revision = expected.saturating_add(1);
            if self.cas(lease.workspace_id, expected, current).await? {
                return Ok(OperationWindowLease {
                    workspace_id: lease.workspace_id,
                    lease_id: lease.lease_id,
                    pinned_parent,
                    expires_at_millis,
                });
            }
        }
        Err(OperationWindowError::Contended)
    }

    /// Closes one lease and atomically claims reconciliation after the last close.
    pub async fn finish(
        &self,
        lease: &OperationWindowLease,
        now_millis: u64,
    ) -> Result<OperationWindowFinish, OperationWindowError<S::Error>> {
        self.finish_with_ticket(lease, now_millis, OperationId::new())
            .await
    }

    /// Closes a lease with a caller-supplied deterministic reconciliation ticket.
    pub async fn finish_with_ticket(
        &self,
        lease: &OperationWindowLease,
        now_millis: u64,
        reconciliation_ticket: OperationId,
    ) -> Result<OperationWindowFinish, OperationWindowError<S::Error>> {
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let mut current = self.snapshot(lease.workspace_id).await?;
            let before = current.phase.clone();
            let result = close_lease(
                &mut current.phase,
                lease.lease_id,
                lease.expires_at_millis,
                now_millis,
                reconciliation_ticket,
            );
            if result.is_none() && current.phase == before {
                return Ok(OperationWindowFinish::AlreadyClosed);
            }
            let expected = current.revision;
            current.revision = expected.saturating_add(1);
            if self.cas(lease.workspace_id, expected, current).await? {
                return Ok(result.unwrap_or(OperationWindowFinish::AlreadyClosed));
            }
        }
        Err(OperationWindowError::Contended)
    }

    /// Closes a lease and runs exactly one core live rebase after the last close.
    ///
    /// Successful, current, and conflicted outcomes are terminal and release
    /// the window. Stale, fenced, and idempotency-conflicted outcomes retain the
    /// durable reconciliation ticket so recovery can retry or inspect it.
    pub async fn finish_workspace<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        &self,
        workspace: &Workspace<A, O>,
        lease: &OperationWindowLease,
        now_millis: u64,
        limits: OperationReconcileLimits,
    ) -> Result<WorkspaceOperationFinish<A, O>, OperationWindowError<S::Error>> {
        if workspace.id() != lease.workspace_id {
            return Err(OperationWindowError::IncompatibleState);
        }
        let reconcile = match self.finish(lease, now_millis).await? {
            OperationWindowFinish::StillActive { remaining } => {
                return Ok(WorkspaceOperationFinish::StillActive { remaining });
            }
            OperationWindowFinish::AlreadyClosed => {
                return Ok(WorkspaceOperationFinish::AlreadyClosed);
            }
            OperationWindowFinish::Reconcile(reconcile) => reconcile,
        };
        self.reconcile_workspace(workspace, reconcile, limits)
            .await
            .map(WorkspaceOperationFinish::Reconciled)
    }

    /// Runs one previously claimed reconciliation ticket.
    ///
    /// This split form lets adapters fence a lease before capturing its mount,
    /// then publish the capture and finish the core rebase without reopening a
    /// race for another tool window.
    pub async fn reconcile_workspace<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        &self,
        workspace: &Workspace<A, O>,
        reconcile: OperationWindowReconcile,
        limits: OperationReconcileLimits,
    ) -> Result<WorkspaceRebase<A, O>, OperationWindowError<S::Error>> {
        let snapshot = self.snapshot(workspace.id()).await?;
        if !matches!(
            snapshot.phase,
            OperationWindowPhase::Reconciling { ticket, .. } if ticket == reconcile.ticket
        ) {
            return Err(OperationWindowError::StaleTicket);
        }
        let outcome = if reconcile.pending_parent.is_some() {
            workspace
                .live_rebase(
                    IdempotencyKey::from_bytes(reconcile.ticket.into_bytes()),
                    limits.maximum_generations,
                    limits.maximum_changes,
                    limits.maximum_conflicts,
                )
                .await
                .map_err(OperationWindowError::Workspace)?
        } else {
            let generation = workspace
                .head()
                .await
                .map_err(OperationWindowError::Workspace)?;
            WorkspaceRebase::Current(generation)
        };
        if matches!(
            outcome,
            WorkspaceRebase::Rebased(_)
                | WorkspaceRebase::AlreadyRebased(_)
                | WorkspaceRebase::Current(_)
                | WorkspaceRebase::Conflicted { .. }
        ) {
            self.complete_reconcile(workspace.id(), reconcile.ticket)
                .await?;
        }
        Ok(outcome)
    }

    /// Claims and completes reconciliation left behind by expired leases or a
    /// crashed adapter. Returns `None` when no recovery work exists.
    pub async fn recover_workspace<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        &self,
        workspace: &Workspace<A, O>,
        now_millis: u64,
        limits: OperationReconcileLimits,
    ) -> Result<Option<WorkspaceRebase<A, O>>, OperationWindowError<S::Error>> {
        let reconcile = self.claim_reconcile(workspace.id(), now_millis).await?;
        match reconcile {
            Some(reconcile) => self
                .reconcile_workspace(workspace, reconcile, limits)
                .await
                .map(Some),
            None => Ok(None),
        }
    }

    /// Claims an expired active window or returns the durable retry ticket for
    /// an interrupted reconciliation.
    pub async fn claim_reconcile(
        &self,
        workspace_id: WorkspaceId,
        now_millis: u64,
    ) -> Result<Option<OperationWindowReconcile>, OperationWindowError<S::Error>> {
        self.claim_reconcile_with_ticket(workspace_id, now_millis, OperationId::new())
            .await
    }

    /// Claims recovery with a caller-supplied deterministic ticket.
    pub async fn claim_reconcile_with_ticket(
        &self,
        workspace_id: WorkspaceId,
        now_millis: u64,
        ticket: OperationId,
    ) -> Result<Option<OperationWindowReconcile>, OperationWindowError<S::Error>> {
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let mut current = self.snapshot(workspace_id).await?;
            match current.phase {
                OperationWindowPhase::Idle => return Ok(None),
                OperationWindowPhase::Reconciling {
                    ticket,
                    pinned_parent,
                    pending_parent,
                    ..
                } => {
                    return Ok(Some(OperationWindowReconcile {
                        ticket,
                        pinned_parent,
                        pending_parent,
                    }));
                }
                OperationWindowPhase::Active { .. } => {}
            }
            let Some(reconcile) = expire_to_reconcile(&mut current.phase, now_millis, ticket)
            else {
                return Ok(None);
            };
            let expected = current.revision;
            current.revision = expected.saturating_add(1);
            if self.cas(workspace_id, expected, current).await? {
                return Ok(Some(reconcile));
            }
        }
        Err(OperationWindowError::Contended)
    }

    /// Completes the exact reconciliation ticket and permits the next window.
    pub async fn complete_reconcile(
        &self,
        workspace_id: WorkspaceId,
        ticket: OperationId,
    ) -> Result<(), OperationWindowError<S::Error>> {
        self.complete_reconcile_with_next_ticket(workspace_id, ticket, OperationId::new())
            .await
    }

    /// Completes reconciliation using a caller-supplied ticket if a newer
    /// parent was observed while reconciliation was active.
    pub async fn complete_reconcile_with_next_ticket(
        &self,
        workspace_id: WorkspaceId,
        ticket: OperationId,
        next_ticket: OperationId,
    ) -> Result<(), OperationWindowError<S::Error>> {
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let mut current = self.snapshot(workspace_id).await?;
            match current.phase {
                OperationWindowPhase::Reconciling {
                    ticket: current_ticket,
                    pinned_parent,
                    subsequent_parent,
                    ..
                } if current_ticket == ticket => {
                    current.phase = match subsequent_parent {
                        Some(parent) => OperationWindowPhase::Reconciling {
                            ticket: next_ticket,
                            pinned_parent,
                            pending_parent: Some(parent),
                            subsequent_parent: None,
                        },
                        None => OperationWindowPhase::Idle,
                    };
                }
                _ => return Err(OperationWindowError::StaleTicket),
            }
            let expected = current.revision;
            current.revision = expected.saturating_add(1);
            if self.cas(workspace_id, expected, current).await? {
                return Ok(());
            }
        }
        Err(OperationWindowError::Contended)
    }

    /// Returns validated durable state for diagnostics and recovery.
    pub async fn inspect(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<OperationWindowSnapshot, OperationWindowError<S::Error>> {
        self.snapshot(workspace_id).await
    }

    /// Loads and validates the current durable window state for inspection.
    pub async fn snapshot(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<OperationWindowSnapshot, OperationWindowError<S::Error>> {
        let stored = self
            .store
            .load(workspace_id)
            .await
            .map_err(OperationWindowError::Store)?;
        let state = stored
            .clone()
            .unwrap_or_else(|| OperationWindowSnapshot::idle(workspace_id));
        if state.version != STATE_VERSION
            || state.workspace_id != workspace_id
            || stored.is_some() && state.revision == 0
            || !snapshot_shape_is_valid(&state)
        {
            return Err(OperationWindowError::IncompatibleState);
        }
        Ok(state)
    }

    async fn cas(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: OperationWindowSnapshot,
    ) -> Result<bool, OperationWindowError<S::Error>> {
        if replacement.version != STATE_VERSION
            || replacement.workspace_id != workspace_id
            || replacement.revision != expected_revision.saturating_add(1)
            || !snapshot_shape_is_valid(&replacement)
        {
            return Err(OperationWindowError::IncompatibleState);
        }
        self.store
            .compare_and_swap(workspace_id, expected_revision, replacement)
            .await
            .map_err(OperationWindowError::Store)
    }
}

fn expire_to_reconcile(
    phase: &mut OperationWindowPhase,
    now_millis: u64,
    ticket: OperationId,
) -> Option<OperationWindowReconcile> {
    let OperationWindowPhase::Active {
        pinned_parent,
        leases,
        pending_parent,
    } = phase
    else {
        return None;
    };
    leases.retain(|_, lease| lease.expires_at_millis > now_millis);
    if !leases.is_empty() {
        return None;
    }
    let reconcile = OperationWindowReconcile {
        ticket,
        pinned_parent: *pinned_parent,
        pending_parent: *pending_parent,
    };
    *phase = OperationWindowPhase::Reconciling {
        ticket: reconcile.ticket,
        pinned_parent: reconcile.pinned_parent,
        pending_parent: reconcile.pending_parent,
        subsequent_parent: None,
    };
    Some(reconcile)
}

fn close_lease(
    phase: &mut OperationWindowPhase,
    lease_id: OperationLeaseId,
    expected_expiry_millis: u64,
    now_millis: u64,
    reconciliation_ticket: OperationId,
) -> Option<OperationWindowFinish> {
    let OperationWindowPhase::Active {
        pinned_parent,
        leases,
        pending_parent,
    } = phase
    else {
        return None;
    };
    let requested_live = leases.get(&lease_id).is_some_and(|lease| {
        lease.expires_at_millis == expected_expiry_millis && lease.expires_at_millis > now_millis
    });
    if leases
        .get(&lease_id)
        .is_some_and(|lease| lease.expires_at_millis != expected_expiry_millis)
    {
        return None;
    }
    leases.remove(&lease_id);
    leases.retain(|_, lease| lease.expires_at_millis > now_millis);
    if !leases.is_empty() {
        if !requested_live {
            return None;
        }
        return Some(OperationWindowFinish::StillActive {
            remaining: u32::try_from(leases.len()).unwrap_or(u32::MAX),
        });
    }
    let reconcile = OperationWindowReconcile {
        ticket: reconciliation_ticket,
        pinned_parent: *pinned_parent,
        pending_parent: *pending_parent,
    };
    *phase = OperationWindowPhase::Reconciling {
        ticket: reconcile.ticket,
        pinned_parent: reconcile.pinned_parent,
        pending_parent: reconcile.pending_parent,
        subsequent_parent: None,
    };
    requested_live.then_some(OperationWindowFinish::Reconcile(reconcile))
}

/// Process-local adapter used by tests and embedded single-process callers.
#[derive(Clone, Default)]
pub struct MemoryOperationWindowStore {
    states: Arc<Mutex<BTreeMap<WorkspaceId, OperationWindowSnapshot>>>,
}

impl MemoryOperationWindowStore {
    /// Creates an empty state adapter.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

/// Process-local store synchronization failure.
#[derive(Debug, Error)]
#[error("operation-window memory store is unavailable")]
pub struct MemoryOperationWindowStoreError;

impl OperationWindowStore for MemoryOperationWindowStore {
    type Error = MemoryOperationWindowStoreError;

    async fn load(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Option<OperationWindowSnapshot>, Self::Error> {
        self.states
            .lock()
            .map_err(|_| MemoryOperationWindowStoreError)
            .map(|states| states.get(&workspace_id).cloned())
    }

    async fn compare_and_swap(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: OperationWindowSnapshot,
    ) -> Result<bool, Self::Error> {
        let mut states = self
            .states
            .lock()
            .map_err(|_| MemoryOperationWindowStoreError)?;
        let revision = states.get(&workspace_id).map_or(0, |state| state.revision);
        if revision != expected_revision {
            return Ok(false);
        }
        states.insert(workspace_id, replacement);
        Ok(true)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::{
        AsyncAuthorityStore, AuthorityId, CancellationToken, Digest, Epoch, ForkOptions, Fs,
        IdempotencyKey, OperationId, ProposedCommit, StreamAuthorityStore, WorkBudget,
    };
    use bytes::Bytes;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[derive(Default)]
    struct TestClock(AtomicU64);

    impl acyclic_stream::UnixMillisClock for TestClock {
        fn now_unix_millis(&self) -> u64 {
            self.0.load(Ordering::SeqCst)
        }
    }

    fn generation(byte: u8) -> GenerationId {
        GenerationId::new(Digest::from_bytes([byte; 32]))
    }

    #[tokio::test]
    async fn persisted_active_windows_require_a_nonempty_identity_keyed_lease_set() {
        let workspace = WorkspaceId::from_bytes([6; 16]);
        let store = MemoryOperationWindowStore::new();
        store.states.lock().expect("state lock").insert(
            workspace,
            OperationWindowSnapshot {
                version: STATE_VERSION,
                revision: 1,
                workspace_id: workspace,
                phase: OperationWindowPhase::Active {
                    pinned_parent: generation(1),
                    leases: BTreeMap::new(),
                    pending_parent: None,
                },
            },
        );
        let coordinator = OperationWindowCoordinator::new(store);

        assert!(matches!(
            coordinator.inspect(workspace).await,
            Err(OperationWindowError::IncompatibleState)
        ));
    }

    #[tokio::test]
    async fn overlapping_leases_coalesce_parent_and_reconcile_once() {
        let workspace = WorkspaceId::derive(
            [7; 16],
            &crate::WorkspaceName::new("agent").expect("valid test workspace"),
        );
        let coordinator = OperationWindowCoordinator::new(MemoryOperationWindowStore::new());
        let first = coordinator
            .begin(workspace, generation(1), "tool-1", 10, 100)
            .await
            .expect("first lease");
        let second = coordinator
            .begin(workspace, generation(1), "tool-2", 11, 100)
            .await
            .expect("second lease");
        assert!(
            coordinator
                .observe_parent(workspace, generation(2))
                .await
                .expect("observe")
        );
        assert_eq!(
            coordinator.finish(&first, 20).await.expect("first close"),
            OperationWindowFinish::StillActive { remaining: 1 }
        );
        let OperationWindowFinish::Reconcile(reconcile) =
            coordinator.finish(&second, 21).await.expect("final close")
        else {
            panic!("final close must claim reconciliation");
        };
        assert_eq!(reconcile.pinned_parent, generation(1));
        assert_eq!(reconcile.pending_parent, Some(generation(2)));
        assert!(matches!(
            coordinator
                .begin(workspace, generation(2), "tool-3", 22, 100)
                .await,
            Err(OperationWindowError::Reconciling)
        ));
        coordinator
            .complete_reconcile(workspace, reconcile.ticket)
            .await
            .expect("complete reconciliation");
        coordinator
            .begin(workspace, generation(2), "tool-3", 23, 100)
            .await
            .expect("next window");
    }

    #[tokio::test]
    async fn parent_advance_after_child_close_rebases_without_another_tool() {
        let fs = Fs::memory();
        let parent = fs.create_workspace("late-parent").await.expect("parent");
        let child = parent
            .fork(
                "late-child",
                ForkOptions::from_generation(
                    parent.head().await.expect("base"),
                    IdempotencyKey::new(),
                ),
            )
            .await
            .expect("child");
        child
            .write_text("/child", "local")
            .await
            .expect("child write");
        let coordinator = OperationWindowCoordinator::new(MemoryOperationWindowStore::new());
        let lease = coordinator
            .begin(
                child.id(),
                parent.head().await.expect("parent head").id(),
                "tool",
                1,
                100,
            )
            .await
            .expect("lease");
        assert!(matches!(
            coordinator
                .finish_workspace(&child, &lease, 2, OperationReconcileLimits::default())
                .await
                .expect("child close"),
            WorkspaceOperationFinish::Reconciled(WorkspaceRebase::Current(_))
        ));
        parent
            .write_text("/parent", "upstream")
            .await
            .expect("parent write");
        assert!(matches!(
            coordinator
                .parent_advanced_workspace(
                    &child,
                    parent.head().await.expect("new parent head").id(),
                    OperationReconcileLimits::default(),
                )
                .await
                .expect("parent advance"),
            Some(WorkspaceRebase::Rebased(_))
        ));
        assert_eq!(
            child.read("/parent", 16).await.expect("upstream"),
            Bytes::from_static(b"upstream")
        );
        assert_eq!(
            child.read("/child", 16).await.expect("local"),
            Bytes::from_static(b"local")
        );
        assert!(matches!(
            coordinator.inspect(child.id()).await.expect("window").phase,
            OperationWindowPhase::Idle
        ));
    }

    #[tokio::test]
    async fn parent_advance_during_child_tool_rebases_at_final_close() {
        let fs = Fs::memory();
        let parent = fs.create_workspace("active-parent").await.expect("parent");
        let child = parent
            .fork(
                "active-child",
                ForkOptions::from_generation(
                    parent.head().await.expect("base"),
                    IdempotencyKey::new(),
                ),
            )
            .await
            .expect("child");
        let coordinator = OperationWindowCoordinator::new(MemoryOperationWindowStore::new());
        let lease = coordinator
            .begin(
                child.id(),
                parent.head().await.expect("parent head").id(),
                "tool",
                1,
                100,
            )
            .await
            .expect("lease");
        parent
            .write_text("/parent", "new")
            .await
            .expect("parent write");
        assert!(
            coordinator
                .parent_advanced_workspace(
                    &child,
                    parent.head().await.expect("new parent head").id(),
                    OperationReconcileLimits::default(),
                )
                .await
                .expect("coalesce")
                .is_none()
        );
        assert!(matches!(
            coordinator
                .finish_workspace(&child, &lease, 2, OperationReconcileLimits::default())
                .await
                .expect("final close"),
            WorkspaceOperationFinish::Reconciled(WorkspaceRebase::Rebased(_))
        ));
        assert_eq!(
            child.read("/parent", 16).await.expect("upstream").as_ref(),
            b"new"
        );
    }

    #[tokio::test]
    async fn deterministic_lease_identity_is_idempotent_but_cannot_be_rebound() {
        let workspace = WorkspaceId::derive(
            [12; 16],
            &crate::WorkspaceName::new("lease-identity").expect("valid test workspace"),
        );
        let coordinator = OperationWindowCoordinator::new(MemoryOperationWindowStore::new());
        let lease_id = OperationLeaseId::from_bytes([42; 16]);
        let first = coordinator
            .begin_with_lease_id(workspace, generation(1), "owner", 1, 10, lease_id)
            .await
            .expect("first lease");
        assert_eq!(
            coordinator
                .begin_with_lease_id(workspace, generation(1), "owner", 2, 10, lease_id)
                .await
                .expect("exact retry"),
            first
        );
        assert!(matches!(
            coordinator
                .begin_with_lease_id(workspace, generation(1), "other", 2, 10, lease_id)
                .await,
            Err(OperationWindowError::LeaseIdentityConflict)
        ));
        assert!(matches!(
            coordinator
                .begin_with_lease_id(workspace, generation(1), "owner", 2, 11, lease_id)
                .await,
            Err(OperationWindowError::LeaseIdentityConflict)
        ));
    }

    #[tokio::test]
    async fn renewal_replaces_the_exact_live_lease_and_fences_stale_handles() {
        let workspace = WorkspaceId::derive(
            [13; 16],
            &crate::WorkspaceName::new("lease-renewal").expect("valid test workspace"),
        );
        let coordinator = OperationWindowCoordinator::new(MemoryOperationWindowStore::new());
        let lease = coordinator
            .begin(workspace, generation(1), "owner", 1, 10)
            .await
            .expect("lease");
        let renewed = coordinator
            .renew(&lease, 2, 20)
            .await
            .expect("renew live lease");
        assert_eq!(renewed.expires_at_millis, 20);
        assert_eq!(renewed.pinned_parent, lease.pinned_parent);
        assert!(matches!(
            coordinator.renew(&lease, 3, 30).await,
            Err(OperationWindowError::StaleLease)
        ));
        assert!(matches!(
            coordinator.finish(&lease, 4).await,
            Ok(OperationWindowFinish::AlreadyClosed)
        ));
        assert!(matches!(
            coordinator.finish(&renewed, 4).await,
            Ok(OperationWindowFinish::Reconcile(_))
        ));
    }

    #[tokio::test]
    async fn parent_observed_during_reconcile_is_not_lost() {
        let workspace = WorkspaceId::derive(
            [8; 16],
            &crate::WorkspaceName::new("agent").expect("valid test workspace"),
        );
        let coordinator = OperationWindowCoordinator::new(MemoryOperationWindowStore::new());
        let lease = coordinator
            .begin(workspace, generation(1), "tool", 10, 100)
            .await
            .expect("lease");
        let OperationWindowFinish::Reconcile(reconcile) =
            coordinator.finish(&lease, 20).await.expect("finish")
        else {
            panic!("final close must claim reconciliation");
        };
        coordinator
            .observe_parent(workspace, generation(2))
            .await
            .expect("observe during reconcile");
        coordinator
            .complete_reconcile(workspace, reconcile.ticket)
            .await
            .expect("complete first reconcile");
        assert!(matches!(
            coordinator.inspect(workspace).await.expect("inspect").phase,
            OperationWindowPhase::Reconciling {
                pending_parent: Some(parent),
                ..
            } if parent == generation(2)
        ));
    }

    #[tokio::test]
    async fn expired_lease_is_fenced_and_recovery_ticket_is_claimable() {
        let workspace = WorkspaceId::derive(
            [9; 16],
            &crate::WorkspaceName::new("expired").expect("valid test workspace"),
        );
        let coordinator = OperationWindowCoordinator::new(MemoryOperationWindowStore::new());
        let lease = coordinator
            .begin(workspace, generation(1), "tool", 10, 20)
            .await
            .expect("lease");
        assert_eq!(
            coordinator.finish(&lease, 20).await.expect("expired close"),
            OperationWindowFinish::AlreadyClosed
        );
        let claimed = coordinator
            .claim_reconcile(workspace, 20)
            .await
            .expect("claim")
            .expect("recovery ticket");
        assert_eq!(claimed.pinned_parent, generation(1));
        assert!(matches!(
            coordinator
                .begin(workspace, generation(1), "late", 21, 30)
                .await,
            Err(OperationWindowError::Reconciling)
        ));
    }

    #[tokio::test]
    async fn expired_close_is_persistently_fenced_while_another_lease_remains() {
        let workspace = WorkspaceId::derive(
            [10; 16],
            &crate::WorkspaceName::new("overlap-expiry").expect("valid test workspace"),
        );
        let coordinator = OperationWindowCoordinator::new(MemoryOperationWindowStore::new());
        let expired = coordinator
            .begin(workspace, generation(1), "expired", 10, 20)
            .await
            .expect("expired lease");
        let active = coordinator
            .begin(workspace, generation(1), "active", 11, 40)
            .await
            .expect("active lease");

        assert_eq!(
            coordinator
                .finish(&expired, 20)
                .await
                .expect("expired close"),
            OperationWindowFinish::AlreadyClosed
        );
        let snapshot = coordinator.inspect(workspace).await.expect("inspect");
        assert!(matches!(
            snapshot.phase,
            OperationWindowPhase::Active { ref leases, .. }
                if leases.len() == 1 && leases.contains_key(&active.lease_id)
        ));
        assert!(matches!(
            coordinator.finish(&active, 22).await.expect("active close"),
            OperationWindowFinish::Reconcile(_)
        ));
    }

    #[tokio::test]
    async fn duplicate_close_persists_expiry_of_the_remaining_lease() {
        let workspace = WorkspaceId::derive(
            [11; 16],
            &crate::WorkspaceName::new("duplicate-expiry").expect("valid test workspace"),
        );
        let coordinator = OperationWindowCoordinator::new(MemoryOperationWindowStore::new());
        let first = coordinator
            .begin(workspace, generation(1), "first", 1, 10)
            .await
            .expect("first lease");
        coordinator
            .begin(workspace, generation(1), "second", 2, 40)
            .await
            .expect("second lease");
        assert_eq!(
            coordinator.finish(&first, 5).await.expect("first close"),
            OperationWindowFinish::StillActive { remaining: 1 }
        );
        assert_eq!(
            coordinator
                .finish(&first, 50)
                .await
                .expect("duplicate close"),
            OperationWindowFinish::AlreadyClosed
        );
        assert!(matches!(
            coordinator.inspect(workspace).await.expect("inspect").phase,
            OperationWindowPhase::Reconciling { .. }
        ));
    }

    #[tokio::test]
    async fn stream_lease_gate_fences_one_writer_without_fencing_its_peer() {
        let clock = Arc::new(TestClock::default());
        clock.0.store(10, Ordering::SeqCst);
        let stream = Arc::new(acyclic_stream::MemoryStream::new_with_clock(
            acyclic_stream::MemoryLimits::default(),
            clock,
        ));
        let authority = StreamAuthorityStore::new(Arc::clone(&stream));
        let windows =
            OperationWindowCoordinator::new(StreamOperationWindowStore::new(Arc::clone(&stream)));
        let workspace = WorkspaceId::from_bytes([61; 16]);
        let authority_id = crate::kernel::volume_authority_id(workspace.volume_id());
        let unrelated_authority = AuthorityId::from_bytes([62; 16]);
        let cancellation = CancellationToken::new();
        AsyncAuthorityStore::create_authority(
            &authority,
            unrelated_authority,
            Epoch::GENESIS,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .expect("create unrelated authority");
        AsyncAuthorityStore::create_authority(
            &authority,
            authority_id,
            Epoch::GENESIS,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .expect("create authority");
        let first = windows
            .begin(workspace, generation(1), "first", 1, 100)
            .await
            .expect("first lease");
        let second = windows
            .begin(workspace, generation(1), "second", 2, 100)
            .await
            .expect("second lease");
        assert_eq!(
            windows.finish(&first, 3).await.expect("close first"),
            OperationWindowFinish::StillActive { remaining: 1 }
        );

        let head = AsyncAuthorityStore::head(
            &authority,
            authority_id,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .expect("authority head")
        .value;
        let rejected = AsyncAuthorityStore::compare_and_append_guarded(
            &authority,
            crate::GuardedAppend {
                authority_id,
                epoch: head.epoch,
                expected: head,
                commit: ProposedCommit {
                    operation_id: OperationId::from_bytes([71; 16]),
                    fingerprint: Digest::from_bytes([72; 32]),
                    payload: Bytes::from_static(b"first"),
                },
                permit: first.publication_permit(),
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .expect("guarded rejection")
        .value;
        assert!(matches!(rejected, crate::AppendOutcome::Fenced { .. }));

        let committed = AsyncAuthorityStore::compare_and_append_guarded(
            &authority,
            crate::GuardedAppend {
                authority_id,
                epoch: head.epoch,
                expected: head,
                commit: ProposedCommit {
                    operation_id: OperationId::from_bytes([73; 16]),
                    fingerprint: Digest::from_bytes([74; 32]),
                    payload: Bytes::from_static(b"second"),
                },
                permit: second.publication_permit(),
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .expect("guarded append")
        .value;
        assert!(matches!(committed, crate::AppendOutcome::Committed(_)));

        let unrelated_head = AsyncAuthorityStore::head(
            &authority,
            unrelated_authority,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .expect("unrelated authority head")
        .value;
        let rejected = AsyncAuthorityStore::compare_and_append_guarded(
            &authority,
            crate::GuardedAppend {
                authority_id: unrelated_authority,
                epoch: unrelated_head.epoch,
                expected: unrelated_head,
                commit: ProposedCommit {
                    operation_id: OperationId::from_bytes([75; 16]),
                    fingerprint: Digest::from_bytes([76; 32]),
                    payload: Bytes::from_static(b"cross-authority"),
                },
                permit: second.publication_permit(),
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .expect("cross-authority lease is a semantic fence")
        .value;
        assert!(matches!(rejected, crate::AppendOutcome::Fenced { .. }));
        assert_eq!(
            AsyncAuthorityStore::head(
                &authority,
                unrelated_authority,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .expect("unrelated authority remains unchanged")
            .value,
            unrelated_head
        );
    }

    #[tokio::test]
    async fn provider_deadline_fences_without_a_separate_expiry_sweep() {
        let clock = Arc::new(TestClock::default());
        clock.0.store(10, Ordering::SeqCst);
        let stream = Arc::new(acyclic_stream::MemoryStream::new_with_clock(
            acyclic_stream::MemoryLimits::default(),
            clock.clone(),
        ));
        let authority = StreamAuthorityStore::new(Arc::clone(&stream));
        let windows =
            OperationWindowCoordinator::new(StreamOperationWindowStore::new(Arc::clone(&stream)));
        let workspace = WorkspaceId::from_bytes([81; 16]);
        let authority_id = crate::kernel::volume_authority_id(workspace.volume_id());
        let cancellation = CancellationToken::new();
        AsyncAuthorityStore::create_authority(
            &authority,
            authority_id,
            Epoch::GENESIS,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .expect("create authority");
        let lease = windows
            .begin(workspace, generation(1), "tool", 10, 20)
            .await
            .expect("lease");
        let head = AsyncAuthorityStore::head(
            &authority,
            authority_id,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .expect("authority head")
        .value;
        clock.0.store(20, Ordering::SeqCst);
        let outcome = AsyncAuthorityStore::compare_and_append_guarded(
            &authority,
            crate::GuardedAppend {
                authority_id,
                epoch: head.epoch,
                expected: head,
                commit: ProposedCommit {
                    operation_id: OperationId::from_bytes([83; 16]),
                    fingerprint: Digest::from_bytes([84; 32]),
                    payload: Bytes::from_static(b"expired"),
                },
                permit: lease.publication_permit(),
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .expect("deadline is a semantic fence")
        .value;
        assert!(matches!(outcome, crate::AppendOutcome::Fenced { .. }));
        assert_eq!(
            AsyncAuthorityStore::head(
                &authority,
                authority_id,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .expect("unchanged authority")
            .value,
            head
        );
    }
}
