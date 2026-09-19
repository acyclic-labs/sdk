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
use std::sync::Mutex;
use thiserror::Error;

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
        #[serde(default)]
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
        if expires_at_millis <= now_millis {
            return Err(OperationWindowError::InvalidExpiry);
        }
        let owner = owner.into();
        let lease_id = OperationLeaseId::new();
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let mut current = self.snapshot(workspace_id).await?;
            if expire_to_reconcile(&mut current.phase, now_millis).is_some() {
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

    /// Closes one lease and atomically claims reconciliation after the last close.
    pub async fn finish(
        &self,
        lease: &OperationWindowLease,
        now_millis: u64,
    ) -> Result<OperationWindowFinish, OperationWindowError<S::Error>> {
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let mut current = self.snapshot(lease.workspace_id).await?;
            let before = current.phase.clone();
            let result = close_lease(&mut current.phase, lease.lease_id, now_millis);
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
            let Some(reconcile) = expire_to_reconcile(&mut current.phase, now_millis) else {
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
                            ticket: OperationId::new(),
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
        let state = self
            .store
            .load(workspace_id)
            .await
            .map_err(OperationWindowError::Store)?
            .unwrap_or_else(|| OperationWindowSnapshot::idle(workspace_id));
        if state.version != STATE_VERSION || state.workspace_id != workspace_id {
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
        self.store
            .compare_and_swap(workspace_id, expected_revision, replacement)
            .await
            .map_err(OperationWindowError::Store)
    }
}

fn expire_to_reconcile(
    phase: &mut OperationWindowPhase,
    now_millis: u64,
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
        ticket: OperationId::new(),
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
    now_millis: u64,
) -> Option<OperationWindowFinish> {
    let OperationWindowPhase::Active {
        pinned_parent,
        leases,
        pending_parent,
    } = phase
    else {
        return None;
    };
    let requested_live = leases
        .get(&lease_id)
        .is_some_and(|lease| lease.expires_at_millis > now_millis);
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
        ticket: OperationId::new(),
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
#[derive(Default)]
pub struct MemoryOperationWindowStore {
    states: Mutex<BTreeMap<WorkspaceId, OperationWindowSnapshot>>,
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
    use crate::Digest;

    fn generation(byte: u8) -> GenerationId {
        GenerationId::new(Digest::from_bytes([byte; 32]))
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
}
