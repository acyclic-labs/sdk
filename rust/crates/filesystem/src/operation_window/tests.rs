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
    store.states.0.lock().expect("state lock").insert(
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
async fn zero_reconcile_limits_do_not_close_or_claim_a_window() {
    for field in 0..3 {
        let fs = Fs::memory();
        let parent = fs
            .create_workspace(
                "invalid-limit-parent",
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .expect("parent")
            .value;
        let child = parent
            .fork::<_, _, crate::WorkspaceError>(
                "invalid-limit-child",
                ForkOptions::from_generation(
                    parent
                        .head(
                            crate::WorkBudget::UNBOUNDED,
                            &crate::CancellationToken::new(),
                        )
                        .await
                        .expect("base")
                        .value,
                    IdempotencyKey::new(),
                ),
                crate::PublicationPermit::Unrestricted,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
                &mut |_| {
                    std::future::ready(Ok(crate::OperationReceipt {
                        value: (),
                        work: crate::WorkCounters::UNCHARGED,
                    }))
                },
            )
            .await
            .expect("child")
            .value;
        let coordinator = OperationWindowCoordinator::new(MemoryOperationWindowStore::new());
        let lease = coordinator
            .begin(
                child.id(),
                parent
                    .head(
                        crate::WorkBudget::UNBOUNDED,
                        &crate::CancellationToken::new(),
                    )
                    .await
                    .expect("parent head")
                    .value
                    .id(),
                "tool",
                1,
                20,
            )
            .await
            .expect("lease");
        let mut limits = OperationReconcileLimits::default();
        match field {
            0 => limits.maximum_generations = 0,
            1 => limits.maximum_changes = 0,
            2 => limits.maximum_conflicts = 0,
            _ => unreachable!("only the three reconcile bounds are covered"),
        }

        assert!(matches!(
            coordinator
                .finish_workspace(&child, &lease, 2, limits)
                .await,
            Err(OperationWindowError::Workspace(WorkspaceError::JoinLimit))
        ));
        assert!(matches!(
            coordinator.inspect(child.id()).await.expect("active window").phase,
            OperationWindowPhase::Active { ref leases, .. }
                if leases.len() == 1 && leases.contains_key(&lease.lease_id)
        ));

        assert!(matches!(
            coordinator.recover_workspace(&child, 20, limits).await,
            Err(OperationWindowError::Workspace(WorkspaceError::JoinLimit))
        ));
        assert!(matches!(
            coordinator.inspect(child.id()).await.expect("unclaimed window").phase,
            OperationWindowPhase::Active { ref leases, .. }
                if leases.len() == 1 && leases.contains_key(&lease.lease_id)
        ));
    }
}

#[tokio::test]
async fn overlapping_leases_coalesce_parent_and_reconcile_once() {
    let workspace = WorkspaceId::derive(
        [7; 16],
        &crate::WorkspaceName::new(
            "agent",
            &crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .expect("valid test workspace")
        .value,
        &crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .expect("unbounded workspace identity")
    .value;
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
    let parent = fs
        .create_workspace(
            "late-parent",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .expect("parent")
        .value;
    let child = parent
        .fork::<_, _, crate::WorkspaceError>(
            "late-child",
            ForkOptions::from_generation(
                parent
                    .head(
                        crate::WorkBudget::UNBOUNDED,
                        &crate::CancellationToken::new(),
                    )
                    .await
                    .expect("base")
                    .value,
                IdempotencyKey::new(),
            ),
            crate::PublicationPermit::Unrestricted,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
            &mut |_| {
                std::future::ready(Ok(crate::OperationReceipt {
                    value: (),
                    work: crate::WorkCounters::UNCHARGED,
                }))
            },
        )
        .await
        .expect("child")
        .value;
    child
        .write_text("/child", "local")
        .await
        .expect("child write");
    let coordinator = OperationWindowCoordinator::new(MemoryOperationWindowStore::new());
    let lease = coordinator
        .begin(
            child.id(),
            parent
                .head(
                    crate::WorkBudget::UNBOUNDED,
                    &crate::CancellationToken::new(),
                )
                .await
                .expect("parent head")
                .value
                .id(),
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
                parent
                    .head(
                        crate::WorkBudget::UNBOUNDED,
                        &crate::CancellationToken::new()
                    )
                    .await
                    .expect("new parent head")
                    .value
                    .id(),
                OperationReconcileLimits::default(),
            )
            .await
            .expect("parent advance"),
        Some(WorkspaceRebase::Rebased(_))
    ));
    assert_eq!(
        child
            .read(
                "/parent",
                16,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await
            .expect("upstream")
            .value
            .as_slice(),
        b"upstream"
    );
    assert_eq!(
        child
            .read(
                "/child",
                16,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await
            .expect("local")
            .value
            .as_slice(),
        b"local"
    );
    assert!(matches!(
        coordinator.inspect(child.id()).await.expect("window").phase,
        OperationWindowPhase::Idle
    ));
}

#[tokio::test]
async fn final_close_of_a_current_fork_releases_the_window_in_one_transition() {
    let fs = Fs::memory();
    let parent = fs
        .create_workspace(
            "current-parent",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .expect("parent")
        .value;
    let child = parent
        .fork::<_, _, crate::WorkspaceError>(
            "current-child",
            ForkOptions::from_generation(
                parent
                    .head(
                        crate::WorkBudget::UNBOUNDED,
                        &crate::CancellationToken::new(),
                    )
                    .await
                    .expect("base")
                    .value,
                IdempotencyKey::new(),
            ),
            crate::PublicationPermit::Unrestricted,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
            &mut |_| {
                std::future::ready(Ok(crate::OperationReceipt {
                    value: (),
                    work: crate::WorkCounters::UNCHARGED,
                }))
            },
        )
        .await
        .expect("child")
        .value;
    let coordinator = OperationWindowCoordinator::new(MemoryOperationWindowStore::new());
    let lease = coordinator
        .begin(
            child.id(),
            parent
                .head(
                    crate::WorkBudget::UNBOUNDED,
                    &crate::CancellationToken::new(),
                )
                .await
                .expect("parent head")
                .value
                .id(),
            "tool",
            1,
            100,
        )
        .await
        .expect("lease");
    let opened = coordinator.inspect(child.id()).await.expect("window");
    assert!(matches!(
        coordinator
            .finish_workspace(&child, &lease, 2, OperationReconcileLimits::default())
            .await
            .expect("final close"),
        WorkspaceOperationFinish::Reconciled(WorkspaceRebase::Current(_))
    ));
    let closed = coordinator.inspect(child.id()).await.expect("window");
    assert_eq!(closed.phase, OperationWindowPhase::Idle);
    assert_eq!(closed.revision, opened.revision + 1);
}

#[tokio::test]
async fn final_close_rebases_onto_a_parent_advance_it_never_observed() {
    let fs = Fs::memory();
    let parent = fs
        .create_workspace(
            "unobserved-parent",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .expect("parent")
        .value;
    let child = parent
        .fork::<_, _, crate::WorkspaceError>(
            "unobserved-child",
            ForkOptions::from_generation(
                parent
                    .head(
                        crate::WorkBudget::UNBOUNDED,
                        &crate::CancellationToken::new(),
                    )
                    .await
                    .expect("base")
                    .value,
                IdempotencyKey::new(),
            ),
            crate::PublicationPermit::Unrestricted,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
            &mut |_| {
                std::future::ready(Ok(crate::OperationReceipt {
                    value: (),
                    work: crate::WorkCounters::UNCHARGED,
                }))
            },
        )
        .await
        .expect("child")
        .value;
    parent
        .write_text("/parent", "unobserved")
        .await
        .expect("parent write");
    let coordinator = OperationWindowCoordinator::new(MemoryOperationWindowStore::new());
    // The window pins the advanced head, which the fork is not based on.
    let lease = coordinator
        .begin(
            child.id(),
            parent
                .head(
                    crate::WorkBudget::UNBOUNDED,
                    &crate::CancellationToken::new(),
                )
                .await
                .expect("parent head")
                .value
                .id(),
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
            .expect("final close"),
        WorkspaceOperationFinish::Reconciled(WorkspaceRebase::Rebased(_))
    ));
    assert_eq!(
        child
            .read(
                "/parent",
                16,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await
            .expect("upstream")
            .value
            .as_ref(),
        b"unobserved"
    );
    assert_eq!(
        coordinator.inspect(child.id()).await.expect("window").phase,
        OperationWindowPhase::Idle
    );
}

#[tokio::test]
async fn parent_advance_during_child_tool_rebases_at_final_close() {
    let fs = Fs::memory();
    let parent = fs
        .create_workspace(
            "active-parent",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .expect("parent")
        .value;
    let child = parent
        .fork::<_, _, crate::WorkspaceError>(
            "active-child",
            ForkOptions::from_generation(
                parent
                    .head(
                        crate::WorkBudget::UNBOUNDED,
                        &crate::CancellationToken::new(),
                    )
                    .await
                    .expect("base")
                    .value,
                IdempotencyKey::new(),
            ),
            crate::PublicationPermit::Unrestricted,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
            &mut |_| {
                std::future::ready(Ok(crate::OperationReceipt {
                    value: (),
                    work: crate::WorkCounters::UNCHARGED,
                }))
            },
        )
        .await
        .expect("child")
        .value;
    let coordinator = OperationWindowCoordinator::new(MemoryOperationWindowStore::new());
    let lease = coordinator
        .begin(
            child.id(),
            parent
                .head(
                    crate::WorkBudget::UNBOUNDED,
                    &crate::CancellationToken::new(),
                )
                .await
                .expect("parent head")
                .value
                .id(),
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
                parent
                    .head(
                        crate::WorkBudget::UNBOUNDED,
                        &crate::CancellationToken::new()
                    )
                    .await
                    .expect("new parent head")
                    .value
                    .id(),
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
        child
            .read(
                "/parent",
                16,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await
            .expect("upstream")
            .value
            .as_ref(),
        b"new"
    );
}

#[tokio::test]
async fn deterministic_lease_identity_is_idempotent_but_cannot_be_rebound() {
    let workspace = WorkspaceId::derive(
        [12; 16],
        &crate::WorkspaceName::new(
            "lease-identity",
            &crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .expect("valid test workspace")
        .value,
        &crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .expect("unbounded workspace identity")
    .value;
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
        &crate::WorkspaceName::new(
            "lease-renewal",
            &crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .expect("valid test workspace")
        .value,
        &crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .expect("unbounded workspace identity")
    .value;
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
        &crate::WorkspaceName::new(
            "agent",
            &crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .expect("valid test workspace")
        .value,
        &crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .expect("unbounded workspace identity")
    .value;
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
        &crate::WorkspaceName::new(
            "expired",
            &crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .expect("valid test workspace")
        .value,
        &crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .expect("unbounded workspace identity")
    .value;
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
        &crate::WorkspaceName::new(
            "overlap-expiry",
            &crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .expect("valid test workspace")
        .value,
        &crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .expect("unbounded workspace identity")
    .value;
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
        &crate::WorkspaceName::new(
            "duplicate-expiry",
            &crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .expect("valid test workspace")
        .value,
        &crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .expect("unbounded workspace identity")
    .value;
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

mod streams;
