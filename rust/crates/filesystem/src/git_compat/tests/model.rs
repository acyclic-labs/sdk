use super::*;
use crate::{
    MemoryOperationWindowStore, OperationLeaseId, OperationWindowCoordinator, OperationWindowError,
    OperationWindowPhase, OperationWindowStore,
};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

// In-memory adapters finish in one poll. A TLS executor is incorrect here:
// Loom's coroutines share the OS thread and may switch while a mutex blocks.
fn ready<F: Future>(mut future: Pin<Box<F>>) -> F::Output {
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("memory operation unexpectedly suspended"),
    }
}

#[test]
fn concurrent_branch_registration_publishes_only_accepted_updates() {
    registrations(MemoryGitCompatStore::default);
}

#[test]
#[should_panic(expected = "lost branch update")]
fn branch_model_detects_unconditional_replacement() {
    registrations(super::model_store::UncheckedStore::default);
}

fn registrations<S: GitCompatStore + 'static>(store: impl Fn() -> S + Send + Sync + 'static) {
    // Two writers, one update each: exhaust every scheduling and CAS ordering
    // through the real repository and instrumented MemoryRecords mutex.
    loom::model(move || {
        let repository = loom::sync::Arc::new(GitCompatRepository::new(
            WorkspaceId::from_bytes([0; 16]),
            super::model_store::CountedStore::new(store()),
        ));
        let mut writers = Vec::new();
        for (name, identity) in [("A", [1; 16]), ("B", [2; 16])] {
            let repository = repository.clone();
            writers.push(loom::thread::spawn(move || {
                let result = ready(Box::pin(repository.register_branch_workspace(
                    name,
                    WorkspaceId::from_bytes(identity),
                    None,
                    false,
                )));
                (name, identity, result)
            }));
        }
        let mut accepted = BTreeMap::new();
        for writer in writers {
            let (name, identity, result) = writer.join().unwrap();
            match result {
                Ok(_) => {
                    accepted.insert(name, WorkspaceId::from_bytes(identity));
                }
                Err(GitCompatError::Contended) => {}
                other => panic!("unexpected registration: {other:?}"),
            }
        }
        assert_eq!(
            repository.store.attempts(),
            2,
            "conditional write was repeated"
        );
        let state = ready(Box::pin(
            repository.store.load(WorkspaceId::from_bytes([0; 16])),
        ))
        .unwrap()
        .unwrap();
        assert!(
            !accepted.is_empty(),
            "neither conditional writer progressed"
        );
        assert_eq!(
            state.branches.len(),
            accepted.len() + 1,
            "lost branch update"
        );
        for name in ["A", "B"] {
            assert_eq!(
                state.branches.get(name).map(|branch| branch.workspace_id),
                accepted.get(name).copied(),
                "lost branch update"
            );
        }
        assert_eq!(state.revision, accepted.len() as u64);
        assert!(validate_git_state(&state).is_ok());
    });
}

#[test]
fn concurrent_renewal_fences_the_superseded_lease() {
    renewals(MemoryOperationWindowStore::new);
}

#[test]
#[should_panic(expected = "stale lease renewed")]
fn lease_model_detects_unconditional_replacement() {
    renewals(super::model_store::UncheckedStore::default);
}

fn renewals<S: OperationWindowStore + 'static>(store: impl Fn() -> S + Send + Sync + 'static) {
    // One lease, two renewals: run the real coordinator's snapshot/CAS
    // path in every interleaving, with deterministic time and identities.
    loom::model(move || {
        let coordinator = loom::sync::Arc::new(OperationWindowCoordinator::new(
            super::model_store::CountedStore::new(store()),
        ));
        let workspace = WorkspaceId::from_bytes([0; 16]);
        let lease = ready(Box::pin(coordinator.begin_with_lease_id(
            workspace,
            generation(1),
            "owner",
            1,
            10,
            OperationLeaseId::from_bytes([1; 16]),
        )))
        .unwrap();
        let mut renewals = Vec::new();
        for expiry in [20, 30] {
            let coordinator = coordinator.clone();
            let lease = lease.clone();
            renewals.push(loom::thread::spawn(move || {
                ready(Box::pin(coordinator.renew(&lease, 5, expiry)))
            }));
        }
        let mut successful = Vec::new();
        for renewal in renewals {
            match renewal.join().unwrap() {
                Ok(lease) => successful.push(lease),
                Err(OperationWindowError::StaleLease | OperationWindowError::Contended) => {}
                other => panic!("unexpected renewal: {other:?}"),
            }
        }
        assert_eq!(successful.len(), 1, "stale lease renewed");
        assert!(
            coordinator.store().attempts() <= 3,
            "conditional write was repeated"
        );
        let snapshot = ready(Box::pin(coordinator.snapshot(workspace))).unwrap();
        let OperationWindowPhase::Active {
            leases,
            pinned_parent,
            ..
        } = snapshot.phase
        else {
            panic!("renewal lost the active window");
        };
        assert_eq!(pinned_parent, generation(1));
        assert_eq!(leases.len(), 1);
        assert_eq!(
            leases[&lease.lease_id].expires_at_millis,
            successful[0].expires_at_millis
        );
        assert!(matches!(
            ready(Box::pin(coordinator.renew(&lease, 5, 40))),
            Err(OperationWindowError::StaleLease)
        ));
    });
}
