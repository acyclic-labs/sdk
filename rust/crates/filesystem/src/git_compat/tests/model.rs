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

struct InterruptedRestore(loom::sync::Mutex<Vec<(OperationId, GitFilesystemAction)>>);

impl GitFilesystemExecutor for InterruptedRestore {
    type Error = TestExecutorError;

    async fn validate_merge_resolution(
        &self,
        _: OperationId,
        _: &GitFilesystemAction,
        _: GitTreeRef,
    ) -> Result<GitFilesystemResult, Self::Error> {
        Err(TestExecutorError)
    }

    async fn execute(
        &self,
        operation: OperationId,
        action: &GitFilesystemAction,
    ) -> Result<GitFilesystemResult, Self::Error> {
        // Leaf trace lock, never held across a repository or authority call.
        self.0.lock().unwrap().push((operation, action.clone()));
        Err(TestExecutorError)
    }
}

#[test]
fn overlapping_published_restores_retain_one_atomic_source_selection() {
    restores(MemoryGitCompatStore::default);
}

#[test]
#[should_panic(expected = "overlapping restore replaced the pending selection")]
fn restore_model_detects_unconditional_replacement() {
    restores(super::model_store::UncheckedStore::default);
}

fn restores<S: GitCompatStore + 'static>(store: impl Fn() -> S + Send + Sync + 'static) {
    loom::model(move || {
        let repository = loom::sync::Arc::new(GitCompatRepository::new(workspace(), store()));
        let executor = loom::sync::Arc::new(InterruptedRestore(loom::sync::Mutex::new(Vec::new())));
        let writers = [2, 3].map(|source| {
            let repository = repository.clone();
            let executor = executor.clone();
            loom::thread::spawn(move || {
                let result = ready(Box::pin(repository.restore_paths_from(
                    tree(source),
                    vec![format!("/source-{source}")],
                    tree(1),
                    executor.as_ref(),
                )));
                assert!(matches!(
                    result,
                    Err(GitCompatRunError::Executor(_))
                        | Err(GitCompatRunError::Compat(
                            GitCompatError::TransitionPending(_)
                        ))
                ));
            })
        });
        for writer in writers {
            writer.join().unwrap();
        }
        let pending = ready(Box::pin(repository.pending_transition()))
            .unwrap()
            .unwrap();
        let trace = executor.0.lock().unwrap();
        assert_eq!(
            trace.len(),
            1,
            "overlapping restore replaced the pending selection"
        );
        assert_eq!(trace[0], (pending.id.operation_id(), pending.action));
    });
}

#[test]
fn concurrent_branch_registration_preserves_both_atomic_updates() {
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
            store(),
        ));
        let mut writers = Vec::new();
        for (name, identity) in [("A", [1; 16]), ("B", [2; 16])] {
            let repository = repository.clone();
            writers.push(loom::thread::spawn(move || {
                ready(Box::pin(repository.register_branch_workspace(
                    name,
                    WorkspaceId::from_bytes(identity),
                    None,
                    false,
                )))
                .unwrap();
            }));
        }
        for writer in writers {
            writer.join().unwrap();
        }
        let state = ready(Box::pin(
            repository.store.load(WorkspaceId::from_bytes([0; 16])),
        ))
        .unwrap()
        .unwrap();
        assert_eq!(state.branches.len(), 3, "lost branch update");
        assert_eq!(
            state.branches["A"].workspace_id,
            WorkspaceId::from_bytes([1; 16])
        );
        assert_eq!(
            state.branches["B"].workspace_id,
            WorkspaceId::from_bytes([2; 16])
        );
        assert_eq!(state.revision, 2);
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
    // One lease, two renewals: run the real coordinator's snapshot/CAS/retry
    // path in every interleaving, with deterministic time and identities.
    loom::model(move || {
        let coordinator = loom::sync::Arc::new(OperationWindowCoordinator::new(store()));
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
                Err(OperationWindowError::StaleLease) => {}
                other => panic!("unexpected renewal: {other:?}"),
            }
        }
        assert_eq!(successful.len(), 1, "stale lease renewed");
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
