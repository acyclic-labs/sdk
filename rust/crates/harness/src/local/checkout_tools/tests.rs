//! The same StreamAuthorityStore/operation-window path used by LocalFs.
use acyclic_fs::{AsyncAuthorityStore, AuthorityWork, CancellationToken, WorkBudget};
use acyclic_stream::MemoryStream;
use std::sync::Arc;

// This fails to compile if the model command forgets to instrument the dependency.
const _: fn(acyclic_stream::query_sync::SyncMutex<()>) -> loom::sync::Mutex<()> = |lock| lock;

struct Clock;
impl acyclic_stream::UnixMillisClock for Clock {
    fn now_unix_millis(&self) -> u64 {
        1
    }
}

/// Runs one model phase to completion on the current loom thread.
///
/// Each phase's future is built in this frame, so it occupies the stack only
/// while that phase runs. Built inline, every phase's future stayed in the
/// model's one frame for the whole model: the 5,408-byte `begin_with_lease_id`
/// future sat beneath the entire `create_authority` call chain. No product
/// caller stacks phase futures in one frame; each awaits one operation at a
/// time.
// Never inlined: inlined, the phase futures return to the model's frame.
#[inline(never)]
fn phase<F: std::future::Future>(start: impl FnOnce() -> F) -> F::Output {
    loom::future::block_on(Box::pin(start()))
}

fn publication_after_revocation(broken: bool, fork: bool) {
    let provider = Arc::new(MemoryStream::new_with_clock(
        Default::default(),
        Arc::new(Clock),
    ));
    let authority = acyclic_fs::StreamAuthorityStore::new(provider.clone());
    let windows = acyclic_fs::OperationWindowCoordinator::new(
        acyclic_fs::StreamOperationWindowStore::new(provider),
    );
    let workspace = acyclic_fs::WorkspaceId::from_bytes([7; 16]);
    let authority_id = acyclic_fs::kernel::volume_authority_id(workspace.volume_id());
    let cancellation = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let head = match phase(|| {
        authority.create_authority::<()>(authority_id, acyclic_fs::Epoch::GENESIS, &mut work, None)
    })
    .unwrap()
    {
        acyclic_fs::CreateAuthorityOutcome::Created(head) => head,
        _ => panic!("model authority already existed"),
    };
    let lease = phase(|| {
        windows.begin_with_lease_id(
            workspace,
            acyclic_fs::GenerationId::new(acyclic_fs::Digest::from_bytes([1; 32])),
            "task:reservation",
            1,
            10_000,
            acyclic_fs::OperationLeaseId::from_bytes([9; 16]),
        )
    })
    .unwrap();
    let (ready, preparing) = loom::sync::mpsc::channel();
    let (closed, revoked) = loom::sync::mpsc::channel();
    let closing = lease.clone();
    let closer = loom::thread::spawn(move || {
        preparing.recv().unwrap();
        phase(|| windows.finish(&closing, 2)).unwrap();
        closed.send(()).unwrap();
    });
    let writer = loom::thread::spawn(move || {
        let cancellation = CancellationToken::new();
        let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let mut preparation = Some((ready, revoked));
        let mut before_publish = move |_| {
            if let Some((ready, revoked)) = preparation.take() {
                ready.send(()).unwrap();
                revoked.recv().unwrap();
            }
            std::future::ready(Ok::<_, acyclic_fs::OperationFailure<acyclic_fs::FsError>>(
                acyclic_fs::OperationReceipt {
                    value: (),
                    work: acyclic_fs::WorkCounters::UNCHARGED,
                },
            ))
        };
        let mut publish = &mut before_publish;
        let request = acyclic_fs::GuardedAppend {
            authority_id,
            epoch: head.epoch,
            expected: head,
            commit: acyclic_fs::ProposedCommit {
                publication: None,
                operation_id: acyclic_fs::OperationId::from_bytes([3; 16]),
                fingerprint: acyclic_fs::Digest::from_bytes([4; 32]),
                payload: bytes::Bytes::from_static(b"model-publication"),
            },
            permit: if broken {
                acyclic_fs::PublicationPermit::Unrestricted
            } else {
                lease.publication_permit()
            },
        };
        if fork {
            let proposed = request.commit;
            let fork = acyclic_fs::WorkspaceForkCommit {
                lineage: Some(acyclic_fs::GenerationForkSource {
                    authority: authority_id,
                    generation: acyclic_fs::GenerationId::new(acyclic_fs::Digest::from_bytes(
                        [1; 32],
                    )),
                    lineage: acyclic_fs::GenerationFork::Independent,
                }),
                destination: acyclic_fs::kernel::volume_authority_id(
                    acyclic_fs::WorkspaceId::from_bytes([8; 16]).volume_id(),
                ),
                retention: acyclic_fs::kernel::volume_authority_id(
                    acyclic_fs::WorkspaceId::from_bytes([6; 16]).volume_id(),
                ),
                creation: proposed.clone(),
                retained: proposed,
            };
            matches!(
                phase(|| {
                    authority.fork_workspace(fork, request.permit, &mut work, Some(&mut publish))
                })
                .unwrap(),
                acyclic_fs::WorkspaceForkOutcome::Committed
            )
        } else {
            matches!(
                phase(|| authority.compare_and_append(request, &mut work, Some(&mut publish)))
                    .unwrap(),
                acyclic_fs::AppendOutcome::Committed(_)
                    | acyclic_fs::AppendOutcome::AlreadyCommitted(_)
            )
        }
    });
    closer.join().unwrap();
    assert!(!writer.join().unwrap(), "stale git publication committed");
}

#[test]
fn task_git_permit_is_checked_atomically_after_preparation() {
    for fork in [false, true] {
        loom::model(move || publication_after_revocation(false, fork));
        let failure = std::panic::catch_unwind(|| {
            loom::model(move || publication_after_revocation(true, fork))
        })
        .expect_err("unrestricted negative control was accepted");
        let message = failure
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| failure.downcast_ref::<&str>().copied())
            .unwrap_or("");
        assert!(
            message.contains("stale git publication committed"),
            "unexpected negative control: {message}"
        );
    }
}
