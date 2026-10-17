use acyclic_fs::{
    AppendOutcome, AsyncAuthorityStore, AuthorityId, AuthorityWork, CancellationToken, Digest,
    Epoch, GenerationId, GuardedAppend, NativeMaterializationWork, OperationId, OperationLease,
    OperationLeaseId, OperationWindowCoordinator, OperationWindowPhase, OperationWindowSnapshot,
    OperationWindowStore, ProposedCommit, PublicationPermit, StreamAuthorityStore,
    StreamOperationWindowStore, WorkCounters, WorkspaceId,
};
use acyclic_stream::MemoryStream;
use bytes::Bytes;
use divan::Bencher;
use std::{collections::BTreeMap, sync::Arc};
use tokio::runtime::Runtime;

const WORKSPACE: WorkspaceId = WorkspaceId::from_bytes([7; 16]);
const LEASE: OperationLeaseId = OperationLeaseId::from_bytes([11; 16]);

pub fn main() {
    if std::env::var_os("ACYCLIC_BENCH_TRACE").is_some() {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_writer(std::io::sink)
            .init();
    }
    divan::main();
}

fn boundary(name: &str, native: bool, begin: bool, enabled: bool) {
    if enabled {
        eprintln!("ACYCLIC_LEASE_BOUNDARY name={name} native={native} begin={begin}");
    }
}

#[divan::bench]
fn open_lease(bencher: Bencher) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let markers = std::env::var_os("ACYCLIC_BENCH_MARKERS").is_some();
    bencher
        .with_inputs(|| {
            let store = StreamOperationWindowStore::new(Arc::new(MemoryStream::default()));
            let snapshot = OperationWindowSnapshot {
                version: 1,
                revision: 1,
                workspace_id: WORKSPACE,
                phase: OperationWindowPhase::Active {
                    pinned_parent: GenerationId::new(Digest::from_bytes([3; 32])),
                    leases: BTreeMap::from([(
                        LEASE,
                        OperationLease {
                            id: LEASE,
                            owner: "bench".into(),
                            expires_at_millis: u64::MAX,
                        },
                    )]),
                    pending_parent: None,
                },
            };
            (store, Some(snapshot))
        })
        .bench_local_refs(|input| {
            let snapshot = input.1.take().unwrap();
            boundary("open", false, true, markers);
            assert!(
                runtime
                    .block_on(input.0.compare_and_swap(WORKSPACE, 0, snapshot))
                    .unwrap()
            );
            boundary("open", false, false, markers);
        });
}

fn commit(
    runtime: &Runtime,
    store: &StreamAuthorityStore<MemoryStream>,
    request: GuardedAppend,
    cancellation: &CancellationToken,
    original: Option<&NativeMaterializationWork>,
) -> WorkCounters {
    let mut work = match original {
        Some(original) => AuthorityWork::native(original, cancellation),
        None => AuthorityWork::new(acyclic_fs::performance::BOUNDARY_BUDGET, cancellation),
    };
    let outcome = runtime
        .block_on(store.compare_and_append(request, &mut work, None::<&mut ()>))
        .unwrap();
    assert!(matches!(outcome, AppendOutcome::Committed(_)));
    work.spent()
}

#[divan::bench(args = [false, true])]
fn commit_under_lease(bencher: Bencher, native: bool) {
    acyclic_native_runtime::initialize_native_io_services().unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let input = || {
        let cancellation = CancellationToken::new();
        let stream = Arc::new(MemoryStream::default());
        let windows =
            OperationWindowCoordinator::new(StreamOperationWindowStore::new(stream.clone()));
        let lease = runtime
            .block_on(windows.begin_with_lease_id(
                WORKSPACE,
                GenerationId::new(Digest::ZERO),
                "bench",
                1,
                u64::MAX,
                LEASE,
            ))
            .unwrap();
        let permit = lease.publication_permit();
        let PublicationPermit::Lease { authority_id, .. } = permit else {
            unreachable!()
        };
        let authority_id = AuthorityId::from_bytes(authority_id);
        let store = StreamAuthorityStore::new(stream);
        let budget = acyclic_fs::performance::BOUNDARY_BUDGET;
        let mut work = AuthorityWork::new(budget, &cancellation);
        runtime
            .block_on(store.create_authority(
                authority_id,
                Epoch::GENESIS,
                &mut work,
                None::<&mut ()>,
            ))
            .unwrap();
        let head = runtime
            .block_on(store.head(authority_id, &mut work))
            .unwrap();
        let request = GuardedAppend {
            authority_id,
            epoch: head.epoch,
            expected: head,
            permit,
            commit: ProposedCommit {
                operation_id: OperationId::from_bytes([13; 16]),
                fingerprint: Digest::from_bytes([17; 32]),
                payload: Bytes::from_static(b"body"),
            },
        };
        let original =
            native.then(|| NativeMaterializationWork::new(budget, &cancellation).unwrap());
        (store, cancellation, original, Some(request))
    };
    if std::env::var_os("ACYCLIC_BENCH_WORK").is_some() {
        let (store, cancellation, original, request) = input();
        let receipt = commit(
            &runtime,
            &store,
            request.unwrap(),
            &cancellation,
            original.as_ref(),
        );
        eprintln!("lease commit native={native} work={receipt:?}");
    }
    let markers = std::env::var_os("ACYCLIC_BENCH_MARKERS").is_some();
    bencher.with_inputs(input).bench_local_refs(|input| {
        let request = input.3.take().unwrap();
        boundary("commit", native, true, markers);
        let receipt = commit(&runtime, &input.0, request, &input.1, input.2.as_ref());
        boundary("commit", native, false, markers);
        receipt
    });
}
