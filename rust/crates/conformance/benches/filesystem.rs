//! Local Filesystem publication and read cost (see `docs/observability.md`).
#![allow(clippy::unwrap_used, reason = "benchmarks abort on setup failure")]

use acyclic_fs::{
    Fs, IdempotencyKey, LocalAuthorityBackend, LocalFs, LocalObjectBackend, LocalOptions, Workspace,
};
use bytes::Bytes;
use divan::Bencher;
use divan::counter::ItemsCount;
use tokio::runtime::Runtime;

type LocalWorkspace = Workspace<LocalAuthorityBackend, LocalObjectBackend>;

pub(crate) fn main() {
    if std::env::var_os("ACYCLIC_BENCH_TRACE").is_some() {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_writer(std::io::sink)
            .init();
    }
    divan::main();
}

#[divan::bench(args = [0, 1, 16, 128], sample_count = 200, sample_size = 64)]
fn accounting_reconcile(bencher: Bencher, observers: usize) {
    accounting_bench(bencher, observers, false);
}

#[divan::bench(args = [0, 128], threads = [4], sample_count = 200, sample_size = 64)]
fn accounting_shared(bencher: Bencher, observers: usize) {
    accounting_bench(bencher, observers, true);
}

fn accounting_bench(bencher: Bencher, observers: usize, shared: bool) {
    let runtime = Runtime::new().unwrap();
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::performance::BOUNDARY_BUDGET;
    let fs = Fs::memory();
    let workspace = runtime
        .block_on(Box::pin(fs.create_workspace(
            "accounting",
            budget,
            &cancellation,
        )))
        .unwrap()
        .value;
    let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation).unwrap();
    let mut checkouts = Vec::with_capacity(observers);
    for _ in 0..observers {
        checkouts.push(
            runtime
                .block_on(Box::pin(workspace.checkout(
                    acyclic_fs::model::GenerationSelector::Head,
                    acyclic_fs::model::CheckoutMode::read_only_pinned(),
                    &work,
                    &cancellation,
                )))
                .unwrap(),
        );
    }
    let delta = acyclic_fs::WorkCounters {
        items_examined: 1,
        ..acyclic_fs::WorkCounters::UNCHARGED
    };
    if std::env::var_os("ACYCLIC_BENCH_WORK").is_some() {
        eprintln!(
            "observers={observers} live={} work={:?}",
            work.live_allocation_bytes(),
            work.receipt()
        );
    }
    if shared {
        bencher.bench(|| work.charge(delta).unwrap());
    } else {
        bencher.bench_local(|| work.charge(delta).unwrap());
    }
    if std::env::var_os("ACYCLIC_BENCH_WORK").is_some() {
        eprintln!("after observers={observers} work={:?}", work.receipt());
    }
    std::hint::black_box(checkouts);
}

#[divan::bench(sample_count = 200, sample_size = 64)]
fn accounting_charge_local(bencher: Bencher) {
    let mut work = acyclic_fs::WorkCounters::UNCHARGED;
    let budget = acyclic_fs::performance::BOUNDARY_BUDGET;
    bencher.bench_local(|| work.charge_items(1, &budget).unwrap());
    std::hint::black_box(work);
}

#[divan::bench(args = [(0, false), (128, false), (128, true)], threads = [1, 4], sample_count = 200, sample_size = 64)]
fn accounting_source_registry(bencher: Bencher, (count, retire): (usize, bool)) {
    use acyclic_fs::demand::native::NativeDemandSource;
    use acyclic_fs::model::{FilesystemProfile, VolumeLimits};
    acyclic_native_runtime::initialize_native_io_services().unwrap();
    let runtime = Runtime::new().unwrap();
    let root = tempfile::tempdir().unwrap();
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::BOUNDARY_BUDGET,
        &cancellation,
    )
    .unwrap();
    let mut sources = Vec::with_capacity(count);
    for _ in 0..count {
        sources.push(
            runtime
                .block_on(NativeDemandSource::open(
                    std::borrow::Cow::Borrowed(root.path()),
                    FilesystemProfile::Portable,
                    VolumeLimits::default(),
                    None,
                    None,
                    &work,
                    &cancellation,
                ))
                .unwrap(),
        );
    }
    if retire {
        sources.clear();
    }
    let delta = acyclic_fs::WorkCounters {
        items_examined: 1,
        ..acyclic_fs::WorkCounters::UNCHARGED
    };
    if std::env::var_os("ACYCLIC_BENCH_WORK").is_some() {
        eprintln!(
            "before sources={count} retired={retire} live={} work={:?}",
            work.live_allocation_bytes(),
            work.receipt()
        );
    }
    bencher.bench(|| work.charge(delta).unwrap());
    if std::env::var_os("ACYCLIC_BENCH_WORK").is_some() {
        eprintln!(
            "after sources={count} retired={retire} live={} work={:?}",
            work.live_allocation_bytes(),
            work.receipt()
        );
    }
    std::hint::black_box(sources);
}

#[divan::bench(args = [false, true], threads = [1, 4])]
fn authority_head(bencher: Bencher, native_accounting: bool) {
    authority_head_bench(bencher, native_accounting, false);
}

#[divan::bench(threads = [1, 4], sample_count = 64, sample_size = 8)]
fn authority_head_shared(bencher: Bencher) {
    authority_head_bench(bencher, true, true);
}

fn authority_head_bench(bencher: Bencher, native_accounting: bool, shared: bool) {
    use acyclic_fs::{AsyncAuthorityStore, AuthorityId, AuthorityWork, Epoch};
    let runtime = Runtime::new().unwrap();
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::performance::BOUNDARY_BUDGET;
    let store = acyclic_fs::distributed::StreamAuthorityStore::new(std::sync::Arc::new(
        acyclic_stream::MemoryStream::default(),
    ));
    let authority = AuthorityId::from_bytes([1; 16]);
    runtime
        .block_on(store.create_authority(
            authority,
            Epoch::GENESIS,
            &mut AuthorityWork::new(budget, &cancellation),
            None::<&mut ()>,
        ))
        .unwrap();
    if std::env::var_os("ACYCLIC_BENCH_WORK").is_some() {
        let original = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation).unwrap();
        let mut work = if native_accounting {
            AuthorityWork::native(&original, &cancellation)
        } else {
            AuthorityWork::new(budget, &cancellation)
        };
        runtime.block_on(store.head(authority, &mut work)).unwrap();
        eprintln!(
            "head native={native_accounting} live={} work={:?}",
            original.live_allocation_bytes(),
            work.spent()
        );
    }
    let shared_original =
        shared.then(|| acyclic_fs::NativeMaterializationWork::new(budget, &cancellation).unwrap());
    let name = if shared {
        "authority_head_shared"
    } else {
        "authority_head"
    };
    benchmark_marker(name, native_accounting, true);
    bencher.bench(|| {
        let original = (native_accounting && !shared)
            .then(|| acyclic_fs::NativeMaterializationWork::new(budget, &cancellation).unwrap());
        let mut work = match shared_original.as_ref().or(original.as_ref()) {
            Some(original) => AuthorityWork::native(original, &cancellation),
            None => AuthorityWork::new(budget, &cancellation),
        };
        runtime.block_on(store.head(authority, &mut work)).unwrap()
    });
    benchmark_marker(name, native_accounting, false);
    if std::env::var_os("ACYCLIC_BENCH_WORK").is_some()
        && let Some(original) = shared_original
    {
        eprintln!(
            "shared head live={} work={:?}",
            original.live_allocation_bytes(),
            original.receipt()
        );
    }
}

fn workspace(runtime: &Runtime) -> (tempfile::TempDir, LocalFs, LocalWorkspace) {
    let root = tempfile::tempdir().unwrap();
    runtime.block_on(async {
        let fs = Box::pin(Fs::local(LocalOptions::new(root.path())))
            .await
            .unwrap();
        let workspace = Box::pin(fs.create_workspace(
            "bench",
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        ))
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .unwrap();
        (root, fs, workspace)
    })
}

/// One small file written and published as its own generation.
#[divan::bench]
fn write_publish(bencher: Bencher) {
    let runtime = Runtime::new().unwrap();
    let (_root, _fs, workspace) = workspace(&runtime);
    let mut next = 0_u32;
    bencher.bench_local(|| {
        next += 1;
        let path = format!("/entry-{next}");
        runtime
            .block_on(Box::pin(
                workspace.write(&path, Bytes::from_static(b"value")),
            ))
            .unwrap();
    });
}

/// Sixteen small files published by one transaction.
#[divan::bench]
fn transaction_publish_16(bencher: Bencher) {
    let runtime = Runtime::new().unwrap();
    let (_root, _fs, workspace) = workspace(&runtime);
    let mut next = 0_u32;
    bencher.counter(ItemsCount::new(16_usize)).bench_local(|| {
        next += 1;
        runtime.block_on(async {
            let mut transaction = Box::pin(async {
                workspace
                    .begin_transaction(
                        IdempotencyKey::new(),
                        acyclic_fs::WorkBudget::UNBOUNDED,
                        &acyclic_fs::CancellationToken::new(),
                    )
                    .await
                    .map(|receipt| receipt.value)
                    .map_err(|failure| failure.error)
            })
            .await
            .unwrap();
            for item in 0..16 {
                let path = format!("/batch-{next}-{item}");
                Box::pin(transaction.write(
                    &path,
                    Bytes::from_static(b"value"),
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    &acyclic_fs::CancellationToken::new(),
                ))
                .await
                .unwrap();
            }
            Box::pin(async {
                transaction
                    .commit(
                        None,
                        acyclic_fs::PublicationPermit::Unrestricted,
                        acyclic_fs::WorkBudget::UNBOUNDED,
                        &acyclic_fs::CancellationToken::new(),
                        |_| async {
                            Ok::<_, acyclic_fs::OperationFailure<acyclic_fs::WorkspaceError>>(
                                acyclic_fs::OperationReceipt {
                                    value: (),
                                    work: acyclic_fs::WorkCounters::default(),
                                },
                            )
                        },
                    )
                    .await
                    .map(|receipt| receipt.value)
                    .map_err(|failure| failure.error)
            })
            .await
            .unwrap();
        });
    });
}

/// One small file read from the head generation.
#[divan::bench(args = [false, true])]
fn read_small(bencher: Bencher, native_accounting: bool) {
    let runtime = Runtime::new().unwrap();
    let (_root, _fs, workspace) = workspace(&runtime);
    runtime
        .block_on(Box::pin(
            workspace.write("/entry", Bytes::from_static(b"value")),
        ))
        .unwrap();
    let cancellation = acyclic_fs::CancellationToken::new();
    if std::env::var_os("ACYCLIC_BENCH_WORK").is_some() && native_accounting {
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::BOUNDARY_BUDGET,
            &cancellation,
        )
        .unwrap();
        let read = runtime
            .block_on(Box::pin(workspace.read("/entry", 64, &work, &cancellation)))
            .unwrap();
        eprintln!("local read native=true: {:?}", work.receipt());
        std::hint::black_box(read);
    } else if std::env::var_os("ACYCLIC_BENCH_WORK").is_some() {
        let read = runtime
            .block_on(Box::pin(workspace.read(
                "/entry",
                64,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &cancellation,
            )))
            .unwrap();
        eprintln!("local read native=false: {:?}", read.work);
    }
    benchmark_marker("read_small", native_accounting, true);
    bencher.bench_local(|| {
        if native_accounting {
            let work = acyclic_fs::NativeMaterializationWork::new(
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &cancellation,
            )
            .unwrap();
            return runtime
                .block_on(Box::pin(workspace.read("/entry", 64, &work, &cancellation)))
                .unwrap();
        }
        runtime
            .block_on(Box::pin(workspace.read(
                "/entry",
                64,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &cancellation,
            )))
            .unwrap()
            .value
    });
    benchmark_marker("read_small", native_accounting, false);
}

#[divan::bench(args = [false, true], threads = [1, 4])]
fn read_memory_small(bencher: Bencher, native_accounting: bool) {
    let runtime = Runtime::new().unwrap();
    let fs = Fs::memory();
    let cancellation = acyclic_fs::CancellationToken::new();
    let workspace = runtime
        .block_on(Box::pin(fs.create_workspace(
            "bench",
            acyclic_fs::performance::BOUNDARY_BUDGET,
            &cancellation,
        )))
        .unwrap()
        .value;
    runtime
        .block_on(Box::pin(
            workspace.write("/entry", Bytes::from_static(b"value")),
        ))
        .unwrap();
    if std::env::var_os("ACYCLIC_BENCH_WORK").is_some() && native_accounting {
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::BOUNDARY_BUDGET,
            &cancellation,
        )
        .unwrap();
        let read = runtime
            .block_on(Box::pin(workspace.read("/entry", 64, &work, &cancellation)))
            .unwrap();
        eprintln!("memory read native=true: {:?}", work.receipt());
        std::hint::black_box(read);
    } else if std::env::var_os("ACYCLIC_BENCH_WORK").is_some() {
        let read = runtime
            .block_on(Box::pin(workspace.read(
                "/entry",
                64,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &cancellation,
            )))
            .unwrap();
        eprintln!("memory read native=false: {:?}", read.work);
    }
    benchmark_marker("read_memory_small", native_accounting, true);
    bencher.bench(|| {
        if native_accounting {
            let work = acyclic_fs::NativeMaterializationWork::new(
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &cancellation,
            )
            .unwrap();
            return runtime
                .block_on(Box::pin(workspace.read("/entry", 64, &work, &cancellation)))
                .unwrap();
        }
        runtime
            .block_on(Box::pin(workspace.read(
                "/entry",
                64,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &cancellation,
            )))
            .unwrap()
            .value
    });
    benchmark_marker("read_memory_small", native_accounting, false);
}

fn benchmark_marker(name: &str, native: bool, begin: bool) {
    if std::env::var_os("ACYCLIC_BENCH_MARKERS").is_some() {
        eprintln!("ACYCLIC_BENCH_BOUNDARY name={name} native={native} begin={begin}");
    }
}
