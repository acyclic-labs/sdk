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
    if std::env::var_os("ACYCLIC_BENCH_WORK").is_some() {
        let read = runtime
            .block_on(Box::pin(workspace.read(
                "/entry",
                64,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &cancellation,
            )))
            .unwrap();
        eprintln!("local read: {:?}", read.work);
    }
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
    if std::env::var_os("ACYCLIC_BENCH_WORK").is_some() {
        let read = runtime
            .block_on(Box::pin(workspace.read(
                "/entry",
                64,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &cancellation,
            )))
            .unwrap();
        eprintln!("memory read: {:?}", read.work);
    }
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
}
