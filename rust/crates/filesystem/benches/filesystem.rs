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

fn main() {
    divan::main();
}

fn workspace(runtime: &Runtime) -> (tempfile::TempDir, LocalFs, LocalWorkspace) {
    let root = tempfile::tempdir().unwrap();
    runtime.block_on(async {
        let fs = Box::pin(Fs::local(LocalOptions::new(root.path())))
            .await
            .unwrap();
        let workspace = Box::pin(fs.create_workspace("bench", acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new())).await.map(|receipt| receipt.value).map_err(|failure| failure.error).unwrap();
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
            let mut transaction = Box::pin(async { workspace.begin_transaction(IdempotencyKey::new(), acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error) })
                .await
                .unwrap();
            for item in 0..16 {
                let path = format!("/batch-{next}-{item}");
                Box::pin(transaction.write(&path, Bytes::from_static(b"value")))
                    .await
                    .unwrap();
            }
            Box::pin(async { transaction.commit(None, acyclic_fs::PublicationPermit::Unrestricted, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new(), |_| async { Ok::<_, acyclic_fs::OperationFailure<acyclic_fs::WorkspaceError>>(acyclic_fs::OperationReceipt { value: (), work: acyclic_fs::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error) }).await.unwrap();
        });
    });
}

/// One small file read from the head generation.
#[divan::bench]
fn read_small(bencher: Bencher) {
    let runtime = Runtime::new().unwrap();
    let (_root, _fs, workspace) = workspace(&runtime);
    runtime
        .block_on(Box::pin(
            workspace.write("/entry", Bytes::from_static(b"value")),
        ))
        .unwrap();
    bencher.bench_local(|| {
        runtime
            .block_on(Box::pin(async { workspace.read("/entry", 64, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error) }))
            .unwrap()
    });
}
