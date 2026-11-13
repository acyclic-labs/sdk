use super::*;
use crate::workspace::ForkOptions;
use crate::{Generation, IdempotencyKey, TransactionCommit, WorkspaceDelete};
use std::error::Error;

#[tokio::test]
async fn settled_ephemeral_forks_retire_their_proof_cache_without_retiring_the_source()
-> Result<(), Box<dyn Error>> {
    for settled in [false, true] {
        let directory = tempfile::tempdir()?;
        let cancel = CancellationToken::new();
        let fs = Fs::local(crate::LocalOptions::new(directory.path())).await?;
        let source = fs.create_workspace("proof-owner", B, &cancel).await?.value;
        source.write_text("/base", "base").await?;
        let head = source.head(B, &cancel).await?.value;
        let (fork, custody) = source
            .fork_ephemeral(
                "proof-fork",
                ForkOptions::from_generation(head, IdempotencyKey::new()),
                B,
                &cancel,
            )
            .await?
            .value;
        fork.write_text("/private", "private").await?;
        if settled {
            custody.settle().await?;
        } else {
            // Negative control: a live fork really does retain its proof slot.
            assert!(
                fs.inner
                    .publication_proof
                    .inner
                    .lock()
                    .unwrap()
                    .retire(fork.id().volume_id())
            );
            custody.settle().await?;
        }
        assert!(
            !fs.inner
                .publication_proof
                .inner
                .lock()
                .unwrap()
                .retire(fork.id().volume_id())
        );
        assert!(
            fs.inner
                .publication_proof
                .inner
                .lock()
                .unwrap()
                .retire(source.id().volume_id())
        );
        assert_eq!(
            source.read("/base", 64, B, &cancel).await?.value.as_ref(),
            b"base"
        );
    }
    Ok(())
}

async fn retained_source_fork_work(
    fs: &crate::LocalFs,
    source: &Generation<crate::LocalAuthorityBackend, crate::LocalObjectBackend>,
    name: &str,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancel: &CancellationToken,
) -> Result<crate::WorkCounters, Box<dyn Error>> {
    // Each probe starts with empty disposable proof, byte and decoded caches. The source
    // must carry its own original witness rather than inherit an earlier probe's slot.
    let discarded = std::mem::take(&mut *fs.inner.publication_proof.inner.lock().unwrap());
    drop(discarded);
    fs.clear_object_cache()?;
    let name = crate::WorkspaceName::new(name, &WorkBudget::UNBOUNDED, cancel)?.value;
    let destination = fs.workspace_id_for_name(&name, accounting)?;
    let (allocation, admitted) = accounting.record_actual_owned(0)?;
    admitted?;
    let start = accounting.spent();
    let (proof, source_object, pending, filtered) = fs
        .materialize_forked_generation_root(
            source,
            destination,
            crate::kernel::Owned::new(None, allocation, 0),
            accounting,
            cancel,
        )
        .await?;
    assert_eq!(source_object, source.id.object_id());
    assert!(!filtered);
    assert!(pending.is_some());
    drop((proof, pending));
    let end = accounting.spent();
    Ok(crate::WorkCounters {
        backend_read_operations: end.backend_read_operations - start.backend_read_operations,
        object_bytes_read: end.object_bytes_read - start.object_bytes_read,
        items_examined: end.items_examined - start.items_examined,
        ..crate::WorkCounters::UNCHARGED
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registered_source_proofs_survive_collection_without_rewalking_the_closure()
-> Result<(), Box<dyn Error>> {
    let mut previous = None;
    for files in [4, 32] {
        let directory = tempfile::tempdir()?;
        let cancel = CancellationToken::new();
        let fs = Fs::local(crate::LocalOptions::new(directory.path())).await?;
        let workspace = fs
            .create_workspace("registered-source", B, &cancel)
            .await?
            .value;
        let mut transaction = workspace
            .begin_transaction(IdempotencyKey::new(), B, &cancel)
            .await?
            .value;
        for file in 0..files {
            transaction
                .write_text(&format!("/{file}"), &"x".repeat(5_000))
                .await?;
        }
        assert!(matches!(
            transaction
                .commit::<WorkspaceError, _, _>(
                    None,
                    crate::PublicationPermit::Unrestricted,
                    B,
                    &cancel,
                    allow::<WorkspaceError>
                )
                .await?
                .value,
            TransactionCommit::Committed(_)
        ));
        drop(transaction);
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancel)
            .map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let source = workspace.head(B, &cancel).await?.value;
        // The checkout and its handback share the original standalone ledger.
        // A fresh handback ledger would promote a third allocation here.
        for selector in [
            GenerationSelector::Head,
            GenerationSelector::Exact(source.id()),
        ] {
            let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(
                WorkBudget::UNBOUNDED,
                &cancel,
            );
            let checkout = workspace
                .checkout_accounted(
                    selector,
                    CheckoutMode::read_only_pinned(),
                    &mut accounting,
                    &cancel,
                )
                .await?;
            let before = accounting.spent().allocation_operations;
            let closure = checkout.into_generation_closure(&mut accounting)?;
            assert_eq!(accounting.spent().allocation_operations - before, 2);
            let alias = closure.clone();
            assert_eq!(accounting.spent().allocation_operations - before, 2);
            drop((closure, alias));
        }
        let native = workspace
            .generation(source.id(), &original, &cancel)
            .await?;
        let mut forgotten = source.clone();
        // The same physical source stays held; removing its witness restores the full walk.
        forgotten.publication_closure = None;
        for _ in 0..2 {
            fs.collect_local_pass(None, true, &cancel).await?;
        }
        // A new observation after the epoch changed cannot inherit the stale summary.
        let late = workspace.head(B, &cancel).await?.value;
        let mut accounting =
            crate::kernel::allocation::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancel);
        let held =
            retained_source_fork_work(&fs, &source, "held", &mut accounting, &cancel).await?;
        let walked =
            retained_source_fork_work(&fs, &forgotten, "forgotten", &mut accounting, &cancel)
                .await?;
        let late_work =
            retained_source_fork_work(&fs, &late, "late", &mut accounting, &cancel).await?;
        let native_work = {
            let mut accounting =
                crate::kernel::allocation::KernelAccounting::native(&original, &cancel);
            retained_source_fork_work(&fs, &native, "native", &mut accounting, &cancel).await?
        };
        eprintln!(
            "registered{files}={held:?} native{files}={native_work:?} forgotten{files}={walked:?} late{files}={late_work:?}"
        );
        // Native work owns physical medium reads; ordinary work counts Objects GETs.
        // Their original item charges also differ. Compare
        // like ledgers across populations; both routes must read the same proof bytes.
        assert_eq!(held.object_bytes_read, native_work.object_bytes_read);

        assert_eq!(walked, late_work);
        assert!(walked.object_bytes_read > held.object_bytes_read);
        if let Some((few, few_native, few_walked)) = previous {
            assert_eq!(
                few, held,
                "retained proof work grew with unrelated source files"
            );
            assert_eq!(
                few_native, native_work,
                "native retained proof work grew with unrelated source files"
            );
            assert!(walked.object_bytes_read > few_walked);
        }
        previous = Some((held, native_work, walked.object_bytes_read));
        let child = workspace
            .fork::<_, _, WorkspaceError>(
                "registered-child",
                ForkOptions::from_generation(source.clone(), IdempotencyKey::new()),
                crate::PublicationPermit::Unrestricted,
                B,
                &cancel,
                &mut allow::<FsError>,
            )
            .await?
            .value;
        child.write_text("/child", "changed").await?;
        assert_eq!(
            workspace.delete(IdempotencyKey::new()).await?,
            WorkspaceDelete::Deleted
        );
        drop((source, native, forgotten, late, workspace));
        fs.collect_local_garbage(None, &cancel).await?;
        for file in [0, files - 1] {
            assert_eq!(
                child
                    .read(&format!("/{file}"), 5_000, B, &cancel)
                    .await?
                    .value
                    .as_ref(),
                "x".repeat(5_000).as_bytes()
            );
        }
        assert_eq!(
            child.read("/child", 7, B, &cancel).await?.value,
            OwnedBytes::from_static(b"changed")
        );
        drop(child);
        let released = fs
            .local_root_release_barrier()
            .ok_or("missing release barrier")?;
        drop(fs);
        released.released().await;
        original.receipt().verify(WorkBudget::UNBOUNDED)?;
        assert_eq!(original.live_allocation_bytes(), baseline);
    }
    Ok(())
}

const CRASH_CHILD: &str = "facade::tests::fork_proof::local::fork_then_crash_child";
const FORK_ONLY: &str = "written only into the fork";

// The child forks the source, writes into the fork, and dies without settling
// it: no destructor, flush or release runs.
#[test]
fn fork_then_crash_child() -> Result<(), Box<dyn Error>> {
    let Some(root) = std::env::var_os("ACYCLIC_EPHEMERAL_FORK_ROOT") else {
        return Ok(());
    };
    let durable = std::env::var_os("ACYCLIC_EPHEMERAL_FORK_DURABLE").is_some();
    let root = std::path::PathBuf::from(root);
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let cancel = CancellationToken::new();
            let fs = Fs::local(crate::LocalOptions::new(&root)).await?;
            let source = fs.create_workspace("source", B, &cancel).await?.value;
            source.write_text("/base", "base").await?;
            let log = std::fs::metadata(root.join("objects/log"))?.len();
            std::fs::write(root.join("baseline"), log.to_string())?;
            let head = source.head(B, &cancel).await?.value;
            let options = ForkOptions::from_generation(head, IdempotencyKey::new());
            let (fork, custody) = if durable {
                let fork = source
                    .fork::<_, _, WorkspaceError>(
                        "fork",
                        options,
                        crate::PublicationPermit::Unrestricted,
                        B,
                        &cancel,
                        &mut allow::<FsError>,
                    )
                    .await?
                    .value;
                (fork, None)
            } else {
                let (fork, custody) = source
                    .fork_ephemeral("fork", options, B, &cancel)
                    .await?
                    .value;
                (fork, Some(custody))
            };
            fork.write_text("/fork", FORK_ONLY).await?;
            // Distinct chunks cross the resident staging window and exercise
            // the OS-owned private spill, rather than only process memory.
            let spill = (0..12 * 1_024 * 1_024_u64)
                .map(|offset| (offset.wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 32) as u8)
                .collect::<Vec<_>>();
            fork.write("/spill", spill.into()).await?;
            assert_eq!(
                fork.read("/fork", 64, B, &cancel).await?.value.as_ref(),
                FORK_ONLY.as_bytes()
            );
            // The owner dies here, with the fork and its custody still live.
            let _live = (fs, source, fork, custody);
            std::process::exit(86)
        })
}

// An ephemeral fork dies with its owner: after a crash its authority is gone
// and none of its objects reached storage, while the source is intact. The
// negative control makes a durable fork the same way: it survives the crash,
// and its objects were stored.
#[test]
fn an_ephemeral_fork_leaves_nothing_after_its_owner_crashes() -> Result<(), Box<dyn Error>> {
    for durable in [false, true] {
        let directory = tempfile::tempdir()?;
        let mut child = std::process::Command::new(std::env::current_exe()?);
        child
            .args(["--exact", CRASH_CHILD, "--nocapture"])
            .env("ACYCLIC_EPHEMERAL_FORK_ROOT", directory.path());
        if durable {
            child.env("ACYCLIC_EPHEMERAL_FORK_DURABLE", "1");
        }
        let output = child.output()?;
        assert_eq!(
            output.status.code(),
            Some(86),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let baseline: u64 = std::fs::read_to_string(directory.path().join("baseline"))?.parse()?;
        assert!(
            std::fs::read_dir(directory.path())?.all(|entry| {
                !entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".acyclic-staging-")
            }),
            "a crashed owner left a named staging file"
        );
        let stored = std::fs::metadata(directory.path().join("objects/log"))?.len();
        if durable {
            assert!(stored > baseline, "the durable fork stored no objects");
        } else {
            assert_eq!(
                stored, baseline,
                "the ephemeral fork's objects reached storage"
            );
        }
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(async {
                let cancel = CancellationToken::new();
                let fs = Fs::local(crate::LocalOptions::new(directory.path())).await?;
                let source = fs.open_workspace("source", B, &cancel).await?.value;
                let head = source.head(B, &cancel).await?.value;
                assert_eq!(
                    head.read("/base", 8, B, &cancel).await?.value.as_ref(),
                    b"base"
                );
                let fork = fs.open_workspace("fork", B, &cancel).await;
                if durable {
                    let head = fork?.value.head(B, &cancel).await?.value;
                    assert_eq!(
                        head.read("/fork", 64, B, &cancel).await?.value.as_ref(),
                        FORK_ONLY.as_bytes()
                    );
                } else {
                    assert!(
                        matches!(
                            fork.map(|_| ()).map_err(|failure| failure.error),
                            Err(WorkspaceError::Filesystem(FsError::Authority(
                                AuthorityStoreError::Missing
                            )))
                        ),
                        "the ephemeral fork survived its owner"
                    );
                }
                Ok::<_, Box<dyn Error>>(())
            })?;
    }
    Ok(())
}
