use super::*;
use crate::workspace::ForkOptions;
use crate::{Generation, IdempotencyKey, TransactionCommit, WorkspaceDelete};
use std::error::Error;

async fn retained_source_fork_work<
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
>(
    fs: &Fs<A, O>,
    source: &Generation<A, O>,
    name: &str,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancel: &CancellationToken,
) -> Result<crate::WorkCounters, Box<dyn Error>> {
    let name = crate::WorkspaceName::new(name, &WorkBudget::UNBOUNDED, cancel)?.value;
    let destination = fs.workspace_id_for_name(&name, accounting, cancel)?;
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
        let discarded = std::mem::take(&mut *fs.inner.publication_proof.inner.lock().unwrap());
        drop(discarded);
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
        assert_eq!(held, native_work);
        assert_eq!(walked, late_work);
        assert!(walked.object_bytes_read > held.object_bytes_read);
        if let Some((few, few_walked)) = previous {
            assert_eq!(
                few, held,
                "retained proof work grew with unrelated source files"
            );
            assert!(walked.object_bytes_read > few_walked);
        }
        previous = Some((held, walked.object_bytes_read));
        eprintln!("registered{files}={held:?} forgotten{files}={walked:?}");
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
