//! A fork's proof extends its source's published proof instead of walking it.

use super::*;
use crate::WorkCounters;
use crate::kernel::allocation::KernelAccounting;

/// The work to prove a fork of `files` settled files plus one more commit,
/// forked from the head, whose proof is published, or from the generation
/// before it, which the slot no longer holds, so its proof walks the closure.
async fn fork_proof_work(
    files: u8,
    from_head: bool,
) -> Result<WorkCounters, Box<dyn std::error::Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let workspace = fs.create_workspace("fork-proof", B, &cancel).await?.value;
    for file in 0..files {
        workspace
            .write_text(&format!("/{file}"), &"x".repeat(5_000 + usize::from(file)))
            .await?;
    }
    let older = workspace.head(B, &cancel).await?.value;
    workspace.write_text("/tip", "tip").await?;
    let source = if from_head {
        workspace.head(B, &cancel).await?.value
    } else {
        older
    };
    let name = crate::WorkspaceName::new("fork-proof-child", &B, &cancel)?.value;
    let mut accounting = KernelAccounting::standalone(B, &cancel);
    let destination = fs.workspace_id_for_name(&name, &mut accounting, &cancel)?;
    let (allocation, admitted) = accounting.record_actual_owned(0)?;
    admitted?;
    let start = accounting.spent();
    let (proof, _, pending, _) = fs
        .materialize_forked_generation_root(
            &source,
            destination,
            crate::kernel::Owned::new(None, allocation, 0),
            &mut accounting,
            &cancel,
        )
        .await?;
    assert!(
        pending.is_some(),
        "the fork's proof must yield a ticket for its own volume"
    );
    drop((proof, pending));
    let end = accounting.spent();
    Ok(WorkCounters {
        object_probes: end.object_probes - start.object_probes,
        backend_read_operations: end.backend_read_operations - start.backend_read_operations,
        object_bytes_read: end.object_bytes_read - start.object_bytes_read,
        items_examined: end.items_examined - start.items_examined,
        ..WorkCounters::UNCHARGED
    })
}

#[tokio::test]
async fn a_fork_extends_its_published_source_proof() -> Result<(), Box<dyn std::error::Error>> {
    let (few, many) = (
        fork_proof_work(4, true).await?,
        fork_proof_work(32, true).await?,
    );
    // Negative control: a source whose proof is not cached walks its closure.
    let (few_walked, many_walked) = (
        fork_proof_work(4, false).await?,
        fork_proof_work(32, false).await?,
    );
    assert_eq!(few, many, "the fork's proof grew with its source's closure");
    assert!(
        many_walked.object_bytes_read > few_walked.object_bytes_read,
        "the walk must grow with the closure: {few_walked:?} vs {many_walked:?}"
    );
    Ok(())
}

async fn first_publication_work(
    files: u8,
    discard_proof: bool,
) -> Result<WorkCounters, Box<dyn std::error::Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace("first-publish-source", B, &cancel)
        .await?
        .value;
    for file in 0..files {
        workspace
            .write_text(&format!("/{file}"), &"x".repeat(5_000 + usize::from(file)))
            .await?;
    }
    let source = workspace.head(B, &cancel).await?.value;
    let child = workspace
        .fork::<_, _, crate::WorkspaceError>(
            "first-publish-child",
            crate::workspace::ForkOptions::from_generation(source, crate::IdempotencyKey::new()),
            crate::PublicationPermit::Unrestricted,
            B,
            &cancel,
            &mut |_| {
                std::future::ready(Ok(crate::OperationReceipt {
                    value: (),
                    work: WorkCounters::UNCHARGED,
                }))
            },
        )
        .await?
        .value;
    if discard_proof {
        // Negative control: the same public fork without its installed proof
        // has to prove its inherited closure at the first publication.
        let discarded = std::mem::take(&mut *fs.inner.publication_proof.inner.lock().unwrap());
        drop(discarded);
    }
    let mut transaction = child
        .begin_transaction(crate::IdempotencyKey::new(), B, &cancel)
        .await?
        .value;
    transaction.write_text("/new", "child only").await?;
    let receipt = transaction
        .commit(
            None,
            crate::PublicationPermit::Unrestricted,
            B,
            &cancel,
            |_| async {
                Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt {
                    value: (),
                    work: WorkCounters::UNCHARGED,
                })
            },
        )
        .await?;
    assert!(matches!(
        receipt.value,
        crate::TransactionCommit::Committed(_)
    ));
    assert_eq!(
        child.read("/new", 32, B, &cancel).await?.value.as_ref(),
        b"child only"
    );
    assert!(workspace.read("/new", 32, B, &cancel).await.is_err());
    Ok(receipt.work)
}

#[tokio::test]
async fn first_publication_reuses_the_public_forks_installed_proof()
-> Result<(), Box<dyn std::error::Error>> {
    let few = first_publication_work(4, false).await?;
    let many = first_publication_work(32, false).await?;
    let few_walked = first_publication_work(4, true).await?;
    let many_walked = first_publication_work(32, true).await?;
    eprintln!(
        "first publication cached4={few:?} cached32={many:?} uncached4={few_walked:?} uncached32={many_walked:?}"
    );
    // The public commit also reads changed authenticated index paths, whose
    // height can grow. Compare the SAME publication with its installed proof
    // removed: forgetting pending.created() makes these savings disappear.
    assert!(few.object_probes < few_walked.object_probes);
    assert!(few.object_bytes_read < few_walked.object_bytes_read);
    assert!(
        many_walked.object_probes - many.object_probes
            > few_walked.object_probes - few.object_probes
    );
    assert!(
        many_walked.object_bytes_read - many.object_bytes_read
            > few_walked.object_bytes_read - few.object_bytes_read
    );
    assert!(many_walked.object_probes > few_walked.object_probes);
    assert!(many_walked.object_bytes_read > few_walked.object_bytes_read);
    assert!(many_walked.object_probes > many.object_probes);
    Ok(())
}
