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
    let (proof, _, pending) = fs
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
