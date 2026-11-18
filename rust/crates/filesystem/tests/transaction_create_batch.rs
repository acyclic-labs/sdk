//! Public create-only batch atomicity, conflict, and staging-cost contracts.

#![cfg(not(target_arch = "wasm32"))]

use crate::fixtures::commit;
use acyclic_fs_format::metadata::FileMetadata;
use std::error::Error;
use {
    acyclic_fs::{
        CancellationToken, FsError, IdempotencyKey, Transaction, TransactionCommit, Workspace,
        WorkspaceError,
    },
    acyclic_fs_work::performance::WorkBudget,
};

type MemoryWorkspace =
    Workspace<acyclic_fs::MemoryAuthorityBackend, acyclic_fs::MemoryObjectBackend>;
type MemoryTransaction =
    Transaction<acyclic_fs::MemoryAuthorityBackend, acyclic_fs::MemoryObjectBackend>;

async fn begin(workspace: &MemoryWorkspace) -> Result<MemoryTransaction, WorkspaceError> {
    unbounded!(workspace, begin_transaction(IdempotencyKey::new()))
}

async fn publish(transaction: &mut MemoryTransaction) -> Result<(), WorkspaceError> {
    assert!(matches!(
        commit(transaction).await?,
        TransactionCommit::Committed(_)
    ));
    Ok(())
}

async fn workspace(folded: bool) -> Result<MemoryWorkspace, WorkspaceError> {
    let mut config = acyclic_fs_format::model::VolumeConfig::portable(
        acyclic_fs_format::model::Lifecycle::Ephemeral,
    );
    if folded {
        config.case_sensitivity = acyclic_fs_format::model::CaseSensitivity::ProfileFolded;
    }
    unbounded!(
        acyclic_fs::Fs::memory(),
        create_workspace_with_config("immutable-batch", config)
    )
}

fn content() -> acyclic_stream::OwnedBytes {
    acyclic_stream::OwnedBytes::from_static(b"original")
}

#[tokio::test]
async fn immutable_batch_rejects_existing_and_duplicate_bindings_atomically()
-> Result<(), Box<dyn Error>> {
    for folded in [false, true] {
        for pending in [false, true] {
            let workspace = workspace(folded).await?;
            let mut setup = begin(&workspace).await?;
            unbounded!(
                setup,
                create_batch(&["/out/directory"], &[("/out/sealed", content())])
            )?;
            unbounded!(setup, create_symbolic_link("/out/link", content()))?;
            publish(&mut setup).await?;
            for target in ["/out/sealed", "/out/directory", "/out/link", "/out/new"] {
                let mut transaction = begin(&workspace).await?;
                if pending {
                    transaction.write_text("/pending", target).await?;
                }
                let second = if folded && target == "/out/new" {
                    "/out/NEW"
                } else {
                    target
                };
                let files = [("/out/new", content()), (second, content())];
                let Err(failure) = transaction
                    .create_batch(
                        &["/uncommitted"],
                        &files,
                        WorkBudget::UNBOUNDED,
                        &CancellationToken::new(),
                    )
                    .await
                else {
                    return Err("invalid create batch was admitted".into());
                };
                let exact_refusal = if folded && target == "/out/new" {
                    matches!(
                        failure.error,
                        WorkspaceError::Filesystem(FsError::NameCollision)
                    )
                } else {
                    matches!(
                        failure.error,
                        WorkspaceError::Filesystem(FsError::Mutation(
                            acyclic_fs::kernel::GenerationMutationError::AlreadyExists
                        ))
                    )
                };
                assert!(
                    exact_refusal,
                    "folded={folded}, pending={pending}, target={target}: {:?}",
                    failure.error
                );
                publish(&mut transaction).await?;
                for absent in ["/out/new", "/uncommitted"] {
                    assert!(
                        workspace
                            .read(absent, 64, WorkBudget::UNBOUNDED, &CancellationToken::new())
                            .await
                            .is_err()
                    );
                }
                assert_eq!(
                    unbounded!(workspace, read("/out/sealed", 64))?.as_ref(),
                    b"original"
                );
                if pending {
                    assert_eq!(
                        unbounded!(workspace, read("/pending", 64))?.as_ref(),
                        target.as_bytes()
                    );
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn immutable_batch_conflicts_with_a_concurrently_created_binding()
-> Result<(), Box<dyn Error>> {
    let workspace = workspace(false).await?;
    let mut transaction = begin(&workspace).await?;
    unbounded!(transaction, create_batch(&[], &[("/sealed", content())]))?;
    workspace.write_text("/sealed", "upstream").await?;
    assert!(matches!(
        commit(&mut transaction).await?,
        TransactionCommit::Conflict { .. }
    ));
    assert!(matches!(
        unbounded!(transaction, rebase(1))?,
        acyclic_fs::TransactionRebase::Conflicted { .. }
    ));
    assert_eq!(
        unbounded!(workspace, read("/sealed", 64))?.as_ref(),
        b"upstream"
    );
    Ok(())
}

#[tokio::test]
async fn immutable_batch_stages_once_instead_of_rewriting_each_prefix() -> Result<(), Box<dyn Error>>
{
    let workspace = workspace(false).await?;
    let mut batched = begin(&workspace).await?;
    let mut serial = begin(&workspace).await?;
    let directories = ["/records/nested", "/records/nested"];
    let files = [
        ("/records/nested/request", content()),
        ("/records/nested/decision", content()),
    ];
    let batched = batched
        .create_batch(
            &directories,
            &files,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .work;
    let mut serial_writes = 0;
    for directory in directories {
        serial_writes += serial
            .create_dir_all(directory, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await?
            .work
            .backend_write_operations;
    }
    for (path, bytes) in files {
        serial_writes += serial
            .create_file(
                path,
                bytes,
                FileMetadata::default(),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await?
            .work
            .backend_write_operations;
    }
    assert!(
        batched.backend_write_operations < serial_writes,
        "immutable staging did not batch backend writes: {} >= {serial_writes}",
        batched.backend_write_operations
    );
    eprintln!(
        "IMMUTABLE_BATCH_WRITES {} {serial_writes}",
        batched.backend_write_operations
    );
    Ok(())
}
