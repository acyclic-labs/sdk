//! Public create-only batch atomicity, conflict, and staging-cost contracts.

#![cfg(not(target_arch = "wasm32"))]

use acyclic_fs::kernel::FileMetadata;
use acyclic_fs::{
    CancellationToken, FsError, IdempotencyKey, Transaction, TransactionCommit, WorkBudget,
    Workspace, WorkspaceError,
};

type MemoryWorkspace =
    Workspace<acyclic_fs::MemoryAuthorityBackend, acyclic_fs::MemoryObjectBackend>;
type MemoryTransaction =
    Transaction<acyclic_fs::MemoryAuthorityBackend, acyclic_fs::MemoryObjectBackend>;

macro_rules! value {
    ($operation:expr) => {
        $operation.await.unwrap().value
    };
}

async fn begin(workspace: &MemoryWorkspace) -> MemoryTransaction {
    value!(workspace.begin_transaction(
        IdempotencyKey::new(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new()
    ))
}

async fn commit(
    transaction: &mut MemoryTransaction,
) -> Result<
    TransactionCommit<acyclic_fs::MemoryAuthorityBackend, acyclic_fs::MemoryObjectBackend>,
    WorkspaceError,
> {
    transaction
        .commit::<WorkspaceError, _, _>(
            None,
            acyclic_fs::PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
            |_| {
                std::future::ready(Ok(acyclic_fs::OperationReceipt {
                    value: (),
                    work: Default::default(),
                }))
            },
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
}

async fn publish(transaction: &mut MemoryTransaction) {
    assert!(matches!(
        commit(transaction).await,
        Ok(TransactionCommit::Committed(_))
    ));
}

async fn workspace(folded: bool) -> MemoryWorkspace {
    let mut config = acyclic_fs::VolumeConfig::portable(acyclic_fs::Lifecycle::Ephemeral);
    if folded {
        config.case_sensitivity = acyclic_fs::model::CaseSensitivity::ProfileFolded;
    }
    value!(acyclic_fs::Fs::memory().create_workspace_with_config(
        "immutable-batch",
        config,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new()
    ))
}

fn content() -> acyclic_stream::OwnedBytes {
    acyclic_stream::OwnedBytes::from_static(b"original")
}

#[tokio::test]
async fn immutable_batch_rejects_existing_and_duplicate_bindings_atomically() {
    for folded in [false, true] {
        for pending in [false, true] {
            let workspace = workspace(folded).await;
            let mut setup = begin(&workspace).await;
            value!(setup.create_batch(
                &["/out/directory"],
                &[("/out/sealed", content())],
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            ));
            value!(setup.create_symbolic_link(
                "/out/link",
                content(),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            ));
            publish(&mut setup).await;
            for target in ["/out/sealed", "/out/directory", "/out/link", "/out/new"] {
                let mut transaction = begin(&workspace).await;
                if pending {
                    transaction.write_text("/pending", target).await.unwrap();
                }
                let second = if folded && target == "/out/new" {
                    "/out/NEW"
                } else {
                    target
                };
                let files = [("/out/new", content()), (second, content())];
                let failure = transaction
                    .create_batch(
                        &["/uncommitted"],
                        &files,
                        WorkBudget::UNBOUNDED,
                        &CancellationToken::new(),
                    )
                    .await
                    .unwrap_err();
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
                if pending {
                    publish(&mut transaction).await;
                } else {
                    assert!(matches!(
                        commit(&mut transaction).await,
                        Err(WorkspaceError::Filesystem(FsError::NoPendingMutations))
                    ));
                }
                for absent in ["/out/new", "/uncommitted"] {
                    assert!(
                        workspace
                            .read(absent, 64, WorkBudget::UNBOUNDED, &CancellationToken::new())
                            .await
                            .is_err()
                    );
                }
                assert_eq!(
                    value!(workspace.read(
                        "/out/sealed",
                        64,
                        WorkBudget::UNBOUNDED,
                        &CancellationToken::new()
                    ))
                    .as_ref(),
                    b"original"
                );
                if pending {
                    assert_eq!(
                        value!(workspace.read(
                            "/pending",
                            64,
                            WorkBudget::UNBOUNDED,
                            &CancellationToken::new()
                        ))
                        .as_ref(),
                        target.as_bytes()
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn immutable_batch_conflicts_with_a_concurrently_created_binding() {
    let workspace = workspace(false).await;
    let mut transaction = begin(&workspace).await;
    value!(transaction.create_batch(
        &[],
        &[("/sealed", content())],
        WorkBudget::UNBOUNDED,
        &CancellationToken::new()
    ));
    workspace.write_text("/sealed", "upstream").await.unwrap();
    assert!(matches!(
        commit(&mut transaction).await.unwrap(),
        TransactionCommit::Conflict { .. }
    ));
    assert!(matches!(
        value!(transaction.rebase(1, WorkBudget::UNBOUNDED, &CancellationToken::new())),
        acyclic_fs::TransactionRebase::Conflicted { .. }
    ));
    assert_eq!(
        value!(workspace.read(
            "/sealed",
            64,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new()
        ))
        .as_ref(),
        b"upstream"
    );
}

#[tokio::test]
async fn immutable_batch_stages_once_instead_of_rewriting_each_prefix() {
    let workspace = workspace(false).await;
    let mut batched = begin(&workspace).await;
    let mut serial = begin(&workspace).await;
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
        .await
        .unwrap()
        .work;
    let mut serial_writes = 0;
    for directory in directories {
        serial_writes += serial
            .create_dir_all(directory, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .unwrap()
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
            .await
            .unwrap()
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
}
