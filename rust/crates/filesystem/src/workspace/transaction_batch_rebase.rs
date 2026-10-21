use super::*;

type MemoryWorkspace = Workspace<crate::MemoryAuthorityBackend, crate::MemoryObjectBackend>;
type MemoryTransaction = Transaction<crate::MemoryAuthorityBackend, crate::MemoryObjectBackend>;

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
) -> TransactionCommit<crate::MemoryAuthorityBackend, crate::MemoryObjectBackend> {
    value!(transaction.commit::<WorkspaceError, _, _>(
        None,
        crate::PublicationPermit::Unrestricted,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
        |_| std::future::ready(Ok(crate::OperationReceipt {
            value: (),
            work: Default::default(),
        }))
    ))
}

async fn publish(transaction: &mut MemoryTransaction) {
    assert!(matches!(
        commit(transaction).await,
        TransactionCommit::Committed(_)
    ));
}

async fn workspace() -> MemoryWorkspace {
    let filesystem = crate::Fs::memory();
    let workspace = value!(filesystem.create_workspace(
        "batch-rebase",
        WorkBudget::UNBOUNDED,
        &CancellationToken::new()
    ));
    let mut setup = begin(&workspace).await;
    value!(setup.create_dir_all("/out", WorkBudget::UNBOUNDED, &CancellationToken::new()));
    value!(setup.create_dir_all("/empty", WorkBudget::UNBOUNDED, &CancellationToken::new()));
    setup.write_text("/out/target", "original").await.unwrap();
    publish(&mut setup).await;
    workspace
}

async fn stage(transaction: &mut MemoryTransaction, parent: &str, path: &str) {
    value!(transaction.write_batch(
        &[parent],
        &[(path, acyclic_stream::OwnedBytes::from_static(b"local"))],
        WorkBudget::UNBOUNDED,
        &CancellationToken::new()
    ));
}

async fn rebase(
    transaction: &mut MemoryTransaction,
) -> crate::TransactionRebase<crate::MemoryAuthorityBackend, crate::MemoryObjectBackend> {
    assert!(matches!(
        commit(transaction).await,
        TransactionCommit::Conflict { .. }
    ));
    value!(transaction.rebase(1, WorkBudget::UNBOUNDED, &CancellationToken::new()))
}

#[tokio::test]
async fn sibling_writes_preserve_safe_batch_rebase() {
    let workspace = workspace().await;
    let mut local = begin(&workspace).await;
    stage(&mut local, "/out", "/out/local").await;
    workspace
        .write_text("/out/sibling", "upstream")
        .await
        .unwrap();
    assert!(matches!(
        rebase(&mut local).await,
        crate::TransactionRebase::Rebased(_)
    ));
    publish(&mut local).await;
    for (path, expected) in [
        ("/out/local", "local"),
        ("/out/sibling", "upstream"),
        ("/out/target", "original"),
    ] {
        assert_eq!(
            value!(workspace.read(path, 32, WorkBudget::UNBOUNDED, &CancellationToken::new()))
                .as_ref(),
            expected.as_bytes()
        );
    }
}

#[tokio::test]
async fn changed_target_content_still_conflicts() {
    let workspace = workspace().await;
    let mut local = begin(&workspace).await;
    stage(&mut local, "/out", "/out/target").await;
    workspace
        .write_text("/out/target", "upstream")
        .await
        .unwrap();
    assert!(matches!(
        rebase(&mut local).await,
        crate::TransactionRebase::Conflicted { .. }
    ));
}

#[tokio::test]
async fn replaced_directory_binding_still_conflicts() {
    let workspace = workspace().await;
    let mut local = begin(&workspace).await;
    stage(&mut local, "/empty", "/empty/local").await;
    let mut upstream = begin(&workspace).await;
    value!(upstream.remove("/empty", WorkBudget::UNBOUNDED, &CancellationToken::new()));
    value!(upstream.create_dir_all("/empty", WorkBudget::UNBOUNDED, &CancellationToken::new()));
    publish(&mut upstream).await;
    assert!(matches!(
        rebase(&mut local).await,
        crate::TransactionRebase::Conflicted { .. }
    ));
}

#[tokio::test]
async fn public_batch_reads_still_observe_terminal_directory_records() {
    for directory in ["/", "/out"] {
        let workspace = workspace().await;
        let mut local = begin(&workspace).await;
        let config = local.checkout.volume_config();
        let path = NamespacePath::from_portable_in_profile(
            &PortablePath::parse(directory, config.limits).unwrap(),
            config.profile,
            config.limits,
        )
        .unwrap();
        value!(local.checkout.lookup_batch_no_follow(
            &[path],
            WorkBudget::UNBOUNDED,
            &CancellationToken::new()
        ));
        stage(&mut local, "/out", "/out/local").await;
        workspace
            .write_text(
                if directory == "/" {
                    "/sibling"
                } else {
                    "/out/sibling"
                },
                "upstream",
            )
            .await
            .unwrap();
        assert!(matches!(
            rebase(&mut local).await,
            crate::TransactionRebase::Conflicted { .. }
        ));
    }
}
