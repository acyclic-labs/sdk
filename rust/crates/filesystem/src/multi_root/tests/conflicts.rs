use super::*;

#[tokio::test]
async fn conflicted_publication_projects_edits_and_continues_durably()
-> Result<(), Box<dyn std::error::Error>> {
    Box::pin(conflicted_publication_projects_edits_and_continues_durably_case()).await
}

async fn conflicted_publication_projects_edits_and_continues_durably_case()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let parent = fs
        .create_workspace(
            "conflict-parent",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value;
    let mut base_tx = parent
        .begin_transaction(
            IdempotencyKey::new(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value;
    base_tx.write_text("/file.txt", "base\n").await?;
    let TransactionCommit::Committed(base) = base_tx
        .commit(
            None,
            crate::PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
            |_| async {
                Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt {
                    value: (),
                    work: crate::WorkCounters::default(),
                })
            },
        )
        .await?
        .value
    else {
        return Err("base did not commit".into());
    };
    let child = parent
        .fork::<_, _, crate::WorkspaceError>(
            "conflict-child",
            ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
            crate::PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
            &mut |_| {
                std::future::ready(Ok(crate::OperationReceipt {
                    value: (),
                    work: crate::WorkCounters::UNCHARGED,
                }))
            },
        )
        .await?
        .value;
    let mut child_tx = child
        .begin_transaction(
            IdempotencyKey::new(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value;
    child_tx.write_text("/file.txt", "child\n").await?;
    let TransactionCommit::Committed(child_head) = child_tx
        .commit(
            None,
            crate::PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
            |_| async {
                Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt {
                    value: (),
                    work: crate::WorkCounters::default(),
                })
            },
        )
        .await?
        .value
    else {
        return Err("child did not commit".into());
    };
    let mut parent_tx = parent
        .begin_transaction(
            IdempotencyKey::new(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value;
    parent_tx.write_text("/file.txt", "parent\n").await?;
    let TransactionCommit::Committed(parent_head) = parent_tx
        .commit(
            None,
            crate::PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
            |_| async {
                Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt {
                    value: (),
                    work: crate::WorkCounters::default(),
                })
            },
        )
        .await?
        .value
    else {
        return Err("parent did not commit".into());
    };
    let resolver = Resolver {
        workspaces: Mutex::new(BTreeMap::from([
            (parent.id(), parent.clone()),
            (child.id(), child.clone()),
        ])),
    };
    let mut drivers = MergeDriverRegistry::new();
    drivers.register("text", std::sync::Arc::new(DefaultTextMergeDriver))?;
    drivers.set_default("text")?;
    let operation_id = OperationId::from_bytes([71; 16]);
    let root_id = WorkspaceRootId::from_bytes([72; 16]);
    let candidate = MultiRootMergeCandidate {
        plan: MultiRootMergePlan {
            operation_id,
            parent_context_id: WorkspaceContextId::from_bytes([73; 16]),
            child_context_id: WorkspaceContextId::from_bytes([74; 16]),
            roots: BTreeMap::from([(
                root_id,
                MultiRootMergeRoot {
                    source_workspace_id: child.id(),
                    merge_workspace_id: None,
                    source_generation: child_head.id(),
                    target_workspace_id: parent.id(),
                    target_generation: parent_head.id(),
                    base_generation: base.id(),
                    child_wins_bindings: BTreeSet::new(),
                },
            )]),
        },
        resolutions: BTreeMap::from([(root_id, BTreeMap::new())]),
    };
    let coordinator = MultiRootPublicationCoordinator::new(
        MemoryMultiRootPublicationStore::default(),
        WorkspaceMultiRootPublisher::new(resolver).with_merge_drivers(std::sync::Arc::new(drivers)),
        Allow,
    );
    let Publication::Conflicted(conflicted) = coordinator.publish(candidate).await? else {
        return Err("publication did not enter conflict state".into());
    };
    assert_eq!(conflicted.projected_roots.len(), 1);
    assert!(
        String::from_utf8(
            parent
                .read(
                    "/file.txt",
                    1_024,
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new()
                )
                .await?
                .value
                .to_vec()
        )?
        .contains("<<<<<<< ours")
    );
    assert!(matches!(
        coordinator
            .continue_conflicted(operation_id, WorkspaceContextId::from_bytes([73; 16]))
            .await,
        Err(MultiRootPublicationError::InvalidCandidate)
    ));
    assert!(matches!(
        coordinator
            .declare_conflicted_paths(
                operation_id,
                WorkspaceContextId::from_bytes([73; 16]),
                root_id,
                &["file.txt".to_owned()],
            )
            .await,
        Err(MultiRootPublicationError::Publisher(
            WorkspaceMultiRootPublisherError::Candidate(_)
        ))
    ));
    let mut resolution = parent
        .begin_transaction(
            IdempotencyKey::new(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value;
    resolution.write_text("/file.txt", "resolved\n").await?;
    resolution
        .commit(
            None,
            crate::PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
            |_| async {
                Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt {
                    value: (),
                    work: crate::WorkCounters::default(),
                })
            },
        )
        .await?
        .value;
    coordinator
        .declare_conflicted_paths(
            operation_id,
            WorkspaceContextId::from_bytes([73; 16]),
            root_id,
            &["file.txt".to_owned()],
        )
        .await
        .expect("resolved conflict path declaration");
    let declared = coordinator
        .store
        .load(operation_id)
        .await?
        .expect("conflict journal");
    assert_eq!(
        declared.conflicts[&root_id]
            .conflicts
            .iter()
            .map(|view| view.key.clone())
            .collect::<BTreeSet<_>>(),
        declared.declared_conflicts[&root_id]
    );
    assert!(matches!(
        coordinator
            .continue_conflicted(operation_id, WorkspaceContextId::from_bytes([73; 16]))
            .await?,
        Publication::Applied(_)
    ));
    assert_eq!(
        parent
            .read(
                "/file.txt",
                32,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await?
            .value
            .as_ref(),
        b"resolved\n"
    );
    assert!(coordinator.pending_operations().await?.is_empty());
    Ok(())
}
