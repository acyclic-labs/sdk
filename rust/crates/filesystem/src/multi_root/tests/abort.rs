use super::*;

#[tokio::test]
async fn conflicted_publication_abort_restores_premerge_generation()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let parent = fs
        .create_workspace(
            "abort-parent",
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
            "abort-child",
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
    child_tx
        .write_text("/file.txt", "discarded child\n")
        .await?;
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
    parent_tx.write_text("/file.txt", "kept parent\n").await?;
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
    let root_id = WorkspaceRootId::from_bytes([72; 16]);
    let abort_operation = OperationId::from_bytes([75; 16]);
    let abort_candidate = MultiRootMergeCandidate {
        plan: MultiRootMergePlan {
            operation_id: abort_operation,
            parent_context_id: WorkspaceContextId::from_bytes([76; 16]),
            child_context_id: WorkspaceContextId::from_bytes([77; 16]),
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
    let aborting = MultiRootPublicationCoordinator::new(
        MemoryMultiRootPublicationStore::default(),
        WorkspaceMultiRootPublisher::new(resolver).with_merge_drivers(std::sync::Arc::new(drivers)),
        Allow,
    );
    assert!(matches!(
        aborting.publish(abort_candidate).await?,
        Publication::Conflicted(_)
    ));
    let mut unrelated = parent
        .begin_transaction(
            IdempotencyKey::new(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value;
    unrelated
        .write_text("/unrelated.txt", "preserved\n")
        .await?;
    let TransactionCommit::Committed(_) = unrelated
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
        return Err("unrelated edit did not commit".into());
    };
    aborting
        .abort_conflicted(abort_operation, WorkspaceContextId::from_bytes([76; 16]))
        .await?;
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
        b"kept parent\n"
    );
    assert_eq!(
        parent
            .read(
                "/unrelated.txt",
                32,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await?
            .value
            .as_ref(),
        b"preserved\n"
    );
    assert!(aborting.pending_operations().await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn aborting_a_conflict_that_changed_no_target_path_succeeds()
-> Result<(), Box<dyn std::error::Error>> {
    // Both sides add the same path with different bytes and no merge
    // driver: the projection holds the record back, so it differs from the
    // pre-merge target in no path. Abort must still clear the operation.
    let fs = Fs::memory();
    let parent = fs
        .create_workspace(
            "empty-abort-parent",
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
    base_tx.write_text("/keep.txt", "base\n").await?;
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
            "empty-abort-child",
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
    child_tx.write_text("/added.txt", "from child\n").await?;
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
    parent_tx.write_text("/added.txt", "from parent\n").await?;
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
    let root_id = WorkspaceRootId::from_bytes([82; 16]);
    let operation = OperationId::from_bytes([85; 16]);
    let parent_context = WorkspaceContextId::from_bytes([86; 16]);
    let candidate = MultiRootMergeCandidate {
        plan: MultiRootMergePlan {
            operation_id: operation,
            parent_context_id: parent_context,
            child_context_id: WorkspaceContextId::from_bytes([87; 16]),
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
        WorkspaceMultiRootPublisher::new(resolver),
        Allow,
    );
    assert!(matches!(
        coordinator.publish(candidate).await?,
        Publication::Conflicted(_)
    ));
    coordinator
        .abort_conflicted(operation, parent_context)
        .await?;
    assert_eq!(
        parent
            .read(
                "/added.txt",
                32,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await?
            .value
            .as_ref(),
        b"from parent\n"
    );
    assert_eq!(
        parent
            .read(
                "/keep.txt",
                32,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await?
            .value
            .as_ref(),
        b"base\n"
    );
    assert!(coordinator.pending_operations().await?.is_empty());
    Ok(())
}
