use super::*;

#[tokio::test]
async fn workspace_publisher_previews_then_publishes_the_pinned_child_generation()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let parent = fs
        .create_workspace(
            "publisher-parent",
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
    base_tx.write_text("/base.txt", "base").await?;
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
        return Err("base commit did not publish".into());
    };
    let child = parent
        .fork::<_, _, crate::WorkspaceError>(
            "publisher-child",
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
    child_tx.write_text("/child.txt", "child").await?;
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
        return Err("child commit did not publish".into());
    };
    let mut parent_tx = parent
        .begin_transaction(
            IdempotencyKey::new(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value;
    parent_tx.write_text("/parent.txt", "parent").await?;
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
        return Err("parent commit did not publish".into());
    };
    let resolver = Resolver {
        workspaces: Mutex::new(BTreeMap::new()),
    };
    resolver
        .workspaces
        .lock()
        .expect("resolver lock")
        .extend([(parent.id(), parent.clone()), (child.id(), child.clone())]);
    let publisher = WorkspaceMultiRootPublisher::new(resolver);
    let operation = OperationId::from_bytes([31; 16]);
    let root_id = WorkspaceRootId::from_bytes([32; 16]);
    let root = MultiRootMergeRoot {
        source_workspace_id: child.id(),
        merge_workspace_id: None,
        source_generation: child_head.id(),
        target_workspace_id: parent.id(),
        target_generation: parent_head.id(),
        base_generation: base.id(),
        child_wins_bindings: BTreeSet::new(),
    };
    let resolutions = BTreeMap::new();
    let fence = match publisher
        .prepare(operation, root_id, &root, &resolutions)
        .await?
    {
        MultiRootPrepare::Ready(fence) => fence,
        MultiRootPrepare::Stale | MultiRootPrepare::Conflicted(_) => {
            return Err("preview rejected a clean join".into());
        }
    };
    assert_eq!(
        parent
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await?
            .value
            .id(),
        parent_head.id()
    );
    let published = publisher
        .publish(operation, root_id, &root, &resolutions, &fence)
        .await?;
    let published_generation = parent
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await?
        .value
        .id();
    assert!(matches!(
        published,
        MultiRootPublishRoot::Published(generation)
            if generation == published_generation
    ));
    assert_eq!(
        parent
            .read(
                "/child.txt",
                16,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await?
            .value
            .as_ref(),
        b"child"
    );
    assert_eq!(
        parent
            .read(
                "/parent.txt",
                16,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await?
            .value
            .as_ref(),
        b"parent"
    );
    assert_eq!(
        publisher
            .publish(operation, root_id, &root, &resolutions, &fence)
            .await?,
        MultiRootPublishRoot::AlreadyPublished(published_generation)
    );
    Ok(())
}

#[tokio::test]
async fn automatic_text_markers_are_reported_not_published()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let parent = fs
        .create_workspace(
            "publisher-conflict-parent",
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
    base_tx.write_text("/conflicted.txt", "base\n").await?;
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
        return Err("base commit did not publish".into());
    };
    let child = parent
        .fork::<_, _, crate::WorkspaceError>(
            "publisher-conflict-child",
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
    child_tx.write_text("/conflicted.txt", "child\n").await?;
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
        return Err("child commit did not publish".into());
    };
    let mut parent_tx = parent
        .begin_transaction(
            IdempotencyKey::new(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value;
    parent_tx.write_text("/conflicted.txt", "parent\n").await?;
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
        return Err("parent commit did not publish".into());
    };
    let resolver = Resolver {
        workspaces: Mutex::new(BTreeMap::from([
            (parent.id(), parent.clone()),
            (child.id(), child.clone()),
        ])),
    };
    let mut registry = MergeDriverRegistry::new();
    registry.register("text", std::sync::Arc::new(crate::DefaultTextMergeDriver))?;
    registry.set_default("text")?;
    let publisher = WorkspaceMultiRootPublisher::new(resolver)
        .with_merge_drivers(std::sync::Arc::new(registry));
    let root = MultiRootMergeRoot {
        source_workspace_id: child.id(),
        merge_workspace_id: None,
        source_generation: child_head.id(),
        target_workspace_id: parent.id(),
        target_generation: parent_head.id(),
        base_generation: base.id(),
        child_wins_bindings: BTreeSet::new(),
    };
    assert!(matches!(
        publisher
            .prepare(
                OperationId::from_bytes([41; 16]),
                WorkspaceRootId::from_bytes([42; 16]),
                &root,
                &BTreeMap::new(),
            )
            .await,
        Ok(MultiRootPrepare::Conflicted(_))
    ));
    assert_eq!(
        parent
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await?
            .value
            .id(),
        parent_head.id()
    );
    assert_eq!(
        parent
            .read(
                "/conflicted.txt",
                32,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await?
            .value
            .as_ref(),
        b"parent\n"
    );
    Ok(())
}

#[tokio::test]
async fn child_wins_binding_policy_overrides_explicit_parent_resolution()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let parent = fs
        .create_workspace(
            "child-wins-parent",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value;
    parent.write_text("/base.txt", "base").await?;
    let base = parent
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await?
        .value;
    let child = parent
        .fork::<_, _, crate::WorkspaceError>(
            "child-wins-child",
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
    child.write_text("/cache.tmp", "child").await?;
    parent.write_text("/cache.tmp", "parent").await?;
    let child_head = child
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await?
        .value;
    let parent_head = parent
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await?
        .value;
    let resolver = Resolver {
        workspaces: Mutex::new(BTreeMap::from([
            (parent.id(), parent.clone()),
            (child.id(), child.clone()),
        ])),
    };
    let publisher = WorkspaceMultiRootPublisher::new(resolver);
    let operation = OperationId::from_bytes([81; 16]);
    let root_id = WorkspaceRootId::from_bytes([82; 16]);
    let mut root = MultiRootMergeRoot {
        source_workspace_id: child.id(),
        merge_workspace_id: None,
        source_generation: child_head.id(),
        target_workspace_id: parent.id(),
        target_generation: parent_head.id(),
        base_generation: base.id(),
        child_wins_bindings: BTreeSet::new(),
    };
    let MultiRootPrepare::Conflicted(plan) = publisher
        .prepare(operation, root_id, &root, &BTreeMap::new())
        .await
        .map_err(|error| format!("initial preview: {error}"))?
    else {
        return Err("same-name creations did not conflict".into());
    };
    let binding = plan
        .conflicts
        .iter()
        .find(|conflict| {
            conflict.kind == crate::ConflictKind::Binding
                && conflict.path.as_deref() == Some("/cache.tmp")
        })
        .ok_or("binding conflict missing")?;
    let resolutions = BTreeMap::from([(
        binding.key.clone(),
        MergeResolution::Select(ConflictSide::Ours),
    )]);
    root.child_wins_bindings.insert("/cache.tmp".to_owned());
    let MultiRootPrepare::Ready(fence) = publisher
        .prepare(operation, root_id, &root, &resolutions)
        .await
        .map_err(|error| format!("resolved preview: {error}"))?
    else {
        return Err("child-wins binding was not ready".into());
    };
    assert!(matches!(
        publisher
            .publish(operation, root_id, &root, &resolutions, &fence)
            .await
            .map_err(|error| format!("resolved publication: {error}"))?,
        MultiRootPublishRoot::Published(_)
    ));
    assert_eq!(
        parent
            .read(
                "/cache.tmp",
                16,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await?
            .value
            .as_ref(),
        b"child"
    );
    Ok(())
}

#[tokio::test]
async fn independent_same_name_directory_creations_merge_by_path()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let parent = fs
        .create_workspace(
            "directory-race-parent",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value;
    let base = parent
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await?
        .value;
    let child = parent
        .fork::<_, _, crate::WorkspaceError>(
            "directory-race-child",
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
        .create_dir_all("/sub", WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await?
        .value;
    child_tx.write_text("/.gitignore", "*.tmp\n").await?;
    child_tx.write_text("/scratch.tmp", "child").await?;
    child_tx.write_text("/sub/.gitignore", "*.tmp\n").await?;
    child_tx.write_text("/sub/cache.tmp", "child").await?;
    child_tx
        .create_dir_all(
            "/sub/nested",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value;
    child_tx
        .write_text("/sub/nested/deep.tmp", "nested child")
        .await?;
    child_tx
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
    let mut parent_tx = parent
        .begin_transaction(
            IdempotencyKey::new(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value;
    parent_tx
        .create_dir_all("/sub", WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await?
        .value;
    parent_tx.write_text("/scratch.tmp", "parent").await?;
    parent_tx.write_text("/sub/cache.tmp", "parent").await?;
    parent_tx
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
    let resolver = Resolver {
        workspaces: Mutex::new(BTreeMap::from([
            (parent.id(), parent.clone()),
            (child.id(), child.clone()),
        ])),
    };
    // Both sides created /sub on their own: directories merge by path, so
    // only the files inside can conflict, and the child-wins bindings
    // settle those.
    let root_id = WorkspaceRootId::from_bytes([84; 16]);
    let operation = OperationId::from_bytes([83; 16]);
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
                    source_generation: child
                        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
                        .await?
                        .value
                        .id(),
                    target_workspace_id: parent.id(),
                    target_generation: parent
                        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
                        .await?
                        .value
                        .id(),
                    base_generation: base.id(),
                    child_wins_bindings: BTreeSet::from([
                        "/scratch.tmp".to_owned(),
                        "/sub/cache.tmp".to_owned(),
                    ]),
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
        Publication::Applied(_)
    ));
    assert_eq!(
        parent
            .read(
                "/sub/cache.tmp",
                16,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await?
            .value
            .as_ref(),
        b"child"
    );
    assert_eq!(
        parent
            .read(
                "/scratch.tmp",
                16,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await?
            .value
            .as_ref(),
        b"child"
    );
    assert_eq!(
        parent
            .read(
                "/sub/nested/deep.tmp",
                32,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await?
            .value
            .as_ref(),
        b"nested child"
    );
    assert_eq!(
        parent
            .read(
                "/sub/.gitignore",
                16,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await?
            .value
            .as_ref(),
        b"*.tmp
"
    );
    Ok(())
}
