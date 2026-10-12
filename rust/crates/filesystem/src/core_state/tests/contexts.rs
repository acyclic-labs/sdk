use super::*;

#[tokio::test]
async fn corrupt_context_identity_cannot_enter_listing_cas_or_root_removal() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = LocalCoreStateStore::new(directory.path());
    let namespace = store.namespace().expect("admitted namespace");
    let target_id = WorkspaceContextId::from_bytes([41; 16]);
    let stored_id = WorkspaceContextId::from_bytes([42; 16]);
    let removed_root = WorkspaceRootId::from_bytes([10; 16]);
    let mut target = workspace_context(
        directory.path(),
        target_id,
        1,
        WorkspaceContextState::Active,
    );
    let mut second = target.roots.values().next().expect("first root").clone();
    second.root_id = removed_root;
    second.source_path = directory.path().join("second");
    second.workspace_id = WorkspaceId::from_bytes([10; 16]);
    second.workspace_name = "second".to_owned();
    target.roots.insert(removed_root, second);
    let corrupt = workspace_context(
        directory.path(),
        WorkspaceContextId::from_bytes([99; 16]),
        1,
        WorkspaceContextState::Active,
    );
    let mut files = Vec::new();
    for (id, record) in [(target_id, target), (stored_id, corrupt)] {
        let paths = context_paths(&namespace, id);
        std::fs::create_dir_all(&paths.directory).expect("record directory");
        write_journaled(&paths, &record).expect("persist fixture");
        files.push((
            paths.current.clone(),
            std::fs::read(&paths.current).expect("original bytes"),
        ));
    }
    let listed = WorkspaceContextStore::list(&store).await;
    let swapped = WorkspaceContextStore::compare_and_swap_many(
        &store,
        BTreeMap::from([(target_id, 1), (stored_id, 1)]),
        Vec::new(),
        true,
    )
    .await;
    let registry = crate::WorkspaceContextRegistry::new(store);
    let removed = tokio::time::timeout(
        Duration::from_secs(2),
        registry.remove_root(target_id, removed_root),
    )
    .await;
    for (path, before) in files {
        assert_eq!(std::fs::read(path).expect("unchanged bytes"), before);
    }
    assert!(
        matches!(
            listed.as_ref(),
            Err(LocalCoreStateStoreError::ContextTransaction(_))
        ) && matches!(
            swapped.as_ref(),
            Err(LocalCoreStateStoreError::ContextTransaction(_))
        ) && matches!(
            removed.as_ref(),
            Ok(Err(crate::WorkspaceContextError::Store(
                LocalCoreStateStoreError::ContextTransaction(_)
            )))
        ),
        "corrupt discovery must fail closed, not retry: list={listed:?}, cas={swapped:?}, removal={removed:?}"
    );
}

#[tokio::test]
async fn workspace_contexts_are_durable_and_discoverable() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = LocalCoreStateStore::new(directory.path());
    let context_id = WorkspaceContextId::from_bytes([8; 16]);
    let root_id = WorkspaceRootId::from_bytes([9; 16]);
    let context = WorkspaceContext {
        version: 1,
        revision: 1,
        context_id,
        parent_context_id: None,
        roots: [(
            root_id,
            WorkspaceContextRoot {
                root_id,
                source_path: directory.path().to_path_buf(),
                workspace_id: workspace(),
                workspace_name: "durable".to_owned(),
                parent_workspace_id: None,
                mount_path: None,
            },
        )]
        .into_iter()
        .collect(),
        state: WorkspaceContextState::Active,
    };
    assert!(
        WorkspaceContextStore::compare_and_swap(&store, context_id, 0, context.clone())
            .await
            .expect("write context")
    );
    let reopened = LocalCoreStateStore::new(directory.path());
    assert_eq!(
        WorkspaceContextStore::list(&reopened)
            .await
            .expect("list contexts"),
        [context]
    );
}

#[tokio::test]
async fn context_discard_reads_only_the_bounded_subtree() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = LocalCoreStateStore::new(directory.path());
    let namespace = store.namespace().expect("admitted namespace");
    let parent_id = WorkspaceContextId::from_bytes([20; 16]);
    let child_id = WorkspaceContextId::from_bytes([21; 16]);
    let unrelated_id = WorkspaceContextId::from_bytes([22; 16]);
    let parent = workspace_context(
        directory.path(),
        parent_id,
        1,
        WorkspaceContextState::Active,
    );
    let mut child = workspace_context(directory.path(), child_id, 1, WorkspaceContextState::Active);
    child.parent_context_id = Some(parent_id);
    let unrelated = workspace_context(
        directory.path(),
        unrelated_id,
        1,
        WorkspaceContextState::Active,
    );
    for context in [parent, child, unrelated] {
        assert!(
            WorkspaceContextStore::compare_and_swap(&store, context.context_id, 0, context,)
                .await
                .expect("register context")
        );
    }

    std::fs::write(context_paths(&namespace, unrelated_id).current, b"not-json")
        .expect("corrupt unrelated record");

    assert_eq!(
        WorkspaceContextStore::discard_subtree(&store, parent_id, child_id, 1)
            .await
            .expect("discard bounded subtree"),
        WorkspaceContextDiscardOutcome::Discarded(vec![child_id])
    );
    assert_eq!(
        WorkspaceContextStore::load(&store, child_id)
            .await
            .expect("load discarded child")
            .expect("child remains durable")
            .state,
        WorkspaceContextState::Discarded
    );
}

#[tokio::test]
async fn context_discard_checks_child_count_before_reading_bucket() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = LocalCoreStateStore::new(directory.path());
    let namespace = store.namespace().expect("admitted namespace");
    let parent_id = WorkspaceContextId::from_bytes([25; 16]);
    let child_id = WorkspaceContextId::from_bytes([26; 16]);
    let parent = workspace_context(
        directory.path(),
        parent_id,
        1,
        WorkspaceContextState::Active,
    );
    let mut child = workspace_context(directory.path(), child_id, 1, WorkspaceContextState::Active);
    child.parent_context_id = Some(parent_id);
    for context in [parent, child] {
        assert!(
            WorkspaceContextStore::compare_and_swap(&store, context.context_id, 0, context,)
                .await
                .expect("register context")
        );
    }
    let count = count_paths(&namespace, child_id);
    std::fs::create_dir_all(&count.directory).expect("child count directory");
    write_journaled(&count, &10_000_u64).expect("write oversized child count");
    let bucket = children_paths(&namespace, child_id);
    std::fs::create_dir_all(&bucket.directory).expect("child bucket directory");
    std::fs::write(&bucket.current, b"not-json").expect("corrupt oversized child bucket");

    assert_eq!(
        WorkspaceContextStore::discard_subtree(&store, parent_id, child_id, 1)
            .await
            .expect("reject before child bucket read"),
        WorkspaceContextDiscardOutcome::TraversalLimit
    );
}

#[tokio::test]
async fn context_discard_bounds_bucket_bytes_before_deserialization() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = LocalCoreStateStore::new(directory.path());
    let namespace = store.namespace().expect("admitted namespace");
    let parent_id = WorkspaceContextId::from_bytes([30; 16]);
    let child_id = WorkspaceContextId::from_bytes([31; 16]);
    let parent = workspace_context(
        directory.path(),
        parent_id,
        1,
        WorkspaceContextState::Active,
    );
    let mut child = workspace_context(directory.path(), child_id, 1, WorkspaceContextState::Active);
    child.parent_context_id = Some(parent_id);
    for context in [parent, child] {
        assert!(
            WorkspaceContextStore::compare_and_swap(&store, context.context_id, 0, context,)
                .await
                .expect("register context")
        );
    }
    let count = count_paths(&namespace, child_id);
    std::fs::create_dir_all(&count.directory).expect("child count directory");
    write_journaled(&count, &1_u64).expect("write understated child count");
    let bucket = children_paths(&namespace, child_id);
    std::fs::create_dir_all(&bucket.directory).expect("child bucket directory");
    std::fs::write(&bucket.current, vec![b' '; 1_000_000]).expect("write oversized child bucket");

    assert!(matches!(
        WorkspaceContextStore::discard_subtree(&store, parent_id, child_id, 2).await,
        Err(LocalCoreStateStoreError::ContextTransaction(message))
            if message.contains("exceeds its declared bound")
    ));
}

#[tokio::test]
async fn context_discard_never_rebuilds_a_missing_index() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = LocalCoreStateStore::new(directory.path());
    let namespace = store.namespace().expect("admitted namespace");
    let parent_id = WorkspaceContextId::from_bytes([27; 16]);
    let child_id = WorkspaceContextId::from_bytes([28; 16]);
    let unrelated_id = WorkspaceContextId::from_bytes([29; 16]);
    let mut child = workspace_context(directory.path(), child_id, 1, WorkspaceContextState::Active);
    child.parent_context_id = Some(parent_id);
    for context in [
        workspace_context(
            directory.path(),
            parent_id,
            1,
            WorkspaceContextState::Active,
        ),
        child,
        workspace_context(
            directory.path(),
            unrelated_id,
            1,
            WorkspaceContextState::Active,
        ),
    ] {
        let paths = context_paths(&namespace, context.context_id);
        std::fs::create_dir_all(&paths.directory).expect("context directory");
        write_journaled(&paths, &context).expect("write context");
    }
    std::fs::write(context_paths(&namespace, unrelated_id).current, b"not-json")
        .expect("corrupt unrelated record");

    assert_eq!(
        WorkspaceContextStore::discard_subtree(&store, parent_id, child_id, 1)
            .await
            .expect("fail closed without rebuilding index"),
        WorkspaceContextDiscardOutcome::IncompatibleState
    );
}

#[tokio::test]
async fn intact_mismatched_frame_cannot_checkpoint_or_hide_context() {
    for owned in [false, true] {
        let directory = tempfile::tempdir().expect("temporary directory");
        let context_id = WorkspaceContextId::from_bytes([41; 16]);
        let store = LocalCoreStateStore::new(directory.path());
        let context = workspace_context(
            directory.path(),
            context_id,
            1,
            WorkspaceContextState::Active,
        );
        assert!(
            WorkspaceContextStore::compare_and_swap(&store, context_id, 0, context)
                .await
                .expect("persist context")
        );
        let namespace = store.namespace().expect("admitted namespace");
        let context_path = context_paths(&namespace, context_id).current;
        let log_path = &namespace
            .record(CORE_LOG_FAMILY, &[0; 16])
            .expect("record paths")
            .current;
        let before = std::fs::read(&context_path).expect("context bytes");
        drop(namespace);
        drop(store);
        let writer = LocalCoreStateStore::open_owned(directory.path()).expect("writer");
        assert!(
            writer
                .compare_and_swap_lazy_workspace(workspace(), 0, lazy_state(workspace(), 1), &mut crate::kernel::KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()))
                .await
                .expect("commit lazy workspace frame")
        );
        drop(writer);
        let frame = rewrite_intact_frame(directory.path(), |frame| {
            frame["change"][0][0] =
                serde_json::to_value(RecordKey::Context(context_id)).expect("context record key");
        });
        let reader = if owned {
            LocalCoreStateStore::open_owned(directory.path()).expect("reader")
        } else {
            LocalCoreStateStore::new(directory.path())
        };
        let listed = WorkspaceContextStore::list(&reader).await;
        let after = std::fs::read(&context_path).expect("context remains readable");
        let after_frame = std::fs::read(log_path).expect("log bytes");
        assert!(
            matches!(listed.as_ref(), Err(LocalCoreStateStoreError::Corrupt(_)))
                && before == after
                && frame == after_frame,
            "owned={owned}: reject before residency/checkpoint: list={listed:?}, context preserved={}, frame preserved={}",
            before == after,
            frame == after_frame
        );
    }
}
