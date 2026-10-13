use super::*;

#[tokio::test]
async fn semantic_change_set_and_overlap_conflict_are_exact() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = unbounded!(fs, create_workspace("main"))?;
    main.write_text("/shared", "base").await?;
    let base = unbounded!(main, head())?;
    let agent = fork_workspace(
        &main,
        "agent",
        ForkOptions::from_generation(base, IdempotencyKey::new()),
    )
    .await?;
    let agent_base = unbounded!(agent, head())?;
    agent.write_text("/shared", "agent").await?;
    let agent_head = unbounded!(agent, head())?;
    let changes = unbounded!(agent, diff(&agent_base, &agent_head, 32))?;
    assert_eq!(changes.from().id(), agent_base.id());
    assert_eq!(changes.to().id(), agent_head.id());
    assert!(!changes.changes().files.is_empty());

    main.write_text("/shared", "main").await?;
    let plan = unbounded!(agent.join_into(&main), plan())?;
    assert!(matches!(
        plan.apply(
            ApplyOptions {
                if_target: plan.target_head(),
                idempotency_key: IdempotencyKey::new(),
            },
            std::collections::BTreeMap::new(),
            crate::PublicationPermit::Unrestricted
        )
        .await?,
        JoinOutcome::Conflicted {
            truncated: false,
            ..
        }
    ));
    assert_eq!(
        unbounded!(main, read("/shared", 16))?,
        OwnedBytes::from_static(b"main")
    );
    Ok(())
}

#[tokio::test]
async fn contiguous_change_sets_compose_by_outer_semantics() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("compose"))?;
    let base = unbounded!(workspace, head())?;
    workspace.write_text("/before", "body").await?;
    let middle = unbounded!(workspace, head())?;
    let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(transaction, rename("/before", "/after", true))?;
    let TransactionCommit::Committed(end) = commit(&mut transaction).await? else {
        return Err("rename did not commit".into());
    };

    let first = unbounded!(workspace, diff(&base, &middle, 32))?;
    let second = unbounded!(workspace, diff(&middle, &end, 32))?;
    let composed = unbounded!(first, compose(&second, 32))?;
    let direct = unbounded!(workspace, diff(&base, &end, 32))?;
    assert_eq!(composed.from().id(), base.id());
    assert_eq!(composed.to().id(), end.id());
    assert_eq!(composed.changes(), direct.changes());
    assert!(matches!(
        unbounded!(second, compose(&first, 32)),
        Err(WorkspaceError::ChangeSetContinuity)
    ));
    Ok(())
}

#[tokio::test]
async fn change_set_resolves_only_changed_bindings_to_portable_paths() -> Result<(), Box<dyn Error>>
{
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("changed-paths"))?;
    let mut initial = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(initial, create_dir_all("/nested/deep"))?;
    unbounded!(initial, write("/rename", OwnedBytes::from_static(b"same")))?;
    unbounded!(initial, write("/remove", OwnedBytes::from_static(b"gone")))?;
    unbounded!(initial, write("/linked", OwnedBytes::from_static(b"links")))?;
    unbounded!(
        initial,
        write("/modified", OwnedBytes::from_static(b"before"))
    )?;
    assert!(matches!(
        commit(&mut initial).await?,
        TransactionCommit::Committed(_)
    ));
    let before = unbounded!(workspace, head())?;

    let mut changed = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(changed, rename("/rename", "/renamed", true))?;
    unbounded!(changed, remove("/remove"))?;
    unbounded!(changed, hard_link("/linked", "/nested/link"))?;
    unbounded!(
        changed,
        write("/nested/deep/added", OwnedBytes::from_static(b"new"))
    )?;
    unbounded!(
        changed,
        write("/modified", OwnedBytes::from_static(b"after"))
    )?;
    let TransactionCommit::Committed(after) = commit(&mut changed).await? else {
        return Err("changed paths did not commit".into());
    };

    let paths = unbounded!(
        unbounded!(before, diff_to(&after, 64))?.0,
        changed_paths(64)
    )?
    .0;
    let names: Vec<_> = paths
        .iter()
        .map(|change| super::namespace_path_text(&change.path))
        .collect::<Result<_, _>>()?;
    assert_eq!(
        names,
        [
            "/modified",
            "/nested/deep/added",
            "/nested/link",
            "/remove",
            "/rename",
            "/renamed"
        ]
    );
    assert!(paths.iter().any(|change| {
        super::namespace_path_text(&change.path).is_ok_and(|path| path == "/remove")
            && change.before.is_some()
            && change.after.is_none()
    }));
    assert!(paths.iter().any(|change| {
        super::namespace_path_text(&change.path).is_ok_and(|path| path == "/nested/deep/added")
            && change.before.is_none()
            && change.after.is_some()
    }));
    assert!(paths.iter().any(|change| {
        super::namespace_path_text(&change.path).is_ok_and(|path| path == "/modified")
            && change.before.is_some()
            && change.after.is_some()
            && change.before != change.after
    }));
    assert!(!paths.iter().any(|change| {
        super::namespace_path_text(&change.path).is_ok_and(|path| path == "/linked")
    }));
    assert!(matches!(
        unbounded!(unbounded!(before, diff_to(&after, 64))?.0, changed_paths(1))
            .map(|value| value.0),
        Err(WorkspaceError::ChangedPathLimit)
    ));
    let exact = unbounded!(before, diff_to(&after, 64))?.0;
    let mut truncated_changes = exact.changes().clone();
    truncated_changes.truncated = true;
    let truncated = ChangeSet {
        from: before.clone(),
        to: after.clone(),
        changes: truncated_changes,
        work: exact.work(),
    };
    assert!(matches!(
        unbounded!(truncated, changed_paths(64)).map(|value| value.0),
        Err(WorkspaceError::ChangedPathLimit)
    ));
    assert!(
        unbounded!(unbounded!(after, diff_to(&after, 1))?.0, changed_paths(0))?
            .0
            .is_empty()
    );
    #[cfg(not(target_arch = "wasm32"))]
    {
        let cancellation = CancellationToken::new();
        let original =
            crate::NativeMaterializationWork::new(selected_transaction_budget(), &cancellation)
                .map_err(|(error, _)| error)?;
        let (native_changes, diff_guard) =
            before.diff_to(&after, 64, &original, &cancellation).await?;
        let (native_paths, path_guard) = native_changes
            .changed_paths(64, &original, &cancellation)
            .await?;
        assert_eq!(native_paths, paths);
        drop(native_paths);
        drop(path_guard);
        assert!(matches!(
            native_changes
                .changed_paths(1, &original, &cancellation)
                .await,
            Err(WorkspaceError::ChangedPathLimit)
        ));
        for deny_copy in [false, true] {
            let mut budget = selected_transaction_budget();
            if deny_copy {
                budget.bytes_copied = 0;
            } else {
                budget.items_examined = 0;
            }
            let denied = crate::NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(error, _)| error)?;
            assert!(
                native_changes
                    .changed_paths(64, &denied, &cancellation)
                    .await
                    .is_err()
            );
        }
        drop(native_changes);
        drop(diff_guard);
    }
    Ok(())
}
