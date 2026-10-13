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
    let changes = agent.diff(&agent_base, &agent_head, 32).await?;
    assert_eq!(changes.from().id(), agent_base.id());
    assert_eq!(changes.to().id(), agent_head.id());
    assert!(!changes.changes().files.is_empty());

    main.write_text("/shared", "main").await?;
    let plan = agent.join_into(&main).plan().await?;
    assert!(matches!(
        plan.apply(
            ApplyOptions {
                if_target: plan.target_head(),
                idempotency_key: IdempotencyKey::new(),
            },
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
    transaction.rename("/before", "/after").await?;
    let TransactionCommit::Committed(end) = commit(&mut transaction).await? else {
        return Err("rename did not commit".into());
    };

    let first = workspace.diff(&base, &middle, 32).await?;
    let second = workspace.diff(&middle, &end, 32).await?;
    let composed = first.compose(&second, 32).await?;
    let direct = workspace.diff(&base, &end, 32).await?;
    assert_eq!(composed.from().id(), base.id());
    assert_eq!(composed.to().id(), end.id());
    assert_eq!(composed.changes(), direct.changes());
    assert!(matches!(
        second.compose(&first, 32).await,
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
    unbounded!(
        initial,
        write("/rename", bytes::Bytes::from_static(b"same"))
    )?;
    unbounded!(
        initial,
        write("/remove", bytes::Bytes::from_static(b"gone"))
    )?;
    unbounded!(
        initial,
        write("/linked", bytes::Bytes::from_static(b"links"))
    )?;
    unbounded!(
        initial,
        write("/modified", bytes::Bytes::from_static(b"before"))
    )?;
    assert!(matches!(
        commit(&mut initial).await?,
        TransactionCommit::Committed(_)
    ));
    let before = unbounded!(workspace, head())?;

    let mut changed = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    changed.rename("/rename", "/renamed").await?;
    unbounded!(changed, remove("/remove"))?;
    unbounded!(changed, hard_link("/linked", "/nested/link"))?;
    unbounded!(
        changed,
        write("/nested/deep/added", bytes::Bytes::from_static(b"new"))
    )?;
    unbounded!(
        changed,
        write("/modified", bytes::Bytes::from_static(b"after"))
    )?;
    let TransactionCommit::Committed(after) = commit(&mut changed).await? else {
        return Err("changed paths did not commit".into());
    };

    let diff = generation_changes(&before, &after, 64).await?;
    let paths = unbounded!(diff, changed_paths(64))?;
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
        unbounded!(diff, changed_paths(1)),
        Err(WorkspaceError::ChangedPathLimit)
    ));
    assert!(
        unbounded!(
            generation_changes(&after, &after, 1).await?,
            changed_paths(0)
        )?
        .is_empty()
    );
    #[cfg(not(target_arch = "wasm32"))]
    {
        let cancellation = CancellationToken::new();
        let original =
            crate::NativeMaterializationWork::new(selected_transaction_budget(), &cancellation)
                .map_err(|(error, _)| error)?;
        let (native_changes, diff_guard) = before
            .diff_to_with_native_work(&after, 64, &original, &cancellation)
            .await
            .map_err(|(error, _)| error)?;
        let (native_paths, path_guard) = native_changes
            .changed_paths_with_native_work(64, &original, &cancellation)
            .await
            .map_err(|(error, _)| error)?;
        assert_eq!(native_paths, paths);
        let before_rename = paths
            .iter()
            .find(|change| {
                super::namespace_path_text(&change.path).is_ok_and(|path| path == "/rename")
            })
            .and_then(|change| change.before)
            .ok_or("missing rename source")?;
        let after_rename = paths
            .iter()
            .find(|change| {
                super::namespace_path_text(&change.path).is_ok_and(|path| path == "/renamed")
            })
            .and_then(|change| change.after)
            .ok_or("missing rename target")?;
        assert_eq!(before_rename.file_id, after_rename.file_id);
        let link = paths
            .iter()
            .find(|change| {
                super::namespace_path_text(&change.path).is_ok_and(|path| path == "/nested/link")
            })
            .and_then(|change| change.after)
            .ok_or("missing hard link")?;
        assert_eq!(link.file_id, unbounded!(after, stat("/linked"))?.file_id);
        drop(native_paths);
        drop(path_guard);
        let failure = match native_changes
            .changed_paths_with_native_work(1, &original, &cancellation)
            .await
        {
            Err(failure) => failure,
            Ok(_) => return Err("native path bound unexpectedly succeeded".into()),
        };
        assert!(matches!(failure.0, WorkspaceError::ChangedPathLimit));
        drop(failure);
        drop(native_changes);
        drop(diff_guard);

        for deny_copy in [false, true] {
            let mut budget = selected_transaction_budget();
            if deny_copy {
                budget.bytes_copied = 0;
            } else {
                budget.items_examined = 0;
            }
            let denied = crate::NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(error, _)| error)?;
            let failure = match diff
                .changed_paths_with_native_work(64, &denied, &cancellation)
                .await
            {
                Err(failure) => failure,
                Ok(_) => return Err("native path admission unexpectedly succeeded".into()),
            };
            drop(failure);
        }
    }
    Ok(())
}
