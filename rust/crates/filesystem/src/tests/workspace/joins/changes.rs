use super::*;

#[tokio::test]
async fn semantic_change_set_and_overlap_conflict_are_exact() -> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let main = fs.create_workspace("main", B, &cancel).await?.value;
    main.write_text("/shared", "base").await?;
    let base = main.head(B, &cancel).await?.value;
    let agent = main
        .fork(
            "agent",
            ForkOptions::from_generation(base, IdempotencyKey::new()),
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            allow::<FsError>,
        )
        .await?
        .value;
    let agent_base = agent.head(B, &cancel).await?.value;
    agent.write_text("/shared", "agent").await?;
    let agent_head = agent.head(B, &cancel).await?.value;
    let changes = agent
        .diff(&agent_base, &agent_head, 32, B, &cancel)
        .await?
        .value;
    assert_eq!(changes.from().id(), agent_base.id());
    assert_eq!(changes.to().id(), agent_head.id());
    assert!(!changes.changes().files.is_empty());

    main.write_text("/shared", "main").await?;
    let plan = agent.join_into(&main).plan(B, &cancel).await?.value;
    assert!(matches!(
        plan.apply(
            ApplyOptions {
                if_target: plan.target_head(),
                idempotency_key: IdempotencyKey::new(),
            },
            PublicationPermit::Unrestricted
        )
        .await?,
        JoinOutcome::Conflicted {
            truncated: false,
            ..
        }
    ));
    assert_eq!(
        main.read("/shared", 16, B, &cancel).await?.value,
        OwnedBytes::from_static(b"main")
    );
    Ok(())
}

#[tokio::test]
async fn contiguous_change_sets_compose_by_outer_semantics() -> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let workspace = fs.create_workspace("compose", B, &cancel).await?.value;
    let base = workspace.head(B, &cancel).await?.value;
    workspace.write_text("/before", "body").await?;
    let middle = workspace.head(B, &cancel).await?.value;
    let mut transaction = workspace
        .begin_transaction(IdempotencyKey::new(), B, &cancel)
        .await?
        .value;
    transaction
        .rename("/before", "/after", true, B, &cancel)
        .await?
        .value;
    let TransactionCommit::Committed(end) = transaction
        .commit(
            None,
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            allow::<WorkspaceError>,
        )
        .await?
        .value
    else {
        return Err("rename did not commit".into());
    };

    let first = workspace.diff(&base, &middle, 32, B, &cancel).await?.value;
    let second = workspace.diff(&middle, &end, 32, B, &cancel).await?.value;
    let composed = first.compose(&second, 32).await?;
    let direct = workspace.diff(&base, &end, 32, B, &cancel).await?.value;
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
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace("changed-paths", B, &cancel)
        .await?
        .value;
    let mut initial = workspace
        .begin_transaction(IdempotencyKey::new(), B, &cancel)
        .await?
        .value;
    initial
        .create_dir_all("/nested/deep", B, &cancel)
        .await?
        .value;
    initial
        .write("/rename", OwnedBytes::from_static(b"same"), B, &cancel)
        .await?;
    initial
        .write("/remove", OwnedBytes::from_static(b"gone"), B, &cancel)
        .await?;
    initial
        .write("/linked", OwnedBytes::from_static(b"links"), B, &cancel)
        .await?;
    initial
        .write("/modified", OwnedBytes::from_static(b"before"), B, &cancel)
        .await?;
    assert!(matches!(
        initial
            .commit(
                None,
                PublicationPermit::Unrestricted,
                B,
                &cancel,
                allow::<WorkspaceError>
            )
            .await?
            .value,
        TransactionCommit::Committed(_)
    ));
    let before = workspace.head(B, &cancel).await?.value;

    let mut changed = workspace
        .begin_transaction(IdempotencyKey::new(), B, &cancel)
        .await?
        .value;
    changed
        .rename("/rename", "/renamed", true, B, &cancel)
        .await?
        .value;
    changed.remove("/remove", B, &cancel).await?;
    changed
        .hard_link("/linked", "/nested/link", B, &cancel)
        .await?
        .value;
    changed
        .write(
            "/nested/deep/added",
            OwnedBytes::from_static(b"new"),
            B,
            &cancel,
        )
        .await?;
    changed
        .write("/modified", OwnedBytes::from_static(b"after"), B, &cancel)
        .await?;
    let TransactionCommit::Committed(after) = changed
        .commit(
            None,
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            allow::<WorkspaceError>,
        )
        .await?
        .value
    else {
        return Err("changed paths did not commit".into());
    };

    let paths = before
        .diff_to(&after, 64, B, &cancel)
        .await?
        .value
        .0
        .changed_paths(64, B, &cancel)
        .await?
        .value
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
        before
            .diff_to(&after, 64, B, &cancel)
            .await?
            .value
            .0
            .changed_paths(1, B, &cancel)
            .await,
        Err(crate::OperationFailure {
            error: WorkspaceError::ChangedPathLimit,
            ..
        })
    ));
    let exact = before.diff_to(&after, 64, B, &cancel).await?.value.0;
    let mut truncated_changes = exact.changes().clone();
    truncated_changes.truncated = true;
    let truncated = ChangeSet {
        from: before.clone(),
        to: after.clone(),
        changes: truncated_changes,
        work: exact.work(),
    };
    assert!(matches!(
        truncated.changed_paths(64, B, &cancel).await,
        Err(crate::OperationFailure {
            error: WorkspaceError::ChangedPathLimit,
            ..
        })
    ));
    assert!(
        after
            .diff_to(&after, 1, B, &cancel)
            .await?
            .value
            .0
            .changed_paths(0, B, &cancel)
            .await?
            .value
            .0
            .is_empty()
    );
    Ok(())
}
