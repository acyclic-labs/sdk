use super::*;

#[tokio::test]
async fn semantic_change_set_and_overlap_conflict_are_exact() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = fs
        .create_workspace(
            "main",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    main.write_text("/shared", "base").await?;
    let base = main
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    let agent = main
        .fork(
            "agent",
            ForkOptions::from_generation(base, IdempotencyKey::new()),
            crate::PublicationPermit::Unrestricted,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
            |_| {
                std::future::ready(Ok(crate::OperationReceipt {
                    value: (),
                    work: crate::WorkCounters::UNCHARGED,
                }))
            },
        )
        .await?
        .value;
    let agent_base = agent
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    agent.write_text("/shared", "agent").await?;
    let agent_head = agent
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
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
        main.read(
            "/shared",
            16,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new()
        )
        .await?
        .value,
        OwnedBytes::from_static(b"main")
    );
    Ok(())
}

#[tokio::test]
async fn contiguous_change_sets_compose_by_outer_semantics() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace(
            "compose",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    let base = workspace
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    workspace.write_text("/before", "body").await?;
    let middle = workspace
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    let mut transaction = workspace
        .begin_transaction(
            IdempotencyKey::new(),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    transaction
        .rename(
            "/before",
            "/after",
            true,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?;
    let TransactionCommit::Committed(end) = transaction
        .commit(
            None,
            crate::PublicationPermit::Unrestricted,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
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
    let workspace = fs
        .create_workspace(
            "changed-paths",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    let mut initial = workspace
        .begin_transaction(
            IdempotencyKey::new(),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    initial
        .create_dir_all(
            "/nested/deep",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    initial
        .write(
            "/rename",
            bytes::Bytes::from_static(b"same"),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    initial
        .write(
            "/remove",
            bytes::Bytes::from_static(b"gone"),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    initial
        .write(
            "/linked",
            bytes::Bytes::from_static(b"links"),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    initial
        .write(
            "/modified",
            bytes::Bytes::from_static(b"before"),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    assert!(matches!(
        initial
            .commit(
                None,
                crate::PublicationPermit::Unrestricted,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
                |_| async {
                    Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(
                        crate::OperationReceipt {
                            value: (),
                            work: crate::WorkCounters::default(),
                        },
                    )
                }
            )
            .await?
            .value,
        TransactionCommit::Committed(_)
    ));
    let before = workspace
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;

    let mut changed = workspace
        .begin_transaction(
            IdempotencyKey::new(),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    changed
        .rename(
            "/rename",
            "/renamed",
            true,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?;
    changed
        .remove(
            "/remove",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    changed
        .hard_link(
            "/linked",
            "/nested/link",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    changed
        .write(
            "/nested/deep/added",
            bytes::Bytes::from_static(b"new"),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    changed
        .write(
            "/modified",
            bytes::Bytes::from_static(b"after"),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    let TransactionCommit::Committed(after) = changed
        .commit(
            None,
            crate::PublicationPermit::Unrestricted,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
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
        return Err("changed paths did not commit".into());
    };

    let paths = before
        .diff_to(
            &after,
            64,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value.0)
        .map_err(|failure| failure.error)?
        .changed_paths(
            64,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value.0)
        .map_err(|failure| failure.error)?;
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
            .diff_to(
                &after,
                64,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await
            .map(|receipt| receipt.value.0)
            .map_err(|failure| failure.error)?
            .changed_paths(
                1,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await
            .map(|receipt| receipt.value.0)
            .map_err(|failure| failure.error),
        Err(WorkspaceError::ChangedPathLimit)
    ));
    let exact = before
        .diff_to(
            &after,
            64,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value.0)
        .map_err(|failure| failure.error)?;
    let mut truncated_changes = exact.changes().clone();
    truncated_changes.truncated = true;
    let truncated = ChangeSet {
        from: before.clone(),
        to: after.clone(),
        changes: truncated_changes,
        work: exact.work(),
    };
    assert!(matches!(
        truncated
            .changed_paths(
                64,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await
            .map(|receipt| receipt.value.0)
            .map_err(|failure| failure.error),
        Err(WorkspaceError::ChangedPathLimit)
    ));
    assert!(
        after
            .diff_to(
                &after,
                1,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await
            .map(|receipt| receipt.value.0)
            .map_err(|failure| failure.error)?
            .changed_paths(
                0,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await
            .map(|receipt| receipt.value.0)
            .map_err(|failure| failure.error)?
            .is_empty()
    );
    #[cfg(not(target_arch = "wasm32"))]
    {
        let cancellation = CancellationToken::new();
        let original =
            crate::NativeMaterializationWork::new(selected_transaction_budget(), &cancellation)
                .map_err(|(error, _)| error)?;
        let (native_changes, diff_guard) = before
            .diff_to(&after, 64, &original, &cancellation)
            .await
            .map(|(value, allocation)| (value, allocation.expect("native result allocation")))?;
        let retained_diff = original.live_allocation_bytes();
        let (native_paths, path_guard) = native_changes
            .changed_paths(64, &original, &cancellation)
            .await
            .map(|(value, allocation)| (value, allocation.expect("native result allocation")))?;
        assert_eq!(native_paths, paths);
        let rename_before = native_paths
            .iter()
            .find(|change| {
                super::namespace_path_text(&change.path).is_ok_and(|path| path == "/rename")
            })
            .and_then(|change| change.before)
            .ok_or("missing rename before")?;
        let rename_after = native_paths
            .iter()
            .find(|change| {
                super::namespace_path_text(&change.path).is_ok_and(|path| path == "/renamed")
            })
            .and_then(|change| change.after)
            .ok_or("missing rename after")?;
        assert_eq!(rename_before.file_id, rename_after.file_id);
        let link = native_paths
            .iter()
            .find(|change| {
                super::namespace_path_text(&change.path).is_ok_and(|path| path == "/nested/link")
            })
            .and_then(|change| change.after)
            .ok_or("missing hardlink after")?;
        assert_eq!(
            link.file_id,
            after
                .stat(
                    "/linked",
                    crate::WorkBudget::UNBOUNDED,
                    &crate::CancellationToken::new()
                )
                .await?
                .value
                .file_id
        );
        drop(native_paths);
        drop(path_guard);
        assert_eq!(original.live_allocation_bytes(), retained_diff);
        let failure = match native_changes
            .changed_paths(1, &original, &cancellation)
            .await
            .map(|(value, allocation)| (value, allocation.expect("native result allocation")))
        {
            Err(failure) => failure,
            Ok(_) => return Err("native path bound unexpectedly succeeded".into()),
        };
        assert!(matches!(failure, WorkspaceError::ChangedPathLimit));
        drop(failure);
        assert_eq!(original.live_allocation_bytes(), retained_diff);
        drop(native_changes);
        drop(diff_guard);
        assert_eq!(
            original.live_allocation_bytes(),
            crate::NativeMaterializationWork::accounting_state_bytes()
        );

        for deny_copy in [false, true] {
            let mut budget = selected_transaction_budget();
            if deny_copy {
                budget.bytes_copied = 0;
            } else {
                budget.items_examined = 0;
            }
            let denied = crate::NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(error, _)| error)?;
            let receipt = denied.receipt();
            let baseline = denied.live_allocation_bytes();
            let failure =
                match exact.changed_paths(64, &denied, &cancellation).await.map(
                    |(value, allocation)| (value, allocation.expect("native result allocation")),
                ) {
                    Err(failure) => failure,
                    Ok(_) => return Err("native path admission unexpectedly succeeded".into()),
                };
            assert_eq!(denied.receipt().page_reads, receipt.page_reads);
            assert_eq!(denied.receipt().bytes_copied, receipt.bytes_copied);
            drop(failure);
            assert_eq!(denied.live_allocation_bytes(), baseline);
        }
    }
    Ok(())
}
