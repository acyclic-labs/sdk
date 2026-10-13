use super::*;

#[tokio::test]
async fn empty_fork_keeps_lineage_without_deleting_excluded_parent_paths()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let parent = unbounded!(fs, create_workspace("scratch-parent"))?;
    parent.write_text("/memory.txt", "parent").await?;
    let pinned = unbounded!(parent, head())?;
    let key = IdempotencyKey::new();
    let child = fork_workspace(
        &parent,
        "scratch-child",
        ForkOptions::from_generation(pinned.clone(), key).empty(),
    )
    .await?;
    let initial = unbounded!(child, head())?;
    assert_eq!(unbounded!(initial, parents())?, vec![pinned.id()]);
    assert!(child.list_directory("/", None, 1).await?.entries.is_empty());
    let retried = fork_workspace(
        &parent,
        "scratch-child",
        ForkOptions::from_generation(pinned.clone(), key).empty(),
    )
    .await?;
    assert_eq!(unbounded!(retried, head())?.id(), initial.id());
    assert!(
        fork_workspace(
            &parent,
            "scratch-child",
            ForkOptions::from_generation(pinned, key)
        )
        .await
        .is_err()
    );
    child.write_text("/new.txt", "child").await?;
    parent.write_text("/concurrent.txt", "user").await?;
    let source = unbounded!(child, head())?;
    let target = unbounded!(parent, head())?;
    let plan = unbounded!(child.join_into(&parent), plan_at(&source, &target))?;
    let options = ApplyOptions {
        if_target: target.id(),
        idempotency_key: IdempotencyKey::new(),
    };
    assert!(matches!(
        plan.apply(options, crate::PublicationPermit::Unrestricted)
            .await?,
        JoinOutcome::Applied(_)
    ));
    assert_eq!(
        unbounded!(parent, read("/memory.txt", 16))?.as_ref(),
        b"parent"
    );
    assert_eq!(
        unbounded!(parent, read("/concurrent.txt", 16))?.as_ref(),
        b"user"
    );
    assert_eq!(unbounded!(parent, read("/new.txt", 16))?.as_ref(), b"child");
    assert_eq!(unbounded!(child, head())?.id(), source.id());
    Ok(())
}

#[tokio::test]
async fn selected_fork_import_preserves_omissions_and_detects_selected_conflicts()
-> Result<(), Box<dyn Error>> {
    for paths in [vec!["/notes/keep.txt"], vec!["/notes"]] {
        let fs = Fs::memory();
        let parent = unbounded!(fs, create_workspace("parent"))?;
        let mut transaction = unbounded!(parent, begin_transaction(IdempotencyKey::new()))?;
        unbounded!(transaction, create_directory("/notes"))?;
        commit(&mut transaction).await?;
        parent.write_text("/notes/keep.txt", "base").await?;
        parent.write_text("/notes/other.txt", "other").await?;
        parent.write_text("/excluded.txt", "excluded").await?;
        let pinned = unbounded!(parent, head())?;
        let child = fork_workspace(
            &parent,
            "child",
            ForkOptions::from_generation(pinned.clone(), IdempotencyKey::new())
                .inherit_paths(paths.iter().map(|path| (*path).to_owned()).collect()),
        )
        .await?;
        assert_eq!(
            unbounded!(unbounded!(child, head())?, parents())?,
            vec![pinned.id()]
        );
        assert_eq!(
            unbounded!(child, read("/notes/keep.txt", 16))?.as_ref(),
            b"base"
        );
        assert!(unbounded!(child, read("/excluded.txt", 16)).is_err());
        child.write_text("/notes/keep.txt", "child").await?;
        parent.write_text("/excluded.txt", "updated").await?;
        let source = unbounded!(child, head())?;
        let target = unbounded!(parent, head())?;
        let plan = unbounded!(child.join_into(&parent), plan_at(&source, &target))?;
        assert!(matches!(
            plan.apply(
                ApplyOptions {
                    if_target: target.id(),
                    idempotency_key: IdempotencyKey::new()
                },
                crate::PublicationPermit::Unrestricted
            )
            .await?,
            JoinOutcome::Applied(_)
        ));
        assert_eq!(
            unbounded!(parent, read("/excluded.txt", 16))?.as_ref(),
            b"updated"
        );
        assert_eq!(
            unbounded!(parent, read("/notes/other.txt", 16))?.as_ref(),
            b"other"
        );
        assert_eq!(
            unbounded!(parent, read("/notes/keep.txt", 16))?.as_ref(),
            b"child"
        );
        assert_eq!(unbounded!(child, head())?.id(), source.id());
        child.write_text("/notes/keep.txt", "child again").await?;
        parent.write_text("/notes/keep.txt", "parent again").await?;
        let plan = child.join_into(&parent).plan().await?;
        assert!(matches!(
            plan.apply(
                ApplyOptions {
                    if_target: unbounded!(parent, head())?.id(),
                    idempotency_key: IdempotencyKey::new()
                },
                crate::PublicationPermit::Unrestricted
            )
            .await?,
            JoinOutcome::Conflicted { .. }
        ));
    }
    Ok(())
}

#[tokio::test]
async fn filtered_fork_subset_model_preserves_unselected_paths() -> Result<(), Box<dyn Error>> {
    for mask in 0_u8..8 {
        for descendant in [false, true] {
            let fs = Fs::memory();
            let parent = unbounded!(fs, create_workspace("subset-parent"))?;
            for index in 0..3 {
                parent.write_text(&format!("/file-{index}"), "base").await?;
            }
            let pinned = unbounded!(parent, head())?;
            let paths = (0..3)
                .filter(|index| mask & (1 << index) != 0)
                .map(|index| format!("/file-{index}"))
                .collect();
            let child = fork_workspace(
                &parent,
                "subset-child",
                ForkOptions::from_generation(pinned, IdempotencyKey::new()).inherit_paths(paths),
            )
            .await?;
            let source = if descendant {
                fork_workspace(
                    &child,
                    "subset-grandchild",
                    ForkOptions::from_generation(unbounded!(child, head())?, IdempotencyKey::new()),
                )
                .await?
            } else {
                child
            };
            for index in 0..3 {
                let path = format!("/file-{index}");
                if mask & (1 << index) != 0 {
                    source.write_text(&path, "selected").await?;
                } else {
                    assert!(unbounded!(source, read(&path, 16)).is_err());
                    parent.write_text(&path, "concurrent").await?;
                }
            }
            source.write_text("/added", "new").await?;
            let selected_head = unbounded!(source, head())?;
            source.write_text("/after-pin", "excluded by pin").await?;
            let source_head = unbounded!(source, head())?;
            let target = unbounded!(parent, head())?;
            let plan = unbounded!(source.join_into(&parent), plan_at(&selected_head, &target))?;
            assert!(matches!(
                plan.apply(
                    ApplyOptions {
                        if_target: target.id(),
                        idempotency_key: IdempotencyKey::new()
                    },
                    crate::PublicationPermit::Unrestricted
                )
                .await?,
                JoinOutcome::Applied(_)
            ));
            for index in 0..3 {
                let expected = if mask & (1 << index) != 0 {
                    "selected"
                } else {
                    "concurrent"
                };
                assert_eq!(
                    unbounded!(parent, read(&format!("/file-{index}"), 16))?.as_ref(),
                    expected.as_bytes()
                );
            }
            assert!(unbounded!(parent, read("/after-pin", 32)).is_err());
            assert_eq!(unbounded!(source, head())?.id(), source_head.id());
            let repeated = unbounded!(
                source.join_into(&parent),
                plan_at(&selected_head, &unbounded!(parent, head())?)
            )?;
            assert!(matches!(
                repeated
                    .apply(
                        ApplyOptions {
                            if_target: repeated.target_head(),
                            idempotency_key: IdempotencyKey::new()
                        },
                        crate::PublicationPermit::Unrestricted
                    )
                    .await?,
                JoinOutcome::NoChanges(_)
            ));
        }
    }
    let fs = Fs::memory();
    let parent = unbounded!(fs, create_workspace("negative-parent"))?;
    parent.write_text("/omitted", "base").await?;
    let child = fork_workspace(
        &parent,
        "negative-child",
        ForkOptions::from_generation(unbounded!(parent, head())?, IdempotencyKey::new()),
    )
    .await?;
    child.remove("/omitted").await?;
    let plan = child.join_into(&parent).plan().await?;
    assert!(matches!(
        plan.apply(
            ApplyOptions {
                if_target: plan.target_head(),
                idempotency_key: IdempotencyKey::new()
            },
            crate::PublicationPermit::Unrestricted
        )
        .await?,
        JoinOutcome::Applied(_)
    ));
    assert!(unbounded!(parent, read("/omitted", 16)).is_err());
    Ok(())
}
