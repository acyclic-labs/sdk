use super::*;

#[tokio::test]
async fn empty_fork_keeps_lineage_without_deleting_excluded_parent_paths()
-> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let parent = fs
        .create_workspace("scratch-parent", B, &cancel)
        .await?
        .value;
    parent.write_text("/memory.txt", "parent").await?;
    let pinned = parent.head(B, &cancel).await?.value;
    let key = IdempotencyKey::new();
    let child = parent
        .fork::<_, _, crate::WorkspaceError>(
            "scratch-child",
            ForkOptions::from_generation(pinned.clone(), key).inherit_paths(Vec::new()),
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            &mut allow::<FsError>,
        )
        .await?
        .value;
    let initial = child.head(B, &cancel).await?.value;
    assert_eq!(initial.parents(B, &cancel).await?.value, vec![pinned.id()]);
    assert!(
        child
            .list_directory("/", None, 1, B, &cancel)
            .await?
            .value
            .0
            .entries
            .is_empty()
    );
    let retried = parent
        .fork::<_, _, crate::WorkspaceError>(
            "scratch-child",
            ForkOptions::from_generation(pinned.clone(), key).inherit_paths(Vec::new()),
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            &mut allow::<FsError>,
        )
        .await?
        .value;
    assert_eq!(retried.head(B, &cancel).await?.value.id(), initial.id());
    assert!(
        parent
            .fork::<_, _, crate::WorkspaceError>(
                "scratch-child",
                ForkOptions::from_generation(pinned, key),
                PublicationPermit::Unrestricted,
                B,
                &cancel,
                &mut allow::<FsError>
            )
            .await
            .is_err()
    );
    child.write_text("/new.txt", "child").await?;
    parent.write_text("/concurrent.txt", "user").await?;
    let source = child.head(B, &cancel).await?.value;
    let target = parent.head(B, &cancel).await?.value;
    let plan = child
        .join_into(&parent)
        .plan_at(&source, &target, B, &cancel)
        .await?
        .value;
    let options = ApplyOptions {
        if_target: target.id(),
        idempotency_key: IdempotencyKey::new(),
    };
    assert!(matches!(
        plan.apply(options, std::collections::BTreeMap::new(), PublicationPermit::Unrestricted)
            .await?,
        JoinOutcome::Applied(_)
    ));
    assert_eq!(
        parent
            .read("/memory.txt", 16, B, &cancel)
            .await?
            .value
            .as_ref(),
        b"parent"
    );
    assert_eq!(
        parent
            .read("/concurrent.txt", 16, B, &cancel)
            .await?
            .value
            .as_ref(),
        b"user"
    );
    assert_eq!(
        parent
            .read("/new.txt", 16, B, &cancel)
            .await?
            .value
            .as_ref(),
        b"child"
    );
    assert_eq!(child.head(B, &cancel).await?.value.id(), source.id());
    Ok(())
}

#[tokio::test]
async fn selected_fork_import_preserves_omissions_and_detects_selected_conflicts()
-> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    for paths in [vec!["/notes/keep.txt"], vec!["/notes"]] {
        let fs = Fs::memory();
        let parent = fs.create_workspace("parent", B, &cancel).await?.value;
        let mut transaction = parent
            .begin_transaction(IdempotencyKey::new(), B, &cancel)
            .await?
            .value;
        transaction
            .create_directory("/notes", B, &cancel)
            .await?
            .value;
        transaction
            .commit(
                None,
                PublicationPermit::Unrestricted,
                B,
                &cancel,
                allow::<WorkspaceError>,
            )
            .await?
            .value;
        parent.write_text("/notes/keep.txt", "base").await?;
        parent.write_text("/notes/other.txt", "other").await?;
        parent.write_text("/excluded.txt", "excluded").await?;
        let pinned = parent.head(B, &cancel).await?.value;
        let child = parent
            .fork::<_, _, crate::WorkspaceError>(
                "child",
                ForkOptions::from_generation(pinned.clone(), IdempotencyKey::new())
                    .inherit_paths(paths.iter().map(|path| (*path).to_owned()).collect()),
                PublicationPermit::Unrestricted,
                B,
                &cancel,
                &mut allow::<FsError>,
            )
            .await?
            .value;
        assert_eq!(
            child
                .head(B, &cancel)
                .await?
                .value
                .parents(B, &cancel)
                .await?
                .value,
            vec![pinned.id()]
        );
        assert_eq!(
            child
                .read("/notes/keep.txt", 16, B, &cancel)
                .await?
                .value
                .as_ref(),
            b"base"
        );
        assert!(child.read("/excluded.txt", 16, B, &cancel).await.is_err());
        child.write_text("/notes/keep.txt", "child").await?;
        parent.write_text("/excluded.txt", "updated").await?;
        let source = child.head(B, &cancel).await?.value;
        let target = parent.head(B, &cancel).await?.value;
        let plan = child
            .join_into(&parent)
            .plan_at(&source, &target, B, &cancel)
            .await?
            .value;
        assert!(matches!(
            plan.apply(
                ApplyOptions {
                    if_target: target.id(),
                    idempotency_key: IdempotencyKey::new()
                }, std::collections::BTreeMap::new(),
                PublicationPermit::Unrestricted
            )
            .await?,
            JoinOutcome::Applied(_)
        ));
        assert_eq!(
            parent
                .read("/excluded.txt", 16, B, &cancel)
                .await?
                .value
                .as_ref(),
            b"updated"
        );
        assert_eq!(
            parent
                .read("/notes/other.txt", 16, B, &cancel)
                .await?
                .value
                .as_ref(),
            b"other"
        );
        assert_eq!(
            parent
                .read("/notes/keep.txt", 16, B, &cancel)
                .await?
                .value
                .as_ref(),
            b"child"
        );
        assert_eq!(child.head(B, &cancel).await?.value.id(), source.id());
        child.write_text("/notes/keep.txt", "child again").await?;
        parent.write_text("/notes/keep.txt", "parent again").await?;
        let plan = child.join_into(&parent).plan(B, &cancel).await?.value;
        assert!(matches!(
            plan.apply(
                ApplyOptions {
                    if_target: parent.head(B, &cancel).await?.value.id(),
                    idempotency_key: IdempotencyKey::new()
                }, std::collections::BTreeMap::new(),
                PublicationPermit::Unrestricted
            )
            .await?,
            JoinOutcome::Conflicted { .. }
        ));
    }
    Ok(())
}

/// Exhausts eight selections and two descendant shapes through the production
/// fork/planner/publisher. The bound is three files and two fork edges; it is
/// evidence for filtered delta semantics, not an unrestricted lineage proof.
#[tokio::test]
async fn filtered_fork_subset_model_preserves_unselected_paths() -> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    for mask in 0_u8..8 {
        for descendant in [false, true] {
            let fs = Fs::memory();
            let parent = fs
                .create_workspace("subset-parent", B, &cancel)
                .await?
                .value;
            for index in 0..3 {
                parent.write_text(&format!("/file-{index}"), "base").await?;
            }
            let pinned = parent.head(B, &cancel).await?.value;
            let paths = (0..3)
                .filter(|index| mask & (1 << index) != 0)
                .map(|index| format!("/file-{index}"))
                .collect();
            let child = parent
                .fork::<_, _, crate::WorkspaceError>(
                    "subset-child",
                    ForkOptions::from_generation(pinned, IdempotencyKey::new())
                        .inherit_paths(paths),
                    PublicationPermit::Unrestricted,
                    B,
                    &cancel,
                    &mut allow::<FsError>,
                )
                .await?
                .value;
            let source = if descendant {
                child
                    .fork::<_, _, crate::WorkspaceError>(
                        "subset-grandchild",
                        ForkOptions::from_generation(
                            child.head(B, &cancel).await?.value,
                            IdempotencyKey::new(),
                        ),
                        PublicationPermit::Unrestricted,
                        B,
                        &cancel,
                        &mut allow::<FsError>,
                    )
                    .await?
                    .value
            } else {
                child
            };
            for index in 0..3 {
                let path = format!("/file-{index}");
                if mask & (1 << index) != 0 {
                    source.write_text(&path, "selected").await?;
                } else {
                    assert!(source.read(&path, 16, B, &cancel).await.is_err());
                    parent.write_text(&path, "concurrent").await?;
                }
            }
            source.write_text("/added", "new").await?;
            let selected_head = source.head(B, &cancel).await?.value;
            source.write_text("/after-pin", "excluded by pin").await?;
            let source_head = source.head(B, &cancel).await?.value;
            let target = parent.head(B, &cancel).await?.value;
            let plan = source
                .join_into(&parent)
                .plan_at(&selected_head, &target, B, &cancel)
                .await?
                .value;
            assert!(matches!(
                plan.apply(
                    ApplyOptions {
                        if_target: target.id(),
                        idempotency_key: IdempotencyKey::new()
                    }, std::collections::BTreeMap::new(),
                    PublicationPermit::Unrestricted
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
                    parent
                        .read(&format!("/file-{index}"), 16, B, &cancel)
                        .await?
                        .value
                        .as_ref(),
                    expected.as_bytes()
                );
            }
            assert!(parent.read("/after-pin", 32, B, &cancel).await.is_err());
            assert_eq!(source.head(B, &cancel).await?.value.id(), source_head.id());
            let repeated = source
                .join_into(&parent)
                .plan_at(
                    &selected_head,
                    &parent.head(B, &cancel).await?.value,
                    B,
                    &cancel,
                )
                .await?
                .value;
            assert!(matches!(
                repeated
                    .apply(
                        ApplyOptions {
                            if_target: repeated.target_head(),
                            idempotency_key: IdempotencyKey::new()
                        }, std::collections::BTreeMap::new(),
                        PublicationPermit::Unrestricted
                    )
                    .await?,
                JoinOutcome::NoChanges(_)
            ));
        }
    }
    // Negative control: deletion on an ordinary full fork IS an authored
    // deletion and must reach the parent. Filtering must not mean fork+clear.
    let fs = Fs::memory();
    let parent = fs
        .create_workspace("negative-parent", B, &cancel)
        .await?
        .value;
    parent.write_text("/omitted", "base").await?;
    let child = parent
        .fork::<_, _, crate::WorkspaceError>(
            "negative-child",
            ForkOptions::from_generation(
                parent.head(B, &cancel).await?.value,
                IdempotencyKey::new(),
            ),
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            &mut allow::<FsError>,
        )
        .await?
        .value;
    child.remove("/omitted").await?;
    let plan = child.join_into(&parent).plan(B, &cancel).await?.value;
    assert!(matches!(
        plan.apply(
            ApplyOptions {
                if_target: plan.target_head(),
                idempotency_key: IdempotencyKey::new()
            }, std::collections::BTreeMap::new(),
            PublicationPermit::Unrestricted
        )
        .await?,
        JoinOutcome::Applied(_)
    ));
    assert!(parent.read("/omitted", 16, B, &cancel).await.is_err());
    Ok(())
}

#[tokio::test]
async fn filtered_fork_rebase_preserves_local_and_parent_changes() -> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let parent = fs
        .create_workspace("filtered-rebase-parent", B, &cancel)
        .await?
        .value;
    parent.write_text("/omitted", "base").await?;
    let child = parent
        .fork::<_, _, crate::WorkspaceError>(
            "filtered-rebase-child",
            ForkOptions::from_generation(
                parent.head(B, &cancel).await?.value,
                IdempotencyKey::new(),
            )
            .inherit_paths(Vec::new()),
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            &mut allow::<FsError>,
        )
        .await?
        .value;
    child.write_text("/local", "child").await?;
    parent.write_text("/omitted", "user edit").await?;
    assert!(matches!(
        child.live_rebase(IdempotencyKey::new(), 64, 64, 16).await?,
        WorkspaceRebase::Rebased(_)
    ));
    assert_eq!(
        child.read("/omitted", 16, B, &cancel).await?.value.as_ref(),
        b"user edit"
    );
    assert_eq!(
        child.read("/local", 16, B, &cancel).await?.value.as_ref(),
        b"child"
    );
    child.write_text("/local", "after rebase").await?;
    let source = child.head(B, &cancel).await?.value;
    let plan = child.join_into(&parent).plan(B, &cancel).await?.value;
    assert!(matches!(
        plan.apply(
            ApplyOptions {
                if_target: plan.target_head(),
                idempotency_key: IdempotencyKey::new()
            }, std::collections::BTreeMap::new(),
            PublicationPermit::Unrestricted
        )
        .await?,
        JoinOutcome::Applied(_)
    ));
    assert_eq!(
        parent
            .read("/omitted", 16, B, &cancel)
            .await?
            .value
            .as_ref(),
        b"user edit"
    );
    assert_eq!(
        parent.read("/local", 16, B, &cancel).await?.value.as_ref(),
        b"after rebase"
    );
    assert_eq!(child.head(B, &cancel).await?.value.id(), source.id());
    Ok(())
}

#[tokio::test]
async fn nested_filtered_fork_uses_its_pinned_inheritance_as_delta_baseline()
-> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let root = fs.create_workspace("nested-root", B, &cancel).await?.value;
    root.write_text("/keep", "root").await?;
    root.write_text("/omit", "root").await?;
    let parent = root
        .fork::<_, _, crate::WorkspaceError>(
            "nested-parent",
            ForkOptions::from_generation(root.head(B, &cancel).await?.value, IdempotencyKey::new()),
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            &mut allow::<FsError>,
        )
        .await?
        .value;
    parent.write_text("/keep", "inherited").await?;
    let child = parent
        .fork::<_, _, crate::WorkspaceError>(
            "nested-child",
            ForkOptions::from_generation(
                parent.head(B, &cancel).await?.value,
                IdempotencyKey::new(),
            )
            .inherit_paths(vec!["/keep".into()]),
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            &mut allow::<FsError>,
        )
        .await?
        .value;
    // Inherited content is the child baseline; only authored child changes
    // enter a destination. An unchanged selected file is not a child edit.
    child.write_text("/new", "child").await?;
    let plan = child.join_into(&root).plan(B, &cancel).await?.value;
    plan.apply(
        ApplyOptions {
            if_target: plan.target_head(),
            idempotency_key: IdempotencyKey::new(),
        }, std::collections::BTreeMap::new(),
        PublicationPermit::Unrestricted,
    )
    .await?;
    assert_eq!(
        root.read("/keep", 32, B, &cancel).await?.value.as_ref(),
        b"root"
    );
    assert_eq!(
        root.read("/omit", 32, B, &cancel).await?.value.as_ref(),
        b"root"
    );
    assert_eq!(
        root.read("/new", 32, B, &cancel).await?.value.as_ref(),
        b"child"
    );
    child.write_text("/keep", "edited").await?;
    let plan = child.join_into(&root).plan(B, &cancel).await?.value;
    assert!(matches!(
        plan.apply(
            ApplyOptions {
                if_target: plan.target_head(),
                idempotency_key: IdempotencyKey::new(),
            }, std::collections::BTreeMap::new(),
            PublicationPermit::Unrestricted
        )
        .await?,
        JoinOutcome::Conflicted { .. }
    ));
    root.write_text("/keep", "inherited").await?;
    let plan = child.join_into(&root).plan(B, &cancel).await?.value;
    plan.apply(
        ApplyOptions {
            if_target: plan.target_head(),
            idempotency_key: IdempotencyKey::new(),
        }, std::collections::BTreeMap::new(),
        PublicationPermit::Unrestricted,
    )
    .await?;
    assert_eq!(
        root.read("/keep", 32, B, &cancel).await?.value.as_ref(),
        b"edited"
    );
    assert_eq!(
        parent.read("/keep", 32, B, &cancel).await?.value.as_ref(),
        b"inherited"
    );
    Ok(())
}
