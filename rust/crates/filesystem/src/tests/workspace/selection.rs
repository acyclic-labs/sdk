use super::*;

#[tokio::test]
async fn empty_fork_keeps_lineage_without_deleting_excluded_parent_paths()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let parent = fs.create_workspace("scratch-parent", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    parent.write_text("/memory.txt", "parent").await?;
    let pinned = parent.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let key = IdempotencyKey::new();
    let child = parent
        .fork(
            "scratch-child",
            ForkOptions::from_generation(pinned.clone(), key).inherit_paths(Vec::new()),
        crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let initial = child.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert_eq!(initial.parents(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, vec![pinned.id()]);
    assert!(child.list_directory("/", None, 1, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value.0).map_err(|failure| failure.error)?.entries.is_empty());
    let retried = parent
        .fork(
            "scratch-child",
            ForkOptions::from_generation(pinned.clone(), key).inherit_paths(Vec::new()),
        crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert_eq!(retried.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), initial.id());
    assert!(
        parent
            .fork("scratch-child", ForkOptions::from_generation(pinned, key), crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)
            .is_err()
    );
    child.write_text("/new.txt", "child").await?;
    parent.write_text("/concurrent.txt", "user").await?;
    let source = child.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let target = parent.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let plan = child.join_into(&parent).plan_at(&source, &target, WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let options = ApplyOptions {
        if_target: target.id(),
        idempotency_key: IdempotencyKey::new(),
    };
    assert!(matches!(
        plan.apply(options, crate::PublicationPermit::Unrestricted).await?,
        JoinOutcome::Applied(_)
    ));
    assert_eq!(parent.read("/memory.txt", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"parent");
    assert_eq!(parent.read("/concurrent.txt", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"user");
    assert_eq!(parent.read("/new.txt", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"child");
    assert_eq!(child.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), source.id());
    Ok(())
}

#[tokio::test]
async fn selected_fork_import_preserves_omissions_and_detects_selected_conflicts()
-> Result<(), Box<dyn Error>> {
    for paths in [vec!["/notes/keep.txt"], vec!["/notes"]] {
        let fs = Fs::memory();
        let parent = fs.create_workspace("parent", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let mut transaction = parent.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        transaction.create_directory("/notes", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        transaction.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        parent.write_text("/notes/keep.txt", "base").await?;
        parent.write_text("/notes/other.txt", "other").await?;
        parent.write_text("/excluded.txt", "excluded").await?;
        let pinned = parent.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let child = parent
            .fork(
                "child",
                ForkOptions::from_generation(pinned.clone(), IdempotencyKey::new())
                    .inherit_paths(paths.iter().map(|path| (*path).to_owned()).collect()),
            crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        assert_eq!(child.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.parents(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, vec![pinned.id()]);
        assert_eq!(child.read("/notes/keep.txt", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"base");
        assert!(child.read("/excluded.txt", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).is_err());
        child.write_text("/notes/keep.txt", "child").await?;
        parent.write_text("/excluded.txt", "updated").await?;
        let source = child.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let target = parent.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let plan = child.join_into(&parent).plan_at(&source, &target, WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        assert!(matches!(
            plan.apply(ApplyOptions {
                if_target: target.id(),
                idempotency_key: IdempotencyKey::new()
            }, crate::PublicationPermit::Unrestricted)
            .await?,
            JoinOutcome::Applied(_)
        ));
        assert_eq!(parent.read("/excluded.txt", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"updated");
        assert_eq!(
            parent.read("/notes/other.txt", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(),
            b"other"
        );
        assert_eq!(parent.read("/notes/keep.txt", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"child");
        assert_eq!(child.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), source.id());
        child.write_text("/notes/keep.txt", "child again").await?;
        parent.write_text("/notes/keep.txt", "parent again").await?;
        let plan = child.join_into(&parent).plan().await?;
        assert!(matches!(
            plan.apply(ApplyOptions {
                if_target: parent.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(),
                idempotency_key: IdempotencyKey::new()
            }, crate::PublicationPermit::Unrestricted)
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
    for mask in 0_u8..8 {
        for descendant in [false, true] {
            let fs = Fs::memory();
            let parent = fs.create_workspace("subset-parent", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
            for index in 0..3 {
                parent.write_text(&format!("/file-{index}"), "base").await?;
            }
            let pinned = parent.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
            let paths = (0..3)
                .filter(|index| mask & (1 << index) != 0)
                .map(|index| format!("/file-{index}"))
                .collect();
            let child = parent
                .fork(
                    "subset-child",
                    ForkOptions::from_generation(pinned, IdempotencyKey::new())
                        .inherit_paths(paths),
                crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
                .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
            let source = if descendant {
                child
                    .fork(
                        "subset-grandchild",
                        ForkOptions::from_generation(child.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, IdempotencyKey::new()),
                    crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
                    .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?
            } else {
                child
            };
            for index in 0..3 {
                let path = format!("/file-{index}");
                if mask & (1 << index) != 0 {
                    source.write_text(&path, "selected").await?;
                } else {
                    assert!(source.read(&path, 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).is_err());
                    parent.write_text(&path, "concurrent").await?;
                }
            }
            source.write_text("/added", "new").await?;
            let selected_head = source.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
            source.write_text("/after-pin", "excluded by pin").await?;
            let source_head = source.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
            let target = parent.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
            let plan = source
                .join_into(&parent)
                .plan_at(&selected_head, &target, WorkBudget::UNBOUNDED, &CancellationToken::new())
                .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
            assert!(matches!(
                plan.apply(ApplyOptions {
                    if_target: target.id(),
                    idempotency_key: IdempotencyKey::new()
                }, crate::PublicationPermit::Unrestricted)
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
                    parent.read(&format!("/file-{index}"), 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(),
                    expected.as_bytes()
                );
            }
            assert!(parent.read("/after-pin", 32, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).is_err());
            assert_eq!(source.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), source_head.id());
            let repeated = source
                .join_into(&parent)
                .plan_at(&selected_head, &parent.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, WorkBudget::UNBOUNDED, &CancellationToken::new())
                .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
            assert!(matches!(
                repeated
                    .apply(ApplyOptions {
                        if_target: repeated.target_head(),
                        idempotency_key: IdempotencyKey::new()
                    }, crate::PublicationPermit::Unrestricted)
                    .await?,
                JoinOutcome::NoChanges(_)
            ));
        }
    }
    // Negative control: deletion on an ordinary full fork IS an authored
    // deletion and must reach the parent. Filtering must not mean fork+clear.
    let fs = Fs::memory();
    let parent = fs.create_workspace("negative-parent", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    parent.write_text("/omitted", "base").await?;
    let child = parent
        .fork(
            "negative-child",
            ForkOptions::from_generation(parent.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, IdempotencyKey::new()),
        crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    child.remove("/omitted").await?;
    let plan = child.join_into(&parent).plan().await?;
    assert!(matches!(
        plan.apply(ApplyOptions {
            if_target: plan.target_head(),
            idempotency_key: IdempotencyKey::new()
        }, crate::PublicationPermit::Unrestricted)
        .await?,
        JoinOutcome::Applied(_)
    ));
    assert!(parent.read("/omitted", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).is_err());
    Ok(())
}

#[tokio::test]
async fn filtered_fork_rebase_preserves_local_and_parent_changes() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let parent = fs.create_workspace("filtered-rebase-parent", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    parent.write_text("/omitted", "base").await?;
    let child = parent
        .fork(
            "filtered-rebase-child",
            ForkOptions::from_generation(parent.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, IdempotencyKey::new()).inherit_paths(Vec::new()),
        crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    child.write_text("/local", "child").await?;
    parent.write_text("/omitted", "user edit").await?;
    assert!(matches!(
        child.live_rebase(IdempotencyKey::new(), 64, 64, 16).await?,
        WorkspaceRebase::Rebased(_)
    ));
    assert_eq!(child.read("/omitted", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"user edit");
    assert_eq!(child.read("/local", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"child");
    child.write_text("/local", "after rebase").await?;
    let source = child.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let plan = child.join_into(&parent).plan().await?;
    assert!(matches!(
        plan.apply(ApplyOptions {
            if_target: plan.target_head(),
            idempotency_key: IdempotencyKey::new()
        }, crate::PublicationPermit::Unrestricted)
        .await?,
        JoinOutcome::Applied(_)
    ));
    assert_eq!(parent.read("/omitted", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"user edit");
    assert_eq!(parent.read("/local", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"after rebase");
    assert_eq!(child.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), source.id());
    Ok(())
}

#[tokio::test]
async fn nested_filtered_fork_uses_its_pinned_inheritance_as_delta_baseline()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let root = fs.create_workspace("nested-root", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    root.write_text("/keep", "root").await?;
    root.write_text("/omit", "root").await?;
    let parent = root
        .fork(
            "nested-parent",
            ForkOptions::from_generation(root.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, IdempotencyKey::new()),
        crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    parent.write_text("/keep", "inherited").await?;
    let child = parent
        .fork(
            "nested-child",
            ForkOptions::from_generation(parent.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, IdempotencyKey::new())
                .inherit_paths(vec!["/keep".into()]),
        crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    // Inherited content is the child baseline; only authored child changes
    // enter a destination. An unchanged selected file is not a child edit.
    child.write_text("/new", "child").await?;
    let plan = child.join_into(&root).plan().await?;
    plan.apply(ApplyOptions {
        if_target: plan.target_head(),
        idempotency_key: IdempotencyKey::new(),
    }, crate::PublicationPermit::Unrestricted)
    .await?;
    assert_eq!(root.read("/keep", 32, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"root");
    assert_eq!(root.read("/omit", 32, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"root");
    assert_eq!(root.read("/new", 32, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"child");
    child.write_text("/keep", "edited").await?;
    let plan = child.join_into(&root).plan().await?;
    assert!(matches!(
        plan.apply(ApplyOptions {
            if_target: plan.target_head(),
            idempotency_key: IdempotencyKey::new(),
        }, crate::PublicationPermit::Unrestricted)
        .await?,
        JoinOutcome::Conflicted { .. }
    ));
    root.write_text("/keep", "inherited").await?;
    let plan = child.join_into(&root).plan().await?;
    plan.apply(ApplyOptions {
        if_target: plan.target_head(),
        idempotency_key: IdempotencyKey::new(),
    }, crate::PublicationPermit::Unrestricted)
    .await?;
    assert_eq!(root.read("/keep", 32, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"edited");
    assert_eq!(parent.read("/keep", 32, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"inherited");
    Ok(())
}
