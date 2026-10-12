use acyclic_stream::OwnedBytes;
use super::*;
use crate::model::{Lifecycle, VolumeConfig};
use crate::{CancellationToken, Fs, WorkBudget};
use std::error::Error;
#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
use std::path::Path;
use std::sync::Arc;

#[tokio::test]
async fn pinned_directory_discovery_does_not_scan_ten_thousand_retained_workspaces()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("discovered-agent", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    workspace.write_text("/visible.txt", "visible").await?;
    let pinned = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let cancellation = CancellationToken::new();
    let before = list_generation_directory(
        &workspace,
        GenerationSelector::Exact(pinned.id()),
        "/",
        None,
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await?;
    let mut retained = Vec::with_capacity(10_000);
    for index in 0..10_000 {
        let other = fs
            .create_workspace(&format!("retained-agent-{index}"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        retained.push(other.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?);
    }
    workspace
        .write_text("/late.txt", "not in pinned view")
        .await?;
    let after = list_generation_directory(
        &workspace,
        GenerationSelector::Exact(pinned.id()),
        "/",
        None,
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await?;
    assert_eq!(retained.len(), 10_000);
    assert_eq!(before.value, after.value);
    assert_eq!(after.value.entries.len(), 1);
    assert!(after.work.backend_read_operations <= before.work.backend_read_operations);
    assert!(after.work.page_reads <= before.work.page_reads);
    assert_eq!(after.work.source_entries_visited, 0);
    assert_eq!(after.work.materializations, 0);
    Ok(())
}

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
            ForkOptions::from_generation(pinned.clone(), key).empty(),
        crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let initial = child.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert_eq!(initial.parents(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, vec![pinned.id()]);
    assert!(child.list_directory("/", None, 1, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.entries.is_empty());
    let retried = parent
        .fork(
            "scratch-child",
            ForkOptions::from_generation(pinned.clone(), key).empty(),
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
    let plan = child.join_into(&parent).plan_at(&source, &target).await?;
    let options = ApplyOptions {
        if_target: target.id(),
        idempotency_key: IdempotencyKey::new(),
    };
    assert!(matches!(
        plan.apply(options).await?,
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
        let plan = child.join_into(&parent).plan_at(&source, &target).await?;
        assert!(matches!(
            plan.apply(ApplyOptions {
                if_target: target.id(),
                idempotency_key: IdempotencyKey::new()
            })
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
            })
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
                .plan_at(&selected_head, &target)
                .await?;
            assert!(matches!(
                plan.apply(ApplyOptions {
                    if_target: target.id(),
                    idempotency_key: IdempotencyKey::new()
                })
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
                .plan_at(&selected_head, &parent.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?)
                .await?;
            assert!(matches!(
                repeated
                    .apply(ApplyOptions {
                        if_target: repeated.target_head(),
                        idempotency_key: IdempotencyKey::new()
                    })
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
        })
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
            ForkOptions::from_generation(parent.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, IdempotencyKey::new()).empty(),
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
        })
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
    })
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
        })
        .await?,
        JoinOutcome::Conflicted { .. }
    ));
    root.write_text("/keep", "inherited").await?;
    let plan = child.join_into(&root).plan().await?;
    plan.apply(ApplyOptions {
        if_target: plan.target_head(),
        idempotency_key: IdempotencyKey::new(),
    })
    .await?;
    assert_eq!(root.read("/keep", 32, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"edited");
    assert_eq!(parent.read("/keep", 32, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"inherited");
    Ok(())
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(48))]
    #[test]
    fn selected_fork_hardlink_aliases_keep_exact_content(bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 1..64), carry_alias in proptest::prelude::any::<bool>()) {
        let result: Result<(), Box<dyn Error>> = futures::executor::block_on(async {
            let fs = Fs::memory();
            let parent = fs.create_workspace("hardlink-parent", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
            parent.write("/file", bytes::Bytes::copy_from_slice(&bytes)).await?;
            let mut transaction = parent.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
            transaction.hard_link("/file", "/alias", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
            transaction.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
            let mut paths = vec!["/file".to_owned()];
            if carry_alias { paths.push("/alias".to_owned()); }
            let child = parent.fork("hardlink-child", ForkOptions::from_generation(parent.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, IdempotencyKey::new()).inherit_paths(paths), crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED }))).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
            assert_eq!(child.read("/file", 64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), bytes.as_slice());
            if carry_alias { assert_eq!(child.read("/alias", 64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), bytes.as_slice()); }
            else { assert!(child.read("/alias", 64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).is_err()); }
            child.write("/file", bytes::Bytes::from_static(b"changed")).await?;
            if carry_alias { assert_eq!(child.read("/alias", 64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"changed"); }
            assert_eq!(parent.read("/alias", 64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), bytes.as_slice());
            Ok(())
        });
        proptest::prop_assert!(result.is_ok(), "{result:?}");
    }
}

#[test]
fn names_are_canonical_bounded_and_path_independent() -> Result<(), Box<dyn Error>> {
    let composed = WorkspaceName::new("caf\u{e9}", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let decomposed = WorkspaceName::new("cafe\u{301}", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert_eq!(composed, decomposed);
    assert_eq!(composed.as_str(), "caf\u{e9}");
    assert!(matches!(WorkspaceName::new("", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).map(|receipt| receipt.value).map_err(|failure| failure.error), Err(WorkspaceError::Name(WorkspaceNameError::Empty))));
    assert!(matches!(WorkspaceName::new("..", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).map(|receipt| receipt.value).map_err(|failure| failure.error), Err(WorkspaceError::Name(WorkspaceNameError::Reserved))));
    assert!(matches!(
        WorkspaceName::new("parent/child", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).map(|receipt| receipt.value).map_err(|failure| failure.error),
        Err(WorkspaceError::Name(WorkspaceNameError::InvalidCharacter))
    ));
    Ok(())
}

#[test]
fn workspace_names_preserve_nfc_and_cancellation()
-> Result<(), Box<dyn Error>> {
    use unicode_normalization::UnicodeNormalization;
    let cancellation = CancellationToken::new();
    for spelling in [
        "cafe\u{301}", "\u{344}", "\u{1100}\u{1161}\u{11a8}",
        "\u{9c7}\u{9be}", "\u{315}\u{300}A", "A\u{30a}\u{301}",
        "a\u{315}\u{300}", "a\u{301}\u{300}",
    ] {
        let expected: String = spelling.nfc().collect();
        let normalized = WorkspaceName::new(
            spelling, WorkBudget::UNBOUNDED, &cancellation,
        )?;
        assert_eq!(normalized.value.as_str(), expected);
        assert!(normalized.work.peak_allocation_bytes >= normalized.value.retained_bytes());
    }
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let failure = WorkspaceName::new("name", WorkBudget::UNBOUNDED, &cancelled)
        .err().ok_or("cancelled canonical name unexpectedly admitted")?;
    assert!(matches!(failure.error, WorkspaceError::Cancelled(_)));
    assert_eq!(*failure.work, crate::WorkCounters::UNCHARGED);
    Ok(())
}

#[test]
fn identity_is_deterministic_and_namespace_scoped() -> Result<(), Box<dyn Error>> {
    let name = WorkspaceName::new("repo", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let first = WorkspaceId::derive([1; 16], &name, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert_eq!(first, WorkspaceId::derive([1; 16], &name, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).map(|receipt| receipt.value).map_err(|failure| failure.error)?);
    assert_ne!(first, WorkspaceId::derive([2; 16], &name, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).map(|receipt| receipt.value).map_err(|failure| failure.error)?);
    assert_ne!(
        first,
        WorkspaceId::derive([1; 16], &WorkspaceName::new("other", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).map(|receipt| receipt.value).map_err(|failure| failure.error)?, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).map(|receipt| receipt.value).map_err(|failure| failure.error)?
    );
    Ok(())
}

#[tokio::test]
async fn named_workspace_opens_and_forks_one_exact_generation() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = fs.create_workspace("repo", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let base = main.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let fork_key = IdempotencyKey::from_bytes([0x42; 16]);
    let fork = main
        .fork(
            "agent-42",
            ForkOptions::from_generation(base.clone(), fork_key),
        crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let retried = main
        .fork(
            "agent-42",
            ForkOptions::from_generation(base.clone(), fork_key),
        crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert_eq!(fs.open_workspace("repo", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), main.id());
    assert_ne!(fork.id(), main.id());
    assert_ne!(fork.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), base.id());
    assert_eq!(retried.id(), fork.id());
    assert_eq!(retried.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), fork.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id());
    assert!(
        main.fork(
            "agent-42",
            ForkOptions::from_generation(base, IdempotencyKey::from_bytes([0x43; 16])),
        crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)
        .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn string_workspace_paths_match_native_namespace_names() -> Result<(), Box<dyn Error>> {
    for profile in [
        crate::model::FilesystemProfile::Posix,
        crate::model::FilesystemProfile::Windows,
    ] {
        let fs = Fs::memory();
        let config = VolumeConfig {
            profile,
            ..VolumeConfig::portable(Lifecycle::Ephemeral)
        };
        let workspace = fs
            .create_workspace_with_config("native-path", config, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        workspace.write_text("/ÃƒÂ©.txt", "native").await?;
        let mut checkout = workspace
            .checkout(GenerationSelector::Head, CheckoutMode::read_only_pinned(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let path = NamespacePath::from_portable_in_profile(
            &PortablePath::parse("/ÃƒÂ©.txt", config.limits)?,
            profile,
            config.limits,
        )?;
        assert!(
            checkout
                .lookup_no_follow(&path, WorkBudget::UNBOUNDED, &CancellationToken::new())
                .await?
                .value
                .record
                .is_some()
        );
    }
    Ok(())
}

#[tokio::test]
async fn conditional_remove_preserves_typed_stale_identity() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("conditional-remove", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let mut create = workspace.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    create.write_text("/file.txt", "current").await?;
    assert!(matches!(
        create.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        TransactionCommit::Committed(_)
    ));
    let actual = workspace.stat("/file.txt", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.file_id;
    let mut stale = FileId::new();
    while stale == actual {
        stale = FileId::new();
    }
    let mut remove = workspace.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert!(matches!(
        remove.remove_if("/file.txt", stale).await,
        Err(WorkspaceError::StaleIdentity)
    ));
    assert_eq!(workspace.read("/file.txt", 64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"current");
    Ok(())
}

#[tokio::test]
async fn exact_generation_restore_is_fenced_and_idempotent() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("restore-exact", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    workspace.write_text("/state", "first").await?;
    let first = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    workspace.write_text("/state", "second").await?;
    let second = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let key = IdempotencyKey::from_bytes([0x51; 16]);

    assert!(matches!(
        workspace.restore_generation(&first, second.id(), key).await?,
        WorkspaceRestore::Restored(ref generation) if generation.id() == first.id()
    ));
    assert!(matches!(
        workspace.restore_generation(&first, second.id(), key).await?,
        WorkspaceRestore::AlreadyRestored(ref generation) if generation.id() == first.id()
    ));
    assert!(matches!(
        workspace
            .restore_generation(&second, first.id(), key)
            .await?,
        WorkspaceRestore::IdempotencyConflict
    ));
    assert!(matches!(
        workspace
            .restore_generation(&second, second.id(), IdempotencyKey::new())
            .await?,
        WorkspaceRestore::Stale(ref generation) if generation.id() == first.id()
    ));
    assert_eq!(workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), first.id());
    Ok(())
}

#[tokio::test]
async fn path_restore_reuses_exact_records_and_preserves_unselected_state()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("restore-paths", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let mut transaction = workspace.begin_transaction(IdempotencyKey::from_bytes([0x60; 16]), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction.create_dir_all("/tree", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction
        .create_file(
            "/tree/tracked",
            bytes::Bytes::from_static(b"committed"),
            FileMetadata::default(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction
        .hard_link("/tree/tracked", "/tree/ignored-link", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert!(matches!(
        transaction.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        TransactionCommit::Committed(_)
    ));
    workspace.write_text("/ignored", "live").await?;
    let committed = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let snapshot = workspace
        .fork(
            "restore-paths-snapshot",
            ForkOptions::from_generation(committed, IdempotencyKey::from_bytes([0x61; 16])),
        crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    snapshot.remove("/ignored").await?;
    let source = snapshot.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;

    workspace.write_text("/tree/tracked", "dirty").await?;
    workspace.write_text("/ignored", "still-live").await?;
    let dirty = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let outcome = workspace
        .restore_paths_from(
            &source,
            &["tree/tracked".to_owned()],
            dirty.id(),
            IdempotencyKey::from_bytes([0x62; 16]), crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::FsError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) },
        )
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert!(matches!(outcome, TransactionCommit::Committed(_)));
    assert_eq!(
        workspace.read("/tree/tracked", 64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"committed")
    );
    assert_eq!(
        workspace.read("/tree/ignored-link", 64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"committed")
    );
    assert_eq!(
        workspace.read("/ignored", 64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"still-live")
    );
    let tracked = workspace.stat("/tree/tracked", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let linked = workspace.stat("/tree/ignored-link", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert_eq!(tracked.file_id, linked.file_id);
    assert_eq!(tracked.link_count, 2);
    Ok(())
}

#[tokio::test]
async fn path_apply_is_three_way_conflict_checked() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("apply-paths", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    workspace.write_text("/file", "base").await?;
    let base = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let source_workspace = workspace
        .fork(
            "apply-paths-source",
            ForkOptions::from_generation(base.clone(), IdempotencyKey::from_bytes([0x63; 16])),
        crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    source_workspace.write_text("/file", "source").await?;
    let source = source_workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let current = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert!(matches!(
        workspace
            .apply_paths_from(
                Some(&base),
                Some(&source),
                &["file".to_owned()],
                current.id(),
                IdempotencyKey::from_bytes([0x64; 16]),
            )
            .await?,
        WorkspacePathApply::Applied(_)
    ));
    assert_eq!(
        workspace.read("/file", 64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"source")
    );

    let conflicting = fs.create_workspace("apply-paths-conflict", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    conflicting.write_text("/file", "base").await?;
    let conflict_base = conflicting.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let conflict_source = conflicting
        .fork(
            "apply-paths-conflict-source",
            ForkOptions::from_generation(
                conflict_base.clone(),
                IdempotencyKey::from_bytes([0x65; 16]),
            ),
        crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    conflict_source.write_text("/file", "source").await?;
    let conflict_source = conflict_source.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    conflicting.write_text("/file", "ours").await?;
    let conflict_current = conflicting.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let WorkspacePathApply::Conflicted(conflicts) = conflicting
        .apply_paths_from(
            Some(&conflict_base),
            Some(&conflict_source),
            &["file".to_owned()],
            conflict_current.id(),
            IdempotencyKey::from_bytes([0x66; 16]),
        )
        .await?
    else {
        return Err("overlapping path application did not conflict".into());
    };
    assert_eq!(conflicts.len(), 1);
    assert_eq!(conflicts[0].path, "file");
    assert_eq!(conflicts[0].kind, ConflictKind::Binary);
    Ok(())
}

#[tokio::test]
async fn repeated_join_preserves_prior_incremental_publications() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let parent = fs.create_workspace("incremental-parent", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let parent_base = parent.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let child = parent
        .fork(
            "incremental-child",
            ForkOptions::from_generation(
                parent_base.clone(),
                IdempotencyKey::from_bytes([0x67; 16]),
            ),
        crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    child.write_text("/first.txt", "first").await?;
    let first = child.join_into(&parent).plan().await?;
    assert!(matches!(
        first
            .apply(ApplyOptions {
                if_target: first.target_head(),
                idempotency_key: IdempotencyKey::from_bytes([0x68; 16]),
            })
            .await?,
        JoinOutcome::Applied(_)
    ));
    child.write_text("/second.txt", "second").await?;
    let second = child.join_into(&parent).plan().await?;
    assert!(matches!(
        second
            .apply(ApplyOptions {
                if_target: second.target_head(),
                idempotency_key: IdempotencyKey::from_bytes([0x69; 16]),
            })
            .await?,
        JoinOutcome::Applied(_)
    ));
    assert_eq!(parent.read("/first.txt", 64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"first");
    assert_eq!(parent.read("/second.txt", 64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"second");
    Ok(())
}

#[tokio::test]
async fn public_workspace_checkout_opens_pinned_and_live_private_views()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("public-checkout", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let head = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let pinned = workspace.checkout(GenerationSelector::Head, CheckoutMode::read_only_pinned(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert_eq!(pinned.generation_id(), head.id());
    let live_private = workspace.checkout(GenerationSelector::Head, CheckoutMode {
                access: crate::model::AccessMode::ReadWrite,
                consistency: crate::model::ConsistencyMode::Live,
                mutations: crate::model::MutationMode::PrivateOverlay,
            }, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert_eq!(live_private.generation_id(), head.id());
    Ok(())
}

#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
#[tokio::test]
async fn public_generation_materialize_path_is_a_complete_consumer_flow()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("materialize-consumer", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let mut transaction = workspace.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction.create_dir_all("/output/nested", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction
        .write_text("/output/nested/status", "ready")
        .await?;
    #[cfg(unix)]
    transaction
        .set_metadata(
            "/output/nested",
            FileMetadata {
                posix_mode: crate::kernel::MetadataField::Value(0o700),
                ..FileMetadata::default()
            }, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let TransactionCommit::Committed(generation) = transaction.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)? else {
        return Err("materialize fixture did not commit".into());
    };
    let destination = tempfile::tempdir()?;
    let receipt = generation
        .materialize(
            std::iter::once("/output/nested/status"),
            &crate::MaterializeOptions {
                destination: destination.path().to_path_buf(),
                expected_root_identity: None,
                maximum_directory_entries: 32,
                maximum_extent_spans: 32,
                transfer_bytes: 4096,
            },
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?;
    assert_eq!(receipt.value.files, 1);
    assert_eq!(
        std::fs::read(destination.path().join("output/nested/status"))?,
        b"ready"
    );
    let directory_destination = tempfile::tempdir()?;
    generation
        .materialize(
            std::iter::once("/output/nested"),
            &crate::MaterializeOptions {
                destination: directory_destination.path().to_path_buf(),
                expected_root_identity: None,
                maximum_directory_entries: 32,
                maximum_extent_spans: 32,
                transfer_bytes: 4096,
            },
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?;
    assert_eq!(
        std::fs::read(directory_destination.path().join("output/nested/status"))?,
        b"ready"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(directory_destination.path().join("output/nested"))?
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o700);
    }
    Ok(())
}

#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
#[tokio::test]
async fn public_generation_materialize_paths_shares_parents_and_hard_link_identity()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("materialize-paths-consumer", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let mut transaction = workspace.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction.create_dir_all("/nested", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction.write_text("/nested/a", "linked").await?;
    transaction.hard_link("/nested/a", "/nested/b", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction.write_text("/nested/c", "sibling").await?;
    let TransactionCommit::Committed(generation) = transaction.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)? else {
        return Err("materialize fixture did not commit".into());
    };
    let destination = tempfile::tempdir()?;
    let receipt = generation
        .materialize(
            ["/nested/a", "/nested/b", "/nested/c"].into_iter(),
            &crate::MaterializeOptions::native(destination.path()),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?;
    assert_eq!(receipt.value.files, 3);
    assert_eq!(
        std::fs::read(destination.path().join("nested/c"))?,
        b"sibling"
    );
    std::fs::write(destination.path().join("nested/a"), b"changed")?;
    assert_eq!(
        std::fs::read(destination.path().join("nested/b"))?,
        b"changed"
    );
    Ok(())
}

#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
#[tokio::test]
async fn public_generation_restore_host_path_replaces_and_removes_exactly_one_path()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("restore-host-consumer", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let mut transaction = workspace.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction.create_dir_all("/nested", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction
        .write_text("/nested/file", "authenticated")
        .await?;
    let TransactionCommit::Committed(generation) = transaction.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)? else {
        return Err("restore fixture did not commit".into());
    };
    let destination = tempfile::tempdir()?;
    std::fs::create_dir(destination.path().join("nested"))?;
    std::fs::write(destination.path().join("nested/file"), b"stale")?;
    std::fs::write(destination.path().join("sibling"), b"untouched")?;
    let options = crate::MaterializeOptions {
        destination: destination.path().to_path_buf(),
        expected_root_identity: None,
        maximum_directory_entries: 32,
        maximum_extent_spans: 32,
        transfer_bytes: 4096,
    };
    let restored = generation
        .restore_host_path(
            Path::new("nested/file"),
            crate::HostPathReplacement::Atomic,
            &options,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?;
    assert_eq!(restored.value, crate::HostPathRestore::Restored);
    assert_eq!(
        std::fs::read(destination.path().join("nested/file"))?,
        b"authenticated"
    );
    assert_eq!(
        std::fs::read(destination.path().join("sibling"))?,
        b"untouched"
    );

    std::fs::write(destination.path().join("missing"), b"remove me")?;
    let removed = generation
        .restore_host_path(
            Path::new("missing"),
            crate::HostPathReplacement::Atomic,
            &options,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?;
    assert_eq!(removed.value, crate::HostPathRestore::Removed);
    assert!(!destination.path().join("missing").exists());
    for invalid in [Path::new(""), Path::new("../escape"), destination.path()] {
        assert!(
            generation
                .restore_host_path(
                    invalid,
                    crate::HostPathReplacement::Atomic,
                    &options,
                    crate::WorkBudget::UNBOUNDED,
                    &crate::CancellationToken::new(),
                )
                .await
                .is_err()
        );
    }
    Ok(())
}

#[cfg(all(feature = "native-mount", not(target_arch = "wasm32"), unix))]
#[tokio::test]
async fn public_generation_restore_host_path_rejects_symlinked_parent() -> Result<(), Box<dyn Error>>
{
    let fs = Fs::memory();
    let workspace = fs.create_workspace("restore-host-containment", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let generation = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let destination = tempfile::tempdir()?;
    let outside = tempfile::tempdir()?;
    std::os::unix::fs::symlink(outside.path(), destination.path().join("escape"))?;
    let result = generation
        .restore_host_path(
            Path::new("escape/file"),
            crate::HostPathReplacement::Atomic,
            &crate::MaterializeOptions {
                destination: destination.path().to_path_buf(),
                expected_root_identity: None,
                maximum_directory_entries: 32,
                maximum_extent_spans: 32,
                transfer_bytes: 4096,
            },
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await;
    assert!(result.is_err());
    assert!(!outside.path().join("file").exists());
    Ok(())
}

#[cfg(all(feature = "native-mount", windows))]
#[tokio::test]
async fn public_generation_restore_host_path_rejects_reparse_parent_when_available()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace("restore-host-containment-windows", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let generation = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let destination = tempfile::tempdir()?;
    let outside = tempfile::tempdir()?;
    if let Err(error) =
        std::os::windows::fs::symlink_dir(outside.path(), destination.path().join("escape"))
    {
        if error.kind() == std::io::ErrorKind::PermissionDenied {
            return Ok(());
        }
        return Err(error.into());
    }
    let result = generation
        .restore_host_path(
            Path::new("escape/file"),
            crate::HostPathReplacement::Atomic,
            &crate::MaterializeOptions {
                destination: destination.path().to_path_buf(),
                expected_root_identity: None,
                maximum_directory_entries: 32,
                maximum_extent_spans: 32,
                transfer_bytes: 4096,
            },
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await;
    assert!(result.is_err());
    assert!(!outside.path().join("file").exists());
    Ok(())
}

#[tokio::test]
async fn workspace_forks_an_unpublished_checkpoint_without_mutating_its_source()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let source = fs.create_workspace("checkpoint-source", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let mut transaction = source.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction
        .write_text("/candidate-only", "candidate")
        .await?;
    let checkpoint = transaction
        .checkout
        .checkpoint(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    let unpublished = source.generation(checkpoint, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let child = source
        .fork(
            "checkpoint-child",
            ForkOptions::from_generation(unpublished, IdempotencyKey::from_bytes([0x44; 16])),
        crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;

    assert_eq!(
        child.read("/candidate-only", 64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"candidate")
    );
    assert!(source.read("/candidate-only", 64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).is_err());
    Ok(())
}

#[tokio::test]
async fn transaction_is_atomic_idempotent_and_visible_to_forks() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("repo", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let key = IdempotencyKey::new();
    let mut transaction = workspace.begin_transaction(key, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction.create_dir_all("/output/nested", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction
        .write_text("/output/nested/status", "ready")
        .await?;
    let TransactionCommit::Committed(generation) = transaction.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)? else {
        return Err("first publication did not commit".into());
    };
    assert_eq!(
        workspace.read("/output/nested/status", 64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"ready")
    );
    let reopened = workspace.generation(generation.id(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert_eq!(
        reopened.read("/output/nested/status", 64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"ready")
    );
    let fork = workspace
        .fork(
            "copy",
            ForkOptions::from_generation(generation.clone(), IdempotencyKey::new()),
        crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert_eq!(
        fork.read("/output/nested/status", 64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"ready")
    );

    let mut exact_retry = workspace.begin_transaction(key, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    exact_retry.create_dir_all("/output/nested", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    exact_retry
        .write_text("/output/nested/status", "ready")
        .await?;
    assert!(matches!(
        exact_retry.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        TransactionCommit::AlreadyCommitted(retried) if retried.id() == generation.id()
    ));

    let mut conflicting_retry = workspace.begin_transaction(key, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    conflicting_retry
        .write_text("/output/nested/status", "different")
        .await?;
    assert!(matches!(
        conflicting_retry.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        TransactionCommit::IdempotencyConflict
    ));
    Ok(())
}

#[tokio::test]
async fn retained_transaction_rebases_disjoint_work_and_rejects_overlap()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("transaction-rebase", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;

    let mut first = workspace.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let mut disjoint = workspace.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    first.write_text("/first", "one").await?;
    disjoint.write_text("/second", "two").await?;
    assert!(matches!(
        first.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        TransactionCommit::Committed(_)
    ));
    assert!(matches!(
        disjoint.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        TransactionCommit::Conflict { .. }
    ));
    assert!(matches!(
        disjoint.rebase(16).await?,
        TransactionRebase::Rebased(_)
    ));
    assert!(matches!(
        disjoint.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        TransactionCommit::Committed(_)
    ));
    assert_eq!(
        workspace.read("/first", 8, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"one")
    );
    assert_eq!(
        workspace.read("/second", 8, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"two")
    );

    let mut winner = workspace.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let mut overlap = workspace.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    winner.write_text("/first", "winner").await?;
    overlap.write_text("/first", "loser").await?;
    assert!(matches!(
        winner.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        TransactionCommit::Committed(_)
    ));
    assert!(matches!(
        overlap.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        TransactionCommit::Conflict { .. }
    ));
    let TransactionRebase::Conflicted {
        conflicts,
        truncated,
    } = overlap.rebase(16).await?
    else {
        return Err("overlapping mutation rebased unsafely".into());
    };
    assert!(!conflicts.is_empty());
    assert!(!truncated);
    assert_eq!(
        workspace.read("/first", 8, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"winner")
    );
    Ok(())
}

#[tokio::test]
async fn exact_generation_transaction_is_stateless_and_rebases_safely() -> Result<(), Box<dyn Error>>
{
    let fs = Fs::memory();
    let workspace = fs.create_workspace("exact-transaction", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let base = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;

    workspace.write_text("/upstream", "one").await?;
    let mut transaction = workspace
        .begin_transaction_at(&base, IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction.write_text("/local", "two").await?;
    assert!(matches!(
        transaction.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        TransactionCommit::Conflict { .. }
    ));
    assert!(matches!(
        transaction.rebase(16).await?,
        TransactionRebase::Rebased(_)
    ));
    assert!(matches!(
        transaction.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        TransactionCommit::Committed(_)
    ));
    assert_eq!(
        workspace.read("/upstream", 8, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"one")
    );
    assert_eq!(
        workspace.read("/local", 8, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"two")
    );

    let other = fs.create_workspace("other-exact-transaction", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let foreign = other.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert!(matches!(
        workspace
            .begin_transaction_at(&foreign, IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error),
        Err(WorkspaceError::ForeignGeneration)
    ));
    Ok(())
}

#[tokio::test]
async fn transaction_preserves_links_sparse_ranges_metadata_and_cow_clones()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("shapes", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let mut transaction = workspace.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction.create_directory("/tree", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction
        .write("/tree/source", bytes::Bytes::from_static(b"abcdef"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction
        .write("/tree/destination", bytes::Bytes::from_static(b"......"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction
        .create_symbolic_link("/tree/symlink", bytes::Bytes::from_static(b"source"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction
        .hard_link("/tree/source", "/tree/hard-link", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction
        .write_range("/tree/source", 1, bytes::Bytes::from_static(b"Z"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction
        .zero_range(
            "/tree/source",
            crate::ByteRange {
                offset: 2,
                length: 2,
            },
            false,
            false,
        )
        .await?;
    transaction
        .preallocate(
            "/tree/source",
            crate::ByteRange {
                offset: 0,
                length: 6,
            },
            true,
        )
        .await?;
    transaction
        .clone_range("/tree/source", 0, "/tree/destination", 1, 5)
        .await?;
    transaction
        .set_metadata("/tree/source", FileMetadata::default(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction.resize("/tree/destination", 8).await?;
    assert!(matches!(
        transaction.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        TransactionCommit::Committed(_)
    ));
    let source = OwnedBytes::from_static(b"aZ\0\0ef");
    assert_eq!(workspace.read("/tree/source", 32, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, source);
    assert_eq!(workspace.read("/tree/hard-link", 32, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, source);
    assert_eq!(
        workspace.read("/tree/destination", 32, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b".aZ\0\0e\0\0")
    );
    Ok(())
}

#[tokio::test]
async fn customer_reads_are_bounded_sparse_link_aware_and_generation_exact()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("read-shapes", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let mut transaction = workspace.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction.create_directory("/tree", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction
        .write("/tree/a", bytes::Bytes::from_static(b"abcdef"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction
        .write("/tree/sparse", bytes::Bytes::from(vec![1; 300]), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction.hard_link("/tree/a", "/tree/b", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction
        .create_symbolic_link("/tree/link", bytes::Bytes::from_static(b"a"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction
        .zero_range(
            "/tree/a",
            crate::ByteRange {
                offset: 2,
                length: 2,
            },
            false,
            false,
        )
        .await?;
    transaction.resize("/tree/a", 8).await?;
    transaction
        .zero_range(
            "/tree/sparse",
            crate::ByteRange {
                offset: 100,
                length: 20,
            },
            true,
            false,
        )
        .await?;
    transaction.resize("/tree/sparse", 400).await?;
    let TransactionCommit::Committed(exact) = transaction.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)? else {
        return Err("shape publication did not commit".into());
    };

    let file_id = workspace.stat("/tree/a", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.file_id;
    let directory_id = workspace.stat("/tree", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.file_id;
    let missing_id = FileId::from_bytes([0xff; 16]);
    let paths = exact
        .paths_for_file_ids([file_id, directory_id, missing_id], 32)
        .await?;
    assert_eq!(
        paths.get(&file_id),
        Some(&vec!["/tree/a".to_owned(), "/tree/b".to_owned()])
    );
    assert_eq!(paths.get(&directory_id), Some(&vec!["/tree".to_owned()]));
    assert!(!paths.contains_key(&missing_id));

    assert_eq!(
        workspace.read_range("/tree/a", 1, 5, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).map(|chunk| chunk.bytes)?,
        OwnedBytes::from_static(b"b\0\0ef")
    );
    let a = workspace.stat("/tree/a", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let b = workspace.stat("/tree/b", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert_eq!(a.file_id, b.file_id);
    assert_eq!(a.link_count, 2);
    assert_eq!(a.logical_bytes, Some(8));
    assert_eq!(a.kind, FileKind::Regular);
    assert_eq!(
        workspace.read_symbolic_link("/tree/link", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).map(|chunk| chunk.bytes)?,
        OwnedBytes::from_static(b"a")
    );

    let listing = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let first = listing.list_directory("/tree", None, 1, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert_eq!(first.entries.len(), 1);
    assert!(first.has_more);
    workspace.write_text("/tree/late", "new").await?;
    let second = listing.list_directory("/tree", Some(&first.entries[0].name), 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert_eq!(second.entries.len(), 3);
    assert!(!second.has_more);

    let extents = workspace.plan_extents("/tree/sparse", 0, 400, 16).await?;
    assert!(
        extents
            .spans
            .iter()
            .any(|span| span.kind == WorkspaceExtentKind::AllocatedZero)
    );
    assert!(
        extents
            .spans
            .iter()
            .any(|span| span.kind == WorkspaceExtentKind::Content)
    );

    workspace.write_text("/tree/a", "changed").await?;
    assert_eq!(
        exact.read("/tree/a", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"ab\0\0ef\0\0")
    );
    assert_eq!(
        workspace.read("/tree/a", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"changed")
    );
    Ok(())
}

#[tokio::test]
async fn checkpoint_retains_the_exact_current_generation() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("repo", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let checkpoint = workspace.checkpoint("baseline").await?;
    assert_eq!(checkpoint.0.as_str(), "baseline");
    assert_eq!(checkpoint.1.id(), workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id());
    let retried = workspace.checkpoint("baseline").await?;
    assert_eq!(retried.1.id(), checkpoint.1.id());
    Ok(())
}

#[tokio::test]
async fn explicit_pin_is_exact_and_conflicting_reuse_fails_closed() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("repo", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let first = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let pin = first.pin("deployment", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert_eq!(pin.0.as_str(), "deployment");
    assert_eq!(pin.1.id(), first.id());
    assert_eq!(first.pin("deployment", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.1.id(), first.id());

    let mut transaction = workspace.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction.write_text("/changed", "yes").await?;
    let TransactionCommit::Committed(second) = transaction.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)? else {
        return Err("second generation did not commit".into());
    };
    assert!(matches!(
        second.pin("deployment", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error),
        Err(WorkspaceError::RetentionConflict)
    ));
    Ok(())
}

#[tokio::test]
async fn deletion_is_terminal_and_does_not_invalidate_a_fork() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("repo", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let generation = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let fork = workspace
        .fork(
            "survivor",
            ForkOptions::from_generation(generation, IdempotencyKey::new()),
        crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let key = IdempotencyKey::new();
    assert_eq!(workspace.delete(key).await?, WorkspaceDelete::Deleted);
    assert_eq!(
        workspace.delete(key).await?,
        WorkspaceDelete::AlreadyDeleted
    );
    assert_eq!(
        fs.delete_workspace("repo", key).await?,
        WorkspaceDelete::AlreadyDeleted
    );
    assert!(fs.open_workspace("repo", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).is_err());
    assert!(fs.create_workspace("repo", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).is_err());
    assert_eq!(fork.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.workspace_id(), fork.id());
    Ok(())
}

#[tokio::test]
async fn side_effect_free_join_combines_independent_fork_and_target_changes()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = fs.create_workspace("main", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    main.write_text("/base", "base").await?;
    let base = main.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let agent = main
        .fork(
            "agent",
            ForkOptions::from_generation(base, IdempotencyKey::new()),
        crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    agent.write_text("/agent", "agent").await?;
    main.write_text("/main", "main").await?;

    let plan = agent.join_into(&main).plan().await?;
    let expected = plan.target_head();
    let idempotency_key = IdempotencyKey::new();
    assert!(main.read("/agent", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).is_err());
    let outcome = plan
        .apply(ApplyOptions {
            if_target: expected,
            idempotency_key,
        })
        .await?;
    let JoinOutcome::Applied(joined) = outcome else {
        return Err("join did not publish".into());
    };
    let witness = plan.commit_witness(&joined, idempotency_key)?;
    assert!(main.verify_join_commit(&witness).await?);
    let mut forged = serde_json::to_value(witness)?;
    forged["source_generation"] =
        serde_json::to_value(crate::GenerationId::new(crate::Digest::ZERO))?;
    let forged: crate::JoinCommitWitness = serde_json::from_value(forged)?;
    assert!(!main.verify_join_commit(&forged).await?);
    assert_eq!(
        main.operation_generation(idempotency_key, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?
            .ok_or("join operation was not recoverable")?
            .id(),
        joined.id()
    );
    assert!(
        main.operation_generation(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?
            .is_none()
    );
    assert_eq!(joined.read("/base", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, OwnedBytes::from_static(b"base"));
    assert_eq!(main.read("/agent", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, OwnedBytes::from_static(b"agent"));
    assert_eq!(main.read("/main", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, OwnedBytes::from_static(b"main"));
    assert_eq!(
        agent.read("/agent", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"agent")
    );
    assert!(matches!(
        plan.apply(ApplyOptions {
            if_target: expected,
            idempotency_key,
        })
        .await?,
        JoinOutcome::AlreadyApplied(_)
    ));
    main.write_text("/after-join", "later").await?;
    assert!(main.verify_join_commit(&witness).await?);
    let ordinary_key = IdempotencyKey::new();
    let mut ordinary = main.begin_transaction(ordinary_key, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    ordinary.write_text("/ordinary", "not a join").await?;
    assert!(matches!(
        ordinary.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        TransactionCommit::Committed(_)
    ));
    let ordinary_generation = main.operation_generation(ordinary_key, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?
        .ok_or("ordinary operation was not recoverable")?;
    let mut impostor = serde_json::to_value(witness)?;
    impostor["operation_id"] = serde_json::to_value(ordinary_key.operation_id())?;
    impostor["result_generation"] = serde_json::to_value(ordinary_generation.id())?;
    let impostor: crate::JoinCommitWitness = serde_json::from_value(impostor)?;
    assert!(!main.verify_join_commit(&impostor).await?);
    Ok(())
}

#[tokio::test]
async fn exact_generation_join_matches_head_plan_and_rejects_stale_target()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = fs.create_workspace("main-exact", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    main.write_text("/base", "base").await?;
    let base = main.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let agent = main
        .fork(
            "agent-exact",
            ForkOptions::from_generation(base, IdempotencyKey::new()),
        crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    agent.write_text("/agent", "agent").await?;
    let source = agent.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let target = main.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let planned = agent.join_into(&main).plan_at(&source, &target).await?;
    assert_eq!(planned.source_head(), source.id());
    assert_eq!(planned.target_head(), target.id());
    let current = agent.join_into(&main).plan().await?;
    assert_eq!(planned.source_head(), current.source_head());
    assert_eq!(planned.target_head(), current.target_head());

    main.write_text("/advanced", "advanced").await?;
    assert!(matches!(
        agent.join_into(&main).plan_at(&source, &target).await,
        Err(WorkspaceError::StaleTarget)
    ));
    assert!(matches!(
        agent
            .join_into(&main)
            .plan_at(&target, &main.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?)
            .await,
        Err(WorkspaceError::ForeignGeneration)
    ));
    Ok(())
}

#[tokio::test]
async fn bounded_common_ancestor_discovery_is_direction_independent_at_the_frontier()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = fs.create_workspace("lineage-main", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let base = main.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let left = main
        .fork(
            "lineage-left",
            ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
        crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let right = main
        .fork(
            "lineage-right",
            ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
        crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;

    assert_eq!(
        main.join_into(&left)
            .bounds(1, 32, 32)
            .plan()
            .await?
            .common_ancestor(),
        base.id()
    );
    assert_eq!(
        left.join_into(&main)
            .bounds(1, 32, 32)
            .plan()
            .await?
            .common_ancestor(),
        base.id()
    );
    assert!(matches!(
        left.join_into(&right).bounds(1, 32, 32).plan().await,
        Err(WorkspaceError::LineageLimit)
    ));
    assert!(matches!(
        right.join_into(&left).bounds(1, 32, 32).plan().await,
        Err(WorkspaceError::LineageLimit)
    ));

    let unrelated = fs.create_workspace("lineage-unrelated", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert!(matches!(
        main.join_into(&unrelated).bounds(1, 32, 32).plan().await,
        Err(WorkspaceError::NoCommonAncestor)
    ));
    assert!(matches!(
        unrelated.join_into(&main).bounds(1, 32, 32).plan().await,
        Err(WorkspaceError::NoCommonAncestor)
    ));
    assert!(matches!(
        main.join_into(&left).bounds(0, 32, 32).plan().await,
        Err(WorkspaceError::JoinLimit)
    ));
    Ok(())
}

#[tokio::test]
async fn live_rebase_advances_fork_lineage_and_preserves_independent_changes()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = fs.create_workspace("main-rebase", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    main.write_text("/base", "base").await?;
    let agent = main
        .fork(
            "agent-rebase",
            ForkOptions::from_generation(main.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, IdempotencyKey::new()),
        crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    agent.write_text("/agent", "local").await?;
    main.write_text("/upstream", "first").await?;

    let first_key = IdempotencyKey::new();
    let WorkspaceRebase::Rebased(first) = agent.live_rebase(first_key, 128, 128, 32).await? else {
        return Err("first live rebase did not publish".into());
    };
    assert_eq!(
        first.read("/agent", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"local")
    );
    assert_eq!(
        first.read("/upstream", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"first")
    );
    assert!(matches!(
        agent.live_rebase(first_key, 128, 128, 32).await?,
        WorkspaceRebase::Current(_)
    ));

    main.write_text("/later", "second").await?;
    assert!(matches!(
        agent
            .live_rebase(IdempotencyKey::new(), 128, 128, 32)
            .await?,
        WorkspaceRebase::Rebased(_)
    ));
    assert_eq!(
        agent.read("/later", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"second")
    );
    assert_eq!(
        agent.read("/agent", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"local")
    );
    Ok(())
}

#[tokio::test]
async fn live_rebase_conflict_and_non_fork_leave_heads_unchanged() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = fs.create_workspace("main-conflict", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    main.write_text("/shared", "base").await?;
    let agent = main
        .fork(
            "agent-conflict",
            ForkOptions::from_generation(main.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, IdempotencyKey::new()),
        crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    agent.write_text("/shared", "local").await?;
    let before = agent.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id();
    main.write_text("/shared", "upstream").await?;
    assert!(matches!(
        agent
            .live_rebase(IdempotencyKey::new(), 128, 128, 32)
            .await?,
        WorkspaceRebase::Conflicted {
            truncated: false,
            ..
        }
    ));
    assert_eq!(agent.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), before);
    assert_eq!(
        agent.read("/shared", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"local")
    );
    assert!(matches!(
        main.live_rebase(IdempotencyKey::new(), 128, 128, 32).await,
        Err(WorkspaceError::NotFork)
    ));
    Ok(())
}

#[tokio::test]
async fn live_rebase_merges_disjoint_sparse_ranges_without_materializing_file()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = fs.create_workspace("main-ranges", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    main.write("/shared", bytes::Bytes::from(vec![b'a'; 128])).await?;
    let agent = main
        .fork(
            "agent-ranges",
            ForkOptions::from_generation(main.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, IdempotencyKey::new()),
        crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let mut local = agent.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    local
        .write_range("/shared", 1, bytes::Bytes::from_static(b"XY"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert!(matches!(
        local.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        TransactionCommit::Committed(_)
    ));
    let mut upstream = main.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    upstream
        .write_range("/shared", 64, bytes::Bytes::from_static(b"UV"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert!(matches!(
        upstream.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        TransactionCommit::Committed(_)
    ));
    assert!(matches!(
        agent
            .live_rebase(IdempotencyKey::new(), 128, 128, 32)
            .await?,
        WorkspaceRebase::Rebased(_)
    ));
    let merged = agent.read("/shared", 128, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert_eq!(&merged[1..3], b"XY");
    assert_eq!(&merged[64..66], b"UV");
    assert!(
        merged[..1]
            .iter()
            .chain(&merged[3..64])
            .all(|byte| *byte == b'a')
    );
    assert!(merged[66..].iter().all(|byte| *byte == b'a'));
    Ok(())
}

#[tokio::test]
async fn join_rejects_a_stale_target_without_mutation() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = fs.create_workspace("main", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let base = main.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let agent = main
        .fork(
            "agent",
            ForkOptions::from_generation(base, IdempotencyKey::new()),
        crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    agent.write_text("/agent", "agent").await?;
    let plan = agent.join_into(&main).plan().await?;
    let expected = plan.target_head();
    main.write_text("/raced", "winner").await?;
    let outcome = plan
        .apply(ApplyOptions {
            if_target: expected,
            idempotency_key: IdempotencyKey::new(),
        })
        .await?;
    assert!(matches!(outcome, JoinOutcome::StaleTarget(_)));
    assert!(main.read("/agent", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).is_err());
    assert_eq!(
        main.read("/raced", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"winner")
    );
    Ok(())
}

#[tokio::test]
async fn semantic_change_set_and_overlap_conflict_are_exact() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = fs.create_workspace("main", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    main.write_text("/shared", "base").await?;
    let base = main.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let agent = main
        .fork(
            "agent",
            ForkOptions::from_generation(base, IdempotencyKey::new()),
        crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let agent_base = agent.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    agent.write_text("/shared", "agent").await?;
    let agent_head = agent.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let changes = agent.diff(&agent_base, &agent_head, 32).await?;
    assert_eq!(changes.from().id(), agent_base.id());
    assert_eq!(changes.to().id(), agent_head.id());
    assert!(!changes.changes().files.is_empty());

    main.write_text("/shared", "main").await?;
    let plan = agent.join_into(&main).plan().await?;
    assert!(matches!(
        plan.apply(ApplyOptions {
            if_target: plan.target_head(),
            idempotency_key: IdempotencyKey::new(),
        })
        .await?,
        JoinOutcome::Conflicted {
            truncated: false,
            ..
        }
    ));
    assert_eq!(main.read("/shared", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, OwnedBytes::from_static(b"main"));
    Ok(())
}

struct SelectTheirsDriver;

impl crate::MergeDriver for SelectTheirsDriver {
    fn fingerprint(&self) -> &[u8] {
        b"workspace-select-theirs-v1"
    }

    fn mode(&self) -> crate::MergeDriverMode {
        crate::MergeDriverMode::Deterministic
    }

    fn resolve(
        &self,
        conflict: &crate::ConflictView,
    ) -> Result<crate::MergeResolution, crate::DriverError> {
        assert_eq!(conflict.kind, crate::ConflictKind::Text);
        assert_eq!(conflict.base, crate::ConflictValue::Text("base".into()));
        assert_eq!(conflict.ours, crate::ConflictValue::Text("main".into()));
        assert_eq!(conflict.theirs, crate::ConflictValue::Text("agent".into()));
        Ok(crate::MergeResolution::Select(crate::ConflictSide::Theirs))
    }
}

#[tokio::test]
async fn merge_drivers_resolve_and_publish_real_workspace_joins() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = fs.create_workspace("driver-main", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    main.write_text("/shared", "base").await?;
    let agent = main
        .fork(
            "driver-agent",
            ForkOptions::from_generation(main.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, IdempotencyKey::new()),
        crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    agent.write_text("/shared", "agent").await?;
    main.write_text("/shared", "main").await?;

    let plan = agent.join_into(&main).plan().await?;
    let mut registry = crate::MergeDriverRegistry::new();
    registry.register("theirs", Arc::new(SelectTheirsDriver))?;
    registry.set_default("theirs")?;
    let mut cache = crate::MemoryMergeResolutionCache::default();
    let join_key = IdempotencyKey::new();
    let outcome = plan
        .apply_with_drivers(
            ApplyOptions {
                if_target: plan.target_head(),
                idempotency_key: join_key,
            },
            &registry,
            &mut cache,
            false,
        )
        .await?;
    let JoinOutcome::Applied(application) = outcome else {
        return Err("resolved join did not publish".into());
    };
    assert!(
        main.verify_join_commit(&plan.commit_witness(&application, join_key)?)
            .await?
    );
    assert_eq!(
        main.read("/shared", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"agent")
    );
    Ok(())
}

#[tokio::test]
async fn default_text_driver_projects_markers_and_publishes_them() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = fs.create_workspace("text-driver-main", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    main.write_text("/shared", "base\n").await?;
    let agent = main
        .fork(
            "text-driver-agent",
            ForkOptions::from_generation(main.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, IdempotencyKey::new()),
        crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    agent.write_text("/shared", "agent\n").await?;
    main.write_text("/shared", "main\n").await?;

    let plan = agent.join_into(&main).plan().await?;
    let JoinOutcome::Conflicted {
        conflicts,
        truncated,
    } = plan
        .apply(ApplyOptions {
            if_target: plan.target_head(),
            idempotency_key: IdempotencyKey::new(),
        })
        .await?
    else {
        return Err("text join did not expose its conflict".into());
    };
    let described = plan.describe_conflicts(&conflicts, truncated).await?;
    assert_eq!(described.conflicts.len(), 1);
    assert_eq!(described.conflicts[0].path.as_deref(), Some("/shared"));
    assert_eq!(described.conflicts[0].kind, crate::ConflictKind::Text);
    assert!(matches!(
        described.conflicts[0].ours,
        crate::ConflictValue::Text(ref text) if text == "main\n"
    ));
    let mut registry = crate::MergeDriverRegistry::new();
    registry.register("text", Arc::new(crate::DefaultTextMergeDriver))?;
    registry.set_default("text")?;
    let mut cache = crate::MemoryMergeResolutionCache::default();
    let outcome = plan
        .apply_with_drivers(
            ApplyOptions {
                if_target: plan.target_head(),
                idempotency_key: IdempotencyKey::new(),
            },
            &registry,
            &mut cache,
            false,
        )
        .await?;
    assert!(matches!(outcome, JoinOutcome::Applied(_)));
    let merged = String::from_utf8(main.read("/shared", 1024, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.to_vec())?;
    assert!(merged.contains("<<<<<<< ours"));
    assert!(merged.contains("main"));
    assert!(merged.contains("agent"));
    Ok(())
}

#[tokio::test]
async fn contiguous_change_sets_compose_by_outer_semantics() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("compose", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let base = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    workspace.write_text("/before", "body").await?;
    let middle = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let mut transaction = workspace.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction.rename("/before", "/after").await?;
    let TransactionCommit::Committed(end) = transaction.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)? else {
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
    let workspace = fs.create_workspace("changed-paths", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let mut initial = workspace.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    initial.create_dir_all("/nested/deep", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    initial
        .write("/rename", bytes::Bytes::from_static(b"same"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    initial
        .write("/remove", bytes::Bytes::from_static(b"gone"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    initial
        .write("/linked", bytes::Bytes::from_static(b"links"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    initial
        .write("/modified", bytes::Bytes::from_static(b"before"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert!(matches!(
        initial.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        TransactionCommit::Committed(_)
    ));
    let before = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;

    let mut changed = workspace.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    changed.rename("/rename", "/renamed").await?;
    changed.remove("/remove", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    changed.hard_link("/linked", "/nested/link", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    changed
        .write("/nested/deep/added", bytes::Bytes::from_static(b"new"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    changed
        .write("/modified", bytes::Bytes::from_static(b"after"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let TransactionCommit::Committed(after) = changed.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)? else {
        return Err("changed paths did not commit".into());
    };

    let paths = before.diff_to(&after, 64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map_err(|failure| failure.error)?.changed_paths(64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
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
        before.diff_to(&after, 64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map_err(|failure| failure.error)?.changed_paths(1, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error),
        Err(WorkspaceError::ChangedPathLimit)
    ));
    let exact = before.diff_to(&after, 64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map_err(|failure| failure.error)?;
    let mut truncated_changes = exact.changes().clone();
    truncated_changes.truncated = true;
    let truncated = ChangeSet {
        from: before.clone(),
        to: after.clone(),
        changes: truncated_changes,
        work: exact.work(),
    };
    assert!(matches!(
        truncated.changed_paths(64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error),
        Err(WorkspaceError::ChangedPathLimit)
    ));
    assert!(
        after
            .diff_to(&after, 1, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await.map_err(|failure| failure.error)?
            .changed_paths(0, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?
            .is_empty()
    );
    #[cfg(not(target_arch = "wasm32"))]
    {
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(selected_transaction_budget(), &cancellation)
            .map_err(|(error, _)| error)?;
        let (native_changes, diff_guard) = before.diff_to_with_native_work(&after, 64, &original, &cancellation)
            .await.map_err(|(error, _)| error)?;
        let retained_diff = original.live_allocation_bytes();
        let (native_paths, path_guard) = native_changes.changed_paths_with_native_work(64, &original, &cancellation)
            .await.map_err(|(error, _)| error)?;
        assert_eq!(native_paths, paths);
        let rename_before = native_paths.iter().find(|change|
            super::namespace_path_text(&change.path).is_ok_and(|path| path == "/rename"))
            .and_then(|change| change.before).ok_or("missing rename before")?;
        let rename_after = native_paths.iter().find(|change|
            super::namespace_path_text(&change.path).is_ok_and(|path| path == "/renamed"))
            .and_then(|change| change.after).ok_or("missing rename after")?;
        assert_eq!(rename_before.file_id, rename_after.file_id);
        let link = native_paths.iter().find(|change|
            super::namespace_path_text(&change.path).is_ok_and(|path| path == "/nested/link"))
            .and_then(|change| change.after).ok_or("missing hardlink after")?;
        assert_eq!(link.file_id, after.stat("/linked", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.file_id);
        drop(native_paths);
        drop(path_guard);
        assert_eq!(original.live_allocation_bytes(), retained_diff);
        let failure = match native_changes.changed_paths_with_native_work(1, &original, &cancellation).await {
            Err(failure) => failure,
            Ok(_) => return Err("native path bound unexpectedly succeeded".into()),
        };
        assert!(matches!(failure.0, WorkspaceError::ChangedPathLimit));
        drop(failure);
        assert_eq!(original.live_allocation_bytes(), retained_diff);
        drop(native_changes);
        drop(diff_guard);
        assert_eq!(original.live_allocation_bytes(), crate::NativeMaterializationWork::accounting_state_bytes());

        for deny_copy in [false, true] {
            let mut budget = selected_transaction_budget();
            if deny_copy { budget.bytes_copied = 0; } else { budget.items_examined = 0; }
            let denied = crate::NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(error, _)| error)?;
            let receipt = denied.receipt();
            let baseline = denied.live_allocation_bytes();
            let failure = match exact.changed_paths_with_native_work(64, &denied, &cancellation).await {
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

#[tokio::test]
async fn generation_lookup_paths_preserves_order_absence_and_duplicates()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("lookup-paths", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    workspace.write_text("/present", "body").await?;
    let generation = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let config = crate::model::VolumeConfig::portable(crate::model::Lifecycle::Ephemeral);
    let present = customer_path("/present", config)?;
    let absent = customer_path("/absent", config)?;
    let records = generation
        .lookup_paths(
            &[present.clone(), absent, present.clone()],
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
    assert_eq!(records.value.len(), 3);
    assert!(records.value[0].is_some());
    assert!(records.value[1].is_none());
    assert_eq!(records.value[0], records.value[2]);

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let cancelled_failure = generation
        .lookup_paths(
            std::slice::from_ref(&present),
            WorkBudget::UNBOUNDED,
            &cancelled,
        )
        .await
        .err()
        .ok_or_else(|| std::io::Error::other("pre-cancelled lookup must fail"))?;
    assert_eq!(*cancelled_failure.work, crate::WorkCounters::default());
    let budget_failure = generation
        .lookup_paths(&[present], WorkBudget::default(), &CancellationToken::new())
        .await
        .err()
        .ok_or_else(|| std::io::Error::other("zero-budget checkout must fail"))?;
    assert_ne!(*budget_failure.work, crate::WorkCounters::default());
    assert!(budget_failure.work.verify(WorkBudget::default()).is_err());
    Ok(())
}

#[tokio::test]
async fn bounded_path_index_hit_never_substitutes_a_false_empty_result()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("bounded-path-index", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let mut transaction = workspace.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction.write_text("/a", "body").await?;
    transaction.hard_link("/a", "/b", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert!(matches!(
        transaction.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        TransactionCommit::Committed(_)
    ));
    let generation = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let file_id = generation.stat("/a", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.file_id;

    let complete = generation
        .namespace_records_for_file_ids([file_id], 16)
        .await?;
    assert!(complete.complete);
    assert_eq!(complete.records.iter().find(|(id, _)| *id == file_id).map(|(_, records)| records.len()), Some(2));

    let bounded = generation
        .namespace_records_for_file_ids([file_id], 1)
        .await?;
    assert!(!bounded.complete);
    assert_eq!(bounded.records.iter().find(|(id, _)| *id == file_id).map(|(_, records)| records.len()), Some(1));
    Ok(())
}

#[tokio::test]
async fn generation_reader_resolves_many_paths_from_one_pinned_root() -> Result<(), Box<dyn Error>>
{
    let fs = Fs::memory();
    let workspace = fs.create_workspace("generation-reader", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    workspace.write_text("/present", "body").await?;
    let generation = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let config = crate::model::VolumeConfig::portable(crate::model::Lifecycle::Ephemeral);
    let present = customer_path("/present", config)?;
    let absent = customer_path("/absent", config)?;

    let reader = generation.reader().await?;
    let descriptions = reader
        .describe_files(
            &[present, absent],
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value;

    assert_eq!(descriptions.len(), 2);
    assert_eq!(
        descriptions[0].as_ref().map(|file| file.logical_bytes),
        Some(4)
    );
    assert!(descriptions[1].is_none());
    Ok(())
}

#[tokio::test]
async fn readonly_creation_lookup_keeps_authentic_initial_generation_and_failed_work()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let config = VolumeConfig::portable(Lifecycle::Ephemeral);
    let operation = IdempotencyKey::from_bytes([0xd4; 16]).operation_id();
    let cancellation = CancellationToken::new();
    let mut allowances = serde_json::to_value(WorkBudget::UNBOUNDED)?;
    for allowance in allowances.as_object_mut().expect("budget fields").values_mut() {
        *allowance = serde_json::json!(100_000_000_u64);
    }
    let budget: WorkBudget = serde_json::from_value(allowances)?;
    let created = fs.create_workspace_with_operation(
        "readonly-original-creation", config, operation, budget, &cancellation,
    ).await?.value;
    let workspace = fs.open_workspace("readonly-original-creation", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    workspace.write_text("/later", "advanced head").await?;
    let advanced = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id();
    assert_ne!(advanced, created.id());
    let original = fs.workspace_for_creation_operation(
        "readonly-original-creation", config, operation, budget, &cancellation,
    ).await?;
    original.work.verify(budget)?;
    assert_eq!(original.value.as_ref().map(|generation| generation.id()), Some(created.id()));
    assert_eq!(original.value.as_ref().map(|generation| generation.workspace_id()), Some(workspace.id()));
    assert_eq!(original.work.authority_records_appended, 0);
    assert_eq!(workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), advanced);

    let wrong_operation = IdempotencyKey::from_bytes([0xd5; 16]).operation_id();
    let mut wrong_config = config;
    wrong_config.lifecycle = Lifecycle::Durable;
    for (requested_config, requested_operation) in [(config, wrong_operation), (wrong_config, operation)] {
        let failure = fs.workspace_for_creation_operation(
            "readonly-original-creation", requested_config, requested_operation, budget, &cancellation,
        ).await.err().expect("mismatched original creation is rejected");
        assert_ne!(*failure.work, crate::WorkCounters::UNCHARGED);
        failure.work.verify(budget)?;
        assert_eq!(failure.work.authority_records_appended, 0);
    }
    let missing = fs.workspace_for_creation_operation(
        "readonly-missing-creation", config, operation, budget, &cancellation,
    ).await?;
    assert!(missing.value.is_none());
    assert_eq!(missing.work.authority_records_appended, 0);
    missing.work.verify(budget)?;
    assert!(fs.open_workspace("readonly-missing-creation", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).is_err());

    let mut denied = budget;
    denied.backend_read_operations = 0;
    let failure = fs.workspace_for_creation_operation(
        "readonly-original-creation", config, operation, denied, &cancellation,
    ).await.err().expect("finite denial retains name admission prefix");
    assert_ne!(*failure.work, crate::WorkCounters::UNCHARGED);
    failure.work.verify(denied)?;
    assert_eq!(failure.work.authority_records_appended, 0);
    assert_eq!(workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), advanced);
    Ok(())
}

/// Where a fork's provider fault lands: before a call reaches the provider,
/// or after a commit applied but before its caller learned so.
#[derive(Clone, Copy, Debug)]
enum ForkCut {
    Before,
    AfterCommit,
}

/// A Stream provider that fails its `fail_at`-th call, and counts commits.
struct CutStream {
    inner: Arc<acyclic_stream::MemoryStream>,
    calls: std::sync::atomic::AtomicUsize,
    commits: std::sync::atomic::AtomicUsize,
    fail_at: std::sync::atomic::AtomicUsize,
    cut: std::sync::Mutex<ForkCut>,
}

impl CutStream {
    fn new(inner: Arc<acyclic_stream::MemoryStream>) -> Self {
        Self {
            inner,
            calls: std::sync::atomic::AtomicUsize::new(0),
            commits: std::sync::atomic::AtomicUsize::new(0),
            fail_at: std::sync::atomic::AtomicUsize::new(usize::MAX),
            cut: std::sync::Mutex::new(ForkCut::Before),
        }
    }

    fn arm(&self, fail_at: usize, cut: ForkCut) {
        use std::sync::atomic::Ordering;
        self.calls.store(0, Ordering::SeqCst);
        self.commits.store(0, Ordering::SeqCst);
        if let Ok(mut current) = self.cut.lock() {
            *current = cut;
        }
        self.fail_at.store(fail_at, Ordering::SeqCst);
    }

    fn disarm(&self) -> bool {
        use std::sync::atomic::Ordering;
        let fired = self.calls.load(Ordering::SeqCst) >= self.fail_at.load(Ordering::SeqCst);
        self.fail_at.store(usize::MAX, Ordering::SeqCst);
        fired
    }

    fn commits(&self) -> usize {
        self.commits.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn fails_now(&self) -> bool {
        use std::sync::atomic::Ordering;
        self.calls.fetch_add(1, Ordering::SeqCst) + 1 == self.fail_at.load(Ordering::SeqCst)
    }

    fn cut(&self) -> ForkCut {
        self.cut.lock().map_or(ForkCut::Before, |cut| *cut)
    }
}

#[async_trait::async_trait]
impl acyclic_stream::StreamProvider for CutStream {
    async fn inspect_idempotency(
        &self,
        key: acyclic_stream::IdempotencyKey,
    ) -> Result<Option<acyclic_stream::IdempotencyObservation>, acyclic_stream::StreamError> {
        if self.fails_now() {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        self.inner.inspect_idempotency(key).await
    }

    async fn tail(
        &self,
        path: acyclic_stream::StreamPath,
    ) -> Result<u64, acyclic_stream::StreamError> {
        if self.fails_now() {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        self.inner.tail(path).await
    }

    async fn bounds(
        &self,
        path: acyclic_stream::StreamPath,
    ) -> Result<acyclic_stream::StreamBounds, acyclic_stream::StreamError> {
        if self.fails_now() {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        self.inner.bounds(path).await
    }

    async fn append(
        &self,
        request: acyclic_stream::AppendRequest,
    ) -> Result<acyclic_stream::AppendOutcome, acyclic_stream::StreamError> {
        if self.fails_now() {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        self.inner.append(request).await
    }

    async fn fork(
        &self,
        request: acyclic_stream::ForkRequest,
    ) -> Result<acyclic_stream::ForkReceipt, acyclic_stream::StreamError> {
        if self.fails_now() {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        self.inner.fork(request).await
    }

    async fn read(
        &self,
        request: acyclic_stream::ReadRequest,
    ) -> Result<acyclic_stream::RecordStream, acyclic_stream::StreamError> {
        if self.fails_now() {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        self.inner.read(request).await
    }

    async fn follow(
        &self,
        path: acyclic_stream::StreamPath,
        from: u64,
    ) -> Result<acyclic_stream::RecordStream, acyclic_stream::StreamError> {
        if self.fails_now() {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        self.inner.follow(path, from).await
    }

    async fn children(
        &self,
        request: acyclic_stream::ChildrenRequest,
    ) -> Result<acyclic_stream::ChildStream, acyclic_stream::StreamError> {
        if self.fails_now() {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        self.inner.children(request).await
    }

    async fn commit(
        &self,
        request: acyclic_stream::CommitRequest,
    ) -> Result<acyclic_stream::CommitOutcome, acyclic_stream::StreamError> {
        let fails = self.fails_now();
        if fails && matches!(self.cut(), ForkCut::Before) {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        let outcome = self.inner.commit(request).await?;
        if matches!(outcome, acyclic_stream::CommitOutcome::Committed(_)) {
            self.commits
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        if fails {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        Ok(outcome)
    }

    async fn read_commit(
        &self,
        commit_id: acyclic_stream::CommitId,
    ) -> Result<acyclic_stream::CommittedEnvelope, acyclic_stream::StreamError> {
        if self.fails_now() {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        self.inner.read_commit(commit_id).await
    }
}

type CutFs = Fs<
    crate::distributed::StreamAuthorityStore<CutStream>,
    crate::LogicalObjectStore<acyclic_objects::v1::MemoryObjects>,
>;

/// The first record of one authority, if it has one.
async fn first_fork_record(
    stream: &Arc<acyclic_stream::MemoryStream>,
    authority: crate::AuthorityId,
) -> Result<Option<crate::foundation::DurableCommit>, Box<dyn Error>> {
    use crate::AsyncAuthorityStore as _;
    let store = crate::distributed::StreamAuthorityStore::new(Arc::clone(stream));
    match store
        .replay(
            authority,
            crate::foundation::Sequence::GENESIS,
            crate::storage::ReplayLimit {
                records: 1,
                payload_bytes: 4 * 1024,
            },
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
    {
        Ok(records) => Ok(records.value.into_iter().next()),
        Err(failure) if matches!(failure.error, crate::storage::AuthorityStoreError::Missing) => {
            Ok(None)
        }
        Err(failure) => Err(failure.error.into()),
    }
}

/// Checks a fork's durable authority state after any interruption: the
/// workspace exists only with its source retained, and a record that
/// exists is exactly the fork's.
async fn assert_fork_state_exact(
    stream: &Arc<acyclic_stream::MemoryStream>,
    source: &crate::Generation<
        crate::distributed::StreamAuthorityStore<CutStream>,
        crate::LogicalObjectStore<acyclic_objects::v1::MemoryObjects>,
    >,
    source_volume: crate::foundation::VolumeId,
    destination: WorkspaceId,
) -> Result<bool, Box<dyn Error>> {
    let retention = crate::kernel::retention_authority_id(
        source_volume,
        crate::kernel::RetentionKind::ForkBase,
        &hex::encode(destination.into_bytes()),
    );
    let retained = first_fork_record(stream, retention).await?;
    if let Some(retained) = &retained {
        let record = crate::kernel::decode_retention_created(&retained.payload, 4 * 1024)?;
        assert_eq!(record.generation_root.digest, source.id().digest());
    }
    let created = first_fork_record(
        stream,
        crate::kernel::volume_authority_id(destination.volume_id()),
    )
    .await?;
    if let Some(created) = &created {
        let record = crate::kernel::decode_volume_created(&created.payload, 4 * 1024, &mut |_| Ok::<_, crate::kernel::VolumeCreatedError>(()))?;
        assert_eq!(record.volume_id, destination.volume_id());
        assert!(
            retained.is_some(),
            "a workspace became durable before its source generation was retained"
        );
    }
    Ok(created.is_some())
}

/// Whether the destination's creation record is found by the operation
/// that made it, as operation observation looks it up.
async fn creation_operation(
    stream: &Arc<acyclic_stream::MemoryStream>,
    destination: WorkspaceId,
    key: IdempotencyKey,
) -> Result<bool, Box<dyn Error>> {
    use crate::AsyncAuthorityStore as _;
    let found = crate::distributed::StreamAuthorityStore::new(Arc::clone(stream))
        .find_operation(
            crate::kernel::volume_authority_id(destination.volume_id()),
            key.operation_id(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?;
    Ok(found
        .value
        .is_some_and(|commit| commit.sequence == crate::foundation::Sequence::new(1)))
}

#[tokio::test]
async fn workspace_creation_is_one_commit_and_exact_at_every_provider_cut()
-> Result<(), Box<dyn Error>> {
    let mut uninterrupted_commits = None;
    for cut in [ForkCut::Before, ForkCut::AfterCommit] {
        for fail_at in 1.. {
            let stream = Arc::new(acyclic_stream::MemoryStream::default());
            let cutting = Arc::new(CutStream::new(Arc::clone(&stream)));
            let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
            let fs: CutFs = Fs::new(
                crate::distributed::StreamAuthorityStore::new(Arc::clone(&cutting)),
                crate::LogicalObjectStore::new(Arc::new(objects), bucket),
                crate::EmbeddedCapabilities::MEMORY,
            );
            cutting.arm(fail_at, cut);
            let attempt = fs.create_workspace("repo", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error);
            let fired = cutting.disarm();
            let committed = cutting.commits();
            let volume = fs.workspace_id("repo")?.volume_id();
            let created =
                first_fork_record(&stream, crate::kernel::volume_authority_id(volume)).await?;
            if let Some(created) = &created {
                assert_eq!(
                    crate::kernel::decode_volume_created(&created.payload, 4 * 1024, &mut |_| Ok::<_, crate::kernel::VolumeCreatedError>(()))?.volume_id,
                    volume
                );
            }
            if !fired {
                attempt?;
                assert!(created.is_some());
                uninterrupted_commits.get_or_insert(committed);
                break;
            }
            assert!(
                attempt.is_err(),
                "a fault at call {fail_at} was not reported"
            );
            // An interruption leaves no volume or the complete one.
            let opened = fs.open_workspace("repo", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error);
            assert_eq!(opened.is_ok(), created.is_some());
            let retried = fs.create_workspace("repo", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
            retried.write_text("/after.txt", "after").await?;
            assert_eq!(
                fs.open_workspace("repo", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
                    .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?
                    .read("/after.txt", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
                OwnedBytes::from_static(b"after")
            );
        }
    }
    assert_eq!(uninterrupted_commits, Some(1));
    Ok(())
}

/// Checks that a fork's authority lineage is exactly the one separate fork
/// and append commits would leave: the source's lineage through the source
/// generation, for a fork of the published head, then the fork's first
/// generation, located at its position. A fork of the fork's head then
/// shares that lineage in turn.
async fn assert_fork_lineage_exact(
    stream: &Arc<acyclic_stream::MemoryStream>,
    main: &crate::Workspace<
        crate::distributed::StreamAuthorityStore<CutStream>,
        crate::LogicalObjectStore<acyclic_objects::v1::MemoryObjects>,
    >,
    source: &crate::Generation<
        crate::distributed::StreamAuthorityStore<CutStream>,
        crate::LogicalObjectStore<acyclic_objects::v1::MemoryObjects>,
    >,
    fork: &crate::Workspace<
        crate::distributed::StreamAuthorityStore<CutStream>,
        crate::LogicalObjectStore<acyclic_objects::v1::MemoryObjects>,
    >,
    independent: bool,
    inherits_base: bool,
) -> Result<(), Box<dyn Error>> {
    let store = crate::distributed::StreamAuthorityStore::new(Arc::clone(stream));
    let authority =
        |workspace: WorkspaceId| crate::kernel::volume_authority_id(workspace.volume_id());
    let head = fork.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id();
    let mut expected = if independent {
        Vec::new()
    } else {
        let source_lineage = store.generation_lineage(authority(main.id())).await?;
        let at = store
            .generation_locator(authority(main.id()), source.id())
            .await?;
        assert_eq!(
            store
                .generation_locator(authority(fork.id()), source.id())
                .await?,
            at
        );
        source_lineage[..usize::try_from(at)?].to_vec()
    };
    expected.push(head);
    assert_eq!(
        store.generation_lineage(authority(fork.id())).await?,
        expected
    );
    assert_eq!(
        store.generation_locator(authority(fork.id()), head).await?,
        u64::try_from(expected.len())?
    );
    // A fork of the fork's published head reuses that lineage.
    let grandchild = fork
        .fork(
            "grandchild",
            ForkOptions::from_generation(
                fork.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
                IdempotencyKey::from_bytes([0x71; 16]),
            ),
        crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    if inherits_base {
        assert_eq!(
            grandchild.read("/base.txt", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
            OwnedBytes::from_static(b"base")
        );
    } else {
        assert!(grandchild.read("/base.txt", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).is_err());
    }
    let grandchild_lineage = store.generation_lineage(authority(grandchild.id())).await?;
    assert_eq!(
        grandchild_lineage.get(..expected.len()),
        Some(&expected[..])
    );
    Ok(())
}

/// Forks one generation through every provider cut, before calls and after
/// commits, checking the durable state after each interruption and that a
/// retry with the same key completes the same workspace. Returns the commits
/// an uninterrupted fork made.
async fn fork_through_every_cut(
    advance_source: bool,
    paths: Option<Vec<String>>,
) -> Result<usize, Box<dyn Error>> {
    let inherits_base = paths
        .as_ref()
        .is_none_or(|paths| paths.iter().any(|path| path == "/base.txt"));
    let mut uninterrupted_commits = None;
    for cut in [ForkCut::Before, ForkCut::AfterCommit] {
        for fail_at in 1.. {
            let stream = Arc::new(acyclic_stream::MemoryStream::default());
            let cutting = Arc::new(CutStream::new(Arc::clone(&stream)));
            let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
            let fs: CutFs = Fs::new(
                crate::distributed::StreamAuthorityStore::new(Arc::clone(&cutting)),
                crate::LogicalObjectStore::new(Arc::new(objects), bucket),
                crate::EmbeddedCapabilities::MEMORY,
            );
            let main = fs.create_workspace("repo", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
            main.write_text("/base.txt", "base").await?;
            main.write_text("/excluded.txt", "excluded").await?;
            let source = main.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
            if advance_source {
                main.write_text("/later.txt", "later").await?;
            }
            let key = IdempotencyKey::from_bytes([0x51; 16]);
            let options = || {
                let options = ForkOptions::from_generation(source.clone(), key);
                match &paths {
                    Some(paths) => options.inherit_paths(paths.clone()),
                    None => options,
                }
            };
            cutting.arm(fail_at, cut);
            let attempt = main.fork("agent", options(), crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED }))).await.map(|receipt| receipt.value).map_err(|failure| failure.error);
            let fired = cutting.disarm();
            let committed = cutting.commits();
            let destination = fs.workspace_id("agent")?;
            let complete =
                assert_fork_state_exact(&stream, &source, main.id().volume_id(), destination)
                    .await?;
            if !fired {
                let fork = attempt?;
                assert!(complete);
                assert_fork_lineage_exact(
                    &stream,
                    &main,
                    &source,
                    &fork,
                    advance_source,
                    inherits_base,
                )
                .await?;
                assert!(creation_operation(&stream, destination, key).await?);
                uninterrupted_commits.get_or_insert(committed);
                break;
            }
            assert!(
                attempt.is_err(),
                "a fault at call {fail_at} was not reported"
            );
            let retried = main.fork("agent", options(), crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED }))).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
            assert_eq!(retried.id(), destination);
            if inherits_base {
                assert_eq!(
                    retried.read("/base.txt", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
                    OwnedBytes::from_static(b"base")
                );
            } else {
                assert!(retried.read("/base.txt", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).is_err());
            }
            assert!(
                retried
                    .head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?
                    .matches_fork_selection(&source, paths.clone(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
                    .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?
            );
            if paths.is_some() {
                assert!(retried.read("/excluded.txt", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).is_err());
            }
            assert!(
                assert_fork_state_exact(&stream, &source, main.id().volume_id(), destination)
                    .await?
            );
            let again = main.fork("agent", options(), crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED }))).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
            assert_eq!(again.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), retried.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id());
            assert_fork_lineage_exact(
                &stream,
                &main,
                &source,
                &retried,
                advance_source,
                inherits_base,
            )
            .await?;
            assert!(
                creation_operation(&stream, destination, key).await?,
                "the fork's creation is not found by its operation"
            );
        }
    }
    uninterrupted_commits.ok_or_else(|| "the fork never completed".into())
}

#[tokio::test]
async fn a_fork_of_the_head_is_one_commit_and_exact_at_every_provider_cut()
-> Result<(), Box<dyn Error>> {
    // The fork of a published head shares its lineage, which the same
    // commit extends with the new workspace's first generation.
    assert_eq!(fork_through_every_cut(false, None).await?, 1);
    Ok(())
}

#[tokio::test]
async fn an_independent_fork_is_one_commit_and_exact_at_every_provider_cut()
-> Result<(), Box<dyn Error>> {
    assert_eq!(fork_through_every_cut(true, None).await?, 1);
    Ok(())
}

#[tokio::test]
async fn filtered_forks_reconcile_at_every_provider_cut() -> Result<(), Box<dyn Error>> {
    for advanced in [false, true] {
        for paths in [Vec::new(), vec!["/base.txt".to_owned()]] {
            assert_eq!(fork_through_every_cut(advanced, Some(paths)).await?, 1);
        }
    }
    Ok(())
}

/// Truncates a local root's Stream journal to `length` bytes, as a power
/// loss before the rest was synchronized would leave it.
#[cfg(all(feature = "local", any(unix, windows)))]
fn cut_journal(root: &std::path::Path, journal: &[u8], length: usize) -> std::io::Result<()> {
    std::fs::write(
        root.join("stream").join("stream.journal"),
        journal.get(..length).unwrap_or(journal),
    )
}

#[cfg(all(feature = "local", any(unix, windows)))]
async fn close_local(
    fs: crate::Fs<crate::LocalAuthorityBackend, crate::LocalObjectBackend>,
) -> Result<(), Box<dyn Error>> {
    let released = fs
        .local_root_release_barrier()
        .ok_or("local root has no release barrier")?;
    drop(fs);
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !released.is_released() {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await?;
    Ok(())
}

#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_local_fork_survives_power_loss_at_every_journal_cut() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let root = directory.path();
    let journal_path = root.join("stream").join("stream.journal");
    let key = IdempotencyKey::from_bytes([0x61; 16]);
    let source_id;
    {
        let fs = Fs::local(crate::LocalOptions::new(root)).await?;
        let main = fs.create_workspace("repo", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        main.write_text("/base.txt", "base").await?;
        source_id = main.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id();
        drop(main);
        close_local(fs).await?;
    }
    let before = std::fs::metadata(&journal_path)?.len();
    let forked_head;
    {
        let fs = Fs::local(crate::LocalOptions::new(root)).await?;
        let main = fs.open_workspace("repo", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let source = main.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        assert_eq!(source.id(), source_id);
        let fork = main
            .fork("agent", ForkOptions::from_generation(source, key), crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        forked_head = fork.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id();
        drop((main, fork));
        close_local(fs).await?;
    }
    let journal = std::fs::read(&journal_path)?;
    let before = usize::try_from(before)?;
    // Each frame the fork appended ends at a power-loss point, and a frame
    // torn anywhere, in its length, command, or checksum, is another. A
    // frame is a 4-byte length, the command, and a 32-byte checksum.
    let mut cuts = vec![before];
    let mut frame = before;
    while frame < journal.len() {
        let length = journal
            .get(frame..frame + 4)
            .and_then(|bytes| <[u8; 4]>::try_from(bytes).ok())
            .map(u32::from_le_bytes)
            .ok_or("fork journal frame is truncated")?;
        let end = frame + 4 + usize::try_from(length)? + 32;
        cuts.extend([
            frame + 2,
            frame + 4 + usize::try_from(length)? / 2,
            end - 1,
            end,
        ]);
        frame = end;
    }
    assert_eq!(frame, journal.len(), "fork frames do not end the journal");
    // The fork of the published head is one commit.
    assert_eq!(cuts.len(), 1 + 4, "the fork did not append one frame");
    for length in cuts {
        cut_journal(root, &journal, length)?;
        let fs = Fs::local(crate::LocalOptions::new(root)).await?;
        let main = fs.open_workspace("repo", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let source = main.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        assert_eq!(source.id(), source_id);
        if let Ok(agent) = fs.open_workspace("agent", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error) {
            assert_eq!(agent.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), forked_head);
            assert_eq!(
                agent.read("/base.txt", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
                OwnedBytes::from_static(b"base")
            );
        }
        let retried = main
            .fork("agent", ForkOptions::from_generation(source, key), crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        assert_eq!(retried.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), forked_head);
        assert_eq!(
            retried.read("/base.txt", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
            OwnedBytes::from_static(b"base")
        );
        drop((main, retried));
        close_local(fs).await?;
    }
    Ok(())
}

/// Deleting a local workspace releases its authority for good; a collection
/// then releases the fork base its creation retained and reclaims what only
/// the deleted workspace held, while its source reads as before.
#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deleted_local_fork_releases_its_authority_and_content() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let root = directory.path();
    let cancellation = CancellationToken::new();
    {
        let fs = Fs::local(crate::LocalOptions::new(root)).await?;
        let main = fs.create_workspace("repo", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        main.write_text("/base.txt", "base").await?;
        let base = main.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let agent = main
            .fork(
                "agent",
                ForkOptions::from_generation(base, IdempotencyKey::new()),
            crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        agent
            .write_text("/agent.txt", &"only the agent ".repeat(8_192))
            .await?;
        assert_eq!(fs.authority().authorities(64).await?.len(), 3);
        let key = IdempotencyKey::new();
        assert_eq!(agent.delete(key).await?, WorkspaceDelete::Deleted);
        assert_eq!(agent.delete(key).await?, WorkspaceDelete::AlreadyDeleted);
        assert_eq!(
            fs.delete_workspace("agent", key).await?,
            WorkspaceDelete::AlreadyDeleted
        );
        assert!(fs.open_workspace("agent", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).is_err());
        assert!(fs.create_workspace("agent", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).is_err());
        assert_eq!(
            fs.authority().authorities(64).await?.len(),
            2,
            "the fork's authority is gone; its base is still retained"
        );
        // The collection runs while the source workspace is open.
        let collected = fs.collect_local_garbage(None, &cancellation).await?;
        assert!(collected.removed >= 1, "{collected:?}");
        let again = fs.collect_local_garbage(None, &cancellation).await?;
        assert_eq!(again.removed, 0);
        assert_eq!(main.read("/base.txt", 64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"base");
        drop((main, agent));
        close_local(fs).await?;
    }

    let fs = Fs::local(crate::LocalOptions::new(root)).await?;
    assert_eq!(
        fs.authority().authorities(64).await?.len(),
        1,
        "the fork base went with its fork"
    );
    let main = fs.open_workspace("repo", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert_eq!(main.read("/base.txt", 64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"base");
    // Content the collection removed is stored again when written again.
    let content = "only the agent ".repeat(8_192);
    main.write_text("/again.txt", &content).await?;
    drop(main);
    close_local(fs).await?;
    let fs = Fs::local(crate::LocalOptions::new(root)).await?;
    let main = fs.open_workspace("repo", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert_eq!(main.read("/again.txt", 1 << 20, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), content.as_bytes());
    Ok(())
}

/// Collections run back to back while a workspace keeps publishing and its
/// forks come and go: every publication succeeds, what the deleted forks
/// held is reclaimed, and everything live reads back after a reopen.
#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn collections_run_alongside_publications() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let root = directory.path();
    let cancellation = CancellationToken::new();
    let fs = Fs::local(crate::LocalOptions::new(root)).await?;
    fs.create_workspace("repo", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let writer = tokio::spawn({
        let fs = fs.clone();
        async move {
            let main = fs.open_workspace("repo", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
            for round in 0..24_u32 {
                let TransactionCommit::Committed(generation) = main
                    .write_text("/churn.txt", &round.to_string().repeat(20_000))
                    .await?
                else {
                    return Err(WorkspaceError::manual_engine("churn fixture did not commit"));
                };
                generation.pin(format!("churn-{round}"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
                let agent = main
                    .fork(
                        &format!("agent-{round}"),
                        ForkOptions::from_generation(generation, IdempotencyKey::new()),
                    crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
                    .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
                agent
                    .write_text("/agent.txt", &format!("agent {round} ").repeat(8_192))
                    .await?;
                agent.delete(IdempotencyKey::new()).await?;
            }
            Ok::<_, WorkspaceError>(())
        }
    });
    let mut removed = 0_u64;
    while !writer.is_finished() {
        removed += fs.collect_local_garbage(None, &cancellation).await?.removed;
    }
    writer.await??;
    removed += fs.collect_local_garbage(None, &cancellation).await?.removed;
    assert!(removed > 0);
    close_local(fs).await?;

    let fs = Fs::local(crate::LocalOptions::new(root)).await?;
    let main = fs.open_workspace("repo", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert_eq!(
        main.read("/churn.txt", 1 << 20, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(),
        "23".repeat(20_000).as_bytes()
    );
    assert_eq!(
        fs.collect_local_garbage(None, &cancellation).await?.removed,
        0
    );
    Ok(())
}

/// A collection forgets a deleted workspace's core-state records unless a
/// live workspace descends from it.
#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_collection_forgets_a_deleted_workspaces_records() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let fs = Fs::local(crate::LocalOptions::new(
        directory.path().join("filesystem"),
    ))
    .await?;
    let store = crate::LocalCoreStateStore::open_owned(directory.path().join("core-state"))?;
    let graph = crate::WorkspaceGraph::new(store.clone());
    let main = fs.create_workspace("repo", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    main.write_text("/base.txt", "base").await?;
    let mut children = Vec::new();
    for name in ["kept", "deleted"] {
        let base = main.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let child = main
            .fork(
                name,
                ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
            crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        graph
            .register_existing_child(&main, &child, base.id())
            .await?;
        children.push(child);
    }
    let deleted = children.pop().ok_or("deleted child")?;
    let kept = children.pop().ok_or("kept child")?;
    // A deleted parent of a live child keeps its records.
    let grandchild_base = kept.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let grandchild = kept
        .fork(
            "grandchild",
            ForkOptions::from_generation(grandchild_base.clone(), IdempotencyKey::new()),
        crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    graph
        .register_existing_child(&kept, &grandchild, grandchild_base.id())
        .await?;
    kept.delete(IdempotencyKey::new()).await?;
    deleted.delete(IdempotencyKey::new()).await?;
    let records = store.workspace_records().await?;
    assert!(records.contains_key(&deleted.id()) && records.contains_key(&kept.id()));

    fs
        .collect_local_garbage(Some(&store), &CancellationToken::new())
        .await?;
    let records = store.workspace_records().await?;
    assert!(!records.contains_key(&deleted.id()));
    assert!(
        records.contains_key(&kept.id()),
        "a live child descends from it"
    );
    assert!(records.contains_key(&grandchild.id()));
    Ok(())
}

/// Reopening local storage between edits and promotions preserves the base
/// objects needed to return a typed conflict for a second sibling.
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
#[tokio::test]
async fn reopened_sibling_promotions_return_conflict_without_missing_objects()
-> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    {
        let fs = Fs::local(crate::LocalOptions::new(directory.path())).await?;
        let main = fs.create_workspace("promote-main", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        main.write_text("/README.md", "base\n").await?;
        let mut transaction = main.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        transaction.create_dir_all("/src", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        transaction.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        main.write_text("/src/main.rs", "fn main() {}\n").await?;
        let base = main.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        main.fork(
            "promote-first",
            ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
        crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        main.fork(
            "promote-second",
            ForkOptions::from_generation(base, IdempotencyKey::new()),
        crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    }
    {
        let fs = Fs::local(crate::LocalOptions::new(directory.path())).await?;
        fs.open_workspace("promote-first", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?
            .write_text("/README.md", "first\n")
            .await?;
    }
    {
        let fs = Fs::local(crate::LocalOptions::new(directory.path())).await?;
        let main = fs.open_workspace("promote-main", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let first = fs.open_workspace("promote-first", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let plan = first
            .join_into(&main)
            .history(JoinHistory::Merge)
            .plan()
            .await?;
        assert!(matches!(
            plan.apply(ApplyOptions {
                if_target: plan.target_head(),
                idempotency_key: IdempotencyKey::new()
            })
            .await?,
            JoinOutcome::Applied(_)
        ));
        assert_eq!(
            main.read("/README.md", 64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
            OwnedBytes::from_static(b"first\n")
        );
    }
    {
        let fs = Fs::local(crate::LocalOptions::new(directory.path())).await?;
        fs.open_workspace("promote-second", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?
            .write_text("/README.md", "second\n")
            .await?;
    }
    {
        let fs = Fs::local(crate::LocalOptions::new(directory.path())).await?;
        let main = fs.open_workspace("promote-main", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let second = fs.open_workspace("promote-second", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let plan = second
            .join_into(&main)
            .history(JoinHistory::Merge)
            .plan()
            .await?;
        assert!(matches!(
            plan.apply(ApplyOptions {
                if_target: plan.target_head(),
                idempotency_key: IdempotencyKey::new()
            })
            .await?,
            JoinOutcome::Conflicted { .. }
        ));
        assert_eq!(
            main.read("/README.md", 64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
            OwnedBytes::from_static(b"first\n")
        );
    }
    Ok(())
}

/// Siblings forked from one head, each adding its own file, join back one
/// after the other. The second join is a real three-way merge (its base is
/// the pre-first-join head) and must apply: nothing in it conflicts.
#[tokio::test]
async fn sibling_forks_adding_distinct_files_join_in_sequence() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = fs.create_workspace("siblings-main", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    main.write_text("/README.md", "base\n").await?;
    let base = main.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let first = main
        .fork(
            "siblings-first",
            ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
        crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let second = main
        .fork(
            "siblings-second",
            ForkOptions::from_generation(base, IdempotencyKey::new()),
        crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    first.write_text("/a.rs", "a\n").await?;
    second.write_text("/b.rs", "b\n").await?;

    for (label, fork) in [("first", &first), ("second", &second)] {
        let plan = fork.join_into(&main).plan().await?;
        let outcome = plan
            .apply(ApplyOptions {
                if_target: plan.target_head(),
                idempotency_key: IdempotencyKey::new(),
            })
            .await?;
        let applied = match outcome {
            JoinOutcome::Applied(_) => true,
            JoinOutcome::Conflicted { conflicts, .. } => {
                let described = plan.describe_conflicts(&conflicts, false).await?;
                panic!("{label} join conflicted: {:?}", described.conflicts);
            }
            _ => false,
        };
        assert!(applied, "{label} join did not apply");
    }
    assert_eq!(main.read("/a.rs", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, OwnedBytes::from_static(b"a\n"));
    assert_eq!(main.read("/b.rs", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, OwnedBytes::from_static(b"b\n"));
    assert_eq!(
        main.read("/README.md", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"base\n")
    );
    Ok(())
}

/// Two siblings each create the same directory (under different identities)
/// and add a file to it. Directories merge by path, as in Git: the second join
/// folds its directory into the first's instead of conflicting on the name,
/// including a nested directory both created.
#[tokio::test]
async fn sibling_directories_created_independently_merge_by_path() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = fs.create_workspace("fold-main", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    main.write_text("/README.md", "base\n").await?;
    let base = main.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let mut forks = Vec::new();
    for (name, file) in [("fold-first", "a.rs"), ("fold-second", "b.rs")] {
        let fork = main
            .fork(
                name,
                ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
            crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let mut transaction = fork.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        transaction.create_dir_all("/pkg/nested", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        transaction.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        fork.write_text(&format!("/pkg/{file}"), file).await?;
        fork.write_text(&format!("/pkg/nested/{file}"), file)
            .await?;
        forks.push(fork);
    }
    for fork in &forks {
        let plan = fork.join_into(&main).plan().await?;
        match plan
            .apply(ApplyOptions {
                if_target: plan.target_head(),
                idempotency_key: IdempotencyKey::new(),
            })
            .await?
        {
            JoinOutcome::Applied(_) => {}
            JoinOutcome::Conflicted { conflicts, .. } => {
                let described = plan.describe_conflicts(&conflicts, false).await?;
                panic!("join conflicted: {:?}", described.conflicts);
            }
            _ => panic!("join did not apply"),
        }
    }
    for path in [
        "/pkg/a.rs",
        "/pkg/b.rs",
        "/pkg/nested/a.rs",
        "/pkg/nested/b.rs",
    ] {
        let name = path.rsplit('/').next().expect("file name");
        assert_eq!(
            main.read(path, 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
            OwnedBytes::copy_from_slice(name.as_bytes())
        );
    }
    Ok(())
}

/// Two siblings each create the same file (under different identities, as two
/// captures of one host file do). Files merge by path like directories: the
/// same content folds into one file, and only different content conflicts.
#[tokio::test]
async fn sibling_files_created_independently_merge_by_path() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = fs.create_workspace("file-fold-main", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    main.write_text("/README.md", "base\n").await?;
    let base = main.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let mut forks = Vec::new();
    for (name, other) in [
        ("file-fold-first", "first\n"),
        ("file-fold-second", "second\n"),
    ] {
        let fork = main
            .fork(
                name,
                ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
            crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        fork.write_text("/same.rs", "same\n").await?;
        fork.write_text("/other.rs", other).await?;
        forks.push(fork);
    }
    let mut outcomes = Vec::new();
    for fork in &forks {
        let plan = fork.join_into(&main).plan().await?;
        outcomes.push(
            match plan
                .apply(ApplyOptions {
                    if_target: plan.target_head(),
                    idempotency_key: IdempotencyKey::new(),
                })
                .await?
            {
                JoinOutcome::Applied(_) => Vec::new(),
                JoinOutcome::Conflicted { conflicts, .. } => plan
                    .describe_conflicts(&conflicts, false)
                    .await?
                    .conflicts
                    .into_iter()
                    .map(|conflict| conflict.path)
                    .collect(),
                _ => panic!("join did not apply"),
            },
        );
    }
    assert_eq!(outcomes, [Vec::new(), vec![Some("/other.rs".to_owned())]]);

    let fork = main
        .fork(
            "file-fold-third",
            ForkOptions::from_generation(base, IdempotencyKey::new()),
        crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    fork.write_text("/same.rs", "same\n").await?;
    let plan = fork.join_into(&main).plan().await?;
    match plan
        .apply(ApplyOptions {
            if_target: plan.target_head(),
            idempotency_key: IdempotencyKey::new(),
        })
        .await?
    {
        // Folded into the file main already holds: nothing left to publish.
        JoinOutcome::NoChanges(_) => {}
        JoinOutcome::Conflicted { conflicts, .. } => {
            let described = plan.describe_conflicts(&conflicts, false).await?;
            panic!(
                "an identical independent creation must fold: {:?}",
                described.conflicts
            );
        }
        _ => panic!("an identical independent creation changes nothing"),
    }
    assert_eq!(
        main.read("/same.rs", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"same\n")
    );
    assert_eq!(
        main.read("/other.rs", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"first\n")
    );
    Ok(())
}

/// The mount bumps a directory's modification time whenever a child is
/// added, so two siblings adding files to one directory always diverge on its
/// metadata. That must reconcile; only authored metadata (the mode) conflicts.
#[tokio::test]
async fn sibling_directory_times_reconcile_but_authored_metadata_conflicts()
-> Result<(), Box<dyn Error>> {
    use crate::kernel::MetadataField;

    let fs = Fs::memory();
    let main = fs.create_workspace("sibling-times-main", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    main.write_text("/README.md", "base\n").await?;
    {
        let mut transaction = main.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        transaction.create_dir_all("/pkg", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        transaction.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    }
    let touch = |modified_ns: i64, posix_mode: u32| FileMetadata {
        modified_ns: MetadataField::Value(modified_ns),
        posix_mode: MetadataField::Value(posix_mode),
        ..FileMetadata::default()
    };
    {
        let mut transaction = main.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        transaction.set_metadata("/pkg", touch(10, 0o755), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        transaction.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    }
    let base = main.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let cancellation = crate::CancellationToken::new();
    let fork = |name: &'static str, base: &Generation<_, _>| {
        main.fork(
            name,
            ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
        crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &cancellation, |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
    };
    let first = fork("sibling-times-first", &base).await?.value;
    let second = fork("sibling-times-second", &base).await?.value;
    for (workspace, file, modified_ns) in [(&first, "/pkg/a.rs", 20), (&second, "/pkg/b.rs", 30)] {
        let mut transaction = workspace.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        transaction.write_text(file, "x\n").await?;
        transaction
            .set_metadata("/pkg", touch(modified_ns, 0o755), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        transaction.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    }
    for fork in [&first, &second] {
        let plan = fork.join_into(&main).plan().await?;
        let outcome = plan
            .apply(ApplyOptions {
                if_target: plan.target_head(),
                idempotency_key: IdempotencyKey::new(),
            })
            .await?;
        if let JoinOutcome::Conflicted { conflicts, .. } = outcome {
            let described = plan.describe_conflicts(&conflicts, false).await?;
            panic!("sibling join conflicted: {:?}", described.conflicts);
        }
    }
    assert_eq!(
        main.read("/pkg/a.rs", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"x\n")
    );
    assert_eq!(
        main.read("/pkg/b.rs", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"x\n")
    );
    assert_eq!(main.stat("/pkg", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.metadata.modified_ns, Some(30));

    // Authored metadata changed both ways is a conflict, reported as such.
    let base = main.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let third = fork("sibling-times-third", &base).await?.value;
    for (workspace, mode) in [(&main, 0o700), (&third, 0o770)] {
        let mut transaction = workspace.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        transaction.set_metadata("/pkg", touch(40, mode), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        transaction.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    }
    let plan = third.join_into(&main).plan().await?;
    let JoinOutcome::Conflicted {
        conflicts,
        truncated,
    } = plan
        .apply(ApplyOptions {
            if_target: plan.target_head(),
            idempotency_key: IdempotencyKey::new(),
        })
        .await?
    else {
        return Err("a mode conflict on the shared directory was not reported".into());
    };
    let described = plan.describe_conflicts(&conflicts, truncated).await?;
    assert_eq!(described.conflicts.len(), 1);
    assert_eq!(described.conflicts[0].kind, crate::ConflictKind::Metadata);
    Ok(())
}

fn selected_transaction_budget() -> WorkBudget {
    WorkBudget {
        authority_records_read: 1_000_000,
        authority_records_appended: 1_000_000,
        authority_bytes_read: 1_000_000,
        authority_bytes_written: 1_000_000,
        object_probes: 1_000_000,
        backend_read_operations: 1_000_000,
        backend_write_operations: 1_000_000,
        durability_operations: 1_000_000,
        page_reads: 1_000_000,
        page_writes: 1_000_000,
        object_bytes_read: 1_000_000,
        object_bytes_written: 1_000_000,
        bytes_hashed: 1_000_000,
        bytes_copied: 1_000_000,
        bytes_encoded: 1_000_000,
        source_bytes_read: 1_000_000,
        source_path_components: 1_000_000,
        source_entries_visited: 1_000_000,
        output_bytes: 1_000_000,
        items_examined: 1_000_000,
        items_returned: 1_000_000,
        allocation_operations: 1_000_000,
        peak_allocation_bytes: 64 * 1024 * 1024,
        materializations: 1_000_000,
    }
}

#[tokio::test]
async fn selected_restore_guard_failure_retains_original_head_and_exact_outside_state()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("guarded-selected-restore", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let mut seed = workspace.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    seed.create_dir_all("/tree", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    seed.write("/tree/file", bytes::Bytes::from_static(b"source"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    seed.hard_link("/tree/file", "/tree/alias", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    seed.set_metadata("/tree/file", FileMetadata {
        posix_mode: MetadataField::Value(0o640), ..FileMetadata::default()
    }, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    seed.write("/outside", bytes::Bytes::from_static(b"base"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    seed.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let source = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let source_metadata = source.stat("/tree/file", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.metadata;
    let mut live = workspace.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    live.write("/tree/file", bytes::Bytes::from_static(b"dirty"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    live.write("/removed", bytes::Bytes::from_static(b"remove me"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    live.write("/outside", bytes::Bytes::from_static(b"user"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    live.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let dirty = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let selected = vec!["tree/file".to_owned(), "removed".to_owned()];
    let budget = selected_transaction_budget();
    let key = IdempotencyKey::new();
    let cancelled = CancellationToken::new();
    let volume = workspace.volume.clone();
    let source_id = source.id();
    let guard_cancellation = cancelled.clone();
    let failed = workspace.restore_paths_from(
        &source, &selected, dirty.id(), key, crate::PublicationPermit::Unrestricted,
        budget, &cancelled, move |allowance| {
            let volume = volume.clone();
            let cancellation = guard_cancellation.clone();
            async move {
                let checked = volume.checkout(
                    GenerationSelector::Exact(source_id), CheckoutMode::read_only_pinned(),
                    allowance, &cancellation,
                ).await?;
                drop(checked.value);
                cancellation.cancel();
                Err(crate::OperationFailure::new(
                    FsError::Cancelled(crate::CancellationError), checked.work,
                ))
            }
        },
    ).await;
    let Err(failed) = failed else { panic!("original seal guard must reject"); };
    assert!(matches!(failed.error, WorkspaceError::Cancelled(_)));
    assert!(failed.work.allocation_operations > 0);
    assert!(failed.work.bytes_copied > 0);
    failed.work.verify(budget)?;
    assert_eq!(workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), dirty.id());
    assert_eq!(workspace.read("/tree/file", 32, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"dirty");
    assert_eq!(workspace.read("/removed", 32, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"remove me");
    assert_eq!(workspace.read("/outside", 32, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"user");

    let cancellation = CancellationToken::new();
    let conflicted = workspace.restore_paths_from(
        &source, &selected, source.id(), IdempotencyKey::new(),
        crate::PublicationPermit::Unrestricted, budget, &cancellation, |_| async { Ok::<_, crate::OperationFailure<crate::FsError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) },
    ).await?;
    conflicted.work.verify(budget)?;
    assert!(matches!(&conflicted.value, TransactionCommit::Conflict { actual } if actual.id() == dirty.id()));
    assert_eq!(workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), dirty.id());
    let volume = workspace.volume.clone();
    let guard_cancellation = cancellation.clone();
    let restored = workspace.restore_paths_from(
        &source, &selected, dirty.id(), key, crate::PublicationPermit::Unrestricted,
        budget, &cancellation, move |allowance| {
            let volume = volume.clone();
            let cancellation = guard_cancellation.clone();
            async move {
                let checked = volume.checkout(
                    GenerationSelector::Exact(source_id), CheckoutMode::read_only_pinned(),
                    allowance, &cancellation,
                ).await?;
                drop(checked.value);
                Ok(crate::OperationReceipt { value: (), work: checked.work })
            }
        },
    ).await?;
    restored.work.verify(budget)?;
    assert!(matches!(restored.value, TransactionCommit::Committed(_)));
    assert_eq!(workspace.read("/tree/file", 32, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"source");
    assert_eq!(workspace.read("/tree/alias", 32, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"source");
    assert_eq!(workspace.stat("/tree/file", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.metadata, source_metadata);
    assert_eq!(workspace.stat("/tree/file", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.file_id, workspace.stat("/tree/alias", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.file_id);
    assert!(matches!(workspace.read("/removed", 32, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error), Err(WorkspaceError::NotFound)));
    assert_eq!(workspace.read("/outside", 32, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"user");
    assert_eq!(source.read("/tree/file", 32, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"source");
    Ok(())
}

#[tokio::test]
async fn original_fork_retry_after_both_heads_advance_needs_no_publication_seal()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let parent = fs.create_workspace("original-fork-parent", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    parent.write_text("/file", "original").await?;
    let frozen = parent.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let key = IdempotencyKey::new();
    let cancellation = CancellationToken::new();
    let budget = selected_transaction_budget();
    let first = parent.fork(
        "original-fork-child", ForkOptions::from_generation(frozen.clone(), key),
        crate::PublicationPermit::Unrestricted, budget, &cancellation,
        |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })),
    ).await?;
    let child = first.value;
    let original_child = child.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    parent.write_text("/file", "later parent").await?;
    child.write_text("/file", "later child").await?;
    let advanced_child = child.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let retry = parent.fork(
        "original-fork-child", ForkOptions::from_generation(frozen, key),
        crate::PublicationPermit::Unrestricted, budget, &cancellation,
        |_| std::future::ready(Err(crate::OperationFailure::before_work(crate::FsError::CreationRejected))),
    ).await?;
    retry.work.verify(budget)?;
    assert_eq!(retry.work.authority_records_appended, 0);
    assert_eq!(retry.value.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), advanced_child.id());
    assert_eq!(retry.value.read("/file", 32, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"later child");
    let creation = fs.workspace_for_creation_operation(
        "original-fork-child", child.volume.config, key.operation_id(), budget, &cancellation,
    ).await?.value.ok_or("original creation missing")?;
    assert_eq!(creation.id(), original_child.id());
    assert_eq!(creation.read("/file", 32, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"original");
    Ok(())
}

#[tokio::test]
async fn guarded_stale_exact_retry_resolves_committed_without_owner_reseal()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("resolved-owner-seal", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let base = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let key = IdempotencyKey::new();
    let mut first = workspace.begin_transaction(key, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    first.write("/file", bytes::Bytes::from_static(b"same input"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let TransactionCommit::Committed(committed) = first.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)? else {
        return Err("initial original commit did not publish".into());
    };
    let mut retry = workspace.begin_transaction_at(&base, key, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    retry.write("/file", bytes::Bytes::from_static(b"same input"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let cancellation = CancellationToken::new();
    let budget = selected_transaction_budget();
    let mut owner_calls = 0;
    let resolved = retry.commit(None, crate::PublicationPermit::Unrestricted, budget, &cancellation, |_| {
            owner_calls += 1;
            std::future::ready(Err(crate::OperationFailure::before_work(
                OrdinaryOwnerError::Denied { task: [119; 16], revision: 914, expired: true },
            )))
        }).await?;
    resolved.work.verify(budget)?;
    assert!(matches!(&resolved.value, TransactionCommit::AlreadyCommitted(generation) if generation.id() == committed.id()));
    assert_eq!(workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), committed.id());
    assert_eq!(owner_calls, 0, "an already committed operation performs no new CAS");
    Ok(())
}

#[derive(Debug, thiserror::Error)]
enum OrdinaryOwnerError {
    #[error(transparent)]
    Sdk(WorkspaceError),
    #[error("ordinary owner denied publication")]
    Denied { task: [u8; 16], revision: u64, expired: bool },
}

impl From<WorkspaceError> for OrdinaryOwnerError {
    fn from(error: WorkspaceError) -> Self { Self::Sdk(error) }
}

impl From<FsError> for OrdinaryOwnerError {
    fn from(error: FsError) -> Self { Self::Sdk(error.into()) }
}

#[tokio::test]
async fn ordinary_owner_denial_preserves_whole_error_work_and_candidate()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("whole-ordinary-owner", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let base = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let cancellation = CancellationToken::new();
    let mut candidate = workspace.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    candidate.write("/file", bytes::Bytes::from_static(b"candidate"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let callback_work = crate::WorkCounters {
        backend_read_operations: 3,
        ..crate::WorkCounters::UNCHARGED
    };
    let failure = candidate.commit(None, crate::PublicationPermit::Unrestricted, selected_transaction_budget(), &cancellation, |_| std::future::ready(Err(crate::OperationFailure::new(
            OrdinaryOwnerError::Denied { task: [117; 16], revision: 912, expired: true },
            callback_work,
        )))).await.err().ok_or("expired owner published")?;
    assert!(matches!(failure.error,
        OrdinaryOwnerError::Denied { task, revision: 912, expired: true } if task == [117; 16]));
    assert!(failure.work.backend_read_operations >= callback_work.backend_read_operations);
    assert!(failure.work.bytes_hashed > 0);
    assert_eq!(failure.work.authority_records_appended, 0);
    assert_eq!(workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), base.id());
    let failure = async { let mut guard = || std::future::ready(Err(OrdinaryOwnerError::Denied {
            task: [118; 16], revision: 913, expired: false,
        })); candidate.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &cancellation, |_| { let future = guard(); async move { future.await.map(|value| crate::OperationReceipt { value, work: crate::WorkCounters::default() }).map_err(crate::OperationFailure::before_work) } }).await.map(|receipt| receipt.value).map_err(|failure| failure.error) }.await.err().ok_or("denied ordinary owner published")?;
    assert!(matches!(failure,
        OrdinaryOwnerError::Denied { task, revision: 913, expired: false } if task == [118; 16]));
    assert_eq!(workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), base.id());
    assert!(matches!(candidate.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, TransactionCommit::Committed(_)));
    assert_eq!(workspace.read("/file", 32, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"candidate");
    let mut cancelled_candidate = workspace.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    cancelled_candidate.write("/cancelled", bytes::Bytes::from_static(b"not published"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    cancellation.cancel();
    let sdk_failure = async { let mut guard = || std::future::ready(Ok::<(), OrdinaryOwnerError>(())); cancelled_candidate.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &cancellation, |_| { let future = guard(); async move { future.await.map(|value| crate::OperationReceipt { value, work: crate::WorkCounters::default() }).map_err(crate::OperationFailure::before_work) } }).await.map(|receipt| receipt.value).map_err(|failure| failure.error) }.await.err().ok_or("cancelled SDK work published")?;
    assert!(matches!(sdk_failure, OrdinaryOwnerError::Sdk(WorkspaceError::Cancelled(_))));
    Ok(())
}

#[tokio::test]
async fn measured_directory_record_restore_preserves_original_ignored_descendants()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("record-not-subtree-restore", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let mut seed = workspace.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    seed.create_dir_all("/tree", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    seed.set_metadata("/tree", FileMetadata {
        posix_mode: MetadataField::Value(0o700), ..FileMetadata::default()
    }, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    seed.write("/tree/tracked", bytes::Bytes::from_static(b"base"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    seed.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let source = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let mut live = workspace.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    live.set_metadata("/tree", FileMetadata {
        posix_mode: MetadataField::Value(0o755), ..FileMetadata::default()
    }, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    live.write("/tree/tracked", bytes::Bytes::from_static(b"dirty"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    live.write("/tree/ignored", bytes::Bytes::from_static(b"live ignored"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    live.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let current = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let cancellation = CancellationToken::new();
    let budget = selected_transaction_budget();
    let restored = workspace.restore_paths_from(
        &source, &["tree".to_owned()], current.id(), IdempotencyKey::new(),
        crate::PublicationPermit::Unrestricted, budget, &cancellation, |_| async { Ok::<_, crate::OperationFailure<crate::FsError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) },
    ).await?;
    restored.work.verify(budget)?;
    assert!(matches!(restored.value, TransactionCommit::Committed(_)));
    assert_eq!(workspace.stat("/tree", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.metadata.posix_mode, Some(0o700));
    assert_eq!(workspace.read("/tree/tracked", 32, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"dirty");
    assert_eq!(workspace.read("/tree/ignored", 32, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"live ignored");
    Ok(())
}

#[tokio::test]
async fn selected_record_restore_prepares_file_to_directory_privately_before_one_seal()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let source_workspace = fs.create_workspace("source-selected-directory-shape", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let mut source_tx = source_workspace.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    source_tx.create_dir_all("/hot/empty/nested", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    source_tx.set_metadata("/hot", FileMetadata {
        posix_mode: MetadataField::Value(0o750), ..FileMetadata::default()
    }, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    source_tx.write("/hot/value", bytes::Bytes::from_static(b"selected source"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    source_tx.hard_link("/hot/value", "/hot/alias", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    source_tx.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let source = source_workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let workspace = fs.create_workspace("target-selected-directory-shape", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let mut target_tx = workspace.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    target_tx.write("/hot", bytes::Bytes::from_static(b"original file"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    target_tx.hard_link("/hot", "/outside-linked", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    target_tx.write("/outside", bytes::Bytes::from_static(b"user"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    target_tx.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let original = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let paths = vec![
        "hot".to_owned(), "hot/empty".to_owned(), "hot/empty/nested".to_owned(),
        "hot/value".to_owned(), "hot/alias".to_owned(),
    ];
    let budget = selected_transaction_budget();
    let cancellation = CancellationToken::new();
    let rejected = workspace.restore_selected_paths_from(
        &source, &paths, original.id(), IdempotencyKey::new(),
        crate::PublicationPermit::Unrestricted, budget, &cancellation,
        |_| async { Err(crate::OperationFailure::before_work(FsError::MutationNotAllowed)) },
    ).await;
    let Err(rejected) = rejected else { panic!("owner rejection must prevent the only publication"); };
    rejected.work.verify(budget)?;
    assert!(rejected.work.allocation_operations > 0);
    assert_eq!(workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), original.id());
    assert_eq!(workspace.read("/hot", 32, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"original file");
    assert_eq!(workspace.stat("/hot", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.file_id, workspace.stat("/outside-linked", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.file_id);
    let volume = workspace.volume.clone();
    let guard_cancellation = cancellation.clone();
    let original_id = original.id();
    let restored = workspace.restore_selected_paths_from(
        &source, &paths, original.id(), IdempotencyKey::new(),
        crate::PublicationPermit::Unrestricted, budget, &cancellation, move |allowance| {
            let volume = volume.clone();
            let cancellation = guard_cancellation.clone();
            async move {
                let checked = volume.checkout(
                    GenerationSelector::Exact(original_id), CheckoutMode::read_only_pinned(),
                    allowance, &cancellation,
                ).await?;
                drop(checked.value);
                Ok(crate::OperationReceipt { value: (), work: checked.work })
            }
        },
    ).await?;
    restored.work.verify(budget)?;
    assert!(matches!(restored.value, TransactionCommit::Committed(_)));
    assert_eq!(workspace.stat("/hot", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.metadata.posix_mode, Some(0o750));
    assert_eq!(workspace.stat("/hot/empty/nested", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.kind, FileKind::Directory);
    assert!(workspace.list_directory("/hot/empty/nested", None, 1, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.entries.is_empty());
    assert_eq!(workspace.read("/hot/value", 32, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"selected source");
    assert_eq!(workspace.read("/hot/alias", 32, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"selected source");
    assert_eq!(workspace.stat("/hot/value", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.file_id, workspace.stat("/hot/alias", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.file_id);
    assert_eq!(workspace.read("/outside-linked", 32, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"original file");
    assert_eq!(workspace.stat("/outside-linked", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.link_count, 1);
    assert_eq!(workspace.read("/outside", 32, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"user");
    Ok(())
}

#[tokio::test]
async fn selected_record_restore_removes_directory_descendants_before_file_replacement()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let source_workspace = fs.create_workspace("source-selected-file-shape", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    source_workspace.write("/hot", bytes::Bytes::from_static(b"replacement file")).await?;
    let source = source_workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let workspace = fs.create_workspace("target-selected-file-shape", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let mut seed = workspace.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    seed.create_dir_all("/hot/nested/empty", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    seed.write("/hot/nested/old", bytes::Bytes::from_static(b"retained outside alias"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    seed.hard_link("/hot/nested/old", "/outside-linked", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    seed.write("/hot/top", bytes::Bytes::from_static(b"old top"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    seed.write("/outside", bytes::Bytes::from_static(b"user"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    seed.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let original = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let paths = vec![
        "hot/nested/empty".to_owned(), "hot/nested/old".to_owned(),
        "hot/nested".to_owned(), "hot/top".to_owned(), "hot".to_owned(),
    ];
    let cancellation = CancellationToken::new();
    let budget = selected_transaction_budget();
    let rejected = workspace.restore_selected_paths_from(
        &source, &paths, original.id(), IdempotencyKey::new(),
        crate::PublicationPermit::Unrestricted, budget, &cancellation,
        |_| async { Err(crate::OperationFailure::before_work(FsError::MutationNotAllowed)) },
    ).await;
    let Err(rejected) = rejected else { panic!("owner rejection must prevent subtree replacement"); };
    rejected.work.verify(budget)?;
    assert_eq!(workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), original.id());
    assert_eq!(workspace.read("/hot/nested/old", 64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"retained outside alias");
    assert_eq!(workspace.stat("/outside-linked", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.link_count, 2);
    let volume = workspace.volume.clone();
    let guard_cancellation = cancellation.clone();
    let original_id = original.id();
    let restored = workspace.restore_selected_paths_from(
        &source, &paths, original.id(), IdempotencyKey::new(),
        crate::PublicationPermit::Unrestricted, budget, &cancellation, move |allowance| {
            let volume = volume.clone();
            let cancellation = guard_cancellation.clone();
            async move {
                let checked = volume.checkout(
                    GenerationSelector::Exact(original_id), CheckoutMode::read_only_pinned(),
                    allowance, &cancellation,
                ).await?;
                drop(checked.value);
                Ok(crate::OperationReceipt { value: (), work: checked.work })
            }
        },
    ).await?;
    restored.work.verify(budget)?;
    assert!(matches!(restored.value, TransactionCommit::Committed(_)));
    assert_eq!(workspace.read("/hot", 64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"replacement file");
    assert_eq!(workspace.stat("/hot", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.kind, FileKind::Regular);
    assert_eq!(workspace.read("/outside-linked", 64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"retained outside alias");
    assert_eq!(workspace.stat("/outside-linked", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.link_count, 1);
    assert_eq!(workspace.read("/outside", 64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"user");
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, thiserror::Error)]
enum NativeCreationGuardTestError {
    #[error("the owner denied this creation phase: {0}")]
    Denied(u32),
    #[error(transparent)]
    Filesystem(#[from] crate::FsError),
    #[error(transparent)]
    Workspace(#[from] WorkspaceError),
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_guarded_creation_preserves_original_callback_error_and_prepared_work()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(
        selected_transaction_budget(), &cancellation,
    ).map_err(|(error, _)| error)?;
    let before = original.receipt();
    let seals = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed_seals = Arc::clone(&seals);
    let guard_original = original.clone();
    let mut deny = move |observed: &crate::NativeMaterializationWork| {
        assert!(observed.shares_ledger(&guard_original));
        observed_seals.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::future::ready(Err(NativeCreationGuardTestError::Denied(23)))
    };
    let failure = fs.create_workspace_with_operation_guarded_with_native_work(
        "native-guarded-creation-denied", VolumeConfig::portable(Lifecycle::Durable),
        IdempotencyKey::new().operation_id(), &original, &cancellation, &mut deny,
    ).await.err().ok_or("the owner denial unexpectedly created a workspace")?;
    assert!(matches!(failure, NativeCreationGuardTestError::Denied(23)));
    assert!(seals.load(std::sync::atomic::Ordering::Relaxed) > 0);
    assert!(original.receipt().bytes_hashed > before.bytes_hashed);
    assert_eq!(original.receipt().authority_records_appended, before.authority_records_appended);
    assert!(fs.open_workspace("native-guarded-creation-denied", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).is_err());
    original.receipt().verify(selected_transaction_budget())?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_guarded_fork_denial_keeps_prepared_work_without_destination_authority()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("native-guarded-fork-source", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    workspace.write("/selected", bytes::Bytes::from_static(b"original")).await?;
    let frozen = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(
        selected_transaction_budget(), &cancellation,
    ).map_err(|(error, _)| error)?;
    let before = original.receipt();
    let seals = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed_seals = Arc::clone(&seals);
    let guard_original = original.clone();
    let mut deny = move |observed: &crate::NativeMaterializationWork| {
        assert!(observed.shares_ledger(&guard_original));
        observed_seals.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::future::ready(Err(WorkspaceError::from(crate::FsError::MutationNotAllowed)))
    };
    let failure = workspace.fork_with_permit_guarded_with_native_work(
        "native-guarded-fork-denied", ForkOptions::from_generation(frozen, IdempotencyKey::new())
            .inherit_paths(vec!["/selected".to_owned()]),
        crate::PublicationPermit::Unrestricted, &original, &cancellation, &mut deny,
    ).await.err().ok_or("the original owner denial unexpectedly published a destination")?;
    assert!(matches!(failure, WorkspaceError::Filesystem(crate::FsError::MutationNotAllowed)));
    assert!(seals.load(std::sync::atomic::Ordering::Relaxed) > 0);
    assert!(original.receipt().bytes_hashed > before.bytes_hashed);
    assert!(fs.open_workspace("native-guarded-fork-denied", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).is_err());
    original.receipt().verify(selected_transaction_budget())?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_selected_restore_honors_the_real_retained_authority_reservation()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("native-selected-reservation", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    workspace.write("/selected", bytes::Bytes::from_static(b"frozen")).await?;
    let frozen = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    workspace.write("/selected", bytes::Bytes::from_static(b"current")).await?;
    let current = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(
        selected_transaction_budget(), &cancellation,
    ).map_err(|(error, _)| error)?;
    let reservation = workspace.reserve_publication_with_native_work(
        IdempotencyKey::new().operation_id(), &original, &cancellation,
    ).await.map_err(|(error, _)| error)?;
    let before = original.receipt();
    let key = IdempotencyKey::new();
    let paths = vec!["/selected".to_owned()];
    let mut allow = |_: &crate::NativeMaterializationWork| std::future::ready(Ok::<(), WorkspaceError>(()));
    let denied = workspace.restore_selected_paths_from_with_permit_guarded_with_native_work(
        &frozen, &paths, current.id(), key, crate::PublicationPermit::Unrestricted,
        &original, &cancellation, &mut allow,
    ).await;
    assert!(!matches!(denied, Ok(TransactionCommit::Committed(_) | TransactionCommit::AlreadyCommitted(_))));
    assert_eq!(workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), current.id());
    assert_eq!(workspace.read("/selected", 7, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, OwnedBytes::from_static(b"current"));
    assert_eq!(original.receipt().authority_records_appended, before.authority_records_appended);
    let permit = crate::PublicationPermit::Reservation {
        operation_id: reservation.operation_id.into_bytes(),
        gate_tail: reservation.gate_tail,
        expected: reservation.expected,
    };
    let restored = workspace.restore_selected_paths_from_with_permit_guarded_with_native_work(
        &frozen, &paths, current.id(), key, permit, &original, &cancellation, &mut allow,
    ).await?;
    assert!(matches!(restored, TransactionCommit::Committed(_) | TransactionCommit::AlreadyCommitted(_)));
    assert_eq!(workspace.read("/selected", 6, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, OwnedBytes::from_static(b"frozen"));
    workspace.release_publication_with_native_work(reservation, &original, &cancellation)
        .await.map_err(|(error, _)| error)?;
    original.receipt().verify(selected_transaction_budget())?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_guarded_fork_rejects_an_unrelated_source_reservation_without_rebinding_it()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("native-fork-reserved-source", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    workspace.write("/value", bytes::Bytes::from_static(b"frozen")).await?;
    let frozen = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(
        selected_transaction_budget(), &cancellation,
    ).map_err(|(error, _)| error)?;
    let reservation = workspace.reserve_publication_with_native_work(
        IdempotencyKey::new().operation_id(), &original, &cancellation,
    ).await.map_err(|(error, _)| error)?;
    let before = original.receipt().authority_records_appended;
    let permit = crate::PublicationPermit::Reservation {
        operation_id: reservation.operation_id.into_bytes(),
        gate_tail: reservation.gate_tail,
        expected: reservation.expected,
    };
    let mut allow = |_: &crate::NativeMaterializationWork| std::future::ready(Ok::<(), WorkspaceError>(()));
    let failed = workspace.fork_with_permit_guarded_with_native_work(
        "native-fork-reserved-destination", ForkOptions::from_generation(frozen, IdempotencyKey::new()),
        permit, &original, &cancellation, &mut allow,
    ).await.err().ok_or("a source reservation was incorrectly rebound to a new destination")?;
    assert!(matches!(failed,
        WorkspaceError::Filesystem(crate::FsError::Authority(
            crate::AuthorityStoreError::RejectedStatic(_)))));
    assert_eq!(original.receipt().authority_records_appended, before);
    assert!(fs.open_workspace("native-fork-reserved-destination", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).is_err());
    workspace.release_publication_with_native_work(reservation, &original, &cancellation)
        .await.map_err(|(error, _)| error)?;
    Ok(())
}

#[tokio::test]
async fn contextual_operation_record_retains_original_witness_after_head_advances()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("original-context-record", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let cancellation = CancellationToken::new();
    let key = IdempotencyKey::new();
    let original_evidence: &'static [u8] = b"opaque original SDK operation witness";
    let context = crate::Digest::from_bytes([0xc7; 32]);
    let mut transaction = workspace.begin_transaction(key, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction.write("/original", bytes::Bytes::from_static(b"original generation"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let committed = transaction.commit(Some((context, acyclic_stream::OwnedBytes::from_static(original_evidence), crate::PublicationPermit::Unrestricted)), crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| std::future::ready(Ok::<_, crate::OperationFailure<WorkspaceError>>(
            crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED },
        ))).await?;
    let original_generation = match committed.value {
        TransactionCommit::Committed(generation) | TransactionCommit::AlreadyCommitted(generation) => generation,
        _ => return Err("the original contextual operation did not publish".into()),
    };
    workspace.write("/later", bytes::Bytes::from_static(b"later unrelated generation")).await?;
    let current = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert_ne!(current.id(), original_generation.id());
    let observed = workspace.operation_generation_record_measured(
        key, WorkBudget::UNBOUNDED, &cancellation,
    ).await?;
    let (generation, record) = observed.value.ok_or("original operation disappeared")?;
    assert_eq!(generation.id(), original_generation.id());
    assert_eq!(record.operation_id, key.operation_id());
    let mut decode_work = crate::WorkCounters::UNCHARGED;
    let mut allocations = crate::kernel::allocation::AllocationLedger::default();
    let mut accounting = crate::kernel::allocation::KernelAccounting::borrowed_standalone(
        &mut allocations, &mut decode_work, WorkBudget::UNBOUNDED, &cancellation,
    );
    let evidence = crate::kernel::decode_publication_evidence_with_accounting(
        &record.payload, 4 * 1024, &mut accounting,
    )?;
    assert_eq!(evidence.publication.generation_root.digest, original_generation.id().digest());
    assert_eq!(evidence.original_evidence, Some(original_evidence));
    assert!(evidence.evidence_digest.is_some());
    drop(accounting);
    assert!(decode_work.bytes_hashed >= original_evidence.len() as u64);
    assert_eq!(workspace.operation_generation(
        key, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value.ok_or("generation-only projection lost the operation")?.id(), original_generation.id());
    assert!(workspace.operation_generation_record_measured(
        IdempotencyKey::new(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value.is_none());
    Ok(())
}

#[tokio::test]
async fn original_fingerprint_survives_current_reservation_rotation_and_rejects_changed_data()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("original-fingerprint-current-reservation", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let cancellation = CancellationToken::new();
    let key = IdempotencyKey::new();
    let context = crate::Digest::from_bytes([0xc8; 32]);
    let evidence = acyclic_stream::OwnedBytes::from_static(b"the original frozen SDK witness");
    let historical_permit = crate::PublicationPermit::Unrestricted;
    let mut transaction = workspace.begin_transaction(key, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction.write("/original", bytes::Bytes::from_static(b"original content"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let first = workspace.reserve_publication(IdempotencyKey::new().operation_id(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let current_permit = crate::PublicationPermit::Reservation {
        operation_id: first.operation_id.into_bytes(), gate_tail: first.gate_tail, expected: first.expected,
    };
    let mut allow = |_| std::future::ready(Ok::<_, crate::OperationFailure<WorkspaceError>>(
        crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED },
    ));
    let committed = transaction.commit(Some((context, evidence.clone(), historical_permit)), current_permit, WorkBudget::UNBOUNDED, &cancellation, &mut allow).await?;
    let generation = match committed.value {
        TransactionCommit::Committed(generation) | TransactionCommit::AlreadyCommitted(generation) => generation,
        _ => return Err("the current genuine reservation did not authorize publication".into()),
    };
    let (_, record) = workspace.operation_generation_record_measured(
        key, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value.ok_or("the original publication disappeared")?;
    let mut work = crate::WorkCounters::UNCHARGED;
    let mut allocations = crate::kernel::allocation::AllocationLedger::default();
    let mut accounting = crate::kernel::allocation::KernelAccounting::borrowed_standalone(
        &mut allocations, &mut work, WorkBudget::UNBOUNDED, &cancellation,
    );
    let publication = crate::kernel::decode_publication_evidence_with_accounting(
        &record.payload, 4 * 1024, &mut accounting,
    )?;
    let request = crate::kernel::PublishGenerationRequest {
        authority_id: workspace.authority_id(), volume_id: workspace.volume.id,
        epoch: record.epoch,
        expected: crate::Head {
            epoch: record.epoch,
            sequence: crate::Sequence::new(record.sequence.get().checked_sub(1).ok_or("missing original predecessor")?),
            digest: record.previous_digest,
        },
        operation_id: key.operation_id(),
        generation_root: publication.publication.generation_root,
    };
    let historical_fingerprint = crate::kernel::contextual_publication_fingerprint_from_payload_with_accounting(
        request, context, &record.payload, &mut accounting,
    )?;
    assert_eq!(record.fingerprint, historical_fingerprint);
    drop(accounting);
    drop(record);
    workspace.release_publication(first, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let rotated = workspace.reserve_publication(IdempotencyKey::new().operation_id(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let rotated_permit = crate::PublicationPermit::Reservation {
        operation_id: rotated.operation_id.into_bytes(), gate_tail: rotated.gate_tail, expected: rotated.expected,
    };
    assert_ne!(rotated_permit, current_permit);
    let recovered = transaction.checkout.retry_stale_commit(key.operation_id(), rotated_permit, WorkBudget::UNBOUNDED, &cancellation, &mut allow, None, Some(context), Some(evidence.clone()), Some(historical_permit)).await?;
    assert!(matches!(recovered.value, Some(crate::CheckoutCommitOutcome::AlreadyCommitted { generation_id, .. })
        if generation_id == generation.id()));
    let changed_evidence = transaction.checkout.retry_stale_commit(key.operation_id(), rotated_permit, WorkBudget::UNBOUNDED, &cancellation, &mut allow, None, Some(context), Some(acyclic_stream::OwnedBytes::from_static(b"a changed SDK witness")), Some(historical_permit)).await?;
    assert!(matches!(changed_evidence.value, Some(crate::CheckoutCommitOutcome::IdempotencyConflict { .. })));
    transaction.write("/original", bytes::Bytes::from_static(b"changed candidate content"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let changed_payload = transaction.checkout.retry_stale_commit(key.operation_id(), rotated_permit, WorkBudget::UNBOUNDED, &cancellation, &mut allow, None, Some(context), Some(evidence), Some(historical_permit)).await?;
    assert!(matches!(changed_payload.value, Some(crate::CheckoutCommitOutcome::IdempotencyConflict { .. })));
    assert_eq!(workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), generation.id());
    workspace.release_publication(rotated, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    Ok(())
}

#[tokio::test]
async fn original_fingerprint_metadata_never_authorizes_a_foreign_current_reservation()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("original-fingerprint-current-target", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let foreign = fs.create_workspace("original-fingerprint-foreign-current-grant", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let cancellation = CancellationToken::new();
    let original_head = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let key = IdempotencyKey::new();
    let mut transaction = workspace.begin_transaction(key, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction.write("/value", bytes::Bytes::from_static(b"the genuine candidate"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let target_reservation = workspace.reserve_publication(IdempotencyKey::new().operation_id(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let foreign_reservation = foreign.reserve_publication(IdempotencyKey::new().operation_id(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let foreign_permit = crate::PublicationPermit::Reservation {
        operation_id: foreign_reservation.operation_id.into_bytes(),
        gate_tail: foreign_reservation.gate_tail, expected: foreign_reservation.expected,
    };
    let rejected = transaction.commit(Some((crate::Digest::from_bytes([0xc9; 32]), acyclic_stream::OwnedBytes::from_static(b"valid original opaque SDK witness"), crate::PublicationPermit::Unrestricted)), foreign_permit, WorkBudget::UNBOUNDED, &cancellation, |_| std::future::ready(Ok::<_, crate::OperationFailure<WorkspaceError>>(
            crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED },
        ))).await.err().ok_or("historical fingerprint metadata authorized a foreign current grant")?;
    assert!(matches!(rejected.error,
        WorkspaceError::Filesystem(crate::FsError::Authority(crate::AuthorityStoreError::RejectedStatic(_)))));
    assert!(rejected.work.bytes_hashed > 0);
    assert_eq!(rejected.work.authority_records_appended, 0);
    assert_eq!(workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), original_head.id());
    assert!(workspace.operation_generation_record_measured(
        key, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value.is_none());
    workspace.release_publication(target_reservation, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    foreign.release_publication(foreign_reservation, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    Ok(())
}
