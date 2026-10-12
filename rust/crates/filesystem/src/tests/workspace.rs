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
    let pinned = workspace.head().await?;
    let cancellation = CancellationToken::new();
    let before = list_generation_directory_measured(
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
        retained.push(other.head().await?);
    }
    workspace
        .write_text("/late.txt", "not in pinned view")
        .await?;
    let after = list_generation_directory_measured(
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
    let pinned = parent.head().await?;
    let key = IdempotencyKey::new();
    let child = parent
        .fork(
            "scratch-child",
            ForkOptions::from_generation(pinned.clone(), key).empty(),
        )
        .await?;
    let initial = child.head().await?;
    assert_eq!(initial.parents().await?, vec![pinned.id()]);
    assert!(child.list_directory("/", None, 1).await?.entries.is_empty());
    let retried = parent
        .fork(
            "scratch-child",
            ForkOptions::from_generation(pinned.clone(), key).empty(),
        )
        .await?;
    assert_eq!(retried.head().await?.id(), initial.id());
    assert!(
        parent
            .fork("scratch-child", ForkOptions::from_generation(pinned, key))
            .await
            .is_err()
    );
    child.write_text("/new.txt", "child").await?;
    parent.write_text("/concurrent.txt", "user").await?;
    let source = child.head().await?;
    let target = parent.head().await?;
    let plan = child.join_into(&parent).plan_at(&source, &target).await?;
    let options = ApplyOptions {
        if_target: target.id(),
        idempotency_key: IdempotencyKey::new(),
    };
    assert!(matches!(
        plan.apply(options).await?,
        JoinOutcome::Applied(_)
    ));
    assert_eq!(parent.read("/memory.txt", 16).await?.as_ref(), b"parent");
    assert_eq!(parent.read("/concurrent.txt", 16).await?.as_ref(), b"user");
    assert_eq!(parent.read("/new.txt", 16).await?.as_ref(), b"child");
    assert_eq!(child.head().await?.id(), source.id());
    Ok(())
}

#[tokio::test]
async fn selected_fork_import_preserves_omissions_and_detects_selected_conflicts()
-> Result<(), Box<dyn Error>> {
    for paths in [vec!["/notes/keep.txt"], vec!["/notes"]] {
        let fs = Fs::memory();
        let parent = fs.create_workspace("parent", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let mut transaction = parent.begin_transaction(IdempotencyKey::new()).await?;
        transaction.create_directory("/notes").await?;
        transaction.commit().await?;
        parent.write_text("/notes/keep.txt", "base").await?;
        parent.write_text("/notes/other.txt", "other").await?;
        parent.write_text("/excluded.txt", "excluded").await?;
        let pinned = parent.head().await?;
        let child = parent
            .fork(
                "child",
                ForkOptions::from_generation(pinned.clone(), IdempotencyKey::new())
                    .inherit_paths(paths.iter().map(|path| (*path).to_owned()).collect()),
            )
            .await?;
        assert_eq!(child.head().await?.parents().await?, vec![pinned.id()]);
        assert_eq!(child.read("/notes/keep.txt", 16).await?.as_ref(), b"base");
        assert!(child.read("/excluded.txt", 16).await.is_err());
        child.write_text("/notes/keep.txt", "child").await?;
        parent.write_text("/excluded.txt", "updated").await?;
        let source = child.head().await?;
        let target = parent.head().await?;
        let plan = child.join_into(&parent).plan_at(&source, &target).await?;
        assert!(matches!(
            plan.apply(ApplyOptions {
                if_target: target.id(),
                idempotency_key: IdempotencyKey::new()
            })
            .await?,
            JoinOutcome::Applied(_)
        ));
        assert_eq!(parent.read("/excluded.txt", 16).await?.as_ref(), b"updated");
        assert_eq!(
            parent.read("/notes/other.txt", 16).await?.as_ref(),
            b"other"
        );
        assert_eq!(parent.read("/notes/keep.txt", 16).await?.as_ref(), b"child");
        assert_eq!(child.head().await?.id(), source.id());
        child.write_text("/notes/keep.txt", "child again").await?;
        parent.write_text("/notes/keep.txt", "parent again").await?;
        let plan = child.join_into(&parent).plan().await?;
        assert!(matches!(
            plan.apply(ApplyOptions {
                if_target: parent.head().await?.id(),
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
            let pinned = parent.head().await?;
            let paths = (0..3)
                .filter(|index| mask & (1 << index) != 0)
                .map(|index| format!("/file-{index}"))
                .collect();
            let child = parent
                .fork(
                    "subset-child",
                    ForkOptions::from_generation(pinned, IdempotencyKey::new())
                        .inherit_paths(paths),
                )
                .await?;
            let source = if descendant {
                child
                    .fork(
                        "subset-grandchild",
                        ForkOptions::from_generation(child.head().await?, IdempotencyKey::new()),
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
                    assert!(source.read(&path, 16).await.is_err());
                    parent.write_text(&path, "concurrent").await?;
                }
            }
            source.write_text("/added", "new").await?;
            let selected_head = source.head().await?;
            source.write_text("/after-pin", "excluded by pin").await?;
            let source_head = source.head().await?;
            let target = parent.head().await?;
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
                    parent.read(&format!("/file-{index}"), 16).await?.as_ref(),
                    expected.as_bytes()
                );
            }
            assert!(parent.read("/after-pin", 32).await.is_err());
            assert_eq!(source.head().await?.id(), source_head.id());
            let repeated = source
                .join_into(&parent)
                .plan_at(&selected_head, &parent.head().await?)
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
            ForkOptions::from_generation(parent.head().await?, IdempotencyKey::new()),
        )
        .await?;
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
    assert!(parent.read("/omitted", 16).await.is_err());
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
            ForkOptions::from_generation(parent.head().await?, IdempotencyKey::new()).empty(),
        )
        .await?;
    child.write_text("/local", "child").await?;
    parent.write_text("/omitted", "user edit").await?;
    assert!(matches!(
        child.live_rebase(IdempotencyKey::new(), 64, 64, 16).await?,
        WorkspaceRebase::Rebased(_)
    ));
    assert_eq!(child.read("/omitted", 16).await?.as_ref(), b"user edit");
    assert_eq!(child.read("/local", 16).await?.as_ref(), b"child");
    child.write_text("/local", "after rebase").await?;
    let source = child.head().await?;
    let plan = child.join_into(&parent).plan().await?;
    assert!(matches!(
        plan.apply(ApplyOptions {
            if_target: plan.target_head(),
            idempotency_key: IdempotencyKey::new()
        })
        .await?,
        JoinOutcome::Applied(_)
    ));
    assert_eq!(parent.read("/omitted", 16).await?.as_ref(), b"user edit");
    assert_eq!(parent.read("/local", 16).await?.as_ref(), b"after rebase");
    assert_eq!(child.head().await?.id(), source.id());
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
            ForkOptions::from_generation(root.head().await?, IdempotencyKey::new()),
        )
        .await?;
    parent.write_text("/keep", "inherited").await?;
    let child = parent
        .fork(
            "nested-child",
            ForkOptions::from_generation(parent.head().await?, IdempotencyKey::new())
                .inherit_paths(vec!["/keep".into()]),
        )
        .await?;
    // Inherited content is the child baseline; only authored child changes
    // enter a destination. An unchanged selected file is not a child edit.
    child.write_text("/new", "child").await?;
    let plan = child.join_into(&root).plan().await?;
    plan.apply(ApplyOptions {
        if_target: plan.target_head(),
        idempotency_key: IdempotencyKey::new(),
    })
    .await?;
    assert_eq!(root.read("/keep", 32).await?.as_ref(), b"root");
    assert_eq!(root.read("/omit", 32).await?.as_ref(), b"root");
    assert_eq!(root.read("/new", 32).await?.as_ref(), b"child");
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
    assert_eq!(root.read("/keep", 32).await?.as_ref(), b"edited");
    assert_eq!(parent.read("/keep", 32).await?.as_ref(), b"inherited");
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
            let mut transaction = parent.begin_transaction(IdempotencyKey::new()).await?;
            transaction.hard_link("/file", "/alias").await?;
            transaction.commit().await?;
            let mut paths = vec!["/file".to_owned()];
            if carry_alias { paths.push("/alias".to_owned()); }
            let child = parent.fork("hardlink-child", ForkOptions::from_generation(parent.head().await?, IdempotencyKey::new()).inherit_paths(paths)).await?;
            assert_eq!(child.read("/file", 64).await?.as_ref(), bytes.as_slice());
            if carry_alias { assert_eq!(child.read("/alias", 64).await?.as_ref(), bytes.as_slice()); }
            else { assert!(child.read("/alias", 64).await.is_err()); }
            child.write("/file", bytes::Bytes::from_static(b"changed")).await?;
            if carry_alias { assert_eq!(child.read("/alias", 64).await?.as_ref(), b"changed"); }
            assert_eq!(parent.read("/alias", 64).await?.as_ref(), bytes.as_slice());
            Ok(())
        });
        proptest::prop_assert!(result.is_ok(), "{result:?}");
    }
}

#[test]
fn names_are_canonical_bounded_and_path_independent() -> Result<(), Box<dyn Error>> {
    let composed = WorkspaceName::new("caf\u{e9}")?;
    let decomposed = WorkspaceName::new("cafe\u{301}")?;
    assert_eq!(composed, decomposed);
    assert_eq!(composed.as_str(), "caf\u{e9}");
    assert_eq!(WorkspaceName::new(""), Err(WorkspaceNameError::Empty));
    assert_eq!(WorkspaceName::new(".."), Err(WorkspaceNameError::Reserved));
    assert_eq!(
        WorkspaceName::new("parent/child"),
        Err(WorkspaceNameError::InvalidCharacter)
    );
    Ok(())
}

#[test]
fn measured_workspace_names_preserve_nfc_and_actual_output_failure_work()
-> Result<(), Box<dyn Error>> {
    use unicode_normalization::UnicodeNormalization;
    let cancellation = CancellationToken::new();
    for spelling in [
        "cafe\u{301}", "\u{344}", "\u{1100}\u{1161}\u{11a8}",
        "\u{9c7}\u{9be}", "\u{315}\u{300}A", "A\u{30a}\u{301}",
        "a\u{315}\u{300}", "a\u{301}\u{300}",
    ] {
        let expected: String = spelling.nfc().collect();
        let normalized = WorkspaceName::new_measured(
            spelling, WorkBudget::UNBOUNDED, &cancellation,
        )?;
        assert_eq!(normalized.value.as_str(), expected);
        assert_eq!(WorkspaceName::new(spelling)?, normalized.value);
        assert!(normalized.work.peak_allocation_bytes >= normalized.value.retained_bytes());
    }
    let failure = match WorkspaceName::new_measured(
        "a\u{315}\u{300}",
        WorkBudget { allocation_operations: 1, ..WorkBudget::UNBOUNDED },
        &cancellation,
    ) {
        Err(failure) => failure,
        Ok(_) => return Err("normalization output growth unexpectedly admitted".into()),
    };
    assert!(matches!(failure.error, WorkspaceError::Work(_)));
    assert_eq!(failure.work.allocation_operations, 1);
    assert_eq!(failure.work.bytes_copied, 1);
    assert!(failure.work.items_examined > 0);
    assert_eq!(failure.work.peak_allocation_bytes, 1);
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let failure = WorkspaceName::new_measured("name", WorkBudget::UNBOUNDED, &cancelled)
        .err().ok_or("cancelled canonical name unexpectedly admitted")?;
    assert!(matches!(failure.error, WorkspaceError::Cancelled(_)));
    assert_eq!(*failure.work, crate::WorkCounters::UNCHARGED);
    Ok(())
}

#[test]
fn identity_is_deterministic_and_namespace_scoped() -> Result<(), Box<dyn Error>> {
    let name = WorkspaceName::new("repo")?;
    let first = WorkspaceId::derive([1; 16], &name);
    assert_eq!(first, WorkspaceId::derive([1; 16], &name));
    assert_ne!(first, WorkspaceId::derive([2; 16], &name));
    assert_ne!(
        first,
        WorkspaceId::derive([1; 16], &WorkspaceName::new("other")?)
    );
    Ok(())
}

#[tokio::test]
async fn named_workspace_opens_and_forks_one_exact_generation() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = fs.create_workspace("repo", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let base = main.head().await?;
    let fork_key = IdempotencyKey::from_bytes([0x42; 16]);
    let fork = main
        .fork(
            "agent-42",
            ForkOptions::from_generation(base.clone(), fork_key),
        )
        .await?;
    let retried = main
        .fork(
            "agent-42",
            ForkOptions::from_generation(base.clone(), fork_key),
        )
        .await?;
    assert_eq!(fs.open_workspace("repo", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), main.id());
    assert_ne!(fork.id(), main.id());
    assert_ne!(fork.head().await?.id(), base.id());
    assert_eq!(retried.id(), fork.id());
    assert_eq!(retried.head().await?.id(), fork.head().await?.id());
    assert!(
        main.fork(
            "agent-42",
            ForkOptions::from_generation(base, IdempotencyKey::from_bytes([0x43; 16])),
        )
        .await
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
        workspace.write_text("/Ã©.txt", "native").await?;
        let mut checkout = workspace
            .engine_checkout(GenerationSelector::Head, CheckoutMode::read_only_pinned())
            .await?;
        let path = NamespacePath::from_portable_in_profile(
            &PortablePath::parse("/Ã©.txt", config.limits)?,
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
    let mut create = workspace.begin_transaction(IdempotencyKey::new()).await?;
    create.write_text("/file.txt", "current").await?;
    assert!(matches!(
        create.commit().await?,
        TransactionCommit::Committed(_)
    ));
    let actual = workspace.stat("/file.txt").await?.file_id;
    let mut stale = FileId::new();
    while stale == actual {
        stale = FileId::new();
    }
    let mut remove = workspace.begin_transaction(IdempotencyKey::new()).await?;
    assert!(matches!(
        remove.remove_if("/file.txt", stale).await,
        Err(WorkspaceError::StaleIdentity)
    ));
    assert_eq!(workspace.read("/file.txt", 64).await?.as_ref(), b"current");
    Ok(())
}

#[tokio::test]
async fn exact_generation_restore_is_fenced_and_idempotent() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("restore-exact", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    workspace.write_text("/state", "first").await?;
    let first = workspace.head().await?;
    workspace.write_text("/state", "second").await?;
    let second = workspace.head().await?;
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
    assert_eq!(workspace.head().await?.id(), first.id());
    Ok(())
}

#[tokio::test]
async fn path_restore_reuses_exact_records_and_preserves_unselected_state()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("restore-paths", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let mut transaction = workspace
        .begin_transaction(IdempotencyKey::from_bytes([0x60; 16]))
        .await?;
    transaction.create_dir_all("/tree").await?;
    transaction
        .create_file(
            "/tree/tracked",
            bytes::Bytes::from_static(b"committed"),
            FileMetadata::default(),
        )
        .await?;
    transaction
        .hard_link("/tree/tracked", "/tree/ignored-link")
        .await?;
    assert!(matches!(
        transaction.commit().await?,
        TransactionCommit::Committed(_)
    ));
    workspace.write_text("/ignored", "live").await?;
    let committed = workspace.head().await?;
    let snapshot = workspace
        .fork(
            "restore-paths-snapshot",
            ForkOptions::from_generation(committed, IdempotencyKey::from_bytes([0x61; 16])),
        )
        .await?;
    snapshot.remove("/ignored").await?;
    let source = snapshot.head().await?;

    workspace.write_text("/tree/tracked", "dirty").await?;
    workspace.write_text("/ignored", "still-live").await?;
    let dirty = workspace.head().await?;
    let outcome = workspace
        .restore_paths_from(
            &source,
            &["tree/tracked".to_owned()],
            dirty.id(),
            IdempotencyKey::from_bytes([0x62; 16]),
        )
        .await?;
    assert!(matches!(outcome, TransactionCommit::Committed(_)));
    assert_eq!(
        workspace.read("/tree/tracked", 64).await?,
        OwnedBytes::from_static(b"committed")
    );
    assert_eq!(
        workspace.read("/tree/ignored-link", 64).await?,
        OwnedBytes::from_static(b"committed")
    );
    assert_eq!(
        workspace.read("/ignored", 64).await?,
        OwnedBytes::from_static(b"still-live")
    );
    let tracked = workspace.stat("/tree/tracked").await?;
    let linked = workspace.stat("/tree/ignored-link").await?;
    assert_eq!(tracked.file_id, linked.file_id);
    assert_eq!(tracked.link_count, 2);
    Ok(())
}

#[tokio::test]
async fn path_apply_is_three_way_conflict_checked() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("apply-paths", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    workspace.write_text("/file", "base").await?;
    let base = workspace.head().await?;
    let source_workspace = workspace
        .fork(
            "apply-paths-source",
            ForkOptions::from_generation(base.clone(), IdempotencyKey::from_bytes([0x63; 16])),
        )
        .await?;
    source_workspace.write_text("/file", "source").await?;
    let source = source_workspace.head().await?;
    let current = workspace.head().await?;
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
        workspace.read("/file", 64).await?,
        OwnedBytes::from_static(b"source")
    );

    let conflicting = fs.create_workspace("apply-paths-conflict", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    conflicting.write_text("/file", "base").await?;
    let conflict_base = conflicting.head().await?;
    let conflict_source = conflicting
        .fork(
            "apply-paths-conflict-source",
            ForkOptions::from_generation(
                conflict_base.clone(),
                IdempotencyKey::from_bytes([0x65; 16]),
            ),
        )
        .await?;
    conflict_source.write_text("/file", "source").await?;
    let conflict_source = conflict_source.head().await?;
    conflicting.write_text("/file", "ours").await?;
    let conflict_current = conflicting.head().await?;
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
    let parent_base = parent.head().await?;
    let child = parent
        .fork(
            "incremental-child",
            ForkOptions::from_generation(
                parent_base.clone(),
                IdempotencyKey::from_bytes([0x67; 16]),
            ),
        )
        .await?;
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
    assert_eq!(parent.read("/first.txt", 64).await?.as_ref(), b"first");
    assert_eq!(parent.read("/second.txt", 64).await?.as_ref(), b"second");
    Ok(())
}

#[tokio::test]
async fn public_workspace_checkout_opens_pinned_and_live_private_views()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("public-checkout", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let head = workspace.head().await?;
    let pinned = workspace
        .checkout(GenerationSelector::Head, CheckoutMode::read_only_pinned())
        .await?;
    assert_eq!(pinned.generation_id(), head.id());
    let live_private = workspace
        .checkout(
            GenerationSelector::Head,
            CheckoutMode {
                access: crate::model::AccessMode::ReadWrite,
                consistency: crate::model::ConsistencyMode::Live,
                mutations: crate::model::MutationMode::PrivateOverlay,
            },
        )
        .await?;
    assert_eq!(live_private.generation_id(), head.id());
    Ok(())
}

#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
#[tokio::test]
async fn public_generation_materialize_path_is_a_complete_consumer_flow()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("materialize-consumer", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let mut transaction = workspace.begin_transaction(IdempotencyKey::new()).await?;
    transaction.create_dir_all("/output/nested").await?;
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
            },
        )
        .await?;
    let TransactionCommit::Committed(generation) = transaction.commit().await? else {
        return Err("materialize fixture did not commit".into());
    };
    let destination = tempfile::tempdir()?;
    let receipt = generation
        .materialize_path(
            "/output/nested/status",
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
        .materialize_path(
            "/output/nested",
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
    let mut transaction = workspace.begin_transaction(IdempotencyKey::new()).await?;
    transaction.create_dir_all("/nested").await?;
    transaction.write_text("/nested/a", "linked").await?;
    transaction.hard_link("/nested/a", "/nested/b").await?;
    transaction.write_text("/nested/c", "sibling").await?;
    let TransactionCommit::Committed(generation) = transaction.commit().await? else {
        return Err("materialize fixture did not commit".into());
    };
    let destination = tempfile::tempdir()?;
    let receipt = generation
        .materialize_paths(
            &[
                "/nested/a".to_owned(),
                "/nested/b".to_owned(),
                "/nested/c".to_owned(),
            ],
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
    let mut transaction = workspace.begin_transaction(IdempotencyKey::new()).await?;
    transaction.create_dir_all("/nested").await?;
    transaction
        .write_text("/nested/file", "authenticated")
        .await?;
    let TransactionCommit::Committed(generation) = transaction.commit().await? else {
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
    let generation = workspace.head().await?;
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
    let generation = workspace.head().await?;
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
    let mut transaction = source.begin_transaction(IdempotencyKey::new()).await?;
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
    let unpublished = source.generation(checkpoint).await?;
    let child = source
        .fork(
            "checkpoint-child",
            ForkOptions::from_generation(unpublished, IdempotencyKey::from_bytes([0x44; 16])),
        )
        .await?;

    assert_eq!(
        child.read("/candidate-only", 64).await?,
        OwnedBytes::from_static(b"candidate")
    );
    assert!(source.read("/candidate-only", 64).await.is_err());
    Ok(())
}

#[tokio::test]
async fn transaction_is_atomic_idempotent_and_visible_to_forks() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("repo", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let key = IdempotencyKey::new();
    let mut transaction = workspace.begin_transaction(key).await?;
    transaction.create_dir_all("/output/nested").await?;
    transaction
        .write_text("/output/nested/status", "ready")
        .await?;
    let TransactionCommit::Committed(generation) = transaction.commit().await? else {
        return Err("first publication did not commit".into());
    };
    assert_eq!(
        workspace.read("/output/nested/status", 64).await?,
        OwnedBytes::from_static(b"ready")
    );
    let reopened = workspace.generation(generation.id()).await?;
    assert_eq!(
        reopened.read("/output/nested/status", 64).await?,
        OwnedBytes::from_static(b"ready")
    );
    let fork = workspace
        .fork(
            "copy",
            ForkOptions::from_generation(generation.clone(), IdempotencyKey::new()),
        )
        .await?;
    assert_eq!(
        fork.read("/output/nested/status", 64).await?,
        OwnedBytes::from_static(b"ready")
    );

    let mut exact_retry = workspace.begin_transaction(key).await?;
    exact_retry.create_dir_all("/output/nested").await?;
    exact_retry
        .write_text("/output/nested/status", "ready")
        .await?;
    assert!(matches!(
        exact_retry.commit().await?,
        TransactionCommit::AlreadyCommitted(retried) if retried.id() == generation.id()
    ));

    let mut conflicting_retry = workspace.begin_transaction(key).await?;
    conflicting_retry
        .write_text("/output/nested/status", "different")
        .await?;
    assert!(matches!(
        conflicting_retry.commit().await?,
        TransactionCommit::IdempotencyConflict
    ));
    Ok(())
}

#[tokio::test]
async fn retained_transaction_rebases_disjoint_work_and_rejects_overlap()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("transaction-rebase", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;

    let mut first = workspace.begin_transaction(IdempotencyKey::new()).await?;
    let mut disjoint = workspace.begin_transaction(IdempotencyKey::new()).await?;
    first.write_text("/first", "one").await?;
    disjoint.write_text("/second", "two").await?;
    assert!(matches!(
        first.commit().await?,
        TransactionCommit::Committed(_)
    ));
    assert!(matches!(
        disjoint.commit().await?,
        TransactionCommit::Conflict { .. }
    ));
    assert!(matches!(
        disjoint.rebase(16).await?,
        TransactionRebase::Rebased(_)
    ));
    assert!(matches!(
        disjoint.commit().await?,
        TransactionCommit::Committed(_)
    ));
    assert_eq!(
        workspace.read("/first", 8).await?,
        OwnedBytes::from_static(b"one")
    );
    assert_eq!(
        workspace.read("/second", 8).await?,
        OwnedBytes::from_static(b"two")
    );

    let mut winner = workspace.begin_transaction(IdempotencyKey::new()).await?;
    let mut overlap = workspace.begin_transaction(IdempotencyKey::new()).await?;
    winner.write_text("/first", "winner").await?;
    overlap.write_text("/first", "loser").await?;
    assert!(matches!(
        winner.commit().await?,
        TransactionCommit::Committed(_)
    ));
    assert!(matches!(
        overlap.commit().await?,
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
        workspace.read("/first", 8).await?,
        OwnedBytes::from_static(b"winner")
    );
    Ok(())
}

#[tokio::test]
async fn exact_generation_transaction_is_stateless_and_rebases_safely() -> Result<(), Box<dyn Error>>
{
    let fs = Fs::memory();
    let workspace = fs.create_workspace("exact-transaction", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let base = workspace.head().await?;

    workspace.write_text("/upstream", "one").await?;
    let mut transaction = workspace
        .begin_transaction_at(&base, IdempotencyKey::new())
        .await?;
    transaction.write_text("/local", "two").await?;
    assert!(matches!(
        transaction.commit().await?,
        TransactionCommit::Conflict { .. }
    ));
    assert!(matches!(
        transaction.rebase(16).await?,
        TransactionRebase::Rebased(_)
    ));
    assert!(matches!(
        transaction.commit().await?,
        TransactionCommit::Committed(_)
    ));
    assert_eq!(
        workspace.read("/upstream", 8).await?,
        OwnedBytes::from_static(b"one")
    );
    assert_eq!(
        workspace.read("/local", 8).await?,
        OwnedBytes::from_static(b"two")
    );

    let other = fs.create_workspace("other-exact-transaction", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let foreign = other.head().await?;
    assert!(matches!(
        workspace
            .begin_transaction_at(&foreign, IdempotencyKey::new())
            .await,
        Err(WorkspaceError::ForeignGeneration)
    ));
    Ok(())
}

#[tokio::test]
async fn transaction_preserves_links_sparse_ranges_metadata_and_cow_clones()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("shapes", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let mut transaction = workspace.begin_transaction(IdempotencyKey::new()).await?;
    transaction.create_directory("/tree").await?;
    transaction
        .write("/tree/source", bytes::Bytes::from_static(b"abcdef"))
        .await?;
    transaction
        .write("/tree/destination", bytes::Bytes::from_static(b"......"))
        .await?;
    transaction
        .create_symbolic_link("/tree/symlink", bytes::Bytes::from_static(b"source"))
        .await?;
    transaction
        .hard_link("/tree/source", "/tree/hard-link")
        .await?;
    transaction
        .write_range("/tree/source", 1, bytes::Bytes::from_static(b"Z"))
        .await?;
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
        .set_metadata("/tree/source", FileMetadata::default())
        .await?;
    transaction.resize("/tree/destination", 8).await?;
    assert!(matches!(
        transaction.commit().await?,
        TransactionCommit::Committed(_)
    ));
    let source = OwnedBytes::from_static(b"aZ\0\0ef");
    assert_eq!(workspace.read("/tree/source", 32).await?, source);
    assert_eq!(workspace.read("/tree/hard-link", 32).await?, source);
    assert_eq!(
        workspace.read("/tree/destination", 32).await?,
        OwnedBytes::from_static(b".aZ\0\0e\0\0")
    );
    Ok(())
}

#[tokio::test]
async fn customer_reads_are_bounded_sparse_link_aware_and_generation_exact()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("read-shapes", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let mut transaction = workspace.begin_transaction(IdempotencyKey::new()).await?;
    transaction.create_directory("/tree").await?;
    transaction
        .write("/tree/a", bytes::Bytes::from_static(b"abcdef"))
        .await?;
    transaction
        .write("/tree/sparse", bytes::Bytes::from(vec![1; 300]))
        .await?;
    transaction.hard_link("/tree/a", "/tree/b").await?;
    transaction
        .create_symbolic_link("/tree/link", bytes::Bytes::from_static(b"a"))
        .await?;
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
    let TransactionCommit::Committed(exact) = transaction.commit().await? else {
        return Err("shape publication did not commit".into());
    };

    let file_id = workspace.stat("/tree/a").await?.file_id;
    let directory_id = workspace.stat("/tree").await?.file_id;
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
        workspace.read_range("/tree/a", 1, 5).await?,
        OwnedBytes::from_static(b"b\0\0ef")
    );
    let a = workspace.stat("/tree/a").await?;
    let b = workspace.stat("/tree/b").await?;
    assert_eq!(a.file_id, b.file_id);
    assert_eq!(a.link_count, 2);
    assert_eq!(a.logical_bytes, Some(8));
    assert_eq!(a.kind, FileKind::Regular);
    assert_eq!(
        workspace.read_symbolic_link("/tree/link").await?,
        OwnedBytes::from_static(b"a")
    );

    let listing = workspace.sync().await?.into_generation();
    let first = listing.list_directory("/tree", None, 1).await?;
    assert_eq!(first.entries.len(), 1);
    assert!(first.has_more);
    workspace.write_text("/tree/late", "new").await?;
    let second = listing
        .list_directory("/tree", Some(&first.entries[0].name), 16)
        .await?;
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
        exact.read("/tree/a", 16).await?,
        OwnedBytes::from_static(b"ab\0\0ef\0\0")
    );
    assert_eq!(
        workspace.read("/tree/a", 16).await?,
        OwnedBytes::from_static(b"changed")
    );
    Ok(())
}

#[tokio::test]
async fn checkpoint_retains_the_exact_current_generation() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("repo", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let checkpoint = workspace.checkpoint("baseline").await?;
    assert_eq!(checkpoint.label().as_str(), "baseline");
    assert_eq!(checkpoint.generation().id(), workspace.head().await?.id());
    let retried = workspace.checkpoint("baseline").await?;
    assert_eq!(retried.generation().id(), checkpoint.generation().id());
    Ok(())
}

#[tokio::test]
async fn explicit_pin_is_exact_and_conflicting_reuse_fails_closed() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("repo", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let first = workspace.head().await?;
    let pin = first.pin("deployment").await?;
    assert_eq!(pin.identity().as_str(), "deployment");
    assert_eq!(pin.generation().id(), first.id());
    assert_eq!(first.pin("deployment").await?.generation().id(), first.id());

    let mut transaction = workspace.begin_transaction(IdempotencyKey::new()).await?;
    transaction.write_text("/changed", "yes").await?;
    let TransactionCommit::Committed(second) = transaction.commit().await? else {
        return Err("second generation did not commit".into());
    };
    assert!(matches!(
        second.pin("deployment").await,
        Err(WorkspaceError::RetentionConflict)
    ));
    Ok(())
}

#[tokio::test]
async fn deletion_is_terminal_and_does_not_invalidate_a_fork() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("repo", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let generation = workspace.head().await?;
    let fork = workspace
        .fork(
            "survivor",
            ForkOptions::from_generation(generation, IdempotencyKey::new()),
        )
        .await?;
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
    assert_eq!(fork.head().await?.workspace_id(), fork.id());
    Ok(())
}

#[tokio::test]
async fn side_effect_free_join_combines_independent_fork_and_target_changes()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = fs.create_workspace("main", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    main.write_text("/base", "base").await?;
    let base = main.head().await?;
    let agent = main
        .fork(
            "agent",
            ForkOptions::from_generation(base, IdempotencyKey::new()),
        )
        .await?;
    agent.write_text("/agent", "agent").await?;
    main.write_text("/main", "main").await?;

    let plan = agent.join_into(&main).plan().await?;
    let expected = plan.target_head();
    let idempotency_key = IdempotencyKey::new();
    assert!(main.read("/agent", 16).await.is_err());
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
        main.operation_generation(idempotency_key)
            .await?
            .ok_or("join operation was not recoverable")?
            .id(),
        joined.id()
    );
    assert!(
        main.operation_generation(IdempotencyKey::new())
            .await?
            .is_none()
    );
    assert_eq!(joined.read("/base", 16).await?, OwnedBytes::from_static(b"base"));
    assert_eq!(main.read("/agent", 16).await?, OwnedBytes::from_static(b"agent"));
    assert_eq!(main.read("/main", 16).await?, OwnedBytes::from_static(b"main"));
    assert_eq!(
        agent.read("/agent", 16).await?,
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
    let mut ordinary = main.begin_transaction(ordinary_key).await?;
    ordinary.write_text("/ordinary", "not a join").await?;
    assert!(matches!(
        ordinary.commit().await?,
        TransactionCommit::Committed(_)
    ));
    let ordinary_generation = main
        .operation_generation(ordinary_key)
        .await?
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
    let base = main.head().await?;
    let agent = main
        .fork(
            "agent-exact",
            ForkOptions::from_generation(base, IdempotencyKey::new()),
        )
        .await?;
    agent.write_text("/agent", "agent").await?;
    let source = agent.head().await?;
    let target = main.head().await?;
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
            .plan_at(&target, &main.head().await?)
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
    let base = main.head().await?;
    let left = main
        .fork(
            "lineage-left",
            ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
        )
        .await?;
    let right = main
        .fork(
            "lineage-right",
            ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
        )
        .await?;

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
            ForkOptions::from_generation(main.head().await?, IdempotencyKey::new()),
        )
        .await?;
    agent.write_text("/agent", "local").await?;
    main.write_text("/upstream", "first").await?;

    let first_key = IdempotencyKey::new();
    let WorkspaceRebase::Rebased(first) = agent.live_rebase(first_key, 128, 128, 32).await? else {
        return Err("first live rebase did not publish".into());
    };
    assert_eq!(
        first.read("/agent", 16).await?,
        OwnedBytes::from_static(b"local")
    );
    assert_eq!(
        first.read("/upstream", 16).await?,
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
        agent.read("/later", 16).await?,
        OwnedBytes::from_static(b"second")
    );
    assert_eq!(
        agent.read("/agent", 16).await?,
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
            ForkOptions::from_generation(main.head().await?, IdempotencyKey::new()),
        )
        .await?;
    agent.write_text("/shared", "local").await?;
    let before = agent.head().await?.id();
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
    assert_eq!(agent.head().await?.id(), before);
    assert_eq!(
        agent.read("/shared", 16).await?,
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
            ForkOptions::from_generation(main.head().await?, IdempotencyKey::new()),
        )
        .await?;
    let mut local = agent.begin_transaction(IdempotencyKey::new()).await?;
    local
        .write_range("/shared", 1, bytes::Bytes::from_static(b"XY"))
        .await?;
    assert!(matches!(
        local.commit().await?,
        TransactionCommit::Committed(_)
    ));
    let mut upstream = main.begin_transaction(IdempotencyKey::new()).await?;
    upstream
        .write_range("/shared", 64, bytes::Bytes::from_static(b"UV"))
        .await?;
    assert!(matches!(
        upstream.commit().await?,
        TransactionCommit::Committed(_)
    ));
    assert!(matches!(
        agent
            .live_rebase(IdempotencyKey::new(), 128, 128, 32)
            .await?,
        WorkspaceRebase::Rebased(_)
    ));
    let merged = agent.read("/shared", 128).await?;
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
    let base = main.head().await?;
    let agent = main
        .fork(
            "agent",
            ForkOptions::from_generation(base, IdempotencyKey::new()),
        )
        .await?;
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
    assert!(main.read("/agent", 16).await.is_err());
    assert_eq!(
        main.read("/raced", 16).await?,
        OwnedBytes::from_static(b"winner")
    );
    Ok(())
}

#[tokio::test]
async fn semantic_change_set_and_overlap_conflict_are_exact() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = fs.create_workspace("main", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    main.write_text("/shared", "base").await?;
    let base = main.head().await?;
    let agent = main
        .fork(
            "agent",
            ForkOptions::from_generation(base, IdempotencyKey::new()),
        )
        .await?;
    let agent_base = agent.head().await?;
    agent.write_text("/shared", "agent").await?;
    let agent_head = agent.head().await?;
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
    assert_eq!(main.read("/shared", 16).await?, OwnedBytes::from_static(b"main"));
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
            ForkOptions::from_generation(main.head().await?, IdempotencyKey::new()),
        )
        .await?;
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
        main.read("/shared", 16).await?,
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
            ForkOptions::from_generation(main.head().await?, IdempotencyKey::new()),
        )
        .await?;
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
    let merged = String::from_utf8(main.read("/shared", 1024).await?.to_vec())?;
    assert!(merged.contains("<<<<<<< ours"));
    assert!(merged.contains("main"));
    assert!(merged.contains("agent"));
    Ok(())
}

#[tokio::test]
async fn contiguous_change_sets_compose_by_outer_semantics() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("compose", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let base = workspace.head().await?;
    workspace.write_text("/before", "body").await?;
    let middle = workspace.head().await?;
    let mut transaction = workspace.begin_transaction(IdempotencyKey::new()).await?;
    transaction.rename("/before", "/after").await?;
    let TransactionCommit::Committed(end) = transaction.commit().await? else {
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
    let mut initial = workspace.begin_transaction(IdempotencyKey::new()).await?;
    initial.create_dir_all("/nested/deep").await?;
    initial
        .write("/rename", bytes::Bytes::from_static(b"same"))
        .await?;
    initial
        .write("/remove", bytes::Bytes::from_static(b"gone"))
        .await?;
    initial
        .write("/linked", bytes::Bytes::from_static(b"links"))
        .await?;
    initial
        .write("/modified", bytes::Bytes::from_static(b"before"))
        .await?;
    assert!(matches!(
        initial.commit().await?,
        TransactionCommit::Committed(_)
    ));
    let before = workspace.head().await?;

    let mut changed = workspace.begin_transaction(IdempotencyKey::new()).await?;
    changed.rename("/rename", "/renamed").await?;
    changed.remove("/remove").await?;
    changed.hard_link("/linked", "/nested/link").await?;
    changed
        .write("/nested/deep/added", bytes::Bytes::from_static(b"new"))
        .await?;
    changed
        .write("/modified", bytes::Bytes::from_static(b"after"))
        .await?;
    let TransactionCommit::Committed(after) = changed.commit().await? else {
        return Err("changed paths did not commit".into());
    };

    let paths = before.diff_to(&after, 64).await?.changed_paths(64).await?;
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
        before.diff_to(&after, 64).await?.changed_paths(1).await,
        Err(WorkspaceError::ChangedPathLimit)
    ));
    let exact = before.diff_to(&after, 64).await?;
    let mut truncated_changes = exact.changes().clone();
    truncated_changes.truncated = true;
    let truncated = ChangeSet {
        from: before.clone(),
        to: after.clone(),
        changes: truncated_changes,
        work: exact.work(),
    };
    assert!(matches!(
        truncated.changed_paths(64).await,
        Err(WorkspaceError::ChangedPathLimit)
    ));
    assert!(
        after
            .diff_to(&after, 1)
            .await?
            .changed_paths(0)
            .await?
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
        assert_eq!(link.file_id, after.stat("/linked").await?.file_id);
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
async fn changed_path_queries_reuse_the_sdk_generation_index() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("changed-path-index", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    for batch in 0..8 {
        let mut initial = workspace.begin_transaction(IdempotencyKey::new()).await?;
        for offset in 0..16 {
            let index = batch * 16 + offset;
            initial
                .write(
                    &format!("/unrelated-{index:04}"),
                    bytes::Bytes::from_static(b"unchanged"),
                )
                .await?;
        }
        assert!(matches!(
            initial.commit().await?,
            TransactionCommit::Committed(_)
        ));
    }
    let mut initial = workspace.begin_transaction(IdempotencyKey::new()).await?;
    initial
        .write("/selected", bytes::Bytes::from_static(b"before"))
        .await?;
    assert!(matches!(
        initial.commit().await?,
        TransactionCommit::Committed(_)
    ));
    let before = workspace.head().await?;
    workspace.write_text("/selected", "after").await?;
    let after = workspace.head().await?;
    let changes = workspace.diff(&before, &after, 1_024).await?;
    let cancellation = crate::CancellationToken::new();
    let cold = changes
        .changed_paths_bounded(1_024, crate::WorkBudget::UNBOUNDED, &cancellation)
        .await?;
    let warm = changes
        .changed_paths_bounded(1_024, crate::WorkBudget::UNBOUNDED, &cancellation)
        .await?;
    assert_eq!(warm.value, cold.value);
    assert!(warm.work.page_reads < cold.work.page_reads);
    assert!(warm.work.items_examined < cold.work.items_examined);
    Ok(())
}

#[tokio::test]
async fn measured_workspace_helpers_retain_lookup_and_planning_failure_work()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("measured-helper-failures", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let before = workspace.head().await?;
    workspace.write_text("/file", "body").await?;
    let generation = workspace.head().await?;
    let cancellation = CancellationToken::new();
    let budget = WorkBudget::UNBOUNDED;

    let stat = match generation.stat_measured("/missing", budget, &cancellation).await {
        Err(failure) => failure,
        Ok(_) => return Err("missing stat unexpectedly succeeded".into()),
    };
    assert!(matches!(stat.error, WorkspaceError::NotFound));
    assert!(stat.work.backend_read_operations > 0);
    let read = match generation.read_measured("/file", 1, budget, &cancellation).await {
        Err(failure) => failure,
        Ok(_) => return Err("limited read unexpectedly succeeded".into()),
    };
    assert!(matches!(read.error, WorkspaceError::ReadLimitExceeded));
    assert!(read.work.backend_read_operations > 0);
    let listed = generation.list_directory_measured("/file", None, 16, budget, &cancellation).await;
    let listed = match listed {
        Err(failure) => failure,
        Ok(_) => return Err("regular file unexpectedly listed".into()),
    };
    assert!(listed.work.backend_read_operations > 0);

    let changes = before.diff_to_bounded(&generation, 64, budget, &cancellation).await?;
    let diff_work = changes.work();
    let failed_paths = match changes.changed_paths_bounded(0, budget, &cancellation).await {
        Err(failure) => failure,
        Ok(_) => return Err("zero path bound unexpectedly succeeded".into()),
    };
    assert!(matches!(failed_paths.error, WorkspaceError::ChangedPathLimit));
    assert_eq!(*failed_paths.work, diff_work);
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let failed_paths = match changes.changed_paths_bounded(64, budget, &cancelled).await {
        Err(failure) => failure,
        Ok(_) => return Err("cancelled planning unexpectedly succeeded".into()),
    };
    assert!(matches!(failed_paths.error, WorkspaceError::Cancelled(_)));
    assert_eq!(*failed_paths.work, diff_work);

    let no_more_allocations = WorkBudget {
        allocation_operations: diff_work.allocation_operations,
        ..WorkBudget::UNBOUNDED
    };
    let failed_paths = match changes.changed_paths_bounded(
        64, no_more_allocations, &cancellation,
    ).await {
        Err(failure) => failure,
        Ok(_) => return Err("changed-path vector allocation unexpectedly admitted".into()),
    };
    assert!(matches!(failed_paths.error, WorkspaceError::Work(_)));
    assert_eq!(failed_paths.work.allocation_operations, diff_work.allocation_operations);
    assert!(failed_paths.work.items_examined > diff_work.items_examined);
    assert!(failed_paths.work.peak_allocation_bytes >= changes.retained_bytes());
    let resolved = changes.changed_paths_bounded(64, budget, &cancellation).await?;
    assert_eq!(resolved.value.len(), 1);
    assert_eq!(super::namespace_path_text(&resolved.value[0].path)?, "/file");
    let output_bytes = (resolved.value.capacity() * std::mem::size_of::<ChangedPath>()) as u64
        + resolved.value.iter().map(|changed| changed.path.retained_bytes()).sum::<u64>();
    assert!(resolved.work.peak_allocation_bytes >= changes.retained_bytes() + output_bytes);

    let mut transaction = workspace.begin_transaction(IdempotencyKey::new()).await?;
    let authored = transaction
        .create_file_measured("/missing/child", OwnedBytes::new(), budget, &cancellation).await;
    let authored = match authored {
        Err(failure) => failure,
        Ok(_) => return Err("absent parent unexpectedly accepted".into()),
    };
    assert_ne!(*authored.work, crate::WorkCounters::default());
    let directories = transaction.create_dir_all_measured("/file/child", budget, &cancellation).await;
    let directories = match directories {
        Err(failure) => failure,
        Ok(_) => return Err("regular parent unexpectedly accepted".into()),
    };
    assert!(matches!(directories.error, WorkspaceError::NotDirectory));
    assert_ne!(*directories.work, crate::WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn generation_lookup_paths_preserves_order_absence_and_duplicates()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("lookup-paths", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    workspace.write_text("/present", "body").await?;
    let generation = workspace.head().await?;
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
    let mut transaction = workspace.begin_transaction(IdempotencyKey::new()).await?;
    transaction.write_text("/a", "body").await?;
    transaction.hard_link("/a", "/b").await?;
    assert!(matches!(
        transaction.commit().await?,
        TransactionCommit::Committed(_)
    ));
    let generation = workspace.head().await?;
    let file_id = generation.stat("/a").await?.file_id;

    let complete = generation
        .namespace_records_for_file_ids([file_id], 16)
        .await?;
    assert!(complete.complete);
    assert_eq!(complete.records.iter().find(|(id, _)| **id == file_id).map(|(_, records)| records.len()), Some(2));

    let bounded = generation
        .namespace_records_for_file_ids([file_id], 1)
        .await?;
    assert!(!bounded.complete);
    assert_eq!(bounded.records.iter().find(|(id, _)| **id == file_id).map(|(_, records)| records.len()), Some(1));
    Ok(())
}

#[tokio::test]
async fn generation_reader_resolves_many_paths_from_one_pinned_root() -> Result<(), Box<dyn Error>>
{
    let fs = Fs::memory();
    let workspace = fs.create_workspace("generation-reader", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    workspace.write_text("/present", "body").await?;
    let generation = workspace.head().await?;
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
    let advanced = workspace.head().await?.id();
    assert_ne!(advanced, created.id());
    let original = fs.workspace_for_creation_operation(
        "readonly-original-creation", config, operation, budget, &cancellation,
    ).await?;
    original.work.verify(budget)?;
    assert_eq!(original.value.as_ref().map(|generation| generation.id()), Some(created.id()));
    assert_eq!(original.value.as_ref().map(|generation| generation.workspace_id()), Some(workspace.id()));
    assert_eq!(original.work.authority_records_appended, 0);
    assert_eq!(workspace.head().await?.id(), advanced);

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
    assert_eq!(workspace.head().await?.id(), advanced);
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
        let record = crate::kernel::decode_volume_created(&created.payload, 4 * 1024)?;
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
                    crate::kernel::decode_volume_created(&created.payload, 4 * 1024)?.volume_id,
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
                    .read("/after.txt", 16)
                    .await?,
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
    let head = fork.head().await?.id();
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
                fork.head().await?,
                IdempotencyKey::from_bytes([0x71; 16]),
            ),
        )
        .await?;
    if inherits_base {
        assert_eq!(
            grandchild.read("/base.txt", 16).await?,
            OwnedBytes::from_static(b"base")
        );
    } else {
        assert!(grandchild.read("/base.txt", 16).await.is_err());
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
            let source = main.head().await?;
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
            let attempt = main.fork("agent", options()).await;
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
            let retried = main.fork("agent", options()).await?;
            assert_eq!(retried.id(), destination);
            if inherits_base {
                assert_eq!(
                    retried.read("/base.txt", 16).await?,
                    OwnedBytes::from_static(b"base")
                );
            } else {
                assert!(retried.read("/base.txt", 16).await.is_err());
            }
            assert!(
                retried
                    .head()
                    .await?
                    .matches_fork_selection(&source, paths.clone())
                    .await?
            );
            if paths.is_some() {
                assert!(retried.read("/excluded.txt", 16).await.is_err());
            }
            assert!(
                assert_fork_state_exact(&stream, &source, main.id().volume_id(), destination)
                    .await?
            );
            let again = main.fork("agent", options()).await?;
            assert_eq!(again.head().await?.id(), retried.head().await?.id());
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
        source_id = main.head().await?.id();
        drop(main);
        close_local(fs).await?;
    }
    let before = std::fs::metadata(&journal_path)?.len();
    let forked_head;
    {
        let fs = Fs::local(crate::LocalOptions::new(root)).await?;
        let main = fs.open_workspace("repo", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let source = main.head().await?;
        assert_eq!(source.id(), source_id);
        let fork = main
            .fork("agent", ForkOptions::from_generation(source, key))
            .await?;
        forked_head = fork.head().await?.id();
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
        let source = main.head().await?;
        assert_eq!(source.id(), source_id);
        if let Ok(agent) = fs.open_workspace("agent", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error) {
            assert_eq!(agent.head().await?.id(), forked_head);
            assert_eq!(
                agent.read("/base.txt", 16).await?,
                OwnedBytes::from_static(b"base")
            );
        }
        let retried = main
            .fork("agent", ForkOptions::from_generation(source, key))
            .await?;
        assert_eq!(retried.head().await?.id(), forked_head);
        assert_eq!(
            retried.read("/base.txt", 16).await?,
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
        let base = main.head().await?;
        let agent = main
            .fork(
                "agent",
                ForkOptions::from_generation(base, IdempotencyKey::new()),
            )
            .await?;
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
        assert_eq!(main.read("/base.txt", 64).await?.as_ref(), b"base");
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
    assert_eq!(main.read("/base.txt", 64).await?.as_ref(), b"base");
    // Content the collection removed is stored again when written again.
    let content = "only the agent ".repeat(8_192);
    main.write_text("/again.txt", &content).await?;
    drop(main);
    close_local(fs).await?;
    let fs = Fs::local(crate::LocalOptions::new(root)).await?;
    let main = fs.open_workspace("repo", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert_eq!(main.read("/again.txt", 1 << 20).await?.as_ref(), content.as_bytes());
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
                generation.pin(format!("churn-{round}")).await?;
                let agent = main
                    .fork(
                        &format!("agent-{round}"),
                        ForkOptions::from_generation(generation, IdempotencyKey::new()),
                    )
                    .await?;
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
        main.read("/churn.txt", 1 << 20).await?.as_ref(),
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
    let distributed = crate::DistributedFs::new(fs.clone(), store.clone());
    let main = fs.create_workspace("repo", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    main.write_text("/base.txt", "base").await?;
    let mut children = Vec::new();
    for name in ["kept", "deleted"] {
        let base = main.head().await?;
        let child = main
            .fork(
                name,
                ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
            )
            .await?;
        distributed
            .lineage()
            .register_existing_child(&main, &child, base.id())
            .await?;
        children.push(child);
    }
    let deleted = children.pop().ok_or("deleted child")?;
    let kept = children.pop().ok_or("kept child")?;
    // A deleted parent of a live child keeps its records.
    let grandchild_base = kept.head().await?;
    let grandchild = kept
        .fork(
            "grandchild",
            ForkOptions::from_generation(grandchild_base.clone(), IdempotencyKey::new()),
        )
        .await?;
    distributed
        .lineage()
        .register_existing_child(&kept, &grandchild, grandchild_base.id())
        .await?;
    kept.delete(IdempotencyKey::new()).await?;
    deleted.delete(IdempotencyKey::new()).await?;
    let records = store.workspace_records().await?;
    assert!(records.contains_key(&deleted.id()) && records.contains_key(&kept.id()));

    distributed
        .collect_garbage(&CancellationToken::new())
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
        let mut transaction = main.begin_transaction(IdempotencyKey::new()).await?;
        transaction.create_dir_all("/src").await?;
        transaction.commit().await?;
        main.write_text("/src/main.rs", "fn main() {}\n").await?;
        let base = main.head().await?;
        main.fork(
            "promote-first",
            ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
        )
        .await?;
        main.fork(
            "promote-second",
            ForkOptions::from_generation(base, IdempotencyKey::new()),
        )
        .await?;
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
            main.read("/README.md", 64).await?,
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
            main.read("/README.md", 64).await?,
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
    let base = main.head().await?;
    let first = main
        .fork(
            "siblings-first",
            ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
        )
        .await?;
    let second = main
        .fork(
            "siblings-second",
            ForkOptions::from_generation(base, IdempotencyKey::new()),
        )
        .await?;
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
    assert_eq!(main.read("/a.rs", 16).await?, OwnedBytes::from_static(b"a\n"));
    assert_eq!(main.read("/b.rs", 16).await?, OwnedBytes::from_static(b"b\n"));
    assert_eq!(
        main.read("/README.md", 16).await?,
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
    let base = main.head().await?;
    let mut forks = Vec::new();
    for (name, file) in [("fold-first", "a.rs"), ("fold-second", "b.rs")] {
        let fork = main
            .fork(
                name,
                ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
            )
            .await?;
        let mut transaction = fork.begin_transaction(IdempotencyKey::new()).await?;
        transaction.create_dir_all("/pkg/nested").await?;
        transaction.commit().await?;
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
            main.read(path, 16).await?,
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
    let base = main.head().await?;
    let mut forks = Vec::new();
    for (name, other) in [
        ("file-fold-first", "first\n"),
        ("file-fold-second", "second\n"),
    ] {
        let fork = main
            .fork(
                name,
                ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
            )
            .await?;
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
        )
        .await?;
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
        main.read("/same.rs", 16).await?,
        OwnedBytes::from_static(b"same\n")
    );
    assert_eq!(
        main.read("/other.rs", 16).await?,
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
        let mut transaction = main.begin_transaction(IdempotencyKey::new()).await?;
        transaction.create_dir_all("/pkg").await?;
        transaction.commit().await?;
    }
    let touch = |modified_ns: i64, posix_mode: u32| FileMetadata {
        modified_ns: MetadataField::Value(modified_ns),
        posix_mode: MetadataField::Value(posix_mode),
        ..FileMetadata::default()
    };
    {
        let mut transaction = main.begin_transaction(IdempotencyKey::new()).await?;
        transaction.set_metadata("/pkg", touch(10, 0o755)).await?;
        transaction.commit().await?;
    }
    let base = main.head().await?;
    let fork = |name: &'static str, base: &Generation<_, _>| {
        main.fork(
            name,
            ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
        )
    };
    let first = fork("sibling-times-first", &base).await?;
    let second = fork("sibling-times-second", &base).await?;
    for (workspace, file, modified_ns) in [(&first, "/pkg/a.rs", 20), (&second, "/pkg/b.rs", 30)] {
        let mut transaction = workspace.begin_transaction(IdempotencyKey::new()).await?;
        transaction.write_text(file, "x\n").await?;
        transaction
            .set_metadata("/pkg", touch(modified_ns, 0o755))
            .await?;
        transaction.commit().await?;
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
        main.read("/pkg/a.rs", 16).await?,
        OwnedBytes::from_static(b"x\n")
    );
    assert_eq!(
        main.read("/pkg/b.rs", 16).await?,
        OwnedBytes::from_static(b"x\n")
    );
    assert_eq!(main.stat("/pkg").await?.metadata.modified_ns, Some(30));

    // Authored metadata changed both ways is a conflict, reported as such.
    let base = main.head().await?;
    let third = fork("sibling-times-third", &base).await?;
    for (workspace, mode) in [(&main, 0o700), (&third, 0o770)] {
        let mut transaction = workspace.begin_transaction(IdempotencyKey::new()).await?;
        transaction.set_metadata("/pkg", touch(40, mode)).await?;
        transaction.commit().await?;
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

#[test]
fn finite_customer_path_reuses_caller_live_ledger() -> Result<(), Box<dyn Error>> {
    let config = VolumeConfig::portable(Lifecycle::Ephemeral);
    let cancellation = CancellationToken::new();
    let mut allocations = crate::kernel::allocation::AllocationLedger::default();
    let mut work = crate::WorkCounters::default();
    allocations.claim_bytes(37, 0, &mut work, WorkBudget::UNBOUNDED)?;
    let first = customer_path_measured(
        "/one", config, &mut allocations, &mut work, WorkBudget::UNBOUNDED, &cancellation,
    )?;
    assert_eq!(allocations.live_bytes(), 37 + first.retained_bytes());
    assert_eq!(work.peak_allocation_bytes, allocations.live_bytes());
    let before = work;
    let live = allocations.live_bytes();
    let denied = customer_path_measured(
        "/two", config, &mut allocations, &mut work,
        WorkBudget { peak_allocation_bytes: live, ..WorkBudget::UNBOUNDED }, &cancellation,
    );
    assert!(matches!(denied, Err(WorkspaceError::Work(_))));
    assert_eq!(allocations.live_bytes(), live);
    assert_eq!(work.allocation_operations, before.allocation_operations);
    assert_eq!(work.bytes_copied, before.bytes_copied);
    allocations.release(first.retained_bytes())?;
    let reused = customer_path_measured(
        "/two", config, &mut allocations, &mut work, WorkBudget::UNBOUNDED, &cancellation,
    )?;
    assert_eq!(allocations.live_bytes(), 37 + reused.retained_bytes());
    Ok(())
}

#[tokio::test]
async fn bounded_alias_reverse_query_keeps_cold_and_warm_actual_work()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("bounded-alias-cache", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let mut transaction = workspace.begin_transaction(IdempotencyKey::new()).await?;
    transaction.write_text("/a", "shared").await?;
    transaction.hard_link("/a", "/b").await?;
    let TransactionCommit::Committed(generation) = transaction.commit().await? else {
        return Err("alias fixture did not commit".into());
    };
    let file_id = generation.stat("/a").await?.file_id;
    let cancellation = CancellationToken::new();
    let cold = generation.paths_for_file_id_measured(
        file_id, 128, WorkBudget::UNBOUNDED, &cancellation,
    ).await?;
    let warm = generation.paths_for_file_id_measured(
        file_id, 128, WorkBudget::UNBOUNDED, &cancellation,
    ).await?;
    assert_eq!(cold.value, vec!["/a", "/b"]);
    assert_eq!(warm.value, cold.value);
    assert!(cold.work.allocation_operations > 0);
    assert!(warm.work.allocation_operations > 0);
    assert!(cold.work.peak_allocation_bytes > 0);
    assert!(warm.work.peak_allocation_bytes > 0);
    assert!(warm.work.bytes_encoded >= 4);
    let denied = generation.paths_for_file_id_measured(
        file_id, 128, WorkBudget { allocation_operations: 0, ..WorkBudget::UNBOUNDED }, &cancellation,
    ).await.expect_err("query ID vector must be admitted before allocation");
    assert!(matches!(denied.error, WorkspaceError::Work(_)));
    assert_eq!(denied.work.allocation_operations, 0);
    assert_eq!(denied.work.backend_read_operations, 0);
    cancellation.cancel();
    let cancelled = generation.paths_for_file_id_measured(
        file_id, 128, WorkBudget::UNBOUNDED, &cancellation,
    ).await.expect_err("cancelled reverse query must not allocate");
    assert_eq!(*cancelled.work, crate::WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn measured_workspace_range_includes_canonical_path_copy_prefix()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("bounded-range-path", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    workspace.write_text("/range", "contents").await?;
    let generation = workspace.head().await?;
    let cancellation = CancellationToken::new();
    let receipt = generation.read_range_measured(
        "/range", 1, 3, WorkBudget::UNBOUNDED, &cancellation,
    ).await?;
    assert_eq!(receipt.value.as_ref(), b"ont");
    assert!(receipt.work.bytes_copied >= 5);
    assert!(receipt.work.source_path_components >= 1);
    let failure = generation.read_range_measured(
        "/range", 1, 3, WorkBudget { bytes_copied: 4, ..WorkBudget::UNBOUNDED }, &cancellation,
    ).await.expect_err("canonical name copy exceeds this bound");
    assert!(matches!(failure.error, WorkspaceError::Work(_)));
    assert!(failure.work.bytes_copied <= 4);
    assert!(failure.work.allocation_operations > 0);
    Ok(())
}

#[tokio::test]
async fn measured_authored_input_vector_is_denied_before_engine_mutation()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("bounded-authored-input", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let mut transaction = workspace.begin_transaction(IdempotencyKey::new()).await?;
    let cancellation = CancellationToken::new();
    let failure = transaction.create_directory_measured(
        "/denied",
        WorkBudget { allocation_operations: 2, ..WorkBudget::UNBOUNDED },
        &cancellation,
    ).await.expect_err("one authored input slot must be admitted after the path");
    assert!(matches!(failure.error, WorkspaceError::Work(_)));
    assert_eq!(failure.work.allocation_operations, 2);
    assert_eq!(failure.work.bytes_copied, 6);
    let path = customer_path("/denied", transaction.checkout.volume_config())?;
    let lookup = transaction.checkout.lookup_no_follow(
        &path, WorkBudget::UNBOUNDED, &cancellation,
    ).await?;
    assert!(lookup.value.record.is_none());
    let completed = transaction.create_directory_measured(
        "/admitted", WorkBudget::UNBOUNDED, &cancellation,
    ).await?;
    assert!(completed.allocation_operations >= 3);
    Ok(())
}

#[test]
fn measured_unicode_projection_keeps_shared_live_and_denies_before_output_allocation()
-> Result<(), Box<dyn Error>> {
    let cancellation = CancellationToken::new();
    let mut allocations = crate::kernel::allocation::AllocationLedger::default();
    let mut work = crate::WorkCounters::default();
    let path = NamespacePath::parse_in_profile_with_ledger(
        "/a/é/𝄞", crate::model::FilesystemProfile::Windows, VolumeConfig::portable(Lifecycle::Ephemeral).limits,
        &mut allocations, &mut work, WorkBudget::UNBOUNDED, &cancellation,
    )?;
    let before = work;
    let live = allocations.live_bytes();
    let expected = "/a/é/𝄞";
    let failure = namespace_path_text_measured(
        &path, &mut allocations, &mut work,
        WorkBudget { output_bytes: expected.len() as u64 - 1, ..WorkBudget::UNBOUNDED },
        &cancellation,
    );
    assert!(matches!(failure, Err(WorkspaceError::Work(_))));
    assert_eq!(allocations.live_bytes(), live);
    assert_eq!(work.allocation_operations, before.allocation_operations);
    assert_eq!(work.bytes_encoded, before.bytes_encoded);
    let output = namespace_path_text_measured(
        &path, &mut allocations, &mut work, WorkBudget::UNBOUNDED, &cancellation,
    )?;
    assert_eq!(output, expected);
    assert_eq!(allocations.live_bytes(), live + output.capacity() as u64);
    assert_eq!(work.peak_allocation_bytes, allocations.live_bytes());
    assert_eq!(work.output_bytes, expected.len() as u64);
    Ok(())
}

#[test]
fn canonical_path_errors_keep_the_same_inline_cause_without_allocating_diagnostics()
-> Result<(), Box<dyn Error>> {
    let config = VolumeConfig::portable(Lifecycle::Ephemeral);
    let ordinary = customer_path("/a/../b", config).expect_err("invalid canonical path");
    let mut allocations = crate::kernel::allocation::AllocationLedger::default();
    let mut work = crate::WorkCounters::default();
    let measured = customer_path_measured(
        "/a/../b", config, &mut allocations, &mut work,
        WorkBudget { allocation_operations: 0, ..WorkBudget::UNBOUNDED }, &CancellationToken::new(),
    ).expect_err("invalid canonical path must fail before allocating");
    let (WorkspaceError::NamespacePath(ordinary), WorkspaceError::NamespacePath(measured)) = (ordinary, measured)
        else { return Err("canonical helpers did not retain the actual typed path cause".into()); };
    assert_eq!(ordinary, measured);
    assert_eq!(work, crate::WorkCounters::default());
    assert_eq!(allocations.live_bytes(), 0);
    let wrapped = WorkspaceError::from(crate::FsError::Path(
        crate::kernel::PathLookupError::NamespacePath(measured),
    ));
    assert!(matches!(wrapped, WorkspaceError::NamespacePath(_)));
    let cancelled = WorkspaceError::from(crate::kernel::NamespacePathError::Cancelled);
    assert!(matches!(cancelled, WorkspaceError::Cancelled(_)));
    Ok(())
}

#[test]
fn opaque_native_projection_error_is_typed_before_any_output_allocation()
-> Result<(), Box<dyn Error>> {
    let limits = VolumeConfig::portable(Lifecycle::Ephemeral).limits;
    let path = NamespacePath::new(vec![
        LogicalName::new(crate::kernel::NameEncoding::WindowsUtf16Le, vec![b'a', 0], 255)?,
        LogicalName::new(crate::kernel::NameEncoding::WindowsUtf16Le, vec![0, 0xd8], 255)?,
    ], limits)?;
    let mut allocations = crate::kernel::allocation::AllocationLedger::default();
    let mut work = crate::WorkCounters::default();
    allocations.claim_bytes(path.retained_bytes(), 0, &mut work, WorkBudget::UNBOUNDED)?;
    let live = allocations.live_bytes();
    let error = namespace_path_text_measured(
        &path, &mut allocations, &mut work,
        WorkBudget { allocation_operations: 0, ..WorkBudget::UNBOUNDED }, &CancellationToken::new(),
    ).expect_err("opaque native code units cannot become a Unicode string");
    assert!(matches!(error, WorkspaceError::NamespacePath(
        crate::kernel::NamespacePathError::NonUnicodeProjection,
    )));
    assert_eq!(work.items_examined, path.depth() as u64);
    assert_eq!(work.allocation_operations, 0);
    assert_eq!(work.bytes_copied, 0);
    assert_eq!(work.bytes_encoded, 0);
    assert_eq!(work.output_bytes, 0);
    assert_eq!(allocations.live_bytes(), live);
    Ok(())
}

#[tokio::test]
async fn measured_selected_transaction_composes_one_finite_pinned_import()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("finite-selected-import", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    workspace.write_text("/removed", "old").await?;
    let pinned = workspace.head().await?;
    let cancellation = CancellationToken::new();
    let budget = selected_transaction_budget();
    assert!(budget.has_finite_allowances());
    let opened = workspace.begin_pinned_transaction_measured(
        &pinned, IdempotencyKey::new(), budget, &cancellation,
    ).await?;
    let mut work = opened.work;
    let mut transaction = opened.value;
    let parents = transaction.create_dir_all_measured(
        "/nested", work.remaining(budget)?, &cancellation,
    ).await?;
    work = work.checked_add(parents)?;
    let written = transaction.write_measured(
        "/nested/file", OwnedBytes::from_static(b"original owned input"),
        work.remaining(budget)?, &cancellation,
    ).await?;
    work = work.checked_add(written)?;
    let replaced = transaction.write_measured(
        "/nested/file", OwnedBytes::from_static(b"replacement"),
        work.remaining(budget)?, &cancellation,
    ).await?;
    work = work.checked_add(replaced)?;
    let removed = transaction.remove_measured(
        "/removed", work.remaining(budget)?, &cancellation,
    ).await?;
    work = work.checked_add(removed)?;
    assert_eq!(workspace.head().await?.id(), pinned.id());
    let committed = transaction.commit_with_permit_measured(
        crate::PublicationPermit::Unrestricted, work.remaining(budget)?, &cancellation,
    ).await?;
    work = work.checked_add(committed.work)?;
    work.verify(budget)?;
    assert!(matches!(committed.value, TransactionCommit::Committed(_)));
    assert_eq!(workspace.read("/nested/file", 64).await?.as_ref(), b"replacement");
    assert!(matches!(workspace.read("/removed", 64).await, Err(WorkspaceError::NotFound)));
    assert!(pinned.read("/nested/file", 64).await.is_err());
    assert_eq!(pinned.read("/removed", 64).await?.as_ref(), b"old");
    Ok(())
}

#[tokio::test]
async fn measured_selected_transaction_failure_preserves_work_and_pending_candidate()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("retained-selected-import", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let original = workspace.head().await?;
    let cancellation = CancellationToken::new();
    let mut transaction = workspace.begin_transaction(IdempotencyKey::new()).await?;
    transaction.create_dir_all("/dir").await?;
    transaction.write("/dir/child", bytes::Bytes::from_static(b"pending")).await?;
    let failed_write = transaction.write_measured(
        "/dir/child", OwnedBytes::from_static(b"must not replace"),
        WorkBudget { bytes_copied: 2, ..WorkBudget::UNBOUNDED }, &cancellation,
    ).await.expect_err("canonical path copying is bounded before mutation");
    assert!(matches!(failed_write.error, WorkspaceError::Work(_)));
    assert!(failed_write.work.allocation_operations > 0);
    assert!(failed_write.work.bytes_copied <= 2);
    let failed_remove = transaction.remove_measured(
        "/dir", WorkBudget::UNBOUNDED, &cancellation,
    ).await.expect_err("nonempty directory removal is atomic");
    assert!(failed_remove.work.bytes_copied >= 3);
    assert!(failed_remove.work.allocation_operations > 0);
    let missing = transaction.remove_measured(
        "/missing", WorkBudget::UNBOUNDED, &cancellation,
    ).await.expect_err("missing binding retains the lookup receipt");
    assert!(matches!(missing.error, WorkspaceError::NotFound));
    assert!(missing.work.bytes_copied >= 7);
    let denied_commit = transaction.commit_with_permit_measured(
        crate::PublicationPermit::Unrestricted,
        WorkBudget { allocation_operations: 0, ..WorkBudget::UNBOUNDED },
        &cancellation,
    ).await;
    let Err(denied_commit) = denied_commit else {
        panic!("commit future must be admitted before publication");
    };
    assert!(matches!(denied_commit.error, WorkspaceError::Work(_)));
    assert_eq!(*denied_commit.work, crate::WorkCounters::UNCHARGED);
    assert_eq!(workspace.head().await?.id(), original.id());
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let denied_write = transaction.write_measured(
        "/dir/child", OwnedBytes::from_static(b"cancelled"), WorkBudget::UNBOUNDED, &cancelled,
    ).await.expect_err("cold cancellation");
    let denied_remove = transaction.remove_measured(
        "/dir/child", WorkBudget::UNBOUNDED, &cancelled,
    ).await.expect_err("cold cancellation");
    assert!(matches!(denied_write.error, WorkspaceError::Cancelled(_)));
    assert!(matches!(denied_remove.error, WorkspaceError::Cancelled(_)));
    assert_eq!(*denied_write.work, crate::WorkCounters::UNCHARGED);
    assert_eq!(*denied_remove.work, crate::WorkCounters::UNCHARGED);
    let result = transaction.commit_with_permit_measured(
        crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation,
    ).await?;
    assert!(matches!(result.value, TransactionCommit::Committed(_)));
    assert_eq!(workspace.read("/dir/child", 32).await?.as_ref(), b"pending");
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
    let mut seed = workspace.begin_transaction(IdempotencyKey::new()).await?;
    seed.create_dir_all("/tree").await?;
    seed.write("/tree/file", bytes::Bytes::from_static(b"source")).await?;
    seed.hard_link("/tree/file", "/tree/alias").await?;
    seed.set_metadata("/tree/file", FileMetadata {
        posix_mode: MetadataField::Value(0o640), ..FileMetadata::default()
    }).await?;
    seed.write("/outside", bytes::Bytes::from_static(b"base")).await?;
    seed.commit().await?;
    let source = workspace.head().await?;
    let source_metadata = source.stat("/tree/file").await?.metadata;
    let mut live = workspace.begin_transaction(IdempotencyKey::new()).await?;
    live.write("/tree/file", bytes::Bytes::from_static(b"dirty")).await?;
    live.write("/removed", bytes::Bytes::from_static(b"remove me")).await?;
    live.write("/outside", bytes::Bytes::from_static(b"user")).await?;
    live.commit().await?;
    let dirty = workspace.head().await?;
    let selected = vec!["tree/file".to_owned(), "removed".to_owned()];
    let budget = selected_transaction_budget();
    let key = IdempotencyKey::new();
    let cancelled = CancellationToken::new();
    let volume = workspace.volume.clone();
    let source_id = source.id();
    let guard_cancellation = cancelled.clone();
    let failed = workspace.restore_paths_from_with_permit_measured_guarded(
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
    assert_eq!(workspace.head().await?.id(), dirty.id());
    assert_eq!(workspace.read("/tree/file", 32).await?.as_ref(), b"dirty");
    assert_eq!(workspace.read("/removed", 32).await?.as_ref(), b"remove me");
    assert_eq!(workspace.read("/outside", 32).await?.as_ref(), b"user");

    let cancellation = CancellationToken::new();
    let conflicted = workspace.restore_paths_from_with_permit_measured(
        &source, &selected, source.id(), IdempotencyKey::new(),
        crate::PublicationPermit::Unrestricted, budget, &cancellation,
    ).await?;
    conflicted.work.verify(budget)?;
    assert!(matches!(&conflicted.value, TransactionCommit::Conflict { actual } if actual.id() == dirty.id()));
    assert_eq!(workspace.head().await?.id(), dirty.id());
    let volume = workspace.volume.clone();
    let guard_cancellation = cancellation.clone();
    let restored = workspace.restore_paths_from_with_permit_measured_guarded(
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
    assert_eq!(workspace.read("/tree/file", 32).await?.as_ref(), b"source");
    assert_eq!(workspace.read("/tree/alias", 32).await?.as_ref(), b"source");
    assert_eq!(workspace.stat("/tree/file").await?.metadata, source_metadata);
    assert_eq!(workspace.stat("/tree/file").await?.file_id, workspace.stat("/tree/alias").await?.file_id);
    assert!(matches!(workspace.read("/removed", 32).await, Err(WorkspaceError::NotFound)));
    assert_eq!(workspace.read("/outside", 32).await?.as_ref(), b"user");
    assert_eq!(source.read("/tree/file", 32).await?.as_ref(), b"source");
    Ok(())
}

#[tokio::test]
async fn original_fork_retry_after_both_heads_advance_needs_no_publication_seal()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let parent = fs.create_workspace("original-fork-parent", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    parent.write_text("/file", "original").await?;
    let frozen = parent.head().await?;
    let key = IdempotencyKey::new();
    let cancellation = CancellationToken::new();
    let budget = selected_transaction_budget();
    let first = parent.fork_measured(
        "original-fork-child", ForkOptions::from_generation(frozen.clone(), key),
        budget, &cancellation,
    ).await?;
    let child = first.value;
    let original_child = child.head().await?;
    parent.write_text("/file", "later parent").await?;
    child.write_text("/file", "later child").await?;
    let advanced_child = child.head().await?;
    let retry = parent.fork_measured_guarded(
        "original-fork-child", ForkOptions::from_generation(frozen, key),
        crate::PublicationPermit::Unrestricted, budget, &cancellation,
        |_| std::future::ready(Err(crate::OperationFailure::before_work(crate::FsError::CreationRejected))),
    ).await?;
    retry.work.verify(budget)?;
    assert_eq!(retry.work.authority_records_appended, 0);
    assert_eq!(retry.value.head().await?.id(), advanced_child.id());
    assert_eq!(retry.value.read("/file", 32).await?.as_ref(), b"later child");
    let creation = fs.workspace_for_creation_operation(
        "original-fork-child", child.volume.config, key.operation_id(), budget, &cancellation,
    ).await?.value.ok_or("original creation missing")?;
    assert_eq!(creation.id(), original_child.id());
    assert_eq!(creation.read("/file", 32).await?.as_ref(), b"original");
    Ok(())
}

#[tokio::test]
async fn guarded_stale_exact_retry_resolves_committed_without_owner_reseal()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("resolved-owner-seal", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let base = workspace.head().await?;
    let key = IdempotencyKey::new();
    let mut first = workspace.begin_transaction(key).await?;
    first.write("/file", bytes::Bytes::from_static(b"same input")).await?;
    let TransactionCommit::Committed(committed) = first.commit().await? else {
        return Err("initial original commit did not publish".into());
    };
    let mut retry = workspace.begin_transaction_at(&base, key).await?;
    retry.write("/file", bytes::Bytes::from_static(b"same input")).await?;
    let cancellation = CancellationToken::new();
    let budget = selected_transaction_budget();
    let mut owner_calls = 0;
    let resolved = retry.commit_with_permit_measured_guarded(
        crate::PublicationPermit::Unrestricted, budget, &cancellation,
        |_| {
            owner_calls += 1;
            std::future::ready(Err(crate::OperationFailure::before_work(
                OrdinaryOwnerError::Denied { task: [119; 16], revision: 914, expired: true },
            )))
        },
    ).await?;
    resolved.work.verify(budget)?;
    assert!(matches!(&resolved.value, TransactionCommit::AlreadyCommitted(generation) if generation.id() == committed.id()));
    assert_eq!(workspace.head().await?.id(), committed.id());
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
    let base = workspace.head().await?;
    let cancellation = CancellationToken::new();
    let mut candidate = workspace.begin_transaction(IdempotencyKey::new()).await?;
    candidate.write("/file", bytes::Bytes::from_static(b"candidate")).await?;
    let callback_work = crate::WorkCounters {
        backend_read_operations: 3,
        ..crate::WorkCounters::UNCHARGED
    };
    let failure = candidate.commit_with_permit_measured_guarded(
        crate::PublicationPermit::Unrestricted, selected_transaction_budget(), &cancellation,
        |_| std::future::ready(Err(crate::OperationFailure::new(
            OrdinaryOwnerError::Denied { task: [117; 16], revision: 912, expired: true },
            callback_work,
        ))),
    ).await.err().ok_or("expired owner published")?;
    assert!(matches!(failure.error,
        OrdinaryOwnerError::Denied { task, revision: 912, expired: true } if task == [117; 16]));
    assert!(failure.work.backend_read_operations >= callback_work.backend_read_operations);
    assert!(failure.work.bytes_hashed > 0);
    assert_eq!(failure.work.authority_records_appended, 0);
    assert_eq!(workspace.head().await?.id(), base.id());
    let failure = candidate.commit_with_permit_guarded(
        crate::PublicationPermit::Unrestricted, &cancellation,
        || std::future::ready(Err(OrdinaryOwnerError::Denied {
            task: [118; 16], revision: 913, expired: false,
        })),
    ).await.err().ok_or("denied ordinary owner published")?;
    assert!(matches!(failure,
        OrdinaryOwnerError::Denied { task, revision: 913, expired: false } if task == [118; 16]));
    assert_eq!(workspace.head().await?.id(), base.id());
    assert!(matches!(candidate.commit().await?, TransactionCommit::Committed(_)));
    assert_eq!(workspace.read("/file", 32).await?.as_ref(), b"candidate");
    let mut cancelled_candidate = workspace.begin_transaction(IdempotencyKey::new()).await?;
    cancelled_candidate.write("/cancelled", bytes::Bytes::from_static(b"not published")).await?;
    cancellation.cancel();
    let sdk_failure = cancelled_candidate.commit_with_permit_guarded(
        crate::PublicationPermit::Unrestricted, &cancellation,
        || std::future::ready(Ok::<(), OrdinaryOwnerError>(())),
    ).await.err().ok_or("cancelled SDK work published")?;
    assert!(matches!(sdk_failure, OrdinaryOwnerError::Sdk(WorkspaceError::Cancelled(_))));
    Ok(())
}

#[test]
fn selected_restore_prefix_copy_admits_before_body_and_releases_denied_storage()
-> Result<(), Box<dyn Error>> {
    let cancellation = CancellationToken::new();
    let mut allocations = crate::kernel::allocation::AllocationLedger::default();
    let mut work = crate::WorkCounters::UNCHARGED;
    let failure = Workspace::<crate::MemoryAuthorityBackend, crate::MemoryObjectBackend>::restore_absolute_path_measured(
        "tree/file", &mut allocations, &mut work,
        WorkBudget { bytes_copied: 0, ..selected_transaction_budget() }, &cancellation,
    ).expect_err("formatting body must not copy before admission");
    assert!(matches!(failure, WorkspaceError::Work(_)));
    assert_eq!(work.allocation_operations, 1);
    assert_eq!(work.bytes_copied, 0);
    assert_eq!(allocations.live_bytes(), 0);
    let absolute = Workspace::<crate::MemoryAuthorityBackend, crate::MemoryObjectBackend>::restore_absolute_path_measured(
        "tree/file", &mut allocations, &mut work, selected_transaction_budget(), &cancellation,
    )?;
    assert_eq!(absolute, "/tree/file");
    assert_eq!(work.bytes_copied, absolute.len() as u64);
    let prefix_bytes = absolute.capacity() as u64;
    let path = customer_path_measured(
        &absolute, VolumeConfig::portable(Lifecycle::Ephemeral),
        &mut allocations, &mut work, selected_transaction_budget(), &cancellation,
    )?;
    assert_eq!(allocations.live_bytes(), prefix_bytes + path.retained_bytes());
    assert!(work.peak_allocation_bytes >= prefix_bytes + path.retained_bytes());
    drop(absolute);
    allocations.release(prefix_bytes)?;
    assert_eq!(allocations.live_bytes(), path.retained_bytes());
    Ok(())
}

#[tokio::test]
async fn measured_directory_record_restore_preserves_original_ignored_descendants()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("record-not-subtree-restore", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let mut seed = workspace.begin_transaction(IdempotencyKey::new()).await?;
    seed.create_dir_all("/tree").await?;
    seed.set_metadata("/tree", FileMetadata {
        posix_mode: MetadataField::Value(0o700), ..FileMetadata::default()
    }).await?;
    seed.write("/tree/tracked", bytes::Bytes::from_static(b"base")).await?;
    seed.commit().await?;
    let source = workspace.head().await?;
    let mut live = workspace.begin_transaction(IdempotencyKey::new()).await?;
    live.set_metadata("/tree", FileMetadata {
        posix_mode: MetadataField::Value(0o755), ..FileMetadata::default()
    }).await?;
    live.write("/tree/tracked", bytes::Bytes::from_static(b"dirty")).await?;
    live.write("/tree/ignored", bytes::Bytes::from_static(b"live ignored")).await?;
    live.commit().await?;
    let current = workspace.head().await?;
    let cancellation = CancellationToken::new();
    let budget = selected_transaction_budget();
    let restored = workspace.restore_paths_from_with_permit_measured(
        &source, &["tree".to_owned()], current.id(), IdempotencyKey::new(),
        crate::PublicationPermit::Unrestricted, budget, &cancellation,
    ).await?;
    restored.work.verify(budget)?;
    assert!(matches!(restored.value, TransactionCommit::Committed(_)));
    assert_eq!(workspace.stat("/tree").await?.metadata.posix_mode, Some(0o700));
    assert_eq!(workspace.read("/tree/tracked", 32).await?.as_ref(), b"dirty");
    assert_eq!(workspace.read("/tree/ignored", 32).await?.as_ref(), b"live ignored");
    Ok(())
}

#[tokio::test]
async fn measured_actual_reservation_retains_proof_across_cancelled_cleanup()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("measured-actual-reservation", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let other = fs.create_workspace("other-reservation-authority", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let original = workspace.head().await?;
    let cancellation = CancellationToken::new();
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let budget = selected_transaction_budget();
    let operation_id = OperationId::new();
    let cold = workspace.reserve_publication_measured(operation_id, budget, &cancelled)
        .await.expect_err("original cancelled token must prevent reservation");
    assert!(matches!(cold.error, WorkspaceError::Cancelled(_)));
    assert_eq!(*cold.work, crate::WorkCounters::UNCHARGED);
    let reserved = workspace.reserve_publication_measured(operation_id, budget, &cancellation).await?;
    reserved.work.verify(budget)?;
    let reservation = reserved.value;
    let retried = workspace.reserve_publication_measured(operation_id, budget, &cancellation).await?;
    retried.work.verify(budget)?;
    assert_eq!(retried.value, reservation);
    assert_eq!(reservation.authority_id, workspace.authority_id());
    assert_eq!(reservation.operation_id, operation_id);
    let foreign = other.release_publication_measured(reservation, budget, &cancellation)
        .await.expect_err("a different workspace cannot release this actual proof");
    assert!(matches!(foreign.error, WorkspaceError::IncompatibleWorkspace));
    assert_eq!(*foreign.work, crate::WorkCounters::UNCHARGED);
    let failed = workspace.release_publication_measured(reservation, budget, &cancelled)
        .await.expect_err("cancelled cleanup must not claim release");
    assert!(matches!(failed.error, WorkspaceError::Cancelled(_)));
    assert_eq!(*failed.work, crate::WorkCounters::UNCHARGED);
    let blocked = workspace.reserve_publication_measured(OperationId::new(), budget, &cancellation)
        .await.expect_err("the original reservation remains active");
    assert!(matches!(blocked.error, WorkspaceError::StaleGeneration));
    assert!(blocked.work.authority_records_read > 0);
    let denied = workspace.release_publication_measured(
        reservation, crate::WorkCounters::UNCHARGED, &cancellation,
    ).await.expect_err("release must admit its original authority work");
    assert!(matches!(denied.error, WorkspaceError::Work(_)));
    let released = workspace.release_publication_measured(reservation, budget, &cancellation).await?;
    released.work.verify(budget)?;
    let released_retry = workspace.release_publication_measured(reservation, budget, &cancellation).await?;
    released_retry.work.verify(budget)?;
    let next = workspace.reserve_publication_measured(OperationId::new(), budget, &cancellation).await?;
    workspace.release_publication_measured(next.value, budget, &cancellation).await?;
    assert_eq!(workspace.head().await?.id(), original.id());
    Ok(())
}

#[tokio::test]
async fn selected_record_restore_prepares_file_to_directory_privately_before_one_seal()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let source_workspace = fs.create_workspace("source-selected-directory-shape", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let mut source_tx = source_workspace.begin_transaction(IdempotencyKey::new()).await?;
    source_tx.create_dir_all("/hot/empty/nested").await?;
    source_tx.set_metadata("/hot", FileMetadata {
        posix_mode: MetadataField::Value(0o750), ..FileMetadata::default()
    }).await?;
    source_tx.write("/hot/value", bytes::Bytes::from_static(b"selected source")).await?;
    source_tx.hard_link("/hot/value", "/hot/alias").await?;
    source_tx.commit().await?;
    let source = source_workspace.head().await?;
    let workspace = fs.create_workspace("target-selected-directory-shape", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let mut target_tx = workspace.begin_transaction(IdempotencyKey::new()).await?;
    target_tx.write("/hot", bytes::Bytes::from_static(b"original file")).await?;
    target_tx.hard_link("/hot", "/outside-linked").await?;
    target_tx.write("/outside", bytes::Bytes::from_static(b"user")).await?;
    target_tx.commit().await?;
    let original = workspace.head().await?;
    let paths = vec![
        "hot".to_owned(), "hot/empty".to_owned(), "hot/empty/nested".to_owned(),
        "hot/value".to_owned(), "hot/alias".to_owned(),
    ];
    let budget = selected_transaction_budget();
    let cancellation = CancellationToken::new();
    let rejected = workspace.restore_selected_paths_from_with_permit_measured_guarded(
        &source, &paths, original.id(), IdempotencyKey::new(),
        crate::PublicationPermit::Unrestricted, budget, &cancellation,
        |_| async { Err(crate::OperationFailure::before_work(FsError::MutationNotAllowed)) },
    ).await;
    let Err(rejected) = rejected else { panic!("owner rejection must prevent the only publication"); };
    rejected.work.verify(budget)?;
    assert!(rejected.work.allocation_operations > 0);
    assert_eq!(workspace.head().await?.id(), original.id());
    assert_eq!(workspace.read("/hot", 32).await?.as_ref(), b"original file");
    assert_eq!(workspace.stat("/hot").await?.file_id, workspace.stat("/outside-linked").await?.file_id);
    let volume = workspace.volume.clone();
    let guard_cancellation = cancellation.clone();
    let original_id = original.id();
    let restored = workspace.restore_selected_paths_from_with_permit_measured_guarded(
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
    assert_eq!(workspace.stat("/hot").await?.metadata.posix_mode, Some(0o750));
    assert_eq!(workspace.stat("/hot/empty/nested").await?.kind, FileKind::Directory);
    assert!(workspace.list_directory("/hot/empty/nested", None, 1).await?.entries.is_empty());
    assert_eq!(workspace.read("/hot/value", 32).await?.as_ref(), b"selected source");
    assert_eq!(workspace.read("/hot/alias", 32).await?.as_ref(), b"selected source");
    assert_eq!(workspace.stat("/hot/value").await?.file_id, workspace.stat("/hot/alias").await?.file_id);
    assert_eq!(workspace.read("/outside-linked", 32).await?.as_ref(), b"original file");
    assert_eq!(workspace.stat("/outside-linked").await?.link_count, 1);
    assert_eq!(workspace.read("/outside", 32).await?.as_ref(), b"user");
    Ok(())
}

#[tokio::test]
async fn selected_record_restore_removes_directory_descendants_before_file_replacement()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let source_workspace = fs.create_workspace("source-selected-file-shape", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    source_workspace.write("/hot", bytes::Bytes::from_static(b"replacement file")).await?;
    let source = source_workspace.head().await?;
    let workspace = fs.create_workspace("target-selected-file-shape", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let mut seed = workspace.begin_transaction(IdempotencyKey::new()).await?;
    seed.create_dir_all("/hot/nested/empty").await?;
    seed.write("/hot/nested/old", bytes::Bytes::from_static(b"retained outside alias")).await?;
    seed.hard_link("/hot/nested/old", "/outside-linked").await?;
    seed.write("/hot/top", bytes::Bytes::from_static(b"old top")).await?;
    seed.write("/outside", bytes::Bytes::from_static(b"user")).await?;
    seed.commit().await?;
    let original = workspace.head().await?;
    let paths = vec![
        "hot/nested/empty".to_owned(), "hot/nested/old".to_owned(),
        "hot/nested".to_owned(), "hot/top".to_owned(), "hot".to_owned(),
    ];
    let cancellation = CancellationToken::new();
    let budget = selected_transaction_budget();
    let rejected = workspace.restore_selected_paths_from_with_permit_measured_guarded(
        &source, &paths, original.id(), IdempotencyKey::new(),
        crate::PublicationPermit::Unrestricted, budget, &cancellation,
        |_| async { Err(crate::OperationFailure::before_work(FsError::MutationNotAllowed)) },
    ).await;
    let Err(rejected) = rejected else { panic!("owner rejection must prevent subtree replacement"); };
    rejected.work.verify(budget)?;
    assert_eq!(workspace.head().await?.id(), original.id());
    assert_eq!(workspace.read("/hot/nested/old", 64).await?.as_ref(), b"retained outside alias");
    assert_eq!(workspace.stat("/outside-linked").await?.link_count, 2);
    let volume = workspace.volume.clone();
    let guard_cancellation = cancellation.clone();
    let original_id = original.id();
    let restored = workspace.restore_selected_paths_from_with_permit_measured_guarded(
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
    assert_eq!(workspace.read("/hot", 64).await?.as_ref(), b"replacement file");
    assert_eq!(workspace.stat("/hot").await?.kind, FileKind::Regular);
    assert_eq!(workspace.read("/outside-linked", 64).await?.as_ref(), b"retained outside alias");
    assert_eq!(workspace.stat("/outside-linked").await?.link_count, 1);
    assert_eq!(workspace.read("/outside", 64).await?.as_ref(), b"user");
    Ok(())
}

#[tokio::test]
async fn measured_reservation_query_is_existing_only_and_preserves_original_proof()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("readonly-reservation-query", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let original = workspace.head().await?;
    let operation_id = OperationId::new();
    let cancellation = CancellationToken::new();
    let budget = selected_transaction_budget();
    let absent = workspace.publication_reservation_measured(operation_id, budget, &cancellation).await?;
    absent.work.verify(budget)?;
    assert!(absent.value.is_none());
    let reserved = workspace.reserve_publication_measured(operation_id, budget, &cancellation).await?;
    let found = workspace.publication_reservation_measured(operation_id, budget, &cancellation).await?;
    found.work.verify(budget)?;
    assert_eq!(found.value, Some(reserved.value));
    let other = workspace.publication_reservation_measured(OperationId::new(), budget, &cancellation).await?;
    other.work.verify(budget)?;
    assert!(other.value.is_none());
    let blocked = workspace.reserve_publication_measured(OperationId::new(), budget, &cancellation)
        .await.expect_err("readonly query must not release the original gate");
    assert!(matches!(blocked.error, WorkspaceError::StaleGeneration));
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let denied = workspace.publication_reservation_measured(operation_id, budget, &cancelled)
        .await.expect_err("query uses the original caller cancellation");
    assert!(matches!(denied.error, WorkspaceError::Cancelled(_)));
    assert_eq!(*denied.work, crate::WorkCounters::UNCHARGED);
    workspace.release_publication_measured(reserved.value, budget, &cancellation).await?;
    let released = workspace.publication_reservation_measured(operation_id, budget, &cancellation).await?;
    released.work.verify(budget)?;
    assert!(released.value.is_none());
    assert_eq!(workspace.head().await?.id(), original.id());
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_permanent_pin_consumes_the_original_shared_publication_custody()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("native-permanent-original-pin", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let committed = match workspace.write("/value", bytes::Bytes::from_static(b"original")).await? {
        TransactionCommit::Committed(generation) => generation,
        _ => return Err("the original publication did not commit".into()),
    };
    let clone = committed.clone();
    assert!(committed.publication_retained_bytes() != 0);
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(
        selected_transaction_budget(), &cancellation,
    ).map_err(|(error, _)| error)?;
    let pinned = committed.pin_with_native_work("original-label", &original, &cancellation)
        .await.map_err(|(error, _)| error)?;
    assert_eq!(pinned.generation().id(), committed.id());
    assert_eq!(pinned.identity().as_str(), "original-label");
    assert_eq!(committed.publication_retained_bytes(), 0);
    assert_eq!(clone.publication_retained_bytes(), 0);
    let repeated = clone.pin_with_native_work("original-label", &original, &cancellation)
        .await.map_err(|(error, _)| error)?;
    assert_eq!(repeated.generation().id(), pinned.generation().id());
    assert_eq!(repeated.identity(), pinned.identity());
    let ordinary = committed.pin("original-label").await?;
    assert_eq!(ordinary.generation().id(), pinned.generation().id());
    original.receipt().verify(selected_transaction_budget())?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_public_transaction_cancellation_keeps_the_original_work_prefix()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("native-public-cancelled-transaction", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let root = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(
        selected_transaction_budget(), &root,
    ).map_err(|(error, _)| error)?;
    let before = original.receipt();
    let caller = CancellationToken::new();
    caller.cancel();
    let failure = workspace.begin_transaction_with_native_work(
        IdempotencyKey::new(), &original, &caller,
    ).await.err().ok_or("cancelled native transaction unexpectedly opened")?;
    assert!(workspace_bounded_error(&failure.0).is_some());
    assert_eq!(original.receipt(), before);
    assert!(failure.1.is_none());
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_whole_file_read_rejects_a_prefix_bound_on_the_exact_generation()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("native-original-whole-file-read", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let committed = match workspace.write("/value", bytes::Bytes::from_static(b"complete")).await? {
        TransactionCommit::Committed(generation) => generation,
        _ => return Err("the original publication did not commit".into()),
    };
    let retained = committed.publication_retained_bytes();
    let _ = workspace.write("/value", bytes::Bytes::from_static(b"changed-current-head")).await?;
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(
        selected_transaction_budget(), &cancellation,
    ).map_err(|(error, _)| error)?;
    let before = original.receipt();
    let failure = committed.read_with_native_work(
        "/value", 7, &original, &cancellation,
    ).await.err().ok_or("a smaller bound unexpectedly authorized a file prefix")?;
    assert!(matches!(failure.0, WorkspaceError::ReadLimitExceeded));
    assert!(failure.1.is_none());
    assert_ne!(original.receipt(), before);
    let bytes = committed.read_with_native_work(
        "/value", 8, &original, &cancellation,
    ).await.map_err(|(error, _)| error)?;
    assert_eq!(bytes, OwnedBytes::from_static(b"complete"));
    assert_eq!(committed.publication_retained_bytes(), retained);
    original.receipt().verify(selected_transaction_budget())?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_public_fork_retains_the_original_selected_generation_and_operation()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("native-selected-original-parent", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    workspace.write("/selected", bytes::Bytes::from_static(b"original")).await?;
    let frozen = match workspace.write("/unselected", bytes::Bytes::from_static(b"private")).await? {
        TransactionCommit::Committed(generation) => generation,
        _ => return Err("the original publication did not commit".into()),
    };
    workspace.write("/selected", bytes::Bytes::from_static(b"advanced-parent")).await?;
    let key = IdempotencyKey::new();
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(
        selected_transaction_budget(), &cancellation,
    ).map_err(|(error, _)| error)?;
    let forked = workspace.fork_with_native_work(
        "native-selected-original-child",
        ForkOptions::from_generation(frozen.clone(), key)
            .inherit_paths(vec!["/selected".to_owned()]),
        &original, &cancellation,
    ).await.map_err(|(error, _)| error)?;
    assert_eq!(forked.read("/selected", 8).await?, OwnedBytes::from_static(b"original"));
    assert!(matches!(forked.read("/unselected", 8).await, Err(WorkspaceError::NotFound)));
    forked.write("/selected", bytes::Bytes::from_static(b"advanced-child")).await?;
    workspace.write("/selected", bytes::Bytes::from_static(b"another-parent-head")).await?;
    let retried = workspace.fork_with_native_work(
        "native-selected-original-child",
        ForkOptions::from_generation(frozen, key)
            .inherit_paths(vec!["/selected".to_owned()]),
        &original, &cancellation,
    ).await.map_err(|(error, _)| error)?;
    assert_eq!(retried.id(), forked.id());
    assert_eq!(retried.read("/selected", 14).await?, OwnedBytes::from_static(b"advanced-child"));
    assert_eq!(original.receipt().authority_records_appended, 2);
    original.receipt().verify(selected_transaction_budget())?;
    Ok(())
}

#[test]
fn ordinary_workspace_merge_overflow_retains_both_observed_operands() {
    let prior = crate::WorkCounters { bytes_hashed: u64::MAX, ..crate::WorkCounters::UNCHARGED };
    let backend = crate::WorkCounters { bytes_hashed: 1, ..crate::WorkCounters::UNCHARGED };
    let results: [crate::MeasuredResult<crate::OperationReceipt<()>, WorkspaceError>; 2] = [
        Ok(crate::OperationReceipt { value: (), work: backend }),
        Err(crate::OperationFailure::new(WorkspaceError::NotFound, backend)),
    ];
    for result in results {
        let mut work = prior;
        let error = merge_workspace_backend(result, &mut work, 0)
            .expect_err("the arithmetic control must overflow");
        assert_eq!(work, prior);
        assert!(matches!(workspace_bounded_error(&error),
            Some(WorkspaceError::WorkMerge { prior: retained, backend: actual, error: crate::WorkError::Overflow })
                if retained == prior && actual == backend));
        assert!(matches!(error,
            WorkspaceError::WorkMerge { prior: retained, backend: actual, error: crate::WorkError::Overflow }
                if retained == prior && actual == backend));
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn native_workspace_name_equal_spelling_does_not_cross_original_ledgers()
-> Result<(), Box<dyn Error>> {
    let cancellation = CancellationToken::new();
    let first = crate::NativeMaterializationWork::new(
        selected_transaction_budget(), &cancellation,
    ).map_err(|(error, _)| error)?;
    let second = crate::NativeMaterializationWork::new(
        selected_transaction_budget(), &cancellation,
    ).map_err(|(error, _)| error)?;
    assert!(!first.shares_ledger(&second));
    let first_baseline = first.live_allocation_bytes();
    let second_baseline = second.live_allocation_bytes();
    let first_name = WorkspaceName::new_with_native_work("cafe\u{301}", &first, &cancellation)?;
    let second_name = WorkspaceName::new_with_native_work("café", &second, &cancellation)?;
    assert_eq!(first_name, second_name);
    let second_held = second.live_allocation_bytes();
    let second_work = second.receipt();
    drop(first_name);
    assert_eq!(first.live_allocation_bytes(), first_baseline);
    assert_eq!(second.live_allocation_bytes(), second_held);
    assert_eq!(second.receipt(), second_work);
    assert_eq!(second_name.as_str(), "café");
    drop(second_name);
    assert_eq!(second.live_allocation_bytes(), second_baseline);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn native_workspace_name_denial_preserves_prior_live_data_and_actual_work()
-> Result<(), Box<dyn Error>> {
    let cancellation = CancellationToken::new();
    for name in ["a", "a\u{301}\u{301}\u{301}\u{301}\u{301}"] {
        let mut budget = selected_transaction_budget();
        budget.peak_allocation_bytes = crate::NativeMaterializationWork::accounting_state_bytes() + 37;
        let original = crate::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(error, _)| error)?;
        let prior_allocation = original.claim(std::mem::size_of::<[u8; 37]>() as u64)?;
        let prior_data = Box::new([0_u8; 37]);
        let before = original.receipt();
        let baseline = original.live_allocation_bytes();
        let error = WorkspaceName::new_with_native_work(name, &original, &cancellation)
            .expect_err("original live data leaves no room for a name or NFC scratch");
        assert!(matches!(error, WorkspaceError::Work(crate::WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes", ..
        })));
        let spent = original.receipt();
        assert!(spent.items_examined > before.items_examined);
        assert_eq!(spent.allocation_operations, before.allocation_operations);
        assert_eq!(spent.peak_allocation_bytes, before.peak_allocation_bytes);
        assert_eq!(spent.bytes_copied, before.bytes_copied);
        assert_eq!(spent.bytes_hashed, before.bytes_hashed);
        assert_eq!(original.live_allocation_bytes(), baseline);
        assert_eq!(prior_data.as_ref(), &[0_u8; 37]);
        drop(prior_data);
        drop(prior_allocation);
        assert_eq!(original.live_allocation_bytes(),
            crate::NativeMaterializationWork::accounting_state_bytes());
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn native_workspace_name_cancellation_and_hash_denial_do_not_fabricate_work()
-> Result<(), Box<dyn Error>> {
    for cancel_original in [false, true] {
        let root = CancellationToken::new();
        let caller = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(selected_transaction_budget(), &root)
            .map_err(|(error, _)| error)?;
        let before = original.receipt();
        let baseline = original.live_allocation_bytes();
        if cancel_original { root.cancel(); } else { caller.cancel(); }
        assert!(matches!(WorkspaceName::new_with_native_work("cafe\u{301}", &original, &caller),
            Err(WorkspaceError::Cancelled(_))));
        assert_eq!(original.receipt(), before);
        assert_eq!(original.live_allocation_bytes(), baseline);
    }
    let cancellation = CancellationToken::new();
    let mut budget = selected_transaction_budget();
    budget.bytes_hashed = 0;
    let original = crate::NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(error, _)| error)?;
    let name = WorkspaceName::new_with_native_work("cafe\u{301}", &original, &cancellation)?;
    let before = original.receipt();
    let baseline = original.live_allocation_bytes();
    assert!(matches!(WorkspaceId::derive_with_native_work([73; 16], &name, &original, &cancellation),
        Err(WorkspaceError::Work(crate::WorkError::BudgetExceeded {
            counter: "bytes_hashed", ..
        }))));
    assert_eq!(original.receipt(), before);
    assert_eq!(original.live_allocation_bytes(), baseline);
    drop(name);
    assert_eq!(original.live_allocation_bytes(),
        crate::NativeMaterializationWork::accounting_state_bytes());
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn native_kernel_errors_keep_typed_workspace_budget_and_cancellation() {
    let work = WorkspaceError::from(crate::FsError::NativeKernel(
        crate::NativeTreeMaterializationError::Work(crate::WorkError::Overflow),
    ));
    assert!(matches!(work, WorkspaceError::Work(crate::WorkError::Overflow)));
    let cancelled = WorkspaceError::from(crate::FsError::NativeKernel(
        crate::NativeTreeMaterializationError::Cancelled,
    ));
    assert!(matches!(cancelled, WorkspaceError::Cancelled(_)));
    let nested = WorkspaceError::from(crate::FsError::NativeKernel(
        crate::NativeTreeMaterializationError::Io(std::io::Error::other(
            crate::NativeTreeMaterializationError::Work(crate::WorkError::Overflow),
        )),
    ));
    assert!(matches!(nested, WorkspaceError::Work(crate::WorkError::Overflow)));
    let object = WorkspaceError::from(crate::FsError::Object(crate::ObjectStoreError::Native(
        crate::NativeTreeMaterializationError::Cancelled,
    )));
    assert!(matches!(object, WorkspaceError::Cancelled(_)));
    let authority = WorkspaceError::from(crate::FsError::Authority(
        crate::AuthorityStoreError::NativeOriginal(crate::NativeTreeMaterializationError::Work(
            crate::WorkError::Overflow,
        )),
    ));
    assert!(matches!(authority, WorkspaceError::Work(crate::WorkError::Overflow)));
}

#[test]
fn root_read_merge_overflow_keeps_both_exact_prefixes_in_workspace_errors() {
    let prior = crate::WorkCounters {
        bytes_copied: u64::MAX,
        ..crate::WorkCounters::UNCHARGED
    };
    let backend = crate::WorkCounters {
        bytes_copied: 1,
        ..crate::WorkCounters::UNCHARGED
    };
    assert_eq!(prior.with_backend(backend, 0), Err(crate::WorkError::Overflow));
    let direct = WorkspaceError::from(crate::FsError::RootReadWorkMerge {
        prior, backend, error: crate::WorkError::Overflow,
    });
    let wrapped = WorkspaceError::engine(crate::OperationFailure::new(
        crate::FsError::RootReadWorkMerge {
            prior, backend, error: crate::WorkError::Overflow,
        },
        backend,
    ));
    for error in [direct, wrapped] {
        match error {
            WorkspaceError::Filesystem(crate::FsError::RootReadWorkMerge {
                prior: actual_prior, backend: actual_backend, error,
            }) => {
                assert_eq!(actual_prior, prior);
                assert_eq!(actual_backend, backend);
                assert_eq!(error, crate::WorkError::Overflow);
            }
            other => panic!("exact completed prefixes were lost: {other}"),
        }
    }
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
    let frozen = workspace.head().await?;
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
    let frozen = workspace.head().await?;
    workspace.write("/selected", bytes::Bytes::from_static(b"current")).await?;
    let current = workspace.head().await?;
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
    assert_eq!(workspace.head().await?.id(), current.id());
    assert_eq!(workspace.read("/selected", 7).await?, OwnedBytes::from_static(b"current"));
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
    assert_eq!(workspace.read("/selected", 6).await?, OwnedBytes::from_static(b"frozen"));
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
    let frozen = workspace.head().await?;
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
    let mut transaction = workspace.begin_transaction(key).await?;
    transaction.write("/original", bytes::Bytes::from_static(b"original generation")).await?;
    let committed = transaction.commit_with_context_with_permit_measured_guarded(
        context, acyclic_stream::OwnedBytes::from_static(original_evidence),
        crate::PublicationPermit::Unrestricted,
        crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation,
        |_| std::future::ready(Ok::<_, crate::OperationFailure<WorkspaceError>>(
            crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED },
        )),
    ).await?;
    let original_generation = match committed.value {
        TransactionCommit::Committed(generation) | TransactionCommit::AlreadyCommitted(generation) => generation,
        _ => return Err("the original contextual operation did not publish".into()),
    };
    workspace.write("/later", bytes::Bytes::from_static(b"later unrelated generation")).await?;
    let current = workspace.head().await?;
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
    assert_eq!(workspace.operation_generation_measured(
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
    let mut transaction = workspace.begin_transaction(key).await?;
    transaction.write("/original", bytes::Bytes::from_static(b"original content")).await?;
    let first = workspace.reserve_publication(IdempotencyKey::new().operation_id()).await?;
    let current_permit = crate::PublicationPermit::Reservation {
        operation_id: first.operation_id.into_bytes(), gate_tail: first.gate_tail, expected: first.expected,
    };
    let mut allow = |_| std::future::ready(Ok::<_, crate::OperationFailure<WorkspaceError>>(
        crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED },
    ));
    let committed = transaction.commit_with_context_with_permit_measured_guarded(
        context, evidence.clone(), historical_permit, current_permit,
        WorkBudget::UNBOUNDED, &cancellation, &mut allow,
    ).await?;
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
    workspace.release_publication(first).await?;
    let rotated = workspace.reserve_publication(IdempotencyKey::new().operation_id()).await?;
    let rotated_permit = crate::PublicationPermit::Reservation {
        operation_id: rotated.operation_id.into_bytes(), gate_tail: rotated.gate_tail, expected: rotated.expected,
    };
    assert_ne!(rotated_permit, current_permit);
    let recovered = transaction.checkout.retry_stale_commit_guarded(
        key.operation_id(), rotated_permit, WorkBudget::UNBOUNDED, &cancellation,
        &mut allow, None, Some(context), Some(evidence.clone()), Some(historical_permit),
    ).await?;
    assert!(matches!(recovered.value, Some(crate::CheckoutCommitOutcome::AlreadyCommitted { generation_id, .. })
        if generation_id == generation.id()));
    let changed_evidence = transaction.checkout.retry_stale_commit_guarded(
        key.operation_id(), rotated_permit, WorkBudget::UNBOUNDED, &cancellation,
        &mut allow, None, Some(context),
        Some(acyclic_stream::OwnedBytes::from_static(b"a changed SDK witness")), Some(historical_permit),
    ).await?;
    assert!(matches!(changed_evidence.value, Some(crate::CheckoutCommitOutcome::IdempotencyConflict { .. })));
    transaction.write("/original", bytes::Bytes::from_static(b"changed candidate content")).await?;
    let changed_payload = transaction.checkout.retry_stale_commit_guarded(
        key.operation_id(), rotated_permit, WorkBudget::UNBOUNDED, &cancellation,
        &mut allow, None, Some(context), Some(evidence), Some(historical_permit),
    ).await?;
    assert!(matches!(changed_payload.value, Some(crate::CheckoutCommitOutcome::IdempotencyConflict { .. })));
    assert_eq!(workspace.head().await?.id(), generation.id());
    workspace.release_publication(rotated).await?;
    Ok(())
}

#[tokio::test]
async fn original_fingerprint_metadata_never_authorizes_a_foreign_current_reservation()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("original-fingerprint-current-target", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let foreign = fs.create_workspace("original-fingerprint-foreign-current-grant", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let cancellation = CancellationToken::new();
    let original_head = workspace.head().await?;
    let key = IdempotencyKey::new();
    let mut transaction = workspace.begin_transaction(key).await?;
    transaction.write("/value", bytes::Bytes::from_static(b"the genuine candidate")).await?;
    let target_reservation = workspace.reserve_publication(IdempotencyKey::new().operation_id()).await?;
    let foreign_reservation = foreign.reserve_publication(IdempotencyKey::new().operation_id()).await?;
    let foreign_permit = crate::PublicationPermit::Reservation {
        operation_id: foreign_reservation.operation_id.into_bytes(),
        gate_tail: foreign_reservation.gate_tail, expected: foreign_reservation.expected,
    };
    let rejected = transaction.commit_with_context_with_permit_measured_guarded(
        crate::Digest::from_bytes([0xc9; 32]),
        acyclic_stream::OwnedBytes::from_static(b"valid original opaque SDK witness"),
        crate::PublicationPermit::Unrestricted, foreign_permit,
        WorkBudget::UNBOUNDED, &cancellation,
        |_| std::future::ready(Ok::<_, crate::OperationFailure<WorkspaceError>>(
            crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED },
        )),
    ).await.err().ok_or("historical fingerprint metadata authorized a foreign current grant")?;
    assert!(matches!(rejected.error,
        WorkspaceError::Filesystem(crate::FsError::Authority(crate::AuthorityStoreError::RejectedStatic(_)))));
    assert!(rejected.work.bytes_hashed > 0);
    assert_eq!(rejected.work.authority_records_appended, 0);
    assert_eq!(workspace.head().await?.id(), original_head.id());
    assert!(workspace.operation_generation_record_measured(
        key, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value.is_none());
    workspace.release_publication(target_reservation).await?;
    foreign.release_publication(foreign_reservation).await?;
    Ok(())
}
