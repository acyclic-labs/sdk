use super::*;
use crate::Fs;
use std::error::Error;

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
    let main = fs.create_workspace("repo").await?;
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
    assert_eq!(fs.open_workspace("repo").await?.id(), main.id());
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
async fn transaction_is_atomic_idempotent_and_visible_to_forks() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("repo").await?;
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
        Bytes::from_static(b"ready")
    );
    let reopened = workspace.generation(generation.id()).await?;
    assert_eq!(
        reopened.read("/output/nested/status", 64).await?,
        Bytes::from_static(b"ready")
    );
    let fork = workspace
        .fork(
            "copy",
            ForkOptions::from_generation(generation.clone(), IdempotencyKey::new()),
        )
        .await?;
    assert_eq!(
        fork.read("/output/nested/status", 64).await?,
        Bytes::from_static(b"ready")
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
    let workspace = fs.create_workspace("transaction-rebase").await?;

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
        Bytes::from_static(b"one")
    );
    assert_eq!(
        workspace.read("/second", 8).await?,
        Bytes::from_static(b"two")
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
        Bytes::from_static(b"winner")
    );
    Ok(())
}

#[tokio::test]
async fn transaction_preserves_links_sparse_ranges_metadata_and_cow_clones()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("shapes").await?;
    let mut transaction = workspace.begin_transaction(IdempotencyKey::new()).await?;
    transaction.create_directory("/tree").await?;
    transaction
        .write("/tree/source", Bytes::from_static(b"abcdef"))
        .await?;
    transaction
        .write("/tree/destination", Bytes::from_static(b"......"))
        .await?;
    transaction
        .create_symbolic_link("/tree/symlink", Bytes::from_static(b"source"))
        .await?;
    transaction
        .hard_link("/tree/source", "/tree/hard-link")
        .await?;
    transaction
        .write_range("/tree/source", 1, Bytes::from_static(b"Z"))
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
    let source = Bytes::from_static(b"aZ\0\0ef");
    assert_eq!(workspace.read("/tree/source", 32).await?, source);
    assert_eq!(workspace.read("/tree/hard-link", 32).await?, source);
    assert_eq!(
        workspace.read("/tree/destination", 32).await?,
        Bytes::from_static(b".aZ\0\0e\0\0")
    );
    Ok(())
}

#[tokio::test]
async fn customer_reads_are_bounded_sparse_link_aware_and_generation_exact()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("read-shapes").await?;
    let mut transaction = workspace.begin_transaction(IdempotencyKey::new()).await?;
    transaction.create_directory("/tree").await?;
    transaction
        .write("/tree/a", Bytes::from_static(b"abcdef"))
        .await?;
    transaction
        .write("/tree/sparse", Bytes::from(vec![1; 300]))
        .await?;
    transaction.hard_link("/tree/a", "/tree/b").await?;
    transaction
        .create_symbolic_link("/tree/link", Bytes::from_static(b"a"))
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

    assert_eq!(
        workspace.read_range("/tree/a", 1, 5).await?,
        Bytes::from_static(b"b\0\0ef")
    );
    let a = workspace.stat("/tree/a").await?;
    let b = workspace.stat("/tree/b").await?;
    assert_eq!(a.file_id, b.file_id);
    assert_eq!(a.link_count, 2);
    assert_eq!(a.logical_bytes, Some(8));
    assert_eq!(a.kind, FileKind::Regular);
    assert_eq!(
        workspace.read_symbolic_link("/tree/link").await?,
        Bytes::from_static(b"a")
    );

    let first = workspace.list_directory("/tree", None, 1).await?;
    assert_eq!(first.entries.len(), 1);
    assert!(first.has_more);
    let second = workspace
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
        Bytes::from_static(b"ab\0\0ef\0\0")
    );
    assert_eq!(
        workspace.read("/tree/a", 16).await?,
        Bytes::from_static(b"changed")
    );
    Ok(())
}

#[tokio::test]
async fn checkpoint_retains_the_exact_current_generation() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("repo").await?;
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
    let workspace = fs.create_workspace("repo").await?;
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
    let workspace = fs.create_workspace("repo").await?;
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
    assert!(fs.open_workspace("repo").await.is_err());
    assert!(fs.create_workspace("repo").await.is_err());
    assert_eq!(fork.head().await?.workspace_id(), fork.id());
    Ok(())
}

#[tokio::test]
async fn side_effect_free_join_combines_independent_fork_and_target_changes()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = fs.create_workspace("main").await?;
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
    assert_eq!(joined.read("/base", 16).await?, Bytes::from_static(b"base"));
    assert_eq!(main.read("/agent", 16).await?, Bytes::from_static(b"agent"));
    assert_eq!(main.read("/main", 16).await?, Bytes::from_static(b"main"));
    assert_eq!(
        agent.read("/agent", 16).await?,
        Bytes::from_static(b"agent")
    );
    assert!(matches!(
        plan.apply(ApplyOptions {
            if_target: expected,
            idempotency_key,
        })
        .await?,
        JoinOutcome::AlreadyApplied(_)
    ));
    Ok(())
}

#[tokio::test]
async fn live_rebase_advances_fork_lineage_and_preserves_independent_changes()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = fs.create_workspace("main-rebase").await?;
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
        Bytes::from_static(b"local")
    );
    assert_eq!(
        first.read("/upstream", 16).await?,
        Bytes::from_static(b"first")
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
        Bytes::from_static(b"second")
    );
    assert_eq!(
        agent.read("/agent", 16).await?,
        Bytes::from_static(b"local")
    );
    Ok(())
}

#[tokio::test]
async fn live_rebase_conflict_and_non_fork_leave_heads_unchanged() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = fs.create_workspace("main-conflict").await?;
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
        Bytes::from_static(b"local")
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
    let main = fs.create_workspace("main-ranges").await?;
    main.write("/shared", Bytes::from(vec![b'a'; 128])).await?;
    let agent = main
        .fork(
            "agent-ranges",
            ForkOptions::from_generation(main.head().await?, IdempotencyKey::new()),
        )
        .await?;
    let mut local = agent.begin_transaction(IdempotencyKey::new()).await?;
    local
        .write_range("/shared", 1, Bytes::from_static(b"XY"))
        .await?;
    assert!(matches!(
        local.commit().await?,
        TransactionCommit::Committed(_)
    ));
    let mut upstream = main.begin_transaction(IdempotencyKey::new()).await?;
    upstream
        .write_range("/shared", 64, Bytes::from_static(b"UV"))
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
    let main = fs.create_workspace("main").await?;
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
        Bytes::from_static(b"winner")
    );
    Ok(())
}

#[tokio::test]
async fn semantic_change_set_and_overlap_conflict_are_exact() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = fs.create_workspace("main").await?;
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
    assert_eq!(main.read("/shared", 16).await?, Bytes::from_static(b"main"));
    Ok(())
}

#[tokio::test]
async fn contiguous_change_sets_compose_by_outer_semantics() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("compose").await?;
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
