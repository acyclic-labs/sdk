use super::*;

#[tokio::test]
async fn filtered_fork_rebase_preserves_local_and_parent_changes() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let parent = unbounded!(fs, create_workspace("filtered-rebase-parent"))?;
    parent.write_text("/omitted", "base").await?;
    let child = fork_workspace(
        &parent,
        "filtered-rebase-child",
        ForkOptions::from_generation(unbounded!(parent, head())?, IdempotencyKey::new()).empty(),
    )
    .await?;
    child.write_text("/local", "child").await?;
    parent.write_text("/omitted", "user edit").await?;
    assert!(matches!(
        child.live_rebase(IdempotencyKey::new(), 64, 64, 16).await?,
        WorkspaceRebase::Rebased(_)
    ));
    assert_eq!(
        unbounded!(child, read("/omitted", 16))?.as_ref(),
        b"user edit"
    );
    assert_eq!(unbounded!(child, read("/local", 16))?.as_ref(), b"child");
    child.write_text("/local", "after rebase").await?;
    let source = unbounded!(child, head())?;
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
    assert_eq!(
        unbounded!(parent, read("/omitted", 16))?.as_ref(),
        b"user edit"
    );
    assert_eq!(
        unbounded!(parent, read("/local", 16))?.as_ref(),
        b"after rebase"
    );
    assert_eq!(unbounded!(child, head())?.id(), source.id());
    Ok(())
}

#[tokio::test]
async fn nested_filtered_fork_uses_its_pinned_inheritance_as_delta_baseline()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let root = unbounded!(fs, create_workspace("nested-root"))?;
    root.write_text("/keep", "root").await?;
    root.write_text("/omit", "root").await?;
    let parent = fork_workspace(
        &root,
        "nested-parent",
        ForkOptions::from_generation(unbounded!(root, head())?, IdempotencyKey::new()),
    )
    .await?;
    parent.write_text("/keep", "inherited").await?;
    let child = fork_workspace(
        &parent,
        "nested-child",
        ForkOptions::from_generation(unbounded!(parent, head())?, IdempotencyKey::new())
            .inherit_paths(vec!["/keep".into()]),
    )
    .await?;
    child.write_text("/new", "child").await?;
    let plan = child.join_into(&root).plan().await?;
    plan.apply(
        ApplyOptions {
            if_target: plan.target_head(),
            idempotency_key: IdempotencyKey::new(),
        },
        crate::PublicationPermit::Unrestricted,
    )
    .await?;
    assert_eq!(unbounded!(root, read("/keep", 32))?.as_ref(), b"root");
    assert_eq!(unbounded!(root, read("/omit", 32))?.as_ref(), b"root");
    assert_eq!(unbounded!(root, read("/new", 32))?.as_ref(), b"child");
    child.write_text("/keep", "edited").await?;
    let plan = child.join_into(&root).plan().await?;
    assert!(matches!(
        plan.apply(
            ApplyOptions {
                if_target: plan.target_head(),
                idempotency_key: IdempotencyKey::new(),
            },
            crate::PublicationPermit::Unrestricted
        )
        .await?,
        JoinOutcome::Conflicted { .. }
    ));
    root.write_text("/keep", "inherited").await?;
    let plan = child.join_into(&root).plan().await?;
    plan.apply(
        ApplyOptions {
            if_target: plan.target_head(),
            idempotency_key: IdempotencyKey::new(),
        },
        crate::PublicationPermit::Unrestricted,
    )
    .await?;
    assert_eq!(unbounded!(root, read("/keep", 32))?.as_ref(), b"edited");
    assert_eq!(
        unbounded!(parent, read("/keep", 32))?.as_ref(),
        b"inherited"
    );
    Ok(())
}

async fn selected_hardlinks(bytes: &[u8], carry_alias: bool) -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let parent = unbounded!(fs, create_workspace("hardlink-parent"))?;
    parent
        .write("/file", bytes::Bytes::copy_from_slice(bytes))
        .await?;
    let mut transaction = unbounded!(parent, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(transaction, hard_link("/file", "/alias"))?;
    commit(&mut transaction).await?;
    let mut paths = vec!["/file".to_owned()];
    if carry_alias {
        paths.push("/alias".to_owned());
    }
    let child = fork_workspace(
        &parent,
        "hardlink-child",
        ForkOptions::from_generation(unbounded!(parent, head())?, IdempotencyKey::new())
            .inherit_paths(paths),
    )
    .await?;
    assert_eq!(unbounded!(child, read("/file", 64))?.as_ref(), bytes);
    if carry_alias {
        assert_eq!(unbounded!(child, read("/alias", 64))?.as_ref(), bytes);
    } else {
        assert!(unbounded!(child, read("/alias", 64)).is_err());
    }
    child
        .write("/file", bytes::Bytes::from_static(b"changed"))
        .await?;
    if carry_alias {
        assert_eq!(unbounded!(child, read("/alias", 64))?.as_ref(), b"changed");
    }
    assert_eq!(unbounded!(parent, read("/alias", 64))?.as_ref(), bytes);
    Ok(())
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(48))]
    #[test]
    fn selected_fork_hardlink_aliases_keep_exact_content(bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 1..64), carry_alias in proptest::prelude::any::<bool>()) {
        let result = futures::executor::block_on(selected_hardlinks(&bytes, carry_alias));
        proptest::prop_assert!(result.is_ok(), "{result:?}");
    }
}

#[tokio::test]
async fn live_rebase_advances_fork_lineage_and_preserves_independent_changes()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = unbounded!(fs, create_workspace("main-rebase"))?;
    main.write_text("/base", "base").await?;
    let agent = fork_workspace(
        &main,
        "agent-rebase",
        ForkOptions::from_generation(unbounded!(main, head())?, IdempotencyKey::new()),
    )
    .await?;
    agent.write_text("/agent", "local").await?;
    main.write_text("/upstream", "first").await?;

    let first_key = IdempotencyKey::new();
    let WorkspaceRebase::Rebased(first) = agent.live_rebase(first_key, 128, 128, 32).await? else {
        return Err("first live rebase did not publish".into());
    };
    assert_eq!(
        unbounded!(first, read("/agent", 16))?,
        OwnedBytes::from_static(b"local")
    );
    assert_eq!(
        unbounded!(first, read("/upstream", 16))?,
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
        unbounded!(agent, read("/later", 16))?,
        OwnedBytes::from_static(b"second")
    );
    assert_eq!(
        unbounded!(agent, read("/agent", 16))?,
        OwnedBytes::from_static(b"local")
    );
    Ok(())
}

#[tokio::test]
async fn live_rebase_conflict_and_non_fork_leave_heads_unchanged() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = unbounded!(fs, create_workspace("main-conflict"))?;
    main.write_text("/shared", "base").await?;
    let agent = fork_workspace(
        &main,
        "agent-conflict",
        ForkOptions::from_generation(unbounded!(main, head())?, IdempotencyKey::new()),
    )
    .await?;
    agent.write_text("/shared", "local").await?;
    let before = unbounded!(agent, head())?.id();
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
    assert_eq!(unbounded!(agent, head())?.id(), before);
    assert_eq!(
        unbounded!(agent, read("/shared", 16))?,
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
    let main = unbounded!(fs, create_workspace("main-ranges"))?;
    main.write("/shared", OwnedBytes::from(vec![b'a'; 128]))
        .await?;
    let agent = fork_workspace(
        &main,
        "agent-ranges",
        ForkOptions::from_generation(unbounded!(main, head())?, IdempotencyKey::new()),
    )
    .await?;
    let mut local = unbounded!(agent, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(
        local,
        write_range("/shared", 1, OwnedBytes::from_static(b"XY"))
    )?;
    assert!(matches!(
        commit(&mut local).await?,
        TransactionCommit::Committed(_)
    ));
    let mut upstream = unbounded!(main, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(
        upstream,
        write_range("/shared", 64, OwnedBytes::from_static(b"UV"))
    )?;
    assert!(matches!(
        commit(&mut upstream).await?,
        TransactionCommit::Committed(_)
    ));
    assert!(matches!(
        agent
            .live_rebase(IdempotencyKey::new(), 128, 128, 32)
            .await?,
        WorkspaceRebase::Rebased(_)
    ));
    let merged = unbounded!(agent, read("/shared", 128))?;
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
