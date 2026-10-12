use super::*;

#[tokio::test]
async fn bounded_common_ancestor_discovery_is_direction_independent_at_the_frontier()
-> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let main = fs.create_workspace("lineage-main", B, &cancel).await?.value;
    let base = main.head(B, &cancel).await?.value;
    let left = main
        .fork::<_, _, crate::WorkspaceError>(
            "lineage-left",
            ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            &mut allow::<FsError>,
        )
        .await?
        .value;
    let right = main
        .fork::<_, _, crate::WorkspaceError>(
            "lineage-right",
            ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            &mut allow::<FsError>,
        )
        .await?
        .value;

    assert_eq!(
        main.join_into(&left)
            .bounds(1, 32, 32)
            .plan(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?
            .common_ancestor(),
        base.id()
    );
    assert_eq!(
        left.join_into(&main)
            .bounds(1, 32, 32)
            .plan(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?
            .common_ancestor(),
        base.id()
    );
    assert!(matches!(
        left.join_into(&right).bounds(1, 32, 32).plan(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error),
        Err(WorkspaceError::LineageLimit)
    ));
    assert!(matches!(
        right.join_into(&left).bounds(1, 32, 32).plan(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error),
        Err(WorkspaceError::LineageLimit)
    ));

    let unrelated = fs
        .create_workspace("lineage-unrelated", B, &cancel)
        .await?
        .value;
    assert!(matches!(
        main.join_into(&unrelated).bounds(1, 32, 32).plan(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error),
        Err(WorkspaceError::NoCommonAncestor)
    ));
    assert!(matches!(
        unrelated.join_into(&main).bounds(1, 32, 32).plan(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error),
        Err(WorkspaceError::NoCommonAncestor)
    ));
    assert!(matches!(
        main.join_into(&left).bounds(0, 32, 32).plan(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error),
        Err(WorkspaceError::JoinLimit)
    ));
    Ok(())
}

#[tokio::test]
async fn live_rebase_advances_fork_lineage_and_preserves_independent_changes()
-> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let main = fs.create_workspace("main-rebase", B, &cancel).await?.value;
    main.write_text("/base", "base").await?;
    let agent = main
        .fork::<_, _, crate::WorkspaceError>(
            "agent-rebase",
            ForkOptions::from_generation(main.head(B, &cancel).await?.value, IdempotencyKey::new()),
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            &mut allow::<FsError>,
        )
        .await?
        .value;
    agent.write_text("/agent", "local").await?;
    main.write_text("/upstream", "first").await?;

    let first_key = IdempotencyKey::new();
    let WorkspaceRebase::Rebased(first) = agent.live_rebase(first_key, 128, 128, 32).await? else {
        return Err("first live rebase did not publish".into());
    };
    assert_eq!(
        first.read("/agent", 16, B, &cancel).await?.value,
        OwnedBytes::from_static(b"local")
    );
    assert_eq!(
        first.read("/upstream", 16, B, &cancel).await?.value,
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
        agent.read("/later", 16, B, &cancel).await?.value,
        OwnedBytes::from_static(b"second")
    );
    assert_eq!(
        agent.read("/agent", 16, B, &cancel).await?.value,
        OwnedBytes::from_static(b"local")
    );
    Ok(())
}

#[tokio::test]
async fn live_rebase_conflict_and_non_fork_leave_heads_unchanged() -> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let main = fs
        .create_workspace("main-conflict", B, &cancel)
        .await?
        .value;
    main.write_text("/shared", "base").await?;
    let agent = main
        .fork::<_, _, crate::WorkspaceError>(
            "agent-conflict",
            ForkOptions::from_generation(main.head(B, &cancel).await?.value, IdempotencyKey::new()),
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            &mut allow::<FsError>,
        )
        .await?
        .value;
    agent.write_text("/shared", "local").await?;
    let before = agent.head(B, &cancel).await?.value.id();
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
    assert_eq!(agent.head(B, &cancel).await?.value.id(), before);
    assert_eq!(
        agent.read("/shared", 16, B, &cancel).await?.value,
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
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let main = fs.create_workspace("main-ranges", B, &cancel).await?.value;
    main.write("/shared", bytes::Bytes::from(vec![b'a'; 128]))
        .await?;
    let agent = main
        .fork::<_, _, crate::WorkspaceError>(
            "agent-ranges",
            ForkOptions::from_generation(main.head(B, &cancel).await?.value, IdempotencyKey::new()),
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            &mut allow::<FsError>,
        )
        .await?
        .value;
    let mut local = agent
        .begin_transaction(IdempotencyKey::new(), B, &cancel)
        .await?
        .value;
    local
        .write_range("/shared", 1, OwnedBytes::from_static(b"XY"), B, &cancel)
        .await?
        .value;
    assert!(matches!(
        local
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
    let mut upstream = main
        .begin_transaction(IdempotencyKey::new(), B, &cancel)
        .await?
        .value;
    upstream
        .write_range("/shared", 64, OwnedBytes::from_static(b"UV"), B, &cancel)
        .await?
        .value;
    assert!(matches!(
        upstream
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
    assert!(matches!(
        agent
            .live_rebase(IdempotencyKey::new(), 128, 128, 32)
            .await?,
        WorkspaceRebase::Rebased(_)
    ));
    let merged = agent.read("/shared", 128, B, &cancel).await?.value;
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
