use super::*;

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
            }, crate::PublicationPermit::Unrestricted)
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
            }, crate::PublicationPermit::Unrestricted)
            .await?,
        JoinOutcome::Applied(_)
    ));
    assert_eq!(parent.read("/first.txt", 64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"first");
    assert_eq!(parent.read("/second.txt", 64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"second");
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
        }, crate::PublicationPermit::Unrestricted)
        .await?;
    let JoinOutcome::Applied(joined) = outcome else {
        return Err("join did not publish".into());
    };
    let witness = plan.commit_witness(&joined, idempotency_key, WorkBudget::UNBOUNDED, &CancellationToken::new()).map(|receipt| receipt.value).map_err(|failure| failure.error)?;
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
        joined.generation.id()
    );
    assert!(
        main.operation_generation(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?
            .is_none()
    );
    assert_eq!(joined.generation.read("/base", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, OwnedBytes::from_static(b"base"));
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
        }, crate::PublicationPermit::Unrestricted)
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
    let planned = agent.join_into(&main).plan_at(&source, &target, WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert_eq!(planned.source_head(), source.id());
    assert_eq!(planned.target_head(), target.id());
    let current = agent.join_into(&main).plan().await?;
    assert_eq!(planned.source_head(), current.source_head());
    assert_eq!(planned.target_head(), current.target_head());

    main.write_text("/advanced", "advanced").await?;
    assert!(matches!(
        agent.join_into(&main).plan_at(&source, &target, WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error),
        Err(WorkspaceError::StaleTarget)
    ));
    assert!(matches!(
        agent
            .join_into(&main)
            .plan_at(&target, &main.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error),
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
        }, crate::PublicationPermit::Unrestricted)
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
        }, crate::PublicationPermit::Unrestricted)
        .await?,
        JoinOutcome::Conflicted {
            truncated: false,
            ..
        }
    ));
    assert_eq!(main.read("/shared", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, OwnedBytes::from_static(b"main"));
    Ok(())
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
        main.verify_join_commit(&plan.commit_witness(&application, join_key, WorkBudget::UNBOUNDED, &CancellationToken::new()).map(|receipt| receipt.value).map_err(|failure| failure.error)?)
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
        }, crate::PublicationPermit::Unrestricted)
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
