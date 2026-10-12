use super::*;

#[tokio::test]
async fn repeated_join_preserves_prior_incremental_publications() -> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let parent = fs
        .create_workspace("incremental-parent", B, &cancel)
        .await?
        .value;
    let parent_base = parent.head(B, &cancel).await?.value;
    let child = parent
        .fork(
            "incremental-child",
            ForkOptions::from_generation(
                parent_base.clone(),
                IdempotencyKey::from_bytes([0x67; 16]),
            ),
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            allow::<FsError>,
        )
        .await?
        .value;
    child.write_text("/first.txt", "first").await?;
    let first = child.join_into(&parent).plan(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert!(matches!(
        first
            .apply(
                ApplyOptions {
                    if_target: first.target_head(),
                    idempotency_key: IdempotencyKey::from_bytes([0x68; 16]),
                },
                PublicationPermit::Unrestricted
            )
            .await?,
        JoinOutcome::Applied(_)
    ));
    child.write_text("/second.txt", "second").await?;
    let second = child.join_into(&parent).plan(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert!(matches!(
        second
            .apply(
                ApplyOptions {
                    if_target: second.target_head(),
                    idempotency_key: IdempotencyKey::from_bytes([0x69; 16]),
                },
                PublicationPermit::Unrestricted
            )
            .await?,
        JoinOutcome::Applied(_)
    ));
    assert_eq!(
        parent
            .read("/first.txt", 64, B, &cancel)
            .await?
            .value
            .as_ref(),
        b"first"
    );
    assert_eq!(
        parent
            .read("/second.txt", 64, B, &cancel)
            .await?
            .value
            .as_ref(),
        b"second"
    );
    Ok(())
}

#[tokio::test]
async fn side_effect_free_join_combines_independent_fork_and_target_changes()
-> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let main = fs.create_workspace("main", B, &cancel).await?.value;
    main.write_text("/base", "base").await?;
    let base = main.head(B, &cancel).await?.value;
    let agent = main
        .fork(
            "agent",
            ForkOptions::from_generation(base, IdempotencyKey::new()),
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            allow::<FsError>,
        )
        .await?
        .value;
    agent.write_text("/agent", "agent").await?;
    main.write_text("/main", "main").await?;

    let plan = agent.join_into(&main).plan(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let expected = plan.target_head();
    let idempotency_key = IdempotencyKey::new();
    assert!(main.read("/agent", 16, B, &cancel).await.is_err());
    let outcome = plan
        .apply(
            ApplyOptions {
                if_target: expected,
                idempotency_key,
            },
            PublicationPermit::Unrestricted,
        )
        .await?;
    let JoinOutcome::Applied(joined) = outcome else {
        return Err("join did not publish".into());
    };
    let witness = plan
        .commit_witness(&joined, idempotency_key, B, &cancel)?
        .value;
    assert!(main.verify_join_commit(&witness).await?);
    let mut forged = serde_json::to_value(witness)?;
    forged["source_generation"] =
        serde_json::to_value(crate::GenerationId::new(crate::Digest::ZERO))?;
    let forged: crate::JoinCommitWitness = serde_json::from_value(forged)?;
    assert!(!main.verify_join_commit(&forged).await?);
    assert_eq!(
        main.operation_generation(idempotency_key, B, &cancel)
            .await?
            .value
            .ok_or("join operation was not recoverable")?
            .id(),
        joined.generation.id()
    );
    assert!(
        main.operation_generation(IdempotencyKey::new(), B, &cancel)
            .await?
            .value
            .is_none()
    );
    assert_eq!(
        joined.generation.read("/base", 16, B, &cancel).await?.value,
        OwnedBytes::from_static(b"base")
    );
    assert_eq!(
        main.read("/agent", 16, B, &cancel).await?.value,
        OwnedBytes::from_static(b"agent")
    );
    assert_eq!(
        main.read("/main", 16, B, &cancel).await?.value,
        OwnedBytes::from_static(b"main")
    );
    assert_eq!(
        agent.read("/agent", 16, B, &cancel).await?.value,
        OwnedBytes::from_static(b"agent")
    );
    assert!(matches!(
        plan.apply(
            ApplyOptions {
                if_target: expected,
                idempotency_key,
            },
            PublicationPermit::Unrestricted
        )
        .await?,
        JoinOutcome::AlreadyApplied(_)
    ));
    main.write_text("/after-join", "later").await?;
    assert!(main.verify_join_commit(&witness).await?);
    let ordinary_key = IdempotencyKey::new();
    let mut ordinary = main
        .begin_transaction(ordinary_key, B, &cancel)
        .await?
        .value;
    ordinary.write_text("/ordinary", "not a join").await?;
    assert!(matches!(
        ordinary
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
    let ordinary_generation = main
        .operation_generation(ordinary_key, B, &cancel)
        .await?
        .value
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
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let main = fs.create_workspace("main-exact", B, &cancel).await?.value;
    main.write_text("/base", "base").await?;
    let base = main.head(B, &cancel).await?.value;
    let agent = main
        .fork(
            "agent-exact",
            ForkOptions::from_generation(base, IdempotencyKey::new()),
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            allow::<FsError>,
        )
        .await?
        .value;
    agent.write_text("/agent", "agent").await?;
    let source = agent.head(B, &cancel).await?.value;
    let target = main.head(B, &cancel).await?.value;
    let planned = agent
        .join_into(&main)
        .plan_at(&source, &target, B, &cancel)
        .await?
        .value;
    assert_eq!(planned.source_head(), source.id());
    assert_eq!(planned.target_head(), target.id());
    let current = agent.join_into(&main).plan(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert_eq!(planned.source_head(), current.source_head());
    assert_eq!(planned.target_head(), current.target_head());

    main.write_text("/advanced", "advanced").await?;
    assert!(matches!(
        agent
            .join_into(&main)
            .plan_at(&source, &target, B, &cancel)
            .await,
        Err(OperationFailure {
            error: WorkspaceError::StaleTarget,
            ..
        })
    ));
    assert!(matches!(
        agent
            .join_into(&main)
            .plan_at(&target, &main.head(B, &cancel).await?.value, B, &cancel)
            .await,
        Err(OperationFailure {
            error: WorkspaceError::ForeignGeneration,
            ..
        })
    ));
    Ok(())
}

#[tokio::test]
async fn join_rejects_a_stale_target_without_mutation() -> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let main = fs.create_workspace("main", B, &cancel).await?.value;
    let base = main.head(B, &cancel).await?.value;
    let agent = main
        .fork(
            "agent",
            ForkOptions::from_generation(base, IdempotencyKey::new()),
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            allow::<FsError>,
        )
        .await?
        .value;
    agent.write_text("/agent", "agent").await?;
    let plan = agent.join_into(&main).plan(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let expected = plan.target_head();
    main.write_text("/raced", "winner").await?;
    let outcome = plan
        .apply(
            ApplyOptions {
                if_target: expected,
                idempotency_key: IdempotencyKey::new(),
            },
            PublicationPermit::Unrestricted,
        )
        .await?;
    assert!(matches!(outcome, JoinOutcome::StaleTarget(_)));
    assert!(main.read("/agent", 16, B, &cancel).await.is_err());
    assert_eq!(
        main.read("/raced", 16, B, &cancel).await?.value,
        OwnedBytes::from_static(b"winner")
    );
    Ok(())
}

#[tokio::test]
async fn merge_drivers_resolve_and_publish_real_workspace_joins() -> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let main = fs.create_workspace("driver-main", B, &cancel).await?.value;
    main.write_text("/shared", "base").await?;
    let agent = main
        .fork(
            "driver-agent",
            ForkOptions::from_generation(main.head(B, &cancel).await?.value, IdempotencyKey::new()),
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            allow::<FsError>,
        )
        .await?
        .value;
    agent.write_text("/shared", "agent").await?;
    main.write_text("/shared", "main").await?;

    let plan = agent.join_into(&main).plan(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
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
        main.verify_join_commit(
            &plan
                .commit_witness(&application, join_key, B, &cancel)?
                .value
        )
        .await?
    );
    assert_eq!(
        main.read("/shared", 16, B, &cancel).await?.value,
        OwnedBytes::from_static(b"agent")
    );
    Ok(())
}

#[tokio::test]
async fn default_text_driver_projects_markers_and_publishes_them() -> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let main = fs
        .create_workspace("text-driver-main", B, &cancel)
        .await?
        .value;
    main.write_text("/shared", "base\n").await?;
    let agent = main
        .fork(
            "text-driver-agent",
            ForkOptions::from_generation(main.head(B, &cancel).await?.value, IdempotencyKey::new()),
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            allow::<FsError>,
        )
        .await?
        .value;
    agent.write_text("/shared", "agent\n").await?;
    main.write_text("/shared", "main\n").await?;

    let plan = agent.join_into(&main).plan(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let JoinOutcome::Conflicted {
        conflicts,
        truncated,
    } = plan
        .apply(
            ApplyOptions {
                if_target: plan.target_head(),
                idempotency_key: IdempotencyKey::new(),
            },
            PublicationPermit::Unrestricted,
        )
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
    let merged = String::from_utf8(main.read("/shared", 1024, B, &cancel).await?.value.to_vec())?;
    assert!(merged.contains("<<<<<<< ours"));
    assert!(merged.contains("main"));
    assert!(merged.contains("agent"));
    Ok(())
}

mod changes;
mod rebase;
