use super::*;

#[tokio::test]
async fn repeated_join_preserves_prior_incremental_publications() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let parent = unbounded!(fs, create_workspace("incremental-parent"))?;
    let parent_base = unbounded!(parent, head())?;
    let child = fork_workspace(
        &parent,
        "incremental-child",
        ForkOptions::from_generation(parent_base.clone(), IdempotencyKey::from_bytes([0x67; 16])),
    )
    .await?;
    child.write_text("/first.txt", "first").await?;
    let first = unbounded!(child.join_into(&parent), plan())?;
    assert!(matches!(
        first
            .apply(
                ApplyOptions {
                    if_target: first.target_head(),
                    idempotency_key: IdempotencyKey::from_bytes([0x68; 16]),
                },
                std::collections::BTreeMap::new(),
                crate::PublicationPermit::Unrestricted
            )
            .await?,
        JoinOutcome::Applied(_)
    ));
    child.write_text("/second.txt", "second").await?;
    let second = unbounded!(child.join_into(&parent), plan())?;
    assert!(matches!(
        second
            .apply(
                ApplyOptions {
                    if_target: second.target_head(),
                    idempotency_key: IdempotencyKey::from_bytes([0x69; 16]),
                },
                std::collections::BTreeMap::new(),
                crate::PublicationPermit::Unrestricted
            )
            .await?,
        JoinOutcome::Applied(_)
    ));
    assert_eq!(
        unbounded!(parent, read("/first.txt", 64))?.as_ref(),
        b"first"
    );
    assert_eq!(
        unbounded!(parent, read("/second.txt", 64))?.as_ref(),
        b"second"
    );
    Ok(())
}

#[tokio::test]
async fn side_effect_free_join_combines_independent_fork_and_target_changes()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = unbounded!(fs, create_workspace("main"))?;
    main.write_text("/base", "base").await?;
    let base = unbounded!(main, head())?;
    let agent = fork_workspace(
        &main,
        "agent",
        ForkOptions::from_generation(base, IdempotencyKey::new()),
    )
    .await?;
    agent.write_text("/agent", "agent").await?;
    main.write_text("/main", "main").await?;

    let plan = unbounded!(agent.join_into(&main), plan())?;
    let expected = plan.target_head();
    let idempotency_key = IdempotencyKey::new();
    assert!(unbounded!(main, read("/agent", 16)).is_err());
    let outcome = plan
        .apply(
            ApplyOptions {
                if_target: expected,
                idempotency_key,
            },
            std::collections::BTreeMap::new(),
            crate::PublicationPermit::Unrestricted,
        )
        .await?;
    let JoinOutcome::Applied(joined) = outcome else {
        return Err("join did not publish".into());
    };
    let witness = value!(plan.commit_witness(
        &joined,
        idempotency_key,
        &WorkBudget::UNBOUNDED,
        &CancellationToken::new()
    ))?;
    assert!(unbounded!(main, verify_join_commit(&witness))?);
    for change_result in [false, true] {
        let mut altered = witness;
        let generation = if change_result {
            &mut altered.result_generation
        } else {
            &mut altered.source_generation
        };
        let mut reference = generation.object_id();
        reference.storage = Some(crate::ObjectStorage {
            locator: crate::Digest::from_bytes([7; 32]),
            root_file_id: crate::FileId::from_bytes([8; 16]),
            origin_generation: crate::Digest::from_bytes([9; 32]),
        });
        *generation = crate::GenerationId::from_root(reference);
        assert!(!unbounded!(main, verify_join_commit(&altered))?);
    }
    let mut forged = serde_json::to_value(witness)?;
    forged["source_generation"] =
        serde_json::to_value(crate::GenerationId::new(crate::Digest::ZERO))?;
    let forged: crate::JoinCommitWitness = serde_json::from_value(forged)?;
    assert!(!unbounded!(main, verify_join_commit(&forged))?);
    assert_eq!(
        unbounded!(main, operation_generation(idempotency_key))?
            .ok_or("join operation was not recoverable")?
            .id(),
        joined.generation.id()
    );
    assert!(unbounded!(main, operation_generation(IdempotencyKey::new()))?.is_none());
    assert_eq!(
        unbounded!(joined.generation, read("/base", 16))?,
        OwnedBytes::from_static(b"base")
    );
    assert_eq!(
        unbounded!(main, read("/agent", 16))?,
        OwnedBytes::from_static(b"agent")
    );
    assert_eq!(
        unbounded!(main, read("/main", 16))?,
        OwnedBytes::from_static(b"main")
    );
    assert_eq!(
        unbounded!(agent, read("/agent", 16))?,
        OwnedBytes::from_static(b"agent")
    );
    assert!(matches!(
        plan.apply(
            ApplyOptions {
                if_target: expected,
                idempotency_key,
            },
            std::collections::BTreeMap::new(),
            crate::PublicationPermit::Unrestricted
        )
        .await?,
        JoinOutcome::AlreadyApplied(_)
    ));
    main.write_text("/after-join", "later").await?;
    assert!(unbounded!(main, verify_join_commit(&witness))?);
    let ordinary_key = IdempotencyKey::new();
    let mut ordinary = unbounded!(main, begin_transaction(ordinary_key))?;
    ordinary.write_text("/ordinary", "not a join").await?;
    assert!(matches!(
        commit(&mut ordinary).await?,
        TransactionCommit::Committed(_)
    ));
    let ordinary_generation = unbounded!(main, operation_generation(ordinary_key))?
        .ok_or("ordinary operation was not recoverable")?;
    let mut impostor = serde_json::to_value(witness)?;
    impostor["operation_id"] = serde_json::to_value(ordinary_key.operation_id())?;
    impostor["result_generation"] = serde_json::to_value(ordinary_generation.id())?;
    let impostor: crate::JoinCommitWitness = serde_json::from_value(impostor)?;
    assert!(!unbounded!(main, verify_join_commit(&impostor))?);
    Ok(())
}

#[tokio::test]
async fn exact_generation_join_matches_head_plan_and_rejects_stale_target()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = unbounded!(fs, create_workspace("main-exact"))?;
    main.write_text("/base", "base").await?;
    let base = unbounded!(main, head())?;
    let agent = fork_workspace(
        &main,
        "agent-exact",
        ForkOptions::from_generation(base, IdempotencyKey::new()),
    )
    .await?;
    agent.write_text("/agent", "agent").await?;
    let source = unbounded!(agent, head())?;
    let target = unbounded!(main, head())?;
    let planned = unbounded!(agent.join_into(&main), plan_at(&source, &target))?;
    assert_eq!(planned.source_head(), source.id());
    assert_eq!(planned.target_head(), target.id());
    let current = unbounded!(agent.join_into(&main), plan())?;
    assert_eq!(planned.source_head(), current.source_head());
    assert_eq!(planned.target_head(), current.target_head());

    main.write_text("/advanced", "advanced").await?;
    assert!(matches!(
        unbounded!(agent.join_into(&main), plan_at(&source, &target)),
        Err(WorkspaceError::StaleTarget)
    ));
    assert!(matches!(
        unbounded!(
            agent.join_into(&main),
            plan_at(&target, &unbounded!(main, head())?)
        ),
        Err(WorkspaceError::ForeignGeneration)
    ));
    Ok(())
}

#[tokio::test]
async fn bounded_common_ancestor_discovery_is_direction_independent_at_the_frontier()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = unbounded!(fs, create_workspace("lineage-main"))?;
    let base = unbounded!(main, head())?;
    let left = fork_workspace(
        &main,
        "lineage-left",
        ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
    )
    .await?;
    let right = fork_workspace(
        &main,
        "lineage-right",
        ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
    )
    .await?;

    assert_eq!(
        unbounded!(main.join_into(&left).bounds(1, 32, 32), plan())?.common_ancestor(),
        base.id()
    );
    assert_eq!(
        unbounded!(left.join_into(&main).bounds(1, 32, 32), plan())?.common_ancestor(),
        base.id()
    );
    assert!(matches!(
        unbounded!(left.join_into(&right).bounds(1, 32, 32), plan()),
        Err(WorkspaceError::LineageLimit)
    ));
    assert!(matches!(
        unbounded!(right.join_into(&left).bounds(1, 32, 32), plan()),
        Err(WorkspaceError::LineageLimit)
    ));

    let unrelated = unbounded!(fs, create_workspace("lineage-unrelated"))?;
    assert!(matches!(
        unbounded!(main.join_into(&unrelated).bounds(1, 32, 32), plan()),
        Err(WorkspaceError::NoCommonAncestor)
    ));
    assert!(matches!(
        unbounded!(unrelated.join_into(&main).bounds(1, 32, 32), plan()),
        Err(WorkspaceError::NoCommonAncestor)
    ));
    assert!(matches!(
        unbounded!(main.join_into(&left).bounds(0, 32, 32), plan()),
        Err(WorkspaceError::JoinLimit)
    ));
    Ok(())
}

#[tokio::test]
async fn join_rejects_a_stale_target_without_mutation() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = unbounded!(fs, create_workspace("main"))?;
    let base = unbounded!(main, head())?;
    let agent = fork_workspace(
        &main,
        "agent",
        ForkOptions::from_generation(base, IdempotencyKey::new()),
    )
    .await?;
    agent.write_text("/agent", "agent").await?;
    let plan = unbounded!(agent.join_into(&main), plan())?;
    let expected = plan.target_head();
    main.write_text("/raced", "winner").await?;
    let outcome = plan
        .apply(
            ApplyOptions {
                if_target: expected,
                idempotency_key: IdempotencyKey::new(),
            },
            std::collections::BTreeMap::new(),
            crate::PublicationPermit::Unrestricted,
        )
        .await?;
    assert!(matches!(outcome, JoinOutcome::StaleTarget(_)));
    assert!(unbounded!(main, read("/agent", 16)).is_err());
    assert_eq!(
        unbounded!(main, read("/raced", 16))?,
        OwnedBytes::from_static(b"winner")
    );
    Ok(())
}
