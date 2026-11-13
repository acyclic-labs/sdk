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
    parent.write_text("/first.txt", "resolved").await?;
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
        b"resolved"
    );
    assert_eq!(
        unbounded!(parent, read("/second.txt", 64))?.as_ref(),
        b"second"
    );
    let repeated = unbounded!(child.join_into(&parent), plan())?;
    assert!(matches!(
        repeated
            .apply(
                ApplyOptions {
                    if_target: repeated.target_head(),
                    idempotency_key: IdempotencyKey::from_bytes([0x6a; 16]),
                },
                std::collections::BTreeMap::new(),
                crate::PublicationPermit::Unrestricted
            )
            .await?,
        JoinOutcome::NoChanges(_)
    ));
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
    let cancellation = CancellationToken::new();
    let mut accounting =
        crate::kernel::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let (_, expected_head) = fs
        .workspace_head_state(&main.volume, &mut accounting, &cancellation)
        .await?;
    let source = unbounded!(agent, head())?;
    let resolutions = std::collections::BTreeMap::new();
    let prepared = fs
        .prepare_workspace_join_with_accounting::<WorkspaceError>(
            crate::facade::WorkspaceJoinRequest {
                target: &main.volume,
                base: plan.base,
                source: &source,
                expected_target: expected,
                expected_head,
                history: crate::JoinHistory::Merge,
                operation_id: idempotency_key.operation_id(),
                permit: crate::PublicationPermit::Unrestricted,
                maximum_generations: None,
                maximum_changes: None,
                maximum_conflicts: None,
                resolutions: crate::facade::WorkspaceJoinResolutions::Map(&resolutions),
            },
            &mut accounting,
            &cancellation,
        )
        .await?;
    assert!(prepared.candidate_root().is_some());
    assert_eq!(unbounded!(main, head())?.id(), expected);
    assert_eq!(unbounded!(agent, head())?.id(), source.id());
    drop(prepared);
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

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn common_ancestor_copy_work_grows_with_the_graph_instead_of_insertion_order()
-> Result<(), Box<dyn Error>> {
    use crate::kernel::{GenerationRoot, KernelAccounting, Owned};

    crate::initialize_test_native_io();
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("wide-lineage"))?;
    let cancellation = CancellationToken::new();
    let mut setup = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let mut leaves = Vec::new();
    let mut template = None;
    for ordinal in 0..64 {
        workspace.write_text("/value", &ordinal.to_string()).await?;
        let generation = unbounded!(workspace, head())?;
        let (root, allocation) = crate::kernel::read_generation_root_with_accounting(
            fs.objects(),
            generation.id().object_id(),
            workspace.volume.config(),
            &mut setup,
            &cancellation,
        )
        .await?;
        let bytes = (root.parents.capacity() * std::mem::size_of::<GenerationId>()) as u64;
        let root = Owned::new(root, allocation, bytes);
        let leaf = GenerationRoot {
            volume_id: root.value.volume_id,
            root_file_id: root.value.root_file_id,
            file_table: root.value.file_table,
            parents: Vec::new(),
            required_features: 0,
        };
        leaves.push(GenerationId::from_root(
            crate::test_support::put_generation_root(fs.objects(), &leaf).await?,
        ));
        template = Some(leaf);
        root.retire(&mut setup)?;
    }
    // Descending identities force the old sorted vector to copy every suffix.
    leaves.sort_unstable_by(|left, right| right.cmp(left));
    let source = unbounded!(workspace, generation(leaves[0]))?;
    let mut copied = Vec::new();
    for count in [16, 64] {
        let mut root = template.clone().ok_or("missing lineage root")?;
        root.parents.extend_from_slice(&leaves[..count]);
        let id = GenerationId::from_root(
            crate::test_support::put_generation_root(fs.objects(), &root).await?,
        );
        let target = unbounded!(workspace, generation(id))?;
        // Parent duplicate validation sorts in the shared decoder. Measure that
        // unchanged work directly so this bound isolates traversal storage.
        let decoding = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let mut accounting = KernelAccounting::native(&decoding, &cancellation);
        let (decoded, allocation) = crate::kernel::read_generation_root_with_accounting(
            fs.objects(),
            id.object_id(),
            workspace.volume.config(),
            &mut accounting,
            &cancellation,
        )
        .await?;
        let bytes = (decoded.parents.capacity() * std::mem::size_of::<GenerationId>()) as u64;
        Owned::new(decoded, allocation, bytes).retire(&mut accounting)?;
        assert_eq!(decoding.live_allocation_bytes(), 0);
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let ancestor = fs
            .workspace_common_ancestor_with_accounting(
                &source,
                &target,
                None,
                &mut KernelAccounting::native(&original, &cancellation),
                &cancellation,
            )
            .await?;
        assert_eq!(ancestor.id, leaves[0]);
        assert_eq!(ancestor.volume_id, root.volume_id);
        copied.push(
            original
                .receipt()
                .bytes_copied
                .checked_sub(decoding.receipt().bytes_copied)
                .ok_or("lineage decoding work exceeded the complete traversal")?,
        );
        assert_eq!(original.live_allocation_bytes(), baseline);
    }
    assert!(copied[1] <= 4 * copied[0], "lineage copy work: {copied:?}");
    Ok(())
}

async fn apply_planned<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>(
    plan: &JoinPlan<A, O>,
    idempotency_key: IdempotencyKey,
) -> Result<JoinOutcome<A, O>, WorkspaceError> {
    plan.apply(
        ApplyOptions {
            if_target: plan.target_head(),
            idempotency_key,
        },
        std::collections::BTreeMap::new(),
        crate::PublicationPermit::Unrestricted,
    )
    .await
}

/// Joins planned together against one target head, writing disjoint keys,
/// all apply in that one round when the authority composes independent
/// publications. Each applied join, composed or not, verifies its witness and
/// replays from its reconstructed plan as already applied. Without
/// composition (the negative control) every join after the first goes stale
/// and leaves the target unchanged.
async fn disjoint_joins_in_one_round<
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
>(
    fs: Fs<A, O>,
    composes: bool,
) -> Result<(), Box<dyn Error>> {
    let main = unbounded!(fs, create_workspace("main"))?;
    let base = unbounded!(main, head())?;
    let (mut agents, mut plans) = (Vec::new(), Vec::new());
    for index in 0..4 {
        let agent = fork_workspace(
            &main,
            &format!("agent-{index}"),
            ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
        )
        .await?;
        agent
            .write_text(&format!("/agent-{index}"), "agent")
            .await?;
        plans.push(unbounded!(agent.join_into(&main), plan())?);
        agents.push(agent);
    }
    for (index, (agent, plan)) in agents.iter().zip(&plans).enumerate() {
        let before = unbounded!(main, head())?.id();
        let key = IdempotencyKey::new();
        let outcome = apply_planned(plan, key).await?;
        if composes || index == 0 {
            let JoinOutcome::Applied(joined) = outcome else {
                return Err(format!("join {index} did not apply in its planned round").into());
            };
            let witness = value!(plan.commit_witness(
                &joined,
                key,
                &WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            ))?;
            assert!(unbounded!(main, verify_join_commit(&witness))?);
            let source = unbounded!(agent, generation(plan.source_head()))?;
            let target = unbounded!(main, generation(plan.target_head()))?;
            let replay = unbounded!(
                agent.join_into(&main),
                plan_for_replay_at(&source, &target, key)
            )?;
            assert!(matches!(
                apply_planned(&replay, key).await?,
                JoinOutcome::AlreadyApplied(_)
            ));
        } else {
            assert!(matches!(outcome, JoinOutcome::StaleTarget(_)));
            assert_eq!(unbounded!(main, head())?.id(), before);
        }
    }
    for index in 0..4 {
        let read = unbounded!(main, read(&format!("/agent-{index}"), 16));
        if composes || index == 0 {
            assert_eq!(read?, OwnedBytes::from_static(b"agent"));
        } else {
            assert!(read.is_err());
        }
    }
    Ok(())
}

#[tokio::test]
async fn disjoint_joins_apply_in_one_round_over_a_moved_target() -> Result<(), Box<dyn Error>> {
    disjoint_joins_in_one_round(Fs::memory(), true).await
}

#[tokio::test]
async fn negative_control_exact_head_joins_go_stale_over_disjoint_moves()
-> Result<(), Box<dyn Error>> {
    disjoint_joins_in_one_round(
        Fs::new(
            crate::MemoryAuthorityStore::default(),
            crate::MemoryObjectStore::default(),
            crate::EmbeddedCapabilities::MEMORY,
        ),
        false,
    )
    .await
}

/// The write rule covers the namespace, not only file contents: a join that
/// adds a file under a directory the target removed after planning goes stale
/// although the two never write one path, and the target keeps its removal.
#[tokio::test]
async fn a_join_goes_stale_when_the_target_removes_its_parent_directory()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = unbounded!(fs, create_workspace("main"))?;
    let mut seed = unbounded!(main, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(seed, create_dir_all("/dir"))?;
    assert!(matches!(
        commit(&mut seed).await?,
        TransactionCommit::Committed(_)
    ));
    let base = unbounded!(main, head())?;
    let agent = fork_workspace(
        &main,
        "agent",
        ForkOptions::from_generation(base, IdempotencyKey::new()),
    )
    .await?;
    agent.write_text("/dir/agent", "agent").await?;
    let plan = unbounded!(agent.join_into(&main), plan())?;
    main.remove("/dir").await?;
    let removed = unbounded!(main, head())?.id();
    assert!(matches!(
        apply_planned(&plan, IdempotencyKey::new()).await?,
        JoinOutcome::StaleTarget(_)
    ));
    assert_eq!(unbounded!(main, head())?.id(), removed);
    assert!(unbounded!(main, read("/dir/agent", 16)).is_err());
    Ok(())
}

/// A target change to a key the join writes leaves the planned join stale
/// without mutation, exactly once: the replanned join then applies.
#[tokio::test]
async fn a_join_goes_stale_once_when_the_target_changes_its_keys() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = unbounded!(fs, create_workspace("main"))?;
    let base = unbounded!(main, head())?;
    let agent = fork_workspace(
        &main,
        "agent",
        ForkOptions::from_generation(base, IdempotencyKey::new()),
    )
    .await?;
    agent.write_text("/shared", "same").await?;
    agent.write_text("/agent", "agent").await?;
    let plan = unbounded!(agent.join_into(&main), plan())?;
    main.write_text("/shared", "same").await?;
    let raced = unbounded!(main, head())?.id();
    assert!(matches!(
        apply_planned(&plan, IdempotencyKey::new()).await?,
        JoinOutcome::StaleTarget(_)
    ));
    assert_eq!(unbounded!(main, head())?.id(), raced);
    assert!(unbounded!(main, read("/agent", 16)).is_err());
    let replanned = unbounded!(agent.join_into(&main), plan())?;
    assert!(matches!(
        apply_planned(&replanned, IdempotencyKey::new()).await?,
        JoinOutcome::Applied(_)
    ));
    assert_eq!(
        unbounded!(main, read("/agent", 16))?,
        OwnedBytes::from_static(b"agent")
    );
    assert_eq!(
        unbounded!(main, read("/shared", 16))?,
        OwnedBytes::from_static(b"same")
    );
    Ok(())
}
