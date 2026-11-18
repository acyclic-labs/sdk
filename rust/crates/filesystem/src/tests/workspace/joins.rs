use super::*;

#[path = "../../../tests/support/join_plan.rs"]
mod join_plan;
use join_plan::apply_planned;

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
        let decoding_baseline = decoding.live_allocation_bytes().unwrap();
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
        assert_eq!(decoding.live_allocation_bytes().unwrap(), decoding_baseline);
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes().unwrap();
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
                .unwrap()
                .bytes_copied
                .checked_sub(decoding.receipt().unwrap().bytes_copied)
                .ok_or("lineage decoding work exceeded the complete traversal")?,
        );
        assert_eq!(original.live_allocation_bytes().unwrap(), baseline);
    }
    assert!(copied[1] <= 4 * copied[0], "lineage copy work: {copied:?}");
    Ok(())
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
            let witness = plan
                .commit_witness(
                    &joined,
                    key,
                    &WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .map(|receipt| receipt.value)
                .map_err(|failure| failure.error)?;
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
