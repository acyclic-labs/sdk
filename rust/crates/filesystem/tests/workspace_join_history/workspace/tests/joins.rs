use acyclic_fs::{
    ApplyOptions, AsyncAuthorityStore, AsyncObjectStore, CancellationToken, EmbeddedCapabilities,
    ForkOptions, Fs, IdempotencyKey, JoinOutcome, JoinPlan, MemoryAuthorityStore,
    MemoryObjectStore, PublicationPermit, TransactionCommit, WorkspaceError,
};
use acyclic_fs_work::performance::WorkBudget;
use acyclic_stream::OwnedBytes;
use std::error::Error;

use crate::fixtures::{commit, fork_workspace};

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
        apply_planned(&first, IdempotencyKey::from_bytes([0x68; 16])).await?,
        JoinOutcome::Applied(_)
    ));
    parent.write_text("/first.txt", "resolved").await?;
    child.write_text("/second.txt", "second").await?;
    let second = unbounded!(child.join_into(&parent), plan())?;
    assert!(matches!(
        apply_planned(&second, IdempotencyKey::from_bytes([0x69; 16])).await?,
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
        apply_planned(&repeated, IdempotencyKey::from_bytes([0x6a; 16])).await?,
        JoinOutcome::NoChanges(_)
    ));
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
        PublicationPermit::Unrestricted,
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
            MemoryAuthorityStore::default(),
            MemoryObjectStore::default(),
            EmbeddedCapabilities::MEMORY,
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
