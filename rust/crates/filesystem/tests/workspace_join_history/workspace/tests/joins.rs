use acyclic_fs::{ForkOptions, Fs, IdempotencyKey, JoinOutcome, TransactionCommit, WorkspaceError};
use std::error::Error;

use crate::fixtures::{commit, fork_workspace};
use crate::join_plan::apply_planned;

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
    assert_eq!(unbounded!(main, read("/agent", 16))?.as_ref(), b"agent");
    assert_eq!(unbounded!(main, read("/shared", 16))?.as_ref(), b"same");
    Ok(())
}
