use acyclic_fs::{
    ApplyOptions, ForkOptions, Fs, IdempotencyKey, JoinOutcome, PublicationPermit,
    TransactionCommit, WorkspaceError,
};
use acyclic_stream::OwnedBytes;
use std::error::Error;

use crate::workspace_merge::fixtures::fork_workspace;
use crate::workspace_merge::transaction::commit;

#[tokio::test]
async fn semantic_change_set_and_overlap_conflict_are_exact() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = unbounded!(fs, create_workspace("main"))?;
    main.write_text("/shared", "base").await?;
    let base = unbounded!(main, head())?;
    let agent = fork_workspace(
        &main,
        "agent",
        ForkOptions::from_generation(base, IdempotencyKey::new()),
    )
    .await?;
    let agent_base = unbounded!(agent, head())?;
    agent.write_text("/shared", "agent").await?;
    let agent_head = unbounded!(agent, head())?;
    let changes = unbounded!(agent, diff(&agent_base, &agent_head, 32))?;
    assert_eq!(changes.from().id(), agent_base.id());
    assert_eq!(changes.to().id(), agent_head.id());
    assert!(!changes.changes().files.is_empty());

    main.write_text("/shared", "main").await?;
    let plan = unbounded!(agent.join_into(&main), plan())?;
    assert!(matches!(
        plan.apply(
            ApplyOptions {
                if_target: plan.target_head(),
                idempotency_key: IdempotencyKey::new(),
            },
            std::collections::BTreeMap::new(),
            PublicationPermit::Unrestricted
        )
        .await?,
        JoinOutcome::Conflicted {
            truncated: false,
            ..
        }
    ));
    assert_eq!(
        unbounded!(main, read("/shared", 16))?,
        OwnedBytes::from_static(b"main")
    );
    Ok(())
}

#[tokio::test]
async fn contiguous_change_sets_compose_by_outer_semantics() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("compose"))?;
    let base = unbounded!(workspace, head())?;
    workspace.write_text("/before", "body").await?;
    let middle = unbounded!(workspace, head())?;
    let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(transaction, rename("/before", "/after", true))?;
    let TransactionCommit::Committed(end) = commit(&mut transaction).await? else {
        return Err("rename did not commit".into());
    };

    let first = unbounded!(workspace, diff(&base, &middle, 32))?;
    let second = unbounded!(workspace, diff(&middle, &end, 32))?;
    let composed = unbounded!(first, compose(&second, Some(32)))?;
    let direct = unbounded!(workspace, diff(&base, &end, 32))?;
    assert_eq!(composed.from().id(), base.id());
    assert_eq!(composed.to().id(), end.id());
    assert_eq!(composed.changes(), direct.changes());
    assert!(matches!(
        unbounded!(second, compose(&first, Some(32))),
        Err(WorkspaceError::ChangeSetContinuity)
    ));
    Ok(())
}
