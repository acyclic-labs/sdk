use acyclic_fs::WorkspaceId;
use acyclic_fs::git_compat::{
    GitCommand, GitCommandOutput, GitCommit, GitCompatError, GitCompatRepository,
    GitCompatRunError, GitCompatState, GitCompatStore, GitFilesystemResult, GitTreeRef,
    MemoryGitCompatStore,
};
use std::collections::BTreeSet;

use crate::fixtures::{TestExecutor, generation, tree, workspace};

#[tokio::test]
async fn foreign_workspace_tree_is_rejected_before_executor_side_effects()
-> Result<(), Box<dyn std::error::Error>> {
    let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::default());
    let foreign_workspace = WorkspaceId::from_bytes([8; 16]);
    let executor = TestExecutor::returning(GitFilesystemResult::Applied {
        tree: None,
        tracked_paths: None,
    });

    let Err(error) = repository
        .run(
            GitCommand::Status,
            GitTreeRef::exact(foreign_workspace, generation(1)),
            &executor,
        )
        .await
    else {
        return Err("foreign tree unexpectedly executed".into());
    };

    assert!(matches!(
        error,
        GitCompatRunError::Compat(GitCompatError::WorkspaceMismatch)
    ));
    assert!(
        executor
            .operations
            .lock()
            .map_err(|_| "operation lock poisoned")?
            .is_empty(),
        "foreign trees must never reach the filesystem executor"
    );
    Ok(())
}

#[tokio::test]
async fn foreign_completion_tree_is_rejected_without_consuming_the_transition()
-> Result<(), Box<dyn std::error::Error>> {
    let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::default());
    let GitCommandOutput::Prepared { transition, .. } = repository
        .execute(
            GitCommand::Commit {
                message: "initial".to_owned(),
                author: "agent".to_owned(),
                authored_at_seconds: 10,
            },
            tree(1),
        )
        .await?
    else {
        return Err("expected prepared commit".into());
    };
    let foreign = WorkspaceId::from_bytes([8; 16]);
    assert!(matches!(
        repository
            .complete_transition_result(
                transition,
                &GitFilesystemResult::Captured {
                    tree: GitTreeRef::exact(foreign, generation(1)),
                    tracked_paths: BTreeSet::new(),
                    proof: None,
                },
            )
            .await,
        Err(GitCompatError::WorkspaceMismatch)
    ));
    assert_eq!(
        repository
            .pending_transition()
            .await?
            .map(|pending| pending.id),
        Some(transition)
    );
    Ok(())
}

#[tokio::test]
async fn persisted_commit_hashes_are_validated_before_use() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryGitCompatStore::default();
    let mut state = GitCompatState::new("main", workspace());
    let mut commit = GitCommit::new(
        tree(1),
        tree(1),
        None,
        BTreeSet::new(),
        Vec::new(),
        "agent",
        10,
        "initial",
    );
    commit.message = "forged".to_owned();
    state.revision = 1;
    state.commits.insert(commit.id, commit.clone());
    state
        .branches
        .get_mut("main")
        .ok_or("missing main branch")?
        .head = Some(commit.id);
    assert!(store.compare_and_swap(workspace(), 0, state).await?);
    let repository = GitCompatRepository::new(workspace(), store);
    assert!(matches!(
        repository.execute(GitCommand::Status, tree(1)).await,
        Err(GitCompatError::InvalidState)
    ));
    Ok(())
}
