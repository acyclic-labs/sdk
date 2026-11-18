use acyclic_fs::{
    GitCompatState, GitCompatStore, StreamAuthorityStore, StreamGitCompatStore, WorkspaceId,
};
use acyclic_stream::MemoryStream;
use std::sync::Arc;

fn state(id: WorkspaceId, revision: u64, branch: &str) -> GitCompatState {
    let mut state = GitCompatState::new(branch, id);
    state.revision = revision;
    state
}

#[tokio::test]
async fn independent_hosts_share_conditional_state_without_crossing_repositories()
-> Result<(), Box<dyn std::error::Error>> {
    let provider = Arc::new(MemoryStream::default());
    let first = StreamGitCompatStore::new(StreamAuthorityStore::new(provider.clone()));
    let second = StreamGitCompatStore::new(StreamAuthorityStore::new(provider.clone()));
    let id = WorkspaceId::from_bytes([1; 16]);
    let foreign = WorkspaceId::from_bytes([2; 16]);
    assert!(GitCompatStore::load(&first, id).await?.is_none());
    let initial = state(id, 1, "main");
    assert!(GitCompatStore::compare_and_swap(&first, id, 0, initial.clone()).await?);
    assert_eq!(GitCompatStore::load(&second, id).await?, Some(initial));
    assert!(GitCompatStore::load(&second, foreign).await?.is_none());

    let (left, right) = tokio::join!(
        GitCompatStore::compare_and_swap(&first, id, 1, state(id, 2, "left")),
        GitCompatStore::compare_and_swap(&second, id, 1, state(id, 2, "right")),
    );
    assert_ne!(left?, right?);
    let reopened = StreamGitCompatStore::new(StreamAuthorityStore::new(provider));
    let committed = GitCompatStore::load(&reopened, id)
        .await?
        .ok_or("missing committed state")?;
    assert_eq!(committed.revision, 2);
    assert!(["left", "right"].contains(&committed.current_branch.as_str()));
    assert!(!GitCompatStore::compare_and_delete(&second, id, 1).await?);
    assert_eq!(GitCompatStore::load(&first, id).await?, Some(committed));
    assert!(GitCompatStore::compare_and_delete(&second, id, 2).await?);
    assert!(GitCompatStore::load(&reopened, id).await?.is_none());
    assert!(!GitCompatStore::compare_and_swap(&first, id, 2, state(id, 3, "stale")).await?);
    assert!(
        GitCompatStore::compare_and_swap(&first, foreign, 0, state(foreign, 1, "foreign")).await?
    );
    assert!(GitCompatStore::load(&reopened, id).await?.is_none());
    Ok(())
}
