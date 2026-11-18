use super::*;
use crate::FilesystemCommitOutcome;
use acyclic_stream::{CommitRequest, MemoryStream};
use std::sync::Arc;

#[tokio::test]
async fn malformed_record_refuses_instead_of_becoming_absent()
-> Result<(), Box<dyn std::error::Error>> {
    let authority = StreamAuthorityStore::new(Arc::new(MemoryStream::default()));
    let store = StreamGitCompatStore::new(authority.clone());
    let id = WorkspaceId::from_bytes([3; 16]);
    let cancellation = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let observed = authority
        .observe_record(
            StreamGitCompatStore::<MemoryStream>::path(id, "refs")?,
            &mut work,
        )
        .await?;
    let mut commit = FilesystemCommit::new(CommitRequest {
        idempotency_key: acyclic_stream::IdempotencyKey::new(bytes::Bytes::from_static(
            b"malformed-state",
        ))?,
        effects: vec![acyclic_stream::CommitEffect::hard(Vec::new(), Vec::new())],
        deadline_unix_millis: None,
    });
    authority.contribute_record(
        &mut commit,
        &observed.observation,
        &StateRecord::Companion {
            kind: CompanionKind::GitRefs,
            payload: bytes::Bytes::from_static(b"invalid JSON").into(),
        },
        crate::PublicationHold::none(),
        &mut work,
    )?;
    authority.seal_observed(&mut commit, &[], None, &mut work)?;
    assert!(matches!(
        authority.submit(commit, &mut work).await?,
        FilesystemCommitOutcome::Committed(_)
    ));
    assert!(matches!(
        GitCompatStore::load(&store, id).await,
        Err(StreamStateStoreError::Codec(_))
    ));
    let mut overwrite = GitCompatState::new("overwrite", id);
    overwrite.revision = 1;
    assert!(
        GitCompatStore::compare_and_swap(&store, id, 0, overwrite)
            .await
            .is_err()
    );
    Ok(())
}
