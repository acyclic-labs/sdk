use super::*;
use crate::content::tree::tests::{Disk, reachable};
use crate::{CommitEffect, CommitRequest, StreamPath};

fn request(key: &str, value: &'static [u8]) -> Result<WriteRequest, StreamError> {
    Ok(WriteRequest::Commit(CommitRequest {
        deadline_unix_millis: None,
        idempotency_key: IdempotencyKey::new(key.to_owned())?,
        effects: vec![CommitEffect {
            soft: false,
            conditions: vec![],
            mutations: vec![CommitMutation::Append {
                path: StreamPath::new("bound/path")?,
                records: vec![OwnedBytes::from_static(value).into()],
            }],
        }],
    }))
}

#[tokio::test]
async fn persisted_stage_seal_is_bound_to_one_identity_and_retains_its_graph()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let storage = Disk::open(directory.path());
    let commit = CommitId::from_bytes([4; 32]);
    let retry = IdempotencyKey::new("retry")?;
    let (open, binding) = Registry::default()
        .begin(&storage, commit, retry.clone(), [7; 32])
        .await?;
    let (same, repeated) = open.begin(&storage, commit, retry.clone(), [7; 32]).await?;
    assert_eq!(same, open);
    assert_eq!(repeated, binding);
    for (other_commit, other_retry, owner) in [
        (CommitId::from_bytes([5; 32]), retry.clone(), [7; 32]),
        (commit, IdempotencyKey::new("other")?, [7; 32]),
        (commit, retry, [8; 32]),
    ] {
        let (_, different) = open
            .begin(&storage, other_commit, other_retry, owner)
            .await?;
        assert_ne!(
            different.id(),
            binding.id(),
            "stage identity omitted one permanent binding"
        );
    }
    let staged = stage(&storage, commit, &request("retry", b"payload")?).await?;
    let prepared = Directory::build(&storage, &staged).await?;
    let sealed = open
        .seal(&storage, &binding, &staged, prepared.root())
        .await?;
    let WriteRequest::Commit(projected_request) = request("retry", b"payload")? else {
        return Err("expected coordinated request".into());
    };
    let projected = stage(
        &storage,
        commit,
        &WriteRequest::CommitAccepted(projected_request),
    )
    .await?;
    let projected_seal = sealed
        .seal(&storage, &binding, &projected, prepared.root())
        .await;
    assert!(
        matches!(&projected_seal, Ok(registry) if registry == &sealed),
        "stage projection changed retry identity: {projected_seal:?}"
    );
    assert_eq!(
        sealed
            .seal(&storage, &binding, &staged, prepared.root())
            .await?,
        sealed
    );
    let checkpoint = sealed.checkpoint(&storage).await?;
    let graph = reachable(&storage, checkpoint).await?;
    assert!(
        graph.contains(&staged.body().part.digest),
        "stage binding omitted its token-body edge"
    );
    assert!(
        graph.contains(&prepared.root().part.digest),
        "stage binding omitted its directory edge"
    );
    assert!(
        open.get(&storage, binding.id())
            .await?
            .ok_or("lost open binding")?
            .sealed()
            .is_none()
    );
    for (other_commit, other_request) in [
        (CommitId::from_bytes([5; 32]), request("retry", b"payload")?),
        (commit, request("other", b"payload")?),
    ] {
        let other = stage(&storage, other_commit, &other_request).await?;
        let prepared = Directory::build(&storage, &other).await?;
        assert!(
            matches!(
                open.seal(&storage, &binding, &other, prepared.root()).await,
                Err(StreamError::InvalidArgument)
            ),
            "stage accepted another commit or retry identity"
        );
    }
    let replacement = stage(&storage, commit, &request("retry", b"different payload")?).await?;
    let replacement_directory = Directory::build(&storage, &replacement).await?;
    assert!(
        matches!(
            sealed
                .seal(
                    &storage,
                    &binding,
                    &replacement,
                    replacement_directory.root()
                )
                .await,
            Err(StreamError::InvalidArgument)
        ),
        "sealed stage accepted a replacement body"
    );
    assert!(
        matches!(
            open.seal(&storage, &binding, &staged, replacement_directory.root())
                .await,
            Err(StreamError::InvalidArgument)
        ),
        "stage accepted another directory's body"
    );
    drop(storage);
    let storage = Disk::open(directory.path());
    let reopened = Registry::open(&storage, checkpoint).await?;
    let found = reopened
        .get(&storage, binding.id())
        .await?
        .ok_or("lost sealed binding")?;
    let (request, root) = found.sealed().ok_or("reopened stage lost its seal")?;
    assert_eq!(request, &staged);
    assert_eq!(root, prepared.root());
    assert_eq!(
        reopened.seal(&storage, &binding, &staged, root).await?,
        reopened
    );
    Ok(())
}
