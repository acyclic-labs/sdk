use super::*;
use crate::{CommitCondition, CommitEffect, CommitMutation};
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// One conditioned commit forking `source`, at tail 1, to `destination`: the
/// shape of a filesystem fork.
fn fork_into(
    source: &StreamPath,
    destination: &StreamPath,
    key: &'static [u8],
) -> std::result::Result<CommitRequest, StreamError> {
    Ok(CommitRequest {
        deadline_unix_millis: None,
        effects: vec![CommitEffect::hard(
            vec![
                CommitCondition::Tail {
                    path: source.clone(),
                    expected: 1,
                },
                CommitCondition::Absent {
                    path: destination.clone(),
                },
            ],
            vec![CommitMutation::Fork {
                exclude: Vec::new(),
                source: source.clone(),
                destination: destination.clone(),
                at_tail: 1,
                records: Vec::new(),
            }],
        )],
        idempotency_key: IdempotencyKey::new(Bytes::from_static(key))?,
    })
}

/// `request` also creating `path` with one record: an adopted path.
fn adopting(mut request: CommitRequest, path: &StreamPath) -> Result<CommitRequest> {
    request
        .effects
        .first_mut()
        .ok_or("no effect")?
        .mutations
        .push(CommitMutation::Append {
            path: path.clone(),
            records: vec![Bytes::from_static(b"adopted").into()],
        });
    Ok(request)
}

fn append(path: &StreamPath, tail: u64) -> std::result::Result<AppendRequest, StreamError> {
    Ok(AppendRequest {
        path: path.clone(),
        records: vec![Bytes::copy_from_slice(&tail.to_le_bytes()).into()],
        if_tail: Some(tail),
        idempotency_key: IdempotencyKey::unique(),
    })
}

fn frames(provider: &LocalStream) -> Result<(u64, u64)> {
    let journal = provider
        .inner
        .journal
        .lock()
        .map_err(|_| "journal poisoned")?;
    Ok((journal.operations, journal.bytes))
}

/// An ephemeral fork, the path it adopts and their appends write no journal
/// frame. A compaction while the lease is held snapshots none of them, and a
/// reopen without any release finds nothing, while a durable fork and the
/// adopted path's durable sibling survive both.
#[tokio::test]
async fn an_unreleased_ephemeral_fork_never_outlives_its_incarnation() -> Result {
    let directory = tempfile::tempdir()?;
    let limits = LocalStreamLimits {
        compaction_operations: 8,
        ..LocalStreamLimits::default()
    };
    let provider = LocalStream::open(directory.path(), limits).await?;
    let source = StreamPath::new("agents/a")?;
    let root = StreamPath::new("scratch")?;
    let ephemeral = StreamPath::new("scratch/a")?;
    let durable = StreamPath::new("kept/a")?;
    let sibling = StreamPath::new("changes/base")?;
    let adopted = StreamPath::new("changes/scratch")?;
    provider.append(append(&source, 0)?).await?;
    provider.append(append(&sibling, 0)?).await?;
    let lease = provider.lease_ephemeral(root.clone()).await?;
    let before = frames(&provider)?;
    let CommitOutcome::Committed(_) = provider
        .commit(adopting(
            fork_into(&source, &ephemeral, b"ephemeral-fork")?,
            &adopted,
        )?)
        .await?
    else {
        return Err("ephemeral fork conflicted".into());
    };
    provider.append(append(&ephemeral, 1)?).await?;
    provider.append(append(&adopted, 1)?).await?;
    assert_eq!(frames(&provider)?, before, "no ephemeral frame");
    assert_eq!(provider.tail(ephemeral.clone()).await?, 2);
    assert_eq!(provider.tail(adopted.clone()).await?, 2);
    // Negative control: the same fork to a durable destination is journalled.
    provider
        .commit(fork_into(&source, &durable, b"durable-fork")?)
        .await?;
    assert_eq!(frames(&provider)?.0, before.0 + 1, "one durable frame");
    // Compact while the lease is held.
    for tail in 1..limits.compaction_operations {
        provider.append(append(&source, tail)?).await?;
    }
    assert_eq!(provider.tail(ephemeral.clone()).await?, 2);
    drop(provider);
    let snapshot = read_snapshot(directory.path(), limits)?.ok_or("snapshot missing")?;
    let restored = MemoryStream::default();
    restored.install_state(&snapshot.state).await?;
    for gone in [&root, &adopted] {
        assert_eq!(
            restored.tail(gone.clone()).await,
            Err(StreamError::NotFound)
        );
    }
    assert_eq!(restored.tail(durable.clone()).await?, 1);
    assert_eq!(restored.tail(sibling.clone()).await?, 1);

    let reopened = LocalStream::open(directory.path(), limits).await?;
    for gone in [&root, &ephemeral, &adopted] {
        assert_eq!(
            reopened.tail(gone.clone()).await,
            Err(StreamError::NotFound)
        );
    }
    assert_eq!(reopened.tail(durable.clone()).await?, 1);
    assert_eq!(reopened.tail(sibling).await?, 1);
    let page = reopened
        .children_page(crate::ChildrenPageRequest {
            tail_stream: None,
            parent: None,
            after: None,
            hierarchy_version: None,
            limit: 16,
        })
        .await?;
    assert_eq!(
        page.children
            .iter()
            .map(|child| child.path.as_str())
            .collect::<Vec<_>>(),
        ["agents", "changes", "kept"]
    );
    // Releasing into the closed incarnation touches nothing reopened, and the
    // root is free to lease again.
    lease.settle().await?;
    assert_eq!(reopened.tail(durable).await?, 1);
    reopened.lease_ephemeral(root).await?.settle().await?;
    Ok(())
}

/// Dropping a lease removes its root subtree and every adopted path at once:
/// followers waiting in either are woken with `NotFound`, and nothing else
/// moves.
#[tokio::test]
async fn dropping_a_lease_removes_its_subtree_and_wakes_its_followers() -> Result {
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    let source = StreamPath::new("agents/a")?;
    let ephemeral = StreamPath::new("scratch/agents/a")?;
    let sibling = StreamPath::new("changes/base")?;
    let adopted = StreamPath::new("changes/scratch")?;
    provider.append(append(&source, 0)?).await?;
    provider.append(append(&sibling, 0)?).await?;
    let lease = provider
        .lease_ephemeral(StreamPath::new("scratch")?)
        .await?;
    provider
        .commit(adopting(
            fork_into(&source, &ephemeral, b"ephemeral-fork")?,
            &adopted,
        )?)
        .await?;
    let mut follows = Vec::new();
    for path in [&ephemeral, &adopted] {
        follows.push(crate::records_only(
            provider
                .follow(crate::FollowRequest::new(path.clone(), 1))
                .await?,
        ));
    }
    for follow in &mut follows {
        assert!(
            futures::poll!(follow.next()).is_pending(),
            "waits at the tail"
        );
    }
    drop(lease);
    for follow in &mut follows {
        assert!(matches!(
            follow.next().await,
            Some(Err(StreamError::NotFound))
        ));
    }
    for gone in [&ephemeral, &adopted] {
        assert_eq!(
            provider.tail(gone.clone()).await,
            Err(StreamError::NotFound)
        );
    }
    assert_eq!(provider.tail(sibling).await?, 1);
    assert_eq!(
        provider.tail(StreamPath::new("scratch")?).await,
        Err(StreamError::NotFound)
    );
    assert_eq!(provider.tail(source).await?, 1);
    Ok(())
}
