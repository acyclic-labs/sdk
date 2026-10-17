//! Reusable black-box qualification of the complete public Stream contract.

use crate::OwnedBytes as Bytes;
use futures::{StreamExt as _, TryStreamExt as _};

use crate::{
    AppendOutcome, AppendRequest, ChildrenPageRequest, CommitCondition, CommitConflict,
    CommitMutation, CommitOutcome, CommitRequest, CommittedMutation, ForkRequest, IdempotencyKey,
    IdempotencyOutcome, ReadRequest, StreamError, StreamPath, StreamProvider,
};

/// Canonical language-neutral Stream conformance inventory.
pub const SUITE: &[u8] = include_bytes!("../conformance/stream.json");
const _: () = assert!(!SUITE.is_empty(), "conformance inventory is empty");

/// Exercises the complete provider-independent hierarchical Stream contract.
#[allow(
    clippy::too_many_lines,
    clippy::cognitive_complexity,
    reason = "one public contract walkthrough"
)]
pub async fn verify(provider: &dyn StreamProvider) -> Result<(), StreamError> {
    let source = path("conformance/source")?;
    let child = path("conformance/child")?;
    let append_key = key(b"stream-append")?;
    if provider
        .inspect_idempotency(append_key.clone())
        .await?
        .is_some()
    {
        return Err(StreamError::Unavailable);
    }
    let initial = AppendRequest {
        path: source.clone(),
        records: vec![Bytes::from_static(b"one"), Bytes::from_static(b"two")],
        if_tail: Some(0),
        idempotency_key: Some(append_key.clone()),
    };
    let first = provider.append(initial.clone()).await?;
    let AppendOutcome::Committed(first_receipt) = &first else {
        return Err(StreamError::Unavailable);
    };
    if first_receipt.start != 0
        || first_receipt.end != 2
        || first_receipt.tail != 2
        || provider.append(initial).await? != first
    {
        return Err(StreamError::Unavailable);
    }
    if provider
        .append(AppendRequest {
            path: source.clone(),
            records: vec![Bytes::from_static(b"different")],
            if_tail: Some(0),
            idempotency_key: Some(append_key.clone()),
        })
        .await
        != Err(StreamError::IdempotencyMismatch)
    {
        return Err(StreamError::Unavailable);
    }
    let append_observation = provider
        .inspect_idempotency(append_key.clone())
        .await?
        .ok_or(StreamError::Unavailable)?;
    if append_observation.idempotency_key != append_key
        || append_observation.request_digest == [0; 32]
        || append_observation.outcome != IdempotencyOutcome::Append(first.clone())
    {
        return Err(StreamError::Unavailable);
    }
    if provider
        .append(AppendRequest {
            path: source.clone(),
            records: vec![Bytes::from_static(b"never")],
            if_tail: Some(1),
            idempotency_key: None,
        })
        .await?
        != (AppendOutcome::TailConflict { actual_tail: 2 })
    {
        return Err(StreamError::Unavailable);
    }
    let fork = provider
        .fork(ForkRequest {
            source: source.clone(),
            destination: child.clone(),
            at_tail: Some(1),
            idempotency_key: Some(key(b"stream-fork")?),
        })
        .await?;
    if fork.forked_at != 1 || fork.tail != 1 {
        return Err(StreamError::Unavailable);
    }
    let inherited = provider
        .read(ReadRequest {
            path: child.clone(),
            from: 0,
            limit: 8,
        })
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    let [item] = inherited.as_slice() else {
        return Err(StreamError::Unavailable);
    };
    if item.value != Bytes::from_static(b"one") {
        return Err(StreamError::Unavailable);
    }
    let children = provider
        .children_page(ChildrenPageRequest {
            parent: Some(path("conformance")?),
            limit: 8,
            after: None,
            hierarchy_version: None,
        })
        .await?
        .children;
    let mut child_paths = children
        .into_iter()
        .map(|child| child.path)
        .collect::<Vec<_>>();
    child_paths.sort();
    if child_paths != vec![path("conformance/child")?, path("conformance/source")?] {
        return Err(StreamError::Unavailable);
    }
    let first_page = provider
        .children_page(ChildrenPageRequest {
            parent: Some(path("conformance")?),
            after: None,
            hierarchy_version: None,
            limit: 1,
        })
        .await?;
    let [first_child] = first_page.children.as_slice() else {
        return Err(StreamError::Unavailable);
    };
    if first_page.next_after.as_ref() != Some(&first_child.path) {
        return Err(StreamError::Unavailable);
    }
    let final_page = provider
        .children_page(ChildrenPageRequest {
            parent: Some(path("conformance")?),
            after: first_page.next_after.clone(),
            hierarchy_version: Some(first_page.hierarchy_version),
            limit: 1,
        })
        .await?;
    let [final_child] = final_page.children.as_slice() else {
        return Err(StreamError::Unavailable);
    };
    if final_page.next_after.is_some() || final_child.path == first_child.path {
        return Err(StreamError::Unavailable);
    }
    provider
        .append(AppendRequest {
            path: path("conformance/paging-new")?,
            records: vec![Bytes::from_static(b"created")],
            if_tail: Some(0),
            idempotency_key: None,
        })
        .await?;
    if provider
        .children_page(ChildrenPageRequest {
            parent: Some(path("conformance")?),
            after: first_page.next_after,
            hierarchy_version: Some(first_page.hierarchy_version),
            limit: 1,
        })
        .await
        != Err(StreamError::HierarchyChanged)
    {
        return Err(StreamError::Unavailable);
    }
    let mut follow = provider.follow(child.clone(), 1).await?;
    provider
        .append(AppendRequest {
            path: child.clone(),
            records: vec![Bytes::from_static(b"live")],
            if_tail: Some(1),
            idempotency_key: None,
        })
        .await?;
    let live = tokio::time::timeout(std::time::Duration::from_secs(1), follow.next())
        .await
        .map_err(|_| StreamError::Unavailable)?
        .ok_or(StreamError::Unavailable)??;
    if live.sequence != 1 || live.value != Bytes::from_static(b"live") {
        return Err(StreamError::Unavailable);
    }
    let complete_history = provider
        .read(ReadRequest {
            path: child.clone(),
            from: 0,
            limit: 8,
        })
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    if !matches!(complete_history.as_slice(), [first, second]
        if first.value == Bytes::from_static(b"one")
            && second.value == Bytes::from_static(b"live"))
        || provider.bounds(child.clone()).await?.tail != 2
    {
        return Err(StreamError::Unavailable);
    }
    let committed_path = path("conformance/committed")?;
    let request = CommitRequest {
        conditions: vec![
            CommitCondition::Tail {
                path: source.clone(),
                expected: 2,
            },
            CommitCondition::Absent {
                path: committed_path.clone(),
            },
        ],
        mutations: vec![CommitMutation::Fork {
            source: source.clone(),
            destination: committed_path,
            at_tail: 2,
            records: Vec::new(),
        }],
        idempotency_key: key(b"stream-commit")?,
    };
    let committed = provider.commit(request.clone()).await?;
    let CommitOutcome::Committed(envelope) = &committed else {
        return Err(StreamError::Unavailable);
    };
    if provider.commit(request).await? != committed
        || provider.read_commit(envelope.commit_id).await? != *envelope
    {
        return Err(StreamError::Unavailable);
    }
    verify_stale_tail_condition(provider, source.clone()).await?;
    verify_fork_with_records(provider, source.clone()).await?;
    verify_read_open_failures(provider, source.clone()).await?;
    let absent = path("conformance/absent-tail")?;
    let absent_key = key(b"stream-absent-tail")?;
    let absent_request = CommitRequest {
        conditions: vec![CommitCondition::Tail {
            path: absent.clone(),
            expected: 0,
        }],
        mutations: vec![CommitMutation::Append {
            path: absent.clone(),
            records: vec![Bytes::from_static(b"must-not-commit")],
        }],
        idempotency_key: absent_key.clone(),
    };
    let absent_conflict = CommitOutcome::Conflict(vec![CommitConflict::Tail {
        path: absent.clone(),
        expected: 0,
        actual: None,
    }]);
    if provider.commit(absent_request.clone()).await? != absent_conflict
        || provider.commit(absent_request).await? != absent_conflict
        || provider.tail(absent).await != Err(StreamError::NotFound)
    {
        return Err(StreamError::Unavailable);
    }
    let absent_observation = provider
        .inspect_idempotency(absent_key.clone())
        .await?
        .ok_or(StreamError::Unavailable)?;
    if absent_observation.idempotency_key != absent_key
        || absent_observation.request_digest == [0; 32]
        || absent_observation.outcome != IdempotencyOutcome::Commit(absent_conflict)
    {
        return Err(StreamError::Unavailable);
    }
    Ok(())
}

/// A finite read reports an absent path or a start beyond the tail when it
/// opens, never as a deferred first item: callers treat `NotFound` from the
/// open as an empty history.
async fn verify_read_open_failures(
    provider: &dyn StreamProvider,
    source: StreamPath,
) -> Result<(), StreamError> {
    let open = |path: StreamPath, from: u64| async move {
        provider
            .read(ReadRequest {
                path,
                from,
                limit: 1,
            })
            .await
            .err()
    };
    let beyond = provider.tail(source.clone()).await? + 1;
    if open(path("conformance/never-written")?, 0).await != Some(StreamError::NotFound)
        || open(source, beyond).await != Some(StreamError::OutOfRange)
    {
        return Err(StreamError::Unavailable);
    }
    Ok(())
}

async fn verify_stale_tail_condition(
    provider: &dyn StreamProvider,
    source: StreamPath,
) -> Result<(), StreamError> {
    let stale_path = path("conformance/stale-commit")?;
    let stale_conflict = CommitOutcome::Conflict(vec![CommitConflict::Tail {
        path: source.clone(),
        expected: 1,
        actual: Some(2),
    }]);
    if provider
        .commit(CommitRequest {
            conditions: vec![
                CommitCondition::Tail {
                    path: source.clone(),
                    expected: 1,
                },
                CommitCondition::Absent {
                    path: stale_path.clone(),
                },
            ],
            mutations: vec![CommitMutation::Fork {
                source: source.clone(),
                destination: stale_path.clone(),
                at_tail: 2,
                records: Vec::new(),
            }],
            idempotency_key: key(b"stream-stale-commit")?,
        })
        .await?
        != stale_conflict
        || provider.tail(stale_path).await != Err(StreamError::NotFound)
        || provider.tail(source).await? != 2
    {
        return Err(StreamError::Unavailable);
    }
    Ok(())
}

/// same commit, and its committed fact carries them.
async fn verify_fork_with_records(
    provider: &dyn StreamProvider,
    source: StreamPath,
) -> Result<(), StreamError> {
    let destination = path("conformance/forked-and-extended")?;
    let request = CommitRequest {
        conditions: vec![
            CommitCondition::Tail {
                path: source.clone(),
                expected: 2,
            },
            CommitCondition::Absent {
                path: destination.clone(),
            },
        ],
        mutations: vec![CommitMutation::Fork {
            source,
            destination: destination.clone(),
            at_tail: 1,
            records: vec![
                Bytes::from_static(b"extended"),
                Bytes::from_static(b"again"),
            ],
        }],
        idempotency_key: key(b"stream-fork-with-records")?,
    };
    let committed = provider.commit(request.clone()).await?;
    let CommitOutcome::Committed(envelope) = &committed else {
        return Err(StreamError::Unavailable);
    };
    let [CommittedMutation::Fork(fork)] = envelope.mutations.as_slice() else {
        return Err(StreamError::Unavailable);
    };
    let appended = fork
        .records
        .iter()
        .map(|record| (record.sequence, record.value.clone(), record.commit_id))
        .collect::<Vec<_>>();
    if fork.forked_at != 1
        || fork.tail != 3
        || appended
            != [
                (1, Bytes::from_static(b"extended"), envelope.commit_id),
                (2, Bytes::from_static(b"again"), envelope.commit_id),
            ]
    {
        return Err(StreamError::Unavailable);
    }
    let values = provider
        .read(ReadRequest {
            path: destination.clone(),
            from: 0,
            limit: 8,
        })
        .await?
        .map_ok(|record| record.value)
        .try_collect::<Vec<_>>()
        .await?;
    if values
        != [
            Bytes::from_static(b"one"),
            Bytes::from_static(b"extended"),
            Bytes::from_static(b"again"),
        ]
        || provider.tail(destination).await? != 3
        || provider.commit(request).await? != committed
    {
        return Err(StreamError::Unavailable);
    }
    Ok(())
}

fn path(value: &str) -> Result<StreamPath, StreamError> {
    StreamPath::new(value)
}

fn key(value: &'static [u8]) -> Result<IdempotencyKey, StreamError> {
    IdempotencyKey::new(Bytes::from_static(value))
}

#[cfg(test)]
mod tests {
    use super::verify;
    use crate::{MemoryLimits, MemoryStream, StreamError};

    #[tokio::test]
    async fn memory_provider_passes_the_public_suite() -> Result<(), StreamError> {
        verify(&MemoryStream::new(MemoryLimits::default())).await
    }
}
