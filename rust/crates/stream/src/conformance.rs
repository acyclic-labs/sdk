//! Reusable black-box qualification of the complete public Stream contract.

mod absence;
mod atomic;
mod backend;
pub(crate) mod key_seek;
mod views;
#[path = "conformance"]
mod histories {
    pub(super) mod wave1;
}
/// Stable selected-history and receipt API. Full qualification uses [`verify`].
pub mod wave1 {
    pub use super::histories::wave1::{Case, Failure, Invariant, RECOVERY, coverage, verify_case};
}
#[cfg(all(target_arch = "wasm32", feature = "wasm"))]
pub use backend::injected;
pub use backend::{Backend, Failure, Fixture, Measurement, bench, suite, verify};

use crate::OwnedBytes as Bytes;
use crate::StreamProviderExt as _;
use futures::TryFutureExt as _;
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
async fn verify_contract<P: StreamProvider + ?Sized>(provider: &P) -> Result<(), StreamError> {
    absence::verify(provider).await?;
    views::verify(provider).await?;
    key_seek::verify(provider).await?;
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
        records: vec![
            Bytes::from_static(b"one").into(),
            Bytes::from_static(b"two").into(),
        ],
        if_tail: Some(0),
        idempotency_key: append_key.clone(),
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
            records: vec![Bytes::from_static(b"different").into()],
            if_tail: Some(0),
            idempotency_key: append_key.clone(),
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
            records: vec![Bytes::from_static(b"never").into()],
            if_tail: Some(1),
            idempotency_key: key(b"conformance-tail-conflict")?,
        })
        .await?
        != (AppendOutcome::TailConflict { actual_tail: 2 })
    {
        return Err(StreamError::Unavailable);
    }
    let fork = provider
        .fork(ForkRequest {
            exclude: Vec::new(),
            source: source.clone(),
            destination: child.clone(),
            at_tail: Some(1),
            idempotency_key: key(b"stream-fork")?,
        })
        .await?;
    if fork.forked_at != 1 || fork.tail != 1 {
        return Err(StreamError::Unavailable);
    }
    let inherited = provider
        .read(ReadRequest {
            key: None,
            path: child.clone(),
            from: 0,
            limit: 8,
            at_least: Default::default(),
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
            tail_stream: None,
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
            tail_stream: None,
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
            tail_stream: None,
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
            records: vec![Bytes::from_static(b"created").into()],
            if_tail: Some(0),
            idempotency_key: key(b"conformance-paging-new")?,
        })
        .await?;
    if provider
        .children_page(ChildrenPageRequest {
            tail_stream: None,
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
    let mut follow = provider
        .follow(crate::FollowRequest::new(child.clone(), 1))
        .map_ok(crate::records_only)
        .await?;
    provider
        .append(AppendRequest {
            path: child.clone(),
            records: vec![Bytes::from_static(b"live").into()],
            if_tail: Some(1),
            idempotency_key: key(b"conformance-live-follow")?,
        })
        .await?;
    #[cfg(not(target_arch = "wasm32"))]
    let live = tokio::time::timeout(std::time::Duration::from_secs(1), follow.next())
        .await
        .map_err(|_| StreamError::Unavailable)?
        .ok_or(StreamError::Unavailable)??;
    #[cfg(target_arch = "wasm32")]
    let live = follow.next().await.ok_or(StreamError::Unavailable)??;
    if live.sequence != 1 || live.value != Bytes::from_static(b"live") {
        return Err(StreamError::Unavailable);
    }
    let complete_history = provider
        .read(ReadRequest {
            key: None,
            path: child.clone(),
            from: 0,
            limit: 8,
            at_least: Default::default(),
        })
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    if !matches!(complete_history.as_slice(), [first, second]
        if first.value == Bytes::from_static(b"one")
            && second.value == Bytes::from_static(b"live"))
        || provider.tail(child.clone()).await? != 2
    {
        return Err(StreamError::Unavailable);
    }
    let committed_path = path("conformance/committed")?;
    let request = CommitRequest {
        deadline_unix_millis: None,
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
            exclude: Vec::new(),
            source: source.clone(),
            destination: committed_path,
            at_tail: 2,
            records: Vec::new(),
        }],
        idempotency_key: key(b"stream-commit")?,
    };
    let committed = provider.commit(request.clone()).await?;
    let CommitOutcome::Committed(receipt) = &committed else {
        return Err(StreamError::Unavailable);
    };
    if provider.commit(request).await? != committed
        || !receipt.matches_envelope(&provider.read_commit(receipt.commit_id).await?)
    {
        return Err(StreamError::Unavailable);
    }
    verify_stale_tail_condition(provider, source.clone()).await?;
    verify_fork_with_records(provider, source.clone()).await?;
    verify_read_open_failures(provider, source.clone()).await?;
    let absent = path("conformance/absent-tail")?;
    let absent_key = key(b"stream-absent-tail")?;
    let absent_request = CommitRequest {
        deadline_unix_millis: None,
        conditions: vec![CommitCondition::Tail {
            path: absent.clone(),
            expected: 0,
        }],
        mutations: vec![CommitMutation::Append {
            path: absent.clone(),
            records: vec![Bytes::from_static(b"first-write").into()],
        }],
        idempotency_key: absent_key.clone(),
    };
    let absent_outcome = provider.commit(absent_request.clone()).await?;
    if !matches!(absent_outcome, CommitOutcome::Committed(_))
        || provider.commit(absent_request).await? != absent_outcome
        || provider.tail(absent).await? != 1
    {
        return Err(StreamError::Unavailable);
    }
    let absent_observation = provider
        .inspect_idempotency(absent_key)
        .await?
        .ok_or(StreamError::Unavailable)?;
    let CommitOutcome::Committed(receipt) = absent_outcome else {
        return Err(StreamError::Unavailable);
    };
    let facts = provider.read_commit(receipt.commit_id).await?;
    if !receipt.matches_envelope(&facts)
        || absent_observation.outcome != IdempotencyOutcome::Commit(facts)
    {
        return Err(StreamError::Unavailable);
    }
    verify_unspent_conflicts(provider).await?;
    verify_subtree_fork(provider).await?;
    verify_page_tails(provider).await?;
    verify_page_tail_bytes(provider).await?;
    Ok(())
}

/// A finite read reports an absent path or a start beyond the tail when it
/// opens, never as a deferred first item: callers treat `NotFound` from the
/// open as an empty history.
async fn verify_read_open_failures(
    provider: &(impl StreamProvider + ?Sized),
    source: StreamPath,
) -> Result<(), StreamError> {
    let open = |path: StreamPath, from: u64| async move {
        provider
            .read(ReadRequest {
                key: None,
                path,
                from,
                limit: 1,
                at_least: Default::default(),
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
    provider: &(impl StreamProvider + ?Sized),
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
            deadline_unix_millis: None,
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
                exclude: Vec::new(),
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
    provider: &(impl StreamProvider + ?Sized),
    source: StreamPath,
) -> Result<(), StreamError> {
    let destination = path("conformance/forked-and-extended")?;
    let request = CommitRequest {
        deadline_unix_millis: None,
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
            exclude: Vec::new(),
            source,
            destination: destination.clone(),
            at_tail: 1,
            records: vec![
                Bytes::from_static(b"extended").into(),
                Bytes::from_static(b"again").into(),
            ],
        }],
        idempotency_key: key(b"stream-fork-with-records")?,
    };
    let committed = provider.commit(request.clone()).await?;
    let CommitOutcome::Committed(receipt) = &committed else {
        return Err(StreamError::Unavailable);
    };
    let envelope = provider.read_commit(receipt.commit_id).await?;
    if !receipt.matches_envelope(&envelope) {
        return Err(StreamError::Unavailable);
    }
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
            key: None,
            path: destination.clone(),
            from: 0,
            limit: 8,
            at_least: Default::default(),
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

/// Conflicts publish nothing: the same intent key may be retried at a fresh tail.
async fn verify_unspent_conflicts(
    provider: &(impl StreamProvider + ?Sized),
) -> Result<(), StreamError> {
    let path = path("conformance/unspent")?;
    let append_key = key(b"unspent-append")?;
    let mut request = AppendRequest {
        path: path.clone(),
        records: vec![Bytes::from_static(b"intent").into()],
        if_tail: Some(1),
        idempotency_key: append_key.clone(),
    };
    if !matches!(
        provider.append(request.clone()).await?,
        AppendOutcome::TailConflict { actual_tail: 0 }
    ) || provider.inspect_idempotency(append_key).await?.is_some()
    {
        return Err(StreamError::Unavailable);
    }
    request.if_tail = Some(0);
    if !matches!(provider.append(request).await?, AppendOutcome::Committed(_)) {
        return Err(StreamError::Unavailable);
    }
    let commit_key = key(b"unspent-commit")?;
    let mut request = CommitRequest {
        deadline_unix_millis: None,
        conditions: vec![CommitCondition::Tail {
            path: path.clone(),
            expected: 0,
        }],
        mutations: vec![CommitMutation::Append {
            path,
            records: vec![Bytes::from_static(b"next").into()],
        }],
        idempotency_key: commit_key.clone(),
    };
    if !matches!(
        provider.commit(request.clone()).await?,
        CommitOutcome::Conflict(_)
    ) || provider.inspect_idempotency(commit_key).await?.is_some()
    {
        return Err(StreamError::Unavailable);
    }
    let Some(CommitCondition::Tail { expected, .. }) = request.conditions.first_mut() else {
        return Err(StreamError::Unavailable);
    };
    *expected = 1;
    if !matches!(provider.commit(request).await?, CommitOutcome::Committed(_)) {
        return Err(StreamError::Unavailable);
    }
    Ok(())
}

/// One pre-commit cut covers the whole namespace, including implicit parents.
pub async fn verify_subtree_fork(
    provider: &(impl StreamProvider + ?Sized),
) -> Result<(), StreamError> {
    let seed = seed_subtree(provider).await?;
    let request = subtree_request()?;
    let child = path("conformance/tree/a/events")?;
    let destination = path("conformance/tree/children/fork")?;
    let inherited = path("conformance/tree/children/fork/a/events")?;
    let result = provider.commit(request.clone()).await?;
    let CommitOutcome::Committed(envelope) = &result else {
        return Err(StreamError::Unavailable);
    };
    if provider.commit(request).await? != result
        || provider.tail(inherited.clone()).await? != 1
        || provider.tail(child.clone()).await? != 2
        || provider.tail(destination.clone()).await? != 1
    {
        return Err(StreamError::Unavailable);
    }
    verify_subtree_records(provider, seed.commit_id, envelope.commit_id).await?;
    verify_subtree_private_writes(provider).await
}

async fn seed_subtree(
    provider: &(impl StreamProvider + ?Sized),
) -> Result<crate::AppendReceipt, StreamError> {
    let child = path("conformance/tree/a/events")?;
    let initial = provider
        .append(AppendRequest {
            path: child.clone(),
            records: vec![Bytes::from_static(b"prefix").into()],
            if_tail: Some(0),
            idempotency_key: key(b"tree-seed")?,
        })
        .await?;
    let AppendOutcome::Committed(seed) = initial else {
        return Err(StreamError::Unavailable);
    };
    provider
        .append(AppendRequest {
            path: path("conformance/tree/children/old/events")?,
            records: vec![Bytes::from_static(b"sibling").into()],
            if_tail: Some(0),
            idempotency_key: key(b"tree-sibling")?,
        })
        .await?;
    // Volume authority streams share the account-tree domain and caller commit.
    for name in ["conformance/volumes/one", "conformance/volumes/two"] {
        provider
            .append(AppendRequest {
                path: path(name)?,
                records: vec![Bytes::from_static(b"volume-head").into()],
                if_tail: Some(0),
                idempotency_key: IdempotencyKey::new(name.as_bytes().to_vec())?,
            })
            .await?;
    }
    Ok(seed)
}

fn subtree_request() -> Result<CommitRequest, StreamError> {
    let source = path("conformance/tree")?;
    let child = path("conformance/tree/a/events")?;
    let destination = path("conformance/tree/children/fork")?;
    let marker = path("conformance/adopted")?;
    Ok(CommitRequest {
        deadline_unix_millis: None,
        conditions: vec![
            CommitCondition::Absent {
                path: destination.clone(),
            },
            CommitCondition::Tail {
                path: child.clone(),
                expected: 1,
            },
            CommitCondition::Tail {
                path: marker.clone(),
                expected: 0,
            },
            CommitCondition::Absent {
                path: path("conformance/fork-volumes/one")?,
            },
            CommitCondition::Absent {
                path: path("conformance/fork-volumes/two")?,
            },
        ],
        mutations: vec![
            CommitMutation::Fork {
                exclude: vec![path("children")?],
                source,
                destination,
                at_tail: 0,
                records: vec![Bytes::from_static(b"bound").into()],
            },
            CommitMutation::Append {
                path: child,
                records: vec![Bytes::from_static(b"source-after-cut").into()],
            },
            CommitMutation::Append {
                path: marker,
                records: vec![Bytes::from_static(b"ForkAdopted").into()],
            },
            CommitMutation::Fork {
                source: path("conformance/volumes/one")?,
                destination: path("conformance/fork-volumes/one")?,
                at_tail: 1,
                exclude: Vec::new(),
                records: vec![Bytes::from_static(b"volume-bound").into()],
            },
            CommitMutation::Fork {
                source: path("conformance/volumes/two")?,
                destination: path("conformance/fork-volumes/two")?,
                at_tail: 1,
                exclude: Vec::new(),
                records: vec![Bytes::from_static(b"volume-bound").into()],
            },
        ],
        idempotency_key: key(b"tree-adopt")?,
    })
}

async fn verify_subtree_records(
    provider: &(impl StreamProvider + ?Sized),
    seed: crate::CommitId,
    committed: crate::CommitId,
) -> Result<(), StreamError> {
    let destination = path("conformance/tree/children/fork")?;
    let inherited = path("conformance/tree/children/fork/a/events")?;
    let marker = path("conformance/adopted")?;
    let records: Vec<_> = provider
        .read(ReadRequest {
            key: None,
            path: inherited.clone(),
            from: 0,
            limit: 8,
            at_least: Default::default(),
        })
        .await?
        .try_collect()
        .await?;
    if records.len() != 1
        || records
            .first()
            .is_none_or(|record| record.commit_id != seed)
    {
        return Err(StreamError::Unavailable);
    }
    for name in [
        "conformance/fork-volumes/one",
        "conformance/fork-volumes/two",
    ] {
        let records: Vec<_> = provider
            .read(ReadRequest {
                key: None,
                path: path(name)?,
                from: 1,
                limit: 1,
                at_least: Default::default(),
            })
            .await?
            .try_collect()
            .await?;
        if records.len() != 1
            || records
                .first()
                .is_none_or(|record| record.commit_id != committed)
        {
            return Err(StreamError::Unavailable);
        }
    }
    let bound: Vec<_> = provider
        .read(ReadRequest {
            key: None,
            path: destination.clone(),
            from: 0,
            limit: 8,
            at_least: Default::default(),
        })
        .await?
        .try_collect()
        .await?;
    let adopted: Vec<_> = provider
        .read(ReadRequest {
            key: None,
            path: marker,
            from: 0,
            limit: 8,
            at_least: Default::default(),
        })
        .await?
        .try_collect()
        .await?;
    if bound
        .first()
        .is_none_or(|record| record.commit_id != committed)
        || adopted
            .first()
            .is_none_or(|record| record.commit_id != committed)
    {
        return Err(StreamError::Unavailable);
    }
    Ok(())
}

async fn verify_subtree_private_writes(
    provider: &(impl StreamProvider + ?Sized),
) -> Result<(), StreamError> {
    let child = path("conformance/tree/a/events")?;
    let destination = path("conformance/tree/children/fork")?;
    let inherited = path("conformance/tree/children/fork/a/events")?;
    provider
        .append(AppendRequest {
            path: inherited.clone(),
            records: vec![Bytes::from_static(b"private").into()],
            if_tail: Some(1),
            idempotency_key: key(b"tree-private")?,
        })
        .await?;
    let source_records: Vec<_> = provider
        .read(ReadRequest {
            key: None,
            path: child,
            from: 1,
            limit: 8,
            at_least: Default::default(),
        })
        .await?
        .try_collect()
        .await?;
    if source_records.len() != 1
        || source_records
            .first()
            .is_none_or(|record| record.value.as_ref() != b"source-after-cut")
    {
        return Err(StreamError::Unavailable);
    }
    let page = provider
        .children_page(ChildrenPageRequest {
            tail_stream: None,
            parent: Some(destination.clone()),
            after: None,
            hierarchy_version: None,
            limit: 1,
        })
        .await?;
    if page.children.first().map(|child| child.path.as_str())
        != Some("conformance/tree/children/fork/a")
    {
        return Err(StreamError::Unavailable);
    }
    // A nested destination must inherit the pre-commit source, never itself.
    if provider
        .tail(path("conformance/tree/children/fork/children/fork")?)
        .await
        != Err(StreamError::NotFound)
    {
        return Err(StreamError::Unavailable);
    }
    if provider
        .tail(path("conformance/tree/children/old/events")?)
        .await?
        != 1
        || provider
            .tail(path("conformance/tree/children/fork/children/old/events")?)
            .await
            != Err(StreamError::NotFound)
    {
        return Err(StreamError::Unavailable);
    }
    let second = path("conformance/tree-second")?;
    provider
        .fork(ForkRequest {
            exclude: Vec::new(),
            source: destination,
            destination: second,
            at_tail: None,
            idempotency_key: key(b"tree-second")?,
        })
        .await?;
    if provider
        .tail(path("conformance/tree-second/a/events")?)
        .await?
        != 2
    {
        return Err(StreamError::Unavailable);
    }
    Ok(())
}

/// A single page captures status with child membership; no extra reads or creation.
pub async fn verify_page_tails(
    provider: &(impl StreamProvider + ?Sized),
) -> Result<(), StreamError> {
    let a = path("conformance/page-tails/a/status")?;
    let b = path("conformance/page-tails/b/status")?;
    let missing = path("conformance/page-tails/c/other")?;
    for target in [&a, &b, &missing] {
        provider
            .append(AppendRequest {
                path: target.clone(),
                records: vec![Bytes::from_static(b"old").into()],
                if_tail: Some(0),
                idempotency_key: IdempotencyKey::new(target.as_str().as_bytes().to_vec())?,
            })
            .await?;
    }
    let page = provider
        .children_page(ChildrenPageRequest {
            parent: Some(path("conformance/page-tails")?),
            after: None,
            hierarchy_version: None,
            limit: 3,
            tail_stream: Some(path("status")?),
        })
        .await?;
    if page.children.len() != 3
        || page
            .children
            .get(2)
            .is_none_or(|child| child.tail_record.is_some())
    {
        return Err(StreamError::Unavailable);
    }
    let cut = provider
        .commit(CommitRequest {
            deadline_unix_millis: None,
            conditions: [&a, &b]
                .into_iter()
                .map(|path| CommitCondition::Tail {
                    path: path.clone(),
                    expected: 1,
                })
                .collect(),
            mutations: vec![a.clone(), b.clone()]
                .into_iter()
                .map(|path| CommitMutation::Append {
                    path,
                    records: vec![Bytes::from_static(b"new").into()],
                })
                .collect(),
            idempotency_key: key(b"page-tails-next")?,
        })
        .await?;
    let CommitOutcome::Committed(cut) = cut else {
        return Err(StreamError::Unavailable);
    };
    for child in page.children.iter().take(2) {
        if child
            .tail_record
            .as_ref()
            .map(|record| record.value.as_ref())
            != Some(b"old".as_slice())
        {
            return Err(StreamError::Unavailable);
        }
    }
    let next = provider
        .children_page(ChildrenPageRequest {
            parent: Some(path("conformance/page-tails")?),
            after: None,
            hierarchy_version: None,
            limit: 3,
            tail_stream: Some(path("status")?),
        })
        .await?;
    if next.children.len() != 3 {
        return Err(StreamError::Unavailable);
    }
    for child in next.children.iter().take(2) {
        if child.tail_record.as_ref().map(|record| record.commit_id) != Some(cut.commit_id) {
            return Err(StreamError::Unavailable);
        }
    }
    if provider
        .tail(path("conformance/page-tails/c/status")?)
        .await
        != Err(StreamError::NotFound)
    {
        return Err(StreamError::Unavailable);
    }
    Ok(())
}

/// Projected values, headers and protobuf framing share the command byte ceiling.
pub async fn verify_page_tail_bytes(
    provider: &(impl StreamProvider + ?Sized),
) -> Result<(), StreamError> {
    // Values alone fit; the indexed headers take the complete page over the bound.
    let value = Bytes::from(vec![0; crate::MAX_RECORD_BYTES / 2]);
    let headers = crate::RecordHeaders::new(vec![vec![1; 512].into()], vec![])?;
    let count = crate::MAX_COMMAND_BYTES / value.len();
    for index in 0..count {
        let target = path(&format!("conformance/page-bytes/{index:04}/status"))?;
        let outcome = provider
            .append(AppendRequest {
                idempotency_key: IdempotencyKey::new(target.as_str().as_bytes().to_vec())?,
                path: target,
                records: vec![crate::AppendRecord {
                    value: value.clone(),
                    headers: headers.clone(),
                }],
                if_tail: Some(0),
            })
            .await?;
        if !matches!(outcome, AppendOutcome::Committed(_)) {
            return Err(StreamError::Unavailable);
        }
    }
    let page = match provider
        .children_page(ChildrenPageRequest {
            parent: Some(path("conformance/page-bytes")?),
            after: None,
            hierarchy_version: None,
            limit: u32::try_from(count).map_err(|_| StreamError::LimitExceeded)?,
            tail_stream: Some(path("status")?),
        })
        .await
    {
        Err(StreamError::LimitExceeded) => return Ok(()),
        result => result?,
    };
    if page.children.is_empty()
        || page.children.len() > count
        || (page.children.len() < count && page.next_after.is_none())
        || page.children.iter().any(|child| {
            child
                .tail_record
                .as_ref()
                .is_none_or(|record| record.value != value || record.headers != headers)
        })
        || prost::Message::encoded_len(&crate::wire_codec::children_page_to_wire(page))
            > crate::MAX_COMMAND_BYTES
    {
        return Err(StreamError::Unavailable);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Failure, Fixture, bench, suite};
    use crate::{MemoryLimits, MemoryStream};

    async fn memory() -> Result<Fixture<MemoryStream>, Failure> {
        Ok(Fixture::new(MemoryStream::new(MemoryLimits::default())))
    }
    suite!(memory_provider_passes_the_public_suite, memory);

    #[tokio::test]
    async fn shared_benchmark_checks_append_read_follow_and_zero_control() -> Result<(), Failure> {
        let sample = bench(&memory().await?).await?;
        assert_eq!(sample.records, 256);
        assert_eq!(sample.requests.control(), 0);
        assert_eq!(sample.requests.data(), 512);
        Ok(())
    }
}
