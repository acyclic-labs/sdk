//! Reusable black-box qualification of the complete public Stream contract.

use bytes::Bytes;
use futures::StreamExt as _;

use crate::{
    AppendOutcome, AppendRequest, ChildrenPageRequest, ChildrenRequest, CommitCondition,
    CommitConflict, CommitMutation, CommitOutcome, CommitRequest, CommittedMutation, ForkRequest,
    IdempotencyKey, IdempotencyOutcome, ReadRequest, StreamError, StreamPath, StreamProvider,
};

/// Canonical language-neutral Stream conformance inventory.
pub const SUITE: &[u8] = include_bytes!("../conformance/stream.json");

/// Exercises the complete provider-independent hierarchical Stream contract.
#[allow(
    clippy::too_many_lines,
    reason = "linear conformance walkthrough; each check is a distinct provider-contract \
              assertion, and splitting it would only move the same sequential checks behind \
              indirection"
)]
#[allow(
    clippy::cognitive_complexity,
    reason = "the linear provider conformance walkthrough keeps each assertion visible"
)]
pub async fn verify(provider: &dyn StreamProvider) -> Result<(), String> {
    if SUITE.is_empty() {
        return Err("Stream conformance inventory is empty".into());
    }
    let source = path("conformance/source")?;
    let child = path("conformance/child")?;
    let append_key = key(b"stream-append")?;
    if provider
        .inspect_idempotency(append_key.clone())
        .await
        .map_err(|err| error(&err))?
        .is_some()
    {
        return Err("unknown idempotency identity was reported as retained".into());
    }
    let initial = AppendRequest {
        path: source.clone(),
        records: vec![Bytes::from_static(b"one"), Bytes::from_static(b"two")],
        if_tail: Some(0),
        idempotency_key: Some(append_key.clone()),
    };
    let first = provider
        .append(initial.clone())
        .await
        .map_err(|err| error(&err))?;
    let AppendOutcome::Committed(first_receipt) = &first else {
        return Err("initial tail CAS conflicted".into());
    };
    if first_receipt.start != 0
        || first_receipt.end != 2
        || first_receipt.tail != 2
        || provider.append(initial).await.map_err(|err| error(&err))? != first
    {
        return Err("atomic append or exact replay changed its receipt".into());
    }
    let source_prefix = provider
        .read(ReadRequest {
            path: source.clone(),
            from: 0,
            limit: 8,
        })
        .await
        .map_err(|err| error(&err))?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| error(&err))?;
    provider
        .append(AppendRequest {
            path: source.clone(),
            records: vec![Bytes::from_static(b"three")],
            if_tail: Some(2),
            idempotency_key: Some(key(b"stream-append-third")?),
        })
        .await
        .map_err(|err| error(&err))?;
    let source_after_append = provider
        .read(ReadRequest {
            path: source.clone(),
            from: 0,
            limit: 8,
        })
        .await
        .map_err(|err| error(&err))?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| error(&err))?;
    if source_after_append.get(..source_prefix.len()) != Some(source_prefix.as_slice()) {
        return Err("append changed an existing immutable stream prefix".into());
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
        return Err("changed idempotency arguments were accepted".into());
    }
    let append_observation = provider
        .inspect_idempotency(append_key.clone())
        .await
        .map_err(|err| error(&err))?
        .ok_or_else(|| "committed append identity was not retained".to_owned())?;
    if append_observation.idempotency_key != append_key
        || append_observation.request_digest == [0; 32]
        || append_observation.outcome != IdempotencyOutcome::Append(first.clone())
    {
        return Err("append inspection did not return the exact retained outcome".into());
    }
    if provider
        .append(AppendRequest {
            path: source.clone(),
            records: vec![Bytes::from_static(b"never")],
            if_tail: Some(1),
            idempotency_key: None,
        })
        .await
        .map_err(|err| error(&err))?
        != (AppendOutcome::TailConflict { actual_tail: 3 })
    {
        return Err("tail conflict was not returned as data".into());
    }
    let fork = provider
        .fork(ForkRequest {
            source: source.clone(),
            destination: child.clone(),
            at_tail: Some(1),
            idempotency_key: Some(key(b"stream-fork")?),
        })
        .await
        .map_err(|err| error(&err))?;
    if fork.forked_at != 1 || fork.tail != 1 {
        return Err("fork did not retain the exact immutable prefix".into());
    }
    let inherited = provider
        .read(ReadRequest {
            path: child.clone(),
            from: 0,
            limit: 8,
        })
        .await
        .map_err(|err| error(&err))?
        .collect::<Vec<_>>()
        .await;
    let [item] = inherited.as_slice() else {
        return Err("forked history was not exact".into());
    };
    if item.as_ref().map_err(ToString::to_string)?.value != Bytes::from_static(b"one") {
        return Err("forked history was not exact".into());
    }
    let children = provider
        .children(ChildrenRequest {
            parent: Some(path("conformance")?),
            limit: 8,
        })
        .await
        .map_err(|err| error(&err))?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| error(&err))?;
    let mut child_paths = children
        .into_iter()
        .map(|child| child.path)
        .collect::<Vec<_>>();
    child_paths.sort();
    if child_paths != vec![path("conformance/child")?, path("conformance/source")?] {
        return Err("direct child listing changed its fixed snapshot".into());
    }
    let first_page = provider
        .children_page(ChildrenPageRequest {
            parent: Some(path("conformance")?),
            after: None,
            hierarchy_version: None,
            limit: 1,
        })
        .await
        .map_err(|err| error(&err))?;
    let [first_child] = first_page.children.as_slice() else {
        return Err("first hierarchy page did not expose one child".into());
    };
    if first_page.next_after.as_ref() != Some(&first_child.path) {
        return Err("first hierarchy page did not expose a continuation".into());
    }
    let final_page = provider
        .children_page(ChildrenPageRequest {
            parent: Some(path("conformance")?),
            after: first_page.next_after.clone(),
            hierarchy_version: Some(first_page.hierarchy_version),
            limit: 1,
        })
        .await
        .map_err(|err| error(&err))?;
    let [final_child] = final_page.children.as_slice() else {
        return Err("final hierarchy page did not expose one child".into());
    };
    if final_page.next_after.is_some() || final_child.path == first_child.path {
        return Err("hierarchy pagination duplicated or omitted a child".into());
    }
    provider
        .append(AppendRequest {
            path: path("conformance/paging-new")?,
            records: vec![Bytes::from_static(b"created")],
            if_tail: Some(0),
            idempotency_key: None,
        })
        .await
        .map_err(|err| error(&err))?;
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
        return Err("stale hierarchy continuation was accepted".into());
    }
    let mut follow = provider
        .follow(child.clone(), 1)
        .await
        .map_err(|err| error(&err))?;
    provider
        .append(AppendRequest {
            path: child.clone(),
            records: vec![Bytes::from_static(b"live")],
            if_tail: Some(1),
            idempotency_key: None,
        })
        .await
        .map_err(|err| error(&err))?;
    let live = tokio::time::timeout(std::time::Duration::from_secs(1), follow.next())
        .await
        .map_err(|_| "follow did not make bounded progress".to_owned())?
        .ok_or_else(|| "follow ended".to_owned())?
        .map_err(|err| error(&err))?;
    if live.sequence != 1 || live.value != Bytes::from_static(b"live") {
        return Err("follow returned a gap or duplicate".into());
    }
    let complete_history = provider
        .read(ReadRequest {
            path: child.clone(),
            from: 0,
            limit: 8,
        })
        .await
        .map_err(|err| error(&err))?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| error(&err))?;
    if !matches!(complete_history.as_slice(), [first, second]
        if first.value == Bytes::from_static(b"one")
            && second.value == Bytes::from_static(b"live"))
        || provider
            .bounds(child.clone())
            .await
            .map_err(|err| error(&err))?
            .tail
            != 2
    {
        return Err("committed history was not retained from sequence zero".into());
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
    let committed = provider
        .commit(request.clone())
        .await
        .map_err(|err| error(&err))?;
    let CommitOutcome::Committed(envelope) = &committed else {
        return Err("valid coordinated commit conflicted".into());
    };
    if provider.commit(request).await.map_err(|err| error(&err))? != committed
        || provider
            .read_commit(envelope.commit_id)
            .await
            .map_err(|err| error(&err))?
            != *envelope
    {
        return Err("coordinated commit replay or envelope changed".into());
    }
    verify_commit_forks(provider, source.clone()).await?;
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
    if provider
        .commit(absent_request.clone())
        .await
        .map_err(|err| error(&err))?
        != absent_conflict
        || provider
            .commit(absent_request)
            .await
            .map_err(|err| error(&err))?
            != absent_conflict
        || provider.tail(absent).await != Err(StreamError::NotFound)
    {
        return Err("absent tail condition did not return one replayable conflict".into());
    }
    let absent_observation = provider
        .inspect_idempotency(absent_key.clone())
        .await
        .map_err(|err| error(&err))?
        .ok_or_else(|| "commit conflict identity was not retained".to_owned())?;
    if absent_observation.idempotency_key != absent_key
        || absent_observation.request_digest == [0; 32]
        || absent_observation.outcome != IdempotencyOutcome::Commit(absent_conflict)
    {
        return Err("commit-conflict inspection did not preserve the terminal result".into());
    }
    Ok(())
}

/// A stale tail condition changes nothing, and a fork mutation extends
/// its new path with records in the same commit.
async fn verify_commit_forks(
    provider: &dyn StreamProvider,
    source: StreamPath,
) -> Result<(), String> {
    verify_stale_tail_condition(provider, source.clone()).await?;
    verify_fork_with_records(provider, source).await
}

async fn verify_stale_tail_condition(
    provider: &dyn StreamProvider,
    source: StreamPath,
) -> Result<(), String> {
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
        .await
        .map_err(|err| error(&err))?
        != stale_conflict
        || provider.tail(stale_path).await != Err(StreamError::NotFound)
        || provider.tail(source).await.map_err(|err| error(&err))? != 2
    {
        return Err("stale tail condition mutated a coordinated commit".into());
    }
    Ok(())
}

/// A fork mutation appends its records after the inherited prefix, in the
/// same commit, and its committed fact carries them.
async fn verify_fork_with_records(
    provider: &dyn StreamProvider,
    source: StreamPath,
) -> Result<(), String> {
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
    let committed = provider
        .commit(request.clone())
        .await
        .map_err(|err| error(&err))?;
    let CommitOutcome::Committed(envelope) = &committed else {
        return Err("fork with records conflicted".into());
    };
    let [CommittedMutation::Fork(fork)] = envelope.mutations.as_slice() else {
        return Err("fork with records did not commit one fork fact".into());
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
        return Err("fork with records did not append after the inherited prefix".into());
    }
    let values = provider
        .read(ReadRequest {
            path: destination.clone(),
            from: 0,
            limit: 8,
        })
        .await
        .map_err(|err| error(&err))?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|record| record.map(|record| record.value))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| error(&err))?;
    if values
        != [
            Bytes::from_static(b"one"),
            Bytes::from_static(b"extended"),
            Bytes::from_static(b"again"),
        ]
        || provider
            .tail(destination)
            .await
            .map_err(|err| error(&err))?
            != 3
        || provider.commit(request).await.map_err(|err| error(&err))? != committed
    {
        return Err("fork with records history or replay changed".into());
    }
    Ok(())
}

fn path(value: &str) -> Result<StreamPath, String> {
    StreamPath::new(value).map_err(|err| error(&err))
}

fn key(value: &'static [u8]) -> Result<IdempotencyKey, String> {
    IdempotencyKey::new(Bytes::from_static(value)).map_err(|err| error(&err))
}

fn error(error: &impl ToString) -> String {
    error.to_string()
}

#[cfg(test)]
mod tests {
    use super::verify;
    use crate::{
        AppendOutcome, AppendRequest, ChildStream, ChildrenPage, ChildrenPageRequest,
        ChildrenRequest, CommitId, CommitOutcome, CommitRequest, CommittedEnvelope, ForkReceipt,
        IdempotencyKey, IdempotencyObservation, MemoryLimits, MemoryStream, ReadRequest,
        RecordStream, StreamBounds, StreamError, StreamPath, StreamProvider,
    };
    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::StreamExt as _;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    /// A deliberately nonconforming provider. It changes one middle record on
    /// the second full read while preserving the first and last records. The
    /// public conformance suite must catch this provider violation; endpoint
    /// anchor checks alone are not a proof for arbitrary middle rewrites.
    struct MiddleRewrite {
        inner: Arc<MemoryStream>,
        source_reads: AtomicUsize,
    }

    impl MiddleRewrite {
        fn new() -> Self {
            Self {
                inner: Arc::new(MemoryStream::new(MemoryLimits::default())),
                source_reads: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl StreamProvider for MiddleRewrite {
        async fn inspect_idempotency(
            &self,
            key: IdempotencyKey,
        ) -> Result<Option<IdempotencyObservation>, StreamError> {
            self.inner.inspect_idempotency(key).await
        }

        async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
            self.inner.tail(path).await
        }

        async fn bounds(&self, path: StreamPath) -> Result<StreamBounds, StreamError> {
            self.inner.bounds(path).await
        }

        async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
            self.inner.append(request).await
        }

        async fn fork(&self, request: crate::ForkRequest) -> Result<ForkReceipt, StreamError> {
            self.inner.fork(request).await
        }

        async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
            let mut records = self
                .inner
                .read(request.clone())
                .await?
                .collect::<Vec<_>>()
                .await;
            if request.path.as_str() == "conformance/source"
                && request.from == 0
                && self.source_reads.fetch_add(1, Ordering::SeqCst) == 1
                && let Some(Ok(record)) = records.get_mut(1)
            {
                record.value = Bytes::from_static(b"middle-rewrite");
            }
            Ok(futures::stream::iter(records).boxed())
        }

        async fn follow(&self, path: StreamPath, from: u64) -> Result<RecordStream, StreamError> {
            self.inner.follow(path, from).await
        }

        async fn children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError> {
            self.inner.children(request).await
        }

        async fn children_page(
            &self,
            request: ChildrenPageRequest,
        ) -> Result<ChildrenPage, StreamError> {
            self.inner.children_page(request).await
        }

        async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
            self.inner.commit(request).await
        }

        async fn commit_before(
            &self,
            request: CommitRequest,
            deadline_unix_millis: u64,
        ) -> Result<CommitOutcome, StreamError> {
            self.inner
                .commit_before(request, deadline_unix_millis)
                .await
        }

        async fn read_commit(&self, commit_id: CommitId) -> Result<CommittedEnvelope, StreamError> {
            self.inner.read_commit(commit_id).await
        }
    }

    #[tokio::test]
    async fn memory_provider_passes_the_public_suite() -> Result<(), String> {
        verify(&MemoryStream::new(MemoryLimits::default())).await
    }

    #[tokio::test]
    async fn conformance_rejects_a_middle_prefix_rewrite() {
        let result = verify(&MiddleRewrite::new()).await;
        assert_eq!(
            result,
            Err("append changed an existing immutable stream prefix".into())
        );
    }
}
