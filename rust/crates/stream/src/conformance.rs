//! Reusable black-box qualification of the complete public Stream contract.

use bytes::Bytes;
use futures::StreamExt as _;

use crate::{
    AppendOutcome, AppendRequest, CommitCondition, CommitMutation, CommitOutcome, CommitRequest,
    ForkRequest, IdempotencyKey, ReadRequest, StreamError, StreamPath, StreamProvider,
};

/// Canonical language-neutral Stream conformance inventory.
pub const SUITE: &[u8] = include_bytes!("../../../../conformance/vectors/stream.json");

/// Exercises the complete provider-independent hierarchical Stream contract.
pub async fn verify(provider: &dyn StreamProvider) -> Result<(), String> {
    if SUITE.is_empty() {
        return Err("Stream conformance inventory is empty".into());
    }
    let source = path("conformance/source")?;
    let child = path("conformance/child")?;
    let append_key = key(b"stream-append")?;
    let initial = AppendRequest {
        path: source.clone(),
        records: vec![Bytes::from_static(b"one"), Bytes::from_static(b"two")],
        if_tail: Some(0),
        idempotency_key: Some(append_key.clone()),
    };
    let first = provider.append(initial.clone()).await.map_err(error)?;
    let AppendOutcome::Committed(first_receipt) = &first else {
        return Err("initial tail CAS conflicted".into());
    };
    if first_receipt.start != 0
        || first_receipt.end != 2
        || first_receipt.tail != 2
        || provider.append(initial).await.map_err(error)? != first
    {
        return Err("atomic append or exact replay changed its receipt".into());
    }
    if provider
        .append(AppendRequest {
            path: source.clone(),
            records: vec![Bytes::from_static(b"different")],
            if_tail: Some(0),
            idempotency_key: Some(append_key),
        })
        .await
        != Err(StreamError::IdempotencyMismatch)
    {
        return Err("changed idempotency arguments were accepted".into());
    }
    if provider
        .append(AppendRequest {
            path: source.clone(),
            records: vec![Bytes::from_static(b"never")],
            if_tail: Some(1),
            idempotency_key: None,
        })
        .await
        .map_err(error)?
        != (AppendOutcome::TailConflict { actual_tail: 2 })
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
        .map_err(error)?;
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
        .map_err(error)?
        .collect::<Vec<_>>()
        .await;
    if inherited.len() != 1
        || inherited[0].as_ref().map_err(ToString::to_string)?.value != Bytes::from_static(b"one")
    {
        return Err("forked history was not exact".into());
    }
    let mut follow = provider.follow(child.clone(), 1).await.map_err(error)?;
    provider
        .append(AppendRequest {
            path: child.clone(),
            records: vec![Bytes::from_static(b"live")],
            if_tail: Some(1),
            idempotency_key: None,
        })
        .await
        .map_err(error)?;
    let live = tokio::time::timeout(std::time::Duration::from_secs(1), follow.next())
        .await
        .map_err(|_| "follow did not make bounded progress".to_owned())?
        .ok_or_else(|| "follow ended".to_owned())?
        .map_err(error)?;
    if live.sequence != 1 || live.value != Bytes::from_static(b"live") {
        return Err("follow returned a gap or duplicate".into());
    }
    let trim_key = key(b"stream-trim")?;
    let trimmed = provider
        .trim(child.clone(), 1, trim_key.clone())
        .await
        .map_err(error)?;
    if trimmed.trim_point != 1
        || provider
            .trim(child.clone(), 1, trim_key.clone())
            .await
            .map_err(error)?
            != trimmed
        || provider.trim(child.clone(), 2, trim_key).await != Err(StreamError::IdempotencyMismatch)
    {
        return Err("logical trim replay or mismatch semantics changed".into());
    }
    let trimmed_read = provider
        .read(ReadRequest {
            path: child.clone(),
            from: 0,
            limit: 1,
        })
        .await;
    let trimmed = match trimmed_read {
        Err(StreamError::OutOfRange) => true,
        Ok(mut records) => matches!(records.next().await, Some(Err(StreamError::OutOfRange))),
        _ => false,
    };
    if !trimmed {
        return Err("trimmed history remained publicly readable".into());
    }
    let delete_key = key(b"stream-delete")?;
    let deleted = provider
        .delete(child.clone(), delete_key.clone())
        .await
        .map_err(error)?;
    if provider
        .delete(child.clone(), delete_key)
        .await
        .map_err(error)?
        != deleted
        || provider.tail(child.clone()).await != Err(StreamError::Retired)
    {
        return Err("permanent deletion or exact replay changed".into());
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
            source,
            destination: committed_path,
            at_tail: 2,
        }],
        idempotency_key: key(b"stream-commit")?,
    };
    let committed = provider.commit(request.clone()).await.map_err(error)?;
    let CommitOutcome::Committed(envelope) = &committed else {
        return Err("valid coordinated commit conflicted".into());
    };
    if provider.commit(request).await.map_err(error)? != committed
        || provider
            .read_commit(envelope.commit_id)
            .await
            .map_err(error)?
            != *envelope
    {
        return Err("coordinated commit replay or envelope changed".into());
    }
    Ok(())
}

fn path(value: &str) -> Result<StreamPath, String> {
    StreamPath::new(value).map_err(error)
}

fn key(value: &'static [u8]) -> Result<IdempotencyKey, String> {
    IdempotencyKey::new(Bytes::from_static(value)).map_err(error)
}

fn error(error: impl ToString) -> String {
    error.to_string()
}

#[cfg(test)]
mod tests {
    use super::verify;
    use crate::{MemoryLimits, MemoryStream};

    #[tokio::test]
    async fn memory_provider_passes_the_public_suite() -> Result<(), String> {
        verify(&MemoryStream::new(MemoryLimits::default())).await
    }
}
