//! Provider-independent assertions formerly confined to the HTTP fixture runner.
use crate::StreamProviderExt as _;
use crate::{StreamError, StreamPath, StreamProvider};

pub(super) async fn verify(provider: &dyn StreamProvider) -> Result<(), super::Failure> {
    let first = StreamPath::new("conformance/atomic-first")?;
    let second = StreamPath::new("conformance/atomic-second")?;
    let atomic_key = crate::IdempotencyKey::new(crate::OwnedBytes::from_static(b"http-atomic"))?;
    let atomic = crate::CommitRequest {
        conditions: vec![
            crate::CommitCondition::Absent {
                path: first.clone(),
            },
            crate::CommitCondition::Absent {
                path: second.clone(),
            },
        ],
        mutations: vec![
            crate::CommitMutation::Append {
                path: first.clone(),
                records: vec![crate::OwnedBytes::from_static(b"first").into()],
            },
            crate::CommitMutation::Append {
                path: second.clone(),
                records: vec![crate::OwnedBytes::from_static(b"second").into()],
            },
        ],
        idempotency_key: atomic_key.clone(),
        deadline_unix_millis: None,
    };
    let committed = provider.commit(atomic.clone()).await?;
    let crate::CommitOutcome::Committed(receipt) = &committed else {
        return Err(violated!());
    };
    if receipt.appends.len() != 2
        || provider.commit(atomic).await? != committed
        || provider.tail(first).await? != 1
        || provider.tail(second).await? != 1
    {
        return Err(violated!());
    }
    let observed = provider
        .inspect_idempotency(atomic_key)
        .await?
        .ok_or(violated!())?;
    let crate::IdempotencyOutcome::Commit(envelope) = observed.outcome else {
        return Err(violated!());
    };
    if envelope.commit_id != receipt.commit_id || envelope.mutations.len() != 2 {
        return Err(violated!());
    }
    Ok(())
}

pub(super) async fn deadline(provider: &dyn StreamProvider) -> Result<(), super::Failure> {
    let deadline_path = StreamPath::new("conformance/deadline")?;
    let request = crate::CommitRequest {
        conditions: vec![crate::CommitCondition::Absent {
            path: deadline_path.clone(),
        }],
        mutations: vec![crate::CommitMutation::Append {
            path: deadline_path.clone(),
            records: vec![crate::OwnedBytes::from_static(b"must-not-commit").into()],
        }],
        idempotency_key: crate::IdempotencyKey::new(crate::OwnedBytes::from_static(
            b"http-deadline",
        ))?,
        deadline_unix_millis: Some(1),
    };
    if provider.commit(request).await != Err(StreamError::DeadlineElapsed)
        || provider.tail(deadline_path).await != Err(StreamError::NotFound)
    {
        return Err(violated!());
    }
    Ok(())
}
