//! Provider-independent assertions formerly confined to the HTTP fixture runner.
use crate::StreamProviderExt as _;
use crate::{StreamError, StreamPath, StreamProvider};

pub(super) async fn verify(provider: &dyn StreamProvider) -> Result<(), StreamError> {
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
        return Err(StreamError::InvalidResponse);
    };
    assert_eq!(receipt.appends.len(), 2);
    assert_eq!(provider.commit(atomic).await?, committed);
    assert_eq!(provider.tail(first).await?, 1);
    assert_eq!(provider.tail(second).await?, 1);
    let observed = provider
        .inspect_idempotency(atomic_key)
        .await?
        .ok_or(StreamError::InvalidResponse)?;
    let crate::IdempotencyOutcome::Commit(envelope) = observed.outcome else {
        return Err(StreamError::InvalidResponse);
    };
    assert_eq!(envelope.commit_id, receipt.commit_id);
    assert_eq!(envelope.mutations.len(), 2);
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
    assert_eq!(
        provider.commit(request).await,
        Err(StreamError::DeadlineElapsed)
    );
    assert_eq!(
        provider.tail(deadline_path).await,
        Err(StreamError::NotFound)
    );
    Ok(())
}
