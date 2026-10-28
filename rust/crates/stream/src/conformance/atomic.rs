//! Provider-independent assertions formerly confined to the HTTP fixture runner.
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
    };
    let committed = provider.commit(atomic.clone()).await?;
    assert!(
        matches!(&committed, crate::CommitOutcome::Committed(envelope) if envelope.mutations.len() == 2)
    );
    assert_eq!(provider.commit(atomic).await?, committed);
    assert_eq!(provider.tail(first).await?, 1);
    assert_eq!(provider.tail(second).await?, 1);
    let observed = provider
        .inspect_idempotency(atomic_key)
        .await?
        .ok_or(StreamError::InvalidResponse)?;
    assert_eq!(
        observed.outcome,
        crate::IdempotencyOutcome::Commit(committed)
    );
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
    };
    assert_eq!(
        provider.commit_before(request, 1).await,
        Err(StreamError::DeadlineElapsed)
    );
    assert_eq!(
        provider.tail(deadline_path).await,
        Err(StreamError::NotFound)
    );
    Ok(())
}
