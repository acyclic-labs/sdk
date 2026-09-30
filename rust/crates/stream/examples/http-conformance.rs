//! Local HTTP qualification against the canonical Rust memory provider fixture.
use acyclic_stream::{StreamError, StreamPath, StreamProvider, conformance, http::HttpStream};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = std::env::args().nth(1).ok_or("missing endpoint")?;
    let provider = HttpStream::new(&endpoint, "conformance", 16 * 1024 * 1024)?;
    conformance::verify(&provider).await?;
    let first = StreamPath::new("conformance/atomic-first")?;
    let second = StreamPath::new("conformance/atomic-second")?;
    let atomic_key =
        acyclic_stream::IdempotencyKey::new(bytes::Bytes::from_static(b"http-atomic"))?;
    let atomic = acyclic_stream::CommitRequest {
        conditions: vec![
            acyclic_stream::CommitCondition::Absent {
                path: first.clone(),
            },
            acyclic_stream::CommitCondition::Absent {
                path: second.clone(),
            },
        ],
        mutations: vec![
            acyclic_stream::CommitMutation::Append {
                path: first.clone(),
                records: vec![bytes::Bytes::from_static(b"first")],
            },
            acyclic_stream::CommitMutation::Append {
                path: second.clone(),
                records: vec![bytes::Bytes::from_static(b"second")],
            },
        ],
        idempotency_key: atomic_key.clone(),
    };
    let committed = provider.commit(atomic.clone()).await?;
    assert!(
        matches!(&committed, acyclic_stream::CommitOutcome::Committed(envelope) if envelope.mutations.len() == 2)
    );
    assert_eq!(provider.commit(atomic).await?, committed);
    verify_commit_only(&endpoint).await?;
    assert_eq!(provider.tail(first).await?, 1);
    assert_eq!(provider.tail(second).await?, 1);
    let observed = provider
        .inspect_idempotency(atomic_key)
        .await?
        .ok_or("missing committed identity")?;
    assert_eq!(
        observed.outcome,
        acyclic_stream::IdempotencyOutcome::Commit(committed)
    );
    verify_deadline_and_bounds(&provider, &endpoint).await?;
    println!("Rust Stream HTTP: full public conformance, authentication and bounds passed");
    Ok(())
}

async fn verify_commit_only(endpoint: &str) -> Result<(), Box<dyn std::error::Error>> {
    let commit_only = HttpStream::new(endpoint, "commit-only", 16 * 1024 * 1024)?;
    let commit_only_request = acyclic_stream::CommitRequest {
        conditions: vec![acyclic_stream::CommitCondition::Absent {
            path: StreamPath::new("conformance/commit-only")?,
        }],
        mutations: vec![acyclic_stream::CommitMutation::Append {
            path: StreamPath::new("conformance/commit-only")?,
            records: vec![bytes::Bytes::from_static(b"admitted")],
        }],
        idempotency_key: acyclic_stream::IdempotencyKey::new(bytes::Bytes::from_static(
            b"commit-only",
        ))?,
    };
    let commit_only_result = commit_only.commit(commit_only_request.clone()).await?;
    assert_eq!(
        commit_only.commit(commit_only_request).await?,
        commit_only_result
    );
    let acyclic_stream::CommitOutcome::Committed(envelope) = commit_only_result else {
        return Err("commit-only mutation did not commit".into());
    };
    assert_eq!(
        commit_only.read_commit(envelope.commit_id).await,
        Err(StreamError::AccessDenied)
    );
    Ok(())
}

async fn verify_deadline_and_bounds(
    provider: &HttpStream,
    endpoint: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline_path = StreamPath::new("conformance/deadline")?;
    let request = acyclic_stream::CommitRequest {
        conditions: vec![acyclic_stream::CommitCondition::Absent {
            path: deadline_path.clone(),
        }],
        mutations: vec![acyclic_stream::CommitMutation::Append {
            path: deadline_path.clone(),
            records: vec![bytes::Bytes::from_static(b"must-not-commit")],
        }],
        idempotency_key: acyclic_stream::IdempotencyKey::new(bytes::Bytes::from_static(
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
    let denied = HttpStream::new(endpoint, "wrong", 1024)?;
    assert_eq!(
        denied.tail(StreamPath::new("conformance/source")?).await,
        Err(StreamError::AccessDenied)
    );
    let bounded = HttpStream::new(endpoint, "conformance", 1)?;
    assert_eq!(
        bounded.tail(StreamPath::new("conformance/source")?).await,
        Err(StreamError::Unavailable)
    );
    Ok(())
}
