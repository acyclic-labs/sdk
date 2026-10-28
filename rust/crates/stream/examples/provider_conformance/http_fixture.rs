//! Credential and bounded-response checks for the local HTTPS fixture only.
use acyclic_stream::{StreamError, StreamPath, StreamProvider, http::HttpStream};

pub async fn verify(endpoint: &str) -> Result<(), Box<dyn std::error::Error>> {
    verify_commit_only(endpoint).await?;
    let ca = std::env::var("ACYCLIC_CONFORMANCE_CA")
        .ok()
        .map(std::fs::read)
        .transpose()?;
    let denied = HttpStream::with_ca_certificate(endpoint, "wrong", 1024, ca.as_deref())?;
    assert_eq!(
        denied.tail(StreamPath::new("conformance/source")?).await,
        Err(StreamError::AccessDenied)
    );
    let bounded = HttpStream::with_ca_certificate(endpoint, "conformance", 1, ca.as_deref())?;
    assert_eq!(
        bounded.tail(StreamPath::new("conformance/source")?).await,
        Err(StreamError::Unavailable)
    );
    Ok(())
}

async fn verify_commit_only(endpoint: &str) -> Result<(), Box<dyn std::error::Error>> {
    let ca = std::env::var("ACYCLIC_CONFORMANCE_CA")
        .ok()
        .map(std::fs::read)
        .transpose()?;
    let commit_only =
        HttpStream::with_ca_certificate(endpoint, "commit-only", 16 * 1024 * 1024, ca.as_deref())?;
    let commit_only_request = acyclic_stream::CommitRequest {
        conditions: vec![acyclic_stream::CommitCondition::Absent {
            path: StreamPath::new("conformance/commit-only")?,
        }],
        mutations: vec![acyclic_stream::CommitMutation::Append {
            path: StreamPath::new("conformance/commit-only")?,
            records: vec![acyclic_stream::OwnedBytes::from_static(b"admitted").into()],
        }],
        idempotency_key: acyclic_stream::IdempotencyKey::new(
            acyclic_stream::OwnedBytes::from_static(b"commit-only"),
        )?,
        deadline_unix_millis: None,
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
