//! Provider-facing canonical validation and retry digests.
//!
//! These checks are independent of provider state. Providers must still check authorization,
//! conditions, source existence, capacity and caller deadlines at their own atomic authority.
use crate::{AppendRequest, CommitRequest, ForkRequest, StreamError, memory};

/// Validates an append and returns the canonical retry digest.
/// The retry identity itself is excluded from the digest.
pub fn append_digest(request: &AppendRequest) -> Result<[u8; 32], StreamError> {
    memory::validate_records(&request.records)?;
    memory::validate_append_size(request)?;
    Ok(memory::append_digest(request))
}

/// Validates a fork and returns its canonical retry digest.
/// Resolve an omitted cut only after replay lookup, at the provider's atomic authority.
pub fn fork_digest(request: &ForkRequest) -> Result<[u8; 32], StreamError> {
    if request.source == request.destination {
        return Err(StreamError::InvalidArgument);
    }
    memory::validate_fork_size(request)?;
    Ok(memory::fork_digest(request))
}

/// Normalizes and validates a coordinated commit, then returns its canonical retry digest.
/// Sorts participants and mutations in place and rejects duplicate paths or missing conditions.
/// A caller deadline is not part of operation identity and must be checked separately.
pub fn commit_digest(request: &mut CommitRequest) -> Result<[u8; 32], StreamError> {
    memory::normalize_commit(request)?;
    memory::validate_commit_shape(request)?;
    Ok(memory::commit_digest(request))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{IdempotencyKey, IdempotencyObservation, MemoryStream, StreamPath, StreamProvider};
    use bytes::Bytes;

    #[tokio::test]
    async fn provider_digest_matches_retained_observation() -> Result<(), StreamError> {
        let key = IdempotencyKey::new(Bytes::from_static(b"provider-digest"))?;
        let request = AppendRequest {
            path: StreamPath::new("digest/events")?,
            records: vec![Bytes::from_static(b"record")],
            if_tail: Some(0),
            idempotency_key: Some(key.clone()),
        };
        let digest = append_digest(&request)?;
        let provider = MemoryStream::default();
        provider.append(request).await?;
        let Some(IdempotencyObservation { request_digest, .. }) =
            provider.inspect_idempotency(key).await?
        else {
            return Err(StreamError::Unavailable);
        };
        assert_eq!(digest, request_digest);
        Ok(())
    }
}
