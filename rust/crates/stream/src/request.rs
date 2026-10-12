//! Provider-facing canonical validation and retry digests.
use crate::{AppendRequest, CommitRequest, ForkRequest, StreamError, memory};

/// Validates an append and returns the canonical retry digest.
pub fn append_digest(request: &AppendRequest) -> Result<[u8; 32], StreamError> {
    memory::validate_records(&request.records)?;
    memory::validate_append_size(request)?;
    Ok(memory::append_digest(request, &memory::MemoryObserver::default())
        .expect("ordinary hashing has no fallible observer"))
}

/// Validates a fork and returns its canonical retry digest.
pub fn fork_digest(request: &ForkRequest) -> Result<[u8; 32], StreamError> {
    if request.source == request.destination {
        return Err(StreamError::InvalidArgument);
    }
    memory::validate_fork_size(request)?;
    Ok(memory::fork_digest(request))
}

/// Normalizes and validates a coordinated commit, then returns its canonical retry digest.
pub fn commit_digest(request: &mut CommitRequest) -> Result<[u8; 32], StreamError> {
    memory::normalize_commit(request)?;
    memory::validate_commit_shape(request)?;
    Ok(normalized_commit_digest(request))
}

/// Computes the canonical digest of a pre-normalized, caller-validated request.
#[must_use]
pub fn normalized_commit_digest(request: &CommitRequest) -> [u8; 32] {
    memory::commit_digest(request, &memory::MemoryObserver::default())
        .expect("ordinary hashing has no fallible observer")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{IdempotencyKey, IdempotencyObservation, MemoryStream, StreamPath, StreamProvider};
    use bytes::Bytes;
    use crate::OwnedBytes;

    #[tokio::test]
    async fn provider_digest_matches_retained_observation() -> Result<(), StreamError> {
        let key = IdempotencyKey::new(Bytes::from_static(b"provider-digest"))?;
        let request = AppendRequest {
            path: StreamPath::new("digest/events")?,
            records: vec![OwnedBytes::from_static(b"record")],
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
