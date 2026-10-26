//! Canonical request validation and retry identity.
use crate::AppendRecord;
use crate::{
    AppendRequest, CommitCondition, CommitMutation, CommitRequest, ForkRequest, MAX_COMMAND_BYTES,
    MAX_ITEMS, MAX_RECORD_BYTES, StreamError, StreamPath,
};
use sha2::{Digest, Sha256};
pub(crate) fn validate_records(records: &[AppendRecord]) -> Result<(), StreamError> {
    if records.is_empty() || records.len() > MAX_ITEMS {
        return Err(StreamError::LimitExceeded);
    }
    for record in records {
        if record.payload_bytes() > MAX_RECORD_BYTES {
            return Err(StreamError::LimitExceeded);
        }
        crate::changes::validate(&record.value, &record.headers)?;
    }
    Ok(())
}

fn command_size(bytes: usize) -> Result<(), StreamError> {
    (bytes <= MAX_COMMAND_BYTES)
        .then_some(())
        .ok_or(StreamError::LimitExceeded)
}

pub(crate) fn validate_append_size(request: &AppendRequest) -> Result<(), StreamError> {
    command_size(crate::wire_codec::requests::append_len(request))
}
pub(crate) fn validate_fork_size(request: &ForkRequest) -> Result<(), StreamError> {
    command_size(crate::wire_codec::requests::fork_len(request))
}

/// Validates an append and returns its canonical retry digest.
pub fn append_digest(request: &AppendRequest) -> Result<[u8; 32], StreamError> {
    validate_records(&request.records)?;
    validate_append_size(request)?;
    let mut hash = request_hasher(b"append");
    hash_path(&mut hash, &request.path);
    hash.update(request.if_tail.unwrap_or(u64::MAX).to_le_bytes());
    hash_records(&mut hash, &request.records);
    Ok(hash.finalize().into())
}

/// Canonical exclusions are segment prefixes, never textual prefixes.
pub(crate) fn canonical_exclusions(
    paths: &[crate::RelativePath],
) -> Result<Vec<crate::RelativePath>, StreamError> {
    if paths.len() > MAX_ITEMS {
        return Err(StreamError::LimitExceeded);
    }
    let all: std::collections::BTreeSet<_> = paths.iter().cloned().collect();
    Ok(all
        .iter()
        .filter(|path| {
            let mut parent = path.parent();
            while let Some(ancestor) = parent {
                if all.contains(&ancestor) {
                    return false;
                }
                parent = ancestor.parent();
            }
            true
        })
        .cloned()
        .collect())
}
fn hash_exclusions(hash: &mut Sha256, paths: &[crate::RelativePath]) -> Result<(), StreamError> {
    let canonical = canonical_exclusions(paths)?;
    hash.update((canonical.len() as u64).to_le_bytes());
    for path in &canonical {
        hash_path(hash, path);
    }
    Ok(())
}

/// Validates a fork and returns its canonical retry digest.
pub fn fork_digest(request: &ForkRequest) -> Result<[u8; 32], StreamError> {
    if request.source == request.destination {
        return Err(StreamError::InvalidArgument);
    }
    validate_fork_size(request)?;
    let mut hash = request_hasher(b"fork");
    hash_path(&mut hash, &request.source);
    hash_path(&mut hash, &request.destination);
    hash.update(request.at_tail.unwrap_or(u64::MAX).to_le_bytes());
    hash_exclusions(&mut hash, &request.exclude)?;
    Ok(hash.finalize().into())
}

/// Normalizes and validates a commit, then returns its canonical retry digest.
pub fn commit_digest(request: &mut CommitRequest) -> Result<[u8; 32], StreamError> {
    normalize_commit(request)?;
    validate_commit_shape(request)?;
    let mut hash = request_hasher(b"commit");
    hash_conditions(&mut hash, &request.conditions);
    for mutation in &mut request.mutations {
        match mutation {
            CommitMutation::Append { path, records } => {
                hash.update([1]);
                hash_path(&mut hash, path);
                hash_records(&mut hash, records);
            }
            CommitMutation::Fork {
                source,
                destination,
                at_tail,
                records,
                exclude,
            } => {
                hash.update([2]);
                hash_path(&mut hash, source);
                hash_path(&mut hash, destination);
                hash.update(at_tail.to_le_bytes());
                hash_exclusions(&mut hash, exclude)?;
                hash_records(&mut hash, records);
            }
        }
    }
    Ok(hash.finalize().into())
}

pub(crate) fn hash_conditions(hash: &mut Sha256, conditions: &[CommitCondition]) {
    for condition in conditions {
        match condition {
            CommitCondition::Head {
                path,
                expected,
                commit_id,
            } => {
                hash.update([3]);
                hash_path(hash, path);
                hash.update(expected.to_le_bytes());
                hash.update(commit_id.as_bytes());
            }
            CommitCondition::Tail { path, expected } => {
                hash.update([1]);
                hash_path(hash, path);
                hash.update(expected.to_le_bytes());
            }
            CommitCondition::Absent { path } => {
                hash.update([2]);
                hash_path(hash, path);
            }
        }
    }
}

pub(crate) fn request_hasher(kind: &[u8]) -> Sha256 {
    let mut hash = Sha256::new();
    hash.update(b"acyclic.stream.request.v1\0");
    hash.update(kind);
    hash
}

#[cfg(not(target_arch = "wasm32"))]
fn path_hash_bytes(path: &StreamPath) -> u64 {
    8 + path.as_str().len() as u64
}

#[cfg(not(target_arch = "wasm32"))]
fn records_hash_bytes(records: &[AppendRecord]) -> u64 {
    8 + records.iter().map(AppendRecord::hash_bytes).sum::<u64>()
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn commit_hash_bytes(request: &CommitRequest) -> u64 {
    b"acyclic.stream.request.v1\0".len() as u64
        + b"commit".len() as u64
        + request
            .conditions
            .iter()
            .map(|condition| match condition {
                CommitCondition::Head { path, .. } => 1 + path_hash_bytes(path) + 8 + 32,
                CommitCondition::Tail { path, .. } => 1 + path_hash_bytes(path) + 8,
                CommitCondition::Absent { path } => 1 + path_hash_bytes(path),
            })
            .sum::<u64>()
        + request
            .mutations
            .iter()
            .map(|mutation| match mutation {
                CommitMutation::Append { path, records } => {
                    1 + path_hash_bytes(path) + records_hash_bytes(records)
                }
                CommitMutation::Fork {
                    source,
                    destination,
                    records,
                    exclude,
                    ..
                } => {
                    8 + exclude.iter().map(path_hash_bytes).sum::<u64>()
                        + 1
                        + path_hash_bytes(source)
                        + path_hash_bytes(destination)
                        + 8
                        + records_hash_bytes(records)
                }
            })
            .sum::<u64>()
}

pub(crate) fn hash_path(hash: &mut Sha256, path: &StreamPath) {
    hash.update((path.as_str().len() as u64).to_le_bytes());
    hash.update(path.as_str().as_bytes());
}

pub(crate) fn hash_records(hash: &mut Sha256, records: &[AppendRecord]) {
    hash.update((records.len() as u64).to_le_bytes());
    for record in records {
        hash.update((record.value.len() as u64).to_le_bytes());
        hash.update(&record.value);
        hash.update((record.headers.keys().len() as u64).to_le_bytes());
        for key in record.headers.keys() {
            hash.update((key.len() as u64).to_le_bytes());
            hash.update(key);
        }
        hash.update((record.headers.attributes().len() as u64).to_le_bytes());
        for attribute in record.headers.attributes() {
            hash.update((attribute.name.len() as u64).to_le_bytes());
            hash.update(attribute.name.as_bytes());
            hash.update((attribute.value.len() as u64).to_le_bytes());
            hash.update(attribute.value);
        }
    }
}

pub(crate) fn condition_path(condition: &CommitCondition) -> &StreamPath {
    match condition {
        CommitCondition::Tail { path, .. }
        | CommitCondition::Head { path, .. }
        | CommitCondition::Absent { path } => path,
    }
}

pub(crate) fn mutation_path(mutation: &CommitMutation) -> &StreamPath {
    match mutation {
        CommitMutation::Fork { destination, .. } => destination,
        CommitMutation::Append { path, .. } => path,
    }
}

pub(crate) fn normalize_commit(request: &mut CommitRequest) -> Result<(), StreamError> {
    if request.conditions.is_empty()
        || request.conditions.len() > MAX_ITEMS
        || request.mutations.is_empty()
        || request.mutations.len() > MAX_ITEMS
    {
        return Err(StreamError::LimitExceeded);
    }
    for mutation in &request.mutations {
        match mutation {
            CommitMutation::Append { records, .. } => validate_records(records)?,
            CommitMutation::Fork { records, .. } if !records.is_empty() => {
                validate_records(records)?;
            }
            CommitMutation::Fork { .. } => {}
        }
    }
    validate_commit_size(request)?;
    if request
        .conditions
        .iter()
        .any(|condition| matches!(condition, CommitCondition::Head { expected: 0, .. }))
    {
        return Err(StreamError::InvalidArgument);
    }
    for mutation in &mut request.mutations {
        if let CommitMutation::Fork { exclude, .. } = mutation {
            *exclude = canonical_exclusions(exclude)?;
        }
    }
    request.conditions.sort_unstable_by(|left, right| {
        condition_path(left)
            .cmp(condition_path(right))
            .then_with(|| {
                matches!(left, CommitCondition::Absent { .. })
                    .cmp(&matches!(right, CommitCondition::Absent { .. }))
            })
    });
    request
        .mutations
        .sort_unstable_by(|left, right| mutation_path(left).cmp(mutation_path(right)));
    if request
        .conditions
        .windows(2)
        .any(|pair| matches!(pair, [left, right] if condition_path(left) == condition_path(right)))
        || request.mutations.windows(2).any(
            |pair| matches!(pair, [left, right] if mutation_path(left) == mutation_path(right)),
        )
    {
        return Err(StreamError::InvalidArgument);
    }
    Ok(())
}

/// Encoded command bytes, using the same wire encoder as request validation.
#[must_use]
pub fn commit_encoded_len(request: &CommitRequest) -> usize {
    crate::wire_codec::requests::commit_len(request)
}

fn validate_commit_size(request: &CommitRequest) -> Result<(), StreamError> {
    command_size(commit_encoded_len(request))
}

pub(crate) fn validate_commit_shape(request: &CommitRequest) -> Result<(), StreamError> {
    for mutation in &request.mutations {
        if let CommitMutation::Fork {
            source,
            destination,
            ..
        } = mutation
            && (source == destination
                || request.mutations.iter().any(|other| {
                    let path = mutation_path(other).as_str();
                    path.strip_prefix(destination.as_str())
                        .is_some_and(|suffix| suffix.starts_with('/'))
                }))
        {
            return Err(StreamError::InvalidArgument);
        }
        // An append may ride on conditions over other paths (a send guarded only
        // by the recipient's closed marker). A fork must prove its destination absent.
        if matches!(mutation, CommitMutation::Fork { .. }) {
            let condition = request
                .conditions
                .binary_search_by(|condition| {
                    condition_path(condition).cmp(mutation_path(mutation))
                })
                .ok()
                .and_then(|index| request.conditions.get(index));
            if !matches!(condition, Some(CommitCondition::Absent { .. })) {
                return Err(StreamError::InvalidArgument);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OwnedBytes as Bytes;
    use crate::{IdempotencyKey, IdempotencyObservation, MemoryStream, StreamPath, StreamProvider};

    #[tokio::test]
    async fn provider_digest_matches_retained_observation() -> Result<(), StreamError> {
        let key = IdempotencyKey::new(Bytes::from_static(b"provider-digest"))?;
        let request = AppendRequest {
            path: StreamPath::new("digest/events")?,
            records: vec![Bytes::from_static(b"record").into()],
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
