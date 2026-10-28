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
    let mut canonical = Vec::new();
    canonical
        .try_reserve_exact(paths.len())
        .map_err(|_| StreamError::LimitExceeded)?;
    canonical.extend_from_slice(paths);
    normalize_exclusions(&mut canonical)?;
    Ok(canonical)
}
fn normalize_exclusions(paths: &mut Vec<crate::RelativePath>) -> Result<(), StreamError> {
    if paths.len() > MAX_ITEMS {
        return Err(StreamError::LimitExceeded);
    }
    paths.sort_unstable();
    paths.dedup();
    let mut kept = 0;
    for index in 0..paths.len() {
        let mut parent = paths[index].parent();
        let mut covered = false;
        while let Some(ancestor) = parent {
            if paths[..kept].binary_search(&ancestor).is_ok() {
                covered = true;
                break;
            }
            parent = ancestor.parent();
        }
        if !covered {
            paths.swap(kept, index);
            kept += 1;
        }
    }
    paths.truncate(kept);
    Ok(())
}
fn hash_exclusions(hash: &mut Sha256, paths: &[crate::RelativePath]) -> Result<(), StreamError> {
    let canonical = canonical_exclusions(paths)?;
    hash_canonical_exclusions(hash, &canonical);
    Ok(())
}
fn hash_canonical_exclusions(hash: &mut Sha256, paths: &[crate::RelativePath]) {
    hash.update((paths.len() as u64).to_le_bytes());
    for path in paths {
        hash_path(hash, path);
    }
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
    commit_binding_with(request, |_| Ok::<(), StreamError>(()))?.map(|binding| binding.digest())
}

/// Retains only the canonical conditions hash state, never a second record copy.
pub struct CommitBinding {
    conditions: Sha256,
    digest: [u8; 32],
}

impl CommitBinding {
    /// The normalized request's retry identity.
    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }

    /// Joins actual retained immutable facts to the originally submitted body.
    /// Admits their exact hash work before hashing their values and headers.
    /// The outer error preserves work refusal; the inner error is malformed facts.
    pub fn matches_with<E>(
        &self,
        envelope: &crate::CommittedEnvelope,
        admit: impl FnOnce(u64) -> Result<(), E>,
    ) -> Result<Result<bool, StreamError>, E> {
        if let Err(error) = crate::persistence::validate_envelope(envelope) {
            return Ok(Err(error));
        }
        let mut bytes = 0;
        for mutation in &envelope.mutations {
            let records = match mutation {
                crate::CommittedMutation::Append(append) => {
                    bytes += 1 + path_hash_bytes(&append.path);
                    &append.records
                }
                crate::CommittedMutation::Fork(fork) => {
                    bytes += 1
                        + path_hash_bytes(&fork.source)
                        + path_hash_bytes(&fork.destination)
                        + 8
                        + 8
                        + fork.exclude.iter().map(path_hash_bytes).sum::<u64>();
                    &fork.records
                }
            };
            bytes += 8 + records
                .iter()
                .map(|record| 8 + record.value.len() as u64 + record.headers.hash_bytes())
                .sum::<u64>();
        }
        admit(bytes)?;
        let mut hash = self.conditions.clone();
        for mutation in &envelope.mutations {
            let records = match mutation {
                crate::CommittedMutation::Append(append) => {
                    hash.update([1]);
                    hash_path(&mut hash, &append.path);
                    &append.records
                }
                crate::CommittedMutation::Fork(fork) => {
                    hash.update([2]);
                    hash_path(&mut hash, &fork.source);
                    hash_path(&mut hash, &fork.destination);
                    hash.update(fork.forked_at.to_le_bytes());
                    hash_canonical_exclusions(&mut hash, &fork.exclude);
                    &fork.records
                }
            };
            hash.update((records.len() as u64).to_le_bytes());
            for record in records {
                hash_record(&mut hash, &record.value, &record.headers);
            }
        }
        Ok(Ok(<[u8; 32]>::from(hash.finalize()) == self.digest))
    }
}

/// Prepares canonical retry/body binding with caller-admitted hash work.
/// The outer error preserves work refusal; the inner error is an invalid request.
pub fn commit_binding_with<E>(
    request: &mut CommitRequest,
    admit: impl FnOnce(u64) -> Result<(), E>,
) -> Result<Result<CommitBinding, StreamError>, E> {
    if let Err(error) = validate_commit(request) {
        return Ok(Err(error));
    }
    admit(commit_hash_bytes(request))?;
    let mut hash = request_hasher(b"commit");
    hash_conditions(&mut hash, &request.conditions);
    let conditions = hash.clone();
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
                hash_canonical_exclusions(&mut hash, exclude);
                hash_records(&mut hash, records);
            }
        }
    }
    Ok(Ok(CommitBinding {
        conditions,
        digest: hash.finalize().into(),
    }))
}

/// Normalizes a commit and admits its shape and complete encoded command size.
/// Callers that only prepare a request need not hash all record bytes again.
pub fn validate_commit(request: &mut CommitRequest) -> Result<(), StreamError> {
    normalize_commit(request)?;
    validate_commit_shape(request)
}

pub(crate) fn hash_conditions(hash: &mut Sha256, conditions: &[CommitCondition]) {
    for condition in conditions {
        match condition {
            CommitCondition::Present { path } => {
                hash.update([4]);
                hash_path(hash, path);
            }
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

fn path_hash_bytes(path: &StreamPath) -> u64 {
    8 + path.as_str().len() as u64
}

fn records_hash_bytes(records: &[AppendRecord]) -> u64 {
    8 + records.iter().map(AppendRecord::hash_bytes).sum::<u64>()
}

pub(crate) fn commit_hash_bytes(request: &CommitRequest) -> u64 {
    b"acyclic.stream.request.v1\0".len() as u64
        + b"commit".len() as u64
        + request
            .conditions
            .iter()
            .map(|condition| match condition {
                CommitCondition::Head { path, .. } => 1 + path_hash_bytes(path) + 8 + 32,
                CommitCondition::Tail { path, .. } => 1 + path_hash_bytes(path) + 8,
                CommitCondition::Absent { path } | CommitCondition::Present { path } => {
                    1 + path_hash_bytes(path)
                }
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
        hash_record(hash, &record.value, &record.headers);
    }
}

fn hash_record(hash: &mut Sha256, value: &[u8], headers: &crate::RecordHeaders) {
    hash.update((value.len() as u64).to_le_bytes());
    hash.update(value);
    hash.update((headers.keys().len() as u64).to_le_bytes());
    for key in headers.keys() {
        hash.update((key.len() as u64).to_le_bytes());
        hash.update(key);
    }
    hash.update((headers.attributes().len() as u64).to_le_bytes());
    for attribute in headers.attributes() {
        hash.update((attribute.name.len() as u64).to_le_bytes());
        hash.update(attribute.name.as_bytes());
        hash.update((attribute.value.len() as u64).to_le_bytes());
        hash.update(attribute.value);
    }
}

pub(crate) fn condition_path(condition: &CommitCondition) -> &StreamPath {
    match condition {
        CommitCondition::Tail { path, .. }
        | CommitCondition::Head { path, .. }
        | CommitCondition::Absent { path }
        | CommitCondition::Present { path } => path,
    }
}

pub(crate) fn mutation_path(mutation: &CommitMutation) -> &StreamPath {
    match mutation {
        CommitMutation::Fork { destination, .. } => destination,
        CommitMutation::Append { path, .. } => path,
    }
}

pub(crate) fn normalize_commit(request: &mut CommitRequest) -> Result<(), StreamError> {
    if request.conditions.len() > MAX_ITEMS
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
            normalize_exclusions(exclude)?;
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
    async fn recovery_binding_refuses_changed_facts_and_unfunded_hash_work()
    -> Result<(), StreamError> {
        let provider = MemoryStream::default();
        let path = StreamPath::new("binding/events")?;
        let mut request = CommitRequest {
            deadline_unix_millis: None,
            idempotency_key: IdempotencyKey::new(Bytes::from_static(b"body-binding"))?,
            conditions: vec![CommitCondition::Absent { path: path.clone() }],
            mutations: vec![CommitMutation::Append {
                path,
                records: vec![Bytes::from_static(b"original").into()],
            }],
        };
        let binding = commit_binding_with(&mut request, |_| Ok::<(), StreamError>(()))??;
        let crate::CommitOutcome::Committed(receipt) = provider.commit(request).await? else {
            return Err(StreamError::Unavailable);
        };
        let mut facts = provider.read_commit(receipt.commit_id).await?;
        let mut charged = 0;
        assert!(binding.matches_with(&facts, |bytes| {
            charged = bytes;
            Ok::<(), StreamError>(())
        })??);
        assert!(charged > 0);
        assert_eq!(
            binding.matches_with(&facts, |_| Err::<(), _>(StreamError::LimitExceeded)),
            Err(StreamError::LimitExceeded)
        );
        let crate::CommittedMutation::Append(append) = &mut facts.mutations[0] else {
            return Err(StreamError::InvalidArgument);
        };
        // Keep the selected range, identity, time and sequence valid. The actual
        // immutable body must still be joined to the original request digest.
        append.records[0].value = Bytes::from_static(b"corrupted");
        assert!(!binding.matches_with(&facts, |_| Ok::<(), StreamError>(()))??);
        Ok(())
    }

    #[tokio::test]
    async fn provider_digest_matches_retained_observation() -> Result<(), StreamError> {
        let key = IdempotencyKey::new(Bytes::from_static(b"provider-digest"))?;
        let request = AppendRequest {
            path: StreamPath::new("digest/events")?,
            records: vec![Bytes::from_static(b"record").into()],
            if_tail: Some(0),
            idempotency_key: key.clone(),
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
