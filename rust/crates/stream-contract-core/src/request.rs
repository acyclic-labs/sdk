//! Canonical request validation and retry identity.
use crate::AppendRecord;
use crate::{
    AppendRequest, CommitCondition, CommitEffect, CommitMutation, CommitRequest, ForkRequest,
    MAX_COMMAND_BYTES, MAX_ITEMS, MAX_RECORD_BYTES, StreamError, StreamPath,
};
use sha2::{Digest, Sha256};

/// The canonical journal command and its validation.
pub mod command;
pub mod encode;
pub mod len;
pub mod validation;

/// Admits a non-empty record list within the item and record limits.
pub fn validate_records(records: &[AppendRecord]) -> Result<(), StreamError> {
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

/// Admits an append's encoded command size.
pub fn validate_append_size(request: &AppendRequest) -> Result<(), StreamError> {
    command_size(len::append_len(request))
}
/// Admits a fork's encoded command size.
pub fn validate_fork_size(request: &ForkRequest) -> Result<(), StreamError> {
    command_size(len::fork_len(request))
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
pub fn canonical_exclusions(
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
        let path = paths.get(index).ok_or(StreamError::InvalidArgument)?;
        let kept_paths = paths.get(..kept).ok_or(StreamError::InvalidArgument)?;
        let covered = path.ancestors_including_self().skip(1).any(|ancestor| {
            kept_paths
                .binary_search_by(|candidate| candidate.as_str().cmp(ancestor))
                .is_ok()
        });
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

/// Admits a fork that does not name one path twice.
pub fn validate_fork(request: &ForkRequest) -> Result<(), StreamError> {
    if request.source == request.destination {
        return Err(StreamError::InvalidArgument);
    }
    validate_fork_size(request)
}

/// Validates a fork and returns its canonical retry digest.
pub fn fork_digest(request: &ForkRequest) -> Result<[u8; 32], StreamError> {
    validate_fork(request)?;
    let mut hash = request_hasher(b"fork");
    hash_path(&mut hash, &request.source);
    hash_path(&mut hash, &request.destination);
    hash.update(request.at_tail.unwrap_or(u64::MAX).to_le_bytes());
    hash_exclusions(&mut hash, &request.exclude)?;
    Ok(hash.finalize().into())
}

/// Normalizes and validates a commit, then returns its canonical retry digest.
pub fn commit_digest(request: &mut CommitRequest) -> Result<[u8; 32], StreamError> {
    validate_commit(request)?;
    // The digest alone retains nothing, so it allocates nothing.
    Ok(canonical_digest(request))
}

/// Hashes a request whose shape was admitted by the shared canonicalizer.
pub fn canonical_digest(request: &CommitRequest) -> [u8; 32] {
    hash_commit(request, |_, _| ())
}

/// Retains, per effect, its header hash state, its digest and its mutation
/// paths: enough to join retained facts to the submitted body without the
/// request, and never a record copy.
pub struct CommitBinding {
    effects: Vec<EffectBinding>,
    /// Every effect's mutation paths, effect by effect, each in canonical order.
    paths: Vec<StreamPath>,
    digest: [u8; 32],
}

/// One effect's identity. A skipped effect leaves no facts, so only this
/// binding remains of it.
struct EffectBinding {
    /// Hash state after the effect's soft flag, conditions and mutation count.
    header: Sha256,
    /// The effect's digest over its submitted mutations.
    digest: [u8; 32],
    soft: bool,
    mutations: usize,
}

impl CommitBinding {
    /// The normalized request's retry identity.
    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }

    /// Joins actual retained immutable facts to the originally submitted body.
    /// Admits their exact hash work before hashing their values and headers.
    /// An effect applied iff its facts exist; a skipped soft effect has none.
    /// The outer error preserves work refusal; the inner error is malformed facts.
    pub fn matches_with<E>(
        &self,
        envelope: &crate::CommittedEnvelope,
        admit: impl FnOnce(u64) -> Result<(), E>,
    ) -> Result<Result<bool, StreamError>, E> {
        if let Err(error) = validation::validate_envelope(envelope) {
            return Ok(Err(error));
        }
        admit(envelope.mutations.iter().map(fact_hash_bytes).sum())?;
        let facts = envelope.mutations.as_slice();
        let fact = |path: &StreamPath| {
            facts
                .binary_search_by(|fact| fact_path(fact).cmp(path))
                .ok()
                .and_then(|index| facts.get(index))
        };
        let mut paths = self.paths.as_slice();
        let mut joined = 0_usize;
        for effect in &self.effects {
            let Some((own, rest)) = paths.split_at_checked(effect.mutations) else {
                return Ok(Ok(false));
            };
            paths = rest;
            // A guard has no facts to find; it applied with its commit.
            if own.first().is_some_and(|path| fact(path).is_none()) {
                // All or none: a skipped effect leaves no fact behind.
                if !effect.soft || own.iter().any(|path| fact(path).is_some()) {
                    return Ok(Ok(false));
                }
                continue;
            }
            let mut hash = effect.header.clone();
            for path in own {
                let Some(found) = fact(path) else {
                    return Ok(Ok(false));
                };
                hash_fact(&mut hash, found);
                joined += 1;
            }
            if <[u8; 32]>::from(hash.finalize()) != effect.digest {
                return Ok(Ok(false));
            }
        }
        Ok(Ok(joined == facts.len()))
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
    let mut effects = Vec::with_capacity(request.effects.len());
    let mut paths = Vec::with_capacity(request.mutations().count());
    let digest = hash_commit(request, |effect, (header, digest)| {
        paths.extend(
            effect
                .mutations
                .iter()
                .map(|m| crate::CommitMutation::path(m).clone()),
        );
        effects.push(EffectBinding {
            header: header.clone(),
            digest,
            soft: effect.soft,
            mutations: effect.mutations.len(),
        });
    });
    Ok(Ok(CommitBinding {
        effects,
        paths,
        digest,
    }))
}

/// The one canonical commit hash: one digest per effect, in request order,
/// then the request digest over them. Each effect, with its header hash state
/// and digest, goes to `retain`, which a digest-only caller drops.
fn hash_commit(
    request: &CommitRequest,
    mut retain: impl FnMut(&CommitEffect, (&Sha256, [u8; 32])),
) -> [u8; 32] {
    let mut hash = request_hasher(b"commit");
    hash.update((request.effects.len() as u64).to_le_bytes());
    for effect in &request.effects {
        let header = effect_hasher(effect.soft, &effect.conditions, effect.mutations.len());
        let mut body = header.clone();
        for mutation in &effect.mutations {
            hash_mutation(&mut body, mutation);
        }
        let digest: [u8; 32] = body.finalize().into();
        hash.update(digest);
        retain(effect, (&header, digest));
    }
    hash.finalize().into()
}

/// The canonical digest of a commit of one hard append effect, without
/// constructing the request, and its exact hash work.
#[cfg(not(target_arch = "wasm32"))]
pub fn hard_append_digest(
    conditions: &[CommitCondition],
    path: &StreamPath,
    records: &[AppendRecord],
) -> ([u8; 32], u64) {
    let mut effect = effect_hasher(false, conditions, 1);
    hash_append(&mut effect, path, records);
    let mut hash = request_hasher(b"commit");
    hash.update(1_u64.to_le_bytes());
    hash.update(<[u8; 32]>::from(effect.finalize()));
    let bytes =
        commit_frame_bytes(1) + effect_header_bytes(conditions) + append_hash_bytes(path, records);
    (hash.finalize().into(), bytes)
}

fn effect_hasher(soft: bool, conditions: &[CommitCondition], mutations: usize) -> Sha256 {
    let mut hash = request_hasher(b"effect");
    hash.update([u8::from(soft)]);
    hash.update((conditions.len() as u64).to_le_bytes());
    hash_conditions(&mut hash, conditions);
    hash.update((mutations as u64).to_le_bytes());
    hash
}

fn hash_append(hash: &mut Sha256, path: &StreamPath, records: &[AppendRecord]) {
    hash.update([1]);
    hash_path(hash, path);
    hash_records(hash, records);
}

fn hash_mutation(hash: &mut Sha256, mutation: &CommitMutation) {
    match mutation {
        CommitMutation::Append { path, records } => hash_append(hash, path, records),
        CommitMutation::Fork {
            source,
            destination,
            at_tail,
            records,
            exclude,
        } => {
            hash.update([2]);
            hash_path(hash, source);
            hash_path(hash, destination);
            hash.update(at_tail.to_le_bytes());
            hash_canonical_exclusions(hash, exclude);
            hash_records(hash, records);
        }
    }
}

/// Hashes a retained fact exactly as [`hash_mutation`] hashed its mutation.
fn hash_fact(hash: &mut Sha256, fact: &crate::CommittedMutation) {
    let records = match fact {
        crate::CommittedMutation::Append(append) => {
            hash.update([1]);
            hash_path(hash, &append.path);
            &append.records
        }
        crate::CommittedMutation::Fork(fork) => {
            hash.update([2]);
            hash_path(hash, &fork.source);
            hash_path(hash, &fork.destination);
            hash.update(fork.forked_at.to_le_bytes());
            hash_canonical_exclusions(hash, &fork.exclude);
            &fork.records
        }
    };
    hash.update((records.len() as u64).to_le_bytes());
    for record in records {
        hash_record(hash, &record.value, &record.headers);
    }
}

fn fact_hash_bytes(fact: &crate::CommittedMutation) -> u64 {
    let (head, records) = match fact {
        crate::CommittedMutation::Append(append) => {
            (1 + path_hash_bytes(&append.path), &append.records)
        }
        crate::CommittedMutation::Fork(fork) => (
            1 + path_hash_bytes(&fork.source)
                + path_hash_bytes(&fork.destination)
                + 8
                + 8
                + fork.exclude.iter().map(path_hash_bytes).sum::<u64>(),
            &fork.records,
        ),
    };
    head + 8
        + records
            .iter()
            .map(|record| 8 + record.value.len() as u64 + record.headers.hash_bytes())
            .sum::<u64>()
}

/// Normalizes a commit and admits its shape and complete encoded command size.
/// Callers that only prepare a request need not hash all record bytes again.
pub fn validate_commit(request: &mut CommitRequest) -> Result<(), StreamError> {
    normalize_commit(request)
}

/// Hashes conditions in their canonical order.
pub fn hash_conditions(hash: &mut Sha256, conditions: &[CommitCondition]) {
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

const REQUEST_DOMAIN: &[u8] = b"acyclic.stream.request.v1\0";

/// A hasher already bound to the request domain and `kind`.
pub fn request_hasher(kind: &[u8]) -> Sha256 {
    let mut hash = Sha256::new();
    hash.update(REQUEST_DOMAIN);
    hash.update(kind);
    hash
}

fn path_hash_bytes(path: &StreamPath) -> u64 {
    8 + path.as_str().len() as u64
}

fn records_hash_bytes(records: &[AppendRecord]) -> u64 {
    8 + records.iter().map(AppendRecord::hash_bytes).sum::<u64>()
}

/// Binds an original publication fence to one borrowed record with empty headers.
/// Admits the exact hash work without constructing or publishing another request.
/// This identity establishes neither authority nor record admission.
pub fn conditioned_record_digest_with<E>(
    condition: &CommitCondition,
    value: &[u8],
    admit: impl FnOnce(u64) -> Result<(), E>,
) -> Result<[u8; 32], E> {
    let domain = b"conditioned-record";
    let headers = crate::RecordHeaders::default();
    admit(
        REQUEST_DOMAIN.len() as u64
            + domain.len() as u64
            + condition_hash_bytes(condition)
            + 8
            + value.len() as u64
            + headers.hash_bytes(),
    )?;
    let mut hash = request_hasher(domain);
    hash_conditions(&mut hash, std::slice::from_ref(condition));
    hash_record(&mut hash, value, &headers);
    Ok(hash.finalize().into())
}

/// Binds an unconditional append's original path and borrowed record.
/// No tail lookup or extra publication is needed to retain its retry identity.
pub fn appended_record_digest_with<E>(
    path: &StreamPath,
    value: &[u8],
    admit: impl FnOnce(u64) -> Result<(), E>,
) -> Result<[u8; 32], E> {
    let domain = b"appended-record";
    let headers = crate::RecordHeaders::default();
    admit(
        REQUEST_DOMAIN.len() as u64
            + domain.len() as u64
            + path_hash_bytes(path)
            + 8
            + value.len() as u64
            + headers.hash_bytes(),
    )?;
    let mut hash = request_hasher(domain);
    hash_path(&mut hash, path);
    hash_record(&mut hash, value, &headers);
    Ok(hash.finalize().into())
}

fn condition_hash_bytes(condition: &CommitCondition) -> u64 {
    match condition {
        CommitCondition::Head { path, .. } => 1 + path_hash_bytes(path) + 8 + 32,
        CommitCondition::Tail { path, .. } => 1 + path_hash_bytes(path) + 8,
        CommitCondition::Absent { path } | CommitCondition::Present { path } => {
            1 + path_hash_bytes(path)
        }
    }
}

/// The request frame: its domain, effect count and one digest per effect.
fn commit_frame_bytes(effects: usize) -> u64 {
    REQUEST_DOMAIN.len() as u64 + b"commit".len() as u64 + 8 + 32 * effects as u64
}

/// One effect's domain, soft flag, conditions and their counts.
fn effect_header_bytes(conditions: &[CommitCondition]) -> u64 {
    REQUEST_DOMAIN.len() as u64
        + b"effect".len() as u64
        + 1
        + 8
        + conditions.iter().map(condition_hash_bytes).sum::<u64>()
        + 8
}

fn append_hash_bytes(path: &StreamPath, records: &[AppendRecord]) -> u64 {
    1 + path_hash_bytes(path) + records_hash_bytes(records)
}

/// Bytes the commit's retry digest hashes, counted without encoding.
pub fn commit_hash_bytes(request: &CommitRequest) -> u64 {
    commit_frame_bytes(request.effects.len())
        + request
            .effects
            .iter()
            .map(|effect| effect_header_bytes(&effect.conditions))
            .sum::<u64>()
        + request
            .mutations()
            .map(|mutation| match mutation {
                CommitMutation::Append { path, records } => append_hash_bytes(path, records),
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

/// Hashes a path's length and bytes.
pub fn hash_path(hash: &mut Sha256, path: &StreamPath) {
    hash.update((path.as_str().len() as u64).to_le_bytes());
    hash.update(path.as_str().as_bytes());
}

/// Hashes a record list's length and each record.
pub fn hash_records(hash: &mut Sha256, records: &[AppendRecord]) {
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

/// The path a committed fact publishes: the key of its canonical order.
pub fn fact_path(fact: &crate::CommittedMutation) -> &StreamPath {
    match fact {
        crate::CommittedMutation::Append(append) => &append.path,
        crate::CommittedMutation::Fork(fork) => &fork.destination,
    }
}

/// Selects conditions for ascending mutation paths within one canonical effect.
/// Each comparison consumes a condition or completes one query; no lookup
/// searches conditions that an earlier mutation already passed.
pub fn effect_conditions<'a>(
    effect: &'a CommitEffect,
) -> impl FnMut(&StreamPath) -> Option<&'a CommitCondition> + 'a {
    effect_conditions_with(effect, || {})
}

fn effect_conditions_with<'a>(
    effect: &'a CommitEffect,
    mut visit: impl FnMut() + 'a,
) -> impl FnMut(&StreamPath) -> Option<&'a CommitCondition> + 'a {
    let mut conditions = effect.conditions.iter().peekable();
    move |path| loop {
        let Some(condition) = conditions.peek().copied() else {
            break None;
        };
        visit();
        match CommitCondition::path(condition).cmp(path) {
            std::cmp::Ordering::Less => {
                conditions.next();
            }
            std::cmp::Ordering::Equal => break conditions.next(),
            std::cmp::Ordering::Greater => break None,
        }
    }
}

/// Sorts each effect's conditions and mutations into canonical path order and
/// refuses a path conditioned twice by one effect or mutated twice by the
/// commit. Effects keep their request order: a receipt names them by index.
pub fn normalize_commit(request: &mut CommitRequest) -> Result<(), StreamError> {
    normalize_commit_with(request, ordinary_index)
}

fn ordinary_index<'a>(length: usize) -> Result<(Vec<MutationIndex<'a>>, ()), StreamError> {
    let mut index = Vec::new();
    index
        .try_reserve_exact(length)
        .map_err(|_| StreamError::Capacity)?;
    Ok((index, ()))
}

/// The caller owns admission for the only cross-effect scratch allocation.
pub fn normalize_commit_with<'a, A>(
    request: &'a mut CommitRequest,
    allocate: impl FnOnce(usize) -> Result<(Vec<MutationIndex<'a>>, A), StreamError>,
) -> Result<(), StreamError> {
    if !crate::preparation::shaped(request)
        || request.conditions().count() > MAX_ITEMS
        || request.mutations().count() > MAX_ITEMS
    {
        return Err(StreamError::LimitExceeded);
    }
    for mutation in request.mutations() {
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
        .conditions()
        .any(|condition| matches!(condition, CommitCondition::Head { expected: 0, .. }))
    {
        return Err(StreamError::InvalidArgument);
    }
    for effect in &mut request.effects {
        for mutation in &mut effect.mutations {
            if let CommitMutation::Fork { exclude, .. } = mutation {
                normalize_exclusions(exclude)?;
            }
        }
        effect.conditions.sort_unstable_by(|left, right| {
            crate::CommitCondition::path(left).cmp(crate::CommitCondition::path(right))
        });
        effect.mutations.sort_unstable_by(|left, right| {
            crate::CommitMutation::path(left).cmp(crate::CommitMutation::path(right))
        });
        if effect.conditions.windows(2).any(
            |pair| matches!(pair, [left, right] if crate::CommitCondition::path(left) == crate::CommitCondition::path(right)),
        ) || effect.mutations.windows(2).any(
            |pair| matches!(pair, [left, right] if crate::CommitMutation::path(left) == crate::CommitMutation::path(right)),
        ) {
            return Err(StreamError::InvalidArgument);
        }
    }
    let view = MutationView::new(request, allocate)?;
    validate_shape(&view, || {})
}

/// Addresses borrow original mutations: no paths, records or payloads are copied.
pub struct MutationIndex<'a> {
    path: &'a str,
}

struct MutationView<'a, A> {
    request: &'a CommitRequest,
    single: Option<&'a [CommitMutation]>,
    index: Vec<MutationIndex<'a>>,
    // Drop the physical backing before retiring its original credit.
    _allocation: Option<A>,
}

enum MutationPaths<'a, 'b> {
    Single(std::slice::Iter<'b, CommitMutation>),
    Indexed(std::slice::Iter<'b, MutationIndex<'a>>),
}

impl<'a, 'b> Iterator for MutationPaths<'a, 'b> {
    type Item = &'b str;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Single(paths) => paths
                .next()
                .map(|mutation| CommitMutation::path(mutation).as_str()),
            Self::Indexed(paths) => paths.next().map(|index| index.path),
        }
    }
}

impl<'a, A> MutationView<'a, A> {
    fn new(
        request: &'a CommitRequest,
        allocate: impl FnOnce(usize) -> Result<(Vec<MutationIndex<'a>>, A), StreamError>,
    ) -> Result<Self, StreamError> {
        let mut nonempty = request
            .effects
            .iter()
            .filter(|effect| !effect.mutations.is_empty());
        let single = nonempty.next().map(|effect| effect.mutations.as_slice());
        if nonempty.next().is_none() {
            return Ok(Self {
                request,
                single,
                index: Vec::new(),
                _allocation: None,
            });
        }
        let (mut index, allocation) = allocate(request.mutations().count())?;
        for effect in &request.effects {
            index.extend(effect.mutations.iter().map(|mutation| MutationIndex {
                path: CommitMutation::path(mutation).as_str(),
            }));
        }
        index.sort_unstable_by_key(|entry| entry.path);
        if index
            .windows(2)
            .any(|pair| matches!(pair, [left, right] if left.path == right.path))
        {
            return Err(StreamError::InvalidArgument);
        }
        Ok(Self {
            request,
            single: None,
            index,
            _allocation: Some(allocation),
        })
    }

    fn len(&self) -> usize {
        self.single
            .map_or(self.index.len(), <[CommitMutation]>::len)
    }

    fn path(&self, position: usize) -> Result<&'a str, StreamError> {
        match self.single {
            Some(mutations) => mutations
                .get(position)
                .map(|mutation| CommitMutation::path(mutation).as_str())
                .ok_or(StreamError::InvalidArgument),
            None => self
                .index
                .get(position)
                .map(|index| index.path)
                .ok_or(StreamError::InvalidArgument),
        }
    }

    fn paths_from<'b>(&'b self, low: usize) -> Result<MutationPaths<'a, 'b>, StreamError> {
        match self.single {
            Some(mutations) => Ok(MutationPaths::Single(
                mutations
                    .get(low..)
                    .ok_or(StreamError::InvalidArgument)?
                    .iter(),
            )),
            None => Ok(MutationPaths::Indexed(
                self.index
                    .get(low..)
                    .ok_or(StreamError::InvalidArgument)?
                    .iter(),
            )),
        }
    }

    fn descendants<'b>(
        &'b self,
        destination: &'b StreamPath,
    ) -> Result<impl Iterator<Item = &'b str> + 'b, StreamError> {
        // Seek the segment prefix itself. Text such as "a." sorts between "a"
        // and "a/", so seeking after the parent would miss its descendants.
        let mut low = 0;
        let mut high = self.len();
        while low < high {
            let middle = low + (high - low) / 2;
            let path = self.path(middle)?;
            let ordering = path
                .bytes()
                .cmp(destination.as_str().bytes().chain(std::iter::once(b'/')));
            if ordering.is_lt() {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        let prefix = destination.as_str();
        Ok(self
            .paths_from(low)?
            .filter_map(move |path| path.strip_prefix(prefix))
            .filter_map(|suffix| suffix.strip_prefix('/')))
    }
}

/// Encoded command bytes, using the same wire encoder as request validation.
#[must_use]
pub fn commit_encoded_len(request: &CommitRequest) -> usize {
    len::commit_len(request)
}

fn validate_commit_size(request: &CommitRequest) -> Result<(), StreamError> {
    command_size(commit_encoded_len(request))
}

#[cfg(test)]
/// Admits a commit shape, visiting each condition through `condition_visit`.
pub fn validate_commit_shape_with(
    request: &CommitRequest,
    condition_visit: impl FnMut(),
) -> Result<(), StreamError> {
    let view = MutationView::new(request, ordinary_index)?;
    validate_shape(&view, condition_visit)
}

fn validate_shape<A>(
    view: &MutationView<'_, A>,
    mut condition_visit: impl FnMut(),
) -> Result<(), StreamError> {
    for effect in &view.request.effects {
        let mut condition_for = effect_conditions_with(effect, &mut condition_visit);
        for mutation in &effect.mutations {
            let CommitMutation::Fork {
                source,
                destination,
                exclude,
                ..
            } = mutation
            else {
                continue;
            };
            let condition = condition_for(destination);
            if source == destination
                || !matches!(condition, Some(CommitCondition::Absent { .. }))
                || view.descendants(destination)?.any(|relative| {
                    // Only the exact excluded child can be installed separately.
                    exclude
                        .binary_search_by(|excluded| excluded.as_str().cmp(relative))
                        .is_err()
                })
            {
                return Err(StreamError::InvalidArgument);
            }
        }
    }
    Ok(())
}
