//! Canonical request validation and retry identity.
use crate::AppendRecord;
use crate::{
    AppendRequest, CommitCondition, CommitEffect, CommitMutation, CommitRequest, ForkRequest,
    MAX_COMMAND_BYTES, MAX_ITEMS, MAX_RECORD_BYTES, StreamError, StreamPath,
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
    validate_commit(request)?;
    // The digest alone retains nothing, so it allocates nothing.
    Ok(canonical_digest(request))
}

/// Hashes a request whose shape was admitted by the shared canonicalizer.
pub(crate) fn canonical_digest(request: &CommitRequest) -> [u8; 32] {
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
        if let Err(error) = crate::persistence::validate_envelope(envelope) {
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
pub(crate) fn hard_append_digest(
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

const REQUEST_DOMAIN: &[u8] = b"acyclic.stream.request.v1\0";

pub(crate) fn request_hasher(kind: &[u8]) -> Sha256 {
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

pub(crate) fn commit_hash_bytes(request: &CommitRequest) -> u64 {
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

/// The path a committed fact publishes: the key of its canonical order.
pub(crate) fn fact_path(fact: &crate::CommittedMutation) -> &StreamPath {
    match fact {
        crate::CommittedMutation::Append(append) => &append.path,
        crate::CommittedMutation::Fork(fork) => &fork.destination,
    }
}

/// Selects conditions for ascending mutation paths within one canonical effect.
/// Each comparison consumes a condition or completes one query; no lookup
/// searches conditions that an earlier mutation already passed.
pub(crate) fn effect_conditions<'a>(
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
pub(crate) fn normalize_commit(request: &mut CommitRequest) -> Result<(), StreamError> {
    normalize_commit_with(request, ordinary_index)
}

fn ordinary_index(length: usize) -> Result<(Vec<MutationIndex>, ()), StreamError> {
    let mut index = Vec::new();
    index
        .try_reserve_exact(length)
        .map_err(|_| StreamError::Capacity)?;
    Ok((index, ()))
}

/// The caller owns admission for the only cross-effect scratch allocation.
pub(crate) fn normalize_commit_with<A>(
    request: &mut CommitRequest,
    allocate: impl FnOnce(usize) -> Result<(Vec<MutationIndex>, A), StreamError>,
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
pub(crate) struct MutationIndex {
    effect: usize,
    mutation: usize,
}

struct MutationView<'a, A> {
    request: &'a CommitRequest,
    single: Option<&'a [CommitMutation]>,
    index: Vec<MutationIndex>,
    // Drop the physical backing before retiring its original credit.
    _allocation: Option<A>,
}

impl<'a, A> MutationView<'a, A> {
    fn new(
        request: &'a CommitRequest,
        allocate: impl FnOnce(usize) -> Result<(Vec<MutationIndex>, A), StreamError>,
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
        let (index, allocation) = allocate(request.mutations().count())?;
        let mut view = Self {
            request,
            single: None,
            index,
            _allocation: Some(allocation),
        };
        for (effect, group) in request.effects.iter().enumerate() {
            view.index.extend(
                (0..group.mutations.len()).map(|mutation| MutationIndex { effect, mutation }),
            );
        }
        let path = |position: &MutationIndex| {
            CommitMutation::path(&request.effects[position.effect].mutations[position.mutation])
        };
        view.index
            .sort_unstable_by(|left, right| path(left).cmp(path(right)));
        if view
            .index
            .windows(2)
            .any(|pair| path(&pair[0]) == path(&pair[1]))
        {
            return Err(StreamError::InvalidArgument);
        }
        Ok(view)
    }

    fn len(&self) -> usize {
        self.single
            .map_or(self.index.len(), <[CommitMutation]>::len)
    }

    fn at(&self, position: usize) -> &CommitMutation {
        match self.single {
            Some(mutations) => &mutations[position],
            None => {
                let index = &self.index[position];
                &self.request.effects[index.effect].mutations[index.mutation]
            }
        }
    }

    fn descendants<'b>(
        &'b self,
        destination: &'b StreamPath,
    ) -> impl Iterator<Item = &'b str> + 'b {
        // Seek the segment prefix itself. Text such as "a." sorts between "a"
        // and "a/", so seeking after the parent would miss its descendants.
        let mut low = 0;
        let mut high = self.len();
        while low < high {
            let middle = low + (high - low) / 2;
            let path = CommitMutation::path(self.at(middle)).as_str();
            let ordering = path
                .bytes()
                .cmp(destination.as_str().bytes().chain(std::iter::once(b'/')));
            if ordering.is_lt() {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        // Own only the offset into the borrowed destination, not its lifetime.
        let prefix_len = destination.as_str().len();
        let prefix = destination.as_str();
        (low..self.len())
            .map(move |position| CommitMutation::path(self.at(position)).as_str())
            .take_while(move |path| {
                path.strip_prefix(prefix)
                    .is_some_and(|suffix| suffix.starts_with('/'))
            })
            .map(move |path| &path[prefix_len + 1..])
    }
}

/// Encoded command bytes, using the same wire encoder as request validation.
#[must_use]
pub fn commit_encoded_len(request: &CommitRequest) -> usize {
    crate::wire_codec::requests::commit_len(request)
}

fn validate_commit_size(request: &CommitRequest) -> Result<(), StreamError> {
    command_size(commit_encoded_len(request))
}

#[cfg(test)]
pub(crate) fn validate_commit_shape_with(
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
                || view.descendants(destination).any(|relative| {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OwnedBytes as Bytes;
    use crate::StreamProviderExt as _;
    use crate::{IdempotencyKey, IdempotencyObservation, MemoryStream, StreamPath, StreamProvider};

    #[test]
    fn cross_effect_forks_validate_segment_ranges_among_empty_guards() -> Result<(), StreamError> {
        let append = |name: &str| -> Result<CommitMutation, StreamError> {
            Ok(CommitMutation::Append {
                path: StreamPath::new(name)?,
                records: vec![Bytes::from_static(b"record").into()],
            })
        };
        let parent = StreamPath::new("a")?;
        let mut request = CommitRequest {
            deadline_unix_millis: None,
            idempotency_key: IdempotencyKey::new(Bytes::from_static(b"fork-segment-range"))?,
            effects: vec![CommitEffect::hard(
                vec![CommitCondition::Absent {
                    path: parent.clone(),
                }],
                vec![CommitMutation::Fork {
                    source: StreamPath::new("source")?,
                    destination: parent,
                    at_tail: 0,
                    records: Vec::new(),
                    exclude: vec![crate::RelativePath::new("child")?],
                }],
            )],
        };
        // Guard-only effects retain request order without contributing paths.
        for _ in 0..257 {
            request.effects.push(CommitEffect::hard(
                vec![CommitCondition::Absent {
                    path: StreamPath::new("guard")?,
                }],
                Vec::new(),
            ));
        }
        request.effects.push(CommitEffect::hard(
            Vec::new(),
            vec![append("a.")?, append("a/child")?],
        ));
        validate_commit(&mut request)?;
        request
            .effects
            .last_mut()
            .ok_or(StreamError::InvalidArgument)?
            .mutations
            .push(append("a/child/deeper")?);
        assert_eq!(
            validate_commit(&mut request),
            Err(StreamError::InvalidArgument)
        );
        request
            .effects
            .last_mut()
            .ok_or(StreamError::InvalidArgument)?
            .mutations
            .pop();
        request.effects.push(CommitEffect {
            soft: true,
            conditions: Vec::new(),
            mutations: vec![append("a/child")?],
        });
        assert_eq!(
            validate_commit(&mut request),
            Err(StreamError::InvalidArgument)
        );
        Ok(())
    }

    #[tokio::test]
    async fn recovery_binding_refuses_changed_facts_and_unfunded_hash_work()
    -> Result<(), StreamError> {
        let provider = MemoryStream::default();
        let path = StreamPath::new("binding/events")?;
        let mut request = CommitRequest {
            deadline_unix_millis: None,
            idempotency_key: IdempotencyKey::new(Bytes::from_static(b"body-binding"))?,
            effects: vec![CommitEffect::hard(
                vec![CommitCondition::Absent { path: path.clone() }],
                vec![CommitMutation::Append {
                    path,
                    records: vec![Bytes::from_static(b"original").into()],
                }],
            )],
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

    #[test]
    fn effect_boundaries_and_soft_flags_are_part_of_the_digest() -> Result<(), StreamError> {
        let append = |name: &str| -> Result<CommitMutation, StreamError> {
            Ok(CommitMutation::Append {
                path: StreamPath::new(name)?,
                records: vec![Bytes::from_static(b"x").into()],
            })
        };
        let present = CommitCondition::Present {
            path: StreamPath::new("to")?,
        };
        let request = |effects| -> Result<CommitRequest, StreamError> {
            Ok(CommitRequest {
                deadline_unix_millis: None,
                effects,
                idempotency_key: IdempotencyKey::new(Bytes::from_static(b"grouping"))?,
            })
        };
        let together = request(vec![CommitEffect::hard(
            vec![present.clone()],
            vec![append("a")?, append("b")?],
        )])?;
        let split = request(vec![
            CommitEffect::hard(vec![present.clone()], vec![append("a")?]),
            CommitEffect::hard(Vec::new(), vec![append("b")?]),
        ])?;
        let mut soft = together.clone();
        soft.effects[0].soft = true;
        let digests = [together, split, soft]
            .map(|mut request| commit_digest(&mut request))
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        assert!(digests[0] != digests[1] && digests[0] != digests[2] && digests[1] != digests[2]);
        Ok(())
    }
}
