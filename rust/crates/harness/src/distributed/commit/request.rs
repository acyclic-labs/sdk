use super::*;

impl CodecQuota<'_, '_> {
    pub(crate) fn reserve<T>(&mut self, capacity: usize, values: &mut Vec<T>) -> Result<()> {
        let old = values.capacity();
        if let Some(io) = self.work().as_deref_mut() {
            crate::store::history::reserve(values, capacity, io)?;
            self.adopt(((values.capacity() - old) * std::mem::size_of::<T>()) as u64)?;
        } else {
            values.try_reserve_exact(capacity.saturating_sub(values.len()))?;
        }
        Ok(())
    }
}

pub(crate) fn commit_request<'a, 'b>(
    key: StreamIdempotencyKey,
    retained_key_bytes: u64,
    work: &'a mut Option<&'b mut dyn HistoryIo>,
) -> (acyclic_stream::CommitRequest, CodecQuota<'a, 'b>) {
    (
        acyclic_stream::CommitRequest {
            conditions: Vec::new(),
            mutations: Vec::new(),
            idempotency_key: key,
        },
        CodecQuota::from_retained(work, retained_key_bytes),
    )
}

pub(crate) fn add_condition(
    (request, custody): &mut (acyclic_stream::CommitRequest, CodecQuota<'_, '_>),
    condition: acyclic_stream::CommitCondition,
) -> Result<()> {
    custody.reserve(request.conditions.len() + 1, &mut request.conditions)?;
    request.conditions.push(condition);
    Ok(())
}

#[cfg(feature = "filesystem")]
pub(crate) fn journal_request<'a, 'b, const FENCE: bool>(
    key: StreamIdempotencyKey,
    retained_key_bytes: u64,
    owner: acyclic_stream::CommitCondition,
    target: acyclic_stream::CommitCondition,
    path: acyclic_stream::StreamPath,
    record: OwnedBytes,
    work: &'a mut Option<&'b mut dyn HistoryIo>,
) -> Result<(acyclic_stream::CommitRequest, CodecQuota<'a, 'b>)> {
    let mut request = commit_request(key, retained_key_bytes, work);
    if FENCE || !cfg!(all(test, acyclic_harness_loom, acyclic_loom)) {
        add_condition(&mut request, owner)?;
    }
    add_condition(&mut request, target)?;
    add_record(&mut request, path, record)?;
    Ok(request)
}

pub(crate) fn add_record(
    (request, custody): &mut (acyclic_stream::CommitRequest, CodecQuota<'_, '_>),
    path: acyclic_stream::StreamPath,
    record: OwnedBytes,
) -> Result<()> {
    custody.reserve(request.mutations.len() + 1, &mut request.mutations)?;
    let mut records = Vec::new();
    custody.reserve(1, &mut records)?;
    records.push(record);
    request
        .mutations
        .push(acyclic_stream::CommitMutation::Append { path, records });
    Ok(())
}

/// Journal dispatch has one finalization boundary after all preparation.
#[cfg(feature = "filesystem")]
pub(crate) async fn commit_journal<P: StreamProvider>(
    request: (acyclic_stream::CommitRequest, CodecQuota<'_, '_>),
    previous_key_bytes: u64,
    client: &Arc<P>,
    operation: OperationId,
) -> Result<(acyclic_stream::CommitOutcome, u64)> {
    commit_journal_impl::<P, true>(request, previous_key_bytes, client, operation).await
}

#[cfg(feature = "filesystem")]
async fn commit_journal_impl<P: StreamProvider, const FINALIZE: bool>(
    mut request: (acyclic_stream::CommitRequest, CodecQuota<'_, '_>),
    previous_key_bytes: u64,
    client: &Arc<P>,
    operation: OperationId,
) -> Result<(acyclic_stream::CommitOutcome, u64)> {
    if FINALIZE || !cfg!(all(acyclic_harness_loom, acyclic_loom)) {
        #[cfg(all(test, not(target_arch = "wasm32")))]
        {
            let measuring = request
                .1
                .work()
                .as_deref()
                .and_then(|work| work.original_native_work())
                .map(crate::filesystem::PreparationIo::from_native_work)
                .transpose()?;
            if request.0.mutations.len() == 2 {
                crate::test_support::accounted::observe_chunk_sync(
                    || finalize_journal_request(&mut request, previous_key_bytes),
                    measuring.as_ref(),
                )?;
            } else {
                finalize_journal_request(&mut request, previous_key_bytes)?;
            }
        }
        #[cfg(not(all(test, not(target_arch = "wasm32"))))]
        finalize_journal_request(&mut request, previous_key_bytes)?;
    }
    super::commit(request, client, Some(operation), None).await
}

/// Finalize once, after every fence and mutation is prepared. The provider's
/// canonical digest includes Head identities as well as all payload bytes.
#[cfg(feature = "filesystem")]
fn finalize_journal_request(
    (request, quota): &mut (acyclic_stream::CommitRequest, CodecQuota<'_, '_>),
    previous_key_bytes: u64,
) -> Result<()> {
    use acyclic_stream::{CommitCondition as C, CommitMutation as M};
    let path_bytes = |path: &acyclic_stream::StreamPath| 8 + path.as_str().len() as u64;
    let record_bytes =
        |records: &[OwnedBytes]| 8 + records.iter().map(|r| 8 + r.len() as u64).sum::<u64>();
    let hashed = b"acyclic.stream.request.v1\0commit".len() as u64
        + request
            .conditions
            .iter()
            .map(|c| match c {
                C::Head { path, .. } => 1 + path_bytes(path) + 8 + 32,
                C::Tail { path, .. } => 1 + path_bytes(path) + 8,
                C::Absent { path } => 1 + path_bytes(path),
            })
            .sum::<u64>()
        + request
            .mutations
            .iter()
            .map(|m| match m {
                M::Append { path, records } => 1 + path_bytes(path) + record_bytes(records),
                M::Fork {
                    source,
                    destination,
                    records,
                    ..
                } => 1 + path_bytes(source) + path_bytes(destination) + 8 + record_bytes(records),
            })
            .sum::<u64>();
    if let Some(work) = quota.work().as_deref_mut() {
        work.hashed_bytes(hashed)?;
    }
    let prepared = acyclic_stream::request::commit_digest(request)
        .map_err(|error| operations::stream_error_with_work(error, quota.work()))?;
    const DOMAIN: &[u8] = b"harness/owned-journal/prepared\0";
    let caller = request.idempotency_key.as_bytes();
    if let Some(work) = quota.work().as_deref_mut() {
        work.hashed_bytes((DOMAIN.len() + 8 + caller.len() + prepared.len()) as u64)?;
    }
    let mut digest = blake3::Hasher::new();
    digest.update(DOMAIN);
    digest.update(&(caller.len() as u64).to_le_bytes());
    digest.update(caller);
    digest.update(&prepared);
    let (key, held) = super::super::digest_key(digest.finalize().as_bytes(), quota.work())?;
    request.idempotency_key = key;
    quota.relinquish(previous_key_bytes)?;
    quota.adopt(held)?;
    Ok(())
}

#[cfg(all(acyclic_harness_loom, acyclic_loom, feature = "filesystem"))]
#[path = "request_model.rs"]
mod model;

#[cfg(all(acyclic_harness_loom, acyclic_loom, feature = "filesystem"))]
pub use model::verify_prepared_journal_identity;

#[cfg(all(
    test,
    feature = "filesystem",
    not(target_arch = "wasm32"),
    not(acyclic_harness_loom)
))]
#[path = "identity_tests.rs"]
mod identity_tests;
