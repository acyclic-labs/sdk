//! Filesystem storage bindings over the canonical public service providers.

use crate::cancellation::CancellationToken;
use crate::foundation::{
    AuthorityId, Digest, DurableCommit, Epoch, GenerationId, Head, OperationId, ProposedCommit,
    Sequence, authority_commit_digest,
};
use crate::kernel::{decode_published_generation, decode_volume_created, volume_authority_id};
use crate::performance::{OperationFailure, WorkBudget, WorkCounters};
use crate::storage::{
    AppendOutcome as FsAppendOutcome, AuthorityReceipt, AuthorityResult, AuthorityStoreError,
    CreateAuthorityOutcome, FenceOutcome, ObjectId, PublicationPermit, PublicationReservation,
    ReplayLimit, ReservationOutcome,
};
use crate::streams_record::StreamsDurableRecord;
use crate::{AsyncAuthorityStore, WorkspaceForkCommit, WorkspaceForkOutcome};
use bytes::Bytes;
use futures::StreamExt;
use std::sync::Arc;

const STREAM_RECORD_LIMIT: u64 = acyclic_stream::MAX_RECORD_BYTES as u64;
const GENESIS_DOMAIN: &[u8] = b"acyclic-fs-stream-genesis-v1\0";
const EPOCH_DOMAIN: &[u8] = b"acyclic-fs-stream-epoch-v1\0";
const LINEAGE_DOMAIN: &[u8] = b"acyclic-fs-stream-lineage-v1\0";
const LINEAGE_TAIL_DOMAIN: &[u8] = b"acyclic-fs-stream-lineage-tail-v1\0";
const AUTHORITY_MARKER: &[u8] = b"acyclic-fs-authority-v1\0";
const AUTHORITY_RETIRED: &[u8] = b"acyclic-fs-authority-retired-v1\0";
const PUBLICATION_GATE_FREE: &[u8] = b"acyclic-fs-publication-gate-v1\0free";
const PUBLICATION_GATE_ACTIVE: &[u8] = b"acyclic-fs-publication-gate-v1\0active\0";

/// Admission and actual completed-work custody for native fork request production.
pub(crate) struct ForkWork<'a> {
    pub(crate) work: WorkCounters,
    pub(crate) budget: WorkBudget,
    pub(crate) cancellation: &'a CancellationToken,
    pub(crate) allocations: crate::kernel::allocation::AllocationLedger,
}

impl<'a> ForkWork<'a> {
    pub(crate) fn new(budget: WorkBudget, cancellation: &'a CancellationToken) -> Self {
        Self { work: WorkCounters::default(), budget, cancellation, allocations: Default::default() }
    }

    pub(crate) fn check(&self) -> Result<(), OperationFailure<AuthorityStoreError>> {
        self.cancellation.check()
            .map_err(|_| OperationFailure::new(AuthorityStoreError::Cancelled, self.work))?;
        self.work.verify(self.budget)
            .map_err(|error| OperationFailure::new(error.into(), self.work))
    }

    pub(crate) fn admit(&self, delta: WorkCounters) -> Result<(), OperationFailure<AuthorityStoreError>> {
        self.check()?;
        self.work.admit(&delta, &self.budget)
            .map_err(|error| OperationFailure::new(error.into(), self.work))
    }

    pub(crate) fn charge(&mut self, delta: WorkCounters) -> Result<(), OperationFailure<AuthorityStoreError>> {
        self.work.try_add_assign(&delta)
            .map_err(|error| OperationFailure::new(error.into(), self.work))?;
        self.check()
    }

    fn claim(&mut self, bytes: u64, operations: u64) -> Result<(), OperationFailure<AuthorityStoreError>> {
        self.allocations.claim_bytes(bytes, operations, &mut self.work, WorkBudget::UNBOUNDED)
            .map_err(|error| OperationFailure::new(
                error.into_work_or(AuthorityStoreError::Rejected("fork allocation failed".to_owned())), self.work))?;
        self.check()
    }

    pub(crate) fn release(&mut self, bytes: u64) -> Result<(), OperationFailure<AuthorityStoreError>> {
        self.allocations.release(bytes)
            .map_err(|error| OperationFailure::new(
                error.into_work_or(AuthorityStoreError::Rejected("fork allocation custody mismatch".to_owned())), self.work))
    }

    pub(crate) fn release_request(&mut self) -> Result<(), OperationFailure<AuthorityStoreError>> {
        self.release(self.allocations.live_bytes())
    }

    pub(crate) fn vec<T>(&mut self, capacity: usize) -> Result<Vec<T>, OperationFailure<AuthorityStoreError>> {
        self.check()?;
        let bytes = capacity.checked_mul(std::mem::size_of::<T>())
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| OperationFailure::new(crate::WorkError::Overflow.into(), self.work))?;
        self.admit(WorkCounters {
            allocation_operations: u64::from(bytes != 0),
            peak_allocation_bytes: self.allocations.live_bytes().checked_add(bytes)
                .ok_or_else(|| OperationFailure::new(crate::WorkError::Overflow.into(), self.work))?,
            ..WorkCounters::default()
        })?;
        let mut value = Vec::new();
        value.try_reserve_exact(capacity).map_err(|_| OperationFailure::new(
            AuthorityStoreError::Rejected("fork allocation failed".to_owned()), self.work))?;
        let actual = value.capacity().checked_mul(std::mem::size_of::<T>())
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| OperationFailure::new(crate::WorkError::Overflow.into(), self.work))?;
        self.claim(actual, u64::from(actual != 0))?;
        Ok(value)
    }

    pub(crate) fn bytes(&mut self, domain: &[u8], fields: &[&[u8]]) -> Result<Bytes, OperationFailure<AuthorityStoreError>> {
        let length = fields.iter().try_fold(domain.len(), |total, field| total.checked_add(field.len()))
            .ok_or_else(|| OperationFailure::new(crate::WorkError::Overflow.into(), self.work))?;
        let writes = WorkCounters { bytes_encoded: length as u64, bytes_copied: length as u64, ..WorkCounters::default() };
        self.admit(writes)?;
        let mut value = self.vec::<u8>(length)?;
        value.extend_from_slice(domain);
        for field in fields { value.extend_from_slice(field); }
        self.charge(writes)?;
        Ok(Bytes::from(value))
    }

    pub(crate) fn path(&mut self, authority: AuthorityId, suffix: &str, generation: Option<GenerationId>)
        -> Result<acyclic_stream::StreamPath, OperationFailure<AuthorityStoreError>> {
        const PREFIX: &[u8] = b"fs/authorities/";
        const HEX: &[u8] = b"0123456789abcdef";
        let length = PREFIX.len().checked_add(32).and_then(|n| n.checked_add(suffix.len()))
            .and_then(|n| n.checked_add(if generation.is_some() { 64 } else { 0 }))
            .filter(|n| *n <= acyclic_stream::MAX_PATH_BYTES)
            .ok_or_else(|| OperationFailure::new(AuthorityStoreError::Rejected("fork path is too long".to_owned()), self.work))?;
        let writes = WorkCounters { bytes_encoded: length as u64, bytes_copied: (PREFIX.len() + suffix.len()) as u64, ..WorkCounters::default() };
        self.admit(WorkCounters {
            bytes_encoded: writes.bytes_encoded,
            bytes_copied: writes.bytes_copied + length as u64,
            allocation_operations: 1,
            peak_allocation_bytes: self.allocations.live_bytes().checked_add(length as u64)
                .ok_or_else(|| OperationFailure::new(crate::WorkError::Overflow.into(), self.work))?,
            ..WorkCounters::default()
        })?;
        let mut text = [0_u8; acyclic_stream::MAX_PATH_BYTES];
        text[..PREFIX.len()].copy_from_slice(PREFIX);
        let mut cursor = PREFIX.len();
        for byte in authority.into_bytes() {
            text[cursor] = HEX[usize::from(byte >> 4)];
            text[cursor + 1] = HEX[usize::from(byte & 15)];
            cursor += 2;
        }
        text[cursor..cursor + suffix.len()].copy_from_slice(suffix.as_bytes());
        cursor += suffix.len();
        if let Some(generation) = generation {
            for byte in generation.digest().as_bytes() {
                text[cursor] = HEX[usize::from(byte >> 4)];
                text[cursor + 1] = HEX[usize::from(byte & 15)];
                cursor += 2;
            }
        }
        self.charge(writes)?;
        let text = std::str::from_utf8(&text[..length]).map_err(|_| OperationFailure::new(
            AuthorityStoreError::Rejected("fork path is not UTF-8".to_owned()), self.work))?;
        self.admit(WorkCounters {
            allocation_operations: 1,
            peak_allocation_bytes: self.allocations.live_bytes().checked_add(length as u64)
                .ok_or_else(|| OperationFailure::new(crate::WorkError::Overflow.into(), self.work))?,
            bytes_copied: length as u64, ..WorkCounters::default()
        })?;
        let path = acyclic_stream::StreamPath::new(text)
            .map_err(|error| OperationFailure::new(map_stream_error(error), self.work))?;
        self.allocations.claim_bytes(length as u64, 1, &mut self.work, WorkBudget::UNBOUNDED)
            .map_err(|error| OperationFailure::new(
                error.into_work_or(AuthorityStoreError::Rejected("fork allocation failed".to_owned())), self.work))?;
        self.charge(WorkCounters { bytes_copied: length as u64, ..WorkCounters::default() })?;
        Ok(path)
    }

    pub(crate) fn path_owned_bytes(path: &acyclic_stream::StreamPath) -> u64 {
        path.as_str().len() as u64
    }

    pub(crate) fn clone_path(&self, path: &acyclic_stream::StreamPath)
        -> Result<acyclic_stream::StreamPath, OperationFailure<AuthorityStoreError>> {
        // StreamPath owns Arc<str>; cloning shares the already admitted text.
        self.check()?;
        Ok(path.clone())
    }

    pub(crate) fn stream_key(&mut self, domain: &[u8], identity: &[u8])
        -> Result<acyclic_stream::IdempotencyKey, OperationFailure<AuthorityStoreError>> {
        let bytes = self.bytes(domain, &[&[0], identity])?;
        acyclic_stream::IdempotencyKey::new(bytes)
            .map_err(|error| OperationFailure::new(map_stream_error(error), self.work))
    }

    fn operation_key(&mut self, authority: AuthorityId, operation: OperationId)
        -> Result<acyclic_stream::IdempotencyKey, OperationFailure<AuthorityStoreError>> {
        let delta = WorkCounters { bytes_copied: 32, ..WorkCounters::default() };
        self.admit(delta)?;
        let mut identity = [0_u8; 32];
        identity[..16].copy_from_slice(&authority.into_bytes());
        identity[16..].copy_from_slice(&operation.into_bytes());
        self.charge(delta)?;
        self.stream_key(b"operation", &identity)
    }

    pub(crate) fn request_write_work(&self, request: &acyclic_stream::CommitRequest)
        -> Result<WorkCounters, OperationFailure<AuthorityStoreError>> {
        let mut delta = WorkCounters { backend_write_operations: 1, durability_operations: 1, ..WorkCounters::default() };
        for mutation in &request.mutations {
            let records = match mutation {
                acyclic_stream::CommitMutation::Append { records, .. }
                | acyclic_stream::CommitMutation::Fork { records, .. } => records,
                _ => continue,
            };
            delta.authority_records_appended = delta.authority_records_appended.checked_add(records.len() as u64)
                .ok_or_else(|| OperationFailure::new(crate::WorkError::Overflow.into(), self.work))?;
            for record in records {
                delta.authority_bytes_written = delta.authority_bytes_written.checked_add(record.len() as u64)
                    .ok_or_else(|| OperationFailure::new(crate::WorkError::Overflow.into(), self.work))?;
            }
        }
        Ok(delta)
    }

    pub(crate) fn remaining(&self) -> Result<WorkBudget, OperationFailure<AuthorityStoreError>> {
        self.check()?;
        let mut budget = self.work.remaining(self.budget)
            .map_err(|error| OperationFailure::new(error.into(), self.work))?;
        budget.peak_allocation_bytes = self.budget.peak_allocation_bytes
            .checked_sub(self.allocations.live_bytes())
            .ok_or_else(|| OperationFailure::new(crate::WorkError::Overflow.into(), self.work))?;
        Ok(budget)
    }

    pub(crate) fn merge_child(&mut self, mut child: WorkCounters) -> Result<(), OperationFailure<AuthorityStoreError>> {
        child.peak_allocation_bytes = self.allocations.live_bytes().checked_add(child.peak_allocation_bytes)
            .ok_or_else(|| OperationFailure::new(crate::WorkError::Overflow.into(), self.work))?;
        self.charge(child)
    }

    pub(crate) fn push<T>(&mut self, values: &mut Vec<T>, value: T)
        -> Result<(), OperationFailure<AuthorityStoreError>> {
        self.check()?;
        if values.len() == values.capacity() {
            return Err(OperationFailure::new(AuthorityStoreError::Rejected(
                "fork request exceeded its admitted vector capacity".to_owned()), self.work));
        }
        let delta = WorkCounters { bytes_copied: std::mem::size_of::<T>() as u64, ..WorkCounters::default() };
        self.admit(delta)?;
        values.push(value);
        self.charge(delta)
    }

    pub(crate) fn finish<T>(&self, value: T) -> AuthorityResult<T> {
        self.check()?;
        Ok(AuthorityReceipt { value, work: self.work })
    }
}

pub(crate) fn fork_fs_failure(failure: OperationFailure<AuthorityStoreError>) -> OperationFailure<crate::FsError> {
    OperationFailure::new(crate::FsError::from(failure.error), *failure.work)
}

pub(crate) fn project_fork_authority_failure(failure: OperationFailure<crate::FsError>)
    -> OperationFailure<AuthorityStoreError> {
    let error = match failure.error {
        crate::FsError::Authority(error) => error,
        crate::FsError::Work(error) => error.into(),
        crate::FsError::Cancelled(_) => AuthorityStoreError::Cancelled,
        crate::FsError::WorkspaceDeleted => AuthorityStoreError::Retired,
        error => AuthorityStoreError::Corrupt(error.to_string()),
    };
    OperationFailure::new(error, *failure.work)
}

pub(crate) async fn seal_fork_guard<F, Fut>(
    meter: &mut ForkWork<'_>, guard: &mut Option<&mut F>,
) -> Result<(), OperationFailure<crate::FsError>>
where
    F: FnMut(WorkBudget) -> Fut + Send,
    Fut: Future<Output = crate::FsResult<()>> + Send,
{
    meter.check().map_err(fork_fs_failure)?;
    if let Some(guard) = guard.as_deref_mut() {
        let result = guard(meter.remaining().map_err(fork_fs_failure)?).await;
        match result {
            Ok(receipt) => meter.merge_child(receipt.work).map_err(fork_fs_failure)?,
            Err(failure) => {
                meter.merge_child(*failure.work).map_err(fork_fs_failure)?;
                return Err(OperationFailure::new(failure.error, meter.work));
            }
        }
    }
    meter.check().map_err(fork_fs_failure)
}

/// Provider failures do not expose whether a lost acknowledgement made records durable.
/// Only a committed envelope witnesses semantic records; every invocation costs one call.
pub(crate) fn commit_work(
    submitted: WorkCounters,
    result: &Result<acyclic_stream::CommitOutcome, acyclic_stream::StreamError>,
) -> WorkCounters {
    if matches!(result, Ok(acyclic_stream::CommitOutcome::Committed(_))) {
        submitted
    } else {
        WorkCounters { backend_write_operations: 1, ..WorkCounters::default() }
    }
}

/// Filesystem authority over native hierarchical Streams.
///
/// Records, writer epochs, and lineage are child paths changed through one atomic Stream commit.
/// Retry results come from the provider's native idempotency authority; the adapter owns no side
/// database or shadow operation streams.
#[derive(Clone)]
pub struct StreamAuthorityStore<P> {
    provider: Arc<P>,
}

impl<P> StreamAuthorityStore<P> {
    /// Binds one authenticated public Stream provider.
    #[must_use]
    pub fn new(provider: Arc<P>) -> Self {
        Self { provider }
    }

    /// Returns the exact public Stream provider that supplies this authority.
    ///
    /// Higher-level protocol adapters use this only for filesystem-adjacent
    /// durable state machines, such as S3 multipart upload sessions. The
    /// filesystem authority itself remains represented by canonical Stream
    /// records and never by an adapter-local database.
    #[must_use]
    pub fn provider(&self) -> Arc<P> {
        Arc::clone(&self.provider)
    }
}

#[derive(Clone, Copy)]
struct AuthoritySnapshot {
    head: Head,
    record_tail: u64,
    epoch_tail: u64,
}

impl<P: acyclic_stream::StreamProvider> StreamAuthorityStore<P> {
    async fn measured_tail(
        &self,
        path: acyclic_stream::StreamPath,
        work: &mut WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<u64, OperationFailure<AuthorityStoreError>> {
        let next = admit_snapshot_read(
            *work,
            WorkCounters {
                backend_read_operations: 1,
                ..WorkCounters::default()
            },
            budget,
            cancellation,
        )?;
        *work = next;
        let result = self.provider.tail(path).await;
        cancellation
            .check()
            .map_err(|_| OperationFailure::new(AuthorityStoreError::Cancelled, *work))?;
        result.map_err(|error| OperationFailure::new(map_stream_error(error), *work))
    }

    async fn measured_record(
        &self,
        path: acyclic_stream::StreamPath,
        from: u64,
        work: &mut WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<acyclic_stream::Record, OperationFailure<AuthorityStoreError>> {
        // Stream bounds records, not bytes per read. Reserve its global record
        // ceiling before calling the provider; charge only returned bytes.
        admit_snapshot_read(
            *work,
            WorkCounters {
                authority_records_read: 1,
                authority_bytes_read: STREAM_RECORD_LIMIT,
                backend_read_operations: 1,
                ..WorkCounters::default()
            },
            budget,
            cancellation,
        )?;
        work.backend_read_operations += 1;
        let mut records = self
            .provider
            .read(acyclic_stream::ReadRequest {
                path,
                from,
                limit: 1,
            })
            .await
            .map_err(|error| OperationFailure::new(map_stream_error(error), *work))?;
        cancellation
            .check()
            .map_err(|_| OperationFailure::new(AuthorityStoreError::Cancelled, *work))?;
        let record = records
            .next()
            .await
            .ok_or_else(|| {
                OperationFailure::new(
                    AuthorityStoreError::Corrupt("Stream returned an empty exact read".to_owned()),
                    *work,
                )
            })?
            .map_err(|error| OperationFailure::new(map_stream_error(error), *work))?;
        *work = work
            .checked_add(WorkCounters {
                authority_records_read: 1,
                authority_bytes_read: u64::try_from(record.value.len()).unwrap_or(u64::MAX),
                ..WorkCounters::default()
            })
            .map_err(|error| OperationFailure::new(error.into(), *work))?;
        cancellation
            .check()
            .map_err(|_| OperationFailure::new(AuthorityStoreError::Cancelled, *work))?;
        admit_authority(*work, budget)?;
        if record.sequence != from {
            return Err(OperationFailure::new(
                AuthorityStoreError::Corrupt("Stream returned a non-contiguous record".to_owned()),
                *work,
            ));
        }
        Ok(record)
    }

    async fn is_retired_measured(
        &self,
        authority_id: AuthorityId,
        work: &mut WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<bool, OperationFailure<AuthorityStoreError>> {
        let path =
            retirement_path(authority_id).map_err(|error| OperationFailure::new(error, *work))?;
        match self
            .measured_tail(path.clone(), work, budget, cancellation)
            .await
        {
            Err(failure) if matches!(failure.error, AuthorityStoreError::Missing) => Ok(false),
            Ok(1) => {
                let marker = self
                    .measured_record(path, 0, work, budget, cancellation)
                    .await?;
                if marker.value.as_ref() != AUTHORITY_RETIRED {
                    return Err(OperationFailure::new(
                        AuthorityStoreError::Corrupt(
                            "filesystem retirement marker is invalid".to_owned(),
                        ),
                        *work,
                    ));
                }
                Ok(true)
            }
            Ok(_) => Err(OperationFailure::new(
                AuthorityStoreError::Corrupt(
                    "filesystem retirement marker has an invalid tail".to_owned(),
                ),
                *work,
            )),
            Err(failure) => Err(failure),
        }
    }

    async fn snapshot_measured(
        &self,
        authority_id: AuthorityId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<AuthoritySnapshot> {
        let mut work = WorkCounters::default();
        if self
            .is_retired_measured(authority_id, &mut work, budget, cancellation)
            .await?
        {
            return Err(OperationFailure::new(AuthorityStoreError::Retired, work));
        }
        let records =
            records_path(authority_id).map_err(|error| OperationFailure::new(error, work))?;
        let epochs =
            epochs_path(authority_id).map_err(|error| OperationFailure::new(error, work))?;
        let record_tail = self
            .measured_tail(records.clone(), &mut work, budget, cancellation)
            .await?;
        let epoch_tail = self
            .measured_tail(epochs.clone(), &mut work, budget, cancellation)
            .await?;
        if record_tail == 0 || epoch_tail == 0 {
            return Err(OperationFailure::new(
                AuthorityStoreError::Corrupt("authority genesis paths are empty".to_owned()),
                work,
            ));
        }
        let epoch_record = self
            .measured_record(epochs, epoch_tail - 1, &mut work, budget, cancellation)
            .await?;
        let epoch = decode_epoch(&epoch_record.value, EPOCH_DOMAIN)
            .map_err(|error| OperationFailure::new(error, work))?;
        let record = self
            .measured_record(records, record_tail - 1, &mut work, budget, cancellation)
            .await?;
        let head = if record_tail == 1 {
            decode_epoch(&record.value, GENESIS_DOMAIN)
                .map_err(|error| OperationFailure::new(error, work))?;
            Head::genesis(epoch)
        } else {
            let durable = decode_durable(authority_id, &record.value)
                .map_err(|error| OperationFailure::new(error, work))?;
            if durable.sequence.get().saturating_add(1) != record_tail {
                return Err(OperationFailure::new(
                    AuthorityStoreError::Corrupt(
                        "Stream sequence does not match filesystem authority sequence".to_owned(),
                    ),
                    work,
                ));
            }
            Head {
                epoch,
                sequence: durable.sequence,
                digest: durable.digest,
            }
        };
        Ok(AuthorityReceipt {
            value: AuthoritySnapshot {
                head,
                record_tail,
                epoch_tail,
            },
            work,
        })
    }

    /// Lists up to `maximum` exact filesystem authority identities, in order.
    ///
    /// Authorities are native direct children in Stream; no filesystem record or object is read.
    pub async fn authorities(&self, maximum: u32) -> Result<Vec<AuthorityId>, AuthorityStoreError> {
        /// Restarts tolerated when authorities come and go mid-listing.
        const RESTARTS: usize = 16;
        let parent = acyclic_stream::StreamPath::new("fs/authorities").map_err(map_stream_error)?;
        let page = u32::try_from(acyclic_stream::MAX_ITEMS).unwrap_or(u32::MAX);
        'listing: for _ in 0..RESTARTS {
            let mut authorities = Vec::new();
            let mut after = None;
            let mut hierarchy_version = None;
            loop {
                let listed = match self
                    .provider
                    .children_page(acyclic_stream::ChildrenPageRequest {
                        parent: Some(parent.clone()),
                        after: after.take(),
                        hierarchy_version,
                        limit: page,
                    })
                    .await
                {
                    Ok(listed) => listed,
                    Err(acyclic_stream::StreamError::HierarchyChanged) => continue 'listing,
                    Err(error) => return Err(map_stream_error(error)),
                };
                for child in &listed.children {
                    let authority = authority_child(&child.path)?;
                    if !self.is_retired(authority).await? {
                        authorities.push(authority);
                    }
                }
                if authorities.len() > maximum as usize {
                    return Err(AuthorityStoreError::Rejected(
                        "authority listing bound exceeded".to_owned(),
                    ));
                }
                let Some(next) = listed.next_after else {
                    return Ok(authorities);
                };
                after = Some(next);
                hierarchy_version = Some(listed.hierarchy_version);
            }
        }
        Err(AuthorityStoreError::Rejected(
            "authorities kept changing while they were listed".to_owned(),
        ))
    }

    async fn snapshot(
        &self,
        authority_id: AuthorityId,
    ) -> Result<AuthoritySnapshot, AuthorityStoreError> {
        if self.is_retired(authority_id).await? {
            return Err(AuthorityStoreError::Retired);
        }
        let records = records_path(authority_id)?;
        let epochs = epochs_path(authority_id)?;
        let record_tail = self
            .provider
            .tail(records.clone())
            .await
            .map_err(map_stream_error)?;
        let epoch_tail = self
            .provider
            .tail(epochs.clone())
            .await
            .map_err(map_stream_error)?;
        if record_tail == 0 || epoch_tail == 0 {
            return Err(AuthorityStoreError::Corrupt(
                "authority genesis paths are empty".to_owned(),
            ));
        }
        let epoch_record = read_one(self.provider.as_ref(), epochs, epoch_tail - 1).await?;
        let epoch = decode_epoch(&epoch_record.value, EPOCH_DOMAIN)?;
        let head = if record_tail == 1 {
            let genesis = read_one(self.provider.as_ref(), records, 0).await?;
            let _genesis_epoch = decode_epoch(&genesis.value, GENESIS_DOMAIN)?;
            Head::genesis(epoch)
        } else {
            let record = read_one(self.provider.as_ref(), records, record_tail - 1).await?;
            let durable = decode_durable(authority_id, &record.value)?;
            if durable.sequence.get().saturating_add(1) != record_tail {
                return Err(AuthorityStoreError::Corrupt(
                    "Stream sequence does not match filesystem authority sequence".to_owned(),
                ));
            }
            Head {
                epoch,
                sequence: durable.sequence,
                digest: durable.digest,
            }
        };
        Ok(AuthoritySnapshot {
            head,
            record_tail,
            epoch_tail,
        })
    }

    async fn is_retired(&self, authority_id: AuthorityId) -> Result<bool, AuthorityStoreError> {
        let path = retirement_path(authority_id)?;
        match self.provider.tail(path.clone()).await {
            Err(acyclic_stream::StreamError::NotFound) => Ok(false),
            Ok(1) => {
                let marker = read_one(self.provider.as_ref(), path, 0).await?;
                if marker.value.as_ref() != AUTHORITY_RETIRED {
                    return Err(AuthorityStoreError::Corrupt(
                        "filesystem retirement marker is invalid".to_owned(),
                    ));
                }
                Ok(true)
            }
            Ok(_) => Err(AuthorityStoreError::Corrupt(
                "filesystem retirement marker has an invalid tail".to_owned(),
            )),
            Err(error) => Err(map_stream_error(error)),
        }
    }

    async fn publication_gate(
        &self,
        authority_id: AuthorityId,
    ) -> Result<(u64, Option<OperationId>), AuthorityStoreError> {
        let path = publication_gate_path(authority_id)?;
        let tail = self
            .provider
            .tail(path.clone())
            .await
            .map_err(map_stream_error)?;
        if tail == 0 {
            return Err(AuthorityStoreError::Corrupt(
                "authority publication gate is empty".to_owned(),
            ));
        }
        let record = read_one(self.provider.as_ref(), path, tail - 1).await?;
        Ok((tail, decode_publication_gate(&record.value)?))
    }

    /// Verifies that an existing authority carries its publication gate, which
    /// every authority is created with.
    async fn verify_publication_gate(
        &self,
        authority_id: AuthorityId,
        mut work: WorkCounters,
        budget: WorkBudget,
    ) -> Result<WorkCounters, OperationFailure<AuthorityStoreError>> {
        let path = publication_gate_path(authority_id).map_err(OperationFailure::before_work)?;
        work = work
            .checked_add(authority_read_work(2))
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        admit_authority(work, budget)?;
        let tail = match self.provider.tail(path.clone()).await {
            Ok(tail) => tail,
            Err(acyclic_stream::StreamError::NotFound) => 0,
            Err(error) => {
                return Err(OperationFailure::new(map_stream_error(error), work));
            }
        };
        if tail == 0 {
            return Err(OperationFailure::new(
                AuthorityStoreError::Corrupt("authority has no publication gate".to_owned()),
                work,
            ));
        }
        let record = read_one(self.provider.as_ref(), path, tail - 1)
            .await
            .map_err(|error| OperationFailure::new(error, work))?;
        decode_publication_gate(&record.value)
            .map_err(|error| OperationFailure::new(error, work))?;
        Ok(work)
    }

    async fn operation(
        &self,
        authority_id: AuthorityId,
        operation_id: OperationId,
    ) -> Result<Option<DurableCommit>, AuthorityStoreError> {
        let key = operation_key(authority_id, operation_id)?;
        let Some(observation) = self
            .provider
            .inspect_idempotency(key)
            .await
            .map_err(map_stream_error)?
        else {
            return Ok(None);
        };
        let acyclic_stream::IdempotencyOutcome::Commit(outcome) = observation.outcome else {
            return Err(AuthorityStoreError::Corrupt(
                "operation identity retained another Stream mutation family".to_owned(),
            ));
        };
        let acyclic_stream::CommitOutcome::Committed(envelope) = outcome else {
            return Ok(None);
        };
        durable_from_operation_envelope(authority_id, operation_id, envelope).map(Some)
    }

    async fn fork_record(
        &self,
        path: acyclic_stream::StreamPath,
        from: u64,
        meter: &mut ForkWork<'_>,
    ) -> Result<acyclic_stream::Record, OperationFailure<AuthorityStoreError>> {
        // Native Stream bounds returned bodies, but does not expose allocation
        // receipts. This ceiling witnesses admission, not invented actual work.
        let peak = meter.allocations.live_bytes().checked_add(STREAM_RECORD_LIMIT)
            .ok_or_else(|| OperationFailure::new(crate::WorkError::Overflow.into(), meter.work))?;
        meter.admit(WorkCounters { peak_allocation_bytes: peak, ..WorkCounters::default() })?;
        self.measured_record(path, from, &mut meter.work, meter.budget, meter.cancellation).await
    }

    async fn fork_tail(
        &self,
        path: acyclic_stream::StreamPath,
        meter: &mut ForkWork<'_>,
    ) -> Result<u64, OperationFailure<AuthorityStoreError>> {
        self.measured_tail(path, &mut meter.work, meter.budget, meter.cancellation).await
    }

    async fn fork_snapshot(
        &self,
        authority: AuthorityId,
        meter: &mut ForkWork<'_>,
    ) -> Result<AuthoritySnapshot, OperationFailure<AuthorityStoreError>> {
        let baseline = meter.allocations.live_bytes();
        let result = self.fork_snapshot_inner(authority, meter).await;
        if result.is_err() {
            // Every temporary path is dropped when the inner future completes.
            // Retire those claims before a missing destination proceeds to creation.
            let remaining = meter.allocations.live_bytes().checked_sub(baseline)
                .ok_or_else(|| OperationFailure::new(crate::WorkError::Overflow.into(), meter.work))?;
            meter.release(remaining)?;
        }
        result
    }

    async fn fork_snapshot_inner(
        &self,
        authority: AuthorityId,
        meter: &mut ForkWork<'_>,
    ) -> Result<AuthoritySnapshot, OperationFailure<AuthorityStoreError>> {
        let retirement = meter.path(authority, "/retirement", None)?;
        let retired = match self.fork_tail(meter.clone_path(&retirement)?, meter).await {
            Err(failure) if matches!(failure.error, AuthorityStoreError::Missing) => false,
            Ok(1) => {
                let marker = self.fork_record(meter.clone_path(&retirement)?, 0, meter).await?;
                if marker.value.as_ref() != AUTHORITY_RETIRED {
                    return Err(OperationFailure::new(AuthorityStoreError::Corrupt("filesystem retirement marker is invalid".to_owned()), meter.work));
                }
                true
            }
            Ok(_) => return Err(OperationFailure::new(AuthorityStoreError::Corrupt("filesystem retirement marker has an invalid tail".to_owned()), meter.work)),
            Err(failure) => return Err(failure),
        };
        let retirement_bytes = retirement.as_str().len() as u64;
        drop(retirement);
        meter.release(retirement_bytes)?;
        if retired { return Err(OperationFailure::new(AuthorityStoreError::Retired, meter.work)); }
        let records = meter.path(authority, "/records", None)?;
        let epochs = meter.path(authority, "/epochs", None)?;
        let record_tail = self.fork_tail(meter.clone_path(&records)?, meter).await?;
        let epoch_tail = self.fork_tail(meter.clone_path(&epochs)?, meter).await?;
        if record_tail == 0 || epoch_tail == 0 {
            return Err(OperationFailure::new(AuthorityStoreError::Corrupt("authority genesis paths are empty".to_owned()), meter.work));
        }
        let epoch_record = self.fork_record(meter.clone_path(&epochs)?, epoch_tail - 1, meter).await?;
        let epoch = decode_epoch(&epoch_record.value, EPOCH_DOMAIN)
            .map_err(|error| OperationFailure::new(error, meter.work))?;
        drop(epoch_record);
        let epochs_bytes = epochs.as_str().len() as u64;
        drop(epochs);
        meter.release(epochs_bytes)?;
        let record = self.fork_record(meter.clone_path(&records)?, record_tail - 1, meter).await?;
        let head = if record_tail == 1 {
            decode_epoch(&record.value, GENESIS_DOMAIN).map_err(|error| OperationFailure::new(error, meter.work))?;
            Head::genesis(epoch)
        } else {
            let durable = StreamsDurableRecord::decode_borrowed(&record.value, STREAM_RECORD_LIMIT)
                .map_err(|error| OperationFailure::new(AuthorityStoreError::Corrupt(error.to_string()), meter.work))?;
            let hashing = WorkCounters {
                bytes_hashed: crate::foundation::AUTHORITY_COMMIT_DIGEST_ENVELOPE_BYTES.checked_add(durable.payload.len() as u64)
                    .ok_or_else(|| OperationFailure::new(crate::WorkError::Overflow.into(), meter.work))?,
                ..WorkCounters::default()
            };
            meter.admit(hashing)?;
            let digest = authority_commit_digest(authority, durable.epoch, durable.sequence, durable.operation_id,
                durable.fingerprint, durable.previous_digest, durable.payload);
            meter.charge(hashing)?;
            if digest != durable.digest || durable.sequence.get().checked_add(1) != Some(record_tail) {
                return Err(OperationFailure::new(AuthorityStoreError::Corrupt("Stream authority record or sequence mismatch".to_owned()), meter.work));
            }
            Head { epoch, sequence: durable.sequence, digest: durable.digest }
        };
        drop(record);
        let records_bytes = records.as_str().len() as u64;
        drop(records);
        meter.release(records_bytes)?;
        Ok(AuthoritySnapshot { head, record_tail, epoch_tail })
    }

    async fn fork_gate(
        &self,
        authority: AuthorityId,
        meter: &mut ForkWork<'_>,
    ) -> Result<(), OperationFailure<AuthorityStoreError>> {
        let gate = meter.path(authority, "/publication-gate", None)?;
        let tail = self.fork_tail(meter.clone_path(&gate)?, meter).await?;
        if tail == 0 {
            return Err(OperationFailure::new(AuthorityStoreError::Corrupt("authority publication gate is empty".to_owned()), meter.work));
        }
        let record = self.fork_record(meter.clone_path(&gate)?, tail - 1, meter).await?;
        decode_publication_gate(&record.value).map_err(|error| OperationFailure::new(error, meter.work))?;
        drop(record);
        let bytes = gate.as_str().len() as u64;
        drop(gate);
        meter.release(bytes)
    }

    async fn existing_fork_destination(
        &self,
        destination_authority: AuthorityId,
        source_generation: GenerationId,
        forked_at: u64,
        meter: &mut ForkWork<'_>,
    ) -> Result<Option<AuthoritySnapshot>, OperationFailure<AuthorityStoreError>> {
        let snapshot = match self.fork_snapshot(destination_authority, meter).await {
            Ok(snapshot) => snapshot,
            Err(failure) if matches!(failure.error, AuthorityStoreError::Missing) => return Ok(None),
            Err(failure) => return Err(failure),
        };
        let locator = meter.path(destination_authority, "/generations/", Some(source_generation))?;
        let locator_bytes = locator.as_str().len() as u64;
        let record = self.fork_record(locator, 0, meter).await?;
        meter.release(locator_bytes)?;
        let located_tail = decode_lineage_tail(&record.value).map_err(|error| OperationFailure::new(error, meter.work))?;
        drop(record);
        if located_tail == 0 {
            return Err(OperationFailure::new(AuthorityStoreError::Corrupt("destination generation locator selected an empty lineage".to_owned()), meter.work));
        }
        let lineage = meter.path(destination_authority, "/lineage", None)?;
        let lineage_bytes = lineage.as_str().len() as u64;
        let selected = self.fork_record(lineage, located_tail - 1, meter).await?;
        meter.release(lineage_bytes)?;
        let generation = decode_lineage_generation(&selected.value).map_err(|error| OperationFailure::new(error, meter.work))?;
        if located_tail == forked_at && generation == source_generation {
            Ok(Some(snapshot))
        } else {
            Err(OperationFailure::new(AuthorityStoreError::Rejected("destination generation fork conflicts with existing state".to_owned()), meter.work))
        }
    }

    /// Resolves the original source identity with bounded paths and exact reads.
    async fn resolve_source_fork_point(
        &self,
        source_authority: AuthorityId,
        source_generation: GenerationId,
        meter: &mut ForkWork<'_>,
    ) -> Result<(acyclic_stream::StreamPath, u64), OperationFailure<AuthorityStoreError>> {
        let source_lineage = meter.path(source_authority, "/lineage", None)?;
        let locator = meter.path(source_authority, "/generations/", Some(source_generation))?;
        let locator_bytes = locator.as_str().len() as u64;
        let record = self.fork_record(locator, 0, meter).await.map_err(|failure| {
            if matches!(failure.error, AuthorityStoreError::Missing) {
                OperationFailure::new(AuthorityStoreError::Rejected("source authority has no generation lineage locator".to_owned()), *failure.work)
            } else { failure }
        })?;
        meter.release(locator_bytes)?;
        let forked_at = decode_lineage_tail(&record.value).map_err(|error| OperationFailure::new(error, meter.work))?;
        drop(record);
        if forked_at == 0 {
            return Err(OperationFailure::new(AuthorityStoreError::Corrupt("generation locator selected an empty lineage".to_owned()), meter.work));
        }
        let selected = self.fork_record(meter.clone_path(&source_lineage)?, forked_at - 1, meter).await?;
        let selected_generation = decode_lineage_generation(&selected.value).map_err(|error| OperationFailure::new(error, meter.work))?;
        if selected_generation != source_generation {
            return Err(OperationFailure::new(AuthorityStoreError::Corrupt("generation locator and lineage disagree".to_owned()), meter.work));
        }
        Ok((source_lineage, forked_at))
    }
}

impl<P: acyclic_stream::StreamProvider> StreamAuthorityStore<P> {
    /// Builds the one commit that creates a forked workspace's retention
    /// authority with its record and the destination authority with its
    /// creation record, under the creation's own retry identity so the
    /// operation is found like any appended one.
    async fn workspace_fork_request(
        &self,
        fork: &WorkspaceForkCommit,
        meter: &mut ForkWork<'_>,
    ) -> Result<(acyclic_stream::CommitRequest, u64, u64), OperationFailure<AuthorityStoreError>> {
        let idempotency_key = meter.operation_key(fork.destination, fork.creation.operation_id)?;
        let mut request = acyclic_stream::CommitRequest {
            conditions: meter.vec(14)?,
            mutations: meter.vec(14)?,
            idempotency_key,
        };
        let lineage = match fork.lineage {
            Some(source) if source.lineage == crate::GenerationFork::PublishedPrefix => {
                let (source_lineage, forked_at) = self
                    .resolve_source_fork_point(source.authority, source.generation, meter).await?;
                Some(ForkedLineage { source_lineage, forked_at, source_generation: source.generation })
            }
            Some(_) | None => None,
        };
        let (records, bytes) = first_record_commit(&mut request, fork.retention, &fork.retained, None, meter)?;
        let (more_records, more_bytes) = first_record_commit(&mut request, fork.destination, &fork.creation, lineage, meter)?;
        Ok((request, records + more_records, bytes + more_bytes))
    }

    async fn first_record_engine<F, Fut>(
        &self, authority: AuthorityId, commit: ProposedCommit, budget: WorkBudget,
        cancellation: &CancellationToken, mut guard: Option<&mut F>,
    ) -> crate::FsResult<bool>
    where F: FnMut(WorkBudget) -> Fut + Send, Fut: Future<Output = crate::FsResult<()>> + Send {
        let mut meter = ForkWork::new(budget, cancellation);
        meter.check().map_err(fork_fs_failure)?;
        let idempotency_key = meter.operation_key(authority, commit.operation_id).map_err(fork_fs_failure)?;
        let mut request = acyclic_stream::CommitRequest {
            conditions: meter.vec(7).map_err(fork_fs_failure)?,
            mutations: meter.vec(7).map_err(fork_fs_failure)?,
            idempotency_key,
        };
        first_record_commit(&mut request, authority, &commit, None, &mut meter).map_err(fork_fs_failure)?;
        let writes = meter.request_write_work(&request).map_err(fork_fs_failure)?;
        meter.admit(writes).map_err(fork_fs_failure)?;
        seal_fork_guard(&mut meter, &mut guard).await?;
        meter.admit(writes).map_err(fork_fs_failure)?;
        let result = self.provider.commit(request).await;
        meter.release_request().map_err(fork_fs_failure)?;
        meter.charge(commit_work(writes, &result)).map_err(fork_fs_failure)?;
        match result {
            Ok(acyclic_stream::CommitOutcome::Committed(_)) => {
                // Native ACKs do not distinguish a fresh commit from keyed replay.
                seal_fork_guard(&mut meter, &mut guard).await?;
                return Ok(crate::FsReceipt { value: true, work: meter.work });
            }
            Err(acyclic_stream::StreamError::IdempotencyMismatch) => return Ok(crate::FsReceipt { value: false, work: meter.work }),
            Ok(acyclic_stream::CommitOutcome::Conflict(_)) => {}
            Err(error) => return Err(OperationFailure::new(crate::FsError::from(map_stream_error(error)), meter.work)),
        }
        let records = meter.path(authority, "/records", None).map_err(fork_fs_failure)?;
        let path_bytes = ForkWork::path_owned_bytes(&records);
        let tail = self.fork_tail(meter.clone_path(&records).map_err(fork_fs_failure)?, &mut meter).await;
        let value = match tail {
            Ok(tail) if tail >= 2 => {
                let record = self.fork_record(records, 1, &mut meter).await.map_err(fork_fs_failure)?;
                let first = StreamsDurableRecord::decode_borrowed(&record.value, STREAM_RECORD_LIMIT)
                    .map_err(|error| OperationFailure::new(crate::FsError::from(AuthorityStoreError::Corrupt(error.to_string())), meter.work))?;
                let hash = WorkCounters {
                    bytes_hashed: crate::foundation::AUTHORITY_COMMIT_DIGEST_ENVELOPE_BYTES + first.payload.len() as u64,
                    ..WorkCounters::default()
                };
                meter.admit(hash).map_err(fork_fs_failure)?;
                let digest = authority_commit_digest(authority, first.epoch, first.sequence, first.operation_id,
                    first.fingerprint, first.previous_digest, first.payload);
                meter.charge(hash).map_err(fork_fs_failure)?;
                if digest != first.digest {
                    return Err(OperationFailure::new(crate::FsError::from(AuthorityStoreError::Corrupt(
                        "Stream durable record digest mismatch".to_owned())), meter.work));
                }
                first.sequence == Sequence::new(1) && first.operation_id == commit.operation_id
                    && first.fingerprint == commit.fingerprint
            }
            Ok(_) => { drop(records); false }
            Err(failure) if matches!(failure.error, AuthorityStoreError::Missing) => { drop(records); false }
            Err(failure) => return Err(fork_fs_failure(failure)),
        };
        meter.release(path_bytes).map_err(fork_fs_failure)?;
        if value { seal_fork_guard(&mut meter, &mut guard).await?; }
        Ok(crate::FsReceipt { value, work: meter.work })
    }

    async fn workspace_fork_engine<F, Fut>(
        &self, fork: WorkspaceForkCommit, permit: PublicationPermit, budget: WorkBudget,
        cancellation: &CancellationToken, mut guard: Option<&mut F>,
    ) -> crate::FsResult<WorkspaceForkOutcome>
    where F: FnMut(WorkBudget) -> Fut + Send, Fut: Future<Output = crate::FsResult<()>> + Send {
        if !matches!(permit, PublicationPermit::Unrestricted) {
            return Err(OperationFailure::before_work(crate::FsError::from(AuthorityStoreError::Rejected(
                "native workspace fork does not support a publication reservation".to_owned()))));
        }
        let mut meter = ForkWork::new(budget, cancellation);
        let (request, _, _) = self.workspace_fork_request(&fork, &mut meter).await.map_err(fork_fs_failure)?;
        let writes = meter.request_write_work(&request).map_err(fork_fs_failure)?;
        meter.admit(writes).map_err(fork_fs_failure)?;
        seal_fork_guard(&mut meter, &mut guard).await?;
        meter.admit(writes).map_err(fork_fs_failure)?;
        let result = self.provider.commit(request).await;
        meter.release_request().map_err(fork_fs_failure)?;
        meter.charge(commit_work(writes, &result)).map_err(fork_fs_failure)?;
        match result {
            Ok(acyclic_stream::CommitOutcome::Committed(_)) => {
                seal_fork_guard(&mut meter, &mut guard).await?;
                return Ok(crate::FsReceipt { value: WorkspaceForkOutcome::Committed, work: meter.work });
            }
            Ok(acyclic_stream::CommitOutcome::Conflict(_))
            | Err(acyclic_stream::StreamError::IdempotencyMismatch) => {}
            Err(error) => return Err(OperationFailure::new(crate::FsError::from(map_stream_error(error)), meter.work)),
        }
        let child_budget = meter.remaining().map_err(fork_fs_failure)?;
        let stepped = if let Some(guard) = guard.as_deref_mut() {
            crate::async_storage::commit_workspace_fork_in_steps_guarded(
                self, fork, PublicationPermit::Unrestricted, child_budget, cancellation, guard,
            ).await
        } else {
            crate::async_storage::commit_workspace_fork_in_steps(self, fork, child_budget, cancellation).await
                .map(|receipt| crate::FsReceipt { value: receipt.value, work: receipt.work }).map_err(fork_fs_failure)
        };
        match stepped {
            Ok(receipt) => {
                meter.merge_child(receipt.work).map_err(fork_fs_failure)?;
                Ok(crate::FsReceipt { value: receipt.value, work: meter.work })
            }
            Err(failure) => {
                meter.merge_child(*failure.work).map_err(fork_fs_failure)?;
                Err(OperationFailure::new(failure.error, meter.work))
            }
        }
    }

    pub(crate) async fn create_authority_engine<F, Fut>(
        &self, authority: AuthorityId, epoch: Epoch, budget: WorkBudget,
        cancellation: &CancellationToken, mut guard: Option<&mut F>,
    ) -> crate::FsResult<CreateAuthorityOutcome>
    where F: FnMut(WorkBudget) -> Fut + Send, Fut: Future<Output = crate::FsResult<()>> + Send {
        let mut meter = ForkWork::new(budget, cancellation);
        match self.fork_snapshot(authority, &mut meter).await {
            Ok(snapshot) => {
                self.fork_gate(authority, &mut meter).await.map_err(fork_fs_failure)?;
                seal_fork_guard(&mut meter, &mut guard).await?;
                return Ok(crate::FsReceipt { value: CreateAuthorityOutcome::Existing(snapshot.head), work: meter.work });
            }
            Err(failure) if matches!(failure.error, AuthorityStoreError::Missing) => {}
            Err(failure) => return Err(fork_fs_failure(failure)),
        }
        let idempotency_key = meter.stream_key(b"create", &authority.into_bytes()).map_err(fork_fs_failure)?;
        let mut conditions = meter.vec(4).map_err(fork_fs_failure)?;
        let mut mutations = meter.vec(4).map_err(fork_fs_failure)?;
        for (suffix, domain, static_value) in [
            ("", None, Some(AUTHORITY_MARKER)),
            ("/records", Some(GENESIS_DOMAIN), None),
            ("/epochs", Some(EPOCH_DOMAIN), None),
            ("/publication-gate", None, Some(PUBLICATION_GATE_FREE)),
        ] {
            let path = meter.path(authority, suffix, None).map_err(fork_fs_failure)?;
            let condition = acyclic_stream::CommitCondition::Absent { path: meter.clone_path(&path).map_err(fork_fs_failure)? };
            meter.push(&mut conditions, condition).map_err(fork_fs_failure)?;
            let mut records = meter.vec(1).map_err(fork_fs_failure)?;
            let value = if let Some(value) = static_value { Bytes::from_static(value) }
                else { meter.bytes(domain.expect("epoch record domain"), &[&epoch.get().to_le_bytes()]).map_err(fork_fs_failure)? };
            meter.push(&mut records, value).map_err(fork_fs_failure)?;
            meter.push(&mut mutations, acyclic_stream::CommitMutation::Append { path, records }).map_err(fork_fs_failure)?;
        }
        let request = acyclic_stream::CommitRequest { conditions, mutations, idempotency_key };
        let writes = meter.request_write_work(&request).map_err(fork_fs_failure)?;
        meter.admit(writes).map_err(fork_fs_failure)?;
        seal_fork_guard(&mut meter, &mut guard).await?;
        meter.admit(writes).map_err(fork_fs_failure)?;
        let result = self.provider.commit(request).await;
        meter.release_request().map_err(fork_fs_failure)?;
        meter.charge(commit_work(writes, &result)).map_err(fork_fs_failure)?;
        let value = match result {
            Ok(acyclic_stream::CommitOutcome::Committed(_)) => {
                seal_fork_guard(&mut meter, &mut guard).await?;
                CreateAuthorityOutcome::Created(Head::genesis(epoch))
            }
            Ok(acyclic_stream::CommitOutcome::Conflict(_)) => {
                let snapshot = self.fork_snapshot(authority, &mut meter).await.map_err(fork_fs_failure)?;
                self.fork_gate(authority, &mut meter).await.map_err(fork_fs_failure)?;
                seal_fork_guard(&mut meter, &mut guard).await?;
                CreateAuthorityOutcome::Existing(snapshot.head)
            }
            Err(error) => return Err(OperationFailure::new(crate::FsError::from(map_stream_error(error)), meter.work)),
        };
        Ok(crate::FsReceipt { value, work: meter.work })
    }
}

#[cfg(test)]
impl<P: acyclic_stream::StreamProvider> StreamAuthorityStore<P> {
    /// Every generation in one authority's lineage, oldest first.
    pub(crate) async fn generation_lineage(
        &self,
        authority: AuthorityId,
    ) -> Result<Vec<GenerationId>, AuthorityStoreError> {
        let path = lineage_path(authority)?;
        let tail = self
            .provider
            .tail(path.clone())
            .await
            .map_err(map_stream_error)?;
        let mut lineage = Vec::new();
        for sequence in 0..tail {
            let record = read_one(self.provider.as_ref(), path.clone(), sequence).await?;
            lineage.push(decode_lineage_generation(&record.value)?);
        }
        Ok(lineage)
    }

    /// The lineage length at which one generation is located.
    pub(crate) async fn generation_locator(
        &self,
        authority: AuthorityId,
        generation: GenerationId,
    ) -> Result<u64, AuthorityStoreError> {
        let record = read_one(
            self.provider.as_ref(),
            generation_path(authority, generation)?,
            0,
        )
        .await?;
        decode_lineage_tail(&record.value)
    }
}

/// The source lineage prefix a forked destination authority starts from.
struct ForkedLineage {
    source_lineage: acyclic_stream::StreamPath,
    forked_at: u64,
    source_generation: GenerationId,
}

/// Adds to `request` everything that creates `authority` with `commit` as
/// its first record: the authority's genesis paths, its lineage forked from
/// a source's prefix when `lineage` is given, and, for a record that
/// publishes a generation, that generation's lineage entry and locator, as
/// a creation or fork followed by an append would leave them. Returns the
/// authority records and payload bytes it appends.
fn first_record_commit(
    request: &mut acyclic_stream::CommitRequest,
    authority: AuthorityId,
    commit: &ProposedCommit,
    lineage: Option<ForkedLineage>,
    meter: &mut ForkWork<'_>,
) -> Result<(u64, u64), OperationFailure<AuthorityStoreError>> {
    use acyclic_stream::CommitCondition::Absent;
    use acyclic_stream::CommitMutation::Append;
    let epoch = Epoch::GENESIS;
    let sequence = Sequence::new(1);
    let previous = Head::genesis(epoch);
    let hash = WorkCounters {
        bytes_hashed: crate::foundation::AUTHORITY_COMMIT_DIGEST_ENVELOPE_BYTES
            .checked_add(commit.payload.len() as u64)
            .ok_or_else(|| OperationFailure::new(crate::WorkError::Overflow.into(), meter.work))?,
        ..WorkCounters::default()
    };
    meter.admit(hash)?;
    let durable = crate::streams_record::BorrowedStreamsDurableRecord {
        epoch,
        sequence,
        operation_id: commit.operation_id,
        fingerprint: commit.fingerprint,
        previous_digest: previous.digest,
        digest: authority_commit_digest(
            authority,
            epoch,
            sequence,
            commit.operation_id,
            commit.fingerprint,
            previous.digest,
            &commit.payload,
        ),
        payload: &commit.payload,
    };
    meter.charge(hash)?;
    let encoded = StreamsDurableRecord::encode_borrowed_with(&durable, STREAM_RECORD_LIMIT, |domain, fields| meter.bytes(domain, fields))
        .map_err(|error| OperationFailure::new(AuthorityStoreError::Rejected(error.to_string()), meter.work))??;
    let mut appended_records = 1_u64;
    let mut appended_bytes = durable.payload.len() as u64;
    let root = meter.path(authority, "", None)?;
    let records = meter.path(authority, "/records", None)?;
    let epochs = meter.path(authority, "/epochs", None)?;
    let gate = meter.path(authority, "/publication-gate", None)?;
    for path in [&root, &records, &epochs, &gate] {
        let condition = Absent { path: meter.clone_path(path)? };
        meter.push(&mut request.conditions, condition)?;
    }
    let mut marker_records = meter.vec(1)?;
    meter.push(&mut marker_records, Bytes::from_static(AUTHORITY_MARKER))?;
    meter.push(&mut request.mutations, Append { path: root, records: marker_records })?;
    let item = WorkCounters { items_examined: 1, ..WorkCounters::default() };
    meter.admit(item)?;
    let generation = generation_from_payload(authority, sequence, durable.payload);
    meter.charge(item)?;
    let generation = generation.map_err(|error| OperationFailure::new(error, meter.work))?;
    let (lineage_records, lineage_bytes) = lineage_commit(request, authority, lineage, generation, meter)?;
    appended_records += lineage_records;
    appended_bytes += lineage_bytes;
    let mut record_values = meter.vec(2)?;
    let genesis = meter.bytes(GENESIS_DOMAIN, &[&epoch.get().to_le_bytes()])?;
    meter.push(&mut record_values, genesis)?;
    meter.push(&mut record_values, encoded)?;
    meter.push(&mut request.mutations, Append { path: records, records: record_values })?;
    let mut epoch_values = meter.vec(1)?;
    let epoch_record = meter.bytes(EPOCH_DOMAIN, &[&epoch.get().to_le_bytes()])?;
    meter.push(&mut epoch_values, epoch_record)?;
    meter.push(&mut request.mutations, Append { path: epochs, records: epoch_values })?;
    let mut gate_values = meter.vec(1)?;
    meter.push(&mut gate_values, Bytes::from_static(PUBLICATION_GATE_FREE))?;
    meter.push(&mut request.mutations, Append { path: gate, records: gate_values })?;
    Ok((appended_records, appended_bytes))
}

/// Adds a new authority's lineage: the source's forked prefix, if any,
/// extended in the same fork mutation by the entry of the generation its
/// first record publishes, and that generation's locator.
fn lineage_commit(
    request: &mut acyclic_stream::CommitRequest,
    authority: AuthorityId,
    lineage: Option<ForkedLineage>,
    generation: Option<GenerationId>,
    meter: &mut ForkWork<'_>,
) -> Result<(u64, u64), OperationFailure<AuthorityStoreError>> {
    use acyclic_stream::CommitCondition::Absent;
    use acyclic_stream::CommitMutation::{Append, Fork};
    if lineage.is_none() && generation.is_none() {
        return Ok((0, 0));
    }
    let lineage_path = meter.path(authority, "/lineage", None)?;
    let condition = Absent { path: meter.clone_path(&lineage_path)? };
    meter.push(&mut request.conditions, condition)?;
    let mut entries = meter.vec(usize::from(generation.is_some()))?;
    if let Some(generation) = generation {
        let entry = meter.bytes(LINEAGE_DOMAIN, &[generation.digest().as_bytes()])?;
        meter.push(&mut entries, entry)?;
    }
    let mut appended_records = entries.len() as u64;
    let mut appended_bytes = entries.iter().map(|entry| entry.len() as u64).sum::<u64>();
    let mut tail = 0;
    let mut forked_locator = None;
    match lineage {
        Some(forked) => {
            let locator = meter.path(authority, "/generations/", Some(forked.source_generation))?;
            let condition = Absent { path: meter.clone_path(&locator)? };
            meter.push(&mut request.conditions, condition)?;
            meter.push(&mut request.mutations, Fork {
                source: forked.source_lineage,
                destination: lineage_path,
                at_tail: forked.forked_at,
                records: entries,
            })?;
            let mut locator_values = meter.vec(1)?;
            let locator_record = meter.bytes(LINEAGE_TAIL_DOMAIN, &[&forked.forked_at.to_le_bytes()])?;
            meter.push(&mut locator_values, locator_record)?;
            let append = Append { path: meter.clone_path(&locator)?, records: locator_values };
            meter.push(&mut request.mutations, append)?;
            tail = forked.forked_at;
            forked_locator = Some(locator);
        }
        None => meter.push(&mut request.mutations, Append { path: lineage_path, records: entries })?,
    }
    if let Some(generation) = generation {
        let locator = meter.path(authority, "/generations/", Some(generation))?;
        if forked_locator.as_ref() == Some(&locator) {
            return Err(OperationFailure::new(AuthorityStoreError::Rejected(
                "a fork cannot publish its source generation again".to_owned(),
            ), meter.work));
        }
        let locator_record = meter.bytes(LINEAGE_TAIL_DOMAIN, &[&tail.saturating_add(1).to_le_bytes()])?;
        appended_records += 1;
        appended_bytes += locator_record.len() as u64;
        let condition = Absent { path: meter.clone_path(&locator)? };
        meter.push(&mut request.conditions, condition)?;
        let mut locator_values = meter.vec(1)?;
        meter.push(&mut locator_values, locator_record)?;
        meter.push(&mut request.mutations, Append { path: locator, records: locator_values })?;
    }
    Ok((appended_records, appended_bytes))
}

impl<P: acyclic_stream::StreamProvider> AsyncAuthorityStore for StreamAuthorityStore<P> {
    /// One durable commit creates the authority with its first record. An
    /// authority that already exists, from an earlier attempt or another
    /// writer, resolves through the ordinary idempotent append instead.
    async fn create_authority_with_first_record(
        &self, authority: AuthorityId, commit: ProposedCommit, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<bool> {
        self.first_record_engine(authority, commit, budget, cancellation,
            None::<&mut fn(WorkBudget) -> std::future::Ready<crate::FsResult<()>>>).await
            .map(|receipt| AuthorityReceipt { value: receipt.value, work: receipt.work })
            .map_err(project_fork_authority_failure)
    }

    async fn create_authority_with_first_record_guarded<F, Fut>(
        &self, authority: AuthorityId, commit: ProposedCommit, budget: WorkBudget,
        cancellation: &CancellationToken, guard: &mut F,
    ) -> crate::FsResult<bool>
    where F: FnMut(WorkBudget) -> Fut + Send, Fut: Future<Output = crate::FsResult<()>> + Send {
        self.first_record_engine(authority, commit, budget, cancellation, Some(guard)).await
    }

    /// One durable commit creates the whole fork. Anything that stops it
    /// from applying as a whole, such as an authority an earlier attempt or
    /// another writer created, or a retry whose lineage changed, resolves
    /// through the idempotent single-authority steps instead.
    async fn commit_workspace_fork(
        &self, fork: WorkspaceForkCommit, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> AuthorityResult<WorkspaceForkOutcome> {
        self.workspace_fork_engine(fork, PublicationPermit::Unrestricted, budget, cancellation,
            None::<&mut fn(WorkBudget) -> std::future::Ready<crate::FsResult<()>>>).await
            .map(|receipt| AuthorityReceipt { value: receipt.value, work: receipt.work })
            .map_err(project_fork_authority_failure)
    }

    async fn commit_workspace_fork_guarded<F, Fut>(
        &self, fork: WorkspaceForkCommit, permit: PublicationPermit, budget: WorkBudget,
        cancellation: &CancellationToken, guard: &mut F,
    ) -> crate::FsResult<WorkspaceForkOutcome>
    where F: FnMut(WorkBudget) -> Fut + Send, Fut: Future<Output = crate::FsResult<()>> + Send {
        self.workspace_fork_engine(fork, permit, budget, cancellation, Some(guard)).await
    }

    fn supports_generation_lineage_prefix(&self) -> bool {
        true
    }

    async fn fork_generation_authority(
        &self,
        source: crate::GenerationForkSource,
        destination_authority: AuthorityId,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<CreateAuthorityOutcome> {
        self.fork_generation_with_owner_guard(
            source, destination_authority, operation_id, budget, cancellation,
            None::<&mut fn(WorkBudget) -> std::future::Ready<crate::FsResult<()>>>,
        ).await
            .map(|receipt| AuthorityReceipt { value: receipt.value, work: receipt.work })
            .map_err(project_fork_authority_failure)
    }

    async fn fork_generation_authority_guarded<F, Fut>(
        &self,
        source: crate::GenerationForkSource,
        destination_authority: AuthorityId,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        guard: &mut F,
    ) -> crate::FsResult<CreateAuthorityOutcome>
    where
        F: FnMut(WorkBudget) -> Fut + Send,
        Fut: Future<Output = crate::FsResult<()>> + Send,
    {
        self.fork_generation_with_owner_guard(
            source, destination_authority, operation_id, budget, cancellation, Some(guard),
        ).await
    }

    async fn create_authority(
        &self, authority_id: AuthorityId, genesis_epoch: Epoch, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<CreateAuthorityOutcome> {
        self.create_authority_engine(authority_id, genesis_epoch, budget, cancellation,
            None::<&mut fn(WorkBudget) -> std::future::Ready<crate::FsResult<()>>>).await
            .map(|receipt| AuthorityReceipt { value: receipt.value, work: receipt.work })
            .map_err(project_fork_authority_failure)
    }

    async fn create_authority_guarded<F, Fut>(
        &self, authority_id: AuthorityId, genesis_epoch: Epoch, budget: WorkBudget,
        cancellation: &CancellationToken, guard: &mut F,
    ) -> crate::FsResult<CreateAuthorityOutcome>
    where F: FnMut(WorkBudget) -> Fut + Send, Fut: Future<Output = crate::FsResult<()>> + Send {
        self.create_authority_engine(authority_id, genesis_epoch, budget, cancellation, Some(guard)).await
    }

    async fn head(
        &self,
        authority_id: AuthorityId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Head> {
        cancellation
            .check()
            .map_err(|_| OperationFailure::before_work(AuthorityStoreError::Cancelled))?;
        let snapshot = self
            .snapshot_measured(authority_id, budget, cancellation)
            .await?;
        Ok(AuthorityReceipt {
            value: snapshot.value.head,
            work: snapshot.work,
        })
    }

    /// Retires filesystem authority metadata without removing its Stream
    /// records. The marker is stable across retries and can be replayed after
    /// an interrupted workspace deletion.
    async fn retire_authority(
        &self,
        authority_id: AuthorityId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<()> {
        cancellation
            .check()
            .map_err(|_| OperationFailure::before_work(AuthorityStoreError::Cancelled))?;
        let root = authority_path(authority_id).map_err(OperationFailure::before_work)?;
        let path = retirement_path(authority_id).map_err(OperationFailure::before_work)?;
        let mut work = authority_read_work(1);
        admit_authority(work, budget)?;
        match self.provider.tail(root).await {
            Err(acyclic_stream::StreamError::NotFound) => {
                return authority_success((), work, budget);
            }
            Ok(_) => {}
            Err(error) => return Err(OperationFailure::new(map_stream_error(error), work)),
        }
        work = work
            .checked_add(authority_write_work(1, AUTHORITY_RETIRED.len() as u64))
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        admit_authority(work, budget)?;
        let request = acyclic_stream::AppendRequest {
            path,
            records: vec![Bytes::from_static(AUTHORITY_RETIRED)],
            if_tail: Some(0),
            idempotency_key: Some(
                stream_key(b"retire-authority", &authority_id.into_bytes())
                    .map_err(OperationFailure::before_work)?,
            ),
        };
        match self.provider.append(request).await {
            Ok(acyclic_stream::AppendOutcome::Committed(_)) => authority_success((), work, budget),
            Ok(acyclic_stream::AppendOutcome::TailConflict { .. }) => {
                work = work
                    .checked_add(authority_read_work(2))
                    .map_err(|error| OperationFailure::new(error.into(), work))?;
                admit_authority(work, budget)?;
                if self
                    .is_retired(authority_id)
                    .await
                    .map_err(|error| OperationFailure::new(error, work))?
                {
                    authority_success((), work, budget)
                } else {
                    Err(OperationFailure::new(
                        AuthorityStoreError::Corrupt(
                            "filesystem retirement marker conflicted without retirement".to_owned(),
                        ),
                        work,
                    ))
                }
            }
            Err(error) => Err(OperationFailure::new(map_stream_error(error), work)),
        }
    }

    async fn compare_and_append(
        &self,
        authority_id: AuthorityId,
        epoch: Epoch,
        expected: Head,
        commit: ProposedCommit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<FsAppendOutcome> {
        self.compare_and_append_guarded(
            crate::GuardedAppend {
                authority_id,
                epoch,
                expected,
                commit,
                permit: PublicationPermit::Unrestricted,
            },
            budget,
            cancellation,
        )
        .await
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one atomic Stream transaction keeps fencing, idempotency, lineage, and terminal mapping auditable together"
    )]
    async fn compare_and_append_guarded(
        &self,
        request: crate::GuardedAppend,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<FsAppendOutcome> {
        let crate::GuardedAppend {
            authority_id,
            epoch,
            expected,
            commit,
            permit,
        } = request;
        cancellation
            .check()
            .map_err(|_| OperationFailure::before_work(AuthorityStoreError::Cancelled))?;
        if let Some(existing) = self
            .operation(authority_id, commit.operation_id)
            .await
            .map_err(OperationFailure::before_work)?
        {
            let value = if existing.fingerprint == commit.fingerprint {
                FsAppendOutcome::AlreadyCommitted(existing)
            } else {
                FsAppendOutcome::IdempotencyConflict {
                    committed_fingerprint: existing.fingerprint,
                }
            };
            return authority_success(value, authority_read_work(1), budget);
        }
        let snapshot = self
            .snapshot(authority_id)
            .await
            .map_err(OperationFailure::before_work)?;
        if snapshot.head.epoch != epoch {
            return authority_success(
                FsAppendOutcome::Fenced {
                    actual_epoch: snapshot.head.epoch,
                },
                authority_read_work(2),
                budget,
            );
        }
        if snapshot.head != expected {
            return authority_success(
                FsAppendOutcome::Conflict {
                    actual: snapshot.head,
                },
                authority_read_work(2),
                budget,
            );
        }
        let (gate_tail, active_reservation) = self
            .publication_gate(authority_id)
            .await
            .map_err(OperationFailure::before_work)?;
        let reservation_admitted = match permit {
            PublicationPermit::Reservation {
                operation_id,
                gate_tail: permitted_tail,
                expected: reserved_head,
            } => {
                permitted_tail == gate_tail
                    && active_reservation == Some(OperationId::from_bytes(operation_id))
                    && reserved_head == expected
            }
            PublicationPermit::Unrestricted => active_reservation.is_none(),
            PublicationPermit::Lease {
                authority_id: permitted_authority,
                workspace_id,
                ..
            } => {
                let permitted_workspace = crate::WorkspaceId::from_bytes(workspace_id);
                let derived_authority =
                    crate::kernel::volume_authority_id(permitted_workspace.volume_id());
                active_reservation.is_none()
                    && AuthorityId::from_bytes(permitted_authority) == authority_id
                    && derived_authority == authority_id
            }
        };
        if !reservation_admitted {
            return authority_success(
                FsAppendOutcome::Fenced {
                    actual_epoch: snapshot.head.epoch,
                },
                authority_read_work(3),
                budget,
            );
        }
        let sequence = expected
            .sequence
            .checked_next()
            .map_err(|_| OperationFailure::before_work(AuthorityStoreError::SequenceExhausted))?;
        let durable = DurableCommit {
            epoch,
            sequence,
            operation_id: commit.operation_id,
            fingerprint: commit.fingerprint,
            previous_digest: expected.digest,
            digest: authority_commit_digest(
                authority_id,
                epoch,
                sequence,
                commit.operation_id,
                commit.fingerprint,
                expected.digest,
                &commit.payload,
            ),
            payload: commit.payload,
        };
        let encoded =
            StreamsDurableRecord::encode(&durable, STREAM_RECORD_LIMIT).map_err(|error| {
                OperationFailure::before_work(AuthorityStoreError::Rejected(error.to_string()))
            })?;
        let records = records_path(authority_id).map_err(OperationFailure::before_work)?;
        let epochs = epochs_path(authority_id).map_err(OperationFailure::before_work)?;
        let gate = publication_gate_path(authority_id).map_err(OperationFailure::before_work)?;
        let mut conditions = vec![
            acyclic_stream::CommitCondition::Absent {
                path: retirement_path(authority_id).map_err(OperationFailure::before_work)?,
            },
            acyclic_stream::CommitCondition::Tail {
                path: records.clone(),
                expected: snapshot.record_tail,
            },
            acyclic_stream::CommitCondition::Tail {
                path: epochs,
                expected: snapshot.epoch_tail,
            },
            acyclic_stream::CommitCondition::Tail {
                path: gate.clone(),
                expected: gate_tail,
            },
        ];
        let mut lease_deadline = None;
        let lease_path = match permit {
            PublicationPermit::Unrestricted | PublicationPermit::Reservation { .. } => None,
            PublicationPermit::Lease {
                authority_id: _,
                workspace_id,
                lease_id,
                expires_at_millis,
            } => {
                lease_deadline = Some(expires_at_millis);
                let path = crate::operation_window::stream_lease_path(
                    crate::WorkspaceId::from_bytes(workspace_id),
                    crate::OperationLeaseId::from_bytes(lease_id),
                    expires_at_millis,
                )
                .map_err(|error| OperationFailure::before_work(map_stream_error(error)))?;
                conditions.push(acyclic_stream::CommitCondition::Tail {
                    path: path.clone(),
                    expected: 1,
                });
                Some(path)
            }
        };
        let mut mutations = vec![acyclic_stream::CommitMutation::Append {
            path: records,
            records: vec![encoded],
        }];
        let mut authority_records = 1_u64;
        let mut authority_bytes = u64::try_from(durable.payload.len()).unwrap_or(u64::MAX);
        if let Some(generation) = generation_from_payload(authority_id, sequence, &durable.payload)
            .map_err(OperationFailure::before_work)?
        {
            let lineage = lineage_path(authority_id).map_err(OperationFailure::before_work)?;
            let locator =
                generation_path(authority_id, generation).map_err(OperationFailure::before_work)?;
            match self.provider.tail(locator.clone()).await {
                Ok(1) => {
                    let record = read_one(self.provider.as_ref(), locator, 0)
                        .await
                        .map_err(OperationFailure::before_work)?;
                    let located_tail = decode_lineage_tail(&record.value)
                        .map_err(OperationFailure::before_work)?;
                    if located_tail == 0 {
                        return Err(OperationFailure::before_work(AuthorityStoreError::Corrupt(
                            "generation locator selected an empty lineage".to_owned(),
                        )));
                    }
                    let lineage_record =
                        read_one(self.provider.as_ref(), lineage, located_tail - 1)
                            .await
                            .map_err(OperationFailure::before_work)?;
                    if decode_lineage_generation(&lineage_record.value)
                        .map_err(OperationFailure::before_work)?
                        != generation
                    {
                        return Err(OperationFailure::before_work(AuthorityStoreError::Corrupt(
                            "generation locator does not identify its lineage record".to_owned(),
                        )));
                    }
                }
                Err(acyclic_stream::StreamError::NotFound) => {
                    let lineage_tail = match self.provider.tail(lineage.clone()).await {
                        Ok(tail) => tail,
                        Err(acyclic_stream::StreamError::NotFound) => 0,
                        Err(error) => {
                            return Err(OperationFailure::before_work(map_stream_error(error)));
                        }
                    };
                    conditions.push(if lineage_tail == 0 {
                        acyclic_stream::CommitCondition::Absent {
                            path: lineage.clone(),
                        }
                    } else {
                        acyclic_stream::CommitCondition::Tail {
                            path: lineage.clone(),
                            expected: lineage_tail,
                        }
                    });
                    conditions.push(acyclic_stream::CommitCondition::Absent {
                        path: locator.clone(),
                    });
                    let lineage_record = encode_lineage_generation(generation);
                    let locator_record = encode_lineage_tail(lineage_tail.saturating_add(1));
                    authority_bytes = authority_bytes
                        .saturating_add(u64::try_from(lineage_record.len()).unwrap_or(u64::MAX))
                        .saturating_add(u64::try_from(locator_record.len()).unwrap_or(u64::MAX));
                    mutations.push(acyclic_stream::CommitMutation::Append {
                        path: lineage,
                        records: vec![lineage_record],
                    });
                    mutations.push(acyclic_stream::CommitMutation::Append {
                        path: locator,
                        records: vec![locator_record],
                    });
                    authority_records = authority_records.saturating_add(2);
                }
                Ok(_) => {
                    return Err(OperationFailure::before_work(AuthorityStoreError::Corrupt(
                        "generation locator contains an invalid record count".to_owned(),
                    )));
                }
                Err(error) => {
                    return Err(OperationFailure::before_work(map_stream_error(error)));
                }
            }
        }
        let request = acyclic_stream::CommitRequest {
            conditions,
            mutations,
            idempotency_key: operation_key(authority_id, durable.operation_id)
                .map_err(OperationFailure::before_work)?,
        };
        let work = authority_write_work(authority_records, authority_bytes);
        admit_authority(work, budget)?;
        let commit_result = if let Some(deadline) = lease_deadline {
            self.provider.commit_before(request, deadline).await
        } else {
            self.provider.commit(request).await
        };
        match commit_result {
            Ok(acyclic_stream::CommitOutcome::Committed(_)) => {
                authority_success(FsAppendOutcome::Committed(durable), work, budget)
            }
            Ok(acyclic_stream::CommitOutcome::Conflict(conflicts)) => {
                if let Some(existing) = self
                    .operation(authority_id, durable.operation_id)
                    .await
                    .map_err(|error| OperationFailure::new(error, work))?
                {
                    let value = if existing.fingerprint == durable.fingerprint {
                        FsAppendOutcome::AlreadyCommitted(existing)
                    } else {
                        FsAppendOutcome::IdempotencyConflict {
                            committed_fingerprint: existing.fingerprint,
                        }
                    };
                    return authority_success(value, work, budget);
                }
                let actual = self
                    .snapshot(authority_id)
                    .await
                    .map_err(|error| OperationFailure::new(error, work))?
                    .head;
                let gate_rejected = conflicts.iter().any(|conflict| match conflict {
                    acyclic_stream::CommitConflict::Tail { path, .. }
                    | acyclic_stream::CommitConflict::Exists { path } => path == &gate,
                });
                let permit_rejected = gate_rejected
                    || lease_path.as_ref().is_some_and(|lease_path| {
                        conflicts.iter().any(|conflict| match conflict {
                            acyclic_stream::CommitConflict::Tail { path, .. }
                            | acyclic_stream::CommitConflict::Exists { path } => path == lease_path,
                        })
                    });
                let value = if permit_rejected {
                    FsAppendOutcome::Fenced {
                        actual_epoch: actual.epoch,
                    }
                } else if actual.epoch == epoch {
                    FsAppendOutcome::Conflict { actual }
                } else {
                    FsAppendOutcome::Fenced {
                        actual_epoch: actual.epoch,
                    }
                };
                authority_success(value, work, budget)
            }
            Err(acyclic_stream::StreamError::IdempotencyMismatch) => {
                let existing = self
                    .operation(authority_id, durable.operation_id)
                    .await
                    .map_err(|error| OperationFailure::new(error, work))?
                    .ok_or_else(|| {
                        OperationFailure::new(
                            AuthorityStoreError::Corrupt(
                                "Stream idempotency mismatch has no operation record".to_owned(),
                            ),
                            work,
                        )
                    })?;
                authority_success(
                    FsAppendOutcome::IdempotencyConflict {
                        committed_fingerprint: existing.fingerprint,
                    },
                    work,
                    budget,
                )
            }
            Err(acyclic_stream::StreamError::DeadlineElapsed) => authority_success(
                FsAppendOutcome::Fenced {
                    actual_epoch: epoch,
                },
                work,
                budget,
            ),
            Err(error) => Err(OperationFailure::new(map_stream_error(error), work)),
        }
    }

    async fn reserve_publication(
        &self,
        authority_id: AuthorityId,
        expected: Head,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<ReservationOutcome> {
        cancellation
            .check()
            .map_err(|_| OperationFailure::before_work(AuthorityStoreError::Cancelled))?;
        let snapshot = self
            .snapshot(authority_id)
            .await
            .map_err(OperationFailure::before_work)?;
        if snapshot.head != expected {
            return authority_success(
                ReservationOutcome::Conflict {
                    actual: snapshot.head,
                },
                authority_read_work(2),
                budget,
            );
        }
        let gate = publication_gate_path(authority_id).map_err(OperationFailure::before_work)?;
        let (gate_tail, active) = self
            .publication_gate(authority_id)
            .await
            .map_err(OperationFailure::before_work)?;
        if active == Some(operation_id) {
            return authority_success(
                ReservationOutcome::AlreadyReserved(PublicationReservation {
                    authority_id,
                    operation_id,
                    expected,
                    gate_tail,
                }),
                authority_read_work(3),
                budget,
            );
        }
        if active.is_some() {
            return authority_success(
                ReservationOutcome::Conflict {
                    actual: snapshot.head,
                },
                authority_read_work(3),
                budget,
            );
        }
        let records = records_path(authority_id).map_err(OperationFailure::before_work)?;
        let epochs = epochs_path(authority_id).map_err(OperationFailure::before_work)?;
        let request = acyclic_stream::CommitRequest {
            conditions: vec![
                acyclic_stream::CommitCondition::Absent {
                    path: retirement_path(authority_id).map_err(OperationFailure::before_work)?,
                },
                acyclic_stream::CommitCondition::Tail {
                    path: records,
                    expected: snapshot.record_tail,
                },
                acyclic_stream::CommitCondition::Tail {
                    path: epochs,
                    expected: snapshot.epoch_tail,
                },
                acyclic_stream::CommitCondition::Tail {
                    path: gate.clone(),
                    expected: gate_tail,
                },
            ],
            mutations: vec![acyclic_stream::CommitMutation::Append {
                path: gate,
                records: vec![encode_active_reservation(operation_id)],
            }],
            idempotency_key: publication_reservation_key(
                b"reserve-publication",
                authority_id,
                operation_id,
                gate_tail,
            )
            .map_err(OperationFailure::before_work)?,
        };
        let work = authority_write_work(1, 16);
        admit_authority(work, budget)?;
        match self.provider.commit(request).await {
            Ok(acyclic_stream::CommitOutcome::Committed(_)) => authority_success(
                ReservationOutcome::Reserved(PublicationReservation {
                    authority_id,
                    operation_id,
                    expected,
                    gate_tail: gate_tail + 1,
                }),
                work,
                budget,
            ),
            Ok(acyclic_stream::CommitOutcome::Conflict(_)) => {
                let actual = self
                    .snapshot(authority_id)
                    .await
                    .map_err(|error| OperationFailure::new(error, work))?
                    .head;
                authority_success(ReservationOutcome::Conflict { actual }, work, budget)
            }
            Err(error) => Err(OperationFailure::new(map_stream_error(error), work)),
        }
    }

    async fn release_publication(
        &self,
        reservation: PublicationReservation,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<()> {
        cancellation
            .check()
            .map_err(|_| OperationFailure::before_work(AuthorityStoreError::Cancelled))?;
        let release_key = publication_reservation_key(
            b"release-publication",
            reservation.authority_id,
            reservation.operation_id,
            reservation.gate_tail,
        )
        .map_err(OperationFailure::before_work)?;
        // The atomic release can commit even when its response is lost. Check the
        // durable idempotency record before inspecting the now-free gate so an
        // exact retry remains successful after the visible gate has advanced.
        let mut work = authority_read_work(1);
        admit_authority(work, budget)?;
        if let Some(observation) = self
            .provider
            .inspect_idempotency(release_key.clone())
            .await
            .map_err(|error| OperationFailure::new(map_stream_error(error), work))?
        {
            return match observation.outcome {
                acyclic_stream::IdempotencyOutcome::Commit(
                    acyclic_stream::CommitOutcome::Committed(_),
                ) => authority_success((), work, budget),
                acyclic_stream::IdempotencyOutcome::Commit(
                    acyclic_stream::CommitOutcome::Conflict(_),
                ) => Err(OperationFailure::new(
                    AuthorityStoreError::Rejected(
                        "publication reservation is no longer active".to_owned(),
                    ),
                    work,
                )),
                _ => Err(OperationFailure::new(
                    AuthorityStoreError::Corrupt(
                        "publication release identity retained another Stream mutation family"
                            .to_owned(),
                    ),
                    work,
                )),
            };
        }
        let gate = publication_gate_path(reservation.authority_id)
            .map_err(|error| OperationFailure::new(error, work))?;
        work = work
            .checked_add(authority_read_work(2))
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        admit_authority(work, budget)?;
        let (current_tail, active) = self
            .publication_gate(reservation.authority_id)
            .await
            .map_err(|error| OperationFailure::new(error, work))?;
        if current_tail != reservation.gate_tail || active != Some(reservation.operation_id) {
            return Err(OperationFailure::new(
                AuthorityStoreError::Rejected(
                    "publication reservation is no longer owned by this operation".to_owned(),
                ),
                work,
            ));
        }
        let request = acyclic_stream::CommitRequest {
            conditions: vec![
                acyclic_stream::CommitCondition::Absent {
                    path: retirement_path(reservation.authority_id)
                        .map_err(OperationFailure::before_work)?,
                },
                acyclic_stream::CommitCondition::Tail {
                    path: gate.clone(),
                    expected: reservation.gate_tail,
                },
            ],
            mutations: vec![acyclic_stream::CommitMutation::Append {
                path: gate,
                records: vec![Bytes::from_static(PUBLICATION_GATE_FREE)],
            }],
            idempotency_key: release_key,
        };
        work = work
            .checked_add(authority_write_work(1, 0))
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        admit_authority(work, budget)?;
        match self.provider.commit(request).await {
            Ok(acyclic_stream::CommitOutcome::Committed(_)) => authority_success((), work, budget),
            Ok(acyclic_stream::CommitOutcome::Conflict(_)) => Err(OperationFailure::new(
                AuthorityStoreError::Rejected(
                    "publication reservation is no longer active".to_owned(),
                ),
                work,
            )),
            Err(acyclic_stream::StreamError::IdempotencyMismatch) => Err(OperationFailure::new(
                AuthorityStoreError::Rejected(
                    "publication reservation release identity was reused".to_owned(),
                ),
                work,
            )),
            Err(error) => Err(OperationFailure::new(map_stream_error(error), work)),
        }
    }

    async fn replay(
        &self,
        authority_id: AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Vec<DurableCommit>> {
        cancellation
            .check()
            .map_err(|_| OperationFailure::before_work(AuthorityStoreError::Cancelled))?;
        if limit.records == 0 || limit.payload_bytes == 0 {
            return Err(OperationFailure::before_work(
                AuthorityStoreError::InvalidReplayLimit,
            ));
        }
        let path = records_path(authority_id).map_err(OperationFailure::before_work)?;
        let records = u64::from(limit.records);
        // Stream reads have a record count, but no byte allowance. Admit the
        // global encoded-record ceiling before requesting the bounded batch.
        admit_snapshot_read(
            WorkCounters::default(),
            WorkCounters {
                authority_records_read: records,
                authority_bytes_read: STREAM_RECORD_LIMIT.saturating_mul(records),
                backend_read_operations: 1,
                ..WorkCounters::default()
            },
            budget,
            cancellation,
        )?;
        let mut work = WorkCounters {
            backend_read_operations: 1,
            ..WorkCounters::default()
        };
        let mut stream = self
            .provider
            .read(acyclic_stream::ReadRequest {
                path,
                from: after.get().saturating_add(1),
                limit: limit.records,
            })
            .await
            .map_err(|error| OperationFailure::new(map_stream_error(error), work))?;
        let mut commits = Vec::new();
        let mut payload_bytes = 0_u64;
        while work.authority_records_read < records {
            cancellation
                .check()
                .map_err(|_| OperationFailure::new(AuthorityStoreError::Cancelled, work))?;
            let Some(record) = stream.next().await else {
                break;
            };
            let record =
                record.map_err(|error| OperationFailure::new(map_stream_error(error), work))?;
            work = work
                .checked_add(WorkCounters {
                    authority_records_read: 1,
                    authority_bytes_read: u64::try_from(record.value.len()).unwrap_or(u64::MAX),
                    ..WorkCounters::default()
                })
                .map_err(|error| OperationFailure::new(error.into(), work))?;
            cancellation
                .check()
                .map_err(|_| OperationFailure::new(AuthorityStoreError::Cancelled, work))?;
            let durable = decode_durable(authority_id, &record.value)
                .map_err(|error| OperationFailure::new(error, work))?;
            payload_bytes = payload_bytes
                .checked_add(u64::try_from(durable.payload.len()).unwrap_or(u64::MAX))
                .ok_or_else(|| {
                    OperationFailure::new(
                        AuthorityStoreError::ReplayRecordTooLarge {
                            observed: u64::MAX,
                            maximum: limit.payload_bytes,
                        },
                        work,
                    )
                })?;
            if payload_bytes > limit.payload_bytes {
                if commits.is_empty() {
                    return Err(OperationFailure::new(
                        AuthorityStoreError::ReplayRecordTooLarge {
                            observed: payload_bytes,
                            maximum: limit.payload_bytes,
                        },
                        work,
                    ));
                }
                break;
            }
            commits.push(durable);
        }
        authority_success(commits, work, budget)
    }

    async fn fence(
        &self,
        authority_id: AuthorityId,
        expected: Head,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<FenceOutcome> {
        cancellation
            .check()
            .map_err(|_| OperationFailure::before_work(AuthorityStoreError::Cancelled))?;
        let snapshot = self
            .snapshot(authority_id)
            .await
            .map_err(OperationFailure::before_work)?;
        if snapshot.head != expected {
            return authority_success(
                FenceOutcome::Conflict {
                    actual: snapshot.head,
                },
                authority_read_work(2),
                budget,
            );
        }
        let next =
            expected.epoch.get().checked_add(1).ok_or_else(|| {
                OperationFailure::before_work(AuthorityStoreError::EpochExhausted)
            })?;
        let epoch = Epoch::new(next)
            .map_err(|_| OperationFailure::before_work(AuthorityStoreError::EpochExhausted))?;
        let records = records_path(authority_id).map_err(OperationFailure::before_work)?;
        let epochs = epochs_path(authority_id).map_err(OperationFailure::before_work)?;
        let mut fence_identity = Vec::with_capacity(64);
        fence_identity.extend_from_slice(&authority_id.into_bytes());
        fence_identity.extend_from_slice(&expected.epoch.get().to_le_bytes());
        fence_identity.extend_from_slice(&expected.sequence.get().to_le_bytes());
        fence_identity.extend_from_slice(expected.digest.as_bytes());
        let request = acyclic_stream::CommitRequest {
            conditions: vec![
                acyclic_stream::CommitCondition::Absent {
                    path: retirement_path(authority_id).map_err(OperationFailure::before_work)?,
                },
                acyclic_stream::CommitCondition::Tail {
                    path: records,
                    expected: snapshot.record_tail,
                },
                acyclic_stream::CommitCondition::Tail {
                    path: epochs.clone(),
                    expected: snapshot.epoch_tail,
                },
            ],
            mutations: vec![acyclic_stream::CommitMutation::Append {
                path: epochs,
                records: vec![encode_epoch(EPOCH_DOMAIN, epoch)],
            }],
            idempotency_key: stream_key(b"fence", &fence_identity)
                .map_err(OperationFailure::before_work)?,
        };
        let work = authority_write_work(1, 0);
        admit_authority(work, budget)?;
        match self.provider.commit(request).await {
            Ok(acyclic_stream::CommitOutcome::Committed(_)) => authority_success(
                FenceOutcome::Advanced(Head { epoch, ..expected }),
                work,
                budget,
            ),
            Ok(acyclic_stream::CommitOutcome::Conflict(_)) => {
                let actual = self
                    .snapshot(authority_id)
                    .await
                    .map_err(|error| OperationFailure::new(error, work))?
                    .head;
                authority_success(FenceOutcome::Conflict { actual }, work, budget)
            }
            Err(error) => Err(OperationFailure::new(map_stream_error(error), work)),
        }
    }

    async fn find_operation(
        &self,
        authority_id: AuthorityId,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Option<DurableCommit>> {
        let work = admit_snapshot_read(
            WorkCounters::default(),
            WorkCounters {
                backend_read_operations: 1,
                ..WorkCounters::default()
            },
            budget,
            cancellation,
        )?;
        let value = self
            .operation(authority_id, operation_id)
            .await
            .map_err(|error| OperationFailure::new(error, work))?;
        cancellation
            .check()
            .map_err(|_| OperationFailure::new(AuthorityStoreError::Cancelled, work))?;
        let records_read = u64::from(value.is_some());
        authority_success(
            value,
            WorkCounters {
                authority_records_read: records_read,
                backend_read_operations: 1,
                ..WorkCounters::default()
            },
            budget,
        )
    }
}

/// The object an [`object_key`] names.
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub(crate) fn object_id_from_key(key: &str) -> Option<ObjectId> {
    let (tag, digest) = key.strip_prefix("fs/v1/")?.split_once('/')?;
    let kind = crate::storage::ObjectKind::from_canonical_tag(tag.parse().ok()?).ok()?;
    let digest = <[u8; 32]>::try_from(hex::decode(digest).ok()?).ok()?;
    let object = ObjectId {
        kind,
        digest: crate::foundation::Digest::from_bytes(digest),
    };
    (object_key(object) == key).then_some(object)
}

pub(crate) fn object_key(object_id: ObjectId) -> String {
    format!(
        "fs/v1/{}/{}",
        object_id.kind.canonical_tag(),
        hex::encode(object_id.digest.as_bytes())
    )
}

/// The authority a direct child of `fs/authorities` names.
fn authority_child(child: &acyclic_stream::StreamPath) -> Result<AuthorityId, AuthorityStoreError> {
    let encoded = child
        .as_str()
        .strip_prefix("fs/authorities/")
        .ok_or_else(|| {
            AuthorityStoreError::Corrupt("Stream authority child escaped its parent".to_owned())
        })?;
    if encoded.len() != 32 || encoded.contains('/') {
        return Err(AuthorityStoreError::Corrupt(
            "Stream authority child has a noncanonical identity".to_owned(),
        ));
    }
    let mut bytes = [0_u8; 16];
    hex::decode_to_slice(encoded, &mut bytes).map_err(|_| {
        AuthorityStoreError::Corrupt(
            "Stream authority child has a noncanonical identity".to_owned(),
        )
    })?;
    Ok(AuthorityId::from_bytes(bytes))
}

fn authority_prefix(authority_id: AuthorityId) -> String {
    format!("fs/authorities/{}", hex::encode(authority_id.into_bytes()))
}

fn authority_path(
    authority_id: AuthorityId,
) -> Result<acyclic_stream::StreamPath, AuthorityStoreError> {
    acyclic_stream::StreamPath::new(authority_prefix(authority_id)).map_err(map_stream_error)
}

fn records_path(
    authority_id: AuthorityId,
) -> Result<acyclic_stream::StreamPath, AuthorityStoreError> {
    acyclic_stream::StreamPath::new(format!("{}/records", authority_prefix(authority_id)))
        .map_err(map_stream_error)
}

fn retirement_path(
    authority_id: AuthorityId,
) -> Result<acyclic_stream::StreamPath, AuthorityStoreError> {
    acyclic_stream::StreamPath::new(format!("{}/retirement", authority_prefix(authority_id)))
        .map_err(map_stream_error)
}

fn epochs_path(
    authority_id: AuthorityId,
) -> Result<acyclic_stream::StreamPath, AuthorityStoreError> {
    acyclic_stream::StreamPath::new(format!("{}/epochs", authority_prefix(authority_id)))
        .map_err(map_stream_error)
}

fn publication_gate_path(
    authority_id: AuthorityId,
) -> Result<acyclic_stream::StreamPath, AuthorityStoreError> {
    acyclic_stream::StreamPath::new(format!(
        "{}/publication-gate",
        authority_prefix(authority_id)
    ))
    .map_err(map_stream_error)
}

fn encode_active_reservation(operation_id: OperationId) -> Bytes {
    let mut value = Vec::with_capacity(PUBLICATION_GATE_ACTIVE.len() + 16);
    value.extend_from_slice(PUBLICATION_GATE_ACTIVE);
    value.extend_from_slice(&operation_id.into_bytes());
    Bytes::from(value)
}

fn decode_publication_gate(value: &[u8]) -> Result<Option<OperationId>, AuthorityStoreError> {
    if value == PUBLICATION_GATE_FREE {
        return Ok(None);
    }
    let operation = value
        .strip_prefix(PUBLICATION_GATE_ACTIVE)
        .and_then(|bytes| <[u8; 16]>::try_from(bytes).ok())
        .ok_or_else(|| {
            AuthorityStoreError::Corrupt("invalid publication gate record".to_owned())
        })?;
    Ok(Some(OperationId::from_bytes(operation)))
}

fn durable_from_operation_envelope(
    authority_id: AuthorityId,
    operation_id: OperationId,
    envelope: acyclic_stream::CommittedEnvelope,
) -> Result<DurableCommit, AuthorityStoreError> {
    let records = records_path(authority_id)?;
    let mut retained = None;
    for mutation in envelope.mutations {
        let acyclic_stream::CommittedMutation::Append(append) = mutation else {
            continue;
        };
        if append.path != records {
            continue;
        }
        // An operation that created its authority appends the genesis
        // record before its own.
        let record = match append.records.as_slice() {
            [record] => record,
            [genesis, record] if decode_epoch(&genesis.value, GENESIS_DOMAIN).is_ok() => record,
            _ => {
                return Err(AuthorityStoreError::Corrupt(
                    "operation appended an invalid authority record count".to_owned(),
                ));
            }
        };
        let durable = decode_durable(authority_id, &record.value)?;
        if durable.operation_id != operation_id || retained.replace(durable).is_some() {
            return Err(AuthorityStoreError::Corrupt(
                "operation envelope does not bind its authority operation".to_owned(),
            ));
        }
    }
    retained.ok_or_else(|| {
        AuthorityStoreError::Corrupt("operation envelope omits its authority record".to_owned())
    })
}

fn lineage_path(
    authority_id: AuthorityId,
) -> Result<acyclic_stream::StreamPath, AuthorityStoreError> {
    acyclic_stream::StreamPath::new(format!("{}/lineage", authority_prefix(authority_id)))
        .map_err(map_stream_error)
}

fn generation_path(
    authority_id: AuthorityId,
    generation: GenerationId,
) -> Result<acyclic_stream::StreamPath, AuthorityStoreError> {
    acyclic_stream::StreamPath::new(format!(
        "{}/generations/{}",
        authority_prefix(authority_id),
        hex::encode(generation.digest().as_bytes())
    ))
    .map_err(map_stream_error)
}

fn stream_key(
    domain: &[u8],
    identity: &[u8],
) -> Result<acyclic_stream::IdempotencyKey, AuthorityStoreError> {
    let mut value = Vec::with_capacity(
        domain
            .len()
            .saturating_add(identity.len())
            .saturating_add(1),
    );
    value.extend_from_slice(domain);
    value.push(0);
    value.extend_from_slice(identity);
    acyclic_stream::IdempotencyKey::new(Bytes::from(value)).map_err(map_stream_error)
}

impl<P: acyclic_stream::StreamProvider> StreamAuthorityStore<P> {
    async fn fork_generation_with_owner_guard<F, Fut>(
        &self,
        source: crate::GenerationForkSource,
        destination_authority: AuthorityId,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        mut guard: Option<&mut F>,
    ) -> crate::FsResult<CreateAuthorityOutcome>
    where
        F: FnMut(WorkBudget) -> Fut + Send,
        Fut: Future<Output = crate::FsResult<()>> + Send,
    {
        if source.lineage == crate::GenerationFork::Independent {
            return match guard.as_deref_mut() {
                Some(guard) => self.create_authority_guarded(destination_authority, Epoch::GENESIS, budget, cancellation, guard).await,
                None => self.create_authority(destination_authority, Epoch::GENESIS, budget, cancellation).await
                    .map(|receipt| crate::OperationReceipt { value: receipt.value, work: receipt.work })
                    .map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work)),
            };
        }
        let mut meter = ForkWork::new(budget, cancellation);
        meter.check().map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?;
        let (source_lineage, forked_at) = self.resolve_source_fork_point(source.authority, source.generation, &mut meter).await
            .map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?;
        if let Some(snapshot) = self.existing_fork_destination(destination_authority, source.generation, forked_at, &mut meter).await
            .map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?
        {
            self.fork_gate(destination_authority, &mut meter).await
                .map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?;
            seal_fork_guard(&mut meter, &mut guard).await?;
            return Ok(crate::OperationReceipt { value: CreateAuthorityOutcome::Existing(snapshot.head), work: meter.work });
        }
        let destination = ForkDestinationPaths {
            root: meter.path(destination_authority, "", None).map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?,
            records: meter.path(destination_authority, "/records", None).map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?,
            epochs: meter.path(destination_authority, "/epochs", None).map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?,
            locator: meter.path(destination_authority, "/generations/", Some(source.generation)).map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?,
            lineage: meter.path(destination_authority, "/lineage", None).map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?,
            gate: meter.path(destination_authority, "/publication-gate", None).map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?,
        };
        let request = fork_generation_commit_request(destination, source_lineage, forked_at, operation_id, &mut meter)
            .map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?;
        let write_work = meter.request_write_work(&request)
            .map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?;
        meter.admit(write_work).map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?;
        seal_fork_guard(&mut meter, &mut guard).await?;
        // The guard may have consumed work: readmit before the physical write.
        meter.admit(write_work).map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?;
        let committed = self.provider.commit(request).await;
        meter.charge(commit_work(write_work, &committed)).map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?;
        meter.release_request().map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?;
        match committed {
            Ok(acyclic_stream::CommitOutcome::Committed(_)) => {
                // The native ACK may be a retained idempotent result; seal its
                // recovery without guessing physical publication freshness.
                seal_fork_guard(&mut meter, &mut guard).await?;
                Ok(crate::OperationReceipt {
                    value: CreateAuthorityOutcome::Created(Head::genesis(Epoch::GENESIS)), work: meter.work,
                })
            }
            Ok(acyclic_stream::CommitOutcome::Conflict(_)) | Err(acyclic_stream::StreamError::IdempotencyMismatch) => {
                let snapshot = self.existing_fork_destination(destination_authority, source.generation, forked_at, &mut meter).await
                    .map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?
                    .ok_or_else(|| OperationFailure::new(crate::FsError::from(AuthorityStoreError::Rejected(
                        "generation fork conflicted without creating its destination".to_owned())), meter.work))?;
                self.fork_gate(destination_authority, &mut meter).await
                    .map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?;
                seal_fork_guard(&mut meter, &mut guard).await?;
                Ok(crate::OperationReceipt { value: CreateAuthorityOutcome::Existing(snapshot.head), work: meter.work })
            }
            Err(error) => Err(OperationFailure::new(map_stream_error(error).into(), meter.work)),
        }
    }
}

/// Destination paths materialized by one authority-fork commit.
struct ForkDestinationPaths {
    root: acyclic_stream::StreamPath,
    records: acyclic_stream::StreamPath,
    epochs: acyclic_stream::StreamPath,
    locator: acyclic_stream::StreamPath,
    lineage: acyclic_stream::StreamPath,
    gate: acyclic_stream::StreamPath,
}

/// Builds the one atomic commit that materializes a forked authority's root,
/// records, epochs, generation locator, and lineage fork in a single request.
fn fork_generation_commit_request(
    destination: ForkDestinationPaths,
    source_lineage: acyclic_stream::StreamPath,
    forked_at: u64,
    operation_id: OperationId,
    meter: &mut ForkWork<'_>,
) -> Result<acyclic_stream::CommitRequest, OperationFailure<AuthorityStoreError>> {
    use acyclic_stream::{CommitCondition, CommitMutation};
    let mut conditions = meter.vec::<CommitCondition>(6)?;
    for path in [&destination.root, &destination.records, &destination.epochs,
        &destination.locator, &destination.lineage, &destination.gate] {
        let path = meter.clone_path(path)?;
        meter.push(&mut conditions, CommitCondition::Absent { path })?;
    }
    let mut mutations = meter.vec::<CommitMutation>(6)?;
    let mut marker = meter.vec::<Bytes>(1)?;
    meter.push(&mut marker, Bytes::from_static(AUTHORITY_MARKER))?;
    meter.push(&mut mutations, CommitMutation::Append { path: destination.root, records: marker })?;
    meter.push(&mut mutations, CommitMutation::Fork {
        source: source_lineage, destination: destination.lineage,
        at_tail: forked_at, records: Vec::new(),
    })?;
    let genesis = meter.bytes(GENESIS_DOMAIN, &[&Epoch::GENESIS.get().to_le_bytes()])?;
    let mut records = meter.vec::<Bytes>(1)?;
    meter.push(&mut records, genesis)?;
    meter.push(&mut mutations, CommitMutation::Append { path: destination.records, records })?;
    let epoch = meter.bytes(EPOCH_DOMAIN, &[&Epoch::GENESIS.get().to_le_bytes()])?;
    let mut epochs = meter.vec::<Bytes>(1)?;
    meter.push(&mut epochs, epoch)?;
    meter.push(&mut mutations, CommitMutation::Append { path: destination.epochs, records: epochs })?;
    let locator = meter.bytes(LINEAGE_TAIL_DOMAIN, &[&forked_at.to_le_bytes()])?;
    let mut locators = meter.vec::<Bytes>(1)?;
    meter.push(&mut locators, locator)?;
    meter.push(&mut mutations, CommitMutation::Append { path: destination.locator, records: locators })?;
    let mut gate = meter.vec::<Bytes>(1)?;
    meter.push(&mut gate, Bytes::from_static(PUBLICATION_GATE_FREE))?;
    meter.push(&mut mutations, CommitMutation::Append { path: destination.gate, records: gate })?;
    let idempotency_key = meter.stream_key(b"fork-authority", &operation_id.into_bytes())?;
    Ok(acyclic_stream::CommitRequest { conditions, mutations, idempotency_key })
}

fn operation_key(
    authority_id: AuthorityId,
    operation_id: OperationId,
) -> Result<acyclic_stream::IdempotencyKey, AuthorityStoreError> {
    let mut identity = [0; 32];
    identity[..16].copy_from_slice(&authority_id.into_bytes());
    identity[16..].copy_from_slice(&operation_id.into_bytes());
    stream_key(b"operation", &identity)
}

fn publication_reservation_key(
    domain: &[u8],
    authority_id: AuthorityId,
    operation_id: OperationId,
    gate_tail: u64,
) -> Result<acyclic_stream::IdempotencyKey, AuthorityStoreError> {
    let mut identity = Vec::with_capacity(40);
    identity.extend_from_slice(&authority_id.into_bytes());
    identity.extend_from_slice(&operation_id.into_bytes());
    identity.extend_from_slice(&gate_tail.to_le_bytes());
    stream_key(domain, &identity)
}

fn encode_epoch(domain: &[u8], epoch: Epoch) -> Bytes {
    let mut value = Vec::with_capacity(domain.len().saturating_add(8));
    value.extend_from_slice(domain);
    value.extend_from_slice(&epoch.get().to_le_bytes());
    Bytes::from(value)
}

fn generation_from_payload(
    authority_id: AuthorityId,
    sequence: Sequence,
    payload: &[u8],
) -> Result<Option<GenerationId>, AuthorityStoreError> {
    // Both canonical facts are fixed-width, borrowed decodes. Opaque records must
    // not manufacture allocating decoder diagnostics merely to detect their type.
    const CREATED: &[u8] = b"acyclic-fs-volume-created-v1\0";
    const PUBLISHED: &[u8] = b"acyclic-fs-publish-generation-v1\0";
    if let Some(created) = (payload.len() == CREATED.len() + 2 + 16 + crate::kernel::CONFIG_ENCODED_BYTES + 1 + 32
        && payload.starts_with(CREATED))
        .then(|| decode_volume_created(payload, STREAM_RECORD_LIMIT)).and_then(Result::ok) {
        if sequence != Sequence::new(1) || volume_authority_id(created.volume_id) != authority_id {
            return Err(AuthorityStoreError::Corrupt(
                "volume creation identity or sequence does not match its authority".to_owned(),
            ));
        }
        return Ok(Some(GenerationId::new(
            created.initial_generation_root.digest,
        )));
    }
    if let Some(published) = (payload.len() == PUBLISHED.len() + 2 + 16 + 1 + 32
        && payload.starts_with(PUBLISHED))
        .then(|| decode_published_generation(payload, STREAM_RECORD_LIMIT)).and_then(Result::ok) {
        if sequence == Sequence::new(1) || volume_authority_id(published.volume_id) != authority_id
        {
            return Err(AuthorityStoreError::Corrupt(
                "generation publication identity or sequence does not match its authority"
                    .to_owned(),
            ));
        }
        return Ok(Some(GenerationId::new(published.generation_root.digest)));
    }
    Ok(None)
}

fn encode_lineage_generation(generation: GenerationId) -> Bytes {
    let mut value = Vec::with_capacity(LINEAGE_DOMAIN.len().saturating_add(32));
    value.extend_from_slice(LINEAGE_DOMAIN);
    value.extend_from_slice(generation.digest().as_bytes());
    Bytes::from(value)
}

fn decode_lineage_generation(encoded: &[u8]) -> Result<GenerationId, AuthorityStoreError> {
    if encoded.len() != LINEAGE_DOMAIN.len().saturating_add(32)
        || !encoded.starts_with(LINEAGE_DOMAIN)
    {
        return Err(AuthorityStoreError::Corrupt(
            "invalid Stream generation-lineage record".to_owned(),
        ));
    }
    let digest = encoded
        .get(LINEAGE_DOMAIN.len()..)
        .ok_or_else(|| {
            AuthorityStoreError::Corrupt("truncated Stream generation-lineage record".to_owned())
        })?
        .try_into()
        .map(Digest::from_bytes)
        .map_err(|_| {
            AuthorityStoreError::Corrupt("truncated Stream generation-lineage record".to_owned())
        })?;
    Ok(GenerationId::new(digest))
}

fn encode_lineage_tail(tail: u64) -> Bytes {
    let mut value = Vec::with_capacity(LINEAGE_TAIL_DOMAIN.len().saturating_add(8));
    value.extend_from_slice(LINEAGE_TAIL_DOMAIN);
    value.extend_from_slice(&tail.to_le_bytes());
    Bytes::from(value)
}

fn decode_lineage_tail(encoded: &[u8]) -> Result<u64, AuthorityStoreError> {
    if encoded.len() != LINEAGE_TAIL_DOMAIN.len().saturating_add(8)
        || !encoded.starts_with(LINEAGE_TAIL_DOMAIN)
    {
        return Err(AuthorityStoreError::Corrupt(
            "invalid Stream generation locator".to_owned(),
        ));
    }
    let bytes: [u8; 8] = encoded
        .get(LINEAGE_TAIL_DOMAIN.len()..)
        .ok_or_else(|| AuthorityStoreError::Corrupt("truncated generation locator".to_owned()))?
        .try_into()
        .map_err(|_| AuthorityStoreError::Corrupt("truncated generation locator".to_owned()))?;
    Ok(u64::from_le_bytes(bytes))
}

fn decode_epoch(encoded: &[u8], domain: &[u8]) -> Result<Epoch, AuthorityStoreError> {
    if encoded.len() != domain.len().saturating_add(8) || !encoded.starts_with(domain) {
        return Err(AuthorityStoreError::Corrupt(
            "invalid Stream epoch record".to_owned(),
        ));
    }
    let bytes: [u8; 8] = encoded
        .get(domain.len()..)
        .ok_or_else(|| AuthorityStoreError::Corrupt("truncated Stream epoch record".to_owned()))?
        .try_into()
        .map_err(|_| AuthorityStoreError::Corrupt("truncated Stream epoch record".to_owned()))?;
    Epoch::new(u64::from_le_bytes(bytes))
        .map_err(|error| AuthorityStoreError::Corrupt(error.to_string()))
}

async fn read_one<P: acyclic_stream::StreamProvider>(
    provider: &P,
    path: acyclic_stream::StreamPath,
    from: u64,
) -> Result<acyclic_stream::Record, AuthorityStoreError> {
    let mut records = provider
        .read(acyclic_stream::ReadRequest {
            path,
            from,
            limit: 1,
        })
        .await
        .map_err(map_stream_error)?;
    let record = records
        .next()
        .await
        .ok_or_else(|| {
            AuthorityStoreError::Corrupt("Stream returned an empty exact read".to_owned())
        })?
        .map_err(map_stream_error)?;
    if record.sequence != from {
        return Err(AuthorityStoreError::Corrupt(
            "Stream returned a non-contiguous record".to_owned(),
        ));
    }
    Ok(record)
}

fn decode_durable(
    authority_id: AuthorityId,
    encoded: &[u8],
) -> Result<DurableCommit, AuthorityStoreError> {
    let durable = StreamsDurableRecord::decode(encoded, STREAM_RECORD_LIMIT)
        .map_err(|error| AuthorityStoreError::Corrupt(error.to_string()))?
        .0;
    let expected = authority_commit_digest(
        authority_id,
        durable.epoch,
        durable.sequence,
        durable.operation_id,
        durable.fingerprint,
        durable.previous_digest,
        &durable.payload,
    );
    if expected != durable.digest {
        return Err(AuthorityStoreError::Corrupt(
            "Stream authority record digest mismatch".to_owned(),
        ));
    }
    Ok(durable)
}

fn map_stream_error(error: acyclic_stream::StreamError) -> AuthorityStoreError {
    match error {
        acyclic_stream::StreamError::NotFound => AuthorityStoreError::Missing,
        acyclic_stream::StreamError::Capacity => {
            AuthorityStoreError::Rejected("Stream capacity exhausted".to_owned())
        }
        other => AuthorityStoreError::Rejected(other.to_string()),
    }
}

fn admit_snapshot_read(
    work: WorkCounters,
    proposed: WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<WorkCounters, OperationFailure<AuthorityStoreError>> {
    cancellation
        .check()
        .map_err(|_| OperationFailure::new(AuthorityStoreError::Cancelled, work))?;
    let next = work
        .checked_add(proposed)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    next.verify(budget)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    Ok(next)
}

fn authority_read_work(records: u64) -> WorkCounters {
    WorkCounters {
        authority_records_read: records,
        backend_read_operations: records,
        ..WorkCounters::default()
    }
}

fn authority_write_work(records: u64, payload_bytes: u64) -> WorkCounters {
    WorkCounters {
        authority_records_appended: records,
        authority_bytes_written: payload_bytes,
        backend_write_operations: 1,
        durability_operations: 1,
        ..WorkCounters::default()
    }
}

fn admit_authority(
    work: WorkCounters,
    budget: WorkBudget,
) -> Result<(), OperationFailure<AuthorityStoreError>> {
    work.verify(budget)
        .map_err(|error| OperationFailure::new(error.into(), work))
}

fn authority_success<T>(value: T, work: WorkCounters, budget: WorkBudget) -> AuthorityResult<T> {
    admit_authority(work, budget)?;
    Ok(AuthorityReceipt { value, work })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::{VolumeCreated, encode_publication_payload, encode_volume_created};
    use crate::model::{Lifecycle, VolumeConfig};
    use crate::storage::ObjectKind;

    #[test]
    fn fork_builders_reject_before_physical_work_and_preserve_completed_hash() -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let mut budget = WorkBudget::UNBOUNDED;
        budget.allocation_operations = 0;
        let mut meter = ForkWork::new(budget, &cancellation);
        let failure = meter.bytes(b"record", &[b"payload"]).err().ok_or("allocation denial succeeded")?;
        assert_eq!(failure.work.bytes_encoded, 0);
        assert_eq!(failure.work.bytes_copied, 0);
        assert_eq!(failure.work.allocation_operations, 0);
        assert_eq!(failure.work.peak_allocation_bytes, 0);
        let failure = meter.path(AuthorityId::from_bytes([7; 16]), "/records", None)
            .err().ok_or("path allocation denial succeeded")?;
        assert_eq!(failure.work.bytes_encoded, 0);
        let mut request = acyclic_stream::CommitRequest {
            conditions: Vec::new(), mutations: Vec::new(),
            idempotency_key: operation_key(AuthorityId::from_bytes([1; 16]), OperationId::from_bytes([2; 16]))?,
        };
        let commit = ProposedCommit {
            operation_id: OperationId::from_bytes([2; 16]), fingerprint: Digest::from_bytes([3; 32]),
            payload: Bytes::from_static(b"opaque-input"),
        };
        let failure = first_record_commit(&mut request, AuthorityId::from_bytes([1; 16]), &commit, None, &mut meter)
            .err().ok_or("first record allocation denial succeeded")?;
        assert_eq!(failure.work.bytes_hashed, crate::foundation::AUTHORITY_COMMIT_DIGEST_ENVELOPE_BYTES + commit.payload.len() as u64);
        assert_eq!(failure.work.bytes_encoded, 0);
        assert!(request.conditions.is_empty());
        assert!(request.mutations.is_empty());
        Ok(())
    }

    #[test]
    fn fork_paths_share_admitted_text_and_record_writer_matches_canonical_bytes() -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let mut meter = ForkWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let authority = AuthorityId::from_bytes([7; 16]);
        let generation = GenerationId::new(Digest::from_bytes([8; 32]));
        let path = meter.path(authority, "/generations/", Some(generation))?;
        assert_eq!(path, generation_path(authority, generation)?);
        let prior = meter.work;
        let cloned = meter.clone_path(&path)?;
        assert_eq!(path, cloned);
        assert_eq!(prior, meter.work);
        let durable = DurableCommit {
            epoch: Epoch::GENESIS, sequence: Sequence::new(1), operation_id: OperationId::from_bytes([2; 16]),
            fingerprint: Digest::from_bytes([3; 32]), previous_digest: Head::genesis(Epoch::GENESIS).digest,
            digest: Digest::from_bytes([4; 32]), payload: Bytes::from_static(b"opaque-input"),
        };
        let canonical = StreamsDurableRecord::encode(&durable, STREAM_RECORD_LIMIT)?;
        let measured = StreamsDurableRecord::encode_with(&durable, STREAM_RECORD_LIMIT,
            |domain, fields| meter.bytes(domain, fields))??;
        assert_eq!(canonical, measured);
        let borrowed = StreamsDurableRecord::decode_borrowed(&measured, STREAM_RECORD_LIMIT)?;
        assert_eq!(borrowed.payload, durable.payload.as_ref());
        assert_eq!(borrowed.payload.as_ptr(), measured[measured.len() - durable.payload.len()..].as_ptr());
        Ok(())
    }

    struct ReadSpy {
        inner: Arc<acyclic_stream::MemoryStream>,
        calls: std::sync::atomic::AtomicU64,
        reads: std::sync::atomic::AtomicU64,
        fail_at: u64,
        cancel_at: u64,
        cancellation: CancellationToken,
    }

    impl ReadSpy {
        fn enter(&self) -> Result<(), acyclic_stream::StreamError> {
            let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            if call == self.cancel_at {
                self.cancellation.cancel();
            }
            if call == self.fail_at {
                return Err(acyclic_stream::StreamError::Unavailable);
            }
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl acyclic_stream::StreamProvider for ReadSpy {
        async fn tail(
            &self,
            path: acyclic_stream::StreamPath,
        ) -> Result<u64, acyclic_stream::StreamError> {
            self.enter()?;
            self.inner.tail(path).await
        }
        async fn read(
            &self,
            request: acyclic_stream::ReadRequest,
        ) -> Result<acyclic_stream::RecordStream, acyclic_stream::StreamError> {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.enter()?;
            self.inner.read(request).await
        }
        async fn inspect_idempotency(
            &self,
            _: acyclic_stream::IdempotencyKey,
        ) -> Result<Option<acyclic_stream::IdempotencyObservation>, acyclic_stream::StreamError>
        {
            Err(acyclic_stream::StreamError::Unsupported)
        }
        async fn bounds(
            &self,
            _: acyclic_stream::StreamPath,
        ) -> Result<acyclic_stream::StreamBounds, acyclic_stream::StreamError> {
            Err(acyclic_stream::StreamError::Unsupported)
        }
        async fn append(
            &self,
            _: acyclic_stream::AppendRequest,
        ) -> Result<acyclic_stream::AppendOutcome, acyclic_stream::StreamError> {
            Err(acyclic_stream::StreamError::Unsupported)
        }
        async fn fork(
            &self,
            _: acyclic_stream::ForkRequest,
        ) -> Result<acyclic_stream::ForkReceipt, acyclic_stream::StreamError> {
            Err(acyclic_stream::StreamError::Unsupported)
        }
        async fn follow(
            &self,
            _: acyclic_stream::StreamPath,
            _: u64,
        ) -> Result<acyclic_stream::RecordStream, acyclic_stream::StreamError> {
            Err(acyclic_stream::StreamError::Unsupported)
        }
        async fn children(
            &self,
            _: acyclic_stream::ChildrenRequest,
        ) -> Result<acyclic_stream::ChildStream, acyclic_stream::StreamError> {
            Err(acyclic_stream::StreamError::Unsupported)
        }
        async fn commit(
            &self,
            _: acyclic_stream::CommitRequest,
        ) -> Result<acyclic_stream::CommitOutcome, acyclic_stream::StreamError> {
            Err(acyclic_stream::StreamError::Unsupported)
        }
        async fn read_commit(
            &self,
            _: acyclic_stream::CommitId,
        ) -> Result<acyclic_stream::CommittedEnvelope, acyclic_stream::StreamError> {
            Err(acyclic_stream::StreamError::Unsupported)
        }
    }

    #[tokio::test]
    async fn measured_replay_provider_controls() -> Result<(), Box<dyn std::error::Error>> {
        use std::sync::atomic::{AtomicU64, Ordering};
        let inner = Arc::new(acyclic_stream::MemoryStream::default());
        let id = AuthorityId::from_bytes([0x69; 16]);
        let commit = ProposedCommit {
            operation_id: OperationId::from_bytes([0x6a; 16]),
            fingerprint: Digest::from_bytes([0x6b; 32]),
            payload: Bytes::from_static(b"original creation"),
        };
        assert!(
            StreamAuthorityStore::new(Arc::clone(&inner))
                .create_authority_with_first_record(
                    id,
                    commit,
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new()
                )
                .await?
                .value
        );
        for (maximum_calls, maximum_records, maximum_bytes, fail_at, cancel_at, expected_calls) in [
            (0, 1, STREAM_RECORD_LIMIT, 0, 0, 0),
            (1, 0, STREAM_RECORD_LIMIT, 0, 0, 0),
            (1, 1, 0, 0, 0, 0),
            (1, 1, STREAM_RECORD_LIMIT, 1, 0, 1),
            (1, 1, STREAM_RECORD_LIMIT, 0, 1, 1),
        ] {
            let cancellation = CancellationToken::new();
            let spy = Arc::new(ReadSpy {
                inner: Arc::clone(&inner),
                calls: AtomicU64::new(0),
                reads: AtomicU64::new(0),
                fail_at,
                cancel_at,
                cancellation: cancellation.clone(),
            });
            let mut budget = WorkBudget::UNBOUNDED;
            budget.backend_read_operations = maximum_calls;
            budget.authority_records_read = maximum_records;
            budget.authority_bytes_read = maximum_bytes;
            let failure = StreamAuthorityStore::new(Arc::clone(&spy))
                .replay(
                    id,
                    Sequence::GENESIS,
                    ReplayLimit {
                        records: 1,
                        payload_bytes: 128,
                    },
                    budget,
                    &cancellation,
                )
                .await
                .err()
                .ok_or("denied replay unexpectedly succeeded")?;
            assert_eq!(spy.calls.load(Ordering::SeqCst), expected_calls);
            assert_eq!(spy.reads.load(Ordering::SeqCst), expected_calls);
            assert_eq!(failure.work.backend_read_operations, expected_calls);
            assert_eq!(failure.work.authority_records_read, 0);
            assert_eq!(failure.work.authority_bytes_read, 0);
            failure.work.verify(budget)?;
            if expected_calls == 0 {
                assert!(matches!(failure.error, AuthorityStoreError::Work(_)));
            } else if cancel_at != 0 {
                assert!(matches!(failure.error, AuthorityStoreError::Cancelled));
            } else {
                assert!(!matches!(
                    failure.error,
                    AuthorityStoreError::Work(_) | AuthorityStoreError::Cancelled
                ));
            }
        }
        let cancellation = CancellationToken::new();
        let spy = Arc::new(ReadSpy {
            inner,
            calls: AtomicU64::new(0),
            reads: AtomicU64::new(0),
            fail_at: 0,
            cancel_at: 0,
            cancellation: cancellation.clone(),
        });
        let receipt = StreamAuthorityStore::new(Arc::clone(&spy))
            .replay(
                id,
                Sequence::GENESIS,
                ReplayLimit {
                    records: 1,
                    payload_bytes: 128,
                },
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
        assert_eq!(spy.reads.load(Ordering::SeqCst), 1);
        assert_eq!(receipt.work.backend_read_operations, 1);
        assert_eq!(receipt.work.authority_records_read, 1);
        assert_eq!(receipt.value.len(), 1);
        assert_eq!(receipt.value[0].payload.as_ref(), b"original creation");
        assert!(receipt.work.authority_bytes_read > receipt.value[0].payload.len() as u64);
        Ok(())
    }

    #[tokio::test]
    async fn measured_head_provider_controls() {
        use std::sync::atomic::{AtomicU64, Ordering};
        let inner = Arc::new(acyclic_stream::MemoryStream::default());
        let id = AuthorityId::from_bytes([0x68; 16]);
        StreamAuthorityStore::new(Arc::clone(&inner))
            .create_authority(
                id,
                Epoch::GENESIS,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .expect("seed actual authority");
        for (maximum_calls, maximum_records, fail_at, cancel_at, expected_calls, expected_reads) in [
            (0, 8, 0, 0, 0, 0),
            (8, 0, 0, 0, 3, 0),
            (8, 8, 5, 0, 5, 2),
            (8, 8, 0, 5, 5, 2),
        ] {
            let cancellation = CancellationToken::new();
            let spy = Arc::new(ReadSpy {
                inner: Arc::clone(&inner),
                calls: AtomicU64::new(0),
                reads: AtomicU64::new(0),
                fail_at,
                cancel_at,
                cancellation: cancellation.clone(),
            });
            let mut budget = WorkBudget::UNBOUNDED;
            budget.backend_read_operations = maximum_calls;
            budget.authority_records_read = maximum_records;
            budget.authority_bytes_read = STREAM_RECORD_LIMIT * 8;
            let failed = StreamAuthorityStore::new(Arc::clone(&spy))
                .head(id, budget, &cancellation)
                .await
                .expect_err("injected rejection");
            assert_eq!(spy.calls.load(Ordering::SeqCst), expected_calls);
            assert_eq!(spy.reads.load(Ordering::SeqCst), expected_reads);
            assert_eq!(failed.work.backend_read_operations, expected_calls);
            failed.work.verify(budget).expect("spent work bounded");
            if expected_reads == 2 {
                assert_eq!(failed.work.authority_records_read, 1);
                assert_eq!(
                    failed.work.authority_bytes_read,
                    (EPOCH_DOMAIN.len() + 8) as u64
                );
                if cancel_at != 0 {
                    assert!(matches!(failed.error, AuthorityStoreError::Cancelled));
                }
            } else {
                assert!(matches!(failed.error, AuthorityStoreError::Work(_)));
            }
        }
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let spy = Arc::new(ReadSpy {
            inner,
            calls: AtomicU64::new(0),
            reads: AtomicU64::new(0),
            fail_at: 0,
            cancel_at: 0,
            cancellation: cancellation.clone(),
        });
        let failed = StreamAuthorityStore::new(Arc::clone(&spy))
            .head(id, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .expect_err("pre-cancelled");
        assert!(matches!(failed.error, AuthorityStoreError::Cancelled));
        assert_eq!(spy.calls.load(Ordering::SeqCst), 0);
        assert_eq!(*failed.work, WorkCounters::default());
    }

    #[tokio::test]
    async fn measured_head_pre_admits_reads_and_retains_partial_failures() {
        let stream = Arc::new(acyclic_stream::MemoryStream::default());
        let authority = StreamAuthorityStore::new(stream);
        let id = AuthorityId::from_bytes([0x67; 16]);
        let cancellation = CancellationToken::new();
        let mut budget = WorkBudget::UNBOUNDED;
        budget.backend_read_operations = 0;
        let denied = authority
            .head(id, budget, &cancellation)
            .await
            .expect_err("no reads admitted");
        assert!(matches!(denied.error, AuthorityStoreError::Work(_)));
        assert_eq!(*denied.work, WorkCounters::default());

        let absent = authority
            .head(id, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .expect_err("missing authority");
        assert!(matches!(absent.error, AuthorityStoreError::Missing));
        assert_eq!(absent.work.backend_read_operations, 2);
        assert_eq!(absent.work.authority_records_read, 0);

        authority
            .create_authority(id, Epoch::GENESIS, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .expect("create authority");
        budget = WorkBudget::UNBOUNDED;
        budget.authority_records_read = 0;
        let denied = authority
            .head(id, budget, &cancellation)
            .await
            .expect_err("record reads denied");
        assert!(matches!(denied.error, AuthorityStoreError::Work(_)));
        assert_eq!(denied.work.backend_read_operations, 3);
        assert_eq!(denied.work.authority_records_read, 0);
        denied
            .work
            .verify(budget)
            .expect("only admitted work spent");

        budget.authority_records_read = 1;
        let partial = authority
            .head(id, budget, &cancellation)
            .await
            .expect_err("second record denied");
        assert!(matches!(partial.error, AuthorityStoreError::Work(_)));
        assert_eq!(partial.work.backend_read_operations, 4);
        assert_eq!(partial.work.authority_records_read, 1);
        assert_eq!(
            partial.work.authority_bytes_read,
            (EPOCH_DOMAIN.len() + 8) as u64
        );
        partial
            .work
            .verify(budget)
            .expect("partial receipt stays within bound");
    }

    #[tokio::test]
    async fn authority_without_publication_gate_fails_closed() {
        let stream = Arc::new(acyclic_stream::MemoryStream::default());
        let authority_id = AuthorityId::from_bytes([0x31; 16]);
        let root = authority_path(authority_id).expect("root path");
        let records = records_path(authority_id).expect("records path");
        let epochs = epochs_path(authority_id).expect("epochs path");
        acyclic_stream::StreamProvider::commit(
            stream.as_ref(),
            acyclic_stream::CommitRequest {
                conditions: vec![
                    acyclic_stream::CommitCondition::Absent { path: root.clone() },
                    acyclic_stream::CommitCondition::Absent {
                        path: records.clone(),
                    },
                    acyclic_stream::CommitCondition::Absent {
                        path: epochs.clone(),
                    },
                ],
                mutations: vec![
                    acyclic_stream::CommitMutation::Append {
                        path: root,
                        records: vec![Bytes::from_static(AUTHORITY_MARKER)],
                    },
                    acyclic_stream::CommitMutation::Append {
                        path: records,
                        records: vec![encode_epoch(GENESIS_DOMAIN, Epoch::GENESIS)],
                    },
                    acyclic_stream::CommitMutation::Append {
                        path: epochs,
                        records: vec![encode_epoch(EPOCH_DOMAIN, Epoch::GENESIS)],
                    },
                ],
                idempotency_key: stream_key(b"gateless-authority", &authority_id.into_bytes())
                    .expect("idempotency key"),
            },
        )
        .await
        .expect("seed gateless authority");

        let authority = StreamAuthorityStore::new(Arc::clone(&stream));
        let refused = authority
            .create_authority(
                authority_id,
                Epoch::GENESIS,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .expect_err("an authority without its publication gate is refused");
        assert!(
            matches!(refused.error, AuthorityStoreError::Corrupt(_)),
            "unexpected refusal: {:?}",
            refused.error
        );
        assert!(matches!(
            acyclic_stream::StreamProvider::tail(
                stream.as_ref(),
                publication_gate_path(authority_id).expect("gate path"),
            )
            .await,
            Err(acyclic_stream::StreamError::NotFound)
        ));
    }

    #[tokio::test]
    async fn existing_authority_rejects_a_corrupt_publication_gate() {
        let stream = Arc::new(acyclic_stream::MemoryStream::default());
        let authority = StreamAuthorityStore::new(Arc::clone(&stream));
        let authority_id = AuthorityId::from_bytes([0x35; 16]);
        let cancellation = CancellationToken::new();
        authority
            .create_authority(
                authority_id,
                Epoch::GENESIS,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .expect("create authority");
        let gate = publication_gate_path(authority_id).expect("gate path");
        acyclic_stream::StreamProvider::commit(
            stream.as_ref(),
            acyclic_stream::CommitRequest {
                conditions: vec![acyclic_stream::CommitCondition::Tail {
                    path: gate.clone(),
                    expected: 1,
                }],
                mutations: vec![acyclic_stream::CommitMutation::Append {
                    path: gate,
                    records: vec![Bytes::from_static(b"invalid-publication-gate")],
                }],
                idempotency_key: stream_key(
                    b"corrupt-publication-gate",
                    &authority_id.into_bytes(),
                )
                .expect("corruption key"),
            },
        )
        .await
        .expect("append invalid gate record");

        let error = authority
            .create_authority(
                authority_id,
                Epoch::GENESIS,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .expect_err("corrupt gate must fail authority reopen");
        assert!(matches!(error.error, AuthorityStoreError::Corrupt(_)));
    }

    #[tokio::test]
    async fn publication_reservation_excludes_unreserved_writers() {
        let stream = Arc::new(acyclic_stream::MemoryStream::default());
        let authority = StreamAuthorityStore::new(Arc::clone(&stream));
        let authority_id = AuthorityId::from_bytes([0x41; 16]);
        let cancellation = CancellationToken::new();
        authority
            .create_authority(
                authority_id,
                Epoch::GENESIS,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .expect("create authority");
        let head = authority
            .head(authority_id, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .expect("head")
            .value;
        let operation_id = OperationId::from_bytes([0x42; 16]);
        let reservation = match authority
            .reserve_publication(
                authority_id,
                head,
                operation_id,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .expect("reserve")
            .value
        {
            ReservationOutcome::Reserved(reservation) => reservation,
            outcome => panic!("unexpected reservation outcome: {outcome:?}"),
        };
        let proposed = ProposedCommit {
            operation_id: OperationId::from_bytes([0x43; 16]),
            fingerprint: Digest::from_bytes([0x44; 32]),
            payload: Bytes::from_static(b"reserved write"),
        };
        let ordinary = authority
            .compare_and_append_guarded(
                crate::GuardedAppend {
                    authority_id,
                    epoch: head.epoch,
                    expected: head,
                    commit: proposed.clone(),
                    permit: PublicationPermit::Unrestricted,
                },
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .expect("ordinary writer is rejected")
            .value;
        assert!(matches!(ordinary, FsAppendOutcome::Fenced { .. }));
        let reserved = authority
            .compare_and_append_guarded(
                crate::GuardedAppend {
                    authority_id,
                    epoch: head.epoch,
                    expected: head,
                    commit: proposed,
                    permit: PublicationPermit::Reservation {
                        operation_id: operation_id.into_bytes(),
                        gate_tail: reservation.gate_tail,
                        expected: reservation.expected,
                    },
                },
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .expect("reserved writer")
            .value;
        assert!(matches!(reserved, FsAppendOutcome::Committed(_)));
        let denied_release = authority
            .release_publication(reservation, WorkBudget::default(), &cancellation)
            .await
            .expect_err("release must charge its idempotency read before doing work");
        assert!(matches!(denied_release.error, AuthorityStoreError::Work(_)));
        authority
            .release_publication(reservation, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .expect("release");
        authority
            .release_publication(reservation, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .expect("exact release retry is idempotent");
        let current = authority
            .head(authority_id, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .expect("head after publication")
            .value;
        let reacquired = match authority
            .reserve_publication(
                authority_id,
                current,
                operation_id,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .expect("reacquire")
            .value
        {
            ReservationOutcome::Reserved(reservation) => reservation,
            outcome => panic!("unexpected reacquisition outcome: {outcome:?}"),
        };
        let forged = PublicationReservation {
            operation_id: OperationId::from_bytes([0x99; 16]),
            ..reacquired
        };
        assert!(
            authority
                .release_publication(forged, WorkBudget::UNBOUNDED, &cancellation)
                .await
                .is_err()
        );
        let gate = publication_gate_path(authority_id).expect("gate path");
        let advanced = acyclic_stream::StreamProvider::commit(
            stream.as_ref(),
            acyclic_stream::CommitRequest {
                conditions: vec![acyclic_stream::CommitCondition::Tail {
                    path: gate.clone(),
                    expected: reacquired.gate_tail,
                }],
                mutations: vec![acyclic_stream::CommitMutation::Append {
                    path: gate.clone(),
                    records: vec![encode_active_reservation(reacquired.operation_id)],
                }],
                idempotency_key: stream_key(b"advance-active-gate", &authority_id.into_bytes())
                    .expect("advance key"),
            },
        )
        .await
        .expect("advance active gate");
        assert!(matches!(
            advanced,
            acyclic_stream::CommitOutcome::Committed(_)
        ));

        let release_key = publication_reservation_key(
            b"release-publication",
            reacquired.authority_id,
            reacquired.operation_id,
            reacquired.gate_tail,
        )
        .expect("release key");
        let retained_conflict = acyclic_stream::StreamProvider::commit(
            stream.as_ref(),
            acyclic_stream::CommitRequest {
                conditions: vec![acyclic_stream::CommitCondition::Tail {
                    path: gate.clone(),
                    expected: reacquired.gate_tail,
                }],
                mutations: vec![acyclic_stream::CommitMutation::Append {
                    path: gate,
                    records: vec![Bytes::from_static(PUBLICATION_GATE_FREE)],
                }],
                idempotency_key: release_key,
            },
        )
        .await
        .expect("retain release conflict");
        assert!(matches!(
            retained_conflict,
            acyclic_stream::CommitOutcome::Conflict(_)
        ));
        authority
            .release_publication(reacquired, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .expect_err("retained release conflict must not become success");
        let (_, active) = authority
            .publication_gate(authority_id)
            .await
            .expect("read active gate");
        assert_eq!(active, Some(reacquired.operation_id));
    }

    #[test]
    fn generation_records_are_bound_to_their_authority_and_sequence()
    -> Result<(), Box<dyn std::error::Error>> {
        let volume_id = crate::foundation::VolumeId::from_bytes([0x71; 16]);
        let authority_id = volume_authority_id(volume_id);
        let root = ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: Digest::from_bytes([0x72; 32]),
        };
        let creation = encode_volume_created(VolumeCreated {
            volume_id,
            config: VolumeConfig::portable(Lifecycle::Ephemeral),
            initial_generation_root: root,
        })?;
        let publication = encode_publication_payload(volume_id, root);
        let generation = GenerationId::new(root.digest);

        assert_eq!(
            generation_from_payload(authority_id, Sequence::new(1), &creation)?,
            Some(generation)
        );
        assert_eq!(
            generation_from_payload(authority_id, Sequence::new(2), &publication)?,
            Some(generation)
        );
        assert!(generation_from_payload(authority_id, Sequence::new(2), &creation).is_err());
        assert!(generation_from_payload(authority_id, Sequence::new(1), &publication).is_err());
        assert!(
            generation_from_payload(
                AuthorityId::from_bytes([0x73; 16]),
                Sequence::new(1),
                &creation,
            )
            .is_err()
        );
        assert!(
            generation_from_payload(
                AuthorityId::from_bytes([0x73; 16]),
                Sequence::new(2),
                &publication,
            )
            .is_err()
        );
        Ok(())
    }
    fn generation_request_paths(
        meter: &mut ForkWork<'_>,
        authority: AuthorityId,
        generation: GenerationId,
    ) -> Result<ForkDestinationPaths, OperationFailure<AuthorityStoreError>> {
        Ok(ForkDestinationPaths {
            root: meter.path(authority, "", None)?,
            records: meter.path(authority, "/records", None)?,
            epochs: meter.path(authority, "/epochs", None)?,
            locator: meter.path(authority, "/generations/", Some(generation))?,
            lineage: meter.path(authority, "/lineage", None)?,
            gate: meter.path(authority, "/publication-gate", None)?,
        })
    }

    #[test]
    fn generation_request_preserves_canonical_records_and_failed_copy_prefix()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let source = AuthorityId::from_bytes([0x81; 16]);
        let destination = AuthorityId::from_bytes([0x82; 16]);
        let generation = GenerationId::new(Digest::from_bytes([0x83; 32]));
        let operation = OperationId::from_bytes([0x84; 16]);
        let mut meter = ForkWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let paths = generation_request_paths(&mut meter, destination, generation)?;
        let source_path = meter.path(source, "/lineage", None)?;
        let request = fork_generation_commit_request(paths, source_path, 7, operation, &mut meter)?;
        assert_eq!(request.conditions.len(), 6);
        assert_eq!(request.mutations.len(), 6);
        assert_eq!(request.idempotency_key, stream_key(b"fork-authority", &operation.into_bytes())?);
        let acyclic_stream::CommitMutation::Fork { source: actual_source, destination: actual_destination, at_tail, records } =
            &request.mutations[1] else { panic!("second mutation must fork the original prefix") };
        assert_eq!(actual_source, &lineage_path(source)?);
        assert_eq!(actual_destination, &lineage_path(destination)?);
        assert_eq!(*at_tail, 7);
        assert!(records.is_empty());
        for (index, expected) in [
            (0, Bytes::from_static(AUTHORITY_MARKER)),
            (2, encode_epoch(GENESIS_DOMAIN, Epoch::GENESIS)),
            (3, encode_epoch(EPOCH_DOMAIN, Epoch::GENESIS)),
            (4, encode_lineage_tail(7)),
            (5, Bytes::from_static(PUBLICATION_GATE_FREE)),
        ] {
            let acyclic_stream::CommitMutation::Append { records, .. } = &request.mutations[index]
                else { panic!("canonical append mutation") };
            assert_eq!(records.as_slice(), &[expected]);
        }
        let writes = meter.request_write_work(&request)?;
        assert_eq!(writes.authority_records_appended, 5);
        assert_eq!(meter.work.backend_write_operations, 0);

        let mut refused = ForkWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let paths = generation_request_paths(&mut refused, destination, generation)?;
        let source_path = refused.path(source, "/lineage", None)?;
        let prior = refused.work;
        refused.budget.allocation_operations = prior.allocation_operations + 1;
        let failure = fork_generation_commit_request(paths, source_path, 7, operation, &mut refused)
            .expect_err("mutation vector allocation refused after conditions");
        assert!(matches!(failure.error, AuthorityStoreError::Work(_)));
        assert_eq!(failure.work.allocation_operations, prior.allocation_operations + 1);
        assert_eq!(failure.work.bytes_copied, prior.bytes_copied + 6 * std::mem::size_of::<acyclic_stream::CommitCondition>() as u64);
        assert_eq!(failure.work.bytes_encoded, prior.bytes_encoded);
        assert_eq!(failure.work.backend_write_operations, 0);
        Ok(())
    }

    #[tokio::test]
    async fn generation_owner_guard_seals_after_planning_and_on_recovery()
    -> Result<(), Box<dyn std::error::Error>> {
        let stream = Arc::new(acyclic_stream::MemoryStream::default());
        let authority = StreamAuthorityStore::new(Arc::clone(&stream));
        let cancellation = CancellationToken::new();
        let volume = crate::foundation::VolumeId::from_bytes([0x91; 16]);
        let source_authority = volume_authority_id(volume);
        let root = ObjectId { kind: ObjectKind::GenerationRoot, digest: Digest::from_bytes([0x92; 32]) };
        let source_payload = encode_volume_created(VolumeCreated {
            volume_id: volume, config: VolumeConfig::portable(Lifecycle::Ephemeral), initial_generation_root: root,
        })?;
        authority.create_authority_with_first_record(source_authority, ProposedCommit {
            operation_id: OperationId::from_bytes([0x93; 16]),
            fingerprint: Digest::from_bytes([0x94; 32]), payload: Bytes::from(source_payload),
        }, WorkBudget::UNBOUNDED, &cancellation).await?;
        let source = crate::GenerationForkSource {
            authority: source_authority, generation: GenerationId::new(root.digest),
            lineage: crate::GenerationFork::PublishedPrefix,
        };
        let destination = AuthorityId::from_bytes([0x95; 16]);
        let operation = OperationId::from_bytes([0x96; 16]);
        let budget = WorkBudget { allocation_operations: 10_000, ..WorkBudget::UNBOUNDED };
        let mut calls = 0;
        let mut guard = |remaining: WorkBudget| {
            calls += 1;
            assert!(remaining.allocation_operations < budget.allocation_operations);
            assert!(remaining.backend_read_operations < budget.backend_read_operations);
            std::future::ready(Err::<crate::FsReceipt<()>, _>(OperationFailure::new(
                crate::FsError::WorkspaceDeleted,
                WorkCounters { items_examined: 1, ..WorkCounters::default() },
            )))
        };
        let failed = authority.fork_generation_authority_guarded(
            source, destination, operation, budget, &cancellation, &mut guard,
        ).await.expect_err("deleted destination owner seals after preparation");
        assert!(matches!(failed.error, crate::FsError::WorkspaceDeleted));
        assert!(failed.work.backend_read_operations > 0);
        assert!(failed.work.allocation_operations > 0);
        assert_eq!(failed.work.backend_write_operations, 0);
        assert!(matches!(acyclic_stream::StreamProvider::tail(
            stream.as_ref(), authority_path(destination)?,
        ).await, Err(acyclic_stream::StreamError::NotFound)));
        authority.fork_generation_authority(
            source, destination, operation, budget, &cancellation,
        ).await?;
        let retried = authority.fork_generation_authority_guarded(
            source, destination, operation, budget, &cancellation, &mut guard,
        ).await.expect_err("existing destination requires a fresh recovery seal");
        assert!(matches!(retried.error, crate::FsError::WorkspaceDeleted));
        assert!(retried.work.backend_read_operations > failed.work.backend_read_operations);
        assert_eq!(retried.work.backend_write_operations, 0);
        drop(guard);
        assert_eq!(calls, 2);
        Ok(())
    }
    #[tokio::test]
    async fn native_first_record_seals_prepared_request_and_ack_recovery()
    -> Result<(), Box<dyn std::error::Error>> {
        let stream = Arc::new(acyclic_stream::MemoryStream::default());
        let authority = StreamAuthorityStore::new(Arc::clone(&stream));
        let cancellation = CancellationToken::new();
        let destination = AuthorityId::from_bytes([0xa1; 16]);
        let proposed = || ProposedCommit {
            operation_id: OperationId::from_bytes([0xa2; 16]),
            fingerprint: Digest::from_bytes([0xa3; 32]),
            payload: Bytes::from_static(b"owned-canonical-first-record"),
        };
        let budget = WorkBudget { allocation_operations: 10_000, ..WorkBudget::UNBOUNDED };
        let mut reject_prepared = |remaining: WorkBudget| {
            assert!(remaining.allocation_operations < budget.allocation_operations);
            assert!(remaining.bytes_hashed < budget.bytes_hashed);
            std::future::ready(Err::<crate::FsReceipt<()>, _>(OperationFailure::before_work(crate::FsError::WorkspaceDeleted)))
        };
        let rejected = authority.create_authority_with_first_record_guarded(
            destination, proposed(), budget, &cancellation, &mut reject_prepared,
        ).await.expect_err("owner rejected the actual prepared canonical request");
        assert!(matches!(rejected.error, crate::FsError::WorkspaceDeleted));
        assert!(rejected.work.bytes_encoded > 0);
        assert!(rejected.work.bytes_hashed > 0);
        assert_eq!(rejected.work.backend_write_operations, 0);
        assert!(matches!(acyclic_stream::StreamProvider::tail(
            stream.as_ref(), authority_path(destination)?,
        ).await, Err(acyclic_stream::StreamError::NotFound)));
        assert!(authority.create_authority_with_first_record(
            destination, proposed(), budget, &cancellation,
        ).await?.value);
        let mut seals = 0;
        let mut reject_recovery = |_| {
            seals += 1;
            std::future::ready(if seals == 1 {
                Ok(crate::OperationReceipt { value: (), work: WorkCounters::default() })
            } else {
                Err(OperationFailure::before_work(crate::FsError::WorkspaceDeleted))
            })
        };
        let recovered = authority.create_authority_with_first_record_guarded(
            destination, proposed(), budget, &cancellation, &mut reject_recovery,
        ).await.expect_err("retained ACK also requires the current owner seal");
        assert!(matches!(recovered.error, crate::FsError::WorkspaceDeleted));
        assert_eq!(recovered.work.backend_write_operations, 1);
        assert!(recovered.work.authority_records_appended > 0);
        drop(reject_recovery);
        assert_eq!(seals, 2);
        Ok(())
    }
}
