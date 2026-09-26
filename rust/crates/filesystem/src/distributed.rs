//! Filesystem storage bindings over the canonical public service providers.

use crate::cancellation::CancellationToken;
use crate::foundation::{
    AuthorityId, Digest, DurableCommit, Epoch, GenerationId, Head, OperationId, ProposedCommit,
    Sequence, authority_commit_digest,
};
use crate::kernel::{decode_published_generation, decode_volume_created, volume_authority_id};
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::storage::{
    AppendOutcome as FsAppendOutcome, AuthorityReceipt, AuthorityResult, AuthorityStoreError,
    CreateAuthorityOutcome, FenceOutcome, ObjectId, ObjectRead, ObjectReadRequest,
    ObjectReadRetention, ObjectReceipt, ObjectResult, ObjectStoreError, ObjectWrite,
    PublicationPermit, PublicationReservation, ReplayLimit, ReservationOutcome, object_digest,
};
use crate::streams_record::StreamsDurableRecord;
use crate::{AsyncAuthorityStore, AsyncObjectStore, WorkspaceForkCommit, WorkspaceForkOutcome};
use acyclic_objects::{
    Condition, GetRequest, ObjectsError, ObjectsProvider, PutRequest, ReadTarget, wire,
};
use bytes::Bytes;
use futures::StreamExt;
use std::collections::BTreeMap;
use std::sync::Arc;

const STREAM_RECORD_LIMIT: u64 = acyclic_stream::MAX_RECORD_BYTES as u64;
const GENESIS_DOMAIN: &[u8] = b"acyclic-fs-stream-genesis-v1\0";
const EPOCH_DOMAIN: &[u8] = b"acyclic-fs-stream-epoch-v1\0";
const LINEAGE_DOMAIN: &[u8] = b"acyclic-fs-stream-lineage-v1\0";
const LINEAGE_TAIL_DOMAIN: &[u8] = b"acyclic-fs-stream-lineage-tail-v1\0";
const AUTHORITY_MARKER: &[u8] = b"acyclic-fs-authority-v1\0";
const PUBLICATION_GATE_FREE: &[u8] = b"acyclic-fs-publication-gate-v1\0free";
const PUBLICATION_GATE_ACTIVE: &[u8] = b"acyclic-fs-publication-gate-v1\0active\0";

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
    /// Lists a bounded snapshot of exact filesystem authority identities.
    ///
    /// Authorities are native direct children in Stream; no filesystem record or object is read.
    pub async fn authorities(&self, maximum: u32) -> Result<Vec<AuthorityId>, AuthorityStoreError> {
        let request_limit = maximum.checked_add(1).ok_or_else(|| {
            AuthorityStoreError::Rejected("authority listing bound is too large".to_owned())
        })?;
        let parent = acyclic_stream::StreamPath::new("fs/authorities").map_err(map_stream_error)?;
        let mut children = self
            .provider
            .children(acyclic_stream::ChildrenRequest {
                parent: Some(parent),
                limit: request_limit,
            })
            .await
            .map_err(map_stream_error)?;
        let mut authorities = Vec::new();
        while let Some(child) = children.next().await {
            let child = child.map_err(map_stream_error)?;
            let encoded = child
                .path
                .as_str()
                .strip_prefix("fs/authorities/")
                .ok_or_else(|| {
                    AuthorityStoreError::Corrupt(
                        "Stream authority child escaped its parent".to_owned(),
                    )
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
            authorities.push(AuthorityId::from_bytes(bytes));
        }
        if authorities.len() > maximum as usize {
            return Err(AuthorityStoreError::Rejected(
                "authority listing bound exceeded".to_owned(),
            ));
        }
        Ok(authorities)
    }

    async fn snapshot(
        &self,
        authority_id: AuthorityId,
    ) -> Result<AuthoritySnapshot, AuthorityStoreError> {
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

    async fn existing_fork_destination(
        &self,
        destination_authority: AuthorityId,
        source_generation: GenerationId,
        forked_at: u64,
    ) -> Result<Option<AuthoritySnapshot>, AuthorityStoreError> {
        let snapshot = match self.snapshot(destination_authority).await {
            Ok(snapshot) => snapshot,
            Err(AuthorityStoreError::Missing) => return Ok(None),
            Err(error) => return Err(error),
        };
        let locator = generation_path(destination_authority, source_generation)?;
        let record = read_one(self.provider.as_ref(), locator, 0).await?;
        let located_tail = decode_lineage_tail(&record.value)?;
        if located_tail == 0 {
            return Err(AuthorityStoreError::Corrupt(
                "destination generation locator selected an empty lineage".to_owned(),
            ));
        }
        let lineage = lineage_path(destination_authority)?;
        let selected = read_one(self.provider.as_ref(), lineage, located_tail - 1).await?;
        if located_tail == forked_at
            && decode_lineage_generation(&selected.value)? == source_generation
        {
            Ok(Some(snapshot))
        } else {
            Err(AuthorityStoreError::Rejected(
                "destination generation fork conflicts with existing state".to_owned(),
            ))
        }
    }

    /// Resolves the source lineage path and fork tail for one generation fork.
    async fn resolve_source_fork_point(
        &self,
        source_authority: AuthorityId,
        source_generation: GenerationId,
    ) -> Result<(acyclic_stream::StreamPath, u64), OperationFailure<AuthorityStoreError>> {
        let source_lineage =
            lineage_path(source_authority).map_err(OperationFailure::before_work)?;
        let source_locator = generation_path(source_authority, source_generation)
            .map_err(OperationFailure::before_work)?;
        let locator_record = match read_one(self.provider.as_ref(), source_locator, 0).await {
            Ok(record) => record,
            Err(AuthorityStoreError::Missing) => {
                return Err(OperationFailure::before_work(
                    AuthorityStoreError::Rejected(
                        "source authority has no generation lineage locator".to_owned(),
                    ),
                ));
            }
            Err(error) => return Err(OperationFailure::before_work(error)),
        };
        let forked_at =
            decode_lineage_tail(&locator_record.value).map_err(OperationFailure::before_work)?;
        if forked_at == 0 {
            return Err(OperationFailure::before_work(AuthorityStoreError::Corrupt(
                "generation locator selected an empty lineage".to_owned(),
            )));
        }
        let selected = read_one(
            self.provider.as_ref(),
            source_lineage.clone(),
            forked_at - 1,
        )
        .await
        .map_err(OperationFailure::before_work)?;
        if decode_lineage_generation(&selected.value).map_err(OperationFailure::before_work)?
            != source_generation
        {
            return Err(OperationFailure::before_work(AuthorityStoreError::Corrupt(
                "generation locator and lineage disagree".to_owned(),
            )));
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
    ) -> Result<(acyclic_stream::CommitRequest, WorkCounters), OperationFailure<AuthorityStoreError>>
    {
        let mut request = acyclic_stream::CommitRequest {
            conditions: Vec::new(),
            mutations: Vec::new(),
            idempotency_key: operation_key(fork.destination, fork.creation.operation_id)
                .map_err(OperationFailure::before_work)?,
        };
        let mut work = WorkCounters::default();
        let lineage = match fork.lineage {
            Some(source) if source.lineage == crate::GenerationFork::PublishedPrefix => {
                let (source_lineage, forked_at) = self
                    .resolve_source_fork_point(source.authority, source.generation)
                    .await?;
                work = authority_read_work(3);
                Some(ForkedLineage {
                    source_lineage,
                    forked_at,
                    source_generation: source.generation,
                })
            }
            Some(_) | None => None,
        };
        let (records, bytes) =
            first_record_commit(&mut request, fork.retention, &fork.retained, None)
                .map_err(OperationFailure::before_work)?;
        let (more_records, more_bytes) =
            first_record_commit(&mut request, fork.destination, &fork.creation, lineage)
                .map_err(OperationFailure::before_work)?;
        work = work
            .checked_add(authority_write_work(
                records.saturating_add(more_records),
                bytes.saturating_add(more_bytes),
            ))
            .map_err(|error| OperationFailure::before_work(error.into()))?;
        Ok((request, work))
    }
}

impl<P: acyclic_stream::StreamProvider> StreamAuthorityStore<P> {
    /// Deletes `path` and every path beneath it, deepest first.
    fn retire_subtree<'a>(
        &'a self,
        path: acyclic_stream::StreamPath,
        work: &'a mut WorkCounters,
        cancellation: &'a CancellationToken,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<(), AuthorityStoreError>> + Send + 'a>> {
        Box::pin(async move {
            loop {
                cancellation
                    .check()
                    .map_err(|_| AuthorityStoreError::Cancelled)?;
                let children = self
                    .provider
                    .children(acyclic_stream::ChildrenRequest {
                        parent: Some(path.clone()),
                        limit: u32::try_from(acyclic_stream::MAX_ITEMS).unwrap_or(u32::MAX),
                    })
                    .await
                    .map_err(map_stream_error)?
                    .map(|child| child.map(|child| child.path))
                    .collect::<Vec<_>>()
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(map_stream_error)?;
                *work = work.checked_add(authority_read_work(1))?;
                if children.is_empty() {
                    break;
                }
                for child in children {
                    self.retire_subtree(child, work, cancellation).await?;
                }
            }
            let key = stream_key(b"retire-path", path.as_str().as_bytes())?;
            *work = work.checked_add(authority_write_work(0, 0))?;
            match self.provider.delete(path, key).await {
                Ok(_)
                | Err(
                    acyclic_stream::StreamError::NotFound | acyclic_stream::StreamError::Retired,
                ) => Ok(()),
                Err(error) => Err(map_stream_error(error)),
            }
        })
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
) -> Result<(u64, u64), AuthorityStoreError> {
    use acyclic_stream::CommitCondition::Absent;
    use acyclic_stream::CommitMutation::Append;
    let epoch = Epoch::GENESIS;
    let sequence = Sequence::new(1);
    let previous = Head::genesis(epoch);
    let durable = DurableCommit {
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
        payload: commit.payload.clone(),
    };
    let encoded = StreamsDurableRecord::encode(&durable, STREAM_RECORD_LIMIT)
        .map_err(|error| AuthorityStoreError::Rejected(error.to_string()))?;
    let mut appended_records = 1_u64;
    let mut appended_bytes = u64::try_from(durable.payload.len()).unwrap_or(u64::MAX);
    let root = authority_path(authority)?;
    let records = records_path(authority)?;
    let epochs = epochs_path(authority)?;
    let gate = publication_gate_path(authority)?;
    for path in [&root, &records, &epochs, &gate] {
        request.conditions.push(Absent { path: path.clone() });
    }
    request.mutations.push(Append {
        path: root,
        records: vec![Bytes::from_static(AUTHORITY_MARKER)],
    });
    let generation = generation_from_payload(authority, sequence, &durable.payload)?;
    let (lineage_records, lineage_bytes) = lineage_commit(request, authority, lineage, generation)?;
    appended_records = appended_records.saturating_add(lineage_records);
    appended_bytes = appended_bytes.saturating_add(lineage_bytes);
    request.mutations.push(Append {
        path: records,
        records: vec![encode_epoch(GENESIS_DOMAIN, epoch), encoded],
    });
    request.mutations.push(Append {
        path: epochs,
        records: vec![encode_epoch(EPOCH_DOMAIN, epoch)],
    });
    request.mutations.push(Append {
        path: gate,
        records: vec![Bytes::from_static(PUBLICATION_GATE_FREE)],
    });
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
) -> Result<(u64, u64), AuthorityStoreError> {
    use acyclic_stream::CommitCondition::Absent;
    use acyclic_stream::CommitMutation::{Append, Fork};
    if lineage.is_none() && generation.is_none() {
        return Ok((0, 0));
    }
    let lineage_path = lineage_path(authority)?;
    request.conditions.push(Absent {
        path: lineage_path.clone(),
    });
    let entries = generation
        .map(encode_lineage_generation)
        .into_iter()
        .collect::<Vec<_>>();
    let mut appended_records = u64::try_from(entries.len()).unwrap_or(u64::MAX);
    let mut appended_bytes = entries.iter().fold(0_u64, |total, entry| {
        total.saturating_add(u64::try_from(entry.len()).unwrap_or(u64::MAX))
    });
    let mut tail = 0;
    let mut forked_locator = None;
    match lineage {
        Some(forked) => {
            let locator = generation_path(authority, forked.source_generation)?;
            request.conditions.push(Absent {
                path: locator.clone(),
            });
            request.mutations.push(Fork {
                source: forked.source_lineage,
                destination: lineage_path,
                at_tail: forked.forked_at,
                records: entries,
            });
            request.mutations.push(Append {
                path: locator.clone(),
                records: vec![encode_lineage_tail(forked.forked_at)],
            });
            tail = forked.forked_at;
            forked_locator = Some(locator);
        }
        None => request.mutations.push(Append {
            path: lineage_path,
            records: entries,
        }),
    }
    if let Some(generation) = generation {
        let locator = generation_path(authority, generation)?;
        if forked_locator.as_ref() == Some(&locator) {
            return Err(AuthorityStoreError::Rejected(
                "a fork cannot publish its source generation again".to_owned(),
            ));
        }
        let locator_record = encode_lineage_tail(tail.saturating_add(1));
        appended_records = appended_records.saturating_add(1);
        appended_bytes =
            appended_bytes.saturating_add(u64::try_from(locator_record.len()).unwrap_or(u64::MAX));
        request.conditions.push(Absent {
            path: locator.clone(),
        });
        request.mutations.push(Append {
            path: locator,
            records: vec![locator_record],
        });
    }
    Ok((appended_records, appended_bytes))
}

impl<P: acyclic_stream::StreamProvider> AsyncAuthorityStore for StreamAuthorityStore<P> {
    /// One durable commit creates the authority with its first record. An
    /// authority that already exists, from an earlier attempt or another
    /// writer, resolves through the ordinary idempotent append instead.
    async fn create_authority_with_first_record(
        &self,
        authority: AuthorityId,
        commit: ProposedCommit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<bool> {
        cancellation
            .check()
            .map_err(|_| OperationFailure::before_work(AuthorityStoreError::Cancelled))?;
        // The commit carries the operation's own retry identity, so the
        // operation is found like any appended one.
        let mut request = acyclic_stream::CommitRequest {
            conditions: Vec::new(),
            mutations: Vec::new(),
            idempotency_key: operation_key(authority, commit.operation_id)
                .map_err(OperationFailure::before_work)?,
        };
        let (records, bytes) = first_record_commit(&mut request, authority, &commit, None)
            .map_err(OperationFailure::before_work)?;
        let mut work = authority_write_work(records, bytes);
        admit_authority(work, budget)?;
        match self.provider.commit(request).await {
            Ok(acyclic_stream::CommitOutcome::Committed(_)) => {
                return authority_success(true, work, budget);
            }
            // The operation identity already named another request.
            Err(acyclic_stream::StreamError::IdempotencyMismatch) => {
                return authority_success(false, work, budget);
            }
            Ok(acyclic_stream::CommitOutcome::Conflict(_)) => {}
            Err(error) => return Err(OperationFailure::new(map_stream_error(error), work)),
        }
        // The authority exists: it is this creation exactly when its first
        // record is this commit. The identity now retains the conflict, so
        // no append under it is attempted.
        work = work
            .checked_add(authority_read_work(2))
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        admit_authority(work, budget)?;
        let records =
            records_path(authority).map_err(|error| OperationFailure::new(error, work))?;
        let first = match self.provider.tail(records.clone()).await {
            Ok(tail) if tail >= 2 => Some(
                read_one(self.provider.as_ref(), records, 1)
                    .await
                    .and_then(|record| decode_durable(authority, &record.value))
                    .map_err(|error| OperationFailure::new(error, work))?,
            ),
            Ok(_) | Err(acyclic_stream::StreamError::NotFound) => None,
            Err(error) => return Err(OperationFailure::new(map_stream_error(error), work)),
        };
        authority_success(
            first.is_some_and(|first| {
                first.sequence == Sequence::new(1)
                    && first.operation_id == commit.operation_id
                    && first.fingerprint == commit.fingerprint
            }),
            work,
            budget,
        )
    }

    /// One durable commit creates the whole fork. Anything that stops it
    /// from applying as a whole, such as an authority an earlier attempt or
    /// another writer created, or a retry whose lineage changed, resolves
    /// through the idempotent single-authority steps instead.
    async fn commit_workspace_fork(
        &self,
        fork: WorkspaceForkCommit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<WorkspaceForkOutcome> {
        cancellation
            .check()
            .map_err(|_| OperationFailure::before_work(AuthorityStoreError::Cancelled))?;
        let (request, work) = self.workspace_fork_request(&fork).await?;
        admit_authority(work, budget)?;
        match self.provider.commit(request).await {
            Ok(acyclic_stream::CommitOutcome::Committed(_)) => {
                return authority_success(WorkspaceForkOutcome::Committed, work, budget);
            }
            Ok(acyclic_stream::CommitOutcome::Conflict(_))
            | Err(acyclic_stream::StreamError::IdempotencyMismatch) => {}
            Err(error) => return Err(OperationFailure::new(map_stream_error(error), work)),
        }
        let stepped = crate::async_storage::commit_workspace_fork_in_steps(
            self,
            fork,
            work.remaining(budget)
                .map_err(|error| OperationFailure::new(error.into(), work))?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        let work = work
            .checked_add(stepped.work)
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        authority_success(stepped.value, work, budget)
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
        cancellation
            .check()
            .map_err(|_| OperationFailure::before_work(AuthorityStoreError::Cancelled))?;
        if source.lineage == crate::GenerationFork::Independent {
            return self
                .create_authority(destination_authority, Epoch::GENESIS, budget, cancellation)
                .await;
        }
        let (source_lineage, forked_at) = self
            .resolve_source_fork_point(source.authority, source.generation)
            .await?;
        let destination_records =
            records_path(destination_authority).map_err(OperationFailure::before_work)?;
        let destination_root =
            authority_path(destination_authority).map_err(OperationFailure::before_work)?;
        let destination_epochs =
            epochs_path(destination_authority).map_err(OperationFailure::before_work)?;
        let destination_lineage =
            lineage_path(destination_authority).map_err(OperationFailure::before_work)?;
        let destination_gate =
            publication_gate_path(destination_authority).map_err(OperationFailure::before_work)?;
        let destination_locator = generation_path(destination_authority, source.generation)
            .map_err(OperationFailure::before_work)?;
        if let Some(snapshot) = self
            .existing_fork_destination(destination_authority, source.generation, forked_at)
            .await
            .map_err(OperationFailure::before_work)?
        {
            let work = self
                .verify_publication_gate(destination_authority, authority_read_work(8), budget)
                .await?;
            return authority_success(
                CreateAuthorityOutcome::Existing(snapshot.head),
                work,
                budget,
            );
        }
        let request = fork_generation_commit_request(
            ForkDestinationPaths {
                root: destination_root,
                records: destination_records,
                epochs: destination_epochs,
                locator: destination_locator,
                lineage: destination_lineage,
                gate: destination_gate,
            },
            source_lineage,
            forked_at,
            operation_id,
        )?;
        let mut work = authority_write_work(4, 96);
        work.authority_records_read = 3;
        work.backend_read_operations = 3;
        admit_authority(work, budget)?;
        match self.provider.commit(request).await {
            Ok(acyclic_stream::CommitOutcome::Committed(_)) => authority_success(
                CreateAuthorityOutcome::Created(Head::genesis(Epoch::GENESIS)),
                work,
                budget,
            ),
            Ok(acyclic_stream::CommitOutcome::Conflict(_)) => {
                work = work
                    .checked_add(authority_read_work(6))
                    .map_err(|error| OperationFailure::new(error.into(), work))?;
                let snapshot = self
                    .existing_fork_destination(destination_authority, source.generation, forked_at)
                    .await
                    .map_err(|error| OperationFailure::new(error, work))?
                    .ok_or_else(|| {
                        OperationFailure::new(
                            AuthorityStoreError::Rejected(
                                "generation fork conflicted without creating its destination"
                                    .to_owned(),
                            ),
                            work,
                        )
                    })?;
                work = self
                    .verify_publication_gate(destination_authority, work, budget)
                    .await?;
                authority_success(
                    CreateAuthorityOutcome::Existing(snapshot.head),
                    work,
                    budget,
                )
            }
            Err(error) => Err(OperationFailure::new(map_stream_error(error), work)),
        }
    }

    async fn create_authority(
        &self,
        authority_id: AuthorityId,
        genesis_epoch: Epoch,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<CreateAuthorityOutcome> {
        cancellation
            .check()
            .map_err(|_| OperationFailure::before_work(AuthorityStoreError::Cancelled))?;
        match self.snapshot(authority_id).await {
            Ok(snapshot) => {
                let work = self
                    .verify_publication_gate(authority_id, authority_read_work(2), budget)
                    .await?;
                return authority_success(
                    CreateAuthorityOutcome::Existing(snapshot.head),
                    work,
                    budget,
                );
            }
            Err(AuthorityStoreError::Missing) => {}
            Err(error) => return Err(OperationFailure::before_work(error)),
        }
        let records = records_path(authority_id).map_err(OperationFailure::before_work)?;
        let epochs = epochs_path(authority_id).map_err(OperationFailure::before_work)?;
        let root = authority_path(authority_id).map_err(OperationFailure::before_work)?;
        let gate = publication_gate_path(authority_id).map_err(OperationFailure::before_work)?;
        let request = acyclic_stream::CommitRequest {
            conditions: vec![
                acyclic_stream::CommitCondition::Absent { path: root.clone() },
                acyclic_stream::CommitCondition::Absent {
                    path: records.clone(),
                },
                acyclic_stream::CommitCondition::Absent {
                    path: epochs.clone(),
                },
                acyclic_stream::CommitCondition::Absent { path: gate.clone() },
            ],
            mutations: vec![
                acyclic_stream::CommitMutation::Append {
                    path: root,
                    records: vec![Bytes::from_static(AUTHORITY_MARKER)],
                },
                acyclic_stream::CommitMutation::Append {
                    path: records,
                    records: vec![encode_epoch(GENESIS_DOMAIN, genesis_epoch)],
                },
                acyclic_stream::CommitMutation::Append {
                    path: epochs,
                    records: vec![encode_epoch(EPOCH_DOMAIN, genesis_epoch)],
                },
                acyclic_stream::CommitMutation::Append {
                    path: gate,
                    records: vec![Bytes::from_static(PUBLICATION_GATE_FREE)],
                },
            ],
            idempotency_key: stream_key(b"create", &authority_id.into_bytes())
                .map_err(OperationFailure::before_work)?,
        };
        let work = authority_write_work(2, 0);
        admit_authority(work, budget)?;
        match self.provider.commit(request).await {
            Ok(acyclic_stream::CommitOutcome::Committed(_)) => authority_success(
                CreateAuthorityOutcome::Created(Head::genesis(genesis_epoch)),
                work,
                budget,
            ),
            Ok(acyclic_stream::CommitOutcome::Conflict(_)) => {
                let snapshot = self
                    .snapshot(authority_id)
                    .await
                    .map_err(|error| OperationFailure::new(error, work))?;
                authority_success(
                    CreateAuthorityOutcome::Existing(snapshot.head),
                    work,
                    budget,
                )
            }
            Err(error) => Err(OperationFailure::new(map_stream_error(error), work)),
        }
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
            .snapshot(authority_id)
            .await
            .map_err(OperationFailure::before_work)?;
        authority_success(snapshot.head, authority_read_work(2), budget)
    }

    /// Deletes the authority's whole subtree of Stream paths, each once
    /// nothing lives beneath it; Stream then answers every path in it as
    /// retired. Each deletion has its own retry identity, so an interrupted
    /// retirement resumes where it stopped.
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
        let mut work = WorkCounters::default();
        self.retire_subtree(root, &mut work, cancellation)
            .await
            .map_err(|error| OperationFailure::new(error, work))?;
        authority_success((), work, budget)
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
                    | acyclic_stream::CommitConflict::Exists { path }
                    | acyclic_stream::CommitConflict::Retired { path } => path == &gate,
                });
                let permit_rejected = gate_rejected
                    || lease_path.as_ref().is_some_and(|lease_path| {
                        conflicts.iter().any(|conflict| match conflict {
                            acyclic_stream::CommitConflict::Tail { path, .. }
                            | acyclic_stream::CommitConflict::Exists { path }
                            | acyclic_stream::CommitConflict::Retired { path } => {
                                path == lease_path
                            }
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
            conditions: vec![acyclic_stream::CommitCondition::Tail {
                path: gate.clone(),
                expected: reservation.gate_tail,
            }],
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
        let mut stream = self
            .provider
            .read(acyclic_stream::ReadRequest {
                path,
                from: after.get().saturating_add(1),
                limit: limit.records,
            })
            .await
            .map_err(|error| OperationFailure::before_work(map_stream_error(error)))?;
        let mut commits = Vec::new();
        let mut payload_bytes = 0_u64;
        while let Some(record) = stream.next().await {
            let record =
                record.map_err(|error| OperationFailure::before_work(map_stream_error(error)))?;
            let durable = decode_durable(authority_id, &record.value)
                .map_err(OperationFailure::before_work)?;
            payload_bytes = payload_bytes
                .checked_add(u64::try_from(durable.payload.len()).unwrap_or(u64::MAX))
                .ok_or_else(|| {
                    OperationFailure::before_work(AuthorityStoreError::ReplayRecordTooLarge {
                        observed: u64::MAX,
                        maximum: limit.payload_bytes,
                    })
                })?;
            if payload_bytes > limit.payload_bytes {
                if commits.is_empty() {
                    return Err(OperationFailure::before_work(
                        AuthorityStoreError::ReplayRecordTooLarge {
                            observed: payload_bytes,
                            maximum: limit.payload_bytes,
                        },
                    ));
                }
                break;
            }
            commits.push(durable);
        }
        let work = WorkCounters {
            authority_records_read: u64::try_from(commits.len()).unwrap_or(u64::MAX),
            authority_bytes_read: payload_bytes,
            backend_read_operations: 1,
            ..WorkCounters::default()
        };
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
        cancellation
            .check()
            .map_err(|_| OperationFailure::before_work(AuthorityStoreError::Cancelled))?;
        let value = self
            .operation(authority_id, operation_id)
            .await
            .map_err(OperationFailure::before_work)?;
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

/// Immutable filesystem-object storage over one exact public Objects bucket.
///
/// The adapter contains no durable state. Object identity remains the canonical
/// filesystem digest; the Objects version is transport/storage evidence only.
#[derive(Clone)]
pub struct ProviderObjectStore<P> {
    provider: Arc<P>,
    bucket: wire::BucketRef,
}

fn provider_put_request(bucket: &wire::BucketRef, write: &ObjectWrite) -> PutRequest {
    PutRequest {
        bucket: bucket.clone(),
        object_key: object_key(write.object_id),
        body: write.bytes.clone(),
        metadata: wire::ObjectMetadata {
            content_type: "application/vnd.acyclic.fs-object-v1".to_owned(),
            ..wire::ObjectMetadata::default()
        },
        condition: Some(Condition::IfAbsent),
        idempotency_key: Some(format!(
            "fs-object-{}",
            hex::encode(write.object_id.digest.as_bytes())
        )),
    }
}

impl<P> ProviderObjectStore<P> {
    /// Binds an authenticated provider to the account's dedicated filesystem bucket.
    #[must_use]
    pub fn new(provider: Arc<P>, bucket: wire::BucketRef) -> Self {
        Self { provider, bucket }
    }

    /// Returns the exact backing bucket identity used by this adapter.
    #[must_use]
    pub fn bucket(&self) -> &wire::BucketRef {
        &self.bucket
    }

    /// Returns the exact public provider used by this stateless adapter.
    #[must_use]
    pub fn provider(&self) -> &Arc<P> {
        &self.provider
    }
}

impl<P: ObjectsProvider> AsyncObjectStore for ProviderObjectStore<P> {
    async fn put(
        &self,
        object_id: ObjectId,
        bytes: Bytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        cancellation
            .check()
            .map_err(|_| OperationFailure::before_work(ObjectStoreError::Cancelled))?;
        if object_digest(object_id.kind, &bytes) != object_id.digest {
            return Err(OperationFailure::before_work(
                ObjectStoreError::DigestMismatch,
            ));
        }
        let byte_count = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        let mut work = WorkCounters {
            backend_write_operations: 1,
            object_bytes_written: byte_count,
            bytes_hashed: byte_count,
            ..WorkCounters::default()
        };
        admit(work, budget)?;
        let request = PutRequest {
            bucket: self.bucket.clone(),
            object_key: object_key(object_id),
            body: bytes,
            metadata: wire::ObjectMetadata {
                content_type: "application/vnd.acyclic.fs-object-v1".to_owned(),
                ..wire::ObjectMetadata::default()
            },
            condition: Some(Condition::IfAbsent),
            idempotency_key: Some(format!(
                "fs-object-{}",
                hex::encode(object_id.digest.as_bytes())
            )),
        };
        match self.provider.put(request).await {
            Ok(version) if version.size == byte_count => success((), work, budget),
            Ok(_) => Err(OperationFailure::new(ObjectStoreError::Corrupt, work)),
            Err(ObjectsError::PreconditionFailed) => {
                work.backend_read_operations = work.backend_read_operations.saturating_add(1);
                let existing = self
                    .provider
                    .get(read_request(&self.bucket, object_id, byte_count))
                    .await
                    .map_err(|error| OperationFailure::new(map_objects_error(error), work))?;
                let existing_bytes = u64::try_from(existing.body.len()).unwrap_or(u64::MAX);
                work.object_bytes_read = work.object_bytes_read.saturating_add(existing_bytes);
                work.bytes_hashed = work.bytes_hashed.saturating_add(existing_bytes);
                if existing_bytes != byte_count
                    || object_digest(object_id.kind, &existing.body) != object_id.digest
                {
                    return Err(OperationFailure::new(ObjectStoreError::Corrupt, work));
                }
                success((), work, budget)
            }
            Err(error) => Err(OperationFailure::new(map_objects_error(error), work)),
        }
    }

    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        cancellation
            .check()
            .map_err(|_| OperationFailure::before_work(ObjectStoreError::Cancelled))?;
        if writes.is_empty() {
            return Err(OperationFailure::before_work(ObjectStoreError::Rejected(
                "object write batch is empty".to_owned(),
            )));
        }
        let (requests, unique_writes, mut work) =
            prepare_provider_put_batch(&self.bucket, writes, budget, cancellation)?;
        work.backend_write_operations = 1;
        admit(work, budget)?;
        let results = self.provider.put_batch(requests).await;
        if results.len() != unique_writes.len() {
            return Err(OperationFailure::new(ObjectStoreError::Corrupt, work));
        }
        for (write_index, result) in unique_writes.into_iter().zip(results) {
            let write = writes
                .get(write_index)
                .ok_or_else(|| OperationFailure::new(ObjectStoreError::Corrupt, work))?;
            let byte_count = u64::try_from(write.bytes.len()).unwrap_or(u64::MAX);
            match result {
                Ok(version) if version.size == byte_count => {}
                Ok(_) => return Err(OperationFailure::new(ObjectStoreError::Corrupt, work)),
                Err(ObjectsError::PreconditionFailed) => {
                    cancellation
                        .check()
                        .map_err(|_| OperationFailure::new(ObjectStoreError::Cancelled, work))?;
                    work.backend_read_operations = work.backend_read_operations.saturating_add(1);
                    let existing = self
                        .provider
                        .get(read_request(&self.bucket, write.object_id, byte_count))
                        .await
                        .map_err(|error| OperationFailure::new(map_objects_error(error), work))?;
                    let existing_bytes = u64::try_from(existing.body.len()).unwrap_or(u64::MAX);
                    work.object_bytes_read = work.object_bytes_read.saturating_add(existing_bytes);
                    work.bytes_hashed = work.bytes_hashed.saturating_add(existing_bytes);
                    if existing_bytes != byte_count
                        || object_digest(write.object_id.kind, &existing.body)
                            != write.object_id.digest
                    {
                        return Err(OperationFailure::new(ObjectStoreError::Corrupt, work));
                    }
                }
                Err(error) => {
                    return Err(OperationFailure::new(map_objects_error(error), work));
                }
            }
        }
        success((), work, budget)
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<ObjectRead> {
        cancellation
            .check()
            .map_err(|_| OperationFailure::before_work(ObjectStoreError::Cancelled))?;
        let value = self
            .provider
            .get(read_request(&self.bucket, object_id, maximum_bytes))
            .await
            .map_err(|error| OperationFailure::before_work(map_objects_error(error)))?;
        let observed = u64::try_from(value.body.len()).unwrap_or(u64::MAX);
        let work = WorkCounters {
            object_probes: 1,
            backend_read_operations: 1,
            object_bytes_read: observed,
            bytes_hashed: observed,
            bytes_copied: observed,
            allocation_operations: u64::from(!value.body.is_empty()),
            peak_allocation_bytes: observed,
            ..WorkCounters::default()
        };
        if observed > maximum_bytes {
            return Err(OperationFailure::new(
                ObjectStoreError::TooLarge {
                    observed,
                    maximum: maximum_bytes,
                },
                work,
            ));
        }
        if object_digest(object_id.kind, &value.body) != object_id.digest {
            return Err(OperationFailure::new(ObjectStoreError::Corrupt, work));
        }
        success(
            ObjectRead {
                bytes: value.body,
                retention: ObjectReadRetention::Owned {
                    logical_bytes: observed,
                },
            },
            work,
            budget,
        )
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<Vec<ObjectRead>> {
        if requests.is_empty() {
            return Err(OperationFailure::before_work(ObjectStoreError::Rejected(
                "object read batch is empty".to_owned(),
            )));
        }
        cancellation
            .check()
            .map_err(|_| OperationFailure::before_work(ObjectStoreError::Cancelled))?;
        let mut values = Vec::new();
        values.try_reserve_exact(requests.len()).map_err(|_| {
            OperationFailure::before_work(ObjectStoreError::Rejected(
                "object batch allocation failed".to_owned(),
            ))
        })?;
        let provider_requests = requests
            .iter()
            .map(|request| read_request(&self.bucket, request.object_id, request.maximum_bytes))
            .collect();
        let results = self.provider.get_batch(provider_requests).await;
        if results.len() != requests.len() {
            return Err(OperationFailure::before_work(ObjectStoreError::Corrupt));
        }
        let vector_bytes = u64::try_from(
            requests
                .len()
                .saturating_mul(size_of::<ObjectRead>() + size_of::<GetRequest>()),
        )
        .unwrap_or(u64::MAX);
        let retained_bytes = results.iter().try_fold(0_u64, |total, result| {
            total.checked_add(result.as_ref().map_or(0, |value| {
                u64::try_from(value.body.len()).unwrap_or(u64::MAX)
            }))
        });
        let retained_bytes = retained_bytes.ok_or_else(|| {
            OperationFailure::before_work(ObjectStoreError::Work(WorkError::Overflow))
        })?;
        let mut work = WorkCounters {
            backend_read_operations: 1,
            allocation_operations: 2,
            peak_allocation_bytes: vector_bytes.checked_add(retained_bytes).ok_or_else(|| {
                OperationFailure::before_work(ObjectStoreError::Work(WorkError::Overflow))
            })?,
            ..WorkCounters::default()
        };
        work.verify(budget)
            .map_err(|error| OperationFailure::before_work(error.into()))?;
        for (request, result) in requests.iter().zip(results) {
            cancellation
                .check()
                .map_err(|_| OperationFailure::new(ObjectStoreError::Cancelled, work))?;
            let value =
                result.map_err(|error| OperationFailure::new(map_objects_error(error), work))?;
            let observed = u64::try_from(value.body.len()).unwrap_or(u64::MAX);
            work = work
                .checked_add(WorkCounters {
                    object_probes: 1,
                    object_bytes_read: observed,
                    bytes_hashed: observed,
                    bytes_copied: observed,
                    ..WorkCounters::default()
                })
                .map_err(|error| OperationFailure::new(error.into(), work))?;
            work.verify(budget)
                .map_err(|error| OperationFailure::new(error.into(), work))?;
            if observed > request.maximum_bytes {
                return Err(OperationFailure::new(
                    ObjectStoreError::TooLarge {
                        observed,
                        maximum: request.maximum_bytes,
                    },
                    work,
                ));
            }
            if object_digest(request.object_id.kind, &value.body) != request.object_id.digest {
                return Err(OperationFailure::new(ObjectStoreError::Corrupt, work));
            }
            values.push(ObjectRead {
                bytes: value.body,
                retention: ObjectReadRetention::Owned {
                    logical_bytes: observed,
                },
            });
        }
        success(values, work, budget)
    }

    async fn contains(
        &self,
        object_id: ObjectId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<bool> {
        cancellation
            .check()
            .map_err(|_| OperationFailure::before_work(ObjectStoreError::Cancelled))?;
        let work = WorkCounters {
            object_probes: 1,
            backend_read_operations: 1,
            ..WorkCounters::default()
        };
        admit(work, budget)?;
        match self
            .provider
            .get(read_request(
                &self.bucket,
                object_id,
                budget.object_bytes_read,
            ))
            .await
        {
            Ok(value) => {
                let valid = object_digest(object_id.kind, &value.body) == object_id.digest;
                if valid {
                    success(true, work, budget)
                } else {
                    Err(OperationFailure::new(ObjectStoreError::Corrupt, work))
                }
            }
            Err(ObjectsError::NotFound) => success(false, work, budget),
            Err(error) => Err(OperationFailure::new(map_objects_error(error), work)),
        }
    }
}

pub(crate) fn object_key(object_id: ObjectId) -> String {
    format!(
        "fs/v1/{}/{}",
        object_id.kind.canonical_tag(),
        hex::encode(object_id.digest.as_bytes())
    )
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
) -> Result<acyclic_stream::CommitRequest, OperationFailure<AuthorityStoreError>> {
    Ok(acyclic_stream::CommitRequest {
        conditions: vec![
            acyclic_stream::CommitCondition::Absent {
                path: destination.root.clone(),
            },
            acyclic_stream::CommitCondition::Absent {
                path: destination.records.clone(),
            },
            acyclic_stream::CommitCondition::Absent {
                path: destination.epochs.clone(),
            },
            acyclic_stream::CommitCondition::Absent {
                path: destination.locator.clone(),
            },
            acyclic_stream::CommitCondition::Absent {
                path: destination.lineage.clone(),
            },
            acyclic_stream::CommitCondition::Absent {
                path: destination.gate.clone(),
            },
        ],
        mutations: vec![
            acyclic_stream::CommitMutation::Append {
                path: destination.root,
                records: vec![Bytes::from_static(AUTHORITY_MARKER)],
            },
            acyclic_stream::CommitMutation::Fork {
                source: source_lineage,
                destination: destination.lineage,
                at_tail: forked_at,
                records: Vec::new(),
            },
            acyclic_stream::CommitMutation::Append {
                path: destination.records,
                records: vec![encode_epoch(GENESIS_DOMAIN, Epoch::GENESIS)],
            },
            acyclic_stream::CommitMutation::Append {
                path: destination.epochs,
                records: vec![encode_epoch(EPOCH_DOMAIN, Epoch::GENESIS)],
            },
            acyclic_stream::CommitMutation::Append {
                path: destination.locator,
                records: vec![encode_lineage_tail(forked_at)],
            },
            acyclic_stream::CommitMutation::Append {
                path: destination.gate,
                records: vec![Bytes::from_static(PUBLICATION_GATE_FREE)],
            },
        ],
        idempotency_key: stream_key(b"fork-authority", &operation_id.into_bytes())
            .map_err(OperationFailure::before_work)?,
    })
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
    if let Ok(created) = decode_volume_created(payload, STREAM_RECORD_LIMIT) {
        if sequence != Sequence::new(1) || volume_authority_id(created.volume_id) != authority_id {
            return Err(AuthorityStoreError::Corrupt(
                "volume creation identity or sequence does not match its authority".to_owned(),
            ));
        }
        return Ok(Some(GenerationId::new(
            created.initial_generation_root.digest,
        )));
    }
    if let Ok(published) = decode_published_generation(payload, STREAM_RECORD_LIMIT) {
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
        acyclic_stream::StreamError::Retired => AuthorityStoreError::Retired,
        acyclic_stream::StreamError::Capacity => {
            AuthorityStoreError::Rejected("Stream capacity exhausted".to_owned())
        }
        other => AuthorityStoreError::Rejected(other.to_string()),
    }
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

fn read_request(bucket: &wire::BucketRef, object_id: ObjectId, maximum_bytes: u64) -> GetRequest {
    GetRequest {
        target: ReadTarget::Bucket(bucket.clone()),
        object_key: object_key(object_id),
        version_id: None,
        range: None,
        if_match: None,
        if_none_match: None,
        maximum_bytes,
    }
}

fn prepare_provider_put_batch(
    bucket: &wire::BucketRef,
    writes: &[ObjectWrite],
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(Vec<PutRequest>, Vec<usize>, WorkCounters), OperationFailure<ObjectStoreError>> {
    let minimum_bytes =
        u64::try_from(writes.len().saturating_mul(size_of::<PutRequest>())).unwrap_or(u64::MAX);
    let prospective = WorkCounters {
        allocation_operations: 1,
        peak_allocation_bytes: minimum_bytes,
        ..WorkCounters::default()
    };
    prospective
        .verify(budget)
        .map_err(|error| OperationFailure::before_work(error.into()))?;
    let mut requests = Vec::new();
    requests.try_reserve_exact(writes.len()).map_err(|_| {
        OperationFailure::before_work(ObjectStoreError::Rejected(
            "object batch allocation failed".to_owned(),
        ))
    })?;
    let mut work = WorkCounters {
        allocation_operations: 1,
        peak_allocation_bytes: u64::try_from(
            requests.capacity().saturating_mul(size_of::<PutRequest>()),
        )
        .unwrap_or(u64::MAX),
        ..WorkCounters::default()
    };
    admit(work, budget)?;
    let mut unique: BTreeMap<ObjectId, usize> = BTreeMap::new();
    let mut unique_writes = Vec::new();
    for (write_index, write) in writes.iter().enumerate() {
        cancellation
            .check()
            .map_err(|_| OperationFailure::new(ObjectStoreError::Cancelled, work))?;
        if object_digest(write.object_id.kind, &write.bytes) != write.object_id.digest {
            return Err(OperationFailure::new(
                ObjectStoreError::DigestMismatch,
                work,
            ));
        }
        let byte_count = u64::try_from(write.bytes.len()).unwrap_or(u64::MAX);
        work.object_bytes_written = work
            .object_bytes_written
            .checked_add(byte_count)
            .ok_or_else(|| {
                OperationFailure::new(
                    ObjectStoreError::Rejected("object batch byte count overflowed".to_owned()),
                    work,
                )
            })?;
        work.bytes_hashed = work.bytes_hashed.checked_add(byte_count).ok_or_else(|| {
            OperationFailure::new(
                ObjectStoreError::Rejected("object batch byte count overflowed".to_owned()),
                work,
            )
        })?;
        admit(work, budget)?;
        if let Some(existing_index) = unique.get(&write.object_id) {
            if writes
                .get(*existing_index)
                .is_none_or(|existing| existing.bytes != write.bytes)
            {
                return Err(OperationFailure::new(
                    ObjectStoreError::DigestMismatch,
                    work,
                ));
            }
        } else {
            unique.insert(write.object_id, write_index);
            unique_writes.push(write_index);
            requests.push(provider_put_request(bucket, write));
        }
    }
    Ok((requests, unique_writes, work))
}

fn map_objects_error(error: ObjectsError) -> ObjectStoreError {
    match error {
        ObjectsError::NotFound => ObjectStoreError::Missing,
        ObjectsError::Capacity => {
            ObjectStoreError::Rejected("Objects capacity exhausted".to_owned())
        }
        other => ObjectStoreError::Rejected(other.to_string()),
    }
}

fn admit(work: WorkCounters, budget: WorkBudget) -> Result<(), OperationFailure<ObjectStoreError>> {
    work.verify(budget)
        .map_err(|error| OperationFailure::new(error.into(), work))
}

fn success<T>(value: T, work: WorkCounters, budget: WorkBudget) -> ObjectResult<T> {
    admit(work, budget)?;
    Ok(ObjectReceipt { value, work })
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::kernel::{VolumeCreated, encode_publication_payload, encode_volume_created};
    use crate::model::{Lifecycle, VolumeConfig};
    use crate::storage::ObjectKind;

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

    #[tokio::test]
    async fn provider_object_batch_reads_once_and_preserves_order()
    -> Result<(), Box<dyn std::error::Error>> {
        let (provider, bucket) = acyclic_objects::MemoryObjects::with_default_bucket();
        let store = ProviderObjectStore::new(Arc::new(provider), bucket);
        let first_bytes = Bytes::from_static(b"first");
        let second_bytes = Bytes::from_static(b"second");
        let first = ObjectId {
            kind: ObjectKind::BlobChunk,
            digest: object_digest(ObjectKind::BlobChunk, &first_bytes),
        };
        let second = ObjectId {
            kind: ObjectKind::BlobChunk,
            digest: object_digest(ObjectKind::BlobChunk, &second_bytes),
        };
        let cancellation = CancellationToken::new();
        store
            .put(
                first,
                first_bytes.clone(),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
        store
            .put(
                second,
                second_bytes.clone(),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
        let receipt = store
            .read_many(
                &[
                    ObjectReadRequest {
                        object_id: second,
                        maximum_bytes: 6,
                    },
                    ObjectReadRequest {
                        object_id: first,
                        maximum_bytes: 5,
                    },
                ],
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
        assert_eq!(
            receipt.value.first().map(|value| &value.bytes),
            Some(&second_bytes)
        );
        assert_eq!(
            receipt.value.get(1).map(|value| &value.bytes),
            Some(&first_bytes)
        );
        assert_eq!(receipt.work.backend_read_operations, 1);
        assert_eq!(receipt.work.object_probes, 2);
        Ok(())
    }

    #[tokio::test]
    async fn provider_object_batch_counts_one_backend_write()
    -> Result<(), Box<dyn std::error::Error>> {
        let (provider, bucket) = acyclic_objects::MemoryObjects::with_default_bucket();
        let store = ProviderObjectStore::new(Arc::new(provider), bucket);
        let first_bytes = Bytes::from_static(b"first");
        let second_bytes = Bytes::from_static(b"second");
        let writes = [
            ObjectWrite {
                object_id: ObjectId {
                    kind: ObjectKind::BlobChunk,
                    digest: object_digest(ObjectKind::BlobChunk, &first_bytes),
                },
                bytes: first_bytes,
            },
            ObjectWrite {
                object_id: ObjectId {
                    kind: ObjectKind::BlobChunk,
                    digest: object_digest(ObjectKind::BlobChunk, &second_bytes),
                },
                bytes: second_bytes,
            },
        ];
        let receipt = store
            .put_many(&writes, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await?;
        assert_eq!(receipt.work.backend_write_operations, 1);
        assert_eq!(receipt.work.object_bytes_written, 11);
        Ok(())
    }

    #[test]
    fn provider_put_batch_interns_identical_object_requests()
    -> Result<(), Box<dyn std::error::Error>> {
        let bucket = wire::BucketRef {
            bucket_id: "bucket".to_owned(),
            name: "bucket".to_owned(),
        };
        let bytes = Bytes::from_static(b"shared");
        let object_id = ObjectId {
            kind: ObjectKind::BlobChunk,
            digest: object_digest(ObjectKind::BlobChunk, &bytes),
        };
        let writes = [
            ObjectWrite {
                object_id,
                bytes: bytes.clone(),
            },
            ObjectWrite { object_id, bytes },
        ];
        let (requests, unique_writes, work) = prepare_provider_put_batch(
            &bucket,
            &writes,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )?;
        assert_eq!(requests.len(), 1);
        assert_eq!(unique_writes, [0]);
        assert_eq!(work.object_bytes_written, 12);
        assert_eq!(work.bytes_hashed, 12);
        Ok(())
    }

    #[test]
    fn provider_put_batch_rejects_unadmitted_request_allocation() {
        let bucket = wire::BucketRef {
            bucket_id: "bucket".to_owned(),
            name: "bucket".to_owned(),
        };
        let bytes = Bytes::from_static(b"body");
        let writes = [ObjectWrite {
            object_id: ObjectId {
                kind: ObjectKind::BlobChunk,
                digest: object_digest(ObjectKind::BlobChunk, &bytes),
            },
            bytes,
        }];
        let mut budget = WorkBudget::UNBOUNDED;
        budget.allocation_operations = 0;
        let failure =
            prepare_provider_put_batch(&bucket, &writes, budget, &CancellationToken::new())
                .err()
                .unwrap_or_else(|| {
                    OperationFailure::before_work(ObjectStoreError::Rejected(
                        "unadmitted allocation unexpectedly succeeded".to_owned(),
                    ))
                });
        assert!(matches!(failure.error, ObjectStoreError::Work(_)));
        assert_eq!(*failure.work, WorkCounters::default());
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
}
