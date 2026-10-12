//! `IndexedDB` authority and immutable-object persistence.

use super::authority_codec::{
    COMMIT_PREFIX_BYTES, GATE_BYTES, HEAD_BYTES, OPERATION_BYTES, OperationRecord,
    PublicationGateRecord, authority_key, commit_key, decode_commit_owned, decode_head,
    decode_operation, decode_publication_gate, encode_commit, encode_head, encode_operation,
    encode_publication_gate, free_publication_gate, operation_key,
};
use crate::storage::{FenceOutcome, ObjectWrite};
use crate::{
    AppendOutcome, AsyncAuthorityStore, AsyncObjectStore, AuthorityFailure, AuthorityId,
    AuthorityResult, AuthorityStoreError, CancellationToken,
    CreateAuthorityOutcome, DurableCommit, Epoch, GenerationFork, GenerationForkSource,
    GuardedAppend, Head, OBJECT_DIGEST_ENVELOPE_BYTES, ObjectFailure, ObjectId, ObjectRead,
    ObjectReadRetention, ObjectResult, ObjectStoreError, OperationId,
    ProposedCommit, PublicationPermit, PublicationReservation, ReplayLimit, ReservationOutcome,
    Sequence, WorkBudget, WorkCounters, WorkspaceForkCommit, WorkspaceForkOutcome,
    authority_commit_digest, object_digest,
};
use acyclic_stream::OwnedBytes;
#[cfg(test)]
use bytes::Bytes;
use indexed_db_futures::database::Database;
use indexed_db_futures::prelude::*;
use indexed_db_futures::transaction::{
    Transaction, TransactionDurability, TransactionMode, TransactionOptions,
};
use indexed_db_futures::typed_array::Uint8Array as IndexedDbUint8Array;
#[cfg(test)]
use js_sys::Promise;
use js_sys::{Array, Uint8Array};
use std::future::{Future, poll_fn};
use std::mem::size_of;
use std::task::Poll;
use thiserror::Error;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::Blob;

const DATABASE_VERSION: u32 = 1;
const OBJECTS: &str = "objects";
const OBJECT_METADATA: &str = "object_metadata";
const AUTHORITY_HEADS: &str = "authority_heads";
const AUTHORITY_COMMITS: &str = "authority_commits";
const AUTHORITY_OPERATIONS: &str = "authority_operations";
const AUTHORITY_GATES: &str = "authority_gates";
const OPERATION_WINDOWS: &str = "operation_windows";
// Window state is control metadata, independent of the much larger immutable
// object ceiling. This also bounds decode work before constructing Rust leases.
const MAXIMUM_WINDOW_BYTES: u64 = 64 * 1024;
const OBJECT_KEY_BYTES: u64 = 66;

fn transaction<'a>(
    database: &'a Database, stores: &[&str], mode: TransactionMode,
) -> Result<Transaction<'a>, indexed_db_futures::error::Error> {
    match mode {
        TransactionMode::Readonly => database.transaction(stores).build(),
        _ => {
            let mut options = TransactionOptions::new();
            options.set_durability(TransactionDurability::Strict);
            database.transaction(stores).with_mode(mode).with_options(options).build()
        }
    }
}

/// Browser database initialization failures.
#[derive(Debug, Error)]
pub enum IndexedDbOpenError {
    /// Database and per-object byte bounds must be non-empty.
    #[error("IndexedDB name and maximum object bytes must be non-empty")]
    InvalidOptions,
    /// `IndexedDB` was unavailable, blocked, or rejected the schema upgrade.
    #[error("IndexedDB open failed: {0}")]
    Open(#[source] indexed_db_futures::error::OpenDbError),
}

impl From<IndexedDbOpenError> for JsValue {
    fn from(error: IndexedDbOpenError) -> Self {
        match error {
            IndexedDbOpenError::Open(indexed_db_futures::error::OpenDbError::Base(
                indexed_db_futures::error::Error::DomException(exception),
            )) => exception.into(),
            other => js_sys::Error::new(&other.to_string()).into(),
        }
    }
}

/// Transactional `IndexedDB` immutable-object backend.
pub struct IndexedDbObjectStore {
    database: Database,
    // Validated at open; copying an admitted object twice fits in u64.
    maximum_object_bytes: u64,
}

impl IndexedDbObjectStore {
    /// Opens or creates the canonical version-1 browser-local filesystem database.
    pub async fn open(
        database_name: &str,
        maximum_object_bytes: u64,
    ) -> Result<Self, IndexedDbOpenError> {
        if database_name.is_empty()
            || maximum_object_bytes == 0
            || maximum_object_bytes > u64::from(u32::MAX)
        {
            return Err(IndexedDbOpenError::InvalidOptions);
        }
        let database = open_database(database_name).await?;
        Ok(Self {
            database,
            maximum_object_bytes,
        })
    }

    pub(crate) fn key(object_id: ObjectId) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut key = String::with_capacity(OBJECT_KEY_BYTES as usize);
        key.push(char::from(b'0' + object_id.kind.canonical_tag()));
        key.push(':');
        for byte in object_id.digest.as_bytes() {
            key.push(char::from(HEX[usize::from(byte >> 4)]));
            key.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
        key
    }

    fn backend(error: impl std::fmt::Display, work: WorkCounters) -> ObjectFailure {
        ObjectFailure::new(ObjectStoreError::Rejected(error.to_string()), work)
    }

    async fn get_blob(
        objects: &indexed_db_futures::object_store::ObjectStore<'_>,
        key: &str,
        cancellation: &CancellationToken,
        work: WorkCounters,
    ) -> Result<Option<Blob>, ObjectFailure> {
        let request = objects
            .get::<JsValue, _, _>(key)
            .primitive()
            .map_err(|error| Self::backend(error, work))?;
        let value = await_cancellable(request, cancellation)
            .await
            .map_err(|error| cancellable_failure(error, work))?;
        value
            .map(|value| {
                value
                    .dyn_into::<Blob>()
                    .map_err(|_| ObjectFailure::new(ObjectStoreError::Corrupt, work))
            })
            .transpose()
    }

    async fn materialize_blob(
        blob: &Blob,
        expected_length: u64,
        cancellation: &CancellationToken,
        work: WorkCounters,
    ) -> Result<OwnedBytes, ObjectFailure> {
        if (blob.size() - expected_length as f64).abs() > 0.5 {
            return Err(ObjectFailure::new(ObjectStoreError::Corrupt, work));
        }
        let buffer = await_cancellable(JsFuture::from(blob.array_buffer()), cancellation)
            .await
            .map_err(|error| match error {
                None => ObjectFailure::new(ObjectStoreError::Cancelled, work),
                Some(error) => Self::backend(js_message(&error), work),
            })?;
        let array = Uint8Array::new(&buffer);
        if u64::from(array.length()) != expected_length {
            return Err(ObjectFailure::new(ObjectStoreError::Corrupt, work));
        }
        Ok(OwnedBytes::from(array.to_vec()))
    }

    async fn read_blob(
        object_id: ObjectId, blob: &Blob, length: u64,
        cancellation: &CancellationToken, work: WorkCounters,
    ) -> Result<ObjectRead, ObjectFailure> {
        let bytes = Self::materialize_blob(blob, length, cancellation, work).await?;
        if object_digest(object_id.kind, &bytes) != object_id.digest {
            return Err(ObjectFailure::new(ObjectStoreError::Corrupt, work));
        }
        Ok(ObjectRead {
            bytes,
            retention: ObjectReadRetention::Owned { logical_bytes: length },
        })
    }

    async fn metadata_length(
        metadata: &indexed_db_futures::object_store::ObjectStore<'_>,
        key: &str,
        cancellation: &CancellationToken,
        work: WorkCounters,
    ) -> Result<Option<u64>, ObjectFailure> {
        let request = metadata
            .get::<String, _, _>(key)
            .primitive()
            .map_err(|error| Self::backend(error, work))?;
        let value =
            await_cancellable(request, cancellation)
                .await
                .map_err(|error| match error {
                    None => ObjectFailure::new(ObjectStoreError::Cancelled, work),
                    Some(error) => Self::backend(error, work),
                })?;
        value
            .map(|value| {
                value
                    .parse::<u64>()
                    .map_err(|_| ObjectFailure::new(ObjectStoreError::Corrupt, work))
            })
            .transpose()
    }


}

/// Transactional `IndexedDB` authority backend.
#[derive(Clone)]
pub struct IndexedDbAuthorityStore {
    database: Database,
    maximum_payload_bytes: u64,
}

struct PreparedAppend {
    durable: DurableCommit,
    encoded_commit: Vec<u8>,
    work: WorkCounters,
}

impl IndexedDbAuthorityStore {
    async fn read_window(
        &self,
        transaction: &Transaction<'_>,
        workspace: crate::WorkspaceId,
        prior: WorkCounters,
        budget: WorkBudget,
    ) -> AuthorityResult<Option<crate::OperationWindowSnapshot>> {
        let work = prior
            .checked_add(authority_fixed_read_work(0))
            .map_err(|error| AuthorityFailure::new(error.into(), prior))?;
        Self::admit(work, budget)?;
        let store = transaction
            .object_store(OPERATION_WINDOWS)
            .map_err(|error| Self::backend(error, work))?;
        let key = authority_key(AuthorityId::from_bytes(workspace.into_bytes()));
        let value = store
            .get::<JsValue, _, _>(key.as_str())
            .primitive()
            .map_err(|error| Self::backend(error, work))?
            .await
            .map_err(|error| Self::backend(error, work))?;
        let Some(array) = value else {
            return Ok(crate::OperationReceipt { value: None, work });
        };
        let array = array.dyn_into::<Uint8Array>().map_err(|error| {
            Self::corrupt(format!("invalid operation window buffer: {error:?}"), work)
        })?;
        let length = u64::from(array.length());
        let maximum = self.maximum_payload_bytes.min(MAXIMUM_WINDOW_BYTES);
        if length > maximum {
            return Err(AuthorityFailure::new(
                AuthorityStoreError::PayloadTooLarge {
                    observed: length,
                    maximum,
                },
                work,
            ));
        }
        let copy_work = WorkCounters {
            bytes_copied: length,
            // Admit a conservative envelope for JSON values, map nodes and the input copy.
            allocation_operations: length + 1,
            peak_allocation_bytes: length * 128,
            items_examined: length,
            ..WorkCounters::default()
        };
        let work = work
            .checked_add(copy_work)
            .map_err(|error| AuthorityFailure::new(error.into(), work))?;
        Self::admit(work, budget)?;
        let snapshot: crate::OperationWindowSnapshot =
            serde_json::from_slice(&array.to_vec()).map_err(|error| Self::corrupt(error, work))?;
        if !snapshot.is_valid_for(workspace) {
            return Err(Self::corrupt("invalid operation window", work));
        }
        Ok(crate::OperationReceipt {
            value: Some(snapshot),
            work,
        })
    }

    async fn check_lease(
        &self,
        transaction: &Transaction<'_>,
        authority_id: AuthorityId,
        permit: PublicationPermit,
        work: WorkCounters,
        budget: WorkBudget,
    ) -> AuthorityResult<bool> {
        match permit {
            PublicationPermit::Lease {
                authority_id: permitted_authority,
                workspace_id,
                lease_id,
                expires_at_millis,
            } => {
                let workspace = crate::WorkspaceId::from_bytes(workspace_id);
                let receipt = self
                    .read_window(transaction, workspace, work, budget)
                    .await?;
                let work = receipt.work;
                let active = match receipt.value {
                    Some(crate::OperationWindowSnapshot {
                        phase: crate::OperationWindowPhase::Active { leases, .. },
                        ..
                    }) => leases
                        .get(&crate::OperationLeaseId::from_bytes(lease_id))
                        .is_some_and(|lease| lease.expires_at_millis == expires_at_millis),
                    _ => false,
                };
                let now = js_sys::Date::now()
                    .to_string()
                    .parse::<u64>()
                    .map_err(|error| Self::backend(error, work))?;
                let value = permitted_authority == authority_id.into_bytes()
                    && crate::kernel::volume_authority_id(workspace.volume_id()) == authority_id
                    && active
                    && now < expires_at_millis;
                Ok(crate::OperationReceipt { value, work })
            }
            _ => Ok(crate::OperationReceipt { value: false, work }),
        }
    }

    /// Opens or creates the canonical version-1 browser-local authority database.
    pub async fn open(
        database_name: &str,
        maximum_payload_bytes: u64,
    ) -> Result<Self, IndexedDbOpenError> {
        if database_name.is_empty()
            || maximum_payload_bytes == 0
            || maximum_payload_bytes > u64::from(u32::MAX) - COMMIT_PREFIX_BYTES as u64
        {
            return Err(IndexedDbOpenError::InvalidOptions);
        }
        Ok(Self {
            database: open_database(database_name).await?,
            maximum_payload_bytes,
        })
    }

    fn validate_payload(
        &self,
        commit: &ProposedCommit,
        work: WorkCounters,
    ) -> Result<(), AuthorityFailure> {
        let observed = commit.payload.len() as u64;
        if observed > self.maximum_payload_bytes {
            return Err(AuthorityFailure::new(
                AuthorityStoreError::PayloadTooLarge {
                    observed,
                    maximum: self.maximum_payload_bytes,
                },
                work,
            ));
        }
        Ok(())
    }

    fn backend(error: impl std::fmt::Display, work: WorkCounters) -> AuthorityFailure {
        AuthorityFailure::new(AuthorityStoreError::Rejected(error.to_string()), work)
    }

    fn corrupt(error: impl std::fmt::Display, work: WorkCounters) -> AuthorityFailure {
        AuthorityFailure::new(AuthorityStoreError::Corrupt(error.to_string()), work)
    }

    fn admit(work: WorkCounters, budget: WorkBudget) -> Result<(), AuthorityFailure> {
        work.verify(budget)
            .map_err(|error| AuthorityFailure::new(error.into(), work))
    }

    async fn get_fixed(
        store: &indexed_db_futures::object_store::ObjectStore<'_>,
        key: &str,
        expected_bytes: usize,
        cancellation: &CancellationToken,
        work: WorkCounters,
    ) -> Result<Option<Vec<u8>>, AuthorityFailure> {
        let request = store
            .get::<IndexedDbUint8Array, _, _>(key)
            .primitive()
            .map_err(|error| Self::backend(error, work))?;
        let value = await_cancellable(request, cancellation)
            .await
            .map_err(|error| authority_cancellable_failure(error, work))?;
        value
            .map(|array| {
                if array.len() != expected_bytes {
                    return Err(Self::corrupt("fixed record has the wrong length", work));
                }
                Ok(array.to_vec())
            })
            .transpose()
    }

    async fn get_blob(
        store: &indexed_db_futures::object_store::ObjectStore<'_>,
        key: &str,
        cancellation: &CancellationToken,
        work: WorkCounters,
    ) -> Result<Option<Blob>, AuthorityFailure> {
        let request = store
            .get::<JsValue, _, _>(key)
            .primitive()
            .map_err(|error| Self::backend(error, work))?;
        let value = await_cancellable(request, cancellation)
            .await
            .map_err(|error| authority_cancellable_failure(error, work))?;
        value
            .map(|value| {
                value
                    .dyn_into::<Blob>()
                    .map_err(|_| Self::corrupt("commit record is not a Blob", work))
            })
            .transpose()
    }

    fn blob_length(blob: &Blob, work: WorkCounters) -> Result<u64, AuthorityFailure> {
        let size = blob.size();
        if !size.is_finite() || size < 0.0 || size.fract() != 0.0 {
            return Err(Self::corrupt("commit Blob has a non-integral size", work));
        }
        if size >= u64::MAX as f64 {
            return Err(Self::corrupt("commit Blob size is not representable", work));
        }
        Ok(size as u64)
    }

    async fn materialize_blob(
        blob: &Blob,
        expected_length: u64,
        cancellation: &CancellationToken,
        work: WorkCounters,
    ) -> Result<Vec<u8>, AuthorityFailure> {
        if Self::blob_length(blob, work)? != expected_length {
            return Err(Self::corrupt("commit Blob length changed", work));
        }
        let buffer = await_cancellable(JsFuture::from(blob.array_buffer()), cancellation)
            .await
            .map_err(|error| match error {
                None => AuthorityFailure::new(AuthorityStoreError::Cancelled, work),
                Some(error) => Self::backend(js_message(&error), work),
            })?;
        let array = Uint8Array::new(&buffer);
        if u64::from(array.length()) != expected_length {
            return Err(Self::corrupt(
                "commit Blob materialized at a different size",
                work,
            ));
        }
        Ok(array.to_vec())
    }

    async fn write_record(
        store: &indexed_db_futures::object_store::ObjectStore<'_>,
        key: &str,
        value: JsValue,
        overwrite: bool,
        cancellation: &CancellationToken,
        work: WorkCounters,
    ) -> Result<(), AuthorityFailure> {
        let request = if overwrite {
            store.put(value).with_key(key).primitive()
        } else {
            store.add(value).with_key(key).primitive()
        }.map_err(|error| Self::backend(error, work))?;
        await_cancellable(request, cancellation).await
            .map_err(|error| authority_cancellable_failure(error, work))?;
        Ok(())
    }

    /// Reads the authority's head, which must exist.
    async fn read_head(
        transaction: &Transaction<'_>,
        key: &str,
        cancellation: &CancellationToken,
        work: WorkCounters,
    ) -> Result<Head, AuthorityFailure> {
        let heads = transaction
            .object_store(AUTHORITY_HEADS)
            .map_err(|error| Self::backend(error, work))?;
        let encoded = Self::get_fixed(&heads, key, HEAD_BYTES, cancellation, work)
            .await?
            .ok_or_else(|| AuthorityFailure::new(AuthorityStoreError::Missing, work))?;
        decode_head(&encoded).map_err(|error| Self::corrupt(error, work))
    }

    /// Reads the authority's publication gate, which every authority is created with.
    async fn read_gate(
        transaction: &Transaction<'_>,
        key: &str,
        cancellation: &CancellationToken,
        work: WorkCounters,
    ) -> Result<PublicationGateRecord, AuthorityFailure> {
        let gates = transaction
            .object_store(AUTHORITY_GATES)
            .map_err(|error| Self::backend(error, work))?;
        let encoded = Self::get_fixed(&gates, key, GATE_BYTES, cancellation, work)
            .await?
            .ok_or_else(|| Self::corrupt("authority has no publication gate", work))?;
        decode_publication_gate(&encoded).map_err(|error| Self::corrupt(error, work))
    }

    async fn read_operation(
        transaction: &Transaction<'_>, authority_id: AuthorityId, operation_id: OperationId,
        budget: WorkBudget, cancellation: &CancellationToken, work: WorkCounters,
    ) -> Result<(Option<OperationRecord>, WorkCounters), AuthorityFailure> {
        let admission = work.checked_add(authority_fixed_read_work(OPERATION_BYTES))
            .map_err(|error| AuthorityFailure::new(error.into(), work))?;
        Self::admit(admission, budget)?;
        let operations = transaction.object_store(AUTHORITY_OPERATIONS)
            .map_err(|error| Self::backend(error, admission))?;
        let encoded = Self::get_fixed(&operations, &operation_key(authority_id, operation_id),
            OPERATION_BYTES, cancellation, admission).await?;
        match encoded {
            Some(encoded) => Ok((Some(decode_operation(&encoded)
                .map_err(|error| Self::corrupt(error, admission))?), admission)),
            None => Ok((None, work.checked_add(authority_fixed_read_work(0))
                .map_err(|error| AuthorityFailure::new(error.into(), work))?)),
        }
    }

    async fn resolve_existing_operation(
        &self,
        transaction: &Transaction<'_>,
        authority_id: AuthorityId,
        commit: &ProposedCommit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        work: WorkCounters,
    ) -> Result<(Option<AppendOutcome>, WorkCounters), AuthorityFailure> {
        let (operation, work) = Self::read_operation(
            transaction, authority_id, commit.operation_id, budget, cancellation, work,
        ).await?;
        let Some(operation) = operation else { return Ok((None, work)); };
        if operation.fingerprint != commit.fingerprint {
            return Ok((
                Some(AppendOutcome::IdempotencyConflict {
                    committed_fingerprint: operation.fingerprint,
                }),
                work,
            ));
        }
        let (durable, work) = self
            .load_indexed_commit(
                transaction,
                authority_id, commit.operation_id, operation, budget, work,
                cancellation,
            )
            .await?;
        Ok((Some(AppendOutcome::AlreadyCommitted(durable)), work))
    }

    async fn load_indexed_commit(
        &self,
        transaction: &Transaction<'_>,
        authority_id: AuthorityId, operation_id: OperationId, operation: OperationRecord,
        budget: WorkBudget, mut work: WorkCounters,
        cancellation: &CancellationToken,
    ) -> Result<(DurableCommit, WorkCounters), AuthorityFailure> {
        work = work
            .checked_add(WorkCounters {
                backend_read_operations: 1,
                ..WorkCounters::default()
            })
            .map_err(|error| AuthorityFailure::new(error.into(), work))?;
        Self::admit(work, budget)?;
        let blob = {
            let commits = transaction
                .object_store(AUTHORITY_COMMITS)
                .map_err(|error| Self::backend(error, work))?;
            Self::get_blob(
                &commits,
                &commit_key(authority_id, operation.sequence),
                cancellation,
                work,
            )
            .await?
            .ok_or_else(|| Self::corrupt("operation commit is missing", work))?
        };
        let encoded_length = Self::blob_length(&blob, work)?;
        let payload_length =
            authority_payload_length(encoded_length, self.maximum_payload_bytes, work)?;
        let materialize_work = authority_commit_read_work(encoded_length, payload_length);
        work = work
            .checked_add(materialize_work)
            .map_err(|error| AuthorityFailure::new(error.into(), work))?;
        Self::admit(work, budget)?;
        let encoded = Self::materialize_blob(&blob, encoded_length, cancellation, work).await?;
        let durable = decode_commit_owned(authority_id, encoded, self.maximum_payload_bytes)
            .map_err(|error| Self::corrupt(error, work))?;
        if durable.operation_id != operation_id
            || durable.sequence != operation.sequence
            || durable.fingerprint != operation.fingerprint
        {
            return Err(Self::corrupt(
                "operation index does not match its commit",
                work,
            ));
        }
        Ok((durable, work))
    }

    fn prepare_append(
        authority_id: AuthorityId,
        epoch: Epoch,
        actual: Head,
        commit: ProposedCommit,
        budget: WorkBudget,
        work: WorkCounters,
    ) -> Result<PreparedAppend, AuthorityFailure> {
        let sequence = actual.sequence.checked_next().map_err(|error| {
            AuthorityFailure::new(
                AuthorityStoreError::Rejected(format!(
                    "authority sequence cannot advance: {error}"
                )),
                work,
            )
        })?;
        // Admission must precede both hashing and allocating the encoded body.
        let payload_bytes = commit.payload.len() as u64;
        let encoded_commit_bytes = payload_bytes + COMMIT_PREFIX_BYTES as u64;
        let fixed_bytes = (HEAD_BYTES + OPERATION_BYTES) as u64;
        let append_work = WorkCounters {
            authority_records_appended: 1,
            authority_bytes_written: payload_bytes,
            backend_write_operations: 3,
            // Multiple first records share one strict transaction commit.
            durability_operations: u64::from(work.durability_operations == 0),
            bytes_hashed: payload_bytes,
            bytes_copied: encoded_commit_bytes * 2 + fixed_bytes,
            bytes_encoded: encoded_commit_bytes + fixed_bytes,
            allocation_operations: 6,
            peak_allocation_bytes: encoded_commit_bytes * 3 + fixed_bytes * 2,
            ..WorkCounters::default()
        };
        let write_work = work
            .checked_add(append_work)
            .map_err(|error| AuthorityFailure::new(error.into(), work))?;
        Self::admit(write_work, budget).map_err(|failure| AuthorityFailure::new(failure.error, work))?;
        let digest = authority_commit_digest(
            authority_id,
            epoch,
            sequence,
            commit.operation_id,
            commit.fingerprint,
            actual.digest,
            &commit.payload,
        );
        let durable = DurableCommit {
            settled_at_micros: None,
            epoch,
            sequence,
            operation_id: commit.operation_id,
            fingerprint: commit.fingerprint,
            previous_digest: actual.digest,
            digest,
            payload: commit.payload,
        };
        let encoded_commit = encode_commit(&durable).map_err(|error| Self::backend(error, work))?;
        Ok(PreparedAppend {
            encoded_commit,
            durable,
            work: write_work,
        })
    }

    async fn publish_prepared(
        transaction: Transaction<'_>,
        authority_id: AuthorityId,
        prepared: PreparedAppend,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<AppendOutcome> {
        Self::write_prepared(&transaction, authority_id, &prepared, cancellation).await?;
        transaction
            .commit()
            .await
            .map_err(|error| Self::backend(error, prepared.work))?;
        Ok(crate::OperationReceipt {
            value: AppendOutcome::Committed(prepared.durable),
            work: prepared.work,
        })
    }

    /// Writes one prepared append into `transaction`, which the caller commits.
    async fn write_prepared(
        transaction: &Transaction<'_>,
        authority_id: AuthorityId,
        prepared: &PreparedAppend,
        cancellation: &CancellationToken,
    ) -> Result<(), AuthorityFailure> {
        let commit_blob = create_blob(&prepared.encoded_commit).map_err(|error| Self::backend(js_message(&error), prepared.work))?;
        {
            let commits = transaction
                .object_store(AUTHORITY_COMMITS)
                .map_err(|error| Self::backend(error, prepared.work))?;
            Self::write_record(&commits, &commit_key(authority_id, prepared.durable.sequence), JsValue::from(commit_blob), false, cancellation, prepared.work)
            .await?;
        }
        {
            let operations = transaction
                .object_store(AUTHORITY_OPERATIONS)
                .map_err(|error| Self::backend(error, prepared.work))?;
            let encoded = encode_operation(OperationRecord {
                sequence: prepared.durable.sequence, fingerprint: prepared.durable.fingerprint,
            });
            Self::write_record(&operations, &operation_key(authority_id, prepared.durable.operation_id), JsValue::from(Uint8Array::from(encoded.as_slice())), false, cancellation, prepared.work)
            .await?;
        }
        {
            let heads = transaction
                .object_store(AUTHORITY_HEADS)
                .map_err(|error| Self::backend(error, prepared.work))?;
            let encoded = encode_head(Head {
                epoch: prepared.durable.epoch, sequence: prepared.durable.sequence, digest: prepared.durable.digest,
            });
            Self::write_record(&heads, &authority_key(authority_id), JsValue::from(Uint8Array::from(encoded.as_slice())), true, cancellation, prepared.work)
            .await?;
        }
        Ok(())
    }

    /// Whether `authority` already exists, read in `transaction`.
    async fn authority_exists(
        transaction: &Transaction<'_>,
        authority: AuthorityId,
        cancellation: &CancellationToken,
        work: WorkCounters,
    ) -> Result<bool, AuthorityFailure> {
        let heads = transaction
            .object_store(AUTHORITY_HEADS)
            .map_err(|error| Self::backend(error, work))?;
        Ok(Self::get_fixed(
            &heads,
            &authority_key(authority),
            HEAD_BYTES,
            cancellation,
            work,
        )
        .await?
        .is_some())
    }

    /// Writes, in `transaction`, a new authority whose first record is `commit`.
    async fn write_first_record(
        &self,
        transaction: &Transaction<'_>,
        authority: AuthorityId,
        commit: ProposedCommit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        work: WorkCounters,
    ) -> Result<WorkCounters, AuthorityFailure> {
        self.validate_payload(&commit, work)?;
        let gate_work = work
            .checked_add(WorkCounters {
                durability_operations: 0,
                ..authority_fixed_write_work(GATE_BYTES, 1)
            })
            .map_err(|error| AuthorityFailure::new(error.into(), work))?;
        Self::admit(gate_work, budget)?;
        let gates = transaction
            .object_store(AUTHORITY_GATES)
            .map_err(|error| Self::backend(error, gate_work))?;
        Self::write_record(&gates, &authority_key(authority), JsValue::from(Uint8Array::from(encode_publication_gate(free_publication_gate()).as_slice())), false, cancellation, gate_work)
        .await?;
        let prepared = Self::prepare_append(
            authority,
            Epoch::GENESIS,
            Head::genesis(Epoch::GENESIS),
            commit,
            budget,
            gate_work,
        )?;
        Self::write_prepared(transaction, authority, &prepared, cancellation).await?;
        Ok(prepared.work)
    }



}

impl crate::OperationWindowStore for IndexedDbAuthorityStore {
    type Error = AuthorityStoreError;

    async fn load(
        &self,
        workspace: crate::WorkspaceId,
    ) -> Result<Option<crate::OperationWindowSnapshot>, Self::Error> {
        let transaction = transaction(&self.database, &[OPERATION_WINDOWS], TransactionMode::Readonly)
            .map_err(|error| AuthorityStoreError::Rejected(error.to_string()))?;
        self.read_window(&transaction, workspace, WorkCounters::default(), WorkBudget::UNBOUNDED)
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
    }

    async fn compare_and_swap(
        &self,
        workspace: crate::WorkspaceId,
        expected_revision: u64,
        replacement: crate::OperationWindowSnapshot,
    ) -> Result<bool, Self::Error> {
        if !replacement.is_valid_for(workspace)
            || expected_revision.checked_add(1) != Some(replacement.revision)
        {
            return Err(AuthorityStoreError::Corrupt(
                "invalid window replacement".to_owned(),
            ));
        }
        let maximum = self.maximum_payload_bytes.min(MAXIMUM_WINDOW_BYTES) as usize;
        let mut encoded = vec![0; maximum];
        let mut encoder = std::io::Cursor::new(encoded.as_mut_slice());
        serde_json::to_writer(&mut encoder, &replacement)
            .map_err(|error| AuthorityStoreError::Rejected(error.to_string()))?;
        let length = encoder.position() as usize;
        encoded.truncate(length);
        let transaction = transaction(&self.database, &[OPERATION_WINDOWS], TransactionMode::Readwrite)
            .map_err(|error| AuthorityStoreError::Rejected(error.to_string()))?;
        if self
            .read_window(&transaction, workspace, WorkCounters::default(), WorkBudget::UNBOUNDED)
            .await
            .map_err(|failure| failure.error)?
            .value
            .map_or(0, |s| s.revision)
            != expected_revision
        {
            return Ok(false);
        }
        let store = transaction
            .object_store(OPERATION_WINDOWS)
            .map_err(|error| AuthorityStoreError::Rejected(error.to_string()))?;
        let key = authority_key(AuthorityId::from_bytes(workspace.into_bytes()));
        store
            .put(JsValue::from(Uint8Array::from(encoded.as_slice())))
            .with_key(key.as_str())
            .primitive()
            .map_err(|error| AuthorityStoreError::Rejected(error.to_string()))?
            .await
            .map_err(|error| AuthorityStoreError::Rejected(error.to_string()))?;
        transaction
            .commit()
            .await
            .map_err(|error| AuthorityStoreError::Rejected(error.to_string()))?;
        Ok(true)
    }
}

fn create_blob(bytes: &[u8]) -> Result<Blob, JsValue> {
    let parts = Array::new();
    parts.push(&Uint8Array::from(bytes));
    Blob::new_with_u8_array_sequence(&parts)
}

async fn open_database(database_name: &str) -> Result<Database, IndexedDbOpenError> {
    Database::open(database_name)
        .with_version(DATABASE_VERSION)
        .with_on_upgrade_needed(|event, database| {
            if event.old_version() > 0.5 {
                return Err(indexed_db_futures::error::Error::from(js_sys::Error::new(
                    "unsupported Acyclic IndexedDB filesystem schema version",
                )));
            }
            for store in [
                OBJECTS,
                OBJECT_METADATA,
                AUTHORITY_HEADS,
                AUTHORITY_COMMITS,
                AUTHORITY_OPERATIONS,
                AUTHORITY_GATES,
                OPERATION_WINDOWS,
            ] {
                database.create_object_store(store).build()?;
            }
            Ok(())
        })
        .await
        .map_err(IndexedDbOpenError::Open)
}

impl AsyncAuthorityStore for IndexedDbAuthorityStore {async fn create_first_record<G: crate::AuthorityGuard>(&self, authority: AuthorityId, commit: ProposedCommit, context: &mut crate::AuthorityWork<'_>, mut guard: Option<&mut G>) -> Result<bool, crate::AuthorityError<G::Error>> {
if guard.is_some() { return crate::async_storage::first_record(self, authority, commit, context, guard).await; }
let budget = context.remaining()?;
let cancellation = context.cancellation;
let result: AuthorityResult<bool> = async {

        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        let work = authority_fixed_read_work(HEAD_BYTES);
        Self::admit(work, budget)?;
        let transaction = transaction(&self.database, &[AUTHORITY_HEADS, AUTHORITY_COMMITS, AUTHORITY_OPERATIONS, AUTHORITY_GATES], TransactionMode::Readwrite)
            .map_err(|error| Self::backend(error, work))?;
        if Self::authority_exists(&transaction, authority, cancellation, work).await? {
            drop(transaction);
            let appended =
                {
                    let mut fallback = crate::AuthorityWork::new(budget, cancellation);
                    fallback.work = work;
                    let result = crate::async_storage::first_record(self, authority, commit, &mut fallback, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity));
                    fallback.receipt(result)?
                };
            return Ok(appended);
        }
        let work = self
            .write_first_record(&transaction, authority, commit, budget, cancellation, work)
            .await?;
        transaction
            .commit()
            .await
            .map_err(|error| Self::backend(error, work))?;
        Ok(crate::OperationReceipt { value: true, work })

}.await;
let value = context.accept(result)?;
Ok(value)
}
async fn fork_workspace<G: crate::AuthorityGuard>(&self, fork: WorkspaceForkCommit, permit: PublicationPermit, context: &mut crate::AuthorityWork<'_>, mut guard: Option<&mut G>) -> Result<WorkspaceForkOutcome, crate::AuthorityError<G::Error>> {
if guard.is_some() || permit != PublicationPermit::Unrestricted { return crate::async_storage::workspace_fork(self, fork, permit, context, guard).await; }
let budget = context.remaining()?;
let cancellation = context.cancellation;
let result: AuthorityResult<WorkspaceForkOutcome> = async {

        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        let published_prefix = fork
            .lineage
            .is_some_and(|source| source.lineage == GenerationFork::PublishedPrefix);
        let work = authority_fixed_read_work(HEAD_BYTES)
            .checked_add(authority_fixed_read_work(HEAD_BYTES))
            .map_err(|error| AuthorityFailure::new(error.into(), WorkCounters::default()))?;
        Self::admit(work, budget)?;
        let transaction = transaction(&self.database, &[AUTHORITY_HEADS, AUTHORITY_COMMITS, AUTHORITY_OPERATIONS, AUTHORITY_GATES], TransactionMode::Readwrite)
            .map_err(|error| Self::backend(error, work))?;
        if published_prefix
            || Self::authority_exists(&transaction, fork.retention, cancellation, work).await?
            || Self::authority_exists(&transaction, fork.destination, cancellation, work).await?
        {
            drop(transaction);
            let mut fallback = crate::AuthorityWork::new(budget, cancellation);
            // The original fallback starts a fresh budget, then adds this probe's work.
            let result = crate::async_storage::workspace_fork(self, fork, PublicationPermit::Unrestricted, &mut fallback, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity));
            return fallback.receipt(result)
                .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))
                .and_then(|stepped| Ok(crate::OperationReceipt {
                    value: stepped.value,
                    work: work.checked_add(stepped.work).map_err(|error| AuthorityFailure::new(error.into(), work))?,
                }));
        }
        let work = self
            .write_first_record(
                &transaction,
                fork.retention,
                fork.retained,
                budget,
                cancellation,
                work,
            )
            .await?;
        let work = self
            .write_first_record(
                &transaction,
                fork.destination,
                fork.creation,
                budget,
                cancellation,
                work,
            )
            .await?;
        transaction
            .commit()
            .await
            .map_err(|error| Self::backend(error, work))?;
        Ok(crate::OperationReceipt {
            value: WorkspaceForkOutcome::Committed,
            work,
        })

}.await;
let value = context.accept(result)?;
Ok(value)
}
async fn fork_authority<G: crate::AuthorityGuard>(&self, source: GenerationForkSource, destination_authority: AuthorityId, _operation_id: OperationId, context: &mut crate::AuthorityWork<'_>, mut guard: Option<&mut G>) -> Result<CreateAuthorityOutcome, crate::AuthorityError<G::Error>> {
crate::async_storage::seal(guard.as_deref_mut(), context).await?;
let budget = context.remaining()?;
let cancellation = context.cancellation;
let result: AuthorityResult<CreateAuthorityOutcome> = async {

        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        if source.lineage == GenerationFork::PublishedPrefix {
            return Err(AuthorityFailure::before_work(
                AuthorityStoreError::Rejected(
                    "IndexedDB authorities do not retain generation lineage prefixes".to_owned(),
                ),
            ));
        }({ let __authority_args = (destination_authority, Epoch::GENESIS, budget, cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = self.create_authority(__authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) })

}.await;
let value = context.accept(result)?;
if matches!(value, CreateAuthorityOutcome::Existing(_)) { crate::async_storage::seal(guard.as_deref_mut(), context).await?; }
Ok(value)
}
async fn create_authority<G: crate::AuthorityGuard>(&self, authority_id: AuthorityId, genesis_epoch: Epoch, context: &mut crate::AuthorityWork<'_>, mut guard: Option<&mut G>) -> Result<CreateAuthorityOutcome, crate::AuthorityError<G::Error>> {
crate::async_storage::seal(guard.as_deref_mut(), context).await?;
let budget = context.remaining()?;
let cancellation = context.cancellation;
let result: AuthorityResult<CreateAuthorityOutcome> = async {

        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        let read_admission = authority_fixed_read_work(HEAD_BYTES);
        Self::admit(read_admission, budget)?;
        let transaction = transaction(&self.database, &[AUTHORITY_HEADS, AUTHORITY_GATES], TransactionMode::Readwrite)
            .map_err(|error| Self::backend(error, read_admission))?;
        let key = authority_key(authority_id);
        let existing = {
            let heads = transaction
                .object_store(AUTHORITY_HEADS)
                .map_err(|error| Self::backend(error, read_admission))?;
            Self::get_fixed(&heads, &key, HEAD_BYTES, cancellation, read_admission).await?
        };
        if let Some(encoded) = existing {
            let head =
                decode_head(&encoded).map_err(|error| Self::corrupt(error, read_admission))?;
            let gate_work = read_admission
                .checked_add(authority_fixed_read_work(GATE_BYTES))
                .map_err(|error| AuthorityFailure::new(error.into(), read_admission))?;
            Self::admit(gate_work, budget)?;
            Self::read_gate(&transaction, &key, cancellation, gate_work).await?;
            return Ok(crate::OperationReceipt {
                value: CreateAuthorityOutcome::Existing(head),
                work: gate_work,
            });
        }
        let read_work = authority_backend_read_work();
        let head = Head::genesis(genesis_epoch);
        let encoded = encode_head(head);
        let write_work = read_work
            .checked_add(authority_fixed_write_work(HEAD_BYTES + GATE_BYTES, 2))
            .map_err(|error| AuthorityFailure::new(error.into(), read_work))?;
        Self::admit(write_work, budget)?;
        {
            let heads = transaction
                .object_store(AUTHORITY_HEADS)
                .map_err(|error| Self::backend(error, read_work))?;
            Self::write_record(&heads, &key, JsValue::from(Uint8Array::from(encoded.as_slice())), false, cancellation, write_work).await?;
            let gates = transaction
                .object_store(AUTHORITY_GATES)
                .map_err(|error| Self::backend(error, write_work))?;
            let encoded_gate = encode_publication_gate(free_publication_gate());
            Self::write_record(&gates, &key, JsValue::from(Uint8Array::from(encoded_gate.as_slice())), false, cancellation, write_work).await?;
        }
        transaction
            .commit()
            .await
            .map_err(|error| Self::backend(error, write_work))?;
        Ok(crate::OperationReceipt {
            value: CreateAuthorityOutcome::Created(head),
            work: write_work,
        })

}.await;
let value = context.accept(result)?;
if matches!(value, CreateAuthorityOutcome::Existing(_)) { crate::async_storage::seal(guard.as_deref_mut(), context).await?; }
Ok(value)
}
async fn head(&self, authority_id: AuthorityId, context: &mut crate::AuthorityWork<'_>) -> Result<Head, AuthorityStoreError> {

let budget = context.remaining()?;
let cancellation = context.cancellation;
let result: AuthorityResult<Head> = async {

        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        let work = authority_fixed_read_work(HEAD_BYTES);
        Self::admit(work, budget)?;
        let transaction = transaction(&self.database, &[AUTHORITY_HEADS], TransactionMode::Readonly)
            .map_err(|error| Self::backend(error, work))?;
        let value = Self::read_head(
            &transaction,
            &authority_key(authority_id),
            cancellation,
            work,
        )
        .await?;
        Ok(crate::OperationReceipt { value, work })

}.await;
let value = context.accept(result)?;
Ok(value)
}
async fn compare_and_append<G: crate::AuthorityGuard>(&self, request: GuardedAppend, context: &mut crate::AuthorityWork<'_>, mut guard: Option<&mut G>) -> Result<(AppendOutcome, crate::AuthorityAllocation), crate::AuthorityError<G::Error>> {
crate::async_storage::seal(guard.as_deref_mut(), context).await?;
let budget = context.remaining()?;
let cancellation = context.cancellation;
let result: AuthorityResult<AppendOutcome> = async {

        let GuardedAppend {
            authority_id,
            epoch,
            expected,
            commit,
            permit,
        } = request;
        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        let payload_bytes = self.payload_bytes(&commit, WorkCounters::default())?;
        let work = authority_fixed_read_work(HEAD_BYTES);
        Self::admit(work, budget)?;
        let transaction = transaction(&self.database, &[ AUTHORITY_HEADS, AUTHORITY_COMMITS, AUTHORITY_OPERATIONS, AUTHORITY_GATES, OPERATION_WINDOWS, ], TransactionMode::Readwrite)
            .map_err(|error| Self::backend(error, work))?;
        let key = authority_key(authority_id);
        let actual = Self::read_head(&transaction, &key, cancellation, work).await?;
        if epoch != actual.epoch {
            return Ok(crate::OperationReceipt {
                value: AppendOutcome::Fenced {
                    actual_epoch: actual.epoch,
                },
                work,
            });
        }
        let (existing, work) = self
            .resolve_existing_operation(
                &transaction,
                authority_id,
                &commit,
                budget,
                cancellation,
                work,
            )
            .await?;
        if let Some(value) = existing {
            return Ok(crate::OperationReceipt { value, work });
        }
        if expected != actual {
            return Ok(crate::OperationReceipt {
                value: AppendOutcome::Conflict { actual },
                work,
            });
        }
        let mut work = work
            .checked_add(authority_fixed_read_work(GATE_BYTES))
            .map_err(|error| AuthorityFailure::new(error.into(), work))?;
        Self::admit(work, budget)?;
        let gate = Self::read_gate(&transaction, &key, cancellation, work).await?;
        let admitted = match permit {
            PublicationPermit::Reservation {
                operation_id,
                gate_tail,
                expected: reserved_head,
            } => {
                gate.tail == gate_tail
                    && gate.active == Some(OperationId::from_bytes(operation_id))
                    && reserved_head == expected
            }
            PublicationPermit::Unrestricted => gate.active.is_none(),
            permit @ PublicationPermit::Lease { .. } => {
                let receipt = self
                    .check_lease(&transaction, authority_id, permit, work, budget)
                    .await?;
                work = receipt.work;
                gate.active.is_none() && receipt.value
            }
        };
        if !admitted {
            return Ok(crate::OperationReceipt {
                value: AppendOutcome::Fenced {
                    actual_epoch: actual.epoch,
                },
                work,
            });
        }
        let prepared = Self::prepare_append(
            authority_id,
            epoch,
            actual,
            commit,
            payload_bytes,
            budget,
            work,
        )?;
        Self::publish_prepared(transaction, authority_id, prepared, cancellation).await

}.await;
let value = context.accept(result)?;
Ok((value, None))
}
async fn reserve_publication(&self, authority_id: AuthorityId, expected: Head, operation_id: OperationId, context: &mut crate::AuthorityWork<'_>) -> Result<ReservationOutcome, AuthorityStoreError> {

let budget = context.remaining()?;
let cancellation = context.cancellation;
let result: AuthorityResult<ReservationOutcome> = async {

        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        let read_work = authority_fixed_read_work(HEAD_BYTES)
            .checked_add(authority_fixed_read_work(GATE_BYTES))
            .map_err(|error| AuthorityFailure::before_work(error.into()))?;
        Self::admit(read_work, budget)?;
        let transaction = transaction(&self.database, &[AUTHORITY_HEADS, AUTHORITY_GATES], TransactionMode::Readwrite)
            .map_err(|error| Self::backend(error, read_work))?;
        let key = authority_key(authority_id);
        let actual = Self::read_head(&transaction, &key, cancellation, read_work).await?;
        let gate = Self::read_gate(&transaction, &key, cancellation, read_work).await?;
        if actual != expected || gate.active.is_some_and(|active| active != operation_id) {
            return Ok(crate::OperationReceipt {
                value: ReservationOutcome::Conflict { actual },
                work: read_work,
            });
        }
        if gate.active == Some(operation_id) {
            return Ok(crate::OperationReceipt {
                value: ReservationOutcome::AlreadyReserved(PublicationReservation {
                    authority_id,
                    operation_id,
                    expected,
                    gate_tail: gate.tail,
                }),
                work: read_work,
            });
        }
        let next_tail = gate.tail.checked_add(1).ok_or_else(|| {
            AuthorityFailure::new(
                AuthorityStoreError::Rejected("publication gate tail exhausted".to_owned()),
                read_work,
            )
        })?;
        let next = PublicationGateRecord {
            tail: next_tail,
            active: Some(operation_id),
            released: None,
        };
        let encoded = encode_publication_gate(next);
        let write_work = read_work
            .checked_add(authority_fixed_write_work(GATE_BYTES, 1))
            .map_err(|error| AuthorityFailure::new(error.into(), read_work))?;
        Self::admit(write_work, budget)?;
        let gates = transaction
            .object_store(AUTHORITY_GATES)
            .map_err(|error| Self::backend(error, write_work))?;
        Self::write_record(&gates, &key, JsValue::from(Uint8Array::from(encoded.as_slice())), true, cancellation, write_work).await?;
        transaction
            .commit()
            .await
            .map_err(|error| Self::backend(error, write_work))?;
        Ok(crate::OperationReceipt {
            value: ReservationOutcome::Reserved(PublicationReservation {
                authority_id,
                operation_id,
                expected,
                gate_tail: next_tail,
            }),
            work: write_work,
        })

}.await;
let value = context.accept(result)?;
Ok(value)
}
async fn release_publication(&self, reservation: PublicationReservation, context: &mut crate::AuthorityWork<'_>) -> Result<(), AuthorityStoreError> {

let budget = context.remaining()?;
let cancellation = context.cancellation;
let result: AuthorityResult<()> = async {

        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        let read_work = authority_fixed_read_work(GATE_BYTES);
        Self::admit(read_work, budget)?;
        let transaction = transaction(&self.database, &[AUTHORITY_GATES], TransactionMode::Readwrite)
            .map_err(|error| Self::backend(error, read_work))?;
        let key = authority_key(reservation.authority_id);
        let gate = Self::read_gate(&transaction, &key, cancellation, read_work).await?;
        if gate.released == Some((reservation.operation_id, reservation.gate_tail)) {
            return Ok(crate::OperationReceipt {
                value: (),
                work: read_work,
            });
        }
        if gate.tail != reservation.gate_tail || gate.active != Some(reservation.operation_id) {
            return Err(AuthorityFailure::new(
                AuthorityStoreError::Rejected(
                    "publication reservation is no longer owned by this operation".to_owned(),
                ),
                read_work,
            ));
        }
        let next_tail = gate.tail.checked_add(1).ok_or_else(|| {
            AuthorityFailure::new(
                AuthorityStoreError::Rejected("publication gate tail exhausted".to_owned()),
                read_work,
            )
        })?;
        let next = PublicationGateRecord {
            tail: next_tail,
            active: None,
            released: Some((reservation.operation_id, reservation.gate_tail)),
        };
        let encoded = encode_publication_gate(next);
        let write_work = read_work
            .checked_add(authority_fixed_write_work(GATE_BYTES, 1))
            .map_err(|error| AuthorityFailure::new(error.into(), read_work))?;
        Self::admit(write_work, budget)?;
        let gates = transaction
            .object_store(AUTHORITY_GATES)
            .map_err(|error| Self::backend(error, write_work))?;
        Self::write_record(&gates, &key, JsValue::from(Uint8Array::from(encoded.as_slice())), true, cancellation, write_work).await?;
        transaction
            .commit()
            .await
            .map_err(|error| Self::backend(error, write_work))?;
        Ok(crate::OperationReceipt {
            value: (),
            work: write_work,
        })

}.await;
let value = context.accept(result)?;
Ok(value)
}
async fn replay(&self, authority_id: AuthorityId, after: Sequence, limit: ReplayLimit, context: &mut crate::AuthorityWork<'_>) -> Result<(Vec<DurableCommit>, crate::AuthorityAllocation), AuthorityStoreError> {

let budget = context.remaining()?;
let cancellation = context.cancellation;
let result: AuthorityResult<Vec<DurableCommit>> = async {

        self.replay_page(authority_id, after, limit, budget, cancellation)
            .await

}.await;
let value = context.accept(result)?;
Ok((value, None))
}
async fn fence(&self, authority_id: AuthorityId, expected: Head, context: &mut crate::AuthorityWork<'_>) -> Result<FenceOutcome, AuthorityStoreError> {

let budget = context.remaining()?;
let cancellation = context.cancellation;
let result: AuthorityResult<FenceOutcome> = async {

        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        let read_work = authority_fixed_read_work(HEAD_BYTES);
        Self::admit(read_work, budget)?;
        let transaction = transaction(&self.database, &[AUTHORITY_HEADS], TransactionMode::Readwrite)
            .map_err(|error| Self::backend(error, read_work))?;
        let key = authority_key(authority_id);
        let actual = Self::read_head(&transaction, &key, cancellation, read_work).await?;
        if actual != expected {
            return Ok(crate::OperationReceipt {
                value: FenceOutcome::Conflict { actual },
                work: read_work,
            });
        }
        let next_epoch = actual
            .epoch
            .get()
            .checked_add(1)
            .ok_or_else(|| AuthorityFailure::new(AuthorityStoreError::EpochExhausted, read_work))?;
        let next = Head {
            epoch: Epoch::new(next_epoch).map_err(|error| Self::backend(error, read_work))?,
            ..actual
        };
        let encoded = encode_head(next);
        let fence_delta = authority_fixed_write_work(HEAD_BYTES, 1)
            .checked_add(WorkCounters {
                authority_records_appended: 1,
                ..WorkCounters::default()
            })
            .map_err(|error| AuthorityFailure::new(error.into(), read_work))?;
        let write_work = read_work
            .checked_add(fence_delta)
            .map_err(|error| AuthorityFailure::new(error.into(), read_work))?;
        Self::admit(write_work, budget)?;
        {
            let heads = transaction
                .object_store(AUTHORITY_HEADS)
                .map_err(|error| Self::backend(error, read_work))?;
            Self::write_record(&heads, &key, JsValue::from(Uint8Array::from(encoded.as_slice())), true, cancellation, write_work).await?;
        }
        transaction
            .commit()
            .await
            .map_err(|error| Self::backend(error, write_work))?;
        Ok(crate::OperationReceipt {
            value: FenceOutcome::Advanced(next),
            work: write_work,
        })

}.await;
let value = context.accept(result)?;
Ok(value)
}
async fn find_operation(&self, authority_id: AuthorityId, operation_id: OperationId, context: &mut crate::AuthorityWork<'_>) -> Result<(Option<DurableCommit>, crate::AuthorityAllocation), AuthorityStoreError> {

let budget = context.remaining()?;
let cancellation = context.cancellation;
let result: AuthorityResult<Option<DurableCommit>> = async {

        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        let head_work = authority_fixed_read_work(HEAD_BYTES);
        Self::admit(head_work, budget)?;
        let transaction = transaction(&self.database, &[AUTHORITY_HEADS, AUTHORITY_COMMITS, AUTHORITY_OPERATIONS], TransactionMode::Readonly)
            .map_err(|error| Self::backend(error, head_work))?;
        Self::read_head(
            &transaction,
            &authority_key(authority_id),
            cancellation,
            head_work,
        )
        .await?;
        let (operation, work) = Self::read_operation(
            &transaction, authority_id, operation_id, budget, cancellation, head_work,
        ).await?;
        let Some(operation) = operation else {
            return Ok(crate::OperationReceipt { value: None, work });
        };
        let (commit, work) = self
            .load_indexed_commit(
                &transaction,
                authority_id, operation_id, operation, budget, work,
                cancellation,
            )
            .await?;
        Ok(crate::OperationReceipt {
            value: Some(commit),
            work,
        })

}.await;
let value = context.accept(result)?;
Ok((value, None))
}
}

impl AsyncObjectStore for IndexedDbObjectStore {
    async fn put(
        &self,
        object_id: ObjectId,
        bytes: OwnedBytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        self.put_many(&[ObjectWrite { object_id, bytes }], budget, cancellation)
            .await
    }

    /// Admits every write in one read-write transaction, so the batch commits, and becomes durable, all at once or not at all.
    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        if writes.is_empty() {
            return Err(ObjectFailure::before_work(ObjectStoreError::Rejected(
                "object write batch is empty".to_owned(),
            )));
        }
        let mut work = WorkCounters::default();
        for write in writes {
            let length = write.bytes.len() as u64;
            if length > self.maximum_object_bytes {
                return Err(ObjectFailure::new(
                    ObjectStoreError::TooLarge {
                        observed: length,
                        maximum: self.maximum_object_bytes,
                    },
                    work,
                ));
            }
            work = work.checked_add(WorkCounters {
                object_probes: 1,
                backend_read_operations: 1,
                bytes_hashed: length + OBJECT_DIGEST_ENVELOPE_BYTES,
                allocation_operations: 1,
                peak_allocation_bytes: OBJECT_KEY_BYTES,
                ..WorkCounters::default()
            })
                .map_err(|error| ObjectFailure::new(error.into(), work))?;
            work.verify(budget)
                .map_err(|error| ObjectFailure::new(error.into(), work))?;
            if object_digest(write.object_id.kind, &write.bytes) != write.object_id.digest {
                return Err(ObjectFailure::new(ObjectStoreError::DigestMismatch, work));
            }
        }
        let retained_handles = writes.len() as u64 * size_of::<(Blob, &OwnedBytes)>() as u64;
        work = work.checked_add(WorkCounters {
            allocation_operations: 1,
            peak_allocation_bytes: retained_handles,
            ..WorkCounters::default()
        }).map_err(|error| ObjectFailure::new(error.into(), work))?;
        work.verify(budget).map_err(|error| ObjectFailure::new(error.into(), work))?;
        let mut existing = Vec::new();
        existing.try_reserve_exact(writes.len())
            .map_err(|error| Self::backend(error, work))?;
        let transaction = transaction(&self.database, &[OBJECTS, OBJECT_METADATA], TransactionMode::Readwrite)
            .map_err(|error| Self::backend(error, work))?;
        let mut added = false;
        for write in writes {
            let key = Self::key(write.object_id);
            let length = write.bytes.len() as u64;
            let existing_length = {
                let metadata = transaction
                    .object_store(OBJECT_METADATA)
                    .map_err(|error| Self::backend(error, work))?;
                Self::metadata_length(&metadata, &key, cancellation, work).await?
            };
            let copied = length * 2;
            let copy_work = WorkCounters {
                bytes_copied: copied,
                allocation_operations: 2 * u64::from(length != 0),
                peak_allocation_bytes: OBJECT_KEY_BYTES + copied + retained_handles,
                ..WorkCounters::default()
            };
            work = match existing_length {
                Some(existing_length) if existing_length != length => {
                    return Err(ObjectFailure::new(ObjectStoreError::Corrupt, work));
                }
                Some(_) => {
                    let prospective = work
                        .checked_add(WorkCounters {
                            backend_read_operations: 1,
                            object_bytes_read: length,
                            ..copy_work
                        })
                        .map_err(|error| ObjectFailure::new(error.into(), work))?;
                    prospective
                        .verify(budget)
                        .map_err(|error| ObjectFailure::new(error.into(), work))?;
                    let existing_blob = {
                        let objects = transaction
                            .object_store(OBJECTS)
                            .map_err(|error| Self::backend(error, work))?;
                        Self::get_blob(&objects, &key, cancellation, work).await?
                    }
                    .ok_or_else(|| ObjectFailure::new(ObjectStoreError::Corrupt, work))?;
                    existing.push((existing_blob, &write.bytes));
                    prospective
                }
                None => {
                    added = true;
                    let prospective = work
                        .checked_add(WorkCounters {
                            backend_write_operations: 2,
                            object_bytes_written: length,
                            ..copy_work
                        })
                        .map_err(|error| ObjectFailure::new(error.into(), work))?;
                    prospective
                        .verify(budget)
                        .map_err(|error| ObjectFailure::new(error.into(), work))?;
                    cancellation
                        .check()
                        .map_err(|_| ObjectFailure::new(ObjectStoreError::Cancelled, work))?;
                    let blob = create_blob(&write.bytes).map_err(|error| Self::backend(js_message(&error), prospective))?;
                    {
                        let objects = transaction
                            .object_store(OBJECTS)
                            .map_err(|error| Self::backend(error, work))?;
                        let request = objects
                            .add(JsValue::from(blob))
                            .with_key(key.as_str())
                            .primitive()
                            .map_err(|error| Self::backend(error, work))?;
                        await_cancellable(request, cancellation)
                            .await
                            .map_err(|error| cancellable_failure(error, work))?;
                    }
                    {
                        let metadata = transaction
                            .object_store(OBJECT_METADATA)
                            .map_err(|error| Self::backend(error, work))?;
                        let encoded_length = length.to_string();
                        let request = metadata
                            .add(encoded_length.as_str())
                            .with_key(key.as_str())
                            .primitive()
                            .map_err(|error| Self::backend(error, work))?;
                        await_cancellable(request, cancellation)
                            .await
                            .map_err(|error| cancellable_failure(error, work))?;
                    }
                    prospective
                }
            };
        }
        if added {
            work = work
                .checked_add(WorkCounters {
                    durability_operations: 1,
                    ..WorkCounters::default()
                })
                .map_err(|error| ObjectFailure::new(error.into(), work))?;
            work.verify(budget)
                .map_err(|error| ObjectFailure::new(error.into(), work))?;
            transaction
                .commit()
                .await
                .map_err(|error| Self::backend(error, work))?;
        } else {
            drop(transaction);
        }
        for (blob, bytes) in existing {
            let stored = Self::materialize_blob(&blob, bytes.len() as u64, cancellation, work).await?;
            if stored != *bytes { return Err(ObjectFailure::new(ObjectStoreError::Corrupt, work)); }
        }
        Ok(crate::OperationReceipt { value: (), work })
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
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        let work = WorkCounters {
            object_probes: 1,
            backend_read_operations: 1,
            allocation_operations: 1,
            peak_allocation_bytes: OBJECT_KEY_BYTES,
            ..WorkCounters::default()
        };
        work.verify(budget)
            .map_err(|error| ObjectFailure::before_work(error.into()))?;
        let key = Self::key(object_id);
        let transaction = transaction(&self.database, &[OBJECTS, OBJECT_METADATA], TransactionMode::Readonly)
            .map_err(|error| Self::backend(error, work))?;
        let length = {
            let metadata = transaction
                .object_store(OBJECT_METADATA)
                .map_err(|error| Self::backend(error, work))?;
            Self::metadata_length(&metadata, &key, cancellation, work)
                .await?
                .ok_or_else(|| ObjectFailure::new(ObjectStoreError::Missing, work))?
        };
        if length > maximum_bytes || length > self.maximum_object_bytes {
            return Err(ObjectFailure::new(
                ObjectStoreError::TooLarge {
                    observed: length,
                    maximum: maximum_bytes.min(self.maximum_object_bytes),
                },
                work,
            ));
        }
        let copied = length * 2;
        let peak_allocation_bytes = OBJECT_KEY_BYTES + copied;
        let prospective = work
            .checked_add(WorkCounters {
                backend_read_operations: 1,
                object_bytes_read: length,
                bytes_hashed: length + OBJECT_DIGEST_ENVELOPE_BYTES,
                bytes_copied: copied,
                allocation_operations: 2 * u64::from(length != 0),
                peak_allocation_bytes,
                ..WorkCounters::default()
            })
            .map_err(|error| ObjectFailure::new(error.into(), work))?;
        prospective
            .verify(budget)
            .map_err(|error| ObjectFailure::new(error.into(), work))?;
        let stored_blob = {
            let objects = transaction
                .object_store(OBJECTS)
                .map_err(|error| Self::backend(error, work))?;
            Self::get_blob(&objects, &key, cancellation, work).await?
        }
        .ok_or_else(|| ObjectFailure::new(ObjectStoreError::Corrupt, prospective))?;
        // Blob reads yield outside IndexedDB request callbacks. No transaction
        // may remain live across this materialization boundary.
        drop(transaction);
        Ok(crate::OperationReceipt {
            value: Self::read_blob(object_id, &stored_blob, length, cancellation, prospective).await?,
            work: prospective,
        })
    }

    async fn read_many(
        &self,
        requests: &[crate::ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<Vec<ObjectRead>> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        if requests.is_empty() {
            return Err(ObjectFailure::before_work(ObjectStoreError::Rejected(
                "object read batch is empty".to_owned(),
            )));
        }
        let item_count = requests.len() as u64;
        let vector_bytes = item_count * size_of::<ObjectRead>() as u64;
        let handle_vector_bytes = item_count * size_of::<(ObjectId, Blob, u64)>() as u64;
        let retained_vectors = vector_bytes + handle_vector_bytes;
        let work = WorkCounters {
            items_examined: item_count,
            allocation_operations: 2,
            peak_allocation_bytes: retained_vectors,
            ..WorkCounters::default()
        };
        let mut admission = work;
        admission.items_returned = item_count;
        admission
            .verify(budget)
            .map_err(|error| ObjectFailure::before_work(error.into()))?;
        let transaction = transaction(&self.database, &[OBJECTS, OBJECT_METADATA], TransactionMode::Readonly)
            .map_err(|error| Self::backend(error, work))?;
        let objects = transaction
            .object_store(OBJECTS)
            .map_err(|error| Self::backend(error, work))?;
        let metadata = transaction
            .object_store(OBJECT_METADATA)
            .map_err(|error| Self::backend(error, work))?;
        let mut values = Vec::new();
        values.try_reserve_exact(requests.len()).map_err(|_| {
            ObjectFailure::before_work(ObjectStoreError::Rejected(
                "object batch result allocation failed".to_owned(),
            ))
        })?;
        let mut handles = Vec::new();
        handles
            .try_reserve_exact(requests.len())
            .map_err(|error| Self::backend(error, work))?;
        let mut work = work;
        let mut retained = retained_vectors;
        for request in requests {
            cancellation.check()
                .map_err(|_| ObjectFailure::new(ObjectStoreError::Cancelled, work))?;
            let key = Self::key(request.object_id);
            work = work.checked_add(WorkCounters {
                object_probes: 1,
                backend_read_operations: 1,
                allocation_operations: 1,
                peak_allocation_bytes: retained.saturating_add(OBJECT_KEY_BYTES),
                ..WorkCounters::default()
            }).map_err(|error| ObjectFailure::new(error.into(), work))?;
            work.verify(budget).map_err(|error| ObjectFailure::new(error.into(), work))?;
            let length = Self::metadata_length(&metadata, &key, cancellation, work).await?
                .ok_or_else(|| ObjectFailure::new(ObjectStoreError::Missing, work))?;
            if length > request.maximum_bytes || length > self.maximum_object_bytes {
                return Err(ObjectFailure::new(ObjectStoreError::TooLarge {
                    observed: length,
                    maximum: request.maximum_bytes.min(self.maximum_object_bytes),
                }, work));
            }
            let prospective = work.checked_add(WorkCounters {
                backend_read_operations: 1,
                ..WorkCounters::default()
            }).map_err(|error| ObjectFailure::new(error.into(), work))?;
            prospective.verify(budget).map_err(|error| ObjectFailure::new(error.into(), work))?;
            work = prospective;
            let blob = Self::get_blob(&objects, &key, cancellation, work).await?
                .ok_or_else(|| ObjectFailure::new(ObjectStoreError::Corrupt, work))?;
            handles.push((request.object_id, blob, length));
        }
        // Blob materialization yields outside IndexedDB request callbacks.
        // Finish all metadata/blob requests before any such yield occurs.
        drop(metadata);
        drop(objects);
        drop(transaction);
        for (object_id, blob, length) in handles {
            let copied = length * 2;
            work = work.checked_add(WorkCounters {
                object_bytes_read: length,
                bytes_hashed: length + OBJECT_DIGEST_ENVELOPE_BYTES,
                bytes_copied: copied,
                allocation_operations: 2 * u64::from(length != 0),
                peak_allocation_bytes: retained.saturating_add(OBJECT_KEY_BYTES + copied),
                ..WorkCounters::default()
            }).map_err(|error| ObjectFailure::new(error.into(), work))?;
            work.verify(budget).map_err(|error| ObjectFailure::new(error.into(), work))?;
            let value = Self::read_blob(object_id, &blob, length, cancellation, work).await?;
            retained = retained.checked_add(length)
                .ok_or_else(|| ObjectFailure::new(crate::WorkError::Overflow.into(), work))?;
            values.push(value);
        }
        work.items_returned = item_count;
        Ok(crate::OperationReceipt {
            value: values,
            work,
        })
    }

    async fn contains(
        &self,
        object_id: ObjectId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<bool> {
        match AsyncObjectStore::read(
            self,
            object_id,
            self.maximum_object_bytes,
            budget,
            cancellation,
        )
        .await
        {
            Ok(receipt) => Ok(crate::OperationReceipt {
                value: true,
                work: receipt.work,
            }),
            Err(ObjectFailure {
                error: ObjectStoreError::Missing,
                work,
            }) => Ok(crate::OperationReceipt {
                value: false,
                work: *work,
            }),
            Err(failure) => Err(failure),
        }
    }
}

fn authority_fixed_read_work(record_bytes: usize) -> WorkCounters {
    let bytes = record_bytes as u64;
    WorkCounters {
        backend_read_operations: 1,
        bytes_copied: bytes,
        allocation_operations: u64::from(bytes != 0),
        peak_allocation_bytes: bytes,
        ..WorkCounters::default()
    }
}

fn authority_fixed_write_work(record_bytes: usize, operations: u64) -> WorkCounters {
    let bytes = record_bytes as u64;
    WorkCounters {
        backend_write_operations: operations,
        durability_operations: 1,
        bytes_copied: bytes,
        bytes_encoded: bytes,
        allocation_operations: u64::from(bytes != 0),
        peak_allocation_bytes: bytes * 2,
        ..WorkCounters::default()
    }
}

fn authority_payload_length(
    encoded_length: u64,
    maximum_payload_bytes: u64,
    work: WorkCounters,
) -> Result<u64, AuthorityFailure> {
    let prefix = COMMIT_PREFIX_BYTES as u64;
    let payload = encoded_length.checked_sub(prefix).ok_or_else(|| {
        IndexedDbAuthorityStore::corrupt("commit record is shorter than its prefix", work)
    })?;
    if payload > maximum_payload_bytes {
        return Err(IndexedDbAuthorityStore::corrupt(
            "commit record exceeds the configured payload bound",
            work,
        ));
    }
    Ok(payload)
}

fn authority_commit_read_work(
    encoded_length: u64,
    payload_length: u64,
) -> WorkCounters {
    // Payload validation bounds each encoded record to u32::MAX bytes.
    let copied = encoded_length * 2;
    WorkCounters {
        authority_records_read: 1,
        authority_bytes_read: payload_length,
        bytes_hashed: payload_length,
        bytes_copied: copied,
        allocation_operations: 2 * u64::from(encoded_length != 0),
        peak_allocation_bytes: copied,
        ..WorkCounters::default()
    }
}

fn cancellable_failure(
    error: Option<indexed_db_futures::error::Error>,
    work: WorkCounters,
) -> ObjectFailure {
    match error {
        None => ObjectFailure::new(ObjectStoreError::Cancelled, work),
        Some(error) => {
            ObjectFailure::new(ObjectStoreError::Rejected(error.to_string()), work)
        }
    }
}

fn authority_cancellable_failure(
    error: Option<indexed_db_futures::error::Error>,
    work: WorkCounters,
) -> AuthorityFailure {
    match error {
        None => AuthorityFailure::new(AuthorityStoreError::Cancelled, work),
        Some(error) => {
            AuthorityFailure::new(AuthorityStoreError::Rejected(error.to_string()), work)
        }
    }
}

fn js_message(value: &JsValue) -> String {
    if let Some(message) = value.as_string() {
        message
    } else {
        "browser JavaScript operation failed".to_owned()
    }
}

async fn await_cancellable<T, E>(
    operation: impl Future<Output = Result<T, E>>,
    cancellation: &CancellationToken,
) -> Result<T, Option<E>> {
    let mut operation = std::pin::pin!(operation);
    let mut cancelled = std::pin::pin!(cancellation.cancelled());
    poll_fn(|context| {
        if cancelled.as_mut().poll(context).is_ready() {
            return Poll::Ready(Err(None));
        }
        operation.as_mut().poll(context).map(|result| result.map_err(Some))
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AsyncObjectStore, Digest, ObjectKind, WorkError};
    use wasm_bindgen::JsValue;
    use wasm_bindgen_test::*;

    wasm_bindgen_test_configure!(run_in_browser);

    #[wasm_bindgen_test]
    async fn obsolete_database_version_is_rejected_without_deletion() -> Result<(), JsValue> {
        const NAME: &str = "acyclic-fs-obsolete-schema-rejection";
        const SENTINELS: &str = "legacy-sentinel-records";
        const KEY: &str = "original-record";
        const VALUE: &[u8] = b"obsolete-version-4-persisted-record";
        delete_database(NAME).await?;
        let obsolete_version = DATABASE_VERSION + 3;
        let old = Database::open(NAME)
            .with_version(obsolete_version)
            .with_on_upgrade_needed(|_, database| {
                database.create_object_store(SENTINELS).build()?;
                Ok(())
            })
            .await
            .map_err(js_error)?;
        let write = old.transaction(SENTINELS)
            .with_mode(TransactionMode::Readwrite)
            .build()
            .map_err(js_error)?;
        write.object_store(SENTINELS)
            .map_err(js_error)?
            .put(JsValue::from(Uint8Array::from(VALUE)))
            .with_key(KEY)
            .primitive()
            .map_err(js_error)?
            .await
            .map_err(js_error)?;
        write.commit().await.map_err(js_error)?;
        old.close();
        let object_failure = IndexedDbObjectStore::open(NAME, 1_024).await.err()
            .ok_or_else(|| js_error("obsolete object database was accepted"))?;
        let authority_failure = IndexedDbAuthorityStore::open(NAME, 1_024).await.err()
            .ok_or_else(|| js_error("obsolete authority database was accepted"))?;
        for failure in [object_failure, authority_failure] {
            let exception = JsValue::from(failure)
                .dyn_into::<web_sys::DomException>()
                .map_err(|_| js_error("version rejection lost its original DOM exception"))?;
            assert_eq!(exception.name(), "VersionError");
        }
        // The rejected version-4 database must remain intact.
        let retained = Database::open(NAME)
            .with_version(obsolete_version)
            .with_on_upgrade_needed(|_, _| {
                Err(indexed_db_futures::error::Error::from(js_sys::Error::new(
                    "rejected database must not be recreated",
                )))
            })
            .await
            .map_err(js_error)?;
        assert_eq!(retained.version(), f64::from(obsolete_version));
        let read = retained.transaction(SENTINELS)
            .build()
            .map_err(js_error)?;
        let unchanged = read.object_store(SENTINELS)
            .map_err(js_error)?
            .get::<JsValue, _, _>(KEY)
            .primitive()
            .map_err(js_error)?
            .await
            .map_err(js_error)?;
        let unchanged = unchanged.ok_or_else(|| js_error("obsolete sentinel was deleted"))?;
        assert_eq!(Uint8Array::new(&unchanged).to_vec().as_slice(), VALUE);
        drop(read);
        retained.close();
        delete_database(NAME).await?;
        Ok(())
    }

    #[wasm_bindgen_test]
    async fn indexed_db_objects_are_bounded_idempotent_and_authenticated() -> Result<(), JsValue> {
        const DATABASE_NAME: &str = "acyclic-fs-object-conformance-v2";
        delete_database(DATABASE_NAME).await?;
        let store = IndexedDbObjectStore::open(DATABASE_NAME, 1_024)
            .await
            .map_err(js_error)?;
        let input = b"browser-object-vector".to_vec();
        let bytes = OwnedBytes::from(input);
        let object_id = ObjectId {
            kind: ObjectKind::BlobChunk,
            digest: object_digest(ObjectKind::BlobChunk, &bytes),
        };
        let cancellation = CancellationToken::new();
        let first = AsyncObjectStore::put(
            &store,
            object_id,
            bytes.clone(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(js_error)?;
        assert_eq!(first.work.object_bytes_written, bytes.len() as u64);

        let retry = AsyncObjectStore::put(
            &store,
            object_id,
            bytes.clone(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(js_error)?;
        assert_eq!(retry.work.object_bytes_written, 0);
        assert_eq!(retry.work.object_bytes_read, bytes.len() as u64);
        let mut below_retry_peak = WorkBudget::UNBOUNDED;
        below_retry_peak.peak_allocation_bytes = retry.work.peak_allocation_bytes - 1;
        let denied = AsyncObjectStore::put(
            &store, object_id, bytes.clone(), below_retry_peak, &cancellation,
        )
        .await
        .err()
        .ok_or_else(|| js_error("retained existing Blob handle escaped admission"))?;
        assert!(matches!(
            denied.error,
            ObjectStoreError::Work(WorkError::BudgetExceeded {
                counter: "peak_allocation_bytes",
                ..
            })
        ));

        let read = AsyncObjectStore::read(
            &store,
            object_id,
            1_024,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(js_error)?;
        assert_eq!(read.value.bytes, bytes);
        assert_eq!(
            read.value.retention,
            ObjectReadRetention::Owned {
                logical_bytes: bytes.len() as u64
            }
        );
        assert!(
            AsyncObjectStore::contains(&store, object_id, WorkBudget::UNBOUNDED, &cancellation,)
                .await
                .map_err(js_error)?
                .value
        );

        let mut one_byte_short = WorkBudget::UNBOUNDED;
        one_byte_short.peak_allocation_bytes = OBJECT_KEY_BYTES + 2 * bytes.len() as u64 - 1;
        let bounded_result =
            AsyncObjectStore::read(&store, object_id, 1_024, one_byte_short, &cancellation).await;
        let bounded = bounded_result.err().ok_or_else(|| {
            JsValue::from_str("one-byte-short browser allocation budget unexpectedly succeeded")
        })?;
        assert!(matches!(
            bounded.error,
            ObjectStoreError::Work(WorkError::BudgetExceeded {
                counter: "peak_allocation_bytes",
                ..
            })
        ));

        let forged = ObjectId {
            kind: ObjectKind::BlobChunk,
            digest: Digest::from_bytes([9; 32]),
        };
        let mismatch_result = AsyncObjectStore::put(
            &store,
            forged,
            OwnedBytes::from_static(b"forged"),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await;
        let mismatch = mismatch_result
            .err()
            .ok_or_else(|| JsValue::from_str("forged browser object unexpectedly succeeded"))?;
        assert!(matches!(mismatch.error, ObjectStoreError::DigestMismatch));
        Ok(())
    }

    fn blob_write(bytes: &'static [u8]) -> ObjectWrite {
        let bytes = OwnedBytes::from_static(bytes);
        ObjectWrite {
            object_id: ObjectId {
                kind: ObjectKind::BlobChunk,
                digest: object_digest(ObjectKind::BlobChunk, &bytes),
            },
            bytes,
        }
    }

    #[wasm_bindgen_test]
    async fn indexed_db_object_batches_commit_all_or_nothing() -> Result<(), JsValue> {
        const DATABASE_NAME: &str = "acyclic-fs-object-batch-v1";
        delete_database(DATABASE_NAME).await?;
        let store = IndexedDbObjectStore::open(DATABASE_NAME, 1_024)
            .await
            .map_err(js_error)?;
        let cancellation = CancellationToken::new();
        let first = blob_write(b"first batch object");
        let second = blob_write(b"second batch object");
        let written = AsyncObjectStore::put_many(
            &store,
            &[first.clone(), second.clone()],
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(js_error)?;
        assert_eq!(
            written.work.object_bytes_written,
            (first.bytes.len() + second.bytes.len()) as u64
        );

        // A batch holding an object already stored verifies it and stores the rest.
        let third = blob_write(b"third batch object");
        let mixed = AsyncObjectStore::put_many(
            &store,
            &[first.clone(), third.clone()],
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(js_error)?;
        assert_eq!(mixed.work.object_bytes_written, third.bytes.len() as u64);
        assert_eq!(mixed.work.object_bytes_read, first.bytes.len() as u64);
        let batch = AsyncObjectStore::read_many(
            &store,
            &[first.clone(), second.clone(), third.clone()].map(|write| crate::ObjectReadRequest {
                object_id: write.object_id,
                maximum_bytes: 1_024,
            }),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(js_error)?;
        assert_eq!(
            batch
                .value
                .iter()
                .map(|read| read.bytes.clone())
                .collect::<Vec<_>>(),
            vec![
                first.bytes.clone(),
                second.bytes.clone(),
                third.bytes.clone()
            ]
        );
        let mut below_peak = WorkBudget::UNBOUNDED;
        below_peak.peak_allocation_bytes = batch.work.peak_allocation_bytes - 1;
        let requests = [first.clone(), second.clone(), third.clone()].map(|write| {
            crate::ObjectReadRequest {
                object_id: write.object_id,
                maximum_bytes: 1_024,
            }
        });
        let rejected = AsyncObjectStore::read_many(
            &store, &requests, below_peak, &cancellation,
        )
        .await
        .err()
        .ok_or_else(|| js_error("batch retained peak escaped admission"))?;
        assert!(matches!(
            rejected.error,
            ObjectStoreError::Work(WorkError::BudgetExceeded {
                counter: "peak_allocation_bytes",
                ..
            })
        ));
        // The second and third blobs must already have been gathered before
        // the first array_buffer promise yields and closes the transaction.
        let reopened = IndexedDbObjectStore::open(DATABASE_NAME, 1_024)
            .await
            .map_err(js_error)?;
        let persisted = AsyncObjectStore::read_many(
            &reopened, &requests, WorkBudget::UNBOUNDED, &cancellation,
        )
        .await
        .map_err(js_error)?;
        assert_eq!(persisted.value, batch.value);
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let rejected = AsyncObjectStore::read_many(
            &reopened, &requests, WorkBudget::UNBOUNDED, &cancelled,
        )
        .await
        .err()
        .ok_or_else(|| js_error("cancelled batch unexpectedly materialized"))?;
        assert!(matches!(rejected.error, ObjectStoreError::Cancelled));
        assert_eq!(rejected.work, WorkCounters::default());

        // One forged write rejects the whole batch before anything is stored.
        let fourth = blob_write(b"fourth batch object");
        let mut forged = blob_write(b"forged batch object");
        forged.object_id.digest = Digest::from_bytes([9; 32]);
        let rejected = AsyncObjectStore::put_many(
            &store,
            &[fourth.clone(), forged],
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .err()
        .ok_or_else(|| JsValue::from_str("forged batch unexpectedly succeeded"))?;
        assert!(matches!(rejected.error, ObjectStoreError::DigestMismatch));
        assert!(
            !AsyncObjectStore::contains(
                &store,
                fourth.object_id,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(js_error)?
            .value
        );
        for write in [first, second, third] {
            let read = AsyncObjectStore::read(
                &store,
                write.object_id,
                1_024,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(js_error)?;
            assert_eq!(read.value.bytes, write.bytes);
        }
        Ok(())
    }

    #[wasm_bindgen_test]
    async fn indexed_db_authority_is_atomic_fenced_idempotent_and_replayable() -> Result<(), JsValue>
    {
        const DATABASE_NAME: &str = "acyclic-fs-authority-conformance-v2";
        let store = open_clean_authority(DATABASE_NAME).await?;
        let authority_id = AuthorityId::from_bytes([7; 16]);
        let cancellation = CancellationToken::new();
        let created =({ let __authority_args = (authority_id, Epoch::GENESIS, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result =  AsyncAuthorityStore::create_authority(&store, __authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) })
        .map_err(js_error)?;
        let genesis = Head::genesis(Epoch::GENESIS);
        assert_eq!(created.value, CreateAuthorityOutcome::Created(genesis));
        assert_eq!(created.work.backend_read_operations, 1);
        assert_eq!(created.work.backend_write_operations, 2);
        assert_eq!(created.work.durability_operations, 1);
        let existing =({ let __authority_args = (authority_id, Epoch::new(9).map_err(js_error)?, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result =  AsyncAuthorityStore::create_authority(&store, __authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) })
        .map_err(js_error)?;
        assert_eq!(existing.value, CreateAuthorityOutcome::Existing(genesis));
        assert_eq!(existing.work.backend_read_operations, 2);
        let denied_gate =({ let __authority_args = (authority_id, Epoch::GENESIS, WorkBudget {
                backend_read_operations: 1,
                ..WorkBudget::UNBOUNDED
            }, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result =  AsyncAuthorityStore::create_authority(&store, __authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) });
        assert!(denied_gate.is_err(), "existing authority gate read escaped admission");

        let first_proposal = ProposedCommit {
            operation_id: OperationId::from_bytes([8; 16]),
            fingerprint: Digest::from_bytes([9; 32]),
            payload: Bytes::from_static(b"first"),
        };
        let denied = IndexedDbAuthorityStore::prepare_append(
            authority_id,
            Epoch::GENESIS,
            genesis,
            first_proposal.clone(),
            WorkBudget {
                bytes_hashed: 0,
                ..WorkBudget::UNBOUNDED
            },
            WorkCounters::default(),
        )
        .err()
        .ok_or_else(|| js_error("append exceeded its hash budget"))?;
        assert_eq!(denied.work.bytes_hashed, 0);
        let first =({ let __authority_args = (authority_id, Epoch::GENESIS, genesis, first_proposal.clone(), WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.4, __authority_args.5); let __authority_result =  AsyncAuthorityStore::compare_and_append(&store, crate::GuardedAppend { authority_id: __authority_args.0, epoch: __authority_args.1, expected: __authority_args.2, commit: __authority_args.3, permit: crate::PublicationPermit::Unrestricted }, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })
        .map_err(js_error)?;
        let AppendOutcome::Committed(first_commit) = first.value else {
            return Err(JsValue::from_str(
                "first authority append was not committed",
            ));
        };
        let retry =({ let __authority_args = (authority_id, Epoch::GENESIS, genesis, first_proposal.clone(), WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.4, __authority_args.5); let __authority_result =  AsyncAuthorityStore::compare_and_append(&store, crate::GuardedAppend { authority_id: __authority_args.0, epoch: __authority_args.1, expected: __authority_args.2, commit: __authority_args.3, permit: crate::PublicationPermit::Unrestricted }, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })
        .map_err(js_error)?;
        assert_eq!(
            retry.value,
            AppendOutcome::AlreadyCommitted(first_commit.clone())
        );
        let conflicting_identity = ProposedCommit {
            fingerprint: Digest::from_bytes([10; 32]),
            ..first_proposal
        };
        let identity_conflict =({ let __authority_args = (authority_id, Epoch::GENESIS, genesis, conflicting_identity, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.4, __authority_args.5); let __authority_result =  AsyncAuthorityStore::compare_and_append(&store, crate::GuardedAppend { authority_id: __authority_args.0, epoch: __authority_args.1, expected: __authority_args.2, commit: __authority_args.3, permit: crate::PublicationPermit::Unrestricted }, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })
        .map_err(js_error)?;
        assert_eq!(
            identity_conflict.value,
            AppendOutcome::IdempotencyConflict {
                committed_fingerprint: first_commit.fingerprint
            }
        );

        let second_commit =
            append_second(&store, authority_id, &cancellation, &first_commit).await?;
        verify_replay_and_lookup(
            &store,
            authority_id,
            &cancellation,
            first_commit,
            second_commit.clone(),
        )
        .await?;
        verify_fencing(&store, authority_id, &cancellation, second_commit).await
    }

    #[wasm_bindgen_test]
    async fn indexed_db_publication_reservations_are_atomic_and_idempotent() -> Result<(), JsValue>
    {
        const DATABASE_NAME: &str = "acyclic-fs-authority-reservation-v3";
        let store = open_clean_authority(DATABASE_NAME).await?;
        let authority_id = AuthorityId::from_bytes([31; 16]);
        let cancellation = CancellationToken::new();
        let genesis = Head::genesis(Epoch::GENESIS);({ let __authority_args = (authority_id, Epoch::GENESIS, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result =
        AsyncAuthorityStore::create_authority(&store, __authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) })
        .map_err(js_error)?;
        let operation_id = OperationId::from_bytes([32; 16]);
        let reserved =({ let __authority_args = (authority_id, genesis, operation_id, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.3, __authority_args.4); let __authority_result =  AsyncAuthorityStore::reserve_publication(&store, __authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work).await; __authority_work.receipt(__authority_result) })
        .map_err(js_error)?;
        let ReservationOutcome::Reserved(reservation) = reserved.value else {
            return Err(JsValue::from_str("publication gate was not reserved"));
        };
        let retry =({ let __authority_args = (authority_id, genesis, operation_id, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.3, __authority_args.4); let __authority_result =  AsyncAuthorityStore::reserve_publication(&store, __authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work).await; __authority_work.receipt(__authority_result) })
        .map_err(js_error)?;
        assert_eq!(
            retry.value,
            ReservationOutcome::AlreadyReserved(reservation)
        );
        let proposal = ProposedCommit {
            operation_id: OperationId::from_bytes([33; 16]),
            fingerprint: Digest::from_bytes([34; 32]),
            payload: Bytes::from_static(b"reserved"),
        };
        let blocked =({ let __authority_args = (authority_id, Epoch::GENESIS, genesis, proposal.clone(), WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.4, __authority_args.5); let __authority_result =  AsyncAuthorityStore::compare_and_append(&store, crate::GuardedAppend { authority_id: __authority_args.0, epoch: __authority_args.1, expected: __authority_args.2, commit: __authority_args.3, permit: crate::PublicationPermit::Unrestricted }, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })
        .map_err(js_error)?;
        assert!(matches!(blocked.value, AppendOutcome::Fenced { .. }));
        let committed =({ let __authority_args = (GuardedAppend {
                authority_id,
                epoch: Epoch::GENESIS,
                expected: genesis,
                commit: proposal,
                permit: PublicationPermit::Reservation {
                    operation_id: operation_id.into_bytes(),
                    gate_tail: reservation.gate_tail,
                    expected: genesis,
                },
            }, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result =  AsyncAuthorityStore::compare_and_append(&store, __authority_args.0, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })
        .map_err(js_error)?;
        assert!(matches!(committed.value, AppendOutcome::Committed(_)));
        release(&store, reservation, &cancellation)
            .await
            .map_err(js_error)?;
        release(&store, reservation, &cancellation)
            .await
            .map_err(js_error)?;
        let mut forged = reservation;
        forged.operation_id = OperationId::from_bytes([99; 16]);
        assert!(release(&store, forged, &cancellation).await.is_err());
        Ok(())
    }

    async fn release(
        store: &IndexedDbAuthorityStore,
        reservation: PublicationReservation,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<()> {({ let __authority_args = (reservation, WorkBudget::UNBOUNDED, cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result =
        AsyncAuthorityStore::release_publication(store, __authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) })
    }

    #[wasm_bindgen_test]
    async fn indexed_db_authority_serializes_competing_connections() -> Result<(), JsValue> {
        const DATABASE_NAME: &str = "acyclic-fs-authority-race-v2";
        let creator = open_clean_authority(DATABASE_NAME).await?;
        let authority_id = AuthorityId::from_bytes([21; 16]);({ let __authority_args = (authority_id, Epoch::GENESIS, WorkBudget::UNBOUNDED, &CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result =
        AsyncAuthorityStore::create_authority(&creator, __authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) })
        .map_err(js_error)?;
        let left = IndexedDbAuthorityStore::open(DATABASE_NAME, 1_024)
            .await
            .map_err(js_error)?;
        let right = IndexedDbAuthorityStore::open(DATABASE_NAME, 1_024)
            .await
            .map_err(js_error)?;
        let left_promise = competing_append(left, authority_id, 22);
        let right_promise = competing_append(right, authority_id, 23);
        let promises = Array::new();
        promises.push(&left_promise);
        promises.push(&right_promise);
        let joined = JsFuture::from(Promise::all(&promises)).await?;
        let results = Array::from(&joined);
        let mut codes = [results.get(0).as_string(), results.get(1).as_string()];
        codes.sort();
        assert_eq!(
            codes,
            [Some("committed".to_owned()), Some("conflict".to_owned())]
        );
        Ok(())
    }

    fn competing_append(
        store: IndexedDbAuthorityStore,
        authority_id: AuthorityId,
        identity_byte: u8,
    ) -> Promise {
        wasm_bindgen_futures::future_to_promise(async move {
            let outcome =({ let __authority_args = (authority_id, Epoch::GENESIS, Head::genesis(Epoch::GENESIS), ProposedCommit {
                    operation_id: OperationId::from_bytes([identity_byte; 16]),
                    fingerprint: Digest::from_bytes([identity_byte; 32]),
                    payload: Bytes::from_static(b"race"),
                }, WorkBudget::UNBOUNDED, &CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.4, __authority_args.5); let __authority_result =  AsyncAuthorityStore::compare_and_append(&store, crate::GuardedAppend { authority_id: __authority_args.0, epoch: __authority_args.1, expected: __authority_args.2, commit: __authority_args.3, permit: crate::PublicationPermit::Unrestricted }, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })
            .map_err(js_error)?;
            let code = match outcome.value {
                AppendOutcome::Committed(_) => "committed",
                AppendOutcome::Conflict { .. } => "conflict",
                _ => return Err(JsValue::from_str("unexpected competing append outcome")),
            };
            Ok(JsValue::from_str(code))
        })
    }

    async fn append_second(
        store: &IndexedDbAuthorityStore,
        authority_id: AuthorityId,
        cancellation: &CancellationToken,
        first: &DurableCommit,
    ) -> Result<DurableCommit, JsValue> {
        let first_head = Head {
            epoch: first.epoch,
            sequence: first.sequence,
            digest: first.digest,
        };
        let second =({ let __authority_args = (authority_id, Epoch::GENESIS, first_head, ProposedCommit {
                operation_id: OperationId::from_bytes([11; 16]),
                fingerprint: Digest::from_bytes([12; 32]),
                payload: Bytes::from_static(b"second"),
            }, WorkBudget::UNBOUNDED, cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.4, __authority_args.5); let __authority_result =  AsyncAuthorityStore::compare_and_append(store, crate::GuardedAppend { authority_id: __authority_args.0, epoch: __authority_args.1, expected: __authority_args.2, commit: __authority_args.3, permit: crate::PublicationPermit::Unrestricted }, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })
        .map_err(js_error)?;
        match second.value {
            AppendOutcome::Committed(commit) => Ok(commit),
            _ => Err(JsValue::from_str(
                "second authority append was not committed",
            )),
        }
    }

    #[wasm_bindgen_test]
    async fn indexed_db_forks_and_creates_in_one_transaction_and_resolves_retries()
    -> Result<(), JsValue> {
        const DATABASE_NAME: &str = "acyclic-fs-authority-fork-v1";
        let store = open_clean_authority(DATABASE_NAME).await?;
        let cancellation = CancellationToken::new();
        let proposal = |seed: u8| ProposedCommit {
            operation_id: OperationId::from_bytes([seed; 16]),
            fingerprint: Digest::from_bytes([seed; 32]),
            payload: Bytes::from(vec![seed; 8]),
        };
        let fork = WorkspaceForkCommit {
            lineage: None,
            destination: AuthorityId::from_bytes([31; 16]),
            creation: proposal(32),
            retention: AuthorityId::from_bytes([33; 16]),
            retained: proposal(34),
        };
        // Both authorities and both first records land in one transaction.
        let committed =({ let __authority_args = (fork.clone(), WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result =  AsyncAuthorityStore::fork_workspace(&store, __authority_args.0, crate::PublicationPermit::Unrestricted, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) })
        .map_err(js_error)?;
        assert_eq!(committed.value, WorkspaceForkOutcome::Committed);
        for (authority, commit) in [
            (fork.destination, &fork.creation),
            (fork.retention, &fork.retained),
        ] {
            let head =({ let __authority_args = (authority, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result =
                AsyncAuthorityStore::head(&store, __authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) })
                    .map_err(js_error)?
                    .value;
            assert_eq!(head.sequence, Sequence::new(1));
            let found =({ let __authority_args = (authority, commit.operation_id, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result =  AsyncAuthorityStore::find_operation(&store, __authority_args.0, __authority_args.1, &mut __authority_work).await.map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })
            .map_err(js_error)?
            .value
            .ok_or_else(|| JsValue::from_str("first record is not found by its operation"))?;
            assert_eq!(found.fingerprint, commit.fingerprint);
        }
        // A retry finds both first records and changes nothing.
        let retried =({ let __authority_args = (fork.clone(), WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result =  AsyncAuthorityStore::fork_workspace(&store, __authority_args.0, crate::PublicationPermit::Unrestricted, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) })
        .map_err(js_error)?;
        assert_eq!(retried.value, WorkspaceForkOutcome::Committed);
        // Another creation at the same destination is rejected.
        let rejected =({ let __authority_args = (WorkspaceForkCommit {
                creation: proposal(35),
                ..fork.clone()
            }, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result =  AsyncAuthorityStore::fork_workspace(&store, __authority_args.0, crate::PublicationPermit::Unrestricted, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) })
        .map_err(js_error)?;
        assert_eq!(rejected.value, WorkspaceForkOutcome::CreationRejected);

        // One authority with its first record, then its retry and a
        // conflicting creation.
        let volume = AuthorityId::from_bytes([36; 16]);
        for (commit, expected) in [
            (proposal(37), true),
            (proposal(37), true),
            (proposal(38), false),
        ] {
            let created =({ let __authority_args = (volume, commit, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result =  AsyncAuthorityStore::create_first_record(&store, __authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) })
            .map_err(js_error)?;
            assert_eq!(created.value, expected);
        }
        Ok(())
    }

    async fn delete_database(name: &str) -> Result<(), JsValue> {
        Database::delete_by_name(name).map_err(js_error)?.await.map_err(js_error)?;
        Ok(())
    }

    async fn open_clean_authority(database_name: &str) -> Result<IndexedDbAuthorityStore, JsValue> {
        delete_database(database_name).await?;
        IndexedDbAuthorityStore::open(database_name, 1_024)
            .await
            .map_err(js_error)
    }

    async fn verify_replay_and_lookup(
        store: &IndexedDbAuthorityStore,
        authority_id: AuthorityId,
        cancellation: &CancellationToken,
        first: DurableCommit,
        second: DurableCommit,
    ) -> Result<(), JsValue> {
        let replay =({ let __authority_args = (authority_id, Sequence::GENESIS, ReplayLimit {
                records: 8,
                payload_bytes: 64,
            }, WorkBudget::UNBOUNDED, cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.3, __authority_args.4); let __authority_result =  AsyncAuthorityStore::replay(store, __authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work).await.map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })
        .map_err(js_error)?;
        assert_eq!(replay.value, vec![first.clone(), second.clone()]);
        let retained_peak = 2 * size_of::<DurableCommit>() as u64
            + COMMIT_PREFIX_BYTES as u64
            + first.payload.len() as u64
            + 2 * (COMMIT_PREFIX_BYTES as u64 + second.payload.len() as u64);
        assert!(replay.work.peak_allocation_bytes >= retained_peak);
        assert!(({ let __authority_args = (authority_id, Sequence::GENESIS, ReplayLimit {
                    records: 8,
                    payload_bytes: 64
                }, WorkBudget {
                    peak_allocation_bytes: retained_peak - 1,
                    ..WorkBudget::UNBOUNDED
                }, cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.3, __authority_args.4); let __authority_result =
            AsyncAuthorityStore::replay(store, __authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work).await.map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })
            .is_err(),
            "replay accepted a budget below its simultaneously retained buffers"
        );
        let resumed =({ let __authority_args = (authority_id, first.sequence, ReplayLimit {
                records: 1,
                payload_bytes: 16,
            }, WorkBudget::UNBOUNDED, cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.3, __authority_args.4); let __authority_result =  AsyncAuthorityStore::replay(store, __authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work).await.map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })
        .map_err(js_error)?;
        assert_eq!(resumed.value, vec![second]);
        let found =({ let __authority_args = (authority_id, first.operation_id, WorkBudget::UNBOUNDED, cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result =  AsyncAuthorityStore::find_operation(store, __authority_args.0, __authority_args.1, &mut __authority_work).await.map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })
        .map_err(js_error)?;
        assert_eq!(found.value, Some(first));
        Ok(())
    }

    async fn verify_fencing(
        store: &IndexedDbAuthorityStore,
        authority_id: AuthorityId,
        cancellation: &CancellationToken,
        second: DurableCommit,
    ) -> Result<(), JsValue> {
        let fenced =({ let __authority_args = (authority_id, Head {
                epoch: Epoch::GENESIS,
                sequence: second.sequence,
                digest: second.digest,
            }, WorkBudget::UNBOUNDED, cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result =  AsyncAuthorityStore::fence(store, __authority_args.0, __authority_args.1, &mut __authority_work).await; __authority_work.receipt(__authority_result) })
        .map_err(js_error)?;
        let FenceOutcome::Advanced(fenced_head) = fenced.value else {
            return Err(js_error("fresh IndexedDB fence conflicted"));
        };
        assert_eq!(fenced_head.epoch, Epoch::new(2).map_err(js_error)?);
        assert_eq!(fenced_head.sequence, second.sequence);
        assert_eq!(fenced_head.digest, second.digest);
        let stale =({ let __authority_args = (authority_id, Epoch::GENESIS, fenced_head, ProposedCommit {
                operation_id: OperationId::from_bytes([13; 16]),
                fingerprint: Digest::from_bytes([14; 32]),
                payload: Bytes::from_static(b"stale"),
            }, WorkBudget::UNBOUNDED, cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.4, __authority_args.5); let __authority_result =  AsyncAuthorityStore::compare_and_append(store, crate::GuardedAppend { authority_id: __authority_args.0, epoch: __authority_args.1, expected: __authority_args.2, commit: __authority_args.3, permit: crate::PublicationPermit::Unrestricted }, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })
        .map_err(js_error)?;
        assert_eq!(
            stale.value,
            AppendOutcome::Fenced {
                actual_epoch: fenced_head.epoch
            }
        );
        Ok(())
    }

    fn js_error(error: impl std::fmt::Display) -> JsValue {
        JsValue::from_str(&error.to_string())
    }
}
