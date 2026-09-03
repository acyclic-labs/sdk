//! `IndexedDB` authority and immutable-object persistence.

use crate::authority_codec::{
    COMMIT_PREFIX_BYTES, HEAD_BYTES, OPERATION_BYTES, OperationRecord, authority_key, commit_key,
    decode_commit_owned, decode_head, decode_operation, encode_commit, encode_head,
    encode_operation, operation_key,
};
use acyclic_fs::storage::FenceOutcome;
use acyclic_fs::{
    AppendOutcome, AsyncAuthorityStore, AsyncObjectStore, AuthorityFailure, AuthorityId,
    AuthorityReceipt, AuthorityResult, AuthorityStoreError, CancellationToken,
    CreateAuthorityOutcome, DurableCommit, Epoch, Head, OBJECT_DIGEST_ENVELOPE_BYTES,
    ObjectFailure, ObjectId, ObjectRead, ObjectReadRetention, ObjectReceipt, ObjectResult,
    ObjectStoreError, OperationId, ProposedCommit, ReplayLimit, Sequence, WorkBudget, WorkCounters,
    authority_commit_digest, object_digest,
};
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

const DATABASE_VERSION: u32 = 2;
const OBJECTS: &str = "objects";
const OBJECT_METADATA: &str = "object_metadata";
const AUTHORITY_HEADS: &str = "authority_heads";
const AUTHORITY_COMMITS: &str = "authority_commits";
const AUTHORITY_OPERATIONS: &str = "authority_operations";
const OBJECT_KEY_BYTES: u64 = 66;

#[derive(Clone, Copy)]
struct ObjectBatchState {
    work: WorkCounters,
    retained: u64,
    budget: WorkBudget,
}

fn strict_transaction_options() -> TransactionOptions {
    let mut options = TransactionOptions::new();
    options.set_durability(TransactionDurability::Strict);
    options
}

/// Browser database initialization failures.
#[derive(Debug, Error)]
pub enum IndexedDbOpenError {
    /// Database and per-object byte bounds must be non-empty.
    #[error("IndexedDB name and maximum object bytes must be non-empty")]
    InvalidOptions,
    /// `IndexedDB` was unavailable, blocked, or rejected the schema upgrade.
    #[error("IndexedDB open failed: {0}")]
    Open(String),
}

/// Transactional `IndexedDB` immutable-object backend.
///
/// Canonical bytes and their exact length live in separate object stores but
/// are always created in one transaction. Reads admit the metadata-reported
/// allocation before requesting the body, preventing an oversized body from
/// being materialized before the caller's hard bounds are checked.
pub struct IndexedDbObjectStore {
    database: Database,
    maximum_object_bytes: u64,
}

impl IndexedDbObjectStore {
    /// Opens or upgrades one browser-local filesystem database.
    ///
    /// # Errors
    ///
    /// Rejects empty options and any blocked or failed `IndexedDB` upgrade.
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
        let mut key = String::with_capacity(usize::try_from(OBJECT_KEY_BYTES).unwrap_or(66));
        key.push(char::from(b'0' + object_id.kind.canonical_tag()));
        key.push(':');
        for byte in object_id.digest.as_bytes() {
            key.push(char::from(HEX[usize::from(byte >> 4)]));
            key.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
        key
    }

    fn initial_work() -> WorkCounters {
        WorkCounters {
            allocation_operations: 1,
            peak_allocation_bytes: OBJECT_KEY_BYTES,
            ..WorkCounters::default()
        }
    }

    fn failure(error: ObjectStoreError, work: WorkCounters) -> ObjectFailure {
        ObjectFailure::new(error, work)
    }

    fn backend(error: impl std::fmt::Display, work: WorkCounters) -> ObjectFailure {
        Self::failure(ObjectStoreError::Rejected(error.to_string()), work)
    }

    fn doubled(length: u64, work: WorkCounters) -> Result<u64, ObjectFailure> {
        length
            .checked_mul(2)
            .ok_or_else(|| Self::failure(acyclic_fs::WorkError::Overflow.into(), work))
    }

    fn peak_with_key(length: u64, work: WorkCounters) -> Result<u64, ObjectFailure> {
        OBJECT_KEY_BYTES
            .checked_add(length)
            .ok_or_else(|| Self::failure(acyclic_fs::WorkError::Overflow.into(), work))
    }

    fn create_blob(bytes: &[u8], work: WorkCounters) -> Result<Blob, ObjectFailure> {
        let array = Uint8Array::from(bytes);
        let parts = Array::new();
        parts.push(&array);
        Blob::new_with_u8_array_sequence(&parts)
            .map_err(|error| Self::backend(js_message(&error), work))
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
                    .map_err(|_| Self::failure(ObjectStoreError::Corrupt, work))
            })
            .transpose()
    }

    async fn materialize_blob(
        blob: &Blob,
        expected_length: u64,
        cancellation: &CancellationToken,
        work: WorkCounters,
    ) -> Result<Bytes, ObjectFailure> {
        let expected_length_u32 = u32::try_from(expected_length)
            .map_err(|_| Self::failure(ObjectStoreError::Corrupt, work))?;
        if (blob.size() - f64::from(expected_length_u32)).abs() > 0.5 {
            return Err(Self::failure(ObjectStoreError::Corrupt, work));
        }
        let buffer = await_cancellable(JsFuture::from(blob.array_buffer()), cancellation)
            .await
            .map_err(|error| match error {
                CancellableError::Cancelled => Self::failure(ObjectStoreError::Cancelled, work),
                CancellableError::Operation(error) => Self::backend(js_message(&error), work),
            })?;
        let array = Uint8Array::new(&buffer);
        if u64::from(array.length()) != expected_length {
            return Err(Self::failure(ObjectStoreError::Corrupt, work));
        }
        Ok(Bytes::from(array.to_vec()))
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
                    CancellableError::Cancelled => Self::failure(ObjectStoreError::Cancelled, work),
                    CancellableError::Operation(error) => Self::backend(error, work),
                })?;
        value
            .map(|value| {
                value
                    .parse::<u64>()
                    .map_err(|_| Self::failure(ObjectStoreError::Corrupt, work))
            })
            .transpose()
    }

    async fn read_batch_item(
        &self,
        objects: &indexed_db_futures::object_store::ObjectStore<'_>,
        metadata: &indexed_db_futures::object_store::ObjectStore<'_>,
        request: acyclic_fs::ObjectReadRequest,
        state: &mut ObjectBatchState,
        cancellation: &CancellationToken,
    ) -> Result<ObjectRead, ObjectFailure> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::new(ObjectStoreError::Cancelled, state.work))?;
        let key = Self::key(request.object_id);
        state.work = state
            .work
            .checked_add(WorkCounters {
                object_probes: 1,
                backend_read_operations: 1,
                allocation_operations: 1,
                peak_allocation_bytes: state.retained.saturating_add(OBJECT_KEY_BYTES),
                ..WorkCounters::default()
            })
            .map_err(|error| ObjectFailure::new(error.into(), state.work))?;
        state.work.peak_allocation_bytes = state
            .work
            .peak_allocation_bytes
            .max(state.retained.saturating_add(OBJECT_KEY_BYTES));
        state
            .work
            .verify(state.budget)
            .map_err(|error| ObjectFailure::new(error.into(), state.work))?;
        let length = Self::metadata_length(metadata, &key, cancellation, state.work)
            .await?
            .ok_or_else(|| Self::failure(ObjectStoreError::Missing, state.work))?;
        if length > request.maximum_bytes || length > self.maximum_object_bytes {
            return Err(Self::failure(
                ObjectStoreError::TooLarge {
                    observed: length,
                    maximum: request.maximum_bytes.min(self.maximum_object_bytes),
                },
                state.work,
            ));
        }
        let copied = Self::doubled(length, state.work)?;
        let transient = OBJECT_KEY_BYTES.saturating_add(copied);
        state.work = state
            .work
            .checked_add(WorkCounters {
                backend_read_operations: 1,
                object_bytes_read: length,
                bytes_hashed: length.saturating_add(OBJECT_DIGEST_ENVELOPE_BYTES),
                bytes_copied: copied,
                allocation_operations: 2 * u64::from(length != 0),
                peak_allocation_bytes: state.retained.saturating_add(transient),
                ..WorkCounters::default()
            })
            .map_err(|error| Self::failure(error.into(), state.work))?;
        state.work.peak_allocation_bytes = state
            .work
            .peak_allocation_bytes
            .max(state.retained.saturating_add(transient));
        state
            .work
            .verify(state.budget)
            .map_err(|error| Self::failure(error.into(), state.work))?;
        let stored_blob = Self::get_blob(objects, &key, cancellation, state.work)
            .await?
            .ok_or_else(|| Self::failure(ObjectStoreError::Corrupt, state.work))?;
        let bytes = Self::materialize_blob(&stored_blob, length, cancellation, state.work).await?;
        if object_digest(request.object_id.kind, &bytes) != request.object_id.digest {
            return Err(Self::failure(ObjectStoreError::Corrupt, state.work));
        }
        state.retained = state
            .retained
            .checked_add(length)
            .ok_or_else(|| Self::failure(acyclic_fs::WorkError::Overflow.into(), state.work))?;
        state.work.peak_allocation_bytes = state.work.peak_allocation_bytes.max(state.retained);
        Ok(ObjectRead {
            bytes,
            retention: ObjectReadRetention::Owned {
                logical_bytes: length,
            },
        })
    }

    async fn put_existing(
        transaction: Transaction<'_>,
        key: &str,
        bytes: &Bytes,
        length: u64,
        budget: WorkBudget,
        work: WorkCounters,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        let copied = Self::doubled(length, work)?;
        let peak_allocation_bytes = Self::peak_with_key(copied, work)?;
        let prospective = work
            .checked_add(WorkCounters {
                backend_read_operations: 1,
                object_bytes_read: length,
                bytes_copied: copied,
                allocation_operations: 2 * u64::from(length != 0),
                peak_allocation_bytes,
                ..WorkCounters::default()
            })
            .map_err(|error| Self::failure(error.into(), work))?;
        prospective
            .verify(budget)
            .map_err(|error| Self::failure(error.into(), work))?;
        let existing_blob = {
            let objects = transaction
                .object_store(OBJECTS)
                .map_err(|error| Self::backend(error, work))?;
            Self::get_blob(&objects, key, cancellation, work).await?
        }
        .ok_or_else(|| Self::failure(ObjectStoreError::Corrupt, work))?;
        let existing =
            Self::materialize_blob(&existing_blob, length, cancellation, prospective).await?;
        if existing != *bytes {
            return Err(Self::failure(ObjectStoreError::Corrupt, prospective));
        }
        Ok(ObjectReceipt {
            value: (),
            work: prospective,
        })
    }

    async fn put_new(
        transaction: Transaction<'_>,
        key: &str,
        bytes: &Bytes,
        length: u64,
        budget: WorkBudget,
        work: WorkCounters,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        let copied = Self::doubled(length, work)?;
        let peak_allocation_bytes = Self::peak_with_key(copied, work)?;
        let prospective = work
            .checked_add(WorkCounters {
                backend_write_operations: 2,
                durability_operations: 1,
                object_bytes_written: length,
                bytes_copied: copied,
                allocation_operations: 2 * u64::from(length != 0),
                peak_allocation_bytes,
                ..WorkCounters::default()
            })
            .map_err(|error| Self::failure(error.into(), work))?;
        prospective
            .verify(budget)
            .map_err(|error| Self::failure(error.into(), work))?;
        cancellation
            .check()
            .map_err(|_| Self::failure(ObjectStoreError::Cancelled, work))?;
        let blob = Self::create_blob(bytes, prospective)?;
        {
            let objects = transaction
                .object_store(OBJECTS)
                .map_err(|error| Self::backend(error, work))?;
            let request = objects
                .add(JsValue::from(blob))
                .with_key(key)
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
                .with_key(key)
                .primitive()
                .map_err(|error| Self::backend(error, work))?;
            await_cancellable(request, cancellation)
                .await
                .map_err(|error| cancellable_failure(error, work))?;
        }
        transaction
            .commit()
            .await
            .map_err(|error| Self::backend(error, prospective))?;
        Ok(ObjectReceipt {
            value: (),
            work: prospective,
        })
    }
}

/// Transactional `IndexedDB` authority backend.
///
/// One read-write transaction owns head comparison, writer fencing,
/// idempotency resolution, commit insertion, and head publication. Commit
/// payloads are stored as `Blob` handles so replay can inspect exact sizes
/// before allocating their bodies.
pub struct IndexedDbAuthorityStore {
    database: Database,
    maximum_payload_bytes: u64,
}

struct PreparedAppend {
    durable: DurableCommit,
    next_head: Head,
    encoded_commit: Vec<u8>,
    encoded_operation: Vec<u8>,
    encoded_head: Vec<u8>,
    work: WorkCounters,
}

struct ReplayBlob {
    sequence: Sequence,
    blob: Blob,
    encoded_length: u64,
    payload_length: u64,
    is_predecessor: bool,
}

struct ReplayFetch {
    authority_id: AuthorityId,
    after: Sequence,
    requested: u64,
    maximum_payload_bytes: u64,
    budget: WorkBudget,
    work: WorkCounters,
}

struct IndexedCommitRequest {
    authority_id: AuthorityId,
    operation_id: OperationId,
    operation: OperationRecord,
    budget: WorkBudget,
    work: WorkCounters,
}

impl IndexedDbAuthorityStore {
    /// Opens one browser-local authority database.
    ///
    /// # Errors
    ///
    /// Rejects empty options, payload bounds that cannot be represented by
    /// browser buffers, and blocked or failed schema upgrades.
    pub async fn open(
        database_name: &str,
        maximum_payload_bytes: u64,
    ) -> Result<Self, IndexedDbOpenError> {
        let maximum_encoded = maximum_payload_bytes
            .checked_add(u64::try_from(COMMIT_PREFIX_BYTES).unwrap_or(u64::MAX));
        if database_name.is_empty()
            || maximum_payload_bytes == 0
            || maximum_encoded.is_none_or(|value| value > u64::from(u32::MAX))
        {
            return Err(IndexedDbOpenError::InvalidOptions);
        }
        Ok(Self {
            database: open_database(database_name).await?,
            maximum_payload_bytes,
        })
    }

    fn failure(error: AuthorityStoreError, work: WorkCounters) -> AuthorityFailure {
        AuthorityFailure::new(error, work)
    }

    fn backend(error: impl std::fmt::Display, work: WorkCounters) -> AuthorityFailure {
        Self::failure(AuthorityStoreError::Rejected(error.to_string()), work)
    }

    fn corrupt(error: impl std::fmt::Display, work: WorkCounters) -> AuthorityFailure {
        Self::failure(AuthorityStoreError::Corrupt(error.to_string()), work)
    }

    fn admit(work: WorkCounters, budget: WorkBudget) -> Result<(), AuthorityFailure> {
        work.verify(budget)
            .map_err(|error| Self::failure(error.into(), work))
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
        if !size.is_finite() || size < 0.0 || size.fract().abs() > f64::EPSILON {
            return Err(Self::corrupt("commit Blob has a non-integral size", work));
        }
        let size_text = size.to_string();
        size_text
            .parse::<u64>()
            .map_err(|_| Self::corrupt("commit Blob size is not representable", work))
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
                CancellableError::Cancelled => Self::failure(AuthorityStoreError::Cancelled, work),
                CancellableError::Operation(error) => Self::backend(js_message(&error), work),
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

    fn create_blob(bytes: &[u8], work: WorkCounters) -> Result<Blob, AuthorityFailure> {
        let array = Uint8Array::from(bytes);
        let parts = Array::new();
        parts.push(&array);
        Blob::new_with_u8_array_sequence(&parts)
            .map_err(|error| Self::backend(js_message(&error), work))
    }

    async fn add_fixed(
        store: &indexed_db_futures::object_store::ObjectStore<'_>,
        key: &str,
        bytes: &[u8],
        cancellation: &CancellationToken,
        work: WorkCounters,
    ) -> Result<(), AuthorityFailure> {
        let array = Uint8Array::from(bytes);
        let request = store
            .add(JsValue::from(array))
            .with_key(key)
            .primitive()
            .map_err(|error| Self::backend(error, work))?;
        await_cancellable(request, cancellation)
            .await
            .map_err(|error| authority_cancellable_failure(error, work))?;
        Ok(())
    }

    async fn put_fixed(
        store: &indexed_db_futures::object_store::ObjectStore<'_>,
        key: &str,
        bytes: &[u8],
        cancellation: &CancellationToken,
        work: WorkCounters,
    ) -> Result<(), AuthorityFailure> {
        let array = Uint8Array::from(bytes);
        let request = store
            .put(JsValue::from(array))
            .with_key(key)
            .primitive()
            .map_err(|error| Self::backend(error, work))?;
        await_cancellable(request, cancellation)
            .await
            .map_err(|error| authority_cancellable_failure(error, work))?;
        Ok(())
    }

    async fn add_blob(
        store: &indexed_db_futures::object_store::ObjectStore<'_>,
        key: &str,
        blob: Blob,
        cancellation: &CancellationToken,
        work: WorkCounters,
    ) -> Result<(), AuthorityFailure> {
        let request = store
            .add(JsValue::from(blob))
            .with_key(key)
            .primitive()
            .map_err(|error| Self::backend(error, work))?;
        await_cancellable(request, cancellation)
            .await
            .map_err(|error| authority_cancellable_failure(error, work))?;
        Ok(())
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
        let operation_admission = work
            .checked_add(authority_fixed_read_work(OPERATION_BYTES))
            .map_err(|error| Self::failure(error.into(), work))?;
        Self::admit(operation_admission, budget)?;
        let operation_record_key = operation_key(authority_id, commit.operation_id);
        let encoded = {
            let operations = transaction
                .object_store(AUTHORITY_OPERATIONS)
                .map_err(|error| Self::backend(error, operation_admission))?;
            Self::get_fixed(
                &operations,
                &operation_record_key,
                OPERATION_BYTES,
                cancellation,
                operation_admission,
            )
            .await?
        };
        let Some(encoded) = encoded else {
            let actual_work = work
                .checked_add(authority_backend_read_work())
                .map_err(|error| Self::failure(error.into(), work))?;
            return Ok((None, actual_work));
        };
        let work = operation_admission;
        let operation = decode_operation(&encoded).map_err(|error| Self::corrupt(error, work))?;
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
                IndexedCommitRequest {
                    authority_id,
                    operation_id: commit.operation_id,
                    operation,
                    budget,
                    work,
                },
                cancellation,
            )
            .await?;
        Ok((Some(AppendOutcome::AlreadyCommitted(durable)), work))
    }

    async fn load_indexed_commit(
        &self,
        transaction: &Transaction<'_>,
        request: IndexedCommitRequest,
        cancellation: &CancellationToken,
    ) -> Result<(DurableCommit, WorkCounters), AuthorityFailure> {
        let IndexedCommitRequest {
            authority_id,
            operation_id,
            operation,
            budget,
            mut work,
        } = request;
        work = work
            .checked_add(WorkCounters {
                backend_read_operations: 1,
                ..WorkCounters::default()
            })
            .map_err(|error| Self::failure(error.into(), work))?;
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
        let materialize_work = authority_commit_read_work(encoded_length, payload_length)
            .map_err(|error| Self::failure(error.into(), work))?;
        work = work
            .checked_add(materialize_work)
            .map_err(|error| Self::failure(error.into(), work))?;
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
        payload_bytes: u64,
        budget: WorkBudget,
        work: WorkCounters,
    ) -> Result<PreparedAppend, AuthorityFailure> {
        let sequence = actual.sequence.checked_next().map_err(|error| {
            Self::failure(
                AuthorityStoreError::Rejected(format!(
                    "authority sequence cannot advance: {error}"
                )),
                work,
            )
        })?;
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
            epoch,
            sequence,
            operation_id: commit.operation_id,
            fingerprint: commit.fingerprint,
            previous_digest: actual.digest,
            digest,
            payload: commit.payload,
        };
        let next_head = Head {
            epoch,
            sequence,
            digest,
        };
        let encoded_commit = encode_commit(&durable).map_err(|error| Self::backend(error, work))?;
        let encoded_commit_bytes = u64::try_from(encoded_commit.len())
            .map_err(|_| Self::failure(acyclic_fs::WorkError::Overflow.into(), work))?;
        let append_work = authority_append_work(payload_bytes, encoded_commit_bytes)
            .map_err(|error| Self::failure(error.into(), work))?;
        let write_work = work
            .checked_add(append_work)
            .map_err(|error| Self::failure(error.into(), work))?;
        Self::admit(write_work, budget)?;
        Ok(PreparedAppend {
            encoded_operation: encode_operation(OperationRecord {
                sequence,
                fingerprint: durable.fingerprint,
            }),
            encoded_head: encode_head(next_head),
            encoded_commit,
            durable,
            next_head,
            work: write_work,
        })
    }

    async fn publish_prepared(
        transaction: Transaction<'_>,
        authority_id: AuthorityId,
        prepared: PreparedAppend,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<AppendOutcome> {
        let commit_blob = Self::create_blob(&prepared.encoded_commit, prepared.work)?;
        {
            let commits = transaction
                .object_store(AUTHORITY_COMMITS)
                .map_err(|error| Self::backend(error, prepared.work))?;
            Self::add_blob(
                &commits,
                &commit_key(authority_id, prepared.next_head.sequence),
                commit_blob,
                cancellation,
                prepared.work,
            )
            .await?;
        }
        {
            let operations = transaction
                .object_store(AUTHORITY_OPERATIONS)
                .map_err(|error| Self::backend(error, prepared.work))?;
            Self::add_fixed(
                &operations,
                &operation_key(authority_id, prepared.durable.operation_id),
                &prepared.encoded_operation,
                cancellation,
                prepared.work,
            )
            .await?;
        }
        {
            let heads = transaction
                .object_store(AUTHORITY_HEADS)
                .map_err(|error| Self::backend(error, prepared.work))?;
            Self::put_fixed(
                &heads,
                &authority_key(authority_id),
                &prepared.encoded_head,
                cancellation,
                prepared.work,
            )
            .await?;
        }
        transaction
            .commit()
            .await
            .map_err(|error| Self::backend(error, prepared.work))?;
        Ok(AuthorityReceipt {
            value: AppendOutcome::Committed(prepared.durable),
            work: prepared.work,
        })
    }

    async fn fetch_replay_blobs(
        &self,
        fetch: ReplayFetch,
        cancellation: &CancellationToken,
    ) -> Result<(Vec<ReplayBlob>, WorkCounters), AuthorityFailure> {
        let ReplayFetch {
            authority_id,
            after,
            requested,
            maximum_payload_bytes,
            budget,
            mut work,
        } = fetch;
        let start = after.checked_next().map_err(|error| {
            Self::failure(
                AuthorityStoreError::Rejected(format!("replay cursor cannot advance: {error}")),
                work,
            )
        })?;
        let mut sequences = Vec::new();
        if after != Sequence::GENESIS {
            sequences.push(after);
        }
        for offset in 0..requested {
            let value = start.get().checked_add(offset).ok_or_else(|| {
                Self::failure(
                    AuthorityStoreError::Rejected("replay sequence range overflowed".to_owned()),
                    work,
                )
            })?;
            sequences.push(Sequence::new(value));
        }
        let transaction = self
            .database
            .transaction(AUTHORITY_COMMITS)
            .build()
            .map_err(|error| Self::backend(error, work))?;
        let mut blobs = Vec::new();
        let mut returned_payload_bytes = 0_u64;
        for sequence in sequences {
            let prospective = work
                .checked_add(WorkCounters {
                    backend_read_operations: 1,
                    ..WorkCounters::default()
                })
                .map_err(|error| Self::failure(error.into(), work))?;
            Self::admit(prospective, budget)?;
            let blob = {
                let commits = transaction
                    .object_store(AUTHORITY_COMMITS)
                    .map_err(|error| Self::backend(error, work))?;
                Self::get_blob(
                    &commits,
                    &commit_key(authority_id, sequence),
                    cancellation,
                    prospective,
                )
                .await?
                .ok_or_else(|| {
                    Self::corrupt("contiguous authority commit is missing", prospective)
                })?
            };
            work = prospective;
            let encoded_length = Self::blob_length(&blob, work)?;
            let payload_length =
                authority_payload_length(encoded_length, self.maximum_payload_bytes, work)?;
            let is_predecessor = sequence == after && after != Sequence::GENESIS;
            if !is_predecessor {
                let next_payload = returned_payload_bytes
                    .checked_add(payload_length)
                    .ok_or_else(|| Self::failure(acyclic_fs::WorkError::Overflow.into(), work))?;
                if next_payload > maximum_payload_bytes {
                    if returned_payload_bytes == 0 {
                        return Err(Self::failure(
                            AuthorityStoreError::ReplayRecordTooLarge {
                                observed: payload_length,
                                maximum: maximum_payload_bytes,
                            },
                            work,
                        ));
                    }
                    break;
                }
                returned_payload_bytes = next_payload;
            }
            blobs.push(ReplayBlob {
                sequence,
                blob,
                encoded_length,
                payload_length,
                is_predecessor,
            });
        }
        drop(transaction);
        Ok((blobs, work))
    }

    async fn decode_replay_blobs(
        &self,
        authority_id: AuthorityId,
        blobs: Vec<ReplayBlob>,
        head: Head,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        mut work: WorkCounters,
    ) -> AuthorityResult<Vec<DurableCommit>> {
        let mut previous_digest = acyclic_fs::Digest::ZERO;
        let mut output = Vec::new();
        for replay in blobs {
            let read_work =
                authority_commit_read_work(replay.encoded_length, replay.payload_length)
                    .map_err(|error| Self::failure(error.into(), work))?;
            let prospective = work
                .checked_add(read_work)
                .map_err(|error| Self::failure(error.into(), work))?;
            Self::admit(prospective, budget)?;
            let encoded = Self::materialize_blob(
                &replay.blob,
                replay.encoded_length,
                cancellation,
                prospective,
            )
            .await?;
            let commit = decode_commit_owned(authority_id, encoded, self.maximum_payload_bytes)
                .map_err(|error| Self::corrupt(error, prospective))?;
            if commit.sequence != replay.sequence {
                return Err(Self::corrupt(
                    "authority commit key does not match its sequence",
                    prospective,
                ));
            }
            if replay.is_predecessor {
                previous_digest = commit.digest;
            } else {
                if commit.previous_digest != previous_digest {
                    return Err(Self::corrupt(
                        "authority replay hash chain is discontinuous",
                        prospective,
                    ));
                }
                previous_digest = commit.digest;
                output.push(commit);
            }
            work = prospective;
        }
        if output
            .last()
            .is_some_and(|commit| commit.sequence == head.sequence)
            && previous_digest != head.digest
        {
            return Err(Self::corrupt(
                "authority head digest does not match its terminal commit",
                work,
            ));
        }
        Ok(AuthorityReceipt {
            value: output,
            work,
        })
    }

    async fn replay_page(
        &self,
        authority_id: AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Vec<DurableCommit>> {
        const MAXIMUM_REPLAY_RECORDS: u32 = 4_096;
        if limit.records == 0 || limit.payload_bytes == 0 {
            return Err(AuthorityFailure::before_work(
                AuthorityStoreError::InvalidReplayLimit,
            ));
        }
        if limit.records > MAXIMUM_REPLAY_RECORDS {
            return Err(AuthorityFailure::before_work(
                AuthorityStoreError::Rejected(format!(
                    "browser replay page has {} records; maximum is {MAXIMUM_REPLAY_RECORDS}",
                    limit.records
                )),
            ));
        }
        let head_receipt =
            AsyncAuthorityStore::head(self, authority_id, budget, cancellation).await?;
        let head = head_receipt.value;
        let work = head_receipt.work;
        if after > head.sequence {
            return Err(Self::failure(
                AuthorityStoreError::Rejected(
                    "replay cursor is ahead of authority head".to_owned(),
                ),
                work,
            ));
        }
        if after == head.sequence {
            return Ok(AuthorityReceipt {
                value: Vec::new(),
                work,
            });
        }
        let available = head
            .sequence
            .get()
            .checked_sub(after.get())
            .ok_or_else(|| Self::corrupt("authority head precedes replay cursor", work))?;
        let requested = u64::from(limit.records).min(available);
        let (blobs, work) = self
            .fetch_replay_blobs(
                ReplayFetch {
                    authority_id,
                    after,
                    requested,
                    maximum_payload_bytes: limit.payload_bytes,
                    budget,
                    work,
                },
                cancellation,
            )
            .await?;
        self.decode_replay_blobs(authority_id, blobs, head, budget, cancellation, work)
            .await
    }
}

async fn open_database(database_name: &str) -> Result<Database, IndexedDbOpenError> {
    Database::open(database_name)
        .with_version(DATABASE_VERSION)
        .with_on_upgrade_needed(|event, database| {
            if event.old_version() > 0.5 && event.old_version() < 1.5 {
                database.delete_object_store(OBJECTS)?;
                database.delete_object_store(OBJECT_METADATA)?;
            }
            if event.old_version() < f64::from(DATABASE_VERSION) {
                database.create_object_store(OBJECTS).build()?;
                database.create_object_store(OBJECT_METADATA).build()?;
                database.create_object_store(AUTHORITY_HEADS).build()?;
                database.create_object_store(AUTHORITY_COMMITS).build()?;
                database.create_object_store(AUTHORITY_OPERATIONS).build()?;
            }
            Ok(())
        })
        .await
        .map_err(|error| IndexedDbOpenError::Open(error.to_string()))
}

impl AsyncAuthorityStore for IndexedDbAuthorityStore {
    async fn create_authority(
        &self,
        authority_id: AuthorityId,
        genesis_epoch: Epoch,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<CreateAuthorityOutcome> {
        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        let read_admission = authority_fixed_read_work(HEAD_BYTES);
        Self::admit(read_admission, budget)?;
        let transaction = self
            .database
            .transaction(AUTHORITY_HEADS)
            .with_mode(TransactionMode::Readwrite)
            .with_options(strict_transaction_options())
            .build()
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
            return Ok(AuthorityReceipt {
                value: CreateAuthorityOutcome::Existing(head),
                work: read_admission,
            });
        }
        let read_work = authority_backend_read_work();
        let head = Head::genesis(genesis_epoch);
        let encoded = encode_head(head);
        let write_work = read_work
            .checked_add(authority_fixed_write_work(HEAD_BYTES, 1))
            .map_err(|error| Self::failure(error.into(), read_work))?;
        Self::admit(write_work, budget)?;
        {
            let heads = transaction
                .object_store(AUTHORITY_HEADS)
                .map_err(|error| Self::backend(error, read_work))?;
            Self::add_fixed(&heads, &key, &encoded, cancellation, write_work).await?;
        }
        transaction
            .commit()
            .await
            .map_err(|error| Self::backend(error, write_work))?;
        Ok(AuthorityReceipt {
            value: CreateAuthorityOutcome::Created(head),
            work: write_work,
        })
    }

    async fn head(
        &self,
        authority_id: AuthorityId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Head> {
        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        let work = authority_fixed_read_work(HEAD_BYTES);
        Self::admit(work, budget)?;
        let transaction = self
            .database
            .transaction(AUTHORITY_HEADS)
            .build()
            .map_err(|error| Self::backend(error, work))?;
        let key = authority_key(authority_id);
        let encoded = {
            let heads = transaction
                .object_store(AUTHORITY_HEADS)
                .map_err(|error| Self::backend(error, work))?;
            Self::get_fixed(&heads, &key, HEAD_BYTES, cancellation, work)
                .await?
                .ok_or_else(|| Self::failure(AuthorityStoreError::Missing, work))?
        };
        let value = decode_head(&encoded).map_err(|error| Self::corrupt(error, work))?;
        Ok(AuthorityReceipt { value, work })
    }

    async fn compare_and_append(
        &self,
        authority_id: AuthorityId,
        epoch: Epoch,
        expected: Head,
        commit: ProposedCommit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<AppendOutcome> {
        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        let payload_bytes = u64::try_from(commit.payload.len()).unwrap_or(u64::MAX);
        if payload_bytes > self.maximum_payload_bytes {
            return Err(AuthorityFailure::before_work(
                AuthorityStoreError::PayloadTooLarge {
                    observed: payload_bytes,
                    maximum: self.maximum_payload_bytes,
                },
            ));
        }
        let work = authority_fixed_read_work(HEAD_BYTES);
        Self::admit(work, budget)?;
        let transaction = self
            .database
            .transaction([AUTHORITY_HEADS, AUTHORITY_COMMITS, AUTHORITY_OPERATIONS])
            .with_mode(TransactionMode::Readwrite)
            .with_options(strict_transaction_options())
            .build()
            .map_err(|error| Self::backend(error, work))?;
        let actual = {
            let heads = transaction
                .object_store(AUTHORITY_HEADS)
                .map_err(|error| Self::backend(error, work))?;
            let encoded = Self::get_fixed(
                &heads,
                &authority_key(authority_id),
                HEAD_BYTES,
                cancellation,
                work,
            )
            .await?
            .ok_or_else(|| Self::failure(AuthorityStoreError::Missing, work))?;
            decode_head(&encoded).map_err(|error| Self::corrupt(error, work))?
        };
        if epoch != actual.epoch {
            return Ok(AuthorityReceipt {
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
            return Ok(AuthorityReceipt { value, work });
        }
        if expected != actual {
            return Ok(AuthorityReceipt {
                value: AppendOutcome::Conflict { actual },
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
    }

    async fn replay(
        &self,
        authority_id: AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Vec<DurableCommit>> {
        self.replay_page(authority_id, after, limit, budget, cancellation)
            .await
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
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        let read_work = authority_fixed_read_work(HEAD_BYTES);
        Self::admit(read_work, budget)?;
        let transaction = self
            .database
            .transaction(AUTHORITY_HEADS)
            .with_mode(TransactionMode::Readwrite)
            .with_options(strict_transaction_options())
            .build()
            .map_err(|error| Self::backend(error, read_work))?;
        let key = authority_key(authority_id);
        let actual = {
            let heads = transaction
                .object_store(AUTHORITY_HEADS)
                .map_err(|error| Self::backend(error, read_work))?;
            let encoded = Self::get_fixed(&heads, &key, HEAD_BYTES, cancellation, read_work)
                .await?
                .ok_or_else(|| Self::failure(AuthorityStoreError::Missing, read_work))?;
            decode_head(&encoded).map_err(|error| Self::corrupt(error, read_work))?
        };
        if actual != expected {
            return Ok(AuthorityReceipt {
                value: FenceOutcome::Conflict { actual },
                work: read_work,
            });
        }
        let next_epoch = actual
            .epoch
            .get()
            .checked_add(1)
            .ok_or_else(|| Self::failure(AuthorityStoreError::EpochExhausted, read_work))?;
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
            .map_err(|error| Self::failure(error.into(), read_work))?;
        let write_work = read_work
            .checked_add(fence_delta)
            .map_err(|error| Self::failure(error.into(), read_work))?;
        Self::admit(write_work, budget)?;
        {
            let heads = transaction
                .object_store(AUTHORITY_HEADS)
                .map_err(|error| Self::backend(error, read_work))?;
            Self::put_fixed(&heads, &key, &encoded, cancellation, write_work).await?;
        }
        transaction
            .commit()
            .await
            .map_err(|error| Self::backend(error, write_work))?;
        Ok(AuthorityReceipt {
            value: FenceOutcome::Advanced(next),
            work: write_work,
        })
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
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        let head_work = authority_fixed_read_work(HEAD_BYTES);
        Self::admit(head_work, budget)?;
        let transaction = self
            .database
            .transaction([AUTHORITY_HEADS, AUTHORITY_COMMITS, AUTHORITY_OPERATIONS])
            .build()
            .map_err(|error| Self::backend(error, head_work))?;
        {
            let heads = transaction
                .object_store(AUTHORITY_HEADS)
                .map_err(|error| Self::backend(error, head_work))?;
            let encoded = Self::get_fixed(
                &heads,
                &authority_key(authority_id),
                HEAD_BYTES,
                cancellation,
                head_work,
            )
            .await?
            .ok_or_else(|| Self::failure(AuthorityStoreError::Missing, head_work))?;
            decode_head(&encoded).map_err(|error| Self::corrupt(error, head_work))?;
        }
        let operation_admission = head_work
            .checked_add(authority_fixed_read_work(OPERATION_BYTES))
            .map_err(|error| Self::failure(error.into(), head_work))?;
        Self::admit(operation_admission, budget)?;
        let operation = {
            let operations = transaction
                .object_store(AUTHORITY_OPERATIONS)
                .map_err(|error| Self::backend(error, operation_admission))?;
            let value = Self::get_fixed(
                &operations,
                &operation_key(authority_id, operation_id),
                OPERATION_BYTES,
                cancellation,
                operation_admission,
            )
            .await?;
            match value {
                Some(encoded) => Some(
                    decode_operation(&encoded)
                        .map_err(|error| Self::corrupt(error, operation_admission))?,
                ),
                None => None,
            }
        };
        let Some(operation) = operation else {
            let work = head_work
                .checked_add(authority_backend_read_work())
                .map_err(|error| Self::failure(error.into(), head_work))?;
            return Ok(AuthorityReceipt { value: None, work });
        };
        let (commit, work) = self
            .load_indexed_commit(
                &transaction,
                IndexedCommitRequest {
                    authority_id,
                    operation_id,
                    operation,
                    budget,
                    work: operation_admission,
                },
                cancellation,
            )
            .await?;
        Ok(AuthorityReceipt {
            value: Some(commit),
            work,
        })
    }
}

impl AsyncObjectStore for IndexedDbObjectStore {
    async fn put(
        &self,
        object_id: ObjectId,
        bytes: Bytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        let length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if length > self.maximum_object_bytes {
            return Err(ObjectFailure::before_work(ObjectStoreError::TooLarge {
                observed: length,
                maximum: self.maximum_object_bytes,
            }));
        }
        let work = Self::initial_work()
            .checked_add(WorkCounters {
                object_probes: 1,
                backend_read_operations: 1,
                bytes_hashed: length.saturating_add(OBJECT_DIGEST_ENVELOPE_BYTES),
                ..WorkCounters::default()
            })
            .map_err(|error| ObjectFailure::before_work(error.into()))?;
        work.verify(budget)
            .map_err(|error| ObjectFailure::before_work(error.into()))?;
        if object_digest(object_id.kind, &bytes) != object_id.digest {
            return Err(Self::failure(ObjectStoreError::DigestMismatch, work));
        }
        let key = Self::key(object_id);
        let transaction = self
            .database
            .transaction([OBJECTS, OBJECT_METADATA])
            .with_mode(TransactionMode::Readwrite)
            .with_options(strict_transaction_options())
            .build()
            .map_err(|error| Self::backend(error, work))?;
        let existing_length = {
            let metadata = transaction
                .object_store(OBJECT_METADATA)
                .map_err(|error| Self::backend(error, work))?;
            Self::metadata_length(&metadata, &key, cancellation, work).await?
        };
        if let Some(existing_length) = existing_length {
            if existing_length != length {
                return Err(Self::failure(ObjectStoreError::Corrupt, work));
            }
            return Self::put_existing(
                transaction,
                &key,
                &bytes,
                length,
                budget,
                work,
                cancellation,
            )
            .await;
        }
        Self::put_new(
            transaction,
            &key,
            &bytes,
            length,
            budget,
            work,
            cancellation,
        )
        .await
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
        let work = Self::initial_work()
            .checked_add(WorkCounters {
                object_probes: 1,
                backend_read_operations: 1,
                ..WorkCounters::default()
            })
            .map_err(|error| ObjectFailure::before_work(error.into()))?;
        work.verify(budget)
            .map_err(|error| ObjectFailure::before_work(error.into()))?;
        let key = Self::key(object_id);
        let transaction = self
            .database
            .transaction([OBJECTS, OBJECT_METADATA])
            .build()
            .map_err(|error| Self::backend(error, work))?;
        let length = {
            let metadata = transaction
                .object_store(OBJECT_METADATA)
                .map_err(|error| Self::backend(error, work))?;
            Self::metadata_length(&metadata, &key, cancellation, work)
                .await?
                .ok_or_else(|| Self::failure(ObjectStoreError::Missing, work))?
        };
        if length > maximum_bytes || length > self.maximum_object_bytes {
            return Err(Self::failure(
                ObjectStoreError::TooLarge {
                    observed: length,
                    maximum: maximum_bytes.min(self.maximum_object_bytes),
                },
                work,
            ));
        }
        let copied = Self::doubled(length, work)?;
        let peak_allocation_bytes = Self::peak_with_key(copied, work)?;
        let prospective = work
            .checked_add(WorkCounters {
                backend_read_operations: 1,
                object_bytes_read: length,
                bytes_hashed: length.saturating_add(OBJECT_DIGEST_ENVELOPE_BYTES),
                bytes_copied: copied,
                allocation_operations: 2 * u64::from(length != 0),
                peak_allocation_bytes,
                ..WorkCounters::default()
            })
            .map_err(|error| Self::failure(error.into(), work))?;
        prospective
            .verify(budget)
            .map_err(|error| Self::failure(error.into(), work))?;
        let stored_blob = {
            let objects = transaction
                .object_store(OBJECTS)
                .map_err(|error| Self::backend(error, work))?;
            Self::get_blob(&objects, &key, cancellation, work).await?
        }
        .ok_or_else(|| Self::failure(ObjectStoreError::Corrupt, prospective))?;
        let bytes = Self::materialize_blob(&stored_blob, length, cancellation, prospective).await?;
        if object_digest(object_id.kind, &bytes) != object_id.digest {
            return Err(Self::failure(ObjectStoreError::Corrupt, prospective));
        }
        Ok(ObjectReceipt {
            value: ObjectRead {
                bytes,
                retention: ObjectReadRetention::Owned {
                    logical_bytes: length,
                },
            },
            work: prospective,
        })
    }

    async fn read_many(
        &self,
        requests: &[acyclic_fs::ObjectReadRequest],
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
        let item_count = u64::try_from(requests.len()).unwrap_or(u64::MAX);
        let vector_bytes =
            item_count.saturating_mul(u64::try_from(size_of::<ObjectRead>()).unwrap_or(u64::MAX));
        let work = WorkCounters {
            items_examined: item_count,
            allocation_operations: 1,
            peak_allocation_bytes: vector_bytes,
            ..WorkCounters::default()
        };
        let mut admission = work;
        admission.items_returned = item_count;
        admission
            .verify(budget)
            .map_err(|error| ObjectFailure::before_work(error.into()))?;
        let transaction = self
            .database
            .transaction([OBJECTS, OBJECT_METADATA])
            .build()
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
        let mut state = ObjectBatchState {
            work,
            retained: vector_bytes,
            budget,
        };
        for request in requests {
            let value = self
                .read_batch_item(&objects, &metadata, *request, &mut state, cancellation)
                .await?;
            values.push(value);
        }
        state.work.items_returned = item_count;
        Ok(ObjectReceipt {
            value: values,
            work: state.work,
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
            Ok(receipt) => Ok(ObjectReceipt {
                value: true,
                work: receipt.work,
            }),
            Err(ObjectFailure {
                error: ObjectStoreError::Missing,
                work,
            }) => Ok(ObjectReceipt {
                value: false,
                work: *work,
            }),
            Err(failure) => Err(failure),
        }
    }
}

fn authority_fixed_read_work(record_bytes: usize) -> WorkCounters {
    let bytes = u64::try_from(record_bytes).unwrap_or(u64::MAX);
    WorkCounters {
        backend_read_operations: 1,
        bytes_copied: bytes,
        allocation_operations: u64::from(bytes != 0),
        peak_allocation_bytes: bytes,
        ..WorkCounters::default()
    }
}

fn authority_backend_read_work() -> WorkCounters {
    WorkCounters {
        backend_read_operations: 1,
        ..WorkCounters::default()
    }
}

fn authority_fixed_write_work(record_bytes: usize, operations: u64) -> WorkCounters {
    let bytes = u64::try_from(record_bytes).unwrap_or(u64::MAX);
    WorkCounters {
        backend_write_operations: operations,
        durability_operations: 1,
        bytes_copied: bytes,
        bytes_encoded: bytes,
        allocation_operations: u64::from(bytes != 0),
        peak_allocation_bytes: bytes.saturating_mul(2),
        ..WorkCounters::default()
    }
}

fn authority_payload_length(
    encoded_length: u64,
    maximum_payload_bytes: u64,
    work: WorkCounters,
) -> Result<u64, AuthorityFailure> {
    let prefix = u64::try_from(COMMIT_PREFIX_BYTES).unwrap_or(u64::MAX);
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
) -> Result<WorkCounters, acyclic_fs::WorkError> {
    let copied = encoded_length
        .checked_mul(2)
        .ok_or(acyclic_fs::WorkError::Overflow)?;
    Ok(WorkCounters {
        authority_records_read: 1,
        authority_bytes_read: payload_length,
        bytes_hashed: payload_length,
        bytes_copied: copied,
        allocation_operations: 2 * u64::from(encoded_length != 0),
        peak_allocation_bytes: copied,
        ..WorkCounters::default()
    })
}

fn authority_append_work(
    payload_bytes: u64,
    encoded_commit_bytes: u64,
) -> Result<WorkCounters, acyclic_fs::WorkError> {
    let fixed_bytes = u64::try_from(HEAD_BYTES.saturating_add(OPERATION_BYTES))
        .map_err(|_| acyclic_fs::WorkError::Overflow)?;
    let encoded_bytes = encoded_commit_bytes
        .checked_add(fixed_bytes)
        .ok_or(acyclic_fs::WorkError::Overflow)?;
    let copied_commit = encoded_commit_bytes
        .checked_mul(2)
        .ok_or(acyclic_fs::WorkError::Overflow)?;
    let bytes_copied = copied_commit
        .checked_add(fixed_bytes)
        .ok_or(acyclic_fs::WorkError::Overflow)?;
    let peak_commit = encoded_commit_bytes
        .checked_mul(3)
        .ok_or(acyclic_fs::WorkError::Overflow)?;
    let peak_fixed = fixed_bytes
        .checked_mul(2)
        .ok_or(acyclic_fs::WorkError::Overflow)?;
    let peak_allocation_bytes = peak_commit
        .checked_add(peak_fixed)
        .ok_or(acyclic_fs::WorkError::Overflow)?;
    Ok(WorkCounters {
        authority_records_appended: 1,
        authority_bytes_written: payload_bytes,
        backend_write_operations: 3,
        durability_operations: 1,
        bytes_hashed: payload_bytes,
        bytes_copied,
        bytes_encoded: encoded_bytes,
        allocation_operations: 6,
        peak_allocation_bytes,
        ..WorkCounters::default()
    })
}

enum CancellableError<E> {
    Cancelled,
    Operation(E),
}

fn cancellable_failure(
    error: CancellableError<indexed_db_futures::error::Error>,
    work: WorkCounters,
) -> ObjectFailure {
    match error {
        CancellableError::Cancelled => ObjectFailure::new(ObjectStoreError::Cancelled, work),
        CancellableError::Operation(error) => {
            ObjectFailure::new(ObjectStoreError::Rejected(error.to_string()), work)
        }
    }
}

fn authority_cancellable_failure(
    error: CancellableError<indexed_db_futures::error::Error>,
    work: WorkCounters,
) -> AuthorityFailure {
    match error {
        CancellableError::Cancelled => AuthorityFailure::new(AuthorityStoreError::Cancelled, work),
        CancellableError::Operation(error) => {
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

async fn await_cancellable<F, T, E>(
    operation: F,
    cancellation: &CancellationToken,
) -> Result<T, CancellableError<E>>
where
    F: Future<Output = Result<T, E>>,
{
    let mut operation = std::pin::pin!(operation);
    let mut cancelled = std::pin::pin!(cancellation.cancelled());
    poll_fn(|context| {
        if cancelled.as_mut().poll(context).is_ready() {
            return Poll::Ready(Err(CancellableError::Cancelled));
        }
        match operation.as_mut().poll(context) {
            Poll::Ready(Ok(value)) => Poll::Ready(Ok(value)),
            Poll::Ready(Err(error)) => Poll::Ready(Err(CancellableError::Operation(error))),
            Poll::Pending => Poll::Pending,
        }
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_fs::{AsyncObjectStore, Digest, ObjectKind, WorkError};
    use wasm_bindgen::JsValue;
    use wasm_bindgen_test::*;

    wasm_bindgen_test_configure!(run_in_browser);

    #[wasm_bindgen_test]
    async fn indexed_db_objects_are_bounded_idempotent_and_authenticated() -> Result<(), JsValue> {
        const DATABASE_NAME: &str = "acyclic-fs-object-conformance-v2";
        Database::delete_by_name(DATABASE_NAME)
            .map_err(js_error)?
            .await
            .map_err(js_error)?;
        let store = IndexedDbObjectStore::open(DATABASE_NAME, 1_024)
            .await
            .map_err(js_error)?;
        let bytes = Bytes::from_static(b"browser-object-vector");
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
            Bytes::from_static(b"forged"),
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

    #[wasm_bindgen_test]
    async fn indexed_db_authority_is_atomic_fenced_idempotent_and_replayable() -> Result<(), JsValue>
    {
        const DATABASE_NAME: &str = "acyclic-fs-authority-conformance-v2";
        let store = open_clean_authority(DATABASE_NAME).await?;
        let authority_id = AuthorityId::from_bytes([7; 16]);
        let cancellation = CancellationToken::new();
        let created = AsyncAuthorityStore::create_authority(
            &store,
            authority_id,
            Epoch::GENESIS,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(js_error)?;
        let genesis = Head::genesis(Epoch::GENESIS);
        assert_eq!(created.value, CreateAuthorityOutcome::Created(genesis));
        let existing = AsyncAuthorityStore::create_authority(
            &store,
            authority_id,
            Epoch::new(9).map_err(js_error)?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(js_error)?;
        assert_eq!(existing.value, CreateAuthorityOutcome::Existing(genesis));

        let first_proposal = ProposedCommit {
            operation_id: OperationId::from_bytes([8; 16]),
            fingerprint: Digest::from_bytes([9; 32]),
            payload: Bytes::from_static(b"first"),
        };
        let first = AsyncAuthorityStore::compare_and_append(
            &store,
            authority_id,
            Epoch::GENESIS,
            genesis,
            first_proposal.clone(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(js_error)?;
        let AppendOutcome::Committed(first_commit) = first.value else {
            return Err(JsValue::from_str(
                "first authority append was not committed",
            ));
        };
        let retry = AsyncAuthorityStore::compare_and_append(
            &store,
            authority_id,
            Epoch::GENESIS,
            genesis,
            first_proposal.clone(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(js_error)?;
        assert_eq!(
            retry.value,
            AppendOutcome::AlreadyCommitted(first_commit.clone())
        );
        let conflicting_identity = ProposedCommit {
            fingerprint: Digest::from_bytes([10; 32]),
            ..first_proposal
        };
        let identity_conflict = AsyncAuthorityStore::compare_and_append(
            &store,
            authority_id,
            Epoch::GENESIS,
            genesis,
            conflicting_identity,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
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
    async fn indexed_db_authority_serializes_competing_connections() -> Result<(), JsValue> {
        const DATABASE_NAME: &str = "acyclic-fs-authority-race-v2";
        let creator = open_clean_authority(DATABASE_NAME).await?;
        let authority_id = AuthorityId::from_bytes([21; 16]);
        AsyncAuthorityStore::create_authority(
            &creator,
            authority_id,
            Epoch::GENESIS,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
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
            let outcome = AsyncAuthorityStore::compare_and_append(
                &store,
                authority_id,
                Epoch::GENESIS,
                Head::genesis(Epoch::GENESIS),
                ProposedCommit {
                    operation_id: OperationId::from_bytes([identity_byte; 16]),
                    fingerprint: Digest::from_bytes([identity_byte; 32]),
                    payload: Bytes::from_static(b"race"),
                },
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
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
        let second = AsyncAuthorityStore::compare_and_append(
            store,
            authority_id,
            Epoch::GENESIS,
            first_head,
            ProposedCommit {
                operation_id: OperationId::from_bytes([11; 16]),
                fingerprint: Digest::from_bytes([12; 32]),
                payload: Bytes::from_static(b"second"),
            },
            WorkBudget::UNBOUNDED,
            cancellation,
        )
        .await
        .map_err(js_error)?;
        match second.value {
            AppendOutcome::Committed(commit) => Ok(commit),
            _ => Err(JsValue::from_str(
                "second authority append was not committed",
            )),
        }
    }

    async fn open_clean_authority(database_name: &str) -> Result<IndexedDbAuthorityStore, JsValue> {
        Database::delete_by_name(database_name)
            .map_err(js_error)?
            .await
            .map_err(js_error)?;
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
        let replay = AsyncAuthorityStore::replay(
            store,
            authority_id,
            Sequence::GENESIS,
            ReplayLimit {
                records: 8,
                payload_bytes: 64,
            },
            WorkBudget::UNBOUNDED,
            cancellation,
        )
        .await
        .map_err(js_error)?;
        assert_eq!(replay.value, vec![first.clone(), second.clone()]);
        let resumed = AsyncAuthorityStore::replay(
            store,
            authority_id,
            first.sequence,
            ReplayLimit {
                records: 1,
                payload_bytes: 16,
            },
            WorkBudget::UNBOUNDED,
            cancellation,
        )
        .await
        .map_err(js_error)?;
        assert_eq!(resumed.value, vec![second]);
        let found = AsyncAuthorityStore::find_operation(
            store,
            authority_id,
            first.operation_id,
            WorkBudget::UNBOUNDED,
            cancellation,
        )
        .await
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
        let fenced = AsyncAuthorityStore::fence(
            store,
            authority_id,
            Head {
                epoch: Epoch::GENESIS,
                sequence: second.sequence,
                digest: second.digest,
            },
            WorkBudget::UNBOUNDED,
            cancellation,
        )
        .await
        .map_err(js_error)?;
        let FenceOutcome::Advanced(fenced_head) = fenced.value else {
            return Err(js_error("fresh IndexedDB fence conflicted"));
        };
        assert_eq!(fenced_head.epoch, Epoch::new(2).map_err(js_error)?);
        assert_eq!(fenced_head.sequence, second.sequence);
        assert_eq!(fenced_head.digest, second.digest);
        let stale = AsyncAuthorityStore::compare_and_append(
            store,
            authority_id,
            Epoch::GENESIS,
            fenced_head,
            ProposedCommit {
                operation_id: OperationId::from_bytes([13; 16]),
                fingerprint: Digest::from_bytes([14; 32]),
                payload: Bytes::from_static(b"stale"),
            },
            WorkBudget::UNBOUNDED,
            cancellation,
        )
        .await
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
