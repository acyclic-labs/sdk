//! `IndexedDB` authority and immutable-object persistence.

use super::authority_codec::{
    COMMIT_PREFIX_BYTES, GATE_BYTES, HEAD_BYTES, OPERATION_BYTES, OperationRecord,
    PublicationGateRecord, authority_key, commit_key, decode_commit_owned, decode_head,
    decode_operation, decode_publication_gate, encode_commit, encode_head, encode_operation,
    encode_publication_gate, free_publication_gate, operation_key,
};
use crate::storage::{FenceOutcome, ObjectWrite};
use crate::{
    AppendOutcome, AsyncAuthorityStore, AsyncObjectStore, AuthorityId, AuthorityStoreError,
    CancellationToken, CreateAuthorityOutcome, DurableCommit, Epoch, GenerationFork, GuardedAppend,
    Head, OBJECT_DIGEST_ENVELOPE_BYTES, ObjectId, ObjectRead, ObjectReadRetention,
    ObjectStoreError, OperationId, ProposedCommit, PublicationPermit, PublicationReservation,
    ReplayLimit, ReservationOutcome, Sequence, WorkBudget, WorkCounters, WorkspaceForkCommit,
    WorkspaceForkOutcome, authority_commit_digest, object_digest,
};
use acyclic_stream::OwnedBytes;
#[cfg(test)]
use bytes::Bytes;
use indexed_db_futures::database::Database;
use indexed_db_futures::prelude::*;
use indexed_db_futures::transaction::{
    Transaction, TransactionDurability, TransactionMode, TransactionOptions,
};
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
const MAXIMUM_WINDOW_BYTES: u64 = 64 * 1024;
const OBJECT_KEY_BYTES: u64 = 66;
const AUTHORITY_STORES: &[&str] = &[
    AUTHORITY_HEADS,
    AUTHORITY_COMMITS,
    AUTHORITY_OPERATIONS,
    AUTHORITY_GATES,
    OPERATION_WINDOWS,
];

/// Browser database initialization failures.
#[derive(Debug, Error)]
pub enum IndexedDbOpenError {
    /// The database name or byte bound is invalid.
    #[error("invalid IndexedDB options")]
    InvalidOptions,
    /// The browser rejected the database open.
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

async fn open_database(name: &str) -> Result<Database, IndexedDbOpenError> {
    Database::open(name)
        .with_version(DATABASE_VERSION)
        .with_on_upgrade_needed(|event, db| {
            if event.old_version() > 0.5 {
                return Err(indexed_db_futures::error::Error::from(js_sys::Error::new(
                    "unsupported filesystem schema",
                )));
            }
            for name in [
                OBJECTS,
                OBJECT_METADATA,
                AUTHORITY_HEADS,
                AUTHORITY_COMMITS,
                AUTHORITY_OPERATIONS,
                AUTHORITY_GATES,
                OPERATION_WINDOWS,
            ] {
                db.create_object_store(name).build()?;
            }
            Ok(())
        })
        .await
        .map_err(IndexedDbOpenError::Open)
}
fn transaction<'a>(
    database: &'a Database,
    stores: &[&str],
    mode: TransactionMode,
) -> Result<Transaction<'a>, indexed_db_futures::error::Error> {
    if mode == TransactionMode::Readonly {
        return database.transaction(stores).build();
    }
    let mut options = TransactionOptions::new();
    options.set_durability(TransactionDurability::Strict);
    database
        .transaction(stores)
        .with_mode(mode)
        .with_options(options)
        .build()
}
fn create_blob(bytes: &[u8]) -> Result<Blob, JsValue> {
    let parts = Array::new();
    parts.push(&Uint8Array::from(bytes));
    Blob::new_with_u8_array_sequence(&parts)
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
        operation
            .as_mut()
            .poll(context)
            .map(|result| result.map_err(Some))
    })
    .await
}
fn js_message(value: &JsValue) -> String {
    value
        .as_string()
        .unwrap_or_else(|| "browser JavaScript operation failed".to_owned())
}
fn backend(error: impl std::fmt::Display) -> AuthorityStoreError {
    (AuthorityStoreError::Rejected(error.to_string()))
}
fn corrupt(error: impl std::fmt::Display) -> AuthorityStoreError {
    (AuthorityStoreError::Corrupt(error.to_string()))
}
fn object_error(error: AuthorityStoreError) -> ObjectStoreError {
    match error {
        AuthorityStoreError::Cancelled => ObjectStoreError::Cancelled,
        AuthorityStoreError::Work(error) => ObjectStoreError::Work(error),
        AuthorityStoreError::Missing => ObjectStoreError::Missing,
        AuthorityStoreError::Corrupt(_) => ObjectStoreError::Corrupt,
        error => ObjectStoreError::Rejected(error.to_string()),
    }
}

fn authority_fixed_read_work(length: usize) -> WorkCounters {
    let bytes = length as u64;
    WorkCounters {
        backend_read_operations: 1,
        bytes_copied: bytes,
        allocation_operations: u64::from(bytes != 0),
        peak_allocation_bytes: bytes,
        ..Default::default()
    }
}
fn authority_fixed_write_work(length: usize, operations: u64) -> WorkCounters {
    let bytes = length as u64;
    WorkCounters {
        backend_write_operations: operations,
        bytes_copied: bytes,
        bytes_encoded: bytes,
        allocation_operations: u64::from(bytes != 0),
        peak_allocation_bytes: bytes * 2,
        ..Default::default()
    }
}
fn authority_commit_read_work(encoded: u64, payload: u64) -> WorkCounters {
    WorkCounters {
        authority_records_read: 1,
        authority_bytes_read: payload,
        bytes_hashed: payload,
        bytes_copied: encoded * 2,
        allocation_operations: 2 * u64::from(encoded != 0),
        peak_allocation_bytes: encoded * 2,
        ..Default::default()
    }
}

/// Transactional browser-local authority persistence.
#[derive(Clone)]
pub struct IndexedDbAuthorityStore {
    database: Database,
    maximum_payload_bytes: u64,
}
impl IndexedDbAuthorityStore {
    /// Opens the canonical version-1 filesystem database.
    pub async fn open(name: &str, maximum_payload_bytes: u64) -> Result<Self, IndexedDbOpenError> {
        if name.is_empty()
            || maximum_payload_bytes == 0
            || maximum_payload_bytes > u64::from(u32::MAX) - COMMIT_PREFIX_BYTES as u64
        {
            return Err(IndexedDbOpenError::InvalidOptions);
        }
        Ok(Self {
            database: open_database(name).await?,
            maximum_payload_bytes,
        })
    }
    fn payload(&self, blob: &Blob) -> Result<u64, AuthorityStoreError> {
        let size = blob.size();
        if !size.is_finite() || size.fract() != 0.0 || size < COMMIT_PREFIX_BYTES as f64 {
            return Err(corrupt("invalid commit Blob size"));
        }
        let payload = size as u64 - COMMIT_PREFIX_BYTES as u64;
        if payload > self.maximum_payload_bytes {
            return Err(corrupt("oversized commit"));
        }
        Ok(payload)
    }
    fn validate(&self, commit: &ProposedCommit) -> Result<(), AuthorityStoreError> {
        if commit.payload.len() as u64 > self.maximum_payload_bytes {
            return Err((AuthorityStoreError::PayloadTooLarge {
                observed: commit.payload.len() as u64,
                maximum: self.maximum_payload_bytes,
            }));
        }
        Ok(())
    }
    async fn decode(
        &self,
        io: &mut crate::AuthorityWork<'_>,
        authority: AuthorityId,
        blob: &Blob,
        retained: u64,
    ) -> Result<DurableCommit, AuthorityStoreError> {
        let payload = self.payload(blob)?;
        let encoded = payload + COMMIT_PREFIX_BYTES as u64;
        let mut delta = authority_commit_read_work(encoded, payload);
        delta.peak_allocation_bytes += retained;
        io.charge(delta)?;
        let bytes = io.bytes(blob, encoded).await?;
        decode_commit_owned(authority, bytes, self.maximum_payload_bytes).map_err(corrupt)
    }
    async fn window(
        &self,
        io: &mut crate::AuthorityWork<'_>,
        tx: &Transaction<'_>,
        workspace: crate::WorkspaceId,
    ) -> Result<Option<crate::OperationWindowSnapshot>, AuthorityStoreError> {
        io.charge(authority_fixed_read_work(0))?;
        let Some(value) = io
            .get(
                tx,
                OPERATION_WINDOWS,
                &authority_key(AuthorityId::from_bytes(workspace.into_bytes())),
            )
            .await?
        else {
            return Ok(None);
        };
        let array = value
            .dyn_into::<Uint8Array>()
            .map_err(|_| corrupt("invalid window buffer"))?;
        let length = u64::from(array.length());
        let maximum = self.maximum_payload_bytes.min(MAXIMUM_WINDOW_BYTES);
        if length > maximum {
            return Err((AuthorityStoreError::PayloadTooLarge {
                observed: length,
                maximum,
            }));
        }
        io.charge(WorkCounters {
            bytes_copied: length,
            allocation_operations: length + 1,
            peak_allocation_bytes: length * 128,
            items_examined: length,
            ..Default::default()
        })?;
        let snapshot: crate::OperationWindowSnapshot =
            serde_json::from_slice(&array.to_vec()).map_err(corrupt)?;
        if !snapshot.is_valid_for(workspace) {
            return Err(corrupt("invalid operation window"));
        }
        Ok(Some(snapshot))
    }
    fn prepare(
        &self,
        io: &mut crate::AuthorityWork<'_>,
        authority: AuthorityId,
        actual: Head,
        commit: ProposedCommit,
    ) -> Result<(DurableCommit, Vec<u8>), AuthorityStoreError> {
        self.validate(&commit)?;
        let sequence = actual.sequence.checked_next().map_err(backend)?;
        let payload = commit.payload.len() as u64;
        let encoded = payload + COMMIT_PREFIX_BYTES as u64;
        let fixed = (HEAD_BYTES + OPERATION_BYTES) as u64;
        io.charge(WorkCounters {
            authority_records_appended: 1,
            authority_bytes_written: payload,
            backend_write_operations: 3,

            bytes_hashed: payload,
            bytes_copied: encoded * 2 + fixed,
            bytes_encoded: encoded + fixed,
            allocation_operations: 6,
            peak_allocation_bytes: encoded * 3 + fixed * 2,
            ..Default::default()
        })?;
        let digest = authority_commit_digest(
            authority,
            actual.epoch,
            sequence,
            commit.operation_id,
            commit.fingerprint,
            actual.digest,
            &commit.payload,
        );
        let durable = DurableCommit {
            settled_at_micros: None,
            epoch: actual.epoch,
            sequence,
            operation_id: commit.operation_id,
            fingerprint: commit.fingerprint,
            previous_digest: actual.digest,
            digest,
            payload: commit.payload,
        };
        let encoded = encode_commit(&durable).map_err(backend)?;
        Ok((durable, encoded))
    }
    async fn write(
        &self,
        io: &crate::AuthorityWork<'_>,
        tx: &Transaction<'_>,
        authority: AuthorityId,
        durable: &DurableCommit,
        encoded: &[u8],
    ) -> Result<(), AuthorityStoreError> {
        let blob = create_blob(encoded).map_err(|e| backend(js_message(&e)))?;
        io.insert(
            tx,
            AUTHORITY_COMMITS,
            &commit_key(authority, durable.sequence),
            blob.into(),
        )
        .await?;
        let operation = encode_operation(OperationRecord {
            sequence: durable.sequence,
            fingerprint: durable.fingerprint,
        });
        io.insert(
            tx,
            AUTHORITY_OPERATIONS,
            &operation_key(authority, durable.operation_id),
            Uint8Array::from(operation.as_slice()).into(),
        )
        .await?;
        io.set(
            tx,
            AUTHORITY_HEADS,
            &authority_key(authority),
            &encode_head(Head {
                epoch: durable.epoch,
                sequence: durable.sequence,
                digest: durable.digest,
            }),
        )
        .await
    }
    async fn first(
        &self,
        io: &mut crate::AuthorityWork<'_>,
        tx: &Transaction<'_>,
        authority: AuthorityId,
        commit: ProposedCommit,
    ) -> Result<(), AuthorityStoreError> {
        self.validate(&commit)?;
        io.charge(WorkCounters {
            durability_operations: 0,
            ..authority_fixed_write_work(GATE_BYTES, 1)
        })?;
        io.insert(
            tx,
            AUTHORITY_GATES,
            &authority_key(authority),
            Uint8Array::from(encode_publication_gate(free_publication_gate()).as_slice()).into(),
        )
        .await?;
        let (durable, encoded) =
            self.prepare(io, authority, Head::genesis(Epoch::GENESIS), commit)?;
        self.write(io, tx, authority, &durable, &encoded).await
    }
}

/// Transactional browser-local immutable objects.
pub struct IndexedDbObjectStore {
    database: Database,
    maximum_object_bytes: u64,
}
impl IndexedDbObjectStore {
    /// Opens the canonical version-1 filesystem database.
    pub async fn open(name: &str, maximum_object_bytes: u64) -> Result<Self, IndexedDbOpenError> {
        if name.is_empty()
            || maximum_object_bytes == 0
            || maximum_object_bytes > u64::from(u32::MAX)
        {
            return Err(IndexedDbOpenError::InvalidOptions);
        }
        Ok(Self {
            database: open_database(name).await?,
            maximum_object_bytes,
        })
    }
    pub(crate) fn key(id: ObjectId) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut key = String::with_capacity(OBJECT_KEY_BYTES as usize);
        key.push(char::from(b'0' + id.kind.canonical_tag()));
        key.push(':');
        for byte in id.digest.as_bytes() {
            key.push(char::from(HEX[usize::from(byte >> 4)]));
            key.push(char::from(HEX[usize::from(byte & 15)]));
        }
        key
    }
    fn bound(&self, length: u64, maximum: u64) -> Result<(), ObjectStoreError> {
        if length > maximum.min(self.maximum_object_bytes) {
            return Err((ObjectStoreError::TooLarge {
                observed: length,
                maximum: maximum.min(self.maximum_object_bytes),
            }));
        }
        Ok(())
    }
    async fn length(
        io: &crate::AuthorityWork<'_>,
        tx: &Transaction<'_>,
        key: &str,
    ) -> Result<Option<u64>, ObjectStoreError> {
        io.get(tx, OBJECT_METADATA, key)
            .await
            .map_err(object_error)?
            .map(|value| {
                value
                    .as_string()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| (ObjectStoreError::Corrupt))
            })
            .transpose()
    }
    async fn read_blob(
        io: &crate::AuthorityWork<'_>,
        id: ObjectId,
        blob: &Blob,
        length: u64,
    ) -> Result<ObjectRead, ObjectStoreError> {
        let bytes = OwnedBytes::from(io.bytes(blob, length).await.map_err(object_error)?);
        if object_digest(id.kind, &bytes) != id.digest {
            return Err((ObjectStoreError::Corrupt));
        }
        Ok(ObjectRead {
            bytes,
            retention: ObjectReadRetention::Owned {
                logical_bytes: length,
            },
        })
    }
}
#[cfg(test)]
mod tests;

mod authority;
mod objects;
mod work;
