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
    AuthorityResult, AuthorityStoreError, CancellationToken, CreateAuthorityOutcome, DurableCommit,
    Epoch, GenerationFork, GenerationForkSource, GuardedAppend, Head, OBJECT_DIGEST_ENVELOPE_BYTES,
    ObjectFailure, ObjectId, ObjectRead, ObjectReadRetention, ObjectResult, ObjectStoreError,
    OperationId, ProposedCommit, PublicationPermit, PublicationReservation, ReplayLimit,
    ReservationOutcome, Sequence, WorkBudget, WorkCounters, WorkspaceForkCommit,
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
fn backend(error: impl std::fmt::Display, work: WorkCounters) -> AuthorityFailure {
    AuthorityFailure::new(AuthorityStoreError::Rejected(error.to_string()), work)
}
fn corrupt(error: impl std::fmt::Display, work: WorkCounters) -> AuthorityFailure {
    AuthorityFailure::new(AuthorityStoreError::Corrupt(error.to_string()), work)
}
fn object_failure(failure: AuthorityFailure) -> ObjectFailure {
    let error = match failure.error {
        AuthorityStoreError::Cancelled => ObjectStoreError::Cancelled,
        AuthorityStoreError::Work(error) => ObjectStoreError::Work(error),
        AuthorityStoreError::Missing => ObjectStoreError::Missing,
        AuthorityStoreError::Corrupt(_) => ObjectStoreError::Corrupt,
        error => ObjectStoreError::Rejected(error.to_string()),
    };
    ObjectFailure::new(error, *failure.work)
}

// One accumulator admits browser requests, copies and retained buffers before dispatch.
struct Io<'a> {
    work: WorkCounters,
    budget: WorkBudget,
    cancellation: &'a CancellationToken,
}
impl<'a> Io<'a> {
    fn new(
        budget: WorkBudget,
        cancellation: &'a CancellationToken,
    ) -> Result<Self, AuthorityFailure> {
        cancellation
            .check()
            .map_err(|_| AuthorityFailure::before_work(AuthorityStoreError::Cancelled))?;
        Ok(Self {
            work: WorkCounters::default(),
            budget,
            cancellation,
        })
    }
    fn admit(&self, delta: WorkCounters) -> Result<WorkCounters, AuthorityFailure> {
        let work = self
            .work
            .checked_add(delta)
            .map_err(|e| AuthorityFailure::new(e.into(), self.work))?;
        work.verify(self.budget)
            .map_err(|e| AuthorityFailure::new(e.into(), self.work))?;
        Ok(work)
    }
    fn charge(&mut self, delta: WorkCounters) -> Result<(), AuthorityFailure> {
        self.work = self.admit(delta)?;
        Ok(())
    }
    fn receipt<T>(&self, value: T) -> crate::OperationReceipt<T> {
        crate::OperationReceipt {
            value,
            work: self.work,
        }
    }
    async fn get(
        &self,
        tx: &Transaction<'_>,
        store: &str,
        key: &str,
    ) -> Result<Option<JsValue>, AuthorityFailure> {
        let store = tx.object_store(store).map_err(|e| backend(e, self.work))?;
        let request = store
            .get::<JsValue, _, _>(key)
            .primitive()
            .map_err(|e| backend(e, self.work))?;
        await_cancellable(request, self.cancellation)
            .await
            .map_err(|e| match e {
                None => AuthorityFailure::new(AuthorityStoreError::Cancelled, self.work),
                Some(e) => backend(e, self.work),
            })
    }
    async fn fixed(
        &mut self,
        tx: &Transaction<'_>,
        store: &str,
        key: &str,
        length: usize,
    ) -> Result<Option<Vec<u8>>, AuthorityFailure> {
        self.admit(authority_fixed_read_work(length))?;
        let value = self.get(tx, store, key).await?;
        let Some(value) = value else {
            self.charge(authority_fixed_read_work(0))?;
            return Ok(None);
        };
        let value = value
            .dyn_into::<Uint8Array>()
            .map_err(|_| corrupt("invalid fixed record", self.work))?;
        if value.length() as usize != length {
            return Err(corrupt("invalid fixed record length", self.work));
        }
        self.charge(authority_fixed_read_work(length))?;
        Ok(Some(value.to_vec()))
    }
    async fn head(
        &mut self,
        tx: &Transaction<'_>,
        authority: AuthorityId,
    ) -> Result<Head, AuthorityFailure> {
        let bytes = self
            .fixed(tx, AUTHORITY_HEADS, &authority_key(authority), HEAD_BYTES)
            .await?
            .ok_or_else(|| AuthorityFailure::new(AuthorityStoreError::Missing, self.work))?;
        decode_head(&bytes).map_err(|e| corrupt(e, self.work))
    }
    async fn gate(
        &mut self,
        tx: &Transaction<'_>,
        authority: AuthorityId,
    ) -> Result<PublicationGateRecord, AuthorityFailure> {
        let bytes = self
            .fixed(tx, AUTHORITY_GATES, &authority_key(authority), GATE_BYTES)
            .await?
            .ok_or_else(|| corrupt("missing publication gate", self.work))?;
        decode_publication_gate(&bytes).map_err(|e| corrupt(e, self.work))
    }
    async fn set(
        &self,
        tx: &Transaction<'_>,
        store: &str,
        key: &str,
        bytes: &[u8],
    ) -> Result<(), AuthorityFailure> {
        let store = tx.object_store(store).map_err(|e| backend(e, self.work))?;
        let request = store
            .put(JsValue::from(Uint8Array::from(bytes)))
            .with_key(key)
            .primitive()
            .map_err(|e| backend(e, self.work))?;
        await_cancellable(request, self.cancellation)
            .await
            .map_err(|e| match e {
                None => AuthorityFailure::new(AuthorityStoreError::Cancelled, self.work),
                Some(e) => backend(e, self.work),
            })?;
        Ok(())
    }
    async fn insert(
        &self,
        tx: &Transaction<'_>,
        store: &str,
        key: &str,
        value: JsValue,
    ) -> Result<(), AuthorityFailure> {
        let store = tx.object_store(store).map_err(|e| backend(e, self.work))?;
        let request = store
            .add(value)
            .with_key(key)
            .primitive()
            .map_err(|e| backend(e, self.work))?;
        await_cancellable(request, self.cancellation)
            .await
            .map_err(|e| match e {
                None => AuthorityFailure::new(AuthorityStoreError::Cancelled, self.work),
                Some(e) => backend(e, self.work),
            })?;
        Ok(())
    }
    async fn blob(
        &self,
        tx: &Transaction<'_>,
        store: &str,
        key: &str,
    ) -> Result<Blob, AuthorityFailure> {
        self.get(tx, store, key)
            .await?
            .ok_or_else(|| corrupt("missing Blob", self.work))?
            .dyn_into::<Blob>()
            .map_err(|_| corrupt("invalid Blob", self.work))
    }
    async fn bytes(&self, blob: &Blob, length: u64) -> Result<Vec<u8>, AuthorityFailure> {
        if blob.size() != length as f64 {
            return Err(corrupt("invalid Blob length", self.work));
        }
        let buffer = await_cancellable(JsFuture::from(blob.array_buffer()), self.cancellation)
            .await
            .map_err(|e| match e {
                None => AuthorityFailure::new(AuthorityStoreError::Cancelled, self.work),
                Some(e) => backend(js_message(&e), self.work),
            })?;
        let array = Uint8Array::new(&buffer);
        if u64::from(array.length()) != length {
            return Err(corrupt("invalid materialized length", self.work));
        }
        Ok(array.to_vec())
    }
    async fn commit(&self, tx: Transaction<'_>) -> Result<(), AuthorityFailure> {
        tx.commit().await.map_err(|e| backend(e, self.work))
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
        durability_operations: 1,
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
    fn payload(&self, blob: &Blob, work: WorkCounters) -> Result<u64, AuthorityFailure> {
        let size = blob.size();
        if !size.is_finite() || size.fract() != 0.0 || size < COMMIT_PREFIX_BYTES as f64 {
            return Err(corrupt("invalid commit Blob size", work));
        }
        let payload = size as u64 - COMMIT_PREFIX_BYTES as u64;
        if payload > self.maximum_payload_bytes {
            return Err(corrupt("oversized commit", work));
        }
        Ok(payload)
    }
    fn validate(
        &self,
        commit: &ProposedCommit,
        work: WorkCounters,
    ) -> Result<(), AuthorityFailure> {
        if commit.payload.len() as u64 > self.maximum_payload_bytes {
            return Err(AuthorityFailure::new(
                AuthorityStoreError::PayloadTooLarge {
                    observed: commit.payload.len() as u64,
                    maximum: self.maximum_payload_bytes,
                },
                work,
            ));
        }
        Ok(())
    }
    async fn decode(
        &self,
        io: &mut Io<'_>,
        authority: AuthorityId,
        blob: &Blob,
        retained: u64,
    ) -> Result<DurableCommit, AuthorityFailure> {
        let payload = self.payload(blob, io.work)?;
        let encoded = payload + COMMIT_PREFIX_BYTES as u64;
        let mut delta = authority_commit_read_work(encoded, payload);
        delta.peak_allocation_bytes += retained;
        io.charge(delta)?;
        let bytes = io.bytes(blob, encoded).await?;
        decode_commit_owned(authority, bytes, self.maximum_payload_bytes)
            .map_err(|e| corrupt(e, io.work))
    }
    async fn window(
        &self,
        io: &mut Io<'_>,
        tx: &Transaction<'_>,
        workspace: crate::WorkspaceId,
    ) -> Result<Option<crate::OperationWindowSnapshot>, AuthorityFailure> {
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
            .map_err(|_| corrupt("invalid window buffer", io.work))?;
        let length = u64::from(array.length());
        let maximum = self.maximum_payload_bytes.min(MAXIMUM_WINDOW_BYTES);
        if length > maximum {
            return Err(AuthorityFailure::new(
                AuthorityStoreError::PayloadTooLarge {
                    observed: length,
                    maximum,
                },
                io.work,
            ));
        }
        io.charge(WorkCounters {
            bytes_copied: length,
            allocation_operations: length + 1,
            peak_allocation_bytes: length * 128,
            items_examined: length,
            ..Default::default()
        })?;
        let snapshot: crate::OperationWindowSnapshot =
            serde_json::from_slice(&array.to_vec()).map_err(|e| corrupt(e, io.work))?;
        if !snapshot.is_valid_for(workspace) {
            return Err(corrupt("invalid operation window", io.work));
        }
        Ok(Some(snapshot))
    }
    fn prepare(
        &self,
        io: &mut Io<'_>,
        authority: AuthorityId,
        actual: Head,
        commit: ProposedCommit,
    ) -> Result<(DurableCommit, Vec<u8>), AuthorityFailure> {
        self.validate(&commit, io.work)?;
        let sequence = actual
            .sequence
            .checked_next()
            .map_err(|e| backend(e, io.work))?;
        let payload = commit.payload.len() as u64;
        let encoded = payload + COMMIT_PREFIX_BYTES as u64;
        let fixed = (HEAD_BYTES + OPERATION_BYTES) as u64;
        io.charge(WorkCounters {
            authority_records_appended: 1,
            authority_bytes_written: payload,
            backend_write_operations: 3,
            durability_operations: u64::from(io.work.durability_operations == 0),
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
        let encoded = encode_commit(&durable).map_err(|e| backend(e, io.work))?;
        Ok((durable, encoded))
    }
    async fn write(
        &self,
        io: &Io<'_>,
        tx: &Transaction<'_>,
        authority: AuthorityId,
        durable: &DurableCommit,
        encoded: &[u8],
    ) -> Result<(), AuthorityFailure> {
        let blob = create_blob(encoded).map_err(|e| backend(js_message(&e), io.work))?;
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
        io: &mut Io<'_>,
        tx: &Transaction<'_>,
        authority: AuthorityId,
        commit: ProposedCommit,
    ) -> Result<(), AuthorityFailure> {
        self.validate(&commit, io.work)?;
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

impl crate::OperationWindowStore for IndexedDbAuthorityStore {
    type Error = AuthorityStoreError;
    async fn load(
        &self,
        workspace: crate::WorkspaceId,
    ) -> Result<Option<crate::OperationWindowSnapshot>, Self::Error> {
        let cancellation = CancellationToken::new();
        let mut io = Io::new(WorkBudget::UNBOUNDED, &cancellation).map_err(|e| e.error)?;
        let tx = transaction(
            &self.database,
            &[OPERATION_WINDOWS],
            TransactionMode::Readonly,
        )
        .map_err(|e| backend(e, io.work).error)?;
        self.window(&mut io, &tx, workspace)
            .await
            .map_err(|e| e.error)
    }
    async fn compare_and_swap(
        &self,
        workspace: crate::WorkspaceId,
        expected: u64,
        replacement: crate::OperationWindowSnapshot,
    ) -> Result<bool, Self::Error> {
        if !replacement.is_valid_for(workspace)
            || expected.checked_add(1) != Some(replacement.revision)
        {
            return Err(AuthorityStoreError::Corrupt(
                "invalid window replacement".to_owned(),
            ));
        }
        let mut bytes = vec![0; self.maximum_payload_bytes.min(MAXIMUM_WINDOW_BYTES) as usize];
        let mut writer = std::io::Cursor::new(bytes.as_mut_slice());
        serde_json::to_writer(&mut writer, &replacement)
            .map_err(|e| backend(e, WorkCounters::default()).error)?;
        let length = writer.position() as usize;
        bytes.truncate(length);
        let cancellation = CancellationToken::new();
        let mut io = Io::new(WorkBudget::UNBOUNDED, &cancellation).map_err(|e| e.error)?;
        let tx = transaction(
            &self.database,
            &[OPERATION_WINDOWS],
            TransactionMode::Readwrite,
        )
        .map_err(|e| backend(e, io.work).error)?;
        if self
            .window(&mut io, &tx, workspace)
            .await
            .map_err(|e| e.error)?
            .map_or(0, |s| s.revision)
            != expected
        {
            return Ok(false);
        }
        io.set(
            &tx,
            OPERATION_WINDOWS,
            &authority_key(AuthorityId::from_bytes(workspace.into_bytes())),
            &bytes,
        )
        .await
        .map_err(|e| e.error)?;
        io.commit(tx).await.map_err(|e| e.error)?;
        Ok(true)
    }
}
impl AsyncAuthorityStore for IndexedDbAuthorityStore {
    async fn create_first_record<G: crate::AuthorityGuard>(
        &self,
        authority: AuthorityId,
        commit: ProposedCommit,
        context: &mut crate::AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<bool, crate::AuthorityError<G::Error>> {
        if guard.is_some() {
            return crate::async_storage::first_record(self, authority, commit, context, guard)
                .await;
        }
        let budget = context.remaining()?;
        let cancellation = context.cancellation;
        let result: AuthorityResult<bool> = async {
            let mut io = Io::new(budget, cancellation)?;
            let tx = transaction(&self.database, AUTHORITY_STORES, TransactionMode::Readwrite)
                .map_err(|e| backend(e, io.work))?;
            if io
                .fixed(&tx, AUTHORITY_HEADS, &authority_key(authority), HEAD_BYTES)
                .await?
                .is_some()
            {
                drop(tx);
                let mut fallback = crate::AuthorityWork::new(budget, cancellation);
                fallback.work = io.work;
                let result = crate::async_storage::first_record(
                    self,
                    authority,
                    commit,
                    &mut fallback,
                    None::<&mut ()>,
                )
                .await
                .map_err(|e| e.into_error(std::convert::identity));
                return fallback.receipt(result);
            }
            self.first(&mut io, &tx, authority, commit).await?;
            io.commit(tx).await?;
            Ok(io.receipt(true))
        }
        .await;
        let value = context.accept(result)?;
        Ok(value)
    }
    async fn fork_workspace<G: crate::AuthorityGuard>(
        &self,
        fork: WorkspaceForkCommit,
        permit: PublicationPermit,
        context: &mut crate::AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<WorkspaceForkOutcome, crate::AuthorityError<G::Error>> {
        if guard.is_some() || permit != PublicationPermit::Unrestricted {
            return crate::async_storage::workspace_fork(self, fork, permit, context, guard).await;
        }
        let budget = context.remaining()?;
        let cancellation = context.cancellation;
        let result: AuthorityResult<WorkspaceForkOutcome> = async {
            let mut io = Io::new(budget, cancellation)?;
            let tx = transaction(&self.database, AUTHORITY_STORES, TransactionMode::Readwrite)
                .map_err(|e| backend(e, io.work))?;
            if fork
                .lineage
                .is_some_and(|s| s.lineage == GenerationFork::PublishedPrefix)
                || io
                    .fixed(
                        &tx,
                        AUTHORITY_HEADS,
                        &authority_key(fork.retention),
                        HEAD_BYTES,
                    )
                    .await?
                    .is_some()
                || io
                    .fixed(
                        &tx,
                        AUTHORITY_HEADS,
                        &authority_key(fork.destination),
                        HEAD_BYTES,
                    )
                    .await?
                    .is_some()
            {
                drop(tx);
                let mut fallback = crate::AuthorityWork::new(budget, cancellation);
                let result = crate::async_storage::workspace_fork(
                    self,
                    fork,
                    PublicationPermit::Unrestricted,
                    &mut fallback,
                    None::<&mut ()>,
                )
                .await
                .map_err(|e| e.into_error(std::convert::identity));
                let receipt = fallback
                    .receipt(result)
                    .map_err(|e| e.map_with_prior_work(io.work, std::convert::identity))?;
                io.work = io
                    .work
                    .checked_add(receipt.work)
                    .map_err(|e| AuthorityFailure::new(e.into(), io.work))?;
                return Ok(io.receipt(receipt.value));
            }
            self.first(&mut io, &tx, fork.retention, fork.retained)
                .await?;
            self.first(&mut io, &tx, fork.destination, fork.creation)
                .await?;
            io.commit(tx).await?;
            Ok(io.receipt(WorkspaceForkOutcome::Committed))
        }
        .await;
        let value = context.accept(result)?;
        Ok(value)
    }
    async fn fork_authority<G: crate::AuthorityGuard>(
        &self,
        source: GenerationForkSource,
        destination: AuthorityId,
        _operation: OperationId,
        context: &mut crate::AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<CreateAuthorityOutcome, crate::AuthorityError<G::Error>> {
        crate::async_storage::seal(guard.as_deref_mut(), context).await?;
        context.check()?;
        if source.lineage == GenerationFork::PublishedPrefix {
            return Err(AuthorityStoreError::Rejected(
                "IndexedDB cannot retain generation lineage prefixes".to_owned(),
            )
            .into());
        }
        self.create_authority(destination, Epoch::GENESIS, context, guard)
            .await
    }
    async fn create_authority<G: crate::AuthorityGuard>(
        &self,
        authority: AuthorityId,
        epoch: Epoch,
        context: &mut crate::AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<CreateAuthorityOutcome, crate::AuthorityError<G::Error>> {
        crate::async_storage::seal(guard.as_deref_mut(), context).await?;
        let budget = context.remaining()?;
        let cancellation = context.cancellation;
        let result: AuthorityResult<CreateAuthorityOutcome> = async {
            let mut io = Io::new(budget, cancellation)?;
            let tx = transaction(&self.database, AUTHORITY_STORES, TransactionMode::Readwrite)
                .map_err(|e| backend(e, io.work))?;
            let key = authority_key(authority);
            if let Some(bytes) = io.fixed(&tx, AUTHORITY_HEADS, &key, HEAD_BYTES).await? {
                let head = decode_head(&bytes).map_err(|e| corrupt(e, io.work))?;
                io.gate(&tx, authority).await?;
                return Ok(io.receipt(CreateAuthorityOutcome::Existing(head)));
            }
            let head = Head::genesis(epoch);
            io.charge(authority_fixed_write_work(HEAD_BYTES + GATE_BYTES, 2))?;
            io.insert(
                &tx,
                AUTHORITY_HEADS,
                &key,
                Uint8Array::from(encode_head(head).as_slice()).into(),
            )
            .await?;
            io.insert(
                &tx,
                AUTHORITY_GATES,
                &key,
                Uint8Array::from(encode_publication_gate(free_publication_gate()).as_slice())
                    .into(),
            )
            .await?;
            io.commit(tx).await?;
            Ok(io.receipt(CreateAuthorityOutcome::Created(head)))
        }
        .await;
        let value = context.accept(result)?;
        if matches!(value, CreateAuthorityOutcome::Existing(_)) {
            crate::async_storage::seal(guard.as_deref_mut(), context).await?;
        }
        Ok(value)
    }
    async fn head(
        &self,
        authority: AuthorityId,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<Head, AuthorityStoreError> {
        let budget = context.remaining()?;
        let cancellation = context.cancellation;
        let result: AuthorityResult<Head> = async {
            let mut io = Io::new(budget, cancellation)?;
            let tx = transaction(
                &self.database,
                &[AUTHORITY_HEADS],
                TransactionMode::Readonly,
            )
            .map_err(|e| backend(e, io.work))?;
            let head = io.head(&tx, authority).await?;
            Ok(io.receipt(head))
        }
        .await;
        let value = context.accept(result)?;
        Ok(value)
    }
    async fn compare_and_append<G: crate::AuthorityGuard>(
        &self,
        request: GuardedAppend,
        context: &mut crate::AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<(AppendOutcome, crate::AuthorityAllocation), crate::AuthorityError<G::Error>> {
        crate::async_storage::seal(guard.as_deref_mut(), context).await?;
        let budget = context.remaining()?;
        let cancellation = context.cancellation;
        let result: AuthorityResult<AppendOutcome> = async {
            let GuardedAppend {
                authority_id: authority,
                epoch,
                expected,
                commit,
                permit,
            } = request;
            let mut io = Io::new(budget, cancellation)?;
            self.validate(&commit, io.work)?;
            let tx = transaction(&self.database, AUTHORITY_STORES, TransactionMode::Readwrite)
                .map_err(|e| backend(e, io.work))?;
            let actual = io.head(&tx, authority).await?;
            if epoch != actual.epoch {
                return Ok(io.receipt(AppendOutcome::Fenced {
                    actual_epoch: actual.epoch,
                }));
            }
            if let Some(bytes) = io
                .fixed(
                    &tx,
                    AUTHORITY_OPERATIONS,
                    &operation_key(authority, commit.operation_id),
                    OPERATION_BYTES,
                )
                .await?
            {
                let op = decode_operation(&bytes).map_err(|e| corrupt(e, io.work))?;
                if op.fingerprint != commit.fingerprint {
                    return Ok(io.receipt(AppendOutcome::IdempotencyConflict {
                        committed_fingerprint: op.fingerprint,
                    }));
                }
                io.charge(authority_fixed_read_work(0))?;
                let blob = io
                    .blob(&tx, AUTHORITY_COMMITS, &commit_key(authority, op.sequence))
                    .await?;
                drop(tx);
                let durable = self.decode(&mut io, authority, &blob, 0).await?;
                if durable.operation_id != commit.operation_id
                    || durable.sequence != op.sequence
                    || durable.fingerprint != op.fingerprint
                {
                    return Err(corrupt("operation index does not match commit", io.work));
                }
                return Ok(io.receipt(AppendOutcome::AlreadyCommitted(durable)));
            }
            if expected != actual {
                return Ok(io.receipt(AppendOutcome::Conflict { actual }));
            }
            let gate = io.gate(&tx, authority).await?;
            let admitted = match permit {
                PublicationPermit::Unrestricted => gate.active.is_none(),
                PublicationPermit::Reservation {
                    operation_id,
                    gate_tail,
                    expected: reserved,
                } => {
                    gate.tail == gate_tail
                        && gate.active == Some(OperationId::from_bytes(operation_id))
                        && reserved == expected
                }
                PublicationPermit::Lease {
                    authority_id,
                    workspace_id,
                    lease_id,
                    expires_at_millis,
                } => {
                    let workspace = crate::WorkspaceId::from_bytes(workspace_id);
                    let snapshot = self.window(&mut io, &tx, workspace).await?;
                    let active = match snapshot {
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
                        .map_err(|e| backend(e, io.work))?;
                    gate.active.is_none()
                        && authority_id == authority.into_bytes()
                        && crate::kernel::volume_authority_id(workspace.volume_id()) == authority
                        && active
                        && now < expires_at_millis
                }
            };
            if !admitted {
                return Ok(io.receipt(AppendOutcome::Fenced {
                    actual_epoch: actual.epoch,
                }));
            }
            let (durable, bytes) = self.prepare(&mut io, authority, actual, commit)?;
            self.write(&io, &tx, authority, &durable, &bytes).await?;
            io.commit(tx).await?;
            Ok(io.receipt(AppendOutcome::Committed(durable)))
        }
        .await;
        let value = context.accept(result)?;
        Ok((value, None))
    }
    async fn reserve_publication(
        &self,
        authority: AuthorityId,
        expected: Head,
        operation: OperationId,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<ReservationOutcome, AuthorityStoreError> {
        let budget = context.remaining()?;
        let cancellation = context.cancellation;
        let result: AuthorityResult<ReservationOutcome> = async {
            let mut io = Io::new(budget, cancellation)?;
            let tx = transaction(
                &self.database,
                &[AUTHORITY_HEADS, AUTHORITY_GATES],
                TransactionMode::Readwrite,
            )
            .map_err(|e| backend(e, io.work))?;
            let actual = io.head(&tx, authority).await?;
            let gate = io.gate(&tx, authority).await?;
            if actual != expected || gate.active.is_some_and(|active| active != operation) {
                return Ok(io.receipt(ReservationOutcome::Conflict { actual }));
            }
            let reservation = PublicationReservation {
                authority_id: authority,
                operation_id: operation,
                expected,
                gate_tail: gate.tail,
            };
            if gate.active == Some(operation) {
                return Ok(io.receipt(ReservationOutcome::AlreadyReserved(reservation)));
            }
            let tail = gate
                .tail
                .checked_add(1)
                .ok_or_else(|| backend("publication gate exhausted", io.work))?;
            io.charge(authority_fixed_write_work(GATE_BYTES, 1))?;
            io.set(
                &tx,
                AUTHORITY_GATES,
                &authority_key(authority),
                &encode_publication_gate(PublicationGateRecord {
                    tail,
                    active: Some(operation),
                    released: None,
                }),
            )
            .await?;
            io.commit(tx).await?;
            Ok(
                io.receipt(ReservationOutcome::Reserved(PublicationReservation {
                    gate_tail: tail,
                    ..reservation
                })),
            )
        }
        .await;
        let value = context.accept(result)?;
        Ok(value)
    }
    async fn release_publication(
        &self,
        reservation: PublicationReservation,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), AuthorityStoreError> {
        let budget = context.remaining()?;
        let cancellation = context.cancellation;
        let result: AuthorityResult<()> = async {
            let mut io = Io::new(budget, cancellation)?;
            let tx = transaction(
                &self.database,
                &[AUTHORITY_GATES],
                TransactionMode::Readwrite,
            )
            .map_err(|e| backend(e, io.work))?;
            let gate = io.gate(&tx, reservation.authority_id).await?;
            if gate.released == Some((reservation.operation_id, reservation.gate_tail)) {
                return Ok(io.receipt(()));
            }
            if gate.tail != reservation.gate_tail || gate.active != Some(reservation.operation_id) {
                return Err(backend(
                    "publication reservation is no longer owned",
                    io.work,
                ));
            }
            let tail = gate
                .tail
                .checked_add(1)
                .ok_or_else(|| backend("publication gate exhausted", io.work))?;
            io.charge(authority_fixed_write_work(GATE_BYTES, 1))?;
            io.set(
                &tx,
                AUTHORITY_GATES,
                &authority_key(reservation.authority_id),
                &encode_publication_gate(PublicationGateRecord {
                    tail,
                    active: None,
                    released: Some((reservation.operation_id, reservation.gate_tail)),
                }),
            )
            .await?;
            io.commit(tx).await?;
            Ok(io.receipt(()))
        }
        .await;
        let value = context.accept(result)?;
        Ok(value)
    }
    async fn fence(
        &self,
        authority: AuthorityId,
        expected: Head,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<FenceOutcome, AuthorityStoreError> {
        let budget = context.remaining()?;
        let cancellation = context.cancellation;
        let result: AuthorityResult<FenceOutcome> = async {
            let mut io = Io::new(budget, cancellation)?;
            let tx = transaction(
                &self.database,
                &[AUTHORITY_HEADS],
                TransactionMode::Readwrite,
            )
            .map_err(|e| backend(e, io.work))?;
            let actual = io.head(&tx, authority).await?;
            if actual != expected {
                return Ok(io.receipt(FenceOutcome::Conflict { actual }));
            }
            let epoch = actual.epoch.get().checked_add(1).ok_or_else(|| {
                AuthorityFailure::new(AuthorityStoreError::EpochExhausted, io.work)
            })?;
            let next = Head {
                epoch: Epoch::new(epoch).map_err(|e| backend(e, io.work))?,
                ..actual
            };
            io.charge(WorkCounters {
                authority_records_appended: 1,
                ..authority_fixed_write_work(HEAD_BYTES, 1)
            })?;
            io.set(
                &tx,
                AUTHORITY_HEADS,
                &authority_key(authority),
                &encode_head(next),
            )
            .await?;
            io.commit(tx).await?;
            Ok(io.receipt(FenceOutcome::Advanced(next)))
        }
        .await;
        let value = context.accept(result)?;
        Ok(value)
    }
    async fn find_operation(
        &self,
        authority: AuthorityId,
        operation: OperationId,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Option<DurableCommit>, crate::AuthorityAllocation), AuthorityStoreError> {
        let budget = context.remaining()?;
        let cancellation = context.cancellation;
        let result: AuthorityResult<Option<DurableCommit>> = async {
            let mut io = Io::new(budget, cancellation)?;
            let tx = transaction(&self.database, AUTHORITY_STORES, TransactionMode::Readonly)
                .map_err(|e| backend(e, io.work))?;
            io.head(&tx, authority).await?;
            let Some(bytes) = io
                .fixed(
                    &tx,
                    AUTHORITY_OPERATIONS,
                    &operation_key(authority, operation),
                    OPERATION_BYTES,
                )
                .await?
            else {
                return Ok(io.receipt(None));
            };
            let op = decode_operation(&bytes).map_err(|e| corrupt(e, io.work))?;
            io.charge(authority_fixed_read_work(0))?;
            let blob = io
                .blob(&tx, AUTHORITY_COMMITS, &commit_key(authority, op.sequence))
                .await?;
            drop(tx);
            let durable = self.decode(&mut io, authority, &blob, 0).await?;
            if durable.operation_id != operation
                || durable.sequence != op.sequence
                || durable.fingerprint != op.fingerprint
            {
                return Err(corrupt("operation index does not match commit", io.work));
            }
            Ok(io.receipt(Some(durable)))
        }
        .await;
        let value = context.accept(result)?;
        Ok((value, None))
    }
    async fn replay(
        &self,
        authority: AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<DurableCommit>, crate::AuthorityAllocation), AuthorityStoreError> {
        let budget = context.remaining()?;
        let cancellation = context.cancellation;
        let result: AuthorityResult<Vec<DurableCommit>> = async {
            if limit.records == 0 || limit.payload_bytes == 0 {
                return Err(AuthorityFailure::before_work(
                    AuthorityStoreError::InvalidReplayLimit,
                ));
            }
            if limit.records > 4096 {
                return Err(AuthorityFailure::before_work(
                    AuthorityStoreError::Rejected(
                        "browser replay page exceeds 4096 records".to_owned(),
                    ),
                ));
            }
            let mut io = Io::new(budget, cancellation)?;
            let tx = transaction(
                &self.database,
                &[AUTHORITY_HEADS, AUTHORITY_COMMITS],
                TransactionMode::Readonly,
            )
            .map_err(|e| backend(e, io.work))?;
            let head = io.head(&tx, authority).await?;
            if after > head.sequence {
                return Err(backend("replay cursor is ahead of head", io.work));
            }
            if after == head.sequence {
                return Ok(io.receipt(Vec::new()));
            }
            let count = (head.sequence.get() - after.get()).min(u64::from(limit.records))
                + u64::from(after != Sequence::GENESIS);
            let start = after.get() + u64::from(after == Sequence::GENESIS);
            let handles = count * size_of::<(Sequence, Blob)>() as u64;
            io.charge(WorkCounters {
                allocation_operations: 1,
                peak_allocation_bytes: handles,
                ..Default::default()
            })?;
            let mut blobs = Vec::new();
            blobs
                .try_reserve_exact(count as usize)
                .map_err(|e| backend(e, io.work))?;
            let mut payload_bytes = 0;
            for offset in 0..count {
                let sequence = Sequence::new(start + offset);
                io.charge(authority_fixed_read_work(0))?;
                let blob = io
                    .blob(&tx, AUTHORITY_COMMITS, &commit_key(authority, sequence))
                    .await?;
                let payload = self.payload(&blob, io.work)?;
                if sequence != after {
                    if payload_bytes + payload > limit.payload_bytes {
                        if payload_bytes == 0 {
                            return Err(AuthorityFailure::new(
                                AuthorityStoreError::ReplayRecordTooLarge {
                                    observed: payload,
                                    maximum: limit.payload_bytes,
                                },
                                io.work,
                            ));
                        }
                        break;
                    }
                    payload_bytes += payload;
                }
                blobs.push((sequence, blob));
            }
            drop(tx);
            let mut retained = blobs.capacity() as u64 * size_of::<(Sequence, Blob)>() as u64
                + blobs.len() as u64 * size_of::<DurableCommit>() as u64;
            io.charge(WorkCounters {
                allocation_operations: 1,
                peak_allocation_bytes: retained,
                ..Default::default()
            })?;
            let mut output = Vec::new();
            output
                .try_reserve_exact(blobs.len())
                .map_err(|e| backend(e, io.work))?;
            let mut previous = crate::Digest::ZERO;
            for (sequence, blob) in blobs {
                let encoded = self.payload(&blob, io.work)? + COMMIT_PREFIX_BYTES as u64;
                let commit = self.decode(&mut io, authority, &blob, retained).await?;
                if commit.sequence != sequence {
                    return Err(corrupt("commit sequence does not match key", io.work));
                }
                if sequence == after {
                    previous = commit.digest;
                    continue;
                }
                if commit.previous_digest != previous {
                    return Err(corrupt("discontinuous authority chain", io.work));
                }
                previous = commit.digest;
                retained += encoded;
                output.push(commit);
            }
            if output.last().is_some_and(|c| c.sequence == head.sequence) && previous != head.digest
            {
                return Err(corrupt(
                    "head digest does not match terminal commit",
                    io.work,
                ));
            }
            Ok(io.receipt(output))
        }
        .await;
        let value = context.accept(result)?;
        Ok((value, None))
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
    fn bound(&self, length: u64, maximum: u64, work: WorkCounters) -> Result<(), ObjectFailure> {
        if length > maximum.min(self.maximum_object_bytes) {
            return Err(ObjectFailure::new(
                ObjectStoreError::TooLarge {
                    observed: length,
                    maximum: maximum.min(self.maximum_object_bytes),
                },
                work,
            ));
        }
        Ok(())
    }
    async fn length(
        io: &Io<'_>,
        tx: &Transaction<'_>,
        key: &str,
    ) -> Result<Option<u64>, ObjectFailure> {
        io.get(tx, OBJECT_METADATA, key)
            .await
            .map_err(object_failure)?
            .map(|value| {
                value
                    .as_string()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| ObjectFailure::new(ObjectStoreError::Corrupt, io.work))
            })
            .transpose()
    }
    async fn read_blob(
        io: &Io<'_>,
        id: ObjectId,
        blob: &Blob,
        length: u64,
    ) -> Result<ObjectRead, ObjectFailure> {
        let bytes = OwnedBytes::from(io.bytes(blob, length).await.map_err(object_failure)?);
        if object_digest(id.kind, &bytes) != id.digest {
            return Err(ObjectFailure::new(ObjectStoreError::Corrupt, io.work));
        }
        Ok(ObjectRead {
            bytes,
            retention: ObjectReadRetention::Owned {
                logical_bytes: length,
            },
        })
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
    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        let mut io = Io::new(budget, cancellation).map_err(object_failure)?;
        if writes.is_empty() {
            return Err(ObjectFailure::before_work(ObjectStoreError::Rejected(
                "empty object write batch".to_owned(),
            )));
        }
        for write in writes {
            let length = write.bytes.len() as u64;
            self.bound(length, self.maximum_object_bytes, io.work)?;
            io.charge(WorkCounters {
                object_probes: 1,
                backend_read_operations: 1,
                bytes_hashed: length + OBJECT_DIGEST_ENVELOPE_BYTES,
                allocation_operations: 1,
                peak_allocation_bytes: OBJECT_KEY_BYTES,
                ..Default::default()
            })
            .map_err(object_failure)?;
            if object_digest(write.object_id.kind, &write.bytes) != write.object_id.digest {
                return Err(ObjectFailure::new(
                    ObjectStoreError::DigestMismatch,
                    io.work,
                ));
            }
        }
        let retained = writes.len() as u64 * size_of::<(Blob, &OwnedBytes)>() as u64;
        io.charge(WorkCounters {
            allocation_operations: 1,
            peak_allocation_bytes: retained,
            ..Default::default()
        })
        .map_err(object_failure)?;
        let mut existing = Vec::new();
        existing
            .try_reserve_exact(writes.len())
            .map_err(|e| object_failure(backend(e, io.work)))?;
        let tx = transaction(
            &self.database,
            &[OBJECTS, OBJECT_METADATA],
            TransactionMode::Readwrite,
        )
        .map_err(|e| object_failure(backend(e, io.work)))?;
        let mut added = false;
        for write in writes {
            let key = Self::key(write.object_id);
            let length = write.bytes.len() as u64;
            let stored = Self::length(&io, &tx, &key).await?;
            let copy = WorkCounters {
                bytes_copied: length * 2,
                allocation_operations: 2 * u64::from(length != 0),
                peak_allocation_bytes: OBJECT_KEY_BYTES + length * 2 + retained,
                ..Default::default()
            };
            if let Some(stored) = stored {
                if stored != length {
                    return Err(ObjectFailure::new(ObjectStoreError::Corrupt, io.work));
                }
                io.charge(WorkCounters {
                    backend_read_operations: 1,
                    object_bytes_read: length,
                    ..copy
                })
                .map_err(object_failure)?;
                let blob = io.blob(&tx, OBJECTS, &key).await.map_err(object_failure)?;
                existing.push((blob, &write.bytes));
            } else {
                io.charge(WorkCounters {
                    backend_write_operations: 2,
                    object_bytes_written: length,
                    ..copy
                })
                .map_err(object_failure)?;
                cancellation
                    .check()
                    .map_err(|_| ObjectFailure::new(ObjectStoreError::Cancelled, io.work))?;
                let blob = create_blob(&write.bytes)
                    .map_err(|e| object_failure(backend(js_message(&e), io.work)))?;
                io.insert(&tx, OBJECTS, &key, blob.into())
                    .await
                    .map_err(object_failure)?;
                io.insert(
                    &tx,
                    OBJECT_METADATA,
                    &key,
                    JsValue::from_str(&length.to_string()),
                )
                .await
                .map_err(object_failure)?;
                added = true;
            }
        }
        if added {
            io.charge(WorkCounters {
                durability_operations: 1,
                ..Default::default()
            })
            .map_err(object_failure)?;
            io.commit(tx).await.map_err(object_failure)?;
        } else {
            drop(tx);
        }
        for (blob, bytes) in existing {
            if io
                .bytes(&blob, bytes.len() as u64)
                .await
                .map_err(object_failure)?
                .as_slice()
                != bytes.as_ref()
            {
                return Err(ObjectFailure::new(ObjectStoreError::Corrupt, io.work));
            }
        }
        Ok(io.receipt(()))
    }
    async fn read(
        &self,
        id: ObjectId,
        maximum: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<ObjectRead> {
        let mut io = Io::new(budget, cancellation).map_err(object_failure)?;
        io.charge(WorkCounters {
            object_probes: 1,
            backend_read_operations: 1,
            allocation_operations: 1,
            peak_allocation_bytes: OBJECT_KEY_BYTES,
            ..Default::default()
        })
        .map_err(object_failure)?;
        let tx = transaction(
            &self.database,
            &[OBJECTS, OBJECT_METADATA],
            TransactionMode::Readonly,
        )
        .map_err(|e| object_failure(backend(e, io.work)))?;
        let key = Self::key(id);
        let length = Self::length(&io, &tx, &key)
            .await?
            .ok_or_else(|| ObjectFailure::new(ObjectStoreError::Missing, io.work))?;
        self.bound(length, maximum, io.work)?;
        io.charge(WorkCounters {
            backend_read_operations: 1,
            object_bytes_read: length,
            bytes_hashed: length + OBJECT_DIGEST_ENVELOPE_BYTES,
            bytes_copied: length * 2,
            allocation_operations: 2 * u64::from(length != 0),
            peak_allocation_bytes: OBJECT_KEY_BYTES + length * 2,
            ..Default::default()
        })
        .map_err(object_failure)?;
        let blob = io.blob(&tx, OBJECTS, &key).await.map_err(object_failure)?;
        drop(tx);
        let value = Self::read_blob(&io, id, &blob, length).await?;
        Ok(io.receipt(value))
    }
    async fn read_many(
        &self,
        requests: &[crate::ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<Vec<ObjectRead>> {
        let mut io = Io::new(budget, cancellation).map_err(object_failure)?;
        if requests.is_empty() {
            return Err(ObjectFailure::before_work(ObjectStoreError::Rejected(
                "empty object read batch".to_owned(),
            )));
        }
        let count = requests.len() as u64;
        let mut retained =
            count * (size_of::<ObjectRead>() + size_of::<(ObjectId, Blob, u64)>()) as u64;
        io.admit(WorkCounters {
            items_examined: count,
            items_returned: count,
            allocation_operations: 2,
            peak_allocation_bytes: retained,
            ..Default::default()
        })
        .map_err(object_failure)?;
        io.charge(WorkCounters {
            items_examined: count,
            allocation_operations: 2,
            peak_allocation_bytes: retained,
            ..Default::default()
        })
        .map_err(object_failure)?;
        let mut output = Vec::new();
        output
            .try_reserve_exact(requests.len())
            .map_err(|e| object_failure(backend(e, io.work)))?;
        let mut handles = Vec::new();
        handles
            .try_reserve_exact(requests.len())
            .map_err(|e| object_failure(backend(e, io.work)))?;
        let tx = transaction(
            &self.database,
            &[OBJECTS, OBJECT_METADATA],
            TransactionMode::Readonly,
        )
        .map_err(|e| object_failure(backend(e, io.work)))?;
        for request in requests {
            io.charge(WorkCounters {
                object_probes: 1,
                backend_read_operations: 1,
                allocation_operations: 1,
                peak_allocation_bytes: retained + OBJECT_KEY_BYTES,
                ..Default::default()
            })
            .map_err(object_failure)?;
            let key = Self::key(request.object_id);
            let length = Self::length(&io, &tx, &key)
                .await?
                .ok_or_else(|| ObjectFailure::new(ObjectStoreError::Missing, io.work))?;
            self.bound(length, request.maximum_bytes, io.work)?;
            io.charge(authority_fixed_read_work(0))
                .map_err(object_failure)?;
            handles.push((
                request.object_id,
                io.blob(&tx, OBJECTS, &key).await.map_err(object_failure)?,
                length,
            ));
        }
        drop(tx);
        for (id, blob, length) in handles {
            io.charge(WorkCounters {
                object_bytes_read: length,
                bytes_hashed: length + OBJECT_DIGEST_ENVELOPE_BYTES,
                bytes_copied: length * 2,
                allocation_operations: 2 * u64::from(length != 0),
                peak_allocation_bytes: retained + OBJECT_KEY_BYTES + length * 2,
                ..Default::default()
            })
            .map_err(object_failure)?;
            output.push(Self::read_blob(&io, id, &blob, length).await?);
            retained += length;
        }
        io.work.items_returned = count;
        Ok(io.receipt(output))
    }
    async fn contains(
        &self,
        id: ObjectId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<bool> {
        match self
            .read(id, self.maximum_object_bytes, budget, cancellation)
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
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests;
