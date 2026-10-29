//! `IndexedDB` immutable-object persistence.

use crate::storage::ObjectWrite;
use crate::{
    AsyncObjectStore, AuthorityStoreError, CancellationToken, ObjectId, ObjectRead,
    ObjectReadRetention, ObjectStoreError, WorkCounters, object_digest,
};
use acyclic_stream::OwnedBytes;
use indexed_db_futures::database::Database;
use indexed_db_futures::prelude::*;
use indexed_db_futures::transaction::{
    Transaction, TransactionDurability, TransactionMode, TransactionOptions,
};
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
fn object_key_bytes(id: ObjectId) -> u64 {
    (id.encoded_len() * 2) as u64
}

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

pub(super) async fn open_database(name: &str) -> Result<Database, IndexedDbOpenError> {
    Database::open(name)
        .with_version(DATABASE_VERSION)
        .with_on_upgrade_needed(|event, db| {
            if event.old_version() > 0.5 {
                return Err(indexed_db_futures::error::Error::from(js_sys::Error::new(
                    "unsupported filesystem schema",
                )));
            }
            db.create_object_store(OBJECTS).build()?;
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
async fn await_cancellable<T, E: std::fmt::Display>(
    operation: impl Future<Output = Result<T, E>>,
    cancellation: &CancellationToken,
) -> Result<T, AuthorityStoreError> {
    let mut operation = std::pin::pin!(operation);
    let mut cancelled = std::pin::pin!(cancellation.cancelled());
    poll_fn(|context| {
        if cancelled.as_mut().poll(context).is_ready() {
            return Poll::Ready(Err(AuthorityStoreError::Cancelled));
        }
        operation
            .as_mut()
            .poll(context)
            .map(|result| result.map_err(backend))
    })
    .await
}
fn js_message(value: &JsValue) -> String {
    value
        .as_string()
        .unwrap_or_else(|| "browser JavaScript operation failed".to_owned())
}
fn backend(error: impl std::fmt::Display) -> AuthorityStoreError {
    AuthorityStoreError::Rejected(error.to_string())
}
fn corrupt(error: impl std::fmt::Display) -> AuthorityStoreError {
    AuthorityStoreError::Corrupt(error.to_string())
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
        let mut key = String::with_capacity(id.encoded_len() * 2);
        for byte in id.to_bytes().as_slice() {
            for nibble in [byte >> 4, byte & 15] {
                key.push(char::from(match nibble {
                    0..=9 => b'0' + nibble,
                    _ => b'a' + nibble - 10,
                }));
            }
        }
        key
    }
    fn bound(&self, length: u64, maximum: u64) -> Result<(), ObjectStoreError> {
        if length > maximum.min(self.maximum_object_bytes) {
            return Err(ObjectStoreError::TooLarge {
                observed: length,
                maximum: maximum.min(self.maximum_object_bytes),
            });
        }
        Ok(())
    }
    async fn object(
        io: &crate::AuthorityWork<'_>,
        tx: &Transaction<'_>,
        key: &str,
    ) -> Result<Option<Blob>, ObjectStoreError> {
        io.get(tx, OBJECTS, key)
            .await
            .map_err(object_error)?
            .map(|value| {
                value
                    .dyn_into::<Blob>()
                    .map_err(|_| ObjectStoreError::Corrupt)
            })
            .transpose()
    }
    async fn read_blob(
        io: &crate::AuthorityWork<'_>,
        id: ObjectId,
        blob: &Blob,
        length: u64,
    ) -> Result<ObjectRead, ObjectStoreError> {
        let bytes = OwnedBytes::from(io.blob_bytes(blob, length).await.map_err(object_error)?);
        if object_digest(&bytes) != id.digest {
            return Err(ObjectStoreError::Corrupt);
        }
        Ok(ObjectRead {
            bytes,
            retention: ObjectReadRetention::Owned {
                logical_bytes: length,
            },
        })
    }
}

mod objects;
mod work;
