//! Authoritative origin-private immutable object storage.

use acyclic_fs::storage::ObjectWrite;
use acyclic_fs::{
    AsyncObjectStore, CancellationToken, OBJECT_DIGEST_ENVELOPE_BYTES, ObjectFailure, ObjectId,
    ObjectRead, ObjectReadRequest, ObjectReadRetention, ObjectReceipt, ObjectResult,
    ObjectStoreError, WorkBudget, WorkCounters, object_digest,
};
use bytes::Bytes;
use futures::future::join_all;
use js_sys::Uint8Array;
use thiserror::Error;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    Blob, FileSystemDirectoryHandle, FileSystemFileHandle, FileSystemGetDirectoryOptions,
    FileSystemGetFileOptions, FileSystemWritableFileStream,
};

/// OPFS profile initialization failure.
#[derive(Debug, Error)]
pub enum OpfsOpenError {
    /// Browser options are empty or exceed the implementation's exact integer range.
    #[error("OPFS options are invalid")]
    InvalidOptions,
    /// The browser does not expose the required origin-private filesystem API.
    #[error("OPFS is unavailable: {0}")]
    Unavailable(String),
}

/// Authoritative authenticated immutable storage in OPFS.
pub struct OpfsAcceleratedObjectStore {
    directory: FileSystemDirectoryHandle,
    maximum_object_bytes: u64,
}

const MAX_BATCH_CONCURRENCY: usize = 16;

impl OpfsAcceleratedObjectStore {
    /// Opens one explicit OPFS namespace.
    ///
    /// # Errors
    ///
    /// Fails when options are invalid or OPFS is unavailable.
    pub async fn open(
        database_name: &str,
        maximum_object_bytes: u64,
    ) -> Result<Self, OpfsOpenError> {
        if database_name.is_empty()
            || maximum_object_bytes == 0
            || maximum_object_bytes > u64::from(u32::MAX)
        {
            return Err(OpfsOpenError::InvalidOptions);
        }
        let window = web_sys::window()
            .ok_or_else(|| OpfsOpenError::Unavailable("window is absent".to_owned()))?;
        let root = JsFuture::from(window.navigator().storage().get_directory())
            .await
            .map_err(|error| OpfsOpenError::Unavailable(js_message(&error)))?
            .dyn_into::<FileSystemDirectoryHandle>()
            .map_err(|_| OpfsOpenError::Unavailable("root handle has the wrong type".to_owned()))?;
        let product = create_directory(&root, "acyclic-fs-v1").await?;
        let directory = create_directory(&product, &namespace_name(database_name)).await?;
        Ok(Self {
            directory,
            maximum_object_bytes,
        })
    }

    fn key(object_id: ObjectId) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut key = String::with_capacity(66);
        key.push(char::from(b'0' + object_id.kind.canonical_tag()));
        key.push(':');
        for byte in object_id.digest.as_bytes() {
            key.push(char::from(
                *HEX.get(usize::from(byte >> 4)).unwrap_or(&b'0'),
            ));
            key.push(char::from(
                *HEX.get(usize::from(byte & 0x0f)).unwrap_or(&b'0'),
            ));
        }
        key
    }

    async fn file_handle(
        &self,
        object_id: ObjectId,
    ) -> Result<FileSystemFileHandle, ObjectStoreError> {
        JsFuture::from(self.directory.get_file_handle(&Self::key(object_id)))
            .await
            .map_err(|error| missing_or_rejected(&error))?
            .dyn_into::<FileSystemFileHandle>()
            .map_err(|_| ObjectStoreError::Corrupt)
    }

    async fn write(
        &self,
        object_id: ObjectId,
        bytes: &Bytes,
        cancellation: &CancellationToken,
    ) -> Result<(), ObjectStoreError> {
        cancellation
            .check()
            .map_err(|_| ObjectStoreError::Cancelled)?;
        let options = FileSystemGetFileOptions::new();
        options.set_create(true);
        let handle = JsFuture::from(
            self.directory
                .get_file_handle_with_options(&Self::key(object_id), &options),
        )
        .await
        .map_err(|error| rejected(&error))?
        .dyn_into::<FileSystemFileHandle>()
        .map_err(|_| ObjectStoreError::Corrupt)?;
        cancellation
            .check()
            .map_err(|_| ObjectStoreError::Cancelled)?;
        let writable = JsFuture::from(handle.create_writable())
            .await
            .map_err(|error| rejected(&error))?
            .dyn_into::<FileSystemWritableFileStream>()
            .map_err(|_| ObjectStoreError::Corrupt)?;
        let cancelled_before_write = cancellation.is_cancelled();
        if !cancelled_before_write {
            JsFuture::from(
                writable
                    .write_with_u8_array(bytes)
                    .map_err(|error| rejected(&error))?,
            )
            .await
            .map_err(|error| rejected(&error))?;
        }
        JsFuture::from(writable.close())
            .await
            .map_err(|error| rejected(&error))?;
        if cancelled_before_write {
            return Err(ObjectStoreError::Cancelled);
        }
        cancellation
            .check()
            .map_err(|_| ObjectStoreError::Cancelled)
    }

    async fn read_exact(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<ObjectRead> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        let key_bytes = u64::try_from(Self::key(object_id).len()).unwrap_or(u64::MAX);
        let mut work = WorkCounters {
            object_probes: 1,
            backend_read_operations: 2,
            allocation_operations: 1,
            peak_allocation_bytes: key_bytes,
            ..WorkCounters::default()
        };
        work.verify(budget)
            .map_err(|error| ObjectFailure::before_work(error.into()))?;
        let handle = self
            .file_handle(object_id)
            .await
            .map_err(|error| ObjectFailure::new(error, work))?;
        cancellation
            .check()
            .map_err(|_| ObjectFailure::new(ObjectStoreError::Cancelled, work))?;
        let file = JsFuture::from(handle.get_file())
            .await
            .map_err(|error| ObjectFailure::new(rejected(&error), work))?
            .dyn_into::<web_sys::File>()
            .map_err(|_| ObjectFailure::new(ObjectStoreError::Corrupt, work))?;
        let blob: Blob = file.unchecked_into();
        let length = exact_file_size(blob.size())
            .ok_or_else(|| ObjectFailure::new(ObjectStoreError::Corrupt, work))?;
        let maximum = maximum_bytes.min(self.maximum_object_bytes);
        if length > maximum {
            return Err(ObjectFailure::new(
                ObjectStoreError::TooLarge {
                    observed: length,
                    maximum,
                },
                work,
            ));
        }
        let copied = length
            .checked_mul(2)
            .ok_or_else(|| ObjectFailure::new(acyclic_fs::WorkError::Overflow.into(), work))?;
        work = work
            .checked_add(WorkCounters {
                backend_read_operations: 1,
                object_bytes_read: length,
                bytes_hashed: length.saturating_add(OBJECT_DIGEST_ENVELOPE_BYTES),
                bytes_copied: copied,
                allocation_operations: 2 * u64::from(length != 0),
                peak_allocation_bytes: copied,
                ..WorkCounters::default()
            })
            .map_err(|error| ObjectFailure::new(error.into(), work))?;
        work.verify(budget)
            .map_err(|error| ObjectFailure::new(error.into(), work))?;
        cancellation
            .check()
            .map_err(|_| ObjectFailure::new(ObjectStoreError::Cancelled, work))?;
        let buffer = JsFuture::from(blob.array_buffer())
            .await
            .map_err(|error| ObjectFailure::new(rejected(&error), work))?;
        let bytes = Bytes::from(Uint8Array::new(&buffer).to_vec());
        if u64::try_from(bytes.len()).ok() != Some(length)
            || object_digest(object_id.kind, &bytes) != object_id.digest
        {
            return Err(ObjectFailure::new(ObjectStoreError::Corrupt, work));
        }
        Ok(ObjectReceipt {
            value: ObjectRead {
                bytes,
                retention: ObjectReadRetention::Owned {
                    logical_bytes: length,
                },
            },
            work,
        })
    }
}

impl AsyncObjectStore for OpfsAcceleratedObjectStore {
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
        if object_digest(object_id.kind, &bytes) != object_id.digest {
            return Err(ObjectFailure::before_work(ObjectStoreError::DigestMismatch));
        }
        let work = put_work(&bytes);
        work.verify(budget)
            .map_err(|error| ObjectFailure::before_work(error.into()))?;
        self.write(object_id, &bytes, cancellation)
            .await
            .map_err(|error| ObjectFailure::new(error, work))?;
        Ok(ObjectReceipt { value: (), work })
    }

    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        if writes.is_empty() {
            return Err(ObjectFailure::before_work(ObjectStoreError::Rejected(
                "object write batch is empty".to_owned(),
            )));
        }
        let mut total = WorkCounters::default();
        for window in writes.chunks(MAX_BATCH_CONCURRENCY) {
            let mut admitted = WorkCounters::default();
            for write in window {
                admitted = admitted
                    .checked_add(put_work(&write.bytes))
                    .map_err(|error| ObjectFailure::new(error.into(), total))?;
            }
            let next = combine_parallel_window(total, admitted)
                .map_err(|error| ObjectFailure::new(error.into(), total))?;
            next.verify(budget)
                .map_err(|error| ObjectFailure::new(error.into(), total))?;
            let results = join_all(window.iter().map(|write| {
                self.put(
                    write.object_id,
                    write.bytes.clone(),
                    put_work(&write.bytes),
                    cancellation,
                )
            }))
            .await;
            total = fold_unit_results(total, results, budget)?;
        }
        Ok(ObjectReceipt {
            value: (),
            work: total,
        })
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<ObjectRead> {
        self.read_exact(object_id, maximum_bytes, budget, cancellation)
            .await
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<Vec<ObjectRead>> {
        if requests.is_empty() {
            return Err(ObjectFailure::before_work(ObjectStoreError::Rejected(
                "object read batch is empty".to_owned(),
            )));
        }
        let mut total = WorkCounters::default();
        let mut values = Vec::new();
        values.try_reserve_exact(requests.len()).map_err(|_| {
            ObjectFailure::before_work(ObjectStoreError::Rejected(
                "object batch result allocation failed".to_owned(),
            ))
        })?;
        for window in requests.chunks(MAX_BATCH_CONCURRENCY) {
            let remaining = total
                .remaining(budget)
                .map_err(|error| ObjectFailure::new(error.into(), total))?;
            let results = join_all(window.iter().map(|request| {
                self.read_exact(
                    request.object_id,
                    request.maximum_bytes,
                    remaining,
                    cancellation,
                )
            }))
            .await;
            let mut window_work = WorkCounters::default();
            for result in results {
                match result {
                    Ok(receipt) => {
                        window_work = combine_parallel_window(window_work, receipt.work)
                            .map_err(|error| ObjectFailure::new(error.into(), total))?;
                        values.push(receipt.value);
                    }
                    Err(failure) => {
                        let spent = combine_parallel_window(window_work, *failure.work)
                            .and_then(|work| combine_sequential(total, work))
                            .unwrap_or(total);
                        return Err(ObjectFailure::new(failure.error, spent));
                    }
                }
            }
            total = combine_sequential(total, window_work)
                .map_err(|error| ObjectFailure::new(error.into(), total))?;
            total
                .verify(budget)
                .map_err(|error| ObjectFailure::new(error.into(), total))?;
        }
        Ok(ObjectReceipt {
            value: values,
            work: total,
        })
    }

    async fn contains(
        &self,
        object_id: ObjectId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<bool> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        let work = WorkCounters {
            object_probes: 1,
            backend_read_operations: 1,
            allocation_operations: 1,
            peak_allocation_bytes: 66,
            ..WorkCounters::default()
        };
        work.verify(budget)
            .map_err(|error| ObjectFailure::before_work(error.into()))?;
        let value =
            match JsFuture::from(self.directory.get_file_handle(&Self::key(object_id))).await {
                Ok(handle) => handle.dyn_into::<FileSystemFileHandle>().is_ok(),
                Err(error) if is_not_found(&error) => false,
                Err(error) => {
                    return Err(ObjectFailure::new(rejected(&error), work));
                }
            };
        cancellation
            .check()
            .map_err(|_| ObjectFailure::new(ObjectStoreError::Cancelled, work))?;
        Ok(ObjectReceipt { value, work })
    }
}

fn fold_unit_results(
    mut total: WorkCounters,
    results: Vec<ObjectResult<()>>,
    budget: WorkBudget,
) -> Result<WorkCounters, ObjectFailure> {
    let mut window = WorkCounters::default();
    for result in results {
        match result {
            Ok(receipt) => {
                window = combine_parallel_window(window, receipt.work)
                    .map_err(|error| ObjectFailure::new(error.into(), total))?;
            }
            Err(failure) => {
                let spent = combine_parallel_window(window, *failure.work)
                    .and_then(|work| combine_sequential(total, work))
                    .unwrap_or(total);
                return Err(ObjectFailure::new(failure.error, spent));
            }
        }
    }
    total = combine_sequential(total, window)
        .map_err(|error| ObjectFailure::new(error.into(), total))?;
    total
        .verify(budget)
        .map_err(|error| ObjectFailure::new(error.into(), total))?;
    Ok(total)
}

fn combine_parallel_window(
    mut left: WorkCounters,
    mut right: WorkCounters,
) -> Result<WorkCounters, acyclic_fs::WorkError> {
    let peak = left
        .peak_allocation_bytes
        .checked_add(right.peak_allocation_bytes)
        .ok_or(acyclic_fs::WorkError::Overflow)?;
    left.peak_allocation_bytes = 0;
    right.peak_allocation_bytes = 0;
    let mut combined = left.checked_add(right)?;
    combined.peak_allocation_bytes = peak;
    Ok(combined)
}

fn combine_sequential(
    left: WorkCounters,
    right: WorkCounters,
) -> Result<WorkCounters, acyclic_fs::WorkError> {
    left.checked_add(right)
}

fn put_work(bytes: &Bytes) -> WorkCounters {
    let length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    WorkCounters {
        backend_write_operations: 1,
        object_bytes_written: length,
        bytes_hashed: length.saturating_add(OBJECT_DIGEST_ENVELOPE_BYTES),
        bytes_copied: length,
        allocation_operations: u64::from(length != 0),
        peak_allocation_bytes: length,
        ..WorkCounters::default()
    }
}

async fn create_directory(
    parent: &FileSystemDirectoryHandle,
    name: &str,
) -> Result<FileSystemDirectoryHandle, OpfsOpenError> {
    let options = FileSystemGetDirectoryOptions::new();
    options.set_create(true);
    JsFuture::from(parent.get_directory_handle_with_options(name, &options))
        .await
        .map_err(|error| OpfsOpenError::Unavailable(js_message(&error)))?
        .dyn_into::<FileSystemDirectoryHandle>()
        .map_err(|_| OpfsOpenError::Unavailable("directory handle has the wrong type".to_owned()))
}

fn namespace_name(database_name: &str) -> String {
    blake3::hash(database_name.as_bytes()).to_hex().to_string()
}
fn rejected(error: &JsValue) -> ObjectStoreError {
    ObjectStoreError::Rejected(js_message(error))
}
fn missing_or_rejected(error: &JsValue) -> ObjectStoreError {
    if is_not_found(error) {
        ObjectStoreError::Missing
    } else {
        rejected(error)
    }
}
fn is_not_found(error: &JsValue) -> bool {
    error
        .dyn_ref::<web_sys::DomException>()
        .is_some_and(|error| error.name() == "NotFoundError")
}
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn exact_file_size(length: f64) -> Option<u64> {
    if !length.is_finite() || length < 0.0 || length.fract() != 0.0 || length > f64::from(u32::MAX)
    {
        return None;
    }
    Some(u64::from(length as u32))
}
fn js_message(error: &JsValue) -> String {
    error
        .as_string()
        .unwrap_or_else(|| "browser storage operation failed".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_fs::ObjectKind;
    use wasm_bindgen_test::*;

    wasm_bindgen_test_configure!(run_in_browser);

    fn object(bytes: &'static [u8]) -> (ObjectId, Bytes) {
        let bytes = Bytes::from_static(bytes);
        (
            ObjectId {
                kind: ObjectKind::BlobChunk,
                digest: object_digest(ObjectKind::BlobChunk, &bytes),
            },
            bytes,
        )
    }

    fn unique_namespace(prefix: &str) -> String {
        format!("{prefix}-{}", js_sys::Date::now().to_bits())
    }

    #[wasm_bindgen_test]
    async fn opfs_is_authoritative_authenticated_and_persistent() {
        let name = unique_namespace("acyclic-fs-opfs-authoritative-v1");
        let store = OpfsAcceleratedObjectStore::open(&name, 1_024)
            .await
            .unwrap_or_else(|error| unreachable!("OPFS open failed: {error}"));
        let (object_id, bytes) = object(b"persistent-opfs-body");
        let cancellation = CancellationToken::new();
        AsyncObjectStore::put(
            &store,
            object_id,
            bytes.clone(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .unwrap_or_else(|failure| unreachable!("OPFS put failed: {}", failure.error));
        drop(store);

        let reopened = OpfsAcceleratedObjectStore::open(&name, 1_024)
            .await
            .unwrap_or_else(|error| unreachable!("OPFS reopen failed: {error}"));
        let read = AsyncObjectStore::read(
            &reopened,
            object_id,
            1_024,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .unwrap_or_else(|failure| unreachable!("OPFS read failed: {}", failure.error));
        assert_eq!(read.value.bytes, bytes);

        reopened
            .write(object_id, &Bytes::from_static(b"corrupt"), &cancellation)
            .await
            .unwrap_or_else(|error| unreachable!("corruption setup failed: {error}"));
        let failure = AsyncObjectStore::read(
            &reopened,
            object_id,
            1_024,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .err()
        .unwrap_or_else(|| unreachable!("corrupt OPFS body was accepted"));
        assert!(matches!(failure.error, ObjectStoreError::Corrupt));
    }

    #[wasm_bindgen_test]
    async fn opfs_batches_preserve_order() {
        let name = unique_namespace("acyclic-fs-opfs-batch-v1");
        let store = OpfsAcceleratedObjectStore::open(&name, 1_024)
            .await
            .unwrap_or_else(|error| unreachable!("OPFS open failed: {error}"));
        let objects = [object(b"first"), object(b"second"), object(b"third")];
        let writes: Vec<_> = objects
            .iter()
            .map(|(object_id, bytes)| ObjectWrite {
                object_id: *object_id,
                bytes: bytes.clone(),
            })
            .collect();
        let cancellation = CancellationToken::new();
        AsyncObjectStore::put_many(&store, &writes, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .unwrap_or_else(|failure| unreachable!("OPFS batch put failed: {}", failure.error));
        let requests: Vec<_> = objects
            .iter()
            .rev()
            .map(|(object_id, _)| ObjectReadRequest {
                object_id: *object_id,
                maximum_bytes: 1_024,
            })
            .collect();
        let reads =
            AsyncObjectStore::read_many(&store, &requests, WorkBudget::UNBOUNDED, &cancellation)
                .await
                .unwrap_or_else(|failure| {
                    unreachable!("OPFS batch read failed: {}", failure.error)
                });
        let actual: Vec<_> = reads.value.into_iter().map(|read| read.bytes).collect();
        let expected: Vec<_> = objects
            .iter()
            .rev()
            .map(|(_, bytes)| bytes.clone())
            .collect();
        assert_eq!(actual, expected);
    }
}
