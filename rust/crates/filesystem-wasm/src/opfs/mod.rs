//! Authoritative origin-private immutable object storage.

use acyclic_fs::storage::ObjectWrite;
use acyclic_fs::{
    AsyncObjectStore, CancellationToken, ObjectFailure, ObjectId,
    ObjectRead, ObjectReadRequest, ObjectReadRetention, ObjectResult, ObjectStoreError, WorkBudget,
    WorkCounters, object_digest,
};
use acyclic_stream::OwnedBytes;
use futures::future::join_all;
use js_sys::Uint8Array;
use thiserror::Error;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    Blob, FileSystemDirectoryHandle, FileSystemFileHandle, FileSystemGetDirectoryOptions,
    FileSystemGetFileOptions, FileSystemWritableFileStream,
};

mod store;
#[cfg(test)]
mod tests;

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
        // Window and Worker both expose navigator.storage.
        let navigator = js_sys::Reflect::get(&js_sys::global(), &JsValue::from_str("navigator"))
            .map_err(|error| OpfsOpenError::Unavailable(js_message(&error)))?;
        let storage = js_sys::Reflect::get(&navigator, &JsValue::from_str("storage"))
            .map_err(|error| OpfsOpenError::Unavailable(js_message(&error)))?
            .dyn_into::<web_sys::StorageManager>()
            .map_err(|_| OpfsOpenError::Unavailable("storage manager is absent".to_owned()))?;
        let root = JsFuture::from(storage.get_directory())
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
        bytes: &OwnedBytes,
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

    async fn prepare_read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<(Blob, u64)> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        let key_bytes = 66;
        let work = WorkCounters {
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
        Ok(acyclic_fs::OperationReceipt {
            value: (blob, length),
            work,
        })
    }

    async fn materialize_read(
        object_id: ObjectId,
        blob: Blob,
        length: u64,
        work: WorkCounters,
        cancellation: &CancellationToken,
    ) -> ObjectResult<ObjectRead> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::new(ObjectStoreError::Cancelled, work))?;
        let buffer = JsFuture::from(blob.array_buffer())
            .await
            .map_err(|error| ObjectFailure::new(rejected(&error), work))?;
        let bytes = OwnedBytes::from(Uint8Array::new(&buffer).to_vec());
        if u64::try_from(bytes.len()).ok() != Some(length)
            || object_digest(&bytes) != object_id.digest
        {
            return Err(ObjectFailure::new(ObjectStoreError::Corrupt, work));
        }
        Ok(acyclic_fs::OperationReceipt {
            value: ObjectRead {
                bytes,
                retention: ObjectReadRetention::Owned {
                    logical_bytes: length,
                },
            },
            work,
        })
    }

    async fn read_exact(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<ObjectRead> {
        let prepared = self
            .prepare_read(object_id, maximum_bytes, budget, cancellation)
            .await?;
        let (blob, length) = prepared.value;
        let work = prepared
            .work
            .checked_add(
                materialize_work(length)
                    .map_err(|error| ObjectFailure::new(error.into(), prepared.work))?,
            )
            .map_err(|error| ObjectFailure::new(error.into(), prepared.work))?;
        work.verify(budget)
            .map_err(|error| ObjectFailure::new(error.into(), prepared.work))?;
        Self::materialize_read(object_id, blob, length, work, cancellation).await
    }
}

fn fold_unit_results(
    mut total: WorkCounters,
    results: Vec<ObjectResult<()>>,
    budget: WorkBudget,
) -> Result<WorkCounters, ObjectFailure> {
    let mut window = WorkCounters::default();
    let mut first_error = None;
    for result in results {
        match result {
            Ok(receipt) => {
                window = combine_parallel_window(window, receipt.work)
                    .map_err(|error| ObjectFailure::new(error.into(), total))?;
            }
            Err(failure) => {
                window = combine_parallel_window(window, *failure.work)
                    .map_err(|error| ObjectFailure::new(error.into(), total))?;
                if first_error.is_none() {
                    first_error = Some(failure.error);
                }
            }
        }
    }
    total = combine_sequential(total, window)
        .map_err(|error| ObjectFailure::new(error.into(), total))?;
    total
        .verify(budget)
        .map_err(|error| ObjectFailure::new(error.into(), total))?;
    if let Some(error) = first_error {
        return Err(ObjectFailure::new(error, total));
    }
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

fn allocation_bytes<T>(count: usize) -> Result<u64, acyclic_fs::WorkError> {
    count
        .checked_mul(size_of::<T>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or(acyclic_fs::WorkError::Overflow)
}

fn join_frames_bytes<F: Future>(_future: &F, count: usize) -> Result<u64, acyclic_fs::WorkError> {
    // Account for pinned futures and completed outputs in the small join_all window.
    allocation_bytes::<futures::future::MaybeDone<F>>(count)
}

fn materialize_work(length: u64) -> Result<WorkCounters, acyclic_fs::WorkError> {
    let copied = length
        .checked_mul(2)
        .ok_or(acyclic_fs::WorkError::Overflow)?;
    Ok(WorkCounters {
        backend_read_operations: 1,
        object_bytes_read: length,
        bytes_hashed: length,
        bytes_copied: copied,
        allocation_operations: 2 * u64::from(length != 0),
        peak_allocation_bytes: copied,
        ..WorkCounters::default()
    })
}

fn put_work(bytes: &OwnedBytes) -> WorkCounters {
    let length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    WorkCounters {
        backend_write_operations: 1,
        object_bytes_written: length,
        bytes_hashed: length,
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
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "the checks above bound length to a non-negative integer no larger than u32::MAX"
)]
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
