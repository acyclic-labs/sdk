//! Authoritative origin-private immutable object storage.

use acyclic_fs::storage::ObjectWrite;
use acyclic_stream::OwnedBytes;
use futures::future::join_all;
use thiserror::Error;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    Blob, FileSystemDirectoryHandle, FileSystemFileHandle, FileSystemGetDirectoryOptions,
    FileSystemGetFileOptions, FileSystemWritableFileStream,
};
use {
    acyclic_fs::{
        AsyncObjectStore, CancellationToken, ObjectFailure, ObjectRead, ObjectReadRequest,
        ObjectReadRetention, ObjectResult, ObjectStoreError, object_digest,
    },
    acyclic_fs_format::object_reference::ObjectId,
    acyclic_fs_work::performance::{WorkBudget, WorkCounters},
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

    fn with_key<T>(
        object_id: ObjectId,
        operation: impl FnOnce(&str) -> T,
    ) -> Result<T, ObjectStoreError> {
        const fn hex_digit(nibble: u8) -> u8 {
            if nibble < 10 {
                b'0' + nibble
            } else {
                b'a' + nibble - 10
            }
        }
        let mut key = [b'0'; 66];
        key[0] = b'0' + object_id.kind.canonical_tag();
        key[1] = b':';
        let (pairs, _) = key[2..].as_chunks_mut::<2>();
        for (byte, [high, low]) in object_id.digest.as_bytes().iter().zip(pairs) {
            *high = hex_digit(byte >> 4);
            *low = hex_digit(byte & 15);
        }
        let key = std::str::from_utf8(&key).map_err(|_| ObjectStoreError::Corrupt)?;
        Ok(operation(key))
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
        let handle = JsFuture::from(Self::with_key(object_id, |key| {
            self.directory.get_file_handle_with_options(key, &options)
        })?)
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
}

impl acyclic_fs::browser::Reader for OpfsAcceleratedObjectStore {
    async fn read(
        &self,
        request: &ObjectReadRequest,
        accounting: &std::cell::RefCell<
            acyclic_fs::kernel::allocation::BorrowedKernelAccounting<'_>,
        >,
        cancellation: &CancellationToken,
    ) -> Result<ObjectRead, ObjectStoreError> {
        accounting
            .borrow_mut()
            .charge(WorkCounters {
                object_probes: 1,
                ..WorkCounters::UNCHARGED
            })
            .map_err(acyclic_fs::ObjectStoreError::from)?;
        let handle = acyclic_fs::browser::promise(
            || Self::with_key(request.object_id, |key| self.directory.get_file_handle(key)),
            accounting,
            cancellation,
        )
        .await?
        .map_err(|error| missing_or_rejected(&error))?
        .dyn_into::<FileSystemFileHandle>()
        .map_err(|_| ObjectStoreError::Corrupt)?;
        let file = acyclic_fs::browser::promise(|| Ok(handle.get_file()), accounting, cancellation)
            .await?
            .map_err(|error| rejected(&error))?
            .dyn_into::<web_sys::File>()
            .map_err(|_| ObjectStoreError::Corrupt)?;
        let blob: Blob = file.unchecked_into();
        let length = acyclic_fs::browser::blob_length(&blob)?;
        let maximum = request.maximum_bytes.min(self.maximum_object_bytes);
        if length > maximum {
            return Err(ObjectStoreError::TooLarge {
                observed: length,
                maximum,
            });
        }
        let bytes =
            acyclic_fs::browser::blob_bytes(&blob, length, accounting, cancellation).await?;
        accounting
            .borrow_mut()
            .charge(WorkCounters {
                bytes_hashed: length,
                ..WorkCounters::UNCHARGED
            })
            .map_err(acyclic_fs::ObjectStoreError::from)?;
        if object_digest(&bytes) != request.object_id.digest {
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
                let observed = match failure.work() {
                    Ok(work) => *work,
                    Err(observation) => {
                        return Err(ObjectFailure::without_receipt(
                            first_error.unwrap_or(failure.error),
                            observation,
                        ));
                    }
                };
                window = combine_parallel_window(window, observed)
                    .map_err(|error| ObjectFailure::new(error.into(), total))?;
                if first_error.is_none() {
                    first_error = Some(failure.error);
                }
            }
        }
    }
    total = total
        .checked_add(window)
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
) -> Result<WorkCounters, acyclic_fs_work::performance::WorkError> {
    let peak = left
        .peak_allocation_bytes
        .checked_add(right.peak_allocation_bytes)
        .ok_or(acyclic_fs_work::performance::WorkError::Overflow)?;
    left.peak_allocation_bytes = 0;
    right.peak_allocation_bytes = 0;
    let mut combined = left.checked_add(right)?;
    combined.peak_allocation_bytes = peak;
    Ok(combined)
}

fn allocation_bytes<T>(count: usize) -> Result<u64, acyclic_fs_work::performance::WorkError> {
    count
        .checked_mul(size_of::<T>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or(acyclic_fs_work::performance::WorkError::Overflow)
}

fn join_frames_bytes<F: Future>(
    _future: &F,
    count: usize,
) -> Result<u64, acyclic_fs_work::performance::WorkError> {
    // Account for pinned futures and completed outputs in the small join_all window.
    allocation_bytes::<futures::future::MaybeDone<F>>(count)
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
fn js_message(error: &JsValue) -> String {
    error
        .as_string()
        .unwrap_or_else(|| "browser storage operation failed".to_owned())
}
