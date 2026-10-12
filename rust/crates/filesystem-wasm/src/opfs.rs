//! Authoritative origin-private immutable object storage.

use acyclic_fs::storage::ObjectWrite;
use acyclic_fs::{
    AsyncObjectStore, CancellationToken, OBJECT_DIGEST_ENVELOPE_BYTES, ObjectFailure, ObjectId,
    ObjectRead, ObjectReadRequest, ObjectReadRetention, ObjectResult,
    ObjectStoreError, WorkBudget, WorkCounters, object_digest,
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
            || object_digest(object_id.kind, &bytes) != object_id.digest
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
        let work = prepared.work
            .checked_add(materialize_work(length)
                .map_err(|error| ObjectFailure::new(error.into(), prepared.work))?)
            .map_err(|error| ObjectFailure::new(error.into(), prepared.work))?;
        work.verify(budget)
            .map_err(|error| ObjectFailure::new(error.into(), prepared.work))?;
        Self::materialize_read(object_id, blob, length, work, cancellation).await
    }
}

impl AsyncObjectStore for OpfsAcceleratedObjectStore {
    async fn put(
        &self,
        object_id: ObjectId,
        bytes: OwnedBytes,
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
        let work = put_work(&bytes);
        work.verify(budget)
            .map_err(|error| ObjectFailure::before_work(error.into()))?;
        if object_digest(object_id.kind, &bytes) != object_id.digest {
            return Err(ObjectFailure::new(ObjectStoreError::DigestMismatch, work));
        }
        self.write(object_id, &bytes, cancellation)
            .await
            .map_err(|error| ObjectFailure::new(error, work))?;
        Ok(acyclic_fs::OperationReceipt { value: (), work })
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
                admitted = combine_parallel_window(admitted, put_work(&write.bytes))
                    .map_err(|error| ObjectFailure::new(error.into(), total))?;
            }
            let mut futures = window.iter().map(|write| {
                self.put(write.object_id, write.bytes.clone(), put_work(&write.bytes), cancellation)
            });
            let first = futures.next().ok_or_else(|| ObjectFailure::new(
                ObjectStoreError::Rejected("OPFS write window is empty".to_owned()), total,
            ))?;
            let frames = join_frames_bytes(&first, window.len())
                .and_then(|bytes| bytes.checked_add(allocation_bytes::<ObjectResult<()>>(window.len())?)
                    .ok_or(acyclic_fs::WorkError::Overflow))
                .map_err(|error| ObjectFailure::new(error.into(), total))?;
            admitted.peak_allocation_bytes = admitted.peak_allocation_bytes.checked_add(frames)
                .ok_or_else(|| ObjectFailure::new(acyclic_fs::WorkError::Overflow.into(), total))?;
            let frame_work = WorkCounters {
                allocation_operations: 2,
                peak_allocation_bytes: admitted.peak_allocation_bytes,
                ..WorkCounters::default()
            };
            let next = total.checked_add(frame_work)
                .and_then(|work| work.checked_add(admitted))
                .map_err(|error| ObjectFailure::new(error.into(), total))?;
            next.verify(budget)
                .map_err(|error| ObjectFailure::new(error.into(), total))?;
            total = total.checked_add(frame_work)
                .map_err(|error| ObjectFailure::new(error.into(), total))?;
            let results = join_all(std::iter::once(first).chain(futures)).await;
            total = fold_unit_results(total, results, budget)?;
        }
        Ok(acyclic_fs::OperationReceipt {
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
        let vector_bytes = allocation_bytes::<ObjectRead>(requests.len())
            .map_err(|error| ObjectFailure::before_work(error.into()))?;
        let mut total = WorkCounters {
            allocation_operations: 1,
            peak_allocation_bytes: vector_bytes,
            ..WorkCounters::default()
        };
        total.verify(budget)
            .map_err(|error| ObjectFailure::before_work(error.into()))?;
        let mut values = Vec::new();
        values.try_reserve_exact(requests.len()).map_err(|_| {
            ObjectFailure::new(ObjectStoreError::Rejected(
                "object batch result allocation failed".to_owned(),
            ), total)
        })?;
        let mut retained = vector_bytes;
        for window in requests.chunks(MAX_BATCH_CONCURRENCY) {
            // Fetch file handles and lengths before any ArrayBuffer allocation.
            // Prior output carriers remain live throughout the next window.
            let handles_bytes = allocation_bytes::<(ObjectId, Blob, u64)>(window.len())
                .map_err(|error| ObjectFailure::new(error.into(), total))?;
            let results_bytes = allocation_bytes::<ObjectResult<ObjectRead>>(window.len())
                .map_err(|error| ObjectFailure::new(error.into(), total))?;
            let live = retained.checked_add(handles_bytes)
                .and_then(|value| value.checked_add(results_bytes))
                .ok_or_else(|| ObjectFailure::new(acyclic_fs::WorkError::Overflow.into(), total))?;
            let prospective = total.checked_add(WorkCounters {
                allocation_operations: 2,
                peak_allocation_bytes: live,
                ..WorkCounters::default()
            }).map_err(|error| ObjectFailure::new(error.into(), total))?;
            prospective.verify(budget)
                .map_err(|error| ObjectFailure::new(error.into(), total))?;
            total = prospective;
            let mut handles = Vec::new();
            handles.try_reserve_exact(window.len()).map_err(|_| {
                ObjectFailure::new(ObjectStoreError::Rejected(
                    "OPFS batch handle allocation failed".to_owned(),
                ), total)
            })?;
            for request in window {
                let mut remaining = total.remaining(budget)
                    .map_err(|error| ObjectFailure::new(error.into(), total))?;
                remaining.peak_allocation_bytes = remaining.peak_allocation_bytes.saturating_sub(live);
                let prepared = self.prepare_read(
                    request.object_id, request.maximum_bytes, remaining, cancellation,
                ).await.map_err(|failure| {
                    let mut failed = *failure.work;
                    failed.peak_allocation_bytes = live.saturating_add(failed.peak_allocation_bytes);
                    let spent = total.checked_add(failed).unwrap_or(total);
                    ObjectFailure::new(failure.error, spent)
                })?;
                let mut work = prepared.work;
                work.peak_allocation_bytes = live.checked_add(work.peak_allocation_bytes)
                    .ok_or_else(|| ObjectFailure::new(acyclic_fs::WorkError::Overflow.into(), total))?;
                total = total.checked_add(work)
                    .map_err(|error| ObjectFailure::new(error.into(), total))?;
                let (blob, length) = prepared.value;
                handles.push((request.object_id, blob, length));
            }
            let mut window_work = WorkCounters::default();
            let mut window_retained = 0_u64;
            for (_, _, length) in &handles {
                window_work = combine_parallel_window(window_work, materialize_work(*length)
                    .map_err(|error| ObjectFailure::new(error.into(), total))?)
                    .map_err(|error| ObjectFailure::new(error.into(), total))?;
                window_retained = window_retained.checked_add(*length)
                    .ok_or_else(|| ObjectFailure::new(acyclic_fs::WorkError::Overflow.into(), total))?;
            }
            let mut futures = handles.into_iter().map(|(object_id, blob, length)| {
                Self::materialize_read(object_id, blob, length, WorkCounters::default(), cancellation)
            });
            let first = futures.next().ok_or_else(|| ObjectFailure::new(
                ObjectStoreError::Rejected("OPFS read window is empty".to_owned()), total,
            ))?;
            let frames = join_frames_bytes(&first, window.len())
                .map_err(|error| ObjectFailure::new(error.into(), total))?;
            window_work.allocation_operations = window_work.allocation_operations.checked_add(1)
                .ok_or_else(|| ObjectFailure::new(acyclic_fs::WorkError::Overflow.into(), total))?;
            window_work.peak_allocation_bytes = window_work.peak_allocation_bytes.checked_add(frames)
                .ok_or_else(|| ObjectFailure::new(acyclic_fs::WorkError::Overflow.into(), total))?;
            window_work.peak_allocation_bytes = live.checked_add(window_work.peak_allocation_bytes)
                .ok_or_else(|| ObjectFailure::new(acyclic_fs::WorkError::Overflow.into(), total))?;
            let admitted = total.checked_add(window_work)
                .map_err(|error| ObjectFailure::new(error.into(), total))?;
            admitted.verify(budget)
                .map_err(|error| ObjectFailure::new(error.into(), total))?;
            let results = join_all(std::iter::once(first).chain(futures)).await;
            // Every future has completed, including after a corrupt/failed blob.
            // Preserve the admitted window's live buffers and all performed work.
            total = admitted;
            let mut first_error = None;
            for result in results {
                match result {
                    Ok(receipt) => values.push(receipt.value),
                    Err(failure) if first_error.is_none() => first_error = Some(failure.error),
                    Err(_) => {}
                }
            }
            if let Some(error) = first_error {
                return Err(ObjectFailure::new(error, total));
            }
            retained = retained.checked_add(window_retained)
                .ok_or_else(|| ObjectFailure::new(acyclic_fs::WorkError::Overflow.into(), total))?;
        }
        Ok(acyclic_fs::OperationReceipt {
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
        Ok(acyclic_fs::OperationReceipt { value, work })
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
    count.checked_mul(size_of::<T>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or(acyclic_fs::WorkError::Overflow)
}

fn join_frames_bytes<F: Future>(
    _future: &F,
    count: usize,
) -> Result<u64, acyclic_fs::WorkError> {
    // Account for pinned futures and completed outputs in the small join_all window.
    allocation_bytes::<futures::future::MaybeDone<F>>(count)
}

fn materialize_work(length: u64) -> Result<WorkCounters, acyclic_fs::WorkError> {
    let copied = length.checked_mul(2).ok_or(acyclic_fs::WorkError::Overflow)?;
    Ok(WorkCounters {
        backend_read_operations: 1,
        object_bytes_read: length,
        bytes_hashed: length.checked_add(OBJECT_DIGEST_ENVELOPE_BYTES)
            .ok_or(acyclic_fs::WorkError::Overflow)?,
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

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_fs::ObjectKind;
    use wasm_bindgen_test::*;

    wasm_bindgen_test_configure!(run_in_browser);

    fn object(bytes: &'static [u8]) -> (ObjectId, OwnedBytes) {
        let bytes = OwnedBytes::from(bytes.to_vec());
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
            .write(object_id, &OwnedBytes::from(b"corrupt".to_vec()), &cancellation)
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

    #[wasm_bindgen_test]
    async fn opfs_batch_admits_retained_carriers_before_materializing_next_window() {
        let name = unique_namespace("acyclic-fs-opfs-retained-carriers-v1");
        let store = OpfsAcceleratedObjectStore::open(&name, 1_024)
            .await
            .unwrap_or_else(|error| unreachable!("OPFS open failed: {error}"));
        let writes: Vec<_> = (0_u8..32).map(|index| {
            let bytes = OwnedBytes::from(vec![index; 1_024]);
            ObjectWrite {
                object_id: ObjectId {
                    kind: ObjectKind::BlobChunk,
                    digest: object_digest(ObjectKind::BlobChunk, &bytes),
                },
                bytes,
            }
        }).collect();
        let requests: Vec<_> = writes.iter().map(|write| ObjectReadRequest {
            object_id: write.object_id,
            maximum_bytes: 1_024,
        }).collect();
        let cancellation = CancellationToken::new();
        store.put_many(&writes, WorkBudget::UNBOUNDED, &cancellation).await
            .unwrap_or_else(|failure| unreachable!("OPFS setup failed: {}", failure.error));
        let read = store.read_many(&requests, WorkBudget::UNBOUNDED, &cancellation).await
            .unwrap_or_else(|failure| unreachable!("OPFS read failed: {}", failure.error));
        let required = allocation_bytes::<ObjectRead>(requests.len())
            .unwrap_or_else(|error| unreachable!("vector size failed: {error}"))
            + allocation_bytes::<(ObjectId, Blob, u64)>(16)
                .unwrap_or_else(|error| unreachable!("handle size failed: {error}"))
            + allocation_bytes::<ObjectResult<ObjectRead>>(16)
                .unwrap_or_else(|error| unreachable!("result size failed: {error}"))
            + 16 * 1_024 + 16 * 2 * 1_024;
        assert!(read.work.peak_allocation_bytes >= required);
        let required = read.work.peak_allocation_bytes;
        let cloned = read.value[0].bytes.clone();
        drop(read);
        assert_eq!(cloned.as_ref(), writes[0].bytes.as_ref());
        let denied = store.read_many(&requests, WorkBudget {
            peak_allocation_bytes: required - 1,
            ..WorkBudget::UNBOUNDED
        }, &cancellation).await
            .err().unwrap_or_else(|| unreachable!("retained carriers escaped admission"));
        assert_eq!(denied.work.object_bytes_read, 16 * 1_024);
        assert!(matches!(denied.error, ObjectStoreError::Work(_)));

        // Complete sibling reads after failure to retain their work and allocation peak.
        store.write(writes[16].object_id, &OwnedBytes::from(vec![255; 1_024]), &cancellation)
            .await.unwrap_or_else(|error| unreachable!("corruption setup failed: {error}"));
        let corrupt = store.read_many(&requests, WorkBudget::UNBOUNDED, &cancellation).await
            .err().unwrap_or_else(|| unreachable!("corrupt OPFS body was accepted"));
        assert!(matches!(corrupt.error, ObjectStoreError::Corrupt));
        assert_eq!(corrupt.work.object_bytes_read, 32 * 1_024);
        assert!(corrupt.work.peak_allocation_bytes >= required);
    }
}
