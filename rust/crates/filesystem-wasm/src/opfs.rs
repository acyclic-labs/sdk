//! OPFS immutable-body acceleration over `IndexedDB` correctness storage.

use crate::indexed_db::{IndexedDbObjectStore, IndexedDbOpenError};
use acyclic_fs::{
    AsyncObjectStore, CancellationToken, OBJECT_DIGEST_ENVELOPE_BYTES, ObjectFailure, ObjectId,
    ObjectRead, ObjectReadRequest, ObjectReadRetention, ObjectReceipt, ObjectResult,
    ObjectStoreError, WorkBudget, WorkCounters, object_digest,
};
use bytes::Bytes;
use js_sys::Uint8Array;
use std::mem::size_of;
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
    /// `IndexedDB` correctness storage could not be opened.
    #[error(transparent)]
    IndexedDb(#[from] IndexedDbOpenError),
    /// The browser does not expose the required origin-private filesystem API.
    #[error("OPFS is unavailable: {0}")]
    Unavailable(String),
}

/// IndexedDB-correct immutable storage with an OPFS body accelerator.
///
/// `IndexedDB` remains the complete correctness fallback. OPFS files are
/// disposable, authenticated cache entries: a missing or corrupt file falls
/// back to `IndexedDB` and cannot change returned bytes.
pub struct OpfsAcceleratedObjectStore {
    canonical: IndexedDbObjectStore,
    directory: FileSystemDirectoryHandle,
    maximum_object_bytes: u64,
}

enum BatchSlot {
    Hit(ObjectRead),
    Miss,
}

struct CacheBatch {
    slots: Vec<BatchSlot>,
    misses: Vec<ObjectReadRequest>,
    output: Vec<ObjectRead>,
    work: WorkCounters,
    retained: u64,
    item_count: u64,
}

impl OpfsAcceleratedObjectStore {
    /// Opens explicit IndexedDB+OPFS browser storage.
    ///
    /// # Errors
    ///
    /// Fails explicitly when either `IndexedDB` or OPFS is unavailable.
    pub async fn open(
        database_name: &str,
        maximum_object_bytes: u64,
    ) -> Result<Self, OpfsOpenError> {
        let canonical = IndexedDbObjectStore::open(database_name, maximum_object_bytes).await?;
        let window = web_sys::window()
            .ok_or_else(|| OpfsOpenError::Unavailable("window is absent".to_owned()))?;
        let root = JsFuture::from(window.navigator().storage().get_directory())
            .await
            .map_err(|error| OpfsOpenError::Unavailable(js_message(&error)))?
            .dyn_into::<FileSystemDirectoryHandle>()
            .map_err(|_| OpfsOpenError::Unavailable("root handle has the wrong type".to_owned()))?;
        let product = create_directory(&root, "acyclic-fs-v1").await?;
        let namespace = namespace_name(database_name);
        let directory = create_directory(&product, &namespace).await?;
        Ok(Self {
            canonical,
            directory,
            maximum_object_bytes,
        })
    }

    async fn write_cache(
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
                .get_file_handle_with_options(&IndexedDbObjectStore::key(object_id), &options),
        )
        .await
        .map_err(|error| rejected(&error))?
        .dyn_into::<FileSystemFileHandle>()
        .map_err(|_| ObjectStoreError::Corrupt)?;
        let writable = JsFuture::from(handle.create_writable())
            .await
            .map_err(|error| rejected(&error))?
            .dyn_into::<FileSystemWritableFileStream>()
            .map_err(|_| ObjectStoreError::Corrupt)?;
        let write = writable
            .write_with_u8_array(bytes)
            .map_err(|error| rejected(&error))?;
        JsFuture::from(write)
            .await
            .map_err(|error| rejected(&error))?;
        JsFuture::from(writable.close())
            .await
            .map_err(|error| rejected(&error))?;
        cancellation
            .check()
            .map_err(|_| ObjectStoreError::Cancelled)
    }

    async fn read_cache(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<Option<ObjectRead>> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        let key = IndexedDbObjectStore::key(object_id);
        let mut work = WorkCounters {
            object_probes: 1,
            backend_read_operations: 1,
            allocation_operations: 1,
            peak_allocation_bytes: u64::try_from(key.len()).unwrap_or(u64::MAX),
            ..WorkCounters::default()
        };
        work.verify(budget)
            .map_err(|error| ObjectFailure::before_work(error.into()))?;
        let Ok(handle) = JsFuture::from(self.directory.get_file_handle(&key)).await else {
            return Ok(ObjectReceipt { value: None, work });
        };
        let Ok(handle) = handle.dyn_into::<FileSystemFileHandle>() else {
            return Ok(ObjectReceipt { value: None, work });
        };
        work = work
            .checked_add(WorkCounters {
                backend_read_operations: 1,
                ..WorkCounters::default()
            })
            .map_err(|error| ObjectFailure::new(error.into(), work))?;
        work.verify(budget)
            .map_err(|error| ObjectFailure::new(error.into(), work))?;
        let Ok(file) = JsFuture::from(handle.get_file()).await else {
            return Ok(ObjectReceipt { value: None, work });
        };
        let Ok(file) = file.dyn_into::<web_sys::File>() else {
            return Ok(ObjectReceipt { value: None, work });
        };
        let blob: Blob = file.unchecked_into();
        let Some(length) = exact_file_size(blob.size()) else {
            return Ok(ObjectReceipt { value: None, work });
        };
        if length > maximum_bytes || length > self.maximum_object_bytes {
            return Ok(ObjectReceipt { value: None, work });
        }
        let Some(copied) = length.checked_mul(2) else {
            return Ok(ObjectReceipt { value: None, work });
        };
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
        let Ok(buffer) = JsFuture::from(blob.array_buffer()).await else {
            return Ok(ObjectReceipt { value: None, work });
        };
        let bytes = Bytes::from(Uint8Array::new(&buffer).to_vec());
        if u64::try_from(bytes.len()).ok() != Some(length)
            || object_digest(object_id.kind, &bytes) != object_id.digest
        {
            return Ok(ObjectReceipt { value: None, work });
        }
        Ok(ObjectReceipt {
            value: Some(ObjectRead {
                bytes,
                retention: ObjectReadRetention::Owned {
                    logical_bytes: length,
                },
            }),
            work,
        })
    }

    async fn probe_batch_cache(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<CacheBatch, ObjectFailure> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        if requests.is_empty() {
            return Err(ObjectFailure::before_work(ObjectStoreError::Rejected(
                "object read batch is empty".to_owned(),
            )));
        }
        let item_count = u64::try_from(requests.len()).unwrap_or(u64::MAX);
        let container_bytes = item_count.saturating_mul(
            u64::try_from(
                size_of::<BatchSlot>() + size_of::<ObjectReadRequest>() + size_of::<ObjectRead>(),
            )
            .unwrap_or(u64::MAX),
        );
        let mut work = WorkCounters {
            items_examined: item_count,
            allocation_operations: 3,
            peak_allocation_bytes: container_bytes,
            ..WorkCounters::default()
        };
        let mut admission = work;
        admission.items_returned = item_count;
        admission
            .verify(budget)
            .map_err(|error| ObjectFailure::before_work(error.into()))?;
        let mut slots = Vec::new();
        let mut misses = Vec::new();
        let mut output = Vec::new();
        for allocation in [
            slots.try_reserve_exact(requests.len()),
            misses.try_reserve_exact(requests.len()),
            output.try_reserve_exact(requests.len()),
        ] {
            allocation.map_err(|_| {
                ObjectFailure::before_work(ObjectStoreError::Rejected(
                    "object batch bookkeeping allocation failed".to_owned(),
                ))
            })?;
        }
        let mut retained = container_bytes;
        for request in requests {
            let mut remaining = work
                .remaining(budget)
                .map_err(|error| ObjectFailure::new(error.into(), work))?;
            remaining.peak_allocation_bytes = budget
                .peak_allocation_bytes
                .checked_sub(retained)
                .ok_or_else(|| ObjectFailure::new(acyclic_fs::WorkError::Overflow.into(), work))?;
            let cached = self
                .read_cache(
                    request.object_id,
                    request.maximum_bytes,
                    remaining,
                    cancellation,
                )
                .await
                .map_err(|failure| merge_failure(work, *failure.work, failure.error, retained))?;
            work = merge_success(work, cached.work, retained, budget)?;
            if let Some(value) = cached.value {
                retained = retained.saturating_add(retained_bytes(&value));
                work.peak_allocation_bytes = work.peak_allocation_bytes.max(retained);
                slots.push(BatchSlot::Hit(value));
            } else {
                misses.push(*request);
                slots.push(BatchSlot::Miss);
            }
        }
        Ok(CacheBatch {
            slots,
            misses,
            output,
            work,
            retained,
            item_count,
        })
    }

    async fn read_many_accelerated(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<Vec<ObjectRead>> {
        let mut batch = self
            .probe_batch_cache(requests, budget, cancellation)
            .await?;
        let canonical = if batch.misses.is_empty() {
            Vec::new()
        } else {
            let mut remaining = batch
                .work
                .remaining(budget)
                .map_err(|error| ObjectFailure::new(error.into(), batch.work))?;
            remaining.peak_allocation_bytes = budget
                .peak_allocation_bytes
                .checked_sub(batch.retained)
                .ok_or_else(|| {
                    ObjectFailure::new(acyclic_fs::WorkError::Overflow.into(), batch.work)
                })?;
            let receipt = AsyncObjectStore::read_many(
                &self.canonical,
                &batch.misses,
                remaining,
                cancellation,
            )
            .await
            .map_err(|failure| {
                merge_failure(batch.work, *failure.work, failure.error, batch.retained)
            })?;
            batch.work = merge_success(batch.work, receipt.work, batch.retained, budget)?;
            batch.retained = batch.retained.saturating_add(
                receipt
                    .value
                    .iter()
                    .map(retained_bytes)
                    .fold(0_u64, u64::saturating_add),
            );
            batch.work.peak_allocation_bytes = batch.work.peak_allocation_bytes.max(batch.retained);
            receipt.value
        };
        let mut canonical = canonical.into_iter();
        for slot in batch.slots {
            batch.output.push(match slot {
                BatchSlot::Hit(value) => value,
                BatchSlot::Miss => canonical
                    .next()
                    .ok_or_else(|| ObjectFailure::new(ObjectStoreError::Corrupt, batch.work))?,
            });
        }
        if canonical.next().is_some() {
            return Err(ObjectFailure::new(ObjectStoreError::Corrupt, batch.work));
        }
        batch.work = batch
            .work
            .checked_add(WorkCounters {
                items_returned: batch.item_count,
                ..WorkCounters::default()
            })
            .map_err(|error| ObjectFailure::new(error.into(), batch.work))?;
        batch
            .work
            .verify(budget)
            .map_err(|error| ObjectFailure::new(error.into(), batch.work))?;
        Ok(ObjectReceipt {
            value: batch.output,
            work: batch.work,
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
        let canonical = AsyncObjectStore::put(
            &self.canonical,
            object_id,
            bytes.clone(),
            budget,
            cancellation,
        )
        .await?;
        let length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        let cache_work = WorkCounters {
            backend_write_operations: 1,
            object_bytes_written: length,
            bytes_copied: length,
            allocation_operations: u64::from(length != 0),
            peak_allocation_bytes: length,
            ..WorkCounters::default()
        };
        let combined = canonical
            .work
            .checked_add(cache_work)
            .map_err(|error| ObjectFailure::new(error.into(), canonical.work))?;
        combined
            .verify(budget)
            .map_err(|error| ObjectFailure::new(error.into(), canonical.work))?;
        self.write_cache(object_id, &bytes, cancellation)
            .await
            .map_err(|error| ObjectFailure::new(error, canonical.work))?;
        Ok(ObjectReceipt {
            value: (),
            work: combined,
        })
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<ObjectRead> {
        let cached = self
            .read_cache(object_id, maximum_bytes, budget, cancellation)
            .await?;
        if let Some(value) = cached.value {
            return Ok(ObjectReceipt {
                value,
                work: cached.work,
            });
        }
        let remaining = cached
            .work
            .remaining(budget)
            .map_err(|error| ObjectFailure::new(error.into(), cached.work))?;
        let canonical = AsyncObjectStore::read(
            &self.canonical,
            object_id,
            maximum_bytes,
            remaining,
            cancellation,
        )
        .await
        .map_err(|failure| merge_failure(cached.work, *failure.work, failure.error, 0))?;
        let work = merge_success(cached.work, canonical.work, 0, budget)?;
        Ok(ObjectReceipt {
            value: canonical.value,
            work,
        })
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<Vec<ObjectRead>> {
        self.read_many_accelerated(requests, budget, cancellation)
            .await
    }

    async fn contains(
        &self,
        object_id: ObjectId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<bool> {
        AsyncObjectStore::contains(&self.canonical, object_id, budget, cancellation).await
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
    let digest = blake3::hash(database_name.as_bytes());
    let mut name = String::with_capacity(64);
    for byte in digest.as_bytes() {
        use std::fmt::Write;
        let _ = write!(name, "{byte:02x}");
    }
    name
}

fn rejected(error: &JsValue) -> ObjectStoreError {
    ObjectStoreError::Rejected(js_message(error))
}

fn retained_bytes(read: &ObjectRead) -> u64 {
    match read.retention {
        ObjectReadRetention::Shared => 0,
        ObjectReadRetention::Owned { logical_bytes } => logical_bytes,
    }
}

fn merge_success(
    prior: WorkCounters,
    mut nested: WorkCounters,
    retained: u64,
    budget: WorkBudget,
) -> Result<WorkCounters, ObjectFailure> {
    let nested_peak = retained
        .checked_add(nested.peak_allocation_bytes)
        .ok_or_else(|| ObjectFailure::new(acyclic_fs::WorkError::Overflow.into(), prior))?;
    nested.peak_allocation_bytes = 0;
    let mut combined = prior
        .checked_add(nested)
        .map_err(|error| ObjectFailure::new(error.into(), prior))?;
    combined.peak_allocation_bytes = combined.peak_allocation_bytes.max(nested_peak);
    combined
        .verify(budget)
        .map_err(|error| ObjectFailure::new(error.into(), combined))?;
    Ok(combined)
}

fn merge_failure(
    prior: WorkCounters,
    mut nested: WorkCounters,
    error: ObjectStoreError,
    retained: u64,
) -> ObjectFailure {
    let Some(nested_peak) = retained.checked_add(nested.peak_allocation_bytes) else {
        return ObjectFailure::new(acyclic_fs::WorkError::Overflow.into(), prior);
    };
    nested.peak_allocation_bytes = 0;
    let Ok(mut combined) = prior.checked_add(nested) else {
        return ObjectFailure::new(acyclic_fs::WorkError::Overflow.into(), prior);
    };
    combined.peak_allocation_bytes = combined.peak_allocation_bytes.max(nested_peak);
    ObjectFailure::new(error, combined)
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
    use acyclic_fs::{ObjectKind, WorkError};
    use js_sys::{Array, Promise};
    use wasm_bindgen_futures::future_to_promise;
    use wasm_bindgen_test::*;

    wasm_bindgen_test_configure!(run_in_browser);

    #[wasm_bindgen_test]
    async fn opfs_cache_is_authenticated_bounded_and_disposable() {
        let store = OpfsAcceleratedObjectStore::open("acyclic-fs-opfs-live-v1", 1_024)
            .await
            .unwrap_or_else(|error| unreachable!("OPFS test open failed: {error}"));
        let bytes = Bytes::from_static(b"opfs-body");
        let object_id = ObjectId {
            kind: ObjectKind::BlobChunk,
            digest: object_digest(ObjectKind::BlobChunk, &bytes),
        };
        let cancellation = CancellationToken::new();
        AsyncObjectStore::put(
            &store,
            object_id,
            bytes.clone(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .unwrap_or_else(|failure| unreachable!("OPFS test put failed: {}", failure.error));
        let warm = AsyncObjectStore::read(
            &store,
            object_id,
            1_024,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .unwrap_or_else(|failure| unreachable!("OPFS warm read failed: {}", failure.error));
        assert_eq!(warm.value.bytes, bytes);

        let length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        let bounded = AsyncObjectStore::read(
            &store,
            object_id,
            1_024,
            WorkBudget {
                peak_allocation_bytes: length.saturating_mul(2).saturating_sub(1),
                ..WorkBudget::UNBOUNDED
            },
            &cancellation,
        )
        .await
        .err()
        .unwrap_or_else(|| unreachable!("underbudgeted OPFS read unexpectedly succeeded"));
        assert!(matches!(
            bounded.error,
            ObjectStoreError::Work(WorkError::BudgetExceeded {
                counter: "peak_allocation_bytes",
                ..
            })
        ));

        store
            .write_cache(object_id, &Bytes::from_static(b"corrupt"), &cancellation)
            .await
            .unwrap_or_else(|error| unreachable!("OPFS corruption setup failed: {error}"));
        let recovered = AsyncObjectStore::read(
            &store,
            object_id,
            1_024,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .unwrap_or_else(|failure| unreachable!("IndexedDB fallback failed: {}", failure.error));
        assert_eq!(recovered.value.bytes, bytes);

        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let failure =
            AsyncObjectStore::read(&store, object_id, 1_024, WorkBudget::UNBOUNDED, &cancelled)
                .await
                .err()
                .unwrap_or_else(|| unreachable!("cancelled OPFS read unexpectedly succeeded"));
        assert!(matches!(failure.error, ObjectStoreError::Cancelled));
        assert_eq!(*failure.work, WorkCounters::default());
    }

    #[wasm_bindgen_test]
    async fn concurrent_opfs_connections_converge_on_one_authenticated_body() {
        let database_name = "acyclic-fs-opfs-concurrent-v1";
        let first = OpfsAcceleratedObjectStore::open(database_name, 1_024)
            .await
            .unwrap_or_else(|error| unreachable!("first OPFS open failed: {error}"));
        let second = OpfsAcceleratedObjectStore::open(database_name, 1_024)
            .await
            .unwrap_or_else(|error| unreachable!("second OPFS open failed: {error}"));
        let bytes = Bytes::from_static(b"concurrent-opfs-body");
        let object_id = ObjectId {
            kind: ObjectKind::BlobChunk,
            digest: object_digest(ObjectKind::BlobChunk, &bytes),
        };
        let first_bytes = bytes.clone();
        let second_bytes = bytes.clone();
        let first_put = future_to_promise(async move {
            AsyncObjectStore::put(
                &first,
                object_id,
                first_bytes,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map(|_| JsValue::UNDEFINED)
            .map_err(|failure| JsValue::from_str(&failure.error.to_string()))
        });
        let second_put = future_to_promise(async move {
            AsyncObjectStore::put(
                &second,
                object_id,
                second_bytes,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map(|_| JsValue::UNDEFINED)
            .map_err(|failure| JsValue::from_str(&failure.error.to_string()))
        });
        let puts = Array::new();
        puts.push(&first_put);
        puts.push(&second_put);
        JsFuture::from(Promise::all(&puts))
            .await
            .unwrap_or_else(|error| unreachable!("concurrent OPFS puts failed: {error:?}"));
        let reader = OpfsAcceleratedObjectStore::open(database_name, 1_024)
            .await
            .unwrap_or_else(|error| unreachable!("reader OPFS open failed: {error}"));
        let read = AsyncObjectStore::read(
            &reader,
            object_id,
            1_024,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .unwrap_or_else(|failure| unreachable!("concurrent OPFS read failed: {}", failure.error));
        assert_eq!(read.value.bytes, bytes);
    }
}
