use super::*;
use crate::Digest;
use crate::ObjectFailure;
use crate::ObjectStoreError;
use crate::async_storage::AsyncObjectStore;
use crate::memory::MemoryObjectStore;
use crate::speculation::{ResidencyHint, ResidencyReason};
use crate::storage::{ObjectRead, ObjectReadRequest};
use crate::test_support::OwnedReadObjectStore;
use acyclic_stream::OwnedBytes;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};

struct PeakRecordingStore {
    calls: AtomicU64,
    peak: u64,
}

impl AsyncObjectStore for PeakRecordingStore {
    async fn put(
        &self,
        _id: ObjectId,
        _bytes: OwnedBytes,
        _work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        unreachable!()
    }
    async fn put_many(
        &self,
        _writes: &[crate::storage::ObjectWrite],
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(ObjectStoreError::NativeUnavailable);
        }
        self.calls.fetch_add(1, Ordering::Relaxed);
        let completed = WorkCounters {
            peak_allocation_bytes: self.peak,
            ..WorkCounters::UNCHARGED
        };
        completed.verify(work.work.remaining(work.budget)?)?;
        work.accept(Ok::<_, ObjectFailure>(crate::OperationReceipt {
            value: (),
            work: completed,
        }))
    }
    async fn read(
        &self,
        _id: ObjectId,
        _max: u64,
        _work: &mut crate::AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        unreachable!()
    }
    async fn read_many(
        &self,
        _requests: &[ObjectReadRequest],
        _work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, crate::AuthorityAllocation), ObjectStoreError> {
        unreachable!()
    }
    async fn contains(
        &self,
        _id: ObjectId,
        _work: &mut crate::AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        unreachable!()
    }
}

struct ScriptedSource {
    bytes: OwnedBytes,
    position: usize,
    maximum_per_read: usize,
    fail_after: Option<usize>,
    cancel_at: Option<usize>,
}

impl AsyncBlobSource for ScriptedSource {
    async fn read<'a>(
        &'a mut self,
        destination: &'a mut [u8],
        _budget: WorkBudget,
        cancellation: &'a CancellationToken,
    ) -> std::io::Result<usize> {
        if self.fail_after.is_some_and(|limit| self.position >= limit) {
            return Err(std::io::Error::other("scripted source failure"));
        }
        if self.position == self.bytes.len() {
            return Ok(0);
        }
        let count = self
            .maximum_per_read
            .min(destination.len())
            .min(self.bytes.len() - self.position);
        destination[..count].copy_from_slice(&self.bytes[self.position..self.position + count]);
        self.position += count;
        if self.cancel_at.is_some_and(|offset| self.position >= offset) {
            cancellation.cancel();
        }
        Ok(count)
    }
}

async fn put<S: AsyncObjectStore>(
    store: &S,
    kind: ObjectKind,
    bytes: OwnedBytes,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let id = ObjectId {
        kind,
        digest: object_digest(kind, &bytes),
    };
    store
        .put(
            id,
            bytes,
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new()),
        )
        .await?;
    Ok(id)
}

#[cfg(not(target_arch = "wasm32"))]
struct NativePartialBlobSource {
    source: std::io::Cursor<&'static [u8]>,
    fail_at: Option<u64>,
    cancel_at: Option<u64>,
}

#[cfg(not(target_arch = "wasm32"))]
impl AsyncBlobSource for NativePartialBlobSource {
    async fn read<'a>(
        &'a mut self,
        _destination: &'a mut [u8],
        _budget: WorkBudget,
        _cancellation: &'a CancellationToken,
    ) -> std::io::Result<usize> {
        Err(std::io::ErrorKind::Unsupported.into())
    }

    async fn read_with_native_work<'a>(
        &'a mut self,
        destination: &'a mut [u8],
        original: &'a crate::NativeMaterializationWork,
        cancellation: &'a CancellationToken,
    ) -> Result<usize, FsError> {
        if self.fail_at.is_some_and(|at| self.source.position() >= at) {
            return Err(BlobBuildError::Source(std::io::ErrorKind::PermissionDenied.into()).into());
        }
        let length = destination.len().min(1);
        let read = self
            .source
            .read_with_native_work(
                destination
                    .get_mut(..length)
                    .ok_or(BlobBuildError::TooLarge)?,
                original,
                cancellation,
            )
            .await?;
        if self
            .cancel_at
            .is_some_and(|at| self.source.position() >= at)
        {
            cancellation.cancel();
        }
        Ok(read)
    }
}
