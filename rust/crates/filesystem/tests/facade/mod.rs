use crate::test_support::allow_publication::allow;
use acyclic_fs::foundation::Epoch;
use acyclic_fs::kernel::{InlineFileData, LogicalName, NameEncoding};
use acyclic_fs::storage::{
    CreateAuthorityOutcome, ObjectId, ObjectKind, ObjectRead, ObjectReadRequest, ObjectStoreError,
    ObjectWrite,
};
use acyclic_fs::*;
use acyclic_stream::OwnedBytes;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[path = "../../tests/support/checkout_fixture.rs"]
mod checkout_fixture;
use checkout_fixture::{config, path, path_parts, writable_pinned};

#[path = "../../tests/support/checkout_manual.rs"]
mod checkout_manual;
use checkout_manual::writable_manual;

#[path = "../../tests/support/checkout_tracking.rs"]
mod checkout_tracking;
use checkout_tracking::tracking;

#[path = "../../tests/support/checkout_live_fixture.rs"]
mod live_fixture;
use live_fixture::writable_live;
#[path = "../../tests/support/checkout_serialized.rs"]
mod checkout_serialized;
use checkout_serialized::serialized_config;

use acyclic_fs::facade::test_snapshot::checkout_state;

#[path = "../../../filesystem-test-support/src/faults.rs"]
mod faults;
use faults::{FaultControl, PostPutObjectStore};

struct CountingObjectStore {
    inner: acyclic_fs::MemoryObjectStore,
    tracked: Arc<std::sync::Mutex<Option<ObjectId>>>,
    reads: Arc<AtomicUsize>,
    unreadable: Arc<std::sync::Mutex<Option<ObjectKind>>>,
}

fn object_context(work: &acyclic_fs::AuthorityWork<'_>) -> Result<(), ObjectStoreError> {
    #[cfg(not(target_arch = "wasm32"))]
    if work.is_native() {
        return Err(ObjectStoreError::NativeUnavailable);
    }
    work.work.remaining(work.budget)?;
    work.cancellation
        .check()
        .map_err(|_| ObjectStoreError::Cancelled)
}
impl CountingObjectStore {
    fn record(&self, id: ObjectId) -> Result<(), ObjectStoreError> {
        if self
            .tracked
            .lock()
            .is_ok_and(|tracked| *tracked == Some(id))
        {
            self.reads.fetch_add(1, Ordering::Relaxed);
        }
        if self
            .unreadable
            .lock()
            .is_ok_and(|kind| *kind == Some(id.kind))
        {
            return Err(ObjectStoreError::Missing);
        }
        Ok(())
    }
}
impl AsyncObjectStore for CountingObjectStore {
    async fn put(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        work: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        object_context(work)?;
        self.inner.put(id, bytes, work).await
    }
    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        work: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        object_context(work)?;
        self.inner.put_many(writes, work).await
    }
    async fn read(
        &self,
        id: ObjectId,
        maximum: u64,
        work: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        object_context(work)?;
        self.record(id)?;
        self.inner.read(id, maximum, work).await
    }
    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        work: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, acyclic_fs::AuthorityAllocation), ObjectStoreError> {
        object_context(work)?;
        for request in requests {
            self.record(request.object_id)?;
        }
        self.inner.read_many(requests, work).await
    }
    async fn contains(
        &self,
        id: ObjectId,
        work: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        object_context(work)?;
        self.inner.contains(id, work).await
    }
}
mod adoption;
mod concurrency;
mod content;
mod creation;
mod detached;
mod fork_proof;
#[cfg(not(target_arch = "wasm32"))]
mod heterogeneous_mount;
mod identity;
mod live;
mod local;
mod selection;
mod tracking;
mod transactions;

mod protocol_helpers;
use protocol_helpers::*;
#[path = "../../tests/support/volume_fixture.rs"]
mod fixtures;
use fixtures::*;
#[path = "../../tests/support/volume_commit.rs"]
mod volume_commit;
#[path = "../../tests/support/volume_read.rs"]
mod volume_read;
use volume_commit::commit;
use volume_read::read;
const B: WorkBudget = WorkBudget::UNBOUNDED;
