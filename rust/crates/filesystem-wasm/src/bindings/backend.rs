use super::{IndexedDbObjectStore, OpfsAcceleratedObjectStore};
use acyclic_fs::storage::{HashedObject, ObjectWrite};
use acyclic_fs::{AsyncObjectStore, LogicalObjectStore, ObjectId, ObjectReadRequest};
use acyclic_stream::OwnedBytes;

pub(super) type BrowserAuthority =
    acyclic_fs::StreamAuthorityStore<dyn acyclic_stream::StreamProvider>;

pub(super) enum BrowserObjects {
    IndexedDb(IndexedDbObjectStore),
    Opfs(OpfsAcceleratedObjectStore),
    Memory(LogicalObjectStore<acyclic_objects::v1::MemoryObjects>),
}

macro_rules! forward {
    ($store:expr, $method:ident $args:tt, $($variant:ident),+) => {
        match $store {
            $(Self::$variant(store) => store.$method $args.await),+
        }
    };
}

impl AsyncObjectStore for BrowserObjects {
    async fn put(
        &self,
        object_id: ObjectId,
        bytes: OwnedBytes,
        context: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<(), acyclic_fs::ObjectStoreError> {
        forward!(
            self,
            put(object_id, bytes, context),
            IndexedDb,
            Opfs,
            Memory
        )
    }

    async fn put_hashed(
        &self,
        object: HashedObject,
        context: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<(), acyclic_fs::ObjectStoreError> {
        forward!(self, put_hashed(object, context), IndexedDb, Opfs, Memory)
    }

    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        context: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<(), acyclic_fs::ObjectStoreError> {
        forward!(self, put_many(writes, context), IndexedDb, Opfs, Memory)
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        context: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<acyclic_fs::ObjectRead, acyclic_fs::ObjectStoreError> {
        forward!(
            self,
            read(object_id, maximum_bytes, context),
            IndexedDb,
            Opfs,
            Memory
        )
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        context: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<
        (Vec<acyclic_fs::ObjectRead>, acyclic_fs::AuthorityAllocation),
        acyclic_fs::ObjectStoreError,
    > {
        forward!(self, read_many(requests, context), IndexedDb, Opfs, Memory)
    }

    async fn contains(
        &self,
        object_id: ObjectId,
        context: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<bool, acyclic_fs::ObjectStoreError> {
        forward!(self, contains(object_id, context), IndexedDb, Opfs, Memory)
    }
}
