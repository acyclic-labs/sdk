use super::{IndexedDbAuthorityStore, IndexedDbObjectStore, OpfsAcceleratedObjectStore};
use acyclic_fs::storage::{HashedObject, ObjectWrite};
use acyclic_fs::{
    AppendOutcome, AsyncAuthorityStore, AsyncObjectStore, AuthorityId, AuthorityResult,
    AuthorityStoreError, CancellationToken, CreateAuthorityOutcome, Epoch, FenceOutcome, FsResult,
    GenerationForkSource, GuardedAppend, Head, LogicalObjectStore, ObjectId, ObjectRead,
    ObjectReadRequest, ObjectResult, OperationId, OperationWindowSnapshot, OperationWindowStore,
    PublicationPermit, PublicationReservation, ReplayLimit, ReservationOutcome, Sequence,
    WorkBudget, WorkspaceForkCommit, WorkspaceForkOutcome, WorkspaceId,
};
use acyclic_stream::{OwnedBytes, ProviderTask};
use std::future::Future;

#[derive(Clone)]
pub(super) enum BrowserAuthority {
    IndexedDb(IndexedDbAuthorityStore),
    Memory(acyclic_fs::StreamAuthorityStore<acyclic_stream::MemoryStream>),
}

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

impl AsyncAuthorityStore for BrowserAuthority {
async fn create_authority<G: acyclic_fs::AuthorityGuard>(&self, authority: acyclic_fs::AuthorityId, epoch: acyclic_fs::Epoch, work: &mut acyclic_fs::AuthorityWork<'_>, guard: Option<&mut G>) -> Result<acyclic_fs::CreateAuthorityOutcome, acyclic_fs::AuthorityError<G::Error>> {
forward!(self, create_authority(authority, epoch, work, guard), IndexedDb, Memory)
}
async fn fork_authority<G: acyclic_fs::AuthorityGuard>(&self, source: acyclic_fs::GenerationForkSource, destination: acyclic_fs::AuthorityId, operation: acyclic_fs::OperationId, work: &mut acyclic_fs::AuthorityWork<'_>, guard: Option<&mut G>) -> Result<acyclic_fs::CreateAuthorityOutcome, acyclic_fs::AuthorityError<G::Error>> {
forward!(self, fork_authority(source, destination, operation, work, guard), IndexedDb, Memory)
}
async fn create_first_record<G: acyclic_fs::AuthorityGuard>(&self, authority: acyclic_fs::AuthorityId, commit: acyclic_fs::ProposedCommit, work: &mut acyclic_fs::AuthorityWork<'_>, guard: Option<&mut G>) -> Result<bool, acyclic_fs::AuthorityError<G::Error>> {
forward!(self, create_first_record(authority, commit, work, guard), IndexedDb, Memory)
}
async fn fork_workspace<G: acyclic_fs::AuthorityGuard>(&self, fork: acyclic_fs::WorkspaceForkCommit, permit: acyclic_fs::PublicationPermit, work: &mut acyclic_fs::AuthorityWork<'_>, guard: Option<&mut G>) -> Result<acyclic_fs::WorkspaceForkOutcome, acyclic_fs::AuthorityError<G::Error>> {
forward!(self, fork_workspace(fork, permit, work, guard), IndexedDb, Memory)
}
async fn compare_and_append<G: acyclic_fs::AuthorityGuard>(&self, request: acyclic_fs::GuardedAppend, work: &mut acyclic_fs::AuthorityWork<'_>, guard: Option<&mut G>) -> Result<(acyclic_fs::AppendOutcome, acyclic_fs::AuthorityAllocation), acyclic_fs::AuthorityError<G::Error>,
    > {
forward!(self, compare_and_append(request, work, guard), IndexedDb, Memory)
}
async fn head(&self, authority: acyclic_fs::AuthorityId, work: &mut acyclic_fs::AuthorityWork<'_>) -> Result<acyclic_fs::Head, acyclic_fs::AuthorityStoreError> {
forward!(self, head(authority, work), IndexedDb, Memory)
}
async fn replay(&self, authority: acyclic_fs::AuthorityId, after: acyclic_fs::Sequence, limit: acyclic_fs::ReplayLimit, work: &mut acyclic_fs::AuthorityWork<'_>) -> Result<(Vec<acyclic_fs::DurableCommit>, acyclic_fs::AuthorityAllocation), acyclic_fs::AuthorityStoreError,
    > {
forward!(self, replay(authority, after, limit, work), IndexedDb, Memory)
}
async fn fence(&self, authority: acyclic_fs::AuthorityId, expected: acyclic_fs::Head, work: &mut acyclic_fs::AuthorityWork<'_>) -> Result<acyclic_fs::FenceOutcome, acyclic_fs::AuthorityStoreError> {
forward!(self, fence(authority, expected, work), IndexedDb, Memory)
}
async fn find_operation(&self, authority: acyclic_fs::AuthorityId, operation: acyclic_fs::OperationId, work: &mut acyclic_fs::AuthorityWork<'_>) -> Result<(Option<acyclic_fs::DurableCommit>, acyclic_fs::AuthorityAllocation), acyclic_fs::AuthorityStoreError,
    > {
forward!(self, find_operation(authority, operation, work), IndexedDb, Memory)
}
async fn retire_authority(&self, authority: acyclic_fs::AuthorityId, work: &mut acyclic_fs::AuthorityWork<'_>) -> Result<(), acyclic_fs::AuthorityStoreError> {
forward!(self, retire_authority(authority, work), IndexedDb, Memory)
}
async fn reserve_publication(&self, authority: acyclic_fs::AuthorityId, expected: acyclic_fs::Head, operation: acyclic_fs::OperationId, work: &mut acyclic_fs::AuthorityWork<'_>) -> Result<acyclic_fs::ReservationOutcome, acyclic_fs::AuthorityStoreError> {
forward!(self, reserve_publication(authority, expected, operation, work), IndexedDb, Memory)
}
async fn release_publication(&self, reservation: acyclic_fs::PublicationReservation, work: &mut acyclic_fs::AuthorityWork<'_>) -> Result<(), acyclic_fs::AuthorityStoreError> {
forward!(self, release_publication(reservation, work), IndexedDb, Memory)
}
fn supports_generation_lineage_prefix(&self) -> bool { match self { Self::IndexedDb(store) => store.supports_generation_lineage_prefix(), Self::Memory(store) => store.supports_generation_lineage_prefix() } }
}

impl AsyncObjectStore for BrowserObjects {
    async fn put(
        &self,
        object_id: ObjectId,
        bytes: OwnedBytes,
        context: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<(), acyclic_fs::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(acyclic_fs::ObjectStoreError::NativeUnavailable);
        }
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
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(acyclic_fs::ObjectStoreError::NativeUnavailable);
        }
        forward!(
            self,
            put_hashed(object, context),
            IndexedDb,
            Opfs,
            Memory
        )
    }

    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        context: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<(), acyclic_fs::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(acyclic_fs::ObjectStoreError::NativeUnavailable);
        }
        forward!(
            self,
            put_many(writes, context),
            IndexedDb,
            Opfs,
            Memory
        )
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        context: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<acyclic_fs::ObjectRead, acyclic_fs::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(acyclic_fs::ObjectStoreError::NativeUnavailable);
        }
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
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(acyclic_fs::ObjectStoreError::NativeUnavailable);
        }
        forward!(
            self,
            read_many(requests, context),
            IndexedDb,
            Opfs,
            Memory
        )
    }

    async fn contains(
        &self,
        object_id: ObjectId,
        context: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<bool, acyclic_fs::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(acyclic_fs::ObjectStoreError::NativeUnavailable);
        }
        forward!(
            self,
            contains(object_id, context),
            IndexedDb,
            Opfs,
            Memory
        )
    }
}

impl OperationWindowStore for BrowserAuthority {
    type Error = AuthorityStoreError;

    async fn load(
        &self,
        workspace: WorkspaceId,
    ) -> Result<Option<OperationWindowSnapshot>, Self::Error> {
        match self {
            Self::IndexedDb(store) => store.load(workspace).await,
            Self::Memory(_) => Err(AuthorityStoreError::Rejected(
                "operation windows require persistent browser authority".to_owned(),
            )),
        }
    }

    async fn compare_and_swap(
        &self,
        workspace: WorkspaceId,
        expected_revision: u64,
        replacement: OperationWindowSnapshot,
    ) -> Result<bool, Self::Error> {
        match self {
            Self::IndexedDb(store) => {
                store
                    .compare_and_swap(workspace, expected_revision, replacement)
                    .await
            }
            Self::Memory(_) => Err(AuthorityStoreError::Rejected(
                "operation windows require persistent browser authority".to_owned(),
            )),
        }
    }
}
