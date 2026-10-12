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
    fn supports_generation_lineage_prefix(&self) -> bool {
        match self {
            Self::IndexedDb(store) => store.supports_generation_lineage_prefix(),
            Self::Memory(store) => store.supports_generation_lineage_prefix(),
        }
    }

    async fn fork_generation_authority(
        &self,
        source: GenerationForkSource,
        destination_authority: AuthorityId,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<CreateAuthorityOutcome> {
        forward!(
            self,
            fork_generation_authority(
                source,
                destination_authority,
                operation_id,
                budget,
                cancellation
            ),
            IndexedDb,
            Memory
        )
    }

    async fn fork_generation_authority_guarded<F, Fut>(
        &self,
        source: GenerationForkSource,
        destination_authority: AuthorityId,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        guard: &mut F,
    ) -> FsResult<CreateAuthorityOutcome>
    where
        F: FnMut(WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = FsResult<()>> + ProviderTask,
    {
        forward!(
            self,
            fork_generation_authority_guarded(
                source,
                destination_authority,
                operation_id,
                budget,
                cancellation,
                guard
            ),
            IndexedDb,
            Memory
        )
    }

    async fn create_authority(
        &self,
        authority_id: AuthorityId,
        genesis_epoch: Epoch,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<CreateAuthorityOutcome> {
        forward!(
            self,
            create_authority(authority_id, genesis_epoch, budget, cancellation),
            IndexedDb,
            Memory
        )
    }

    async fn create_authority_guarded<F, Fut>(
        &self,
        authority_id: AuthorityId,
        genesis_epoch: Epoch,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        guard: &mut F,
    ) -> FsResult<CreateAuthorityOutcome>
    where
        F: FnMut(WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = FsResult<()>> + ProviderTask,
    {
        forward!(
            self,
            create_authority_guarded(authority_id, genesis_epoch, budget, cancellation, guard),
            IndexedDb,
            Memory
        )
    }

    async fn retire_authority(
        &self,
        authority_id: AuthorityId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<()> {
        forward!(
            self,
            retire_authority(authority_id, budget, cancellation),
            IndexedDb,
            Memory
        )
    }

    async fn head(
        &self,
        authority_id: AuthorityId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Head> {
        forward!(
            self,
            head(authority_id, budget, cancellation),
            IndexedDb,
            Memory
        )
    }

    async fn compare_and_append(
        &self,
        authority_id: AuthorityId,
        epoch: Epoch,
        expected: Head,
        commit: acyclic_fs::ProposedCommit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<AppendOutcome> {
        forward!(
            self,
            compare_and_append(
                authority_id,
                epoch,
                expected,
                commit,
                budget,
                cancellation
            ),
            IndexedDb,
            Memory
        )
    }

    async fn compare_and_append_guarded(
        &self,
        request: GuardedAppend,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<AppendOutcome> {
        forward!(
            self,
            compare_and_append_guarded(request, budget, cancellation),
            IndexedDb,
            Memory
        )
    }

    async fn create_authority_with_first_record(
        &self,
        authority: AuthorityId,
        commit: acyclic_fs::ProposedCommit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<bool> {
        forward!(
            self,
            create_authority_with_first_record(authority, commit, budget, cancellation),
            IndexedDb,
            Memory
        )
    }

    async fn create_authority_with_first_record_guarded<F, Fut>(
        &self,
        authority: AuthorityId,
        commit: acyclic_fs::ProposedCommit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        guard: &mut F,
    ) -> FsResult<bool>
    where
        F: FnMut(WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = FsResult<()>> + ProviderTask,
    {
        forward!(
            self,
            create_authority_with_first_record_guarded(
                authority,
                commit,
                budget,
                cancellation,
                guard
            ),
            IndexedDb,
            Memory
        )
    }

    async fn commit_workspace_fork(
        &self,
        fork: WorkspaceForkCommit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<WorkspaceForkOutcome> {
        forward!(
            self,
            commit_workspace_fork(fork, budget, cancellation),
            IndexedDb,
            Memory
        )
    }

    async fn commit_workspace_fork_guarded<F, Fut>(
        &self,
        fork: WorkspaceForkCommit,
        permit: PublicationPermit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        guard: &mut F,
    ) -> FsResult<WorkspaceForkOutcome>
    where
        F: FnMut(WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = FsResult<()>> + ProviderTask,
    {
        forward!(
            self,
            commit_workspace_fork_guarded(fork, permit, budget, cancellation, guard),
            IndexedDb,
            Memory
        )
    }

    async fn publication_reservation(
        &self,
        _authority_id: AuthorityId,
        _operation_id: OperationId,
        _budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Option<PublicationReservation>> {
        forward!(
            self,
            publication_reservation(_authority_id, _operation_id, _budget, cancellation),
            IndexedDb,
            Memory
        )
    }

    async fn reserve_publication(
        &self,
        _authority_id: AuthorityId,
        _expected: Head,
        _operation_id: OperationId,
        _budget: WorkBudget,
        _cancellation: &CancellationToken,
    ) -> AuthorityResult<ReservationOutcome> {
        forward!(
            self,
            reserve_publication(
                _authority_id,
                _expected,
                _operation_id,
                _budget,
                _cancellation
            ),
            IndexedDb,
            Memory
        )
    }

    async fn release_publication(
        &self,
        _reservation: PublicationReservation,
        _budget: WorkBudget,
        _cancellation: &CancellationToken,
    ) -> AuthorityResult<()> {
        forward!(
            self,
            release_publication(_reservation, _budget, _cancellation),
            IndexedDb,
            Memory
        )
    }

    async fn replay(
        &self,
        authority_id: AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Vec<acyclic_fs::foundation::DurableCommit>> {
        forward!(
            self,
            replay(authority_id, after, limit, budget, cancellation),
            IndexedDb,
            Memory
        )
    }

    async fn fence(
        &self,
        authority_id: AuthorityId,
        expected: Head,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<FenceOutcome> {
        forward!(
            self,
            fence(authority_id, expected, budget, cancellation),
            IndexedDb,
            Memory
        )
    }

    async fn find_operation(
        &self,
        authority_id: AuthorityId,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Option<acyclic_fs::foundation::DurableCommit>> {
        forward!(
            self,
            find_operation(authority_id, operation_id, budget, cancellation),
            IndexedDb,
            Memory
        )
    }
}

impl AsyncObjectStore for BrowserObjects {
    async fn put(
        &self,
        object_id: ObjectId,
        bytes: OwnedBytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        forward!(
            self,
            put(object_id, bytes, budget, cancellation),
            IndexedDb,
            Opfs,
            Memory
        )
    }

    async fn put_hashed(
        &self,
        object: HashedObject,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        forward!(
            self,
            put_hashed(object, budget, cancellation),
            IndexedDb,
            Opfs,
            Memory
        )
    }

    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        forward!(
            self,
            put_many(writes, budget, cancellation),
            IndexedDb,
            Opfs,
            Memory
        )
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<ObjectRead> {
        forward!(
            self,
            read(object_id, maximum_bytes, budget, cancellation),
            IndexedDb,
            Opfs,
            Memory
        )
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<Vec<ObjectRead>> {
        forward!(
            self,
            read_many(requests, budget, cancellation),
            IndexedDb,
            Opfs,
            Memory
        )
    }

    async fn contains(
        &self,
        object_id: ObjectId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<bool> {
        forward!(
            self,
            contains(object_id, budget, cancellation),
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
