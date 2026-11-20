use super::*;
use acyclic_fs::model::VolumeLimits;
use acyclic_fs::storage::{ObjectId, ObjectRead, ObjectReadRequest, ObjectWrite};
use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, EmbeddedCapabilities, LocalFs, LocalOptions, MemoryFs,
};
use acyclic_stream::OwnedBytes;
use std::sync::Arc;

#[derive(Clone)]
enum Stores {
    Memory(MemoryFs),
    Local(LocalFs),
}

macro_rules! delegate {
        ($self:ident, $trait:ident, $field:ident, $method:ident($($argument:expr),* $(,)?)) => {
            match $self {
                Stores::Memory(fs) => $trait::$method(&fs.inner.$field, $($argument),*).await,
                Stores::Local(fs) => $trait::$method(&fs.inner.$field, $($argument),*).await,
            }
        };

        ($trait:ident, $field:ident, {$($extra:item)*},
            {$(fn $method:ident($context:ident: $context_type:ty $(, $argument:ident: $type:ty)* $(,)?) -> $result:ty;)*}
        ) => {
            impl $trait for Stores {
                $($extra)*
                $(async fn $method(&self, $($argument: $type,)* $context: $context_type) -> $result {
                    #[cfg(not(target_arch = "wasm32"))]
                    if $context.original.is_some() {
                        return Err(acyclic_fs::ObjectStoreError::NativeUnavailable);
                    }
                    match self {
                        Stores::Memory(fs) => $trait::$method(&fs.inner.$field, $($argument,)* $context).await,
                        Stores::Local(fs) => $trait::$method(&fs.inner.$field, $($argument,)* $context).await,
                    }
                })*
            }
        };
    }
impl AsyncAuthorityStore for Stores {
    fn retains_operation_payload(&self) -> bool {
        match self {
            Self::Memory(fs) => fs.inner.authority.retains_operation_payload(),
            Self::Local(fs) => fs.inner.authority.retains_operation_payload(),
        }
    }

    fn operation_claims(&self) -> &acyclic_stream::OperationClaims {
        match self {
            Self::Memory(fs) => fs.operation_claims(),
            Self::Local(fs) => fs.operation_claims(),
        }
    }
    fn actor_scope(&self) -> [u8; 16] {
        [0; 16]
    }

    async fn create_authority<G: acyclic_fs::AuthorityGuard>(
        &self,
        authority: acyclic_fs::AuthorityId,
        epoch: acyclic_fs::Epoch,
        work: &mut acyclic_fs::AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<acyclic_fs::CreateAuthorityOutcome, acyclic_fs::AuthorityError<G::Error>> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(acyclic_fs::AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        acyclic_fs::async_storage::seal(guard.as_deref_mut(), work).await?;
        let value = delegate!(
            self,
            AsyncAuthorityStore,
            authority,
            create_authority(authority, epoch, work, None::<&mut ()>)
        )
        .map_err(|error| {
            acyclic_fs::AuthorityError::Authority(error.into_error(std::convert::identity))
        })?;
        if matches!(value, acyclic_fs::CreateAuthorityOutcome::Existing(_)) {
            acyclic_fs::async_storage::seal(guard, work).await?;
        }
        Ok(value)
    }
    async fn fork_authority<G: acyclic_fs::AuthorityGuard>(
        &self,
        source: acyclic_fs::GenerationForkSource,
        destination: acyclic_fs::AuthorityId,
        operation: acyclic_fs::OperationId,
        work: &mut acyclic_fs::AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<acyclic_fs::CreateAuthorityOutcome, acyclic_fs::AuthorityError<G::Error>> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(acyclic_fs::AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        acyclic_fs::async_storage::seal(guard.as_deref_mut(), work).await?;
        let value = delegate!(
            self,
            AsyncAuthorityStore,
            authority,
            fork_authority(source, destination, operation, work, None::<&mut ()>)
        )
        .map_err(|error| {
            acyclic_fs::AuthorityError::Authority(error.into_error(std::convert::identity))
        })?;
        if matches!(value, acyclic_fs::CreateAuthorityOutcome::Existing(_)) {
            acyclic_fs::async_storage::seal(guard, work).await?;
        }
        Ok(value)
    }
    // A store with prefix lineage forks atomically, its changes included.
    async fn fork_workspace<G: acyclic_fs::AuthorityGuard>(
        &self,
        fork: acyclic_fs::WorkspaceForkCommit,
        permit: acyclic_fs::PublicationPermit,
        work: &mut acyclic_fs::AuthorityWork<'_>,
        guard: Option<&mut G>,
    ) -> Result<acyclic_fs::WorkspaceForkOutcome, acyclic_fs::AuthorityError<G::Error>> {
        delegate!(
            self,
            AsyncAuthorityStore,
            authority,
            fork_workspace(fork, permit, work, guard)
        )
    }
    async fn compare_and_append<G: acyclic_fs::AuthorityGuard>(
        &self,
        request: acyclic_fs::GuardedAppend,
        work: &mut acyclic_fs::AuthorityWork<'_>,
        guard: Option<&mut G>,
    ) -> Result<acyclic_fs::AppendOutcome, acyclic_fs::AuthorityError<G::Error>> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(acyclic_fs::AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        if request.permit != acyclic_fs::PublicationPermit::Unrestricted {
            return Err(acyclic_fs::AuthorityStoreError::Rejected(
                "authority backend cannot atomically evaluate operation leases".to_owned(),
            )
            .into());
        }
        acyclic_fs::async_storage::seal(guard, work).await?;
        delegate!(
            self,
            AsyncAuthorityStore,
            authority,
            compare_and_append(request, work, None::<&mut ()>)
        )
        .map_err(|error| {
            acyclic_fs::AuthorityError::Authority(error.into_error(std::convert::identity))
        })
    }
    async fn head(
        &self,
        authority: acyclic_fs::AuthorityId,
        work: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<acyclic_fs::Head, acyclic_fs::AuthorityStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(acyclic_fs::AuthorityStoreError::NativeOriginalUnsupported);
        }

        delegate!(self, AsyncAuthorityStore, authority, head(authority, work))
    }
    async fn replay(
        &self,
        authority: acyclic_fs::AuthorityId,
        after: acyclic_fs::Sequence,
        limit: acyclic_fs::ReplayLimit,
        work: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<
        (
            Vec<acyclic_fs::DurableCommit>,
            acyclic_fs::AuthorityAllocation,
        ),
        acyclic_fs::AuthorityStoreError,
    > {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(acyclic_fs::AuthorityStoreError::NativeOriginalUnsupported);
        }

        delegate!(
            self,
            AsyncAuthorityStore,
            authority,
            replay(authority, after, limit, work)
        )
    }
    async fn fence(
        &self,
        authority: acyclic_fs::AuthorityId,
        expected: acyclic_fs::Head,
        work: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<acyclic_fs::FenceOutcome, acyclic_fs::AuthorityStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(acyclic_fs::AuthorityStoreError::NativeOriginalUnsupported);
        }

        delegate!(
            self,
            AsyncAuthorityStore,
            authority,
            fence(authority, expected, work)
        )
    }
    async fn find_operation(
        &self,
        authority: acyclic_fs::AuthorityId,
        operation: acyclic_fs::OperationId,
        work: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<Option<acyclic_fs::DurableCommit>, acyclic_fs::AuthorityStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(acyclic_fs::AuthorityStoreError::NativeOriginalUnsupported);
        }

        delegate!(
            self,
            AsyncAuthorityStore,
            authority,
            find_operation(authority, operation, work)
        )
    }
    fn supports_generation_lineage_prefix(&self) -> bool {
        match self {
            Self::Memory(fs) => {
                AsyncAuthorityStore::supports_generation_lineage_prefix(&fs.inner.authority)
            }
            Self::Local(fs) => {
                AsyncAuthorityStore::supports_generation_lineage_prefix(&fs.inner.authority)
            }
        }
    }
}

delegate!(
    AsyncObjectStore,
    objects,
    {
        fn collection(&self) -> Option<&Arc<acyclic_fs::Collection>> {
            match self {
                Self::Memory(_) => None,
                Self::Local(fs) => fs.inner.objects.collection(),
            }
        }
    },
    {
        fn put(
            context: &mut acyclic_fs::AuthorityWork<'_>,
            id: ObjectId,
            bytes: OwnedBytes,
        ) -> Result<(), acyclic_fs::ObjectStoreError>;
        fn put_many(
            context: &mut acyclic_fs::AuthorityWork<'_>,
            writes: &[ObjectWrite],
        ) -> Result<(), acyclic_fs::ObjectStoreError>;
        fn read(
            context: &mut acyclic_fs::AuthorityWork<'_>,
            id: ObjectId,
            maximum: u64,
        ) -> Result<ObjectRead, acyclic_fs::ObjectStoreError>;
        fn read_many(
            context: &mut acyclic_fs::AuthorityWork<'_>,
            requests: &[ObjectReadRequest],
        ) -> Result<(Vec<ObjectRead>, acyclic_fs::AuthorityAllocation), acyclic_fs::ObjectStoreError>;
        fn contains(
            context: &mut acyclic_fs::AuthorityWork<'_>,
            id: ObjectId,
        ) -> Result<bool, acyclic_fs::ObjectStoreError>;
        fn flush_before_publish(
            context: &mut acyclic_fs::AuthorityWork<'_>,
            scope: acyclic_fs::PublicationScope<'_>,
        ) -> Result<acyclic_fs::PublicationHold, acyclic_fs::ObjectStoreError>;
    }
);

#[tokio::test]
async fn mounted_view_routes_memory_and_local_objects_through_existing_traits()
-> Result<(), Box<dyn std::error::Error>> {
    let cancel = CancellationToken::new();
    let directory = tempfile::tempdir()?;
    let local = Fs::local(LocalOptions::new(directory.path())).await?;
    let memory_store = Stores::Memory(Fs::memory());
    let local_store = Stores::Local(local);
    let memory = Fs::new(
        memory_store.clone(),
        memory_store,
        EmbeddedCapabilities::MEMORY,
    );
    let disk = Fs::new(
        local_store.clone(),
        local_store,
        EmbeddedCapabilities::MEMORY,
    );
    let root = memory.create_workspace("root", B, &cancel).await?.value;
    let scratch = disk.create_workspace("scratch", B, &cancel).await?.value;
    root.write_text("/file", "memory").await?;
    scratch.write_text("/file", "disk").await?;
    let root_head = root.head(B, &cancel).await?.value;
    let scratch_head = scratch.head(B, &cancel).await?.value;
    assert!(matches!(
        scratch
            .fork::<_, _, acyclic_fs::WorkspaceError>(
                "foreign",
                acyclic_fs::ForkOptions::from_generation(
                    root_head.clone(),
                    acyclic_fs::IdempotencyKey::from_bytes([60; 16]),
                ),
                acyclic_fs::PublicationPermit::Unrestricted,
                B,
                &cancel,
                &mut |_| std::future::ready(Ok(acyclic_fs::OperationReceipt {
                    value: (),
                    work: acyclic_fs::WorkCounters::UNCHARGED
                }))
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error),
        Err(acyclic_fs::WorkspaceError::ForeignGeneration)
    ));
    assert!(matches!(
        root.fork::<_, _, acyclic_fs::WorkspaceError>(
            "foreign",
            acyclic_fs::ForkOptions::from_generation(
                scratch_head.clone(),
                acyclic_fs::IdempotencyKey::from_bytes([61; 16]),
            ),
            acyclic_fs::PublicationPermit::Unrestricted,
            B,
            &cancel,
            &mut |_| std::future::ready(Ok(acyclic_fs::OperationReceipt {
                value: (),
                work: acyclic_fs::WorkCounters::UNCHARGED
            }))
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error),
        Err(acyclic_fs::WorkspaceError::ForeignGeneration)
    ));
    for (workspace, head) in [(&root, &root_head), (&scratch, &scratch_head)] {
        let fork = workspace
            .fork::<_, _, acyclic_fs::WorkspaceError>(
                "child",
                acyclic_fs::ForkOptions::from_generation(
                    head.clone(),
                    acyclic_fs::IdempotencyKey::from_bytes([62; 16]),
                ),
                acyclic_fs::PublicationPermit::Unrestricted,
                B,
                &cancel,
                &mut |_| {
                    std::future::ready(Ok(acyclic_fs::OperationReceipt {
                        value: (),
                        work: acyclic_fs::WorkCounters::UNCHARGED,
                    }))
                },
            )
            .await?
            .value;
        fork.write_text("/file", "child only").await?;
        assert_eq!(workspace.head(B, &cancel).await?.value.id(), head.id());
        assert_ne!(fork.head(B, &cancel).await?.value.id(), head.id());
    }
    let mut view = acyclic_fs::MountedView::builder()
        .mount(
            "/",
            root.checkout(
                GenerationSelector::Head,
                CheckoutMode::read_only_pinned(),
                B,
                &cancel,
            )
            .await?
            .value,
        )?
        .mount(
            "/.scratch",
            scratch
                .checkout(
                    GenerationSelector::Head,
                    CheckoutMode::read_only_pinned(),
                    B,
                    &cancel,
                )
                .await?
                .value,
        )?
        .build()?;
    let cancellation = CancellationToken::new();
    let root_path = acyclic_fs::path::PortablePath::parse("/file", VolumeLimits::default())?;
    let scratch_path =
        acyclic_fs::path::PortablePath::parse("/.scratch/file", VolumeLimits::default())?;
    for path in [&root_path, &scratch_path] {
        let route = view.route_mut(path)?;
        assert!(
            route
                .checkout
                .lookup_no_follow(&route.path, B, &cancellation)
                .await?
                .value
                .record
                .is_some()
        );
    }
    assert_eq!(view.snapshot().bindings.len(), 2);
    assert_eq!(
        view.resolve(&root_path)
            .ok_or("memory route")?
            .checkout()
            .generation_id(),
        root_head.id()
    );
    assert_eq!(
        view.resolve(&scratch_path)
            .ok_or("local route")?
            .checkout()
            .generation_id(),
        scratch_head.id()
    );
    assert_eq!(
        view.validate_rename(&root_path, &scratch_path),
        Err(acyclic_fs::MountError::CrossVolume)
    );
    assert_eq!(
        root.read("/file", 32, B, &cancellation)
            .await?
            .value
            .as_ref(),
        b"memory"
    );
    assert_eq!(
        scratch
            .read("/file", 32, B, &cancellation)
            .await?
            .value
            .as_ref(),
        b"disk"
    );
    Ok(())
}
