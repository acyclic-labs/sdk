use super::*;
    use crate::foundation::{AuthorityId, Epoch, Head, OperationId, ProposedCommit, Sequence};
    use crate::model::VolumeLimits;
    use crate::storage::{
        AppendOutcome, AuthorityResult, CreateAuthorityOutcome, FenceOutcome, ObjectId, ObjectRead,
        ObjectReadRequest, ObjectResult, ObjectWrite, ReplayLimit,
    };
    use crate::{
        AsyncAuthorityStore, AsyncObjectStore, EmbeddedCapabilities, LocalFs, LocalOptions,
        MemoryFs,
    };
    use acyclic_stream::OwnedBytes;
    use std::sync::Arc;

    #[derive(Clone)]
    enum Stores {
        Memory(MemoryFs),
        Local(LocalFs),
    }

    macro_rules! delegate {
        ($self:ident, $trait:ident, $field:ident, $method:ident($($argument:expr),*)) => {
            match $self {
                Stores::Memory(fs) => $trait::$method(&fs.inner.$field, $($argument),*).await,
                Stores::Local(fs) => $trait::$method(&fs.inner.$field, $($argument),*).await,
            }
        };

        ($trait:ident, $field:ident, {$($extra:item)*},
            $(fn $method:ident($context:ident: $context_type:ty $(, $argument:ident: $type:ty)*) -> $result:ty;)*
        ) => {
            impl $trait for Stores {
                $($extra)*
                $(async fn $method(&self, $($argument: $type,)* $context: $context_type) -> $result {
                    #[cfg(not(target_arch = "wasm32"))]
                    if $context.original.is_some() {
                        return Err(crate::ObjectStoreError::NativeUnavailable);
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
async fn create_authority<G: crate::AuthorityGuard>(&self, authority: crate::AuthorityId, epoch: crate::Epoch, work: &mut crate::AuthorityWork<'_>, mut guard: Option<&mut G>) -> Result<crate::CreateAuthorityOutcome, crate::AuthorityError<G::Error>> {
#[cfg(not(target_arch = "wasm32"))]
if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }
crate::async_storage::seal(guard.as_deref_mut(), work).await?;
let value = delegate!(self, AsyncAuthorityStore, authority, create_authority(authority, epoch, work, None::<&mut ()>)).map_err(|error| crate::AuthorityError::Authority(error.into_error(std::convert::identity)))?;
if matches!(value, crate::CreateAuthorityOutcome::Existing(_)) { crate::async_storage::seal(guard.as_deref_mut(), work).await?; }
Ok(value)
}
async fn fork_authority<G: crate::AuthorityGuard>(&self, source: crate::GenerationForkSource, destination: crate::AuthorityId, operation: crate::OperationId, work: &mut crate::AuthorityWork<'_>, mut guard: Option<&mut G>) -> Result<crate::CreateAuthorityOutcome, crate::AuthorityError<G::Error>> {
#[cfg(not(target_arch = "wasm32"))]
if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }
crate::async_storage::seal(guard.as_deref_mut(), work).await?;
let value = delegate!(self, AsyncAuthorityStore, authority, fork_authority(source, destination, operation, work, None::<&mut ()>)).map_err(|error| crate::AuthorityError::Authority(error.into_error(std::convert::identity)))?;
if matches!(value, crate::CreateAuthorityOutcome::Existing(_)) { crate::async_storage::seal(guard.as_deref_mut(), work).await?; }
Ok(value)
}
async fn compare_and_append<G: crate::AuthorityGuard>(&self, request: crate::GuardedAppend, work: &mut crate::AuthorityWork<'_>, mut guard: Option<&mut G>) -> Result<crate::AppendOutcome, crate::AuthorityError<G::Error>> {
#[cfg(not(target_arch = "wasm32"))]
if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }
if request.permit != crate::PublicationPermit::Unrestricted { return Err(crate::AuthorityStoreError::Rejected("authority backend cannot atomically evaluate operation leases".to_owned()).into()); }
crate::async_storage::seal(guard.as_deref_mut(), work).await?;
delegate!(self, AsyncAuthorityStore, authority, compare_and_append(request, work, None::<&mut ()>)).map_err(|error| crate::AuthorityError::Authority(error.into_error(std::convert::identity)))
}
async fn head(&self, authority: crate::AuthorityId, work: &mut crate::AuthorityWork<'_>) -> Result<crate::Head, crate::AuthorityStoreError> {
#[cfg(not(target_arch = "wasm32"))]
if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }

delegate!(self, AsyncAuthorityStore, authority, head(authority, work))

}
async fn replay(&self, authority: crate::AuthorityId, after: crate::Sequence, limit: crate::ReplayLimit, work: &mut crate::AuthorityWork<'_>) -> Result<(Vec<crate::DurableCommit>, crate::AuthorityAllocation), crate::AuthorityStoreError> {
#[cfg(not(target_arch = "wasm32"))]
if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }

delegate!(self, AsyncAuthorityStore, authority, replay(authority, after, limit, work))

}
async fn fence(&self, authority: crate::AuthorityId, expected: crate::Head, work: &mut crate::AuthorityWork<'_>) -> Result<crate::FenceOutcome, crate::AuthorityStoreError> {
#[cfg(not(target_arch = "wasm32"))]
if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }

delegate!(self, AsyncAuthorityStore, authority, fence(authority, expected, work))

}
async fn find_operation(&self, authority: crate::AuthorityId, operation: crate::OperationId, work: &mut crate::AuthorityWork<'_>) -> Result<Option<crate::DurableCommit>, crate::AuthorityStoreError> {
#[cfg(not(target_arch = "wasm32"))]
if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }

delegate!(self, AsyncAuthorityStore, authority, find_operation(authority, operation, work))

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

    delegate!(AsyncObjectStore, objects, {
        fn collection(&self) -> Option<&Arc<crate::Collection>> {
            match self {
                Self::Memory(_) => None,
                Self::Local(fs) => fs.inner.objects.collection(),
            }
        }
    },
        fn put(context: &mut crate::AuthorityWork<'_>, id: ObjectId, bytes: OwnedBytes) -> Result<(), crate::ObjectStoreError>;
        fn put_many(context: &mut crate::AuthorityWork<'_>, writes: &[ObjectWrite]) -> Result<(), crate::ObjectStoreError>;
        fn read(context: &mut crate::AuthorityWork<'_>, id: ObjectId, maximum: u64) -> Result<ObjectRead, crate::ObjectStoreError>;
        fn read_many(context: &mut crate::AuthorityWork<'_>, requests: &[ObjectReadRequest]) -> Result<(Vec<ObjectRead>, crate::AuthorityAllocation), crate::ObjectStoreError>;
        fn contains(context: &mut crate::AuthorityWork<'_>, id: ObjectId) -> Result<bool, crate::ObjectStoreError>;
        fn flush_before_publish(context: &mut crate::AuthorityWork<'_>, scope: crate::PublicationScope<'_>) -> Result<crate::PublicationHold, crate::ObjectStoreError>;
    );

    #[tokio::test]
    async fn mounted_view_routes_memory_and_local_objects_through_existing_traits()
    -> Result<(), Box<dyn std::error::Error>> {
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
        let root = memory.create_workspace("root", WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let scratch = disk.create_workspace("scratch", WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        root.write_text("/file", "memory").await?;
        scratch.write_text("/file", "disk").await?;
        let root_head = root.head(WorkBudget::UNBOUNDED, &CancellationToken::new()).await?.value;
        let scratch_head = scratch.head(WorkBudget::UNBOUNDED, &CancellationToken::new()).await?.value;
        assert!(matches!(
            scratch
                .fork(
                    "foreign",
                    crate::ForkOptions::from_generation(
                        root_head.clone(),
                        crate::IdempotencyKey::from_bytes([60; 16]),
                    ),
                crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
                .await.map(|receipt| receipt.value).map_err(|failure| failure.error), Err(crate::WorkspaceError::ForeignGeneration)
        ));
        assert!(matches!(
            root.fork(
                "foreign",
                crate::ForkOptions::from_generation(
                    scratch_head.clone(),
                    crate::IdempotencyKey::from_bytes([61; 16]),
                ),
            crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error), Err(crate::WorkspaceError::ForeignGeneration)
        ));
        for (workspace, head) in [(&root, &root_head), (&scratch, &scratch_head)] {
            let fork = workspace
                .fork(
                    "child",
                    crate::ForkOptions::from_generation(
                        head.clone(),
                        crate::IdempotencyKey::from_bytes([62; 16]),
                    ),
                crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
                .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
            fork.write_text("/file", "child only").await?;
            assert_eq!(workspace.head(WorkBudget::UNBOUNDED, &CancellationToken::new()).await?.value.id(), head.id());
            assert_ne!(fork.head(WorkBudget::UNBOUNDED, &CancellationToken::new()).await?.value.id(), head.id());
        }
        let mut view = crate::MountedView::builder()
            .mount(
                "/",
                root.checkout(GenerationSelector::Head, CheckoutMode::read_only_pinned(), WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await?.value,
            )?
            .mount(
                "/.scratch",
                scratch
                    .checkout(GenerationSelector::Head, CheckoutMode::read_only_pinned(), WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await?.value,
            )?
            .build()?;
        let cancellation = CancellationToken::new();
        let root_path = crate::path::PortablePath::parse("/file", VolumeLimits::default())?;
        let scratch_path =
            crate::path::PortablePath::parse("/.scratch/file", VolumeLimits::default())?;
        for path in [&root_path, &scratch_path] {
            let route = view.route_mut(path)?;
            assert!(
                route
                    .checkout
                    .lookup_no_follow(&route.path, WorkBudget::UNBOUNDED, &cancellation)
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
            Err(crate::MountError::CrossVolume)
        );
        assert_eq!(root.read("/file", 32, WorkBudget::UNBOUNDED, &cancellation).await?.value.as_ref(), b"memory");
        assert_eq!(scratch.read("/file", 32, WorkBudget::UNBOUNDED, &cancellation).await?.value.as_ref(), b"disk");
        Ok(())
    }
