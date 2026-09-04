//! One native callback adapter for every embedded checkout consumer.

use super::{
    CaptureOptions, MountAttributePage, MountAttributeWriteMode, MountDirectoryEntry,
    MountDirectoryPage, MountFilesystem, MountLookup, MountNode, MountNodeKind, MountOpenFile,
    MountPath, MountPublication, MountRangeAllocation, MountSeekTarget, MountSourceError,
    NativeMountError, capture_paths, capture_root_identity, seal_checkout,
};
use crate::kernel::{
    AttributeClass, AttributeName, ExtentSeekTarget, FileKind, FileMetadata, FilePayload,
    FileRecord, LogicalName, NameEncoding, NamespacePath,
};
use crate::model::{FilesystemProfile, VolumeConfig, VolumeLimits};
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, AuthoredMutation, ByteRange, CancellationToken,
    Checkout, DetachedFile, FileId, FsError, NamedAttributeWriteMode, OperationFailure,
    OperationId, VolumeId, WorkBudget,
};
use bytes::Bytes;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

const CALLBACK_TIMEOUT: Duration = Duration::from_secs(30);

/// Blocking native callback adapter over one async canonical checkout.
pub struct CheckoutMountSource<A, O> {
    checkout: Arc<SharedCheckout<A, O>>,
    root: NamespacePath,
    limits: VolumeLimits,
    profile: FilesystemProfile,
    cancellation: CancellationToken,
    runtime: CallbackRuntime,
}

#[derive(Clone)]
struct CallbackRuntime {
    handle: tokio::runtime::Handle,
}

static CALLBACK_RUNTIME: OnceLock<Result<tokio::runtime::Runtime, String>> = OnceLock::new();

impl CallbackRuntime {
    fn create() -> Result<Self, NativeMountError> {
        let runtime = CALLBACK_RUNTIME.get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .thread_name("acyclic-fs-mount")
                .build()
                .map_err(|error| error.to_string())
        });
        match runtime {
            Ok(runtime) => Ok(Self {
                handle: runtime.handle().clone(),
            }),
            Err(error) => Err(NativeMountError::Driver(error.clone())),
        }
    }

    fn block_on<F>(&self, create: impl FnOnce() -> F + Send) -> Result<F::Output, MountSourceError>
    where
        F: Future,
        F::Output: Send,
    {
        match tokio::runtime::Handle::try_current() {
            Ok(current)
                if current.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread =>
            {
                Ok(tokio::task::block_in_place(|| {
                    self.handle.block_on(create())
                }))
            }
            Ok(_) => std::thread::scope(|scope| {
                let worker = std::thread::Builder::new()
                    .name("acyclic-fs-callback".to_owned())
                    .spawn_scoped(scope, || self.handle.block_on(create()))
                    .map_err(|error| MountSourceError::Engine(error.to_string()))?;
                match worker.join() {
                    Ok(value) => Ok(value),
                    Err(payload) => std::panic::resume_unwind(payload),
                }
            }),
            Err(_) => Ok(self.handle.block_on(create())),
        }
    }

    fn wait<T: Send, F: Future<Output = Result<T, MountSourceError>>>(
        &self,
        create: impl FnOnce() -> F + Send,
    ) -> Result<T, MountSourceError> {
        self.block_on(|| async {
            tokio::time::timeout(CALLBACK_TIMEOUT, create())
                .await
                .map_err(|_| MountSourceError::Stale)?
        })?
    }
}

struct DetachedMountState<A, O> {
    file: DetachedFile<A, O>,
    metadata: FileMetadata,
}

struct CheckoutDetachedFile<A, O> {
    state: tokio::sync::Mutex<DetachedMountState<A, O>>,
    runtime: CallbackRuntime,
    cancellation: CancellationToken,
    profile: FilesystemProfile,
    limits: VolumeLimits,
}

struct CheckoutAttachedFile<A, O> {
    checkout: Arc<SharedCheckout<A, O>>,
    file_id: FileId,
    runtime: CallbackRuntime,
    cancellation: CancellationToken,
    profile: FilesystemProfile,
    limits: VolumeLimits,
}

/// One process-local serialization and publication-fencing boundary for every
/// adapter, watcher, and transport view of the same checkout.
pub struct SharedCheckout<A, O> {
    state: tokio::sync::Mutex<SharedCheckoutState<A, O>>,
}

/// Locked checkout state. Dereferencing reaches the canonical checkout while
/// publication helpers retain an indeterminate operation in the same lock.
pub struct SharedCheckoutState<A, O> {
    checkout: Checkout<A, O>,
    publication_operation: Option<OperationId>,
    publication: MountPublication,
}

impl<A, O> SharedCheckout<A, O> {
    /// Creates the sole shared adapter boundary for one canonical checkout.
    #[must_use]
    pub fn new(checkout: Checkout<A, O>) -> Self {
        Self::with_publication(checkout, MountPublication::CloseAndSync)
    }

    /// Creates a shared checkout with one explicit native publication policy.
    #[must_use]
    pub fn with_publication(checkout: Checkout<A, O>, publication: MountPublication) -> Self {
        Self {
            state: tokio::sync::Mutex::new(SharedCheckoutState {
                checkout,
                publication_operation: None,
                publication,
            }),
        }
    }

    /// Serializes all checkout access with publication admission.
    pub async fn lock(&self) -> tokio::sync::MutexGuard<'_, SharedCheckoutState<A, O>> {
        self.state.lock().await
    }
}

impl<A, O> SharedCheckoutState<A, O> {
    async fn seal(&mut self, cancellation: &CancellationToken) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let operation_id = self.retained_operation_id();
        let result = seal_checkout(
            &mut self.checkout,
            operation_id,
            boundary_budget(),
            cancellation,
        )
        .await;
        if result.is_ok() {
            self.clear_retained_operation(operation_id);
        }
        result
    }

    async fn publish_after_mutation(
        &mut self,
        cancellation: &CancellationToken,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        if self.publication == MountPublication::PerMutation {
            self.seal(cancellation).await?;
        }
        Ok(())
    }

    async fn publish_at_native_boundary(
        &mut self,
        cancellation: &CancellationToken,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        if self.publication != MountPublication::Manual {
            self.seal(cancellation).await?;
        }
        Ok(())
    }

    /// Rejects later mutation while one publication has an unresolved result.
    ///
    /// # Errors
    ///
    /// Returns [`MountSourceError::Stale`] while publication is indeterminate.
    pub fn ensure_publication_resolved(&self) -> Result<(), MountSourceError> {
        if self.publication_operation.is_some() {
            return Err(MountSourceError::Stale);
        }
        Ok(())
    }

    /// Returns the exact operation retained until publication resolves.
    pub fn retained_operation_id(&mut self) -> OperationId {
        *self
            .publication_operation
            .get_or_insert_with(OperationId::new)
    }

    /// Retains a caller-selected operation identity, or rejects a conflicting
    /// unresolved publication from another surface sharing this checkout.
    ///
    /// # Errors
    ///
    /// Returns [`MountSourceError::Stale`] for a different retained operation.
    pub fn retain_operation_id(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), MountSourceError> {
        match self.publication_operation {
            None => {
                self.publication_operation = Some(operation_id);
                Ok(())
            }
            Some(retained) if retained == operation_id => Ok(()),
            Some(_) => Err(MountSourceError::Stale),
        }
    }

    /// Clears only the operation that reached a known terminal success.
    pub fn clear_retained_operation(&mut self, operation_id: OperationId) {
        if self.publication_operation == Some(operation_id) {
            self.publication_operation = None;
        }
    }
}

impl<A, O> Deref for SharedCheckoutState<A, O> {
    type Target = Checkout<A, O>;

    fn deref(&self) -> &Self::Target {
        &self.checkout
    }
}

impl<A, O> DerefMut for SharedCheckoutState<A, O> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.checkout
    }
}

impl<A, O> CheckoutDetachedFile<A, O> {
    fn attribute_name(&self, bytes: &[u8]) -> Result<AttributeName, MountSourceError> {
        if self.profile != FilesystemProfile::Posix {
            return Err(MountSourceError::Unsupported(
                "native POSIX attributes require a POSIX volume profile".to_owned(),
            ));
        }
        AttributeName::new(
            AttributeClass::PosixXattr,
            bytes.to_vec(),
            self.limits.maximum_component_bytes,
        )
        .map_err(engine_error)
    }
}

impl<A, O> MountOpenFile for CheckoutDetachedFile<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn lookup(&self) -> Result<MountLookup, MountSourceError> {
        self.runtime.wait(|| async {
            let state = self.state.lock().await;
            Ok(MountLookup {
                node: MountNode {
                    file_id: state.file.file_id(),
                    kind: MountNodeKind::Regular,
                    logical_bytes: state.file.logical_bytes(),
                    link_count: 0,
                    device: None,
                },
                metadata: state.metadata,
            })
        })
    }

    fn read_range(&self, offset: u64, length: u32) -> Result<Bytes, MountSourceError> {
        self.runtime.wait(|| async {
            let state = self.state.lock().await;
            state
                .file
                .read_range(
                    ByteRange {
                        offset,
                        length: u64::from(length),
                    },
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map(|receipt| receipt.value.bytes)
                .map_err(engine_error)
        })
    }

    fn seek(&self, offset: u64, target: MountSeekTarget) -> Result<Option<u64>, MountSourceError> {
        self.runtime.wait(|| async {
            let state = self.state.lock().await;
            state
                .file
                .seek(
                    offset,
                    match target {
                        MountSeekTarget::Data => ExtentSeekTarget::Data,
                        MountSeekTarget::Hole => ExtentSeekTarget::Hole,
                    },
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map(|receipt| receipt.value)
                .map_err(engine_error)
        })
    }

    fn write_range(&self, offset: u64, bytes: Bytes) -> Result<(), MountSourceError> {
        self.runtime.wait(|| async {
            let mut state = self.state.lock().await;
            state
                .file
                .write_range(offset, bytes, boundary_budget(), &self.cancellation)
                .await
                .map(|_| ())
                .map_err(engine_error)
        })
    }

    fn resize(&self, logical_bytes: u64) -> Result<(), MountSourceError> {
        self.runtime.wait(|| async {
            let mut state = self.state.lock().await;
            state
                .file
                .resize(logical_bytes, boundary_budget(), &self.cancellation)
                .await
                .map(|_| ())
                .map_err(engine_error)
        })
    }

    fn allocate_range(
        &self,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
    ) -> Result<(), MountSourceError> {
        self.runtime.wait(|| async {
            let mut state = self.state.lock().await;
            let range = ByteRange { offset, length };
            match operation {
                MountRangeAllocation::PunchHole => {
                    state
                        .file
                        .zero_range(range, false, false, boundary_budget(), &self.cancellation)
                        .await
                }
                MountRangeAllocation::ZeroRange { extend } => {
                    state
                        .file
                        .zero_range(range, true, extend, boundary_budget(), &self.cancellation)
                        .await
                }
                MountRangeAllocation::Preallocate { keep_size } => {
                    state
                        .file
                        .preallocate(range, keep_size, boundary_budget(), &self.cancellation)
                        .await
                }
            }
            .map(|_| ())
            .map_err(engine_error)
        })
    }

    fn set_attributes(
        &self,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
    ) -> Result<(), MountSourceError> {
        self.runtime.wait(|| async {
            let mut state = self.state.lock().await;
            state
                .file
                .set_attributes(
                    metadata,
                    logical_bytes,
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(engine_error)?;
            state.metadata = metadata;
            Ok(())
        })
    }

    fn read_attribute(&self, name: &[u8]) -> Result<Option<Bytes>, MountSourceError> {
        let name = self.attribute_name(name)?;
        self.runtime.wait(|| async {
            let state = self.state.lock().await;
            state
                .file
                .read_named_attribute(&name, boundary_budget(), &self.cancellation)
                .await
                .map(|receipt| receipt.value)
                .map_err(engine_error)
        })
    }

    fn list_attributes(
        &self,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountAttributePage, MountSourceError> {
        let cursor = cursor.map(|name| self.attribute_name(name)).transpose()?;
        self.runtime.wait(|| async {
            let state = self.state.lock().await;
            let receipt = state
                .file
                .list_named_attributes(
                    cursor.as_ref(),
                    maximum_entries,
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(engine_error)?;
            let has_more = receipt.value.has_more;
            let names = receipt
                .value
                .entries
                .into_iter()
                .map(|entry| {
                    if entry.name.class() != AttributeClass::PosixXattr {
                        return Err(MountSourceError::Unsupported(
                            "POSIX mount encountered a non-POSIX named attribute".to_owned(),
                        ));
                    }
                    Ok(entry.name.as_bytes().to_vec())
                })
                .collect::<Result<Vec<_>, _>>()?;
            let next_cursor = has_more.then(|| names.last().cloned()).flatten();
            Ok(MountAttributePage { names, next_cursor })
        })
    }

    fn write_attribute(
        &self,
        name: &[u8],
        value: Bytes,
        mode: MountAttributeWriteMode,
    ) -> Result<(), MountSourceError> {
        let name = self.attribute_name(name)?;
        let mode = match mode {
            MountAttributeWriteMode::Upsert => NamedAttributeWriteMode::Upsert,
            MountAttributeWriteMode::Create => NamedAttributeWriteMode::Create,
            MountAttributeWriteMode::Replace => NamedAttributeWriteMode::Replace,
        };
        self.runtime.wait(|| async {
            let mut state = self.state.lock().await;
            let receipt = state
                .file
                .write_named_attribute(name, value, mode, boundary_budget(), &self.cancellation)
                .await
                .map_err(attribute_error)?;
            state.metadata = receipt.value;
            Ok(())
        })
    }

    fn remove_attribute(&self, name: &[u8]) -> Result<(), MountSourceError> {
        let name = self.attribute_name(name)?;
        self.runtime.wait(|| async {
            let mut state = self.state.lock().await;
            let receipt = state
                .file
                .remove_named_attribute(name, boundary_budget(), &self.cancellation)
                .await
                .map_err(attribute_error)?;
            state.metadata = receipt.value;
            Ok(())
        })
    }
}

impl<A, O> CheckoutAttachedFile<A, O> {
    fn attribute_name(&self, bytes: &[u8]) -> Result<AttributeName, MountSourceError> {
        if self.profile != FilesystemProfile::Posix {
            return Err(MountSourceError::Unsupported(
                "native POSIX attributes require a POSIX volume profile".to_owned(),
            ));
        }
        AttributeName::new(
            AttributeClass::PosixXattr,
            bytes.to_vec(),
            self.limits.maximum_component_bytes,
        )
        .map_err(engine_error)
    }
}

impl<A, O> MountOpenFile for CheckoutAttachedFile<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn lookup(&self) -> Result<MountLookup, MountSourceError> {
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            let record = checkout
                .read_file_record_by_id(self.file_id, boundary_budget(), &self.cancellation)
                .await
                .map_err(engine_error)?
                .value;
            let metadata = checkout
                .read_metadata_by_id(self.file_id, boundary_budget(), &self.cancellation)
                .await
                .map_err(engine_error)?
                .value;
            Ok(MountLookup {
                node: mount_node(record),
                metadata,
            })
        })
    }

    fn read_range(&self, offset: u64, length: u32) -> Result<Bytes, MountSourceError> {
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout
                .read_file_range_by_id(
                    self.file_id,
                    ByteRange {
                        offset,
                        length: u64::from(length),
                    },
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map(|receipt| receipt.value.bytes)
                .map_err(engine_error)
        })
    }

    fn seek(&self, offset: u64, target: MountSeekTarget) -> Result<Option<u64>, MountSourceError> {
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout
                .seek_file_extent_by_id(
                    self.file_id,
                    offset,
                    match target {
                        MountSeekTarget::Data => ExtentSeekTarget::Data,
                        MountSeekTarget::Hole => ExtentSeekTarget::Hole,
                    },
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map(|receipt| receipt.value)
                .map_err(engine_error)
        })
    }

    fn write_range(&self, offset: u64, bytes: Bytes) -> Result<(), MountSourceError> {
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            checkout
                .write_file_by_id(
                    self.file_id,
                    offset,
                    bytes,
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(engine_error)?;
            checkout.publish_after_mutation(&self.cancellation).await
        })
    }

    fn resize(&self, logical_bytes: u64) -> Result<(), MountSourceError> {
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            checkout
                .resize_file_by_id(
                    self.file_id,
                    logical_bytes,
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(engine_error)?;
            checkout.publish_after_mutation(&self.cancellation).await
        })
    }

    fn allocate_range(
        &self,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
    ) -> Result<(), MountSourceError> {
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            let range = ByteRange { offset, length };
            match operation {
                MountRangeAllocation::PunchHole => {
                    checkout
                        .zero_file_range_by_id(
                            self.file_id,
                            range,
                            false,
                            false,
                            boundary_budget(),
                            &self.cancellation,
                        )
                        .await
                }
                MountRangeAllocation::ZeroRange { extend } => {
                    checkout
                        .zero_file_range_by_id(
                            self.file_id,
                            range,
                            true,
                            extend,
                            boundary_budget(),
                            &self.cancellation,
                        )
                        .await
                }
                MountRangeAllocation::Preallocate { keep_size } => {
                    checkout
                        .preallocate_file_by_id(
                            self.file_id,
                            range,
                            keep_size,
                            boundary_budget(),
                            &self.cancellation,
                        )
                        .await
                }
            }
            .map_err(engine_error)?;
            checkout.publish_after_mutation(&self.cancellation).await
        })
    }

    fn set_attributes(
        &self,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
    ) -> Result<(), MountSourceError> {
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            checkout
                .set_attributes_by_id(
                    self.file_id,
                    metadata,
                    logical_bytes,
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(engine_error)?;
            checkout.publish_after_mutation(&self.cancellation).await
        })
    }

    fn read_attribute(&self, name: &[u8]) -> Result<Option<Bytes>, MountSourceError> {
        let name = self.attribute_name(name)?;
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout
                .read_named_attribute_by_id(
                    self.file_id,
                    &name,
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map(|receipt| receipt.value)
                .map_err(engine_error)
        })
    }

    fn list_attributes(
        &self,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountAttributePage, MountSourceError> {
        let cursor = cursor.map(|name| self.attribute_name(name)).transpose()?;
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            let receipt = checkout
                .list_named_attributes_by_id(
                    self.file_id,
                    cursor.as_ref(),
                    maximum_entries,
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(engine_error)?;
            let has_more = receipt.value.has_more;
            let names = receipt
                .value
                .entries
                .into_iter()
                .map(|entry| {
                    if entry.name.class() != AttributeClass::PosixXattr {
                        return Err(MountSourceError::Unsupported(
                            "POSIX mount encountered a non-POSIX named attribute".to_owned(),
                        ));
                    }
                    Ok(entry.name.as_bytes().to_vec())
                })
                .collect::<Result<Vec<_>, _>>()?;
            let next_cursor = has_more.then(|| names.last().cloned()).flatten();
            Ok(MountAttributePage { names, next_cursor })
        })
    }

    fn write_attribute(
        &self,
        name: &[u8],
        value: Bytes,
        mode: MountAttributeWriteMode,
    ) -> Result<(), MountSourceError> {
        let name = self.attribute_name(name)?;
        let mode = match mode {
            MountAttributeWriteMode::Upsert => NamedAttributeWriteMode::Upsert,
            MountAttributeWriteMode::Create => NamedAttributeWriteMode::Create,
            MountAttributeWriteMode::Replace => NamedAttributeWriteMode::Replace,
        };
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            checkout
                .write_named_attribute_by_id(
                    self.file_id,
                    name,
                    value,
                    mode,
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(attribute_error)?;
            checkout.publish_after_mutation(&self.cancellation).await
        })
    }

    fn remove_attribute(&self, name: &[u8]) -> Result<(), MountSourceError> {
        let name = self.attribute_name(name)?;
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            checkout
                .remove_named_attribute_by_id(
                    self.file_id,
                    name,
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(attribute_error)?;
            checkout.publish_after_mutation(&self.cancellation).await
        })
    }
}

impl<A, O> CheckoutMountSource<A, O> {
    /// Creates an independently cancellable adapter using the shared callback runtime.
    ///
    /// # Errors
    ///
    /// Returns a driver failure when the bounded callback runtime cannot start.
    pub fn new(
        checkout: Arc<SharedCheckout<A, O>>,
        config: VolumeConfig,
    ) -> Result<Self, NativeMountError> {
        let root = NamespacePath::new(Vec::new(), config.limits)
            .map_err(|error| NativeMountError::Driver(error.to_string()))?;
        Self::new_at(checkout, config, root)
    }

    /// Creates an adapter rooted at an authenticated workspace subtree.
    ///
    /// # Errors
    ///
    /// Returns a profile or runtime failure before any native mount starts.
    pub fn new_at(
        checkout: Arc<SharedCheckout<A, O>>,
        config: VolumeConfig,
        root: NamespacePath,
    ) -> Result<Self, NativeMountError> {
        if !profile_is_native(config.profile) {
            return Err(NativeMountError::ProfileUnavailable {
                profile: profile_name(config.profile),
                platform: std::env::consts::OS,
            });
        }
        let runtime = CallbackRuntime::create()?;
        Ok(Self {
            checkout,
            root,
            limits: config.limits,
            profile: config.profile,
            cancellation: CancellationToken::new(),
            runtime,
        })
    }

    /// Returns the checkout's owning volume without filesystem I/O.
    ///
    /// # Errors
    ///
    /// Returns stale when callback admission exceeds the finite deadline.
    pub fn volume_id(&self) -> Result<VolumeId, MountSourceError>
    where
        A: Send + Sync,
        O: Send + Sync,
    {
        self.runtime
            .wait(|| async { Ok(self.checkout.lock().await.volume_id()) })
    }

    /// Cancels future and in-flight canonical operations owned by this mount.
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    /// Publishes every pending mount mutation regardless of automatic policy.
    ///
    /// # Errors
    ///
    /// Returns the exact fenced publication failure and retains retry identity.
    pub fn sync(&self) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync,
        O: AsyncObjectStore + Send + Sync,
    {
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.seal(&self.cancellation).await
        })
    }

    /// Asynchronously publishes every pending mount mutation.
    ///
    /// This is the native customer handle path and does not enter the blocking
    /// foreign-thread callback bridge.
    ///
    /// # Errors
    ///
    /// Returns the exact fenced publication failure and retains retry identity.
    pub async fn sync_async(&self) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let mut checkout = self.checkout.lock().await;
        checkout.seal(&self.cancellation).await
    }

    fn path(&self, path: &MountPath) -> Result<NamespacePath, MountSourceError> {
        let mut components = self.root.components().to_vec();
        components.extend(
            path.components()
                .iter()
                .map(|component| self.logical_name(component))
                .collect::<Result<Vec<_>, _>>()?,
        );
        NamespacePath::new(components, self.limits).map_err(engine_error)
    }

    fn logical_name(&self, bytes: &[u8]) -> Result<LogicalName, MountSourceError> {
        let (encoding, logical_bytes) = match (self.profile, std::env::consts::OS) {
            (FilesystemProfile::Posix, "linux" | "macos") => {
                (NameEncoding::PosixBytes, bytes.to_vec())
            }
            (FilesystemProfile::Portable | FilesystemProfile::Browser, "linux" | "macos") => {
                std::str::from_utf8(bytes).map_err(|_| {
                    MountSourceError::Unsupported(
                        "portable volume cannot represent a non-UTF-8 native name".to_owned(),
                    )
                })?;
                (NameEncoding::Utf8, bytes.to_vec())
            }
            (FilesystemProfile::Windows, "windows") => {
                (NameEncoding::WindowsUtf16Le, bytes.to_vec())
            }
            (FilesystemProfile::Portable | FilesystemProfile::Browser, "windows") => {
                if !bytes.len().is_multiple_of(2) {
                    return Err(MountSourceError::Invalid(
                        "native UTF-16 name has an odd byte length".to_owned(),
                    ));
                }
                let units = bytes
                    .chunks_exact(2)
                    .map(|unit| u16::from_le_bytes([unit[0], unit[1]]))
                    .collect::<Vec<_>>();
                let value = String::from_utf16(&units).map_err(|_| {
                    MountSourceError::Unsupported(
                        "portable volume cannot represent an unpaired UTF-16 native name"
                            .to_owned(),
                    )
                })?;
                (NameEncoding::Utf8, value.into_bytes())
            }
            _ => {
                return Err(MountSourceError::Unsupported(
                    "volume name profile is incompatible with this native mount".to_owned(),
                ));
            }
        };
        LogicalName::new(encoding, logical_bytes, self.limits.maximum_component_bytes)
            .map_err(engine_error)
    }

    fn posix_attribute_name(&self, bytes: &[u8]) -> Result<AttributeName, MountSourceError> {
        if self.profile != FilesystemProfile::Posix {
            return Err(MountSourceError::Unsupported(
                "native POSIX attributes require a POSIX volume profile".to_owned(),
            ));
        }
        AttributeName::new(
            AttributeClass::PosixXattr,
            bytes.to_vec(),
            self.limits.maximum_component_bytes,
        )
        .map_err(engine_error)
    }

    fn native_name(name: &LogicalName) -> Result<Vec<u8>, MountSourceError> {
        match (name.encoding(), std::env::consts::OS) {
            (NameEncoding::Utf8 | NameEncoding::PosixBytes, "linux" | "macos") => {
                Ok(name.as_bytes().to_vec())
            }
            (NameEncoding::WindowsUtf16Le, "windows") => Ok(name.as_bytes().to_vec()),
            (NameEncoding::Utf8, "windows") => {
                let text = std::str::from_utf8(name.as_bytes()).map_err(|_| {
                    MountSourceError::Invalid("authenticated UTF-8 name is malformed".to_owned())
                })?;
                Ok(text.encode_utf16().flat_map(u16::to_le_bytes).collect())
            }
            _ => Err(MountSourceError::Unsupported(
                "authenticated name encoding is incompatible with this native mount".to_owned(),
            )),
        }
    }
}

fn profile_is_native(profile: FilesystemProfile) -> bool {
    match std::env::consts::OS {
        "linux" | "macos" => !matches!(profile, FilesystemProfile::Windows),
        "windows" => !matches!(profile, FilesystemProfile::Posix),
        _ => false,
    }
}

fn profile_name(profile: FilesystemProfile) -> &'static str {
    match profile {
        FilesystemProfile::Portable => "portable",
        FilesystemProfile::Posix => "posix",
        FilesystemProfile::Windows => "windows",
        FilesystemProfile::Browser => "browser",
    }
}

impl<A, O> MountFilesystem for CheckoutMountSource<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn lookup(&self, path: &MountPath) -> Result<Option<MountLookup>, MountSourceError> {
        let path = self.path(path)?;
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            let receipt = checkout
                .lookup_no_follow_with_metadata(&path, boundary_budget(), &self.cancellation)
                .await
                .map_err(engine_error)?;
            Ok(receipt.value.map(|value| MountLookup {
                node: mount_node(value.record),
                metadata: value.metadata,
            }))
        })
    }

    fn open_file(&self, path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        let path = self.path(path)?;
        let file_id = self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            let record = checkout
                .lookup_no_follow(&path, boundary_budget(), &self.cancellation)
                .await
                .map_err(engine_error)?
                .value
                .record
                .ok_or(MountSourceError::NotFound)?;
            if record.kind != FileKind::Regular {
                return Err(MountSourceError::Invalid(
                    "only regular files can own native file handles".to_owned(),
                ));
            }
            Ok(record.file_id)
        })?;
        Ok(Arc::new(CheckoutAttachedFile {
            checkout: Arc::clone(&self.checkout),
            file_id,
            runtime: self.runtime.clone(),
            cancellation: self.cancellation.clone(),
            profile: self.profile,
            limits: self.limits,
        }))
    }

    fn detach_file(&self, path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        let path = self.path(path)?;
        let (file, metadata) = self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            let lookup = checkout
                .lookup_no_follow_with_metadata(&path, boundary_budget(), &self.cancellation)
                .await
                .map_err(engine_error)?
                .value
                .ok_or(MountSourceError::NotFound)?;
            if lookup.record.kind != FileKind::Regular {
                return Err(MountSourceError::Invalid(
                    "only regular files can own detached native handles".to_owned(),
                ));
            }
            let file = checkout
                .detach_regular_file(&path, boundary_budget(), &self.cancellation)
                .await
                .map_err(engine_error)?
                .value;
            Ok((file, lookup.metadata))
        })?;
        Ok(Arc::new(CheckoutDetachedFile {
            state: tokio::sync::Mutex::new(DetachedMountState { file, metadata }),
            runtime: self.runtime.clone(),
            cancellation: self.cancellation.clone(),
            profile: self.profile,
            limits: self.limits,
        }))
    }

    fn read_link(&self, path: &MountPath) -> Result<Bytes, MountSourceError> {
        let path = self.path(path)?;
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout
                .read_symbolic_link(&path, boundary_budget(), &self.cancellation)
                .await
                .map(|receipt| receipt.value)
                .map_err(engine_error)
        })
    }

    fn read_range(
        &self,
        path: &MountPath,
        offset: u64,
        length: u32,
    ) -> Result<Bytes, MountSourceError> {
        let path = self.path(path)?;
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout
                .read_file_range(
                    &path,
                    ByteRange {
                        offset,
                        length: u64::from(length),
                    },
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map(|receipt| receipt.value.bytes)
                .map_err(engine_error)
        })
    }

    fn seek(
        &self,
        path: &MountPath,
        offset: u64,
        target: MountSeekTarget,
    ) -> Result<Option<u64>, MountSourceError> {
        let path = self.path(path)?;
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout
                .seek_file_extent(
                    &path,
                    offset,
                    match target {
                        MountSeekTarget::Data => ExtentSeekTarget::Data,
                        MountSeekTarget::Hole => ExtentSeekTarget::Hole,
                    },
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map(|receipt| receipt.value)
                .map_err(engine_error)
        })
    }

    fn read_directory(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountDirectoryPage, MountSourceError> {
        let path = self.path(path)?;
        let cursor = cursor.map(|bytes| self.logical_name(bytes)).transpose()?;
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            let receipt = checkout
                .list_directory_records(
                    &path,
                    cursor.as_ref(),
                    maximum_entries,
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(engine_error)?;
            let has_more = receipt.value.has_more;
            let entries = receipt
                .value
                .entries
                .into_iter()
                .map(|entry| {
                    Ok(MountDirectoryEntry {
                        name: Self::native_name(&entry.name)?,
                        node: mount_node(entry.record),
                        metadata: entry.metadata,
                    })
                })
                .collect::<Result<Vec<_>, MountSourceError>>()?;
            let next_cursor = has_more
                .then(|| entries.last().map(|entry| entry.name.clone()))
                .flatten();
            Ok(MountDirectoryPage {
                entries,
                next_cursor,
            })
        })
    }

    fn create_file(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError> {
        let path = self.path(path)?;
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            let receipt = checkout
                .apply_authored_transaction(
                    vec![AuthoredMutation::CreateFile {
                        path,
                        bytes: Bytes::new(),
                        metadata,
                    }],
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(engine_error)?;
            let file_id = receipt.value.created_file_ids[0].ok_or_else(|| {
                MountSourceError::Engine("create omitted file identity".to_owned())
            })?;
            checkout.publish_after_mutation(&self.cancellation).await?;
            Ok(MountLookup {
                node: MountNode {
                    file_id,
                    kind: MountNodeKind::Regular,
                    logical_bytes: 0,
                    link_count: 1,
                    device: None,
                },
                metadata,
            })
        })
    }

    fn create_directory(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError> {
        let path = self.path(path)?;
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            let receipt = checkout
                .apply_authored_transaction(
                    vec![AuthoredMutation::CreateDirectory { path, metadata }],
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(engine_error)?;
            let file_id = receipt.value.created_file_ids[0].ok_or_else(|| {
                MountSourceError::Engine("create omitted file identity".to_owned())
            })?;
            checkout.publish_after_mutation(&self.cancellation).await?;
            Ok(MountLookup {
                node: MountNode {
                    file_id,
                    kind: MountNodeKind::Directory,
                    logical_bytes: 0,
                    link_count: 1,
                    device: None,
                },
                metadata,
            })
        })
    }

    fn create_symbolic_link(
        &self,
        path: &MountPath,
        target: Bytes,
        metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError> {
        let path = self.path(path)?;
        let logical_bytes = u64::try_from(target.len()).map_err(|_| {
            MountSourceError::Unsupported("symbolic-link target is too large".to_owned())
        })?;
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            let receipt = checkout
                .apply_authored_transaction(
                    vec![AuthoredMutation::CreateSymbolicLink {
                        path,
                        target,
                        metadata,
                    }],
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(engine_error)?;
            let file_id = receipt.value.created_file_ids[0].ok_or_else(|| {
                MountSourceError::Engine("create omitted file identity".to_owned())
            })?;
            checkout.publish_after_mutation(&self.cancellation).await?;
            Ok(MountLookup {
                node: MountNode {
                    file_id,
                    kind: MountNodeKind::SymbolicLink,
                    logical_bytes,
                    link_count: 1,
                    device: None,
                },
                metadata,
            })
        })
    }

    fn create_special(
        &self,
        path: &MountPath,
        kind: MountNodeKind,
        device: Option<(u32, u32)>,
        metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError> {
        let path = self.path(path)?;
        let (file_kind, mutation) = match (kind, device) {
            (MountNodeKind::Fifo, None) => (
                FileKind::Fifo,
                AuthoredMutation::CreateEmptySpecial {
                    path,
                    kind: FileKind::Fifo,
                    metadata,
                },
            ),
            (MountNodeKind::Socket, None) => (
                FileKind::Socket,
                AuthoredMutation::CreateEmptySpecial {
                    path,
                    kind: FileKind::Socket,
                    metadata,
                },
            ),
            (MountNodeKind::CharacterDevice, Some((major, minor))) => (
                FileKind::CharacterDevice,
                AuthoredMutation::CreateDevice {
                    path,
                    kind: FileKind::CharacterDevice,
                    major,
                    minor,
                    metadata,
                },
            ),
            (MountNodeKind::BlockDevice, Some((major, minor))) => (
                FileKind::BlockDevice,
                AuthoredMutation::CreateDevice {
                    path,
                    kind: FileKind::BlockDevice,
                    major,
                    minor,
                    metadata,
                },
            ),
            _ => {
                return Err(MountSourceError::Unsupported(
                    "special-file kind and device identity disagree".to_owned(),
                ));
            }
        };
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            let receipt = checkout
                .apply_authored_transaction(vec![mutation], boundary_budget(), &self.cancellation)
                .await
                .map_err(engine_error)?;
            let file_id = receipt.value.created_file_ids[0].ok_or_else(|| {
                MountSourceError::Engine("create omitted file identity".to_owned())
            })?;
            checkout.publish_after_mutation(&self.cancellation).await?;
            Ok(MountLookup {
                node: MountNode {
                    file_id,
                    kind,
                    logical_bytes: 0,
                    link_count: 1,
                    device: match file_kind {
                        FileKind::CharacterDevice | FileKind::BlockDevice => device,
                        _ => None,
                    },
                },
                metadata,
            })
        })
    }

    fn set_attributes(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
    ) -> Result<(), MountSourceError> {
        let path = self.path(path)?;
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            let mut authored = vec![AuthoredMutation::SetMetadata {
                path: path.clone(),
                metadata,
            }];
            if let Some(logical_bytes) = logical_bytes {
                authored.push(AuthoredMutation::Resize {
                    path,
                    logical_bytes,
                });
            }
            checkout
                .apply_authored_transaction(authored, boundary_budget(), &self.cancellation)
                .await
                .map_err(engine_error)?;
            checkout.publish_after_mutation(&self.cancellation).await
        })
    }

    fn read_attribute(
        &self,
        path: &MountPath,
        name: &[u8],
    ) -> Result<Option<Bytes>, MountSourceError> {
        let path = self.path(path)?;
        let name = self.posix_attribute_name(name)?;
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout
                .read_named_attribute(&path, &name, boundary_budget(), &self.cancellation)
                .await
                .map(|receipt| receipt.value)
                .map_err(engine_error)
        })
    }

    fn list_attributes(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountAttributePage, MountSourceError> {
        let path = self.path(path)?;
        let cursor = cursor
            .map(|name| self.posix_attribute_name(name))
            .transpose()?;
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            let receipt = checkout
                .list_named_attributes(
                    &path,
                    cursor.as_ref(),
                    maximum_entries,
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(engine_error)?;
            let has_more = receipt.value.has_more;
            let names = receipt
                .value
                .entries
                .into_iter()
                .map(|entry| {
                    if entry.name.class() != AttributeClass::PosixXattr {
                        return Err(MountSourceError::Unsupported(
                            "POSIX mount encountered a non-POSIX named attribute".to_owned(),
                        ));
                    }
                    Ok(entry.name.as_bytes().to_vec())
                })
                .collect::<Result<Vec<_>, _>>()?;
            let next_cursor = has_more.then(|| names.last().cloned()).flatten();
            Ok(MountAttributePage { names, next_cursor })
        })
    }

    fn write_attribute(
        &self,
        path: &MountPath,
        name: &[u8],
        value: Bytes,
        mode: MountAttributeWriteMode,
    ) -> Result<(), MountSourceError> {
        let path = self.path(path)?;
        let name = self.posix_attribute_name(name)?;
        let mode = match mode {
            MountAttributeWriteMode::Upsert => NamedAttributeWriteMode::Upsert,
            MountAttributeWriteMode::Create => NamedAttributeWriteMode::Create,
            MountAttributeWriteMode::Replace => NamedAttributeWriteMode::Replace,
        };
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            checkout
                .write_named_attribute(
                    path,
                    name,
                    value,
                    mode,
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(attribute_error)?;
            checkout.publish_after_mutation(&self.cancellation).await
        })
    }

    fn remove_attribute(&self, path: &MountPath, name: &[u8]) -> Result<(), MountSourceError> {
        let path = self.path(path)?;
        let name = self.posix_attribute_name(name)?;
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            checkout
                .remove_named_attribute(path, name, boundary_budget(), &self.cancellation)
                .await
                .map_err(attribute_error)?;
            checkout.publish_after_mutation(&self.cancellation).await
        })
    }

    fn write_range(
        &self,
        path: &MountPath,
        offset: u64,
        bytes: Bytes,
    ) -> Result<(), MountSourceError> {
        let path = self.path(path)?;
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            checkout
                .write_file(path, offset, bytes, boundary_budget(), &self.cancellation)
                .await
                .map_err(engine_error)?;
            checkout.publish_after_mutation(&self.cancellation).await
        })
    }

    fn resize(&self, path: &MountPath, logical_bytes: u64) -> Result<(), MountSourceError> {
        let path = self.path(path)?;
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            checkout
                .resize_file(path, logical_bytes, boundary_budget(), &self.cancellation)
                .await
                .map_err(engine_error)?;
            checkout.publish_after_mutation(&self.cancellation).await
        })
    }

    fn allocate_range(
        &self,
        path: &MountPath,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
    ) -> Result<(), MountSourceError> {
        let path = self.path(path)?;
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            match operation {
                MountRangeAllocation::PunchHole => {
                    checkout
                        .zero_file_range(
                            path,
                            ByteRange { offset, length },
                            false,
                            false,
                            boundary_budget(),
                            &self.cancellation,
                        )
                        .await
                }
                MountRangeAllocation::ZeroRange { extend } => {
                    checkout
                        .zero_file_range(
                            path,
                            ByteRange { offset, length },
                            true,
                            extend,
                            boundary_budget(),
                            &self.cancellation,
                        )
                        .await
                }
                MountRangeAllocation::Preallocate { keep_size } => {
                    checkout
                        .preallocate_file(
                            path,
                            ByteRange { offset, length },
                            keep_size,
                            boundary_budget(),
                            &self.cancellation,
                        )
                        .await
                }
            }
            .map_err(engine_error)?;
            checkout.publish_after_mutation(&self.cancellation).await
        })
    }

    fn clone_range(
        &self,
        source: &MountPath,
        source_offset: u64,
        destination: &MountPath,
        destination_offset: u64,
        length: u64,
    ) -> Result<(), MountSourceError> {
        let source = self.path(source)?;
        let destination = self.path(destination)?;
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            checkout
                .clone_file_range(
                    crate::FileCloneRequest {
                        source,
                        source_offset,
                        destination,
                        destination_offset,
                        length,
                    },
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(engine_error)?;
            checkout.publish_after_mutation(&self.cancellation).await
        })
    }

    fn clone_range_by_id(
        &self,
        source_file_id: FileId,
        source_offset: u64,
        destination_file_id: FileId,
        destination_offset: u64,
        length: u64,
    ) -> Result<(), MountSourceError> {
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            checkout
                .clone_file_range_by_id(
                    source_file_id,
                    source_offset,
                    destination_file_id,
                    destination_offset,
                    length,
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(engine_error)?;
            checkout.publish_after_mutation(&self.cancellation).await
        })
    }

    fn remove(&self, path: &MountPath, expected: Option<FileId>) -> Result<(), MountSourceError> {
        let path = self.path(path)?;
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            checkout
                .remove(path, expected, boundary_budget(), &self.cancellation)
                .await
                .map_err(engine_error)?;
            checkout.publish_after_mutation(&self.cancellation).await
        })
    }

    fn rename(
        &self,
        source: &MountPath,
        destination: &MountPath,
        replace: bool,
    ) -> Result<(), MountSourceError> {
        let source = self.path(source)?;
        let destination = self.path(destination)?;
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            checkout
                .rename(
                    source,
                    destination,
                    replace,
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(engine_error)?;
            checkout.publish_after_mutation(&self.cancellation).await
        })
    }

    fn hard_link(
        &self,
        source: &MountPath,
        destination: &MountPath,
    ) -> Result<(), MountSourceError> {
        let source = self.path(source)?;
        let destination = self.path(destination)?;
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            checkout
                .hard_link(source, destination, boundary_budget(), &self.cancellation)
                .await
                .map_err(engine_error)?;
            checkout.publish_after_mutation(&self.cancellation).await
        })
    }

    fn flush(&self) -> Result<(), MountSourceError> {
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout
                .publish_at_native_boundary(&self.cancellation)
                .await
        })
    }

    fn capture_host_path(
        &self,
        source_root: &Path,
        path: &MountPath,
    ) -> Result<(), MountSourceError> {
        let path = self.path(path)?;
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            let expected_root_identity =
                capture_root_identity(source_root).map_err(engine_error)?;
            capture_paths(
                &mut checkout,
                &[path],
                &CaptureOptions {
                    source_root: source_root.to_path_buf(),
                    expected_root_identity,
                    maximum_paths: 1,
                    maximum_extent_spans: 65_536,
                },
                boundary_budget(),
                &self.cancellation,
            )
            .await
            .map_err(engine_error)?;
            checkout
                .publish_at_native_boundary(&self.cancellation)
                .await
        })
    }
}

fn mount_node(record: FileRecord) -> MountNode {
    let kind = match record.kind {
        FileKind::Regular => MountNodeKind::Regular,
        FileKind::Directory => MountNodeKind::Directory,
        FileKind::SymbolicLink => MountNodeKind::SymbolicLink,
        FileKind::Fifo => MountNodeKind::Fifo,
        FileKind::Socket => MountNodeKind::Socket,
        FileKind::CharacterDevice => MountNodeKind::CharacterDevice,
        FileKind::BlockDevice => MountNodeKind::BlockDevice,
        FileKind::ReparsePoint | FileKind::MountBoundary => MountNodeKind::Unsupported,
    };
    let logical_bytes = match &record.payload {
        FilePayload::InlineRegular(data) => {
            u64::try_from(data.as_bytes().len()).unwrap_or(u64::MAX)
        }
        FilePayload::Regular { logical_bytes, .. } => *logical_bytes,
        FilePayload::SymbolicLink { target_bytes, .. } => *target_bytes,
        FilePayload::Directory { .. }
        | FilePayload::Empty
        | FilePayload::Device { .. }
        | FilePayload::ReparsePoint { .. } => 0,
    };
    let device = match &record.payload {
        FilePayload::Device { major, minor } => Some((*major, *minor)),
        _ => None,
    };
    MountNode {
        file_id: record.file_id,
        kind,
        logical_bytes,
        link_count: record.link_count,
        device,
    }
}

fn engine_error(error: impl std::fmt::Display) -> MountSourceError {
    MountSourceError::Engine(error.to_string())
}

fn attribute_error(error: OperationFailure<FsError>) -> MountSourceError {
    match error.error {
        FsError::CreationRejected => MountSourceError::AlreadyExists,
        FsError::NotFound => MountSourceError::NotFound,
        other => MountSourceError::Engine(other.to_string()),
    }
}

fn boundary_budget() -> WorkBudget {
    const OPERATIONS: u64 = 1_000_000;
    const BYTES: u64 = 256 * 1024 * 1024;
    WorkBudget {
        authority_records_read: OPERATIONS,
        authority_records_appended: OPERATIONS,
        authority_bytes_read: BYTES,
        authority_bytes_written: BYTES,
        object_probes: OPERATIONS,
        backend_read_operations: OPERATIONS,
        backend_write_operations: OPERATIONS,
        durability_operations: OPERATIONS,
        page_reads: OPERATIONS,
        page_writes: OPERATIONS,
        object_bytes_read: BYTES,
        object_bytes_written: BYTES,
        bytes_hashed: BYTES,
        bytes_copied: BYTES,
        bytes_encoded: BYTES,
        source_bytes_read: BYTES,
        output_bytes: BYTES,
        items_examined: OPERATIONS,
        items_returned: OPERATIONS,
        allocation_operations: OPERATIONS,
        peak_allocation_bytes: BYTES,
        materializations: OPERATIONS,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Fs;
    use crate::model::{
        AccessMode, CheckoutMode, ConsistencyMode, GenerationSelector, Lifecycle, MutationMode,
    };
    #[cfg(target_os = "linux")]
    use std::os::unix::ffi::OsStringExt;

    type MemorySource = CheckoutMountSource<
        crate::facade::MemoryAuthorityBackend,
        crate::facade::MemoryObjectBackend,
    >;

    fn metadata() -> FileMetadata {
        FileMetadata {
            posix_mode: crate::kernel::MetadataField::Unavailable,
            posix_uid: crate::kernel::MetadataField::Unavailable,
            posix_gid: crate::kernel::MetadataField::Unavailable,
            posix_flags: crate::kernel::MetadataField::Unavailable,
            windows_attributes: crate::kernel::MetadataField::Unavailable,
            created_ns: crate::kernel::MetadataField::Unavailable,
            modified_ns: crate::kernel::MetadataField::Unavailable,
            accessed_ns: crate::kernel::MetadataField::Unavailable,
            changed_ns: crate::kernel::MetadataField::Unavailable,
            named_attributes: crate::kernel::MetadataField::Unavailable,
            acl: crate::kernel::MetadataField::Unavailable,
            security_descriptor: crate::kernel::MetadataField::Unavailable,
        }
    }

    fn checkout_state(
        source: &MemorySource,
    ) -> Result<(crate::GenerationId, bool), MountSourceError> {
        source.runtime.block_on(|| async {
            let checkout = source.checkout.lock().await;
            (checkout.generation_id(), checkout.has_pending_mutations())
        })
    }

    fn source(profile: FilesystemProfile) -> Result<MemorySource, Box<dyn std::error::Error>> {
        Ok(shared_sources(profile)?.0)
    }

    fn shared_sources(
        profile: FilesystemProfile,
    ) -> Result<(MemorySource, MemorySource), Box<dyn std::error::Error>> {
        shared_sources_with_publication(profile, MountPublication::CloseAndSync)
    }

    fn shared_sources_with_publication(
        profile: FilesystemProfile,
        publication: MountPublication,
    ) -> Result<(MemorySource, MemorySource), Box<dyn std::error::Error>> {
        let mut config = VolumeConfig::portable(Lifecycle::Ephemeral);
        config.profile = profile;
        let fs = Fs::memory();
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        let checkout = runtime.block_on(async {
            let cancellation = CancellationToken::new();
            let volume = fs
                .create_volume(config, WorkBudget::UNBOUNDED, &cancellation)
                .await?
                .value;
            volume
                .checkout(
                    GenerationSelector::Head,
                    CheckoutMode {
                        access: AccessMode::ReadWrite,
                        consistency: ConsistencyMode::Pinned,
                        mutations: MutationMode::PrivateOverlay,
                    },
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
                .map(|receipt| receipt.value)
        })?;
        let checkout = Arc::new(SharedCheckout::with_publication(checkout, publication));
        Ok((
            CheckoutMountSource::new(Arc::clone(&checkout), config)?,
            CheckoutMountSource::new(checkout, config)?,
        ))
    }

    fn native_test_path(name: &str) -> MountPath {
        #[cfg(target_os = "windows")]
        let bytes = name.encode_utf16().flat_map(u16::to_le_bytes).collect();
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        let bytes = name.as_bytes().to_vec();
        MountPath::root().child(bytes)
    }

    #[test]
    fn callback_runtime_is_safe_inside_multi_and_current_thread_runtimes()
    -> Result<(), Box<dyn std::error::Error>> {
        let callback = CallbackRuntime::create()?;
        let multi = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .build()?;
        let current = tokio::runtime::Builder::new_current_thread().build()?;
        let profile = if cfg!(target_os = "windows") {
            FilesystemProfile::Windows
        } else {
            FilesystemProfile::Posix
        };
        for runtime in [&multi, &current] {
            let source = source(profile)?;
            runtime.block_on(async {
                let value = callback.block_on(|| async {
                    // This future is deliberately non-Send across a real reactor wait.
                    let local = std::rc::Rc::new(42_u8);
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    *local
                })?;
                assert_eq!(value, 42);
                let path = native_test_path("runtime.bin");
                source.volume_id()?;
                let created = source.create_file(&path, metadata())?;
                source.write_range(&path, 0, Bytes::from_static(b"base"))?;
                let attached = source.open_file(&path)?;
                attached.write_range(4, Bytes::from_static(b"-open"))?;
                assert_eq!(attached.read_range(0, 9)?.as_ref(), b"base-open");
                let detached = source.detach_file(&path)?;
                source.remove(&path, Some(created.node.file_id))?;
                detached.write_range(0, Bytes::from_static(b"live"))?;
                assert_eq!(detached.read_range(0, 9)?.as_ref(), b"live-open");
                source.sync()?;
                Ok::<(), MountSourceError>(())
            })?;
        }
        Ok(())
    }

    #[test]
    fn publication_policies_are_exact_and_subtree_roots_do_not_scan_or_escape()
    -> Result<(), Box<dyn std::error::Error>> {
        let profile = if cfg!(target_os = "windows") {
            FilesystemProfile::Windows
        } else {
            FilesystemProfile::Posix
        };
        let (manual, _) = shared_sources_with_publication(profile, MountPublication::Manual)?;
        let initial = checkout_state(&manual)?.0;
        manual.create_directory(&native_test_path("src"), metadata())?;
        manual.flush()?;
        assert_eq!(checkout_state(&manual)?, (initial, true));
        manual.sync()?;
        let published = checkout_state(&manual)?.0;
        assert_ne!(published, initial);

        let config = {
            let mut config = VolumeConfig::portable(Lifecycle::Ephemeral);
            config.profile = profile;
            config
        };
        let root = NamespacePath::new(
            vec![manual.logical_name(native_test_path("src").components()[0].as_slice())?],
            config.limits,
        )?;
        let subtree = CheckoutMountSource::new_at(Arc::clone(&manual.checkout), config, root)?;
        let nested = native_test_path("nested.bin");
        subtree.create_file(&nested, metadata())?;
        assert!(manual.lookup(&native_test_path("nested.bin"))?.is_none());
        assert!(
            manual
                .lookup(&native_test_path("src"))?
                .is_some_and(|entry| entry.node.kind == MountNodeKind::Directory)
        );
        assert!(subtree.lookup(&nested)?.is_some());

        let (per_mutation, _) =
            shared_sources_with_publication(profile, MountPublication::PerMutation)?;
        let before = checkout_state(&per_mutation)?.0;
        per_mutation.create_file(&native_test_path("published.bin"), metadata())?;
        let (after, pending) = checkout_state(&per_mutation)?;
        assert_ne!(after, before);
        assert!(!pending);
        Ok(())
    }

    #[test]
    fn detached_mount_file_survives_last_binding_with_sparse_mutation_and_metadata()
    -> Result<(), Box<dyn std::error::Error>> {
        let profile = if cfg!(target_os = "windows") {
            FilesystemProfile::Windows
        } else {
            FilesystemProfile::Posix
        };
        let source = source(profile)?;
        let file = native_test_path("detached.bin");
        source.create_file(&file, metadata())?;
        source.resize(&file, 64)?;
        source.write_range(&file, 8, Bytes::from_static(b"visible"))?;

        let detached = source.detach_file(&file)?;
        let expected = source.lookup(&file)?.map(|lookup| lookup.node.file_id);
        source.remove(&file, expected)?;
        assert_eq!(source.lookup(&file)?, None);
        assert_eq!(detached.lookup()?.node.link_count, 0);
        assert_eq!(detached.read_range(8, 7)?.as_ref(), b"visible");

        detached.write_range(32, Bytes::from_static(b"open"))?;
        detached.allocate_range(16, 8, MountRangeAllocation::Preallocate { keep_size: true })?;
        let changed = FileMetadata {
            modified_ns: crate::kernel::MetadataField::Value(42),
            ..metadata()
        };
        detached.set_attributes(changed, Some(40))?;
        let lookup = detached.lookup()?;
        assert_eq!(lookup.node.logical_bytes, 40);
        assert_eq!(lookup.metadata, changed);
        assert_eq!(detached.read_range(32, 4)?.as_ref(), b"open");
        assert_eq!(detached.seek(0, MountSeekTarget::Data)?, Some(8));
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let attribute = b"user.acyclic.detached";
            detached.write_attribute(
                attribute,
                Bytes::from_static(b"retained"),
                MountAttributeWriteMode::Create,
            )?;
            assert_eq!(
                detached.read_attribute(attribute)?.as_deref(),
                Some(b"retained".as_slice())
            );
            let attributes = detached.list_attributes(None, 8)?;
            assert_eq!(attributes.names, vec![attribute.to_vec()]);
            detached.remove_attribute(attribute)?;
            assert_eq!(detached.read_attribute(attribute)?, None);
        }
        Ok(())
    }

    #[test]
    fn shared_checkout_fence_blocks_every_adapter_until_exact_resolution()
    -> Result<(), Box<dyn std::error::Error>> {
        let profile = if cfg!(target_os = "windows") {
            FilesystemProfile::Windows
        } else {
            FilesystemProfile::Posix
        };
        let (first, second) = shared_sources(profile)?;
        let operation_id = OperationId::new();
        first.runtime.block_on(|| async {
            first
                .checkout
                .lock()
                .await
                .retain_operation_id(operation_id)
        })??;
        let path = native_test_path("fenced.bin");
        assert!(matches!(
            second.create_file(&path, metadata()),
            Err(MountSourceError::Stale)
        ));
        first.runtime.block_on(|| async {
            first
                .checkout
                .lock()
                .await
                .clear_retained_operation(operation_id);
        })?;
        second.create_file(&path, metadata())?;
        Ok(())
    }

    #[test]
    fn detached_sparse_transfer_preserves_holes_and_allocated_spans()
    -> Result<(), Box<dyn std::error::Error>> {
        let profile = if cfg!(target_os = "windows") {
            FilesystemProfile::Windows
        } else {
            FilesystemProfile::Posix
        };
        let source = source(profile)?;
        let from = native_test_path("sparse-source.bin");
        let to = native_test_path("sparse-destination.bin");
        source.create_file(&from, metadata())?;
        source.resize(&from, 128)?;
        source.write_range(&from, 64, Bytes::from_static(b"payload"))?;
        source.create_file(&to, metadata())?;
        source.write_range(&to, 0, Bytes::from_static(b"dense"))?;
        let from = source.detach_file(&from)?;
        let to = source.detach_file(&to)?;
        let sparse = from.read_sparse_range(0, 128)?;
        to.write_sparse_range(0, &sparse)?;
        assert_eq!(to.lookup()?.node.logical_bytes, 128);
        assert_eq!(to.seek(0, MountSeekTarget::Data)?, Some(64));
        assert_eq!(to.seek(64, MountSeekTarget::Hole)?, Some(71));
        assert_eq!(to.read_range(64, 7)?.as_ref(), b"payload");
        Ok(())
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn host_capture_preserves_unobservable_named_metadata() -> Result<(), Box<dyn std::error::Error>>
    {
        let source = source(FilesystemProfile::Posix)?;
        let path = native_test_path("metadata.bin");
        source.create_file(&path, metadata())?;
        source.write_attribute(
            &path,
            b"user.acyclic.retained",
            Bytes::from_static(b"opaque"),
            MountAttributeWriteMode::Create,
        )?;
        let root = tempfile::tempdir()?;
        std::fs::write(root.path().join("metadata.bin"), b"host")?;
        source.capture_host_path(root.path(), &path)?;
        assert_eq!(
            source
                .read_attribute(&path, b"user.acyclic.retained")?
                .as_deref(),
            Some(b"opaque".as_slice())
        );
        Ok(())
    }

    #[test]
    fn attached_open_file_tracks_identity_across_unlink_hard_link_and_rename()
    -> Result<(), Box<dyn std::error::Error>> {
        let profile = if cfg!(target_os = "windows") {
            FilesystemProfile::Windows
        } else {
            FilesystemProfile::Posix
        };
        let source = source(profile)?;
        let original = native_test_path("original.bin");
        let alias = native_test_path("alias.bin");
        let renamed = native_test_path("renamed.bin");
        let created = source.create_file(&original, metadata())?;
        source.write_range(&original, 0, Bytes::from_static(b"base"))?;
        let open = source.open_file(&original)?;
        source.hard_link(&original, &alias)?;
        source.remove(&original, Some(created.node.file_id))?;

        assert_eq!(source.lookup(&original)?, None);
        assert_eq!(open.lookup()?.node.link_count, 1);
        open.write_range(4, Bytes::from_static(b"-open"))?;
        assert_eq!(source.read_range(&alias, 0, 9)?.as_ref(), b"base-open");

        source.rename(&alias, &renamed, false)?;
        open.write_range(0, Bytes::from_static(b"live"))?;
        assert_eq!(source.lookup(&alias)?, None);
        assert_eq!(source.read_range(&renamed, 0, 9)?.as_ref(), b"live-open");
        assert_eq!(open.lookup()?.node.file_id, created.node.file_id);

        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let name = b"user.acyclic.attached";
            open.write_attribute(
                name,
                Bytes::from_static(b"identity"),
                MountAttributeWriteMode::Create,
            )?;
            assert_eq!(
                open.read_attribute(name)?.as_deref(),
                Some(b"identity".as_slice())
            );
            assert_eq!(
                source.read_attribute(&renamed, name)?.as_deref(),
                Some(b"identity".as_slice())
            );
        }
        Ok(())
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn sparse_seek_and_special_nodes_preserve_canonical_native_semantics()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = source(FilesystemProfile::Posix)?;
        let file = MountPath::root().child(b"sparse".to_vec());
        source.create_file(&file, metadata())?;
        source.resize(&file, 100)?;
        source.write_range(&file, 20, Bytes::from_static(b"data"))?;
        assert_eq!(source.seek(&file, 0, MountSeekTarget::Data)?, Some(20));
        assert_eq!(source.seek(&file, 20, MountSeekTarget::Hole)?, Some(24));
        assert_eq!(source.seek(&file, 24, MountSeekTarget::Data)?, None);
        assert_eq!(source.seek(&file, 100, MountSeekTarget::Hole)?, Some(100));

        let fifo = MountPath::root().child(b"fifo".to_vec());
        let created = source.create_special(&fifo, MountNodeKind::Fifo, None, metadata())?;
        assert_eq!(created.node.kind, MountNodeKind::Fifo);
        let device = MountPath::root().child(b"device".to_vec());
        let created = source.create_special(
            &device,
            MountNodeKind::CharacterDevice,
            Some((12, 34)),
            metadata(),
        )?;
        assert_eq!(created.node.device, Some((12, 34)));
        assert_eq!(source.lookup(&device)?, Some(created));
        Ok(())
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn windows_mount_path_preserves_exact_utf16_components()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = source(FilesystemProfile::Windows)?;
        let (initial, pending) = checkout_state(&source)?;
        assert!(!pending);
        let name = "exact-α-😀"
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect::<Vec<_>>();
        let path = MountPath::root().child(name.clone());
        let exact_metadata = FileMetadata {
            windows_attributes: crate::kernel::MetadataField::Value(0x20),
            created_ns: crate::kernel::MetadataField::Value(1_234_500),
            modified_ns: crate::kernel::MetadataField::Value(2_345_600),
            ..metadata()
        };
        let created = source.create_file(&path, exact_metadata)?;
        assert_eq!(source.lookup(&path)?, Some(created));
        let page = source.read_directory(&MountPath::root(), None, 8)?;
        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].name, name);
        assert_eq!(page.entries[0].metadata, exact_metadata);
        assert!(checkout_state(&source)?.1);
        source.flush()?;
        source.flush()?;
        let (published, pending) = checkout_state(&source)?;
        assert_ne!(published, initial);
        assert!(!pending);

        let host = tempfile::tempdir()?;
        let captured_text = "captured-β.bin";
        std::fs::write(host.path().join(captured_text), b"captured")?;
        let captured_name = captured_text
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect::<Vec<_>>();
        let captured_path = MountPath::root().child(captured_name);
        source.capture_host_path(host.path(), &captured_path)?;
        assert_eq!(
            source.read_range(&captured_path, 0, 8)?.as_ref(),
            b"captured"
        );
        std::fs::remove_file(host.path().join(captured_text))?;
        source.capture_host_path(host.path(), &captured_path)?;
        assert_eq!(source.lookup(&captured_path)?, None);
        Ok(())
    }

    #[cfg(target_os = "windows")]
    #[test]
    #[ignore = "requires the live Windows ProjFS optional feature"]
    fn writable_projfs_captures_closes_renames_links_and_deletes()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = Arc::new(source(FilesystemProfile::Windows)?);
        let temporary = tempfile::tempdir()?;
        let destination = temporary.path().join("projection");
        std::fs::create_dir(&destination)?;
        let mut session = crate::mount_native(
            crate::NativeMountRequest {
                mount_id: crate::MountId::new(),
                volume_id: source.volume_id()?,
                destination: destination.clone(),
                writable: true,
            },
            Arc::clone(&source) as Arc<dyn MountFilesystem>,
        )?;

        let first_name = "written-α.bin";
        let second_name = "renamed-β.bin";
        let linked_name = "linked-γ.bin";
        let write_status = std::process::Command::new("powershell.exe")
            .args([
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "[IO.File]::WriteAllBytes($env:ACYCLIC_FS_TEST_PATH, [Text.Encoding]::UTF8.GetBytes('projected'))",
            ])
            .env("ACYCLIC_FS_TEST_PATH", destination.join(first_name))
            .status()?;
        assert!(write_status.success());
        let first = windows_path(first_name);
        assert_eq!(source.read_range(&first, 0, 9)?.as_ref(), b"projected");

        let rename_status = std::process::Command::new("powershell.exe")
            .args([
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "[IO.File]::Move($env:ACYCLIC_FS_TEST_SOURCE, $env:ACYCLIC_FS_TEST_DESTINATION)",
            ])
            .env("ACYCLIC_FS_TEST_SOURCE", destination.join(first_name))
            .env("ACYCLIC_FS_TEST_DESTINATION", destination.join(second_name))
            .status()?;
        assert!(rename_status.success());
        let second = windows_path(second_name);
        assert_eq!(source.lookup(&first)?, None);
        assert!(source.lookup(&second)?.is_some());

        let link_status = std::process::Command::new("powershell.exe")
            .args([
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "New-Item -ItemType HardLink -Path $env:ACYCLIC_FS_TEST_DESTINATION -Target $env:ACYCLIC_FS_TEST_SOURCE | Out-Null",
            ])
            .env("ACYCLIC_FS_TEST_SOURCE", destination.join(second_name))
            .env("ACYCLIC_FS_TEST_DESTINATION", destination.join(linked_name))
            .status()?;
        assert!(link_status.success());
        let linked = windows_path(linked_name);
        let second_id = source
            .lookup(&second)?
            .ok_or("renamed source absent")?
            .node
            .file_id;
        assert_eq!(
            source
                .lookup(&linked)?
                .ok_or("hard link absent")?
                .node
                .file_id,
            second_id
        );

        let delete_status = std::process::Command::new("powershell.exe")
            .args([
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "[IO.File]::Delete($env:ACYCLIC_FS_TEST_PATH)",
            ])
            .env("ACYCLIC_FS_TEST_PATH", destination.join(second_name))
            .status()?;
        assert!(delete_status.success());
        assert_eq!(source.lookup(&second)?, None);
        assert!(source.lookup(&linked)?.is_some());
        assert!(session.stop()?);
        Ok(())
    }

    #[cfg(target_os = "windows")]
    fn windows_path(value: &str) -> MountPath {
        MountPath::root().child(value.encode_utf16().flat_map(u16::to_le_bytes).collect())
    }

    // Two independent volumes, mounted one after the other by fully
    // sequential Rust calls (`mount_native(a)` returns before
    // `mount_native(b)` is even called). Written to be a control for the
    // concurrent-start test below, on the assumption that only literal
    // thread-level concurrency could trigger the vendor race in BUGS.md.
    // That assumption is false: a live run of this exact test reproduced
    // the race anyway (`fuse-t.log`, 2026-08-31T15:35:18+01:00 — the second
    // `go-nfsv4` helper hit "Failed to listen: 52100" once, immediately
    // after the first helper's own `mount [-o port=52100,...]` line, and
    // never advanced to 52101 before dying 10s later at "receiveRequest
    // error: EOF"), even though this call sequence has no thread overlap
    // and ports 52101-52105 were free. So the trigger is proximity of
    // `go-nfsv4` *process* start time, not concurrency in this crate's own
    // Rust call graph — this test is not a reliable pass/fail control and
    // is expected to be flaky until `FuseTSession::start` staggers/gates
    // session starts (see BUGS.md's go-nfsv4 startup-race note).
    #[cfg(target_os = "macos")]
    #[test]
    #[ignore = "mounts two live FUSE-T sessions in one process; run manually with \
                `cargo test -p acyclic-fs-mount -- --ignored fuse_t_mount`"]
    fn two_sequential_fuse_t_mounts_in_one_process_both_stay_independently_writable()
    -> Result<(), Box<dyn std::error::Error>> {
        let source_a = Arc::new(source(FilesystemProfile::Posix)?);
        let source_b = Arc::new(source(FilesystemProfile::Posix)?);
        let temporary_a = tempfile::tempdir()?;
        let temporary_b = tempfile::tempdir()?;

        let mut mount_a = crate::mount_native(
            crate::NativeMountRequest {
                mount_id: crate::MountId::new(),
                volume_id: source_a.volume_id()?,
                destination: temporary_a.path().to_path_buf(),
                writable: true,
            },
            Arc::clone(&source_a) as Arc<dyn MountFilesystem>,
        )?;
        let mut mount_b = crate::mount_native(
            crate::NativeMountRequest {
                mount_id: crate::MountId::new(),
                volume_id: source_b.volume_id()?,
                destination: temporary_b.path().to_path_buf(),
                writable: true,
            },
            Arc::clone(&source_b) as Arc<dyn MountFilesystem>,
        )?;

        std::fs::write(temporary_a.path().join("a.txt"), b"from-a")?;
        std::fs::write(temporary_b.path().join("b.txt"), b"from-b")?;
        assert_eq!(std::fs::read(temporary_a.path().join("a.txt"))?, b"from-a");
        assert_eq!(std::fs::read(temporary_b.path().join("b.txt"))?, b"from-b");

        assert!(mount_a.stop()?);
        assert!(mount_b.stop()?);
        Ok(())
    }

    // Reproduces the vendor-level FUSE-T race documented in BUGS.md: two
    // `go-nfsv4` helpers launched milliseconds apart in the same process can
    // both probe the shared 52100-52105 port pool from stale state, so the
    // second helper reports "Failed to listen: 52100" once and never walks
    // to 52101, leaving its mount to die silently at FUSE_INIT until this
    // driver's own 10-second visibility deadline expires. Unlike the
    // sequential test above, this one starting both mounts from the same
    // instant is expected to be flaky pending a same-process staggering fix
    // in `FuseTSession::start`; it exists to make that race reproducible on
    // demand, not to gate CI.
    #[cfg(target_os = "macos")]
    #[test]
    #[ignore = "mounts two live FUSE-T sessions concurrently in one process to reproduce a \
                vendor startup race; run manually with \
                `cargo test -p acyclic-fs-mount -- --ignored fuse_t_mount`"]
    fn two_concurrent_fuse_t_mounts_in_one_process_both_become_visible() {
        let source_a = Arc::new(source(FilesystemProfile::Posix).expect("volume a"));
        let source_b = Arc::new(source(FilesystemProfile::Posix).expect("volume b"));
        let temporary_a = tempfile::tempdir().expect("tempdir a");
        let temporary_b = tempfile::tempdir().expect("tempdir b");
        let request_a = crate::NativeMountRequest {
            mount_id: crate::MountId::new(),
            volume_id: source_a.volume_id().expect("volume id a"),
            destination: temporary_a.path().to_path_buf(),
            writable: true,
        };
        let request_b = crate::NativeMountRequest {
            mount_id: crate::MountId::new(),
            volume_id: source_b.volume_id().expect("volume id b"),
            destination: temporary_b.path().to_path_buf(),
            writable: true,
        };
        let dyn_source_a = Arc::clone(&source_a) as Arc<dyn MountFilesystem>;
        let dyn_source_b = Arc::clone(&source_b) as Arc<dyn MountFilesystem>;

        let (result_a, result_b) = std::thread::scope(|scope| {
            let handle_a = scope.spawn(|| crate::mount_native(request_a, dyn_source_a));
            let handle_b = scope.spawn(|| crate::mount_native(request_b, dyn_source_b));
            (
                handle_a.join().expect("mount a thread panicked"),
                handle_b.join().expect("mount b thread panicked"),
            )
        });

        let a_ok = result_a.is_ok();
        let b_ok = result_b.is_ok();
        if let Err(error) = &result_a {
            eprintln!("mount a failed: {error}");
        }
        if let Err(error) = &result_b {
            eprintln!("mount b failed: {error}");
        }
        let mut sessions = Vec::new();
        if let Ok(session) = result_a {
            sessions.push(session);
        }
        if let Ok(session) = result_b {
            sessions.push(session);
        }
        for mut session in sessions {
            let _ = session.stop();
        }
        assert!(
            a_ok && b_ok,
            "expected both concurrent FUSE-T mounts to become visible; see BUGS.md's \
             go-nfsv4 startup-race note if this fails"
        );
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn posix_mount_path_preserves_non_utf8_components() -> Result<(), Box<dyn std::error::Error>> {
        let source = source(FilesystemProfile::Posix)?;
        let (initial, pending) = checkout_state(&source)?;
        assert!(!pending);
        let name = vec![b'r', 0xff, b'w'];
        let path = MountPath::root().child(name.clone());
        let created = source.create_file(&path, metadata())?;
        assert_eq!(source.lookup(&path)?, Some(created));
        source.write_attribute(
            &path,
            b"user.acyclic",
            Bytes::from_static(b"native-xattr"),
            MountAttributeWriteMode::Create,
        )?;
        assert_eq!(
            source.read_attribute(&path, b"user.acyclic")?.as_deref(),
            Some(b"native-xattr".as_slice())
        );
        let attributes = source.list_attributes(&path, None, 8)?;
        assert_eq!(attributes.names, vec![b"user.acyclic".to_vec()]);
        assert_eq!(attributes.next_cursor, None);
        source.remove_attribute(&path, b"user.acyclic")?;
        assert_eq!(source.read_attribute(&path, b"user.acyclic")?, None);
        let page = source.read_directory(&MountPath::root(), None, 8)?;
        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].name, name);
        assert!(checkout_state(&source)?.1);
        source.flush()?;
        source.flush()?;
        let (published, pending) = checkout_state(&source)?;
        assert_ne!(published, initial);
        assert!(!pending);
        Ok(())
    }

    // The FUSE release callback flushes (seals) the checkout before the
    // kernel's next namespace mutation arrives, so mutations must stay
    // valid across a publication boundary.
    #[test]
    fn rename_after_flush_preserves_namespace() -> Result<(), Box<dyn std::error::Error>> {
        let profile = if cfg!(target_os = "windows") {
            FilesystemProfile::Windows
        } else {
            FilesystemProfile::Posix
        };
        let source = source(profile)?;
        let path = native_test_path("payload.bin");
        source.create_file(&path, metadata())?;
        let open = source.open_file(&path)?;
        open.write_range(0, Bytes::from_static(b"BRAVO"))?;
        drop(open);
        source.flush()?;
        source.flush()?;
        let renamed = native_test_path("renamed.bin");
        source.rename(&path, &renamed, true)?;
        assert!(source.lookup(&path)?.is_none());
        let landed = source.lookup(&renamed)?;
        assert!(landed.is_some());
        Ok(())
    }

    #[test]
    fn preexisting_entry_survives_published_link_unlink_and_recreate_cycles()
    -> Result<(), Box<dyn std::error::Error>> {
        let profile = if cfg!(target_os = "windows") {
            FilesystemProfile::Windows
        } else {
            FilesystemProfile::Posix
        };
        let source = source(profile)?;
        let seed = native_test_path("shared-seed.bin");
        source.create_file(&seed, metadata())?;
        source.write_range(&seed, 0, Bytes::from_static(b"seed"))?;
        source.flush()?;

        let payload = native_test_path("payload.bin");
        source.create_file(&payload, metadata())?;
        source.write_range(&payload, 0, Bytes::from_static(b"BRAVO"))?;
        source.flush()?;
        let renamed = native_test_path("renamed.bin");
        source.rename(&payload, &renamed, true)?;
        let link = native_test_path("link.bin");
        source.hard_link(&renamed, &link)?;
        let renamed_id = source
            .lookup(&renamed)?
            .ok_or("renamed file is absent")?
            .node
            .file_id;
        source.remove(&renamed, Some(renamed_id))?;
        let link_id = source
            .lookup(&link)?
            .ok_or("linked file is absent")?
            .node
            .file_id;
        source.remove(&link, Some(link_id))?;
        source.create_file(&link, metadata())?;
        source.write_range(&link, 0, Bytes::from_static(b"replacement"))?;
        source.flush()?;

        assert_eq!(source.read_range(&seed, 0, 4)?.as_ref(), b"seed");
        let page = source.read_directory(&MountPath::root(), None, 16)?;
        let mut names = page
            .entries
            .into_iter()
            .map(|entry| entry.name)
            .collect::<Vec<_>>();
        names.sort();
        assert_eq!(
            names,
            vec![
                native_test_path("link.bin").components()[0].clone(),
                native_test_path("shared-seed.bin").components()[0].clone(),
            ]
        );
        assert_eq!(page.next_cursor, None);
        Ok(())
    }

    // Host-side capture of raw-byte names requires a host filesystem that
    // admits them; APFS enforces valid UTF-8 and rejects such names with
    // EILSEQ before this crate is involved, so the host half runs on Linux
    // only.
    #[cfg(target_os = "linux")]
    #[test]
    fn host_capture_preserves_non_utf8_names() -> Result<(), Box<dyn std::error::Error>> {
        let source = source(FilesystemProfile::Posix)?;
        let host = tempfile::tempdir()?;
        let captured_name = vec![b'c', 0xfe, b'p'];
        let host_path = host
            .path()
            .join(std::ffi::OsString::from_vec(captured_name.clone()));
        std::fs::write(&host_path, b"captured")?;
        let captured_path = MountPath::root().child(captured_name);
        source.capture_host_path(host.path(), &captured_path)?;
        assert_eq!(
            source.read_range(&captured_path, 0, 8)?.as_ref(),
            b"captured"
        );
        std::fs::remove_file(host_path)?;
        source.capture_host_path(host.path(), &captured_path)?;
        assert_eq!(source.lookup(&captured_path)?, None);
        Ok(())
    }
}
