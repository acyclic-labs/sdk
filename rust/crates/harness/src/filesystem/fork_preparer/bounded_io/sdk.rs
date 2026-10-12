//! Filesystem operations on the preparation context.
use super::*;
use crate::filesystem::{WorkspaceObservation, filesystem_key};
use acyclic_fs::{Generation, GenerationId, TransactionCommit, Workspace};

impl acyclic_fs::facade::FsWork for &PreparationIo {
    type Output<T, E> = (
        std::result::Result<T, E>,
        std::result::Result<(), acyclic_fs::FsError>,
    );
    type OwnedOutput<T> = Self::Output<T, acyclic_fs::FsError>;
    fn finish_owned<T>(
        &self,
        result: std::result::Result<
            (T, acyclic_fs::kernel::KernelOwnedAllocation),
            acyclic_fs::FsError,
        >,
        accounting: &acyclic_fs::kernel::KernelAccounting<'_>,
    ) -> Self::OwnedOutput<T> {
        let result = result.and_then(|(value, allocation)| {
            #[cfg(target_arch = "wasm32")]
            let _ = allocation;
            #[cfg(not(target_arch = "wasm32"))]
            if let acyclic_fs::kernel::KernelOwnedAllocation::NativeOriginal(mut allocation) =
                allocation
            {
                let PreparationIo::Native { custody, .. } = self else {
                    drop(value);
                    return Err(acyclic_fs::FsError::NativeKernel(
                        acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
                    ));
                };
                if let Err(cause) = custody
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .absorb(&mut allocation)
                {
                    drop(value);
                    return Err(cause.into());
                }
            }
            Ok(value)
        });
        self.finish(result, accounting)
    }
    fn accounting<'a>(
        &'a self,
        cancellation: &'a CancellationToken,
    ) -> acyclic_fs::kernel::KernelAccounting<'a> {
        use acyclic_fs::kernel::KernelAccounting;
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            PreparationIo::Native { work, .. } => {
                KernelAccounting::NativeOriginal { work, cancellation }
            }
            PreparationIo::Owned { .. } => KernelAccounting::Standalone {
                allocations: Default::default(),
                work: WorkCounters::default(),
                budget: self
                    .remaining()
                    .unwrap_or_else(|_| acyclic_fs::performance::work_budget(0, 0)),
                cancellation,
            },
        }
    }
    fn finish<T, E>(
        &self,
        result: std::result::Result<T, E>,
        accounting: &acyclic_fs::kernel::KernelAccounting<'_>,
    ) -> Self::Output<T, E> {
        use acyclic_fs::kernel::KernelAccounting;
        let work = match accounting {
            KernelAccounting::Standalone { work, .. } => *work,
            KernelAccounting::BorrowedStandalone { work, .. } => **work,
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { .. } => return (result, Ok(())),
        };
        let recorded = self
            .spent(work)
            .and_then(|()| match (result.is_ok(), self) {
                (true, PreparationIo::Owned { budget, .. }) => {
                    self.receipt().verify(*budget).map_err(Into::into)
                }
                _ => Ok(()),
            });
        (result, recorded)
    }
}

impl PreparationIo {
    pub(crate) fn sdk<T, E: Into<Error>>(
        &self,
        (result, recorded): (
            std::result::Result<T, E>,
            std::result::Result<(), acyclic_fs::FsError>,
        ),
    ) -> Result<T> {
        recorded?;
        result.map_err(Into::into)
    }
}

pub(in crate::filesystem) async fn open<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>,
    reference: &WorkspaceRef,
    io: &PreparationIo,
) -> Result<Workspace<A, O>> {
    reference.validate(&mut Some(&mut io.history_io()))?;
    if reference.as_resource().provider() != &host.provider {
        return Err(io.invalid("resource belongs to another Filesystem provider"));
    }
    let name = std::str::from_utf8(reference.as_resource().key())
        .map_err(|_| io.invalid("workspace reference key must be UTF-8"))?;
    io.sdk(
        host.filesystem
            .open_workspace(name, io, io.cancellation())
            .await,
    )
}

pub(in crate::filesystem) async fn generation<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>,
    workspace: &Workspace<A, O>,
    reference: &crate::resources::GenerationRef,
    io: &PreparationIo,
) -> Result<Generation<A, O>> {
    reference.validate(&mut Some(&mut io.history_io()))?;
    if reference.as_resource().provider() != &host.provider {
        return Err(io.invalid("resource belongs to another Filesystem provider"));
    }
    let key = reference
        .as_resource()
        .key()
        .try_into()
        .map_err(|_| io.invalid("generation reference key must be 32 bytes"))?;
    io.remaining()?;
    io.sdk(
        workspace
            .generation(
                GenerationId::new(acyclic_fs::Digest::from_bytes(key)),
                io,
                io.cancellation(),
            )
            .await,
    )
}

pub(in crate::filesystem) async fn operation_generation<
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
>(
    host: &FilesystemHost<A, O>,
    reference: &WorkspaceRef,
    key: &IdempotencyKey,
    io: Option<&PreparationIo>,
) -> Result<Option<Generation<A, O>>> {
    let Some(io) = io else {
        return host
            .open(reference)
            .await?
            .operation_generation(
                filesystem_key(key),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| map_error(failure.error));
    };
    let workspace = open(host, reference, io).await?;
    let key = operation_key(key, Some(io))?;
    io.remaining()?;
    io.sdk(
        workspace
            .operation_generation(key, io, io.cancellation())
            .await,
    )
}

pub(in crate::filesystem) async fn read<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>,
    reference: &WorkspaceRef,
    path: &str,
    maximum: u64,
    io: Option<&PreparationIo>,
) -> Result<acyclic_stream::OwnedBytes> {
    let Some(io) = io else {
        return host.read(reference, None, path, maximum).await;
    };
    let workspace = open(host, reference, io).await?;
    io.remaining()?;
    let bytes = io.sdk(workspace.read(path, maximum, io, io.cancellation()).await)?;
    if io.original_native_work().is_none() {
        io.retain_owned(bytes.retained_capacity().unwrap_or(0) as u64)?;
    }
    Ok(bytes)
}

pub(in crate::filesystem) async fn resolve<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>,
    reference: &WorkspaceRef,
    io: Option<&PreparationIo>,
) -> Result<WorkspaceObservation> {
    let Some(io) = io else {
        return host.resolve(reference).await;
    };
    let workspace = open(host, reference, io).await?;
    io.remaining()?;
    let head = io.sdk(workspace.head(io, io.cancellation()).await)?;
    let copied = crate::executor::with_history_io(Some(io), |work| {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let copied = crate::core::copy_resource(reference.as_resource(), &mut quota)?;
        let copied = WorkspaceRef::from_resource_with_work(copied, quota.work())?;
        quota.transfer();
        Ok::<_, Error>(copied)
    })?;
    Ok(WorkspaceObservation {
        workspace: copied,
        generation: generation_ref(&host.provider, head.id(), Some(io))?,
    })
}

pub(in crate::filesystem) fn generation_ref(
    provider: &ProviderRef,
    generation: GenerationId,
    io: Option<&PreparationIo>,
) -> Result<crate::resources::GenerationRef> {
    crate::executor::with_history_io(io, |work| {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let provider = crate::core::copy_provider(provider, &mut quota)?;
        let key = crate::core::copy_copy_vector(&generation.digest().into_bytes(), &mut quota)?;
        let resource = ResourceRef::new(
            crate::resources::ResourceKind::Generation,
            provider,
            key,
            None,
            quota.work(),
        )?;
        let reference =
            crate::resources::GenerationRef::from_resource_with_work(resource, quota.work())?;
        quota.transfer();
        Ok(reference)
    })
}

pub(in crate::filesystem) async fn create<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>,
    name: &str,
    key: &IdempotencyKey,
    io: Option<&PreparationIo>,
    preparation: Option<&Arc<TaskResourcePreparation>>,
) -> Result<()> {
    let lifecycle = if host.filesystem.capabilities().durable {
        acyclic_fs::Lifecycle::Durable
    } else {
        acyclic_fs::Lifecycle::Ephemeral
    };
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
        let guard = Arc::clone(preparation.ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "native creation requires its original issued task guard",
        })?);
        guard
            .verify_creation_with_native_work(io.original_native_work().unwrap())?
            .await?;
        let mut before = acyclic_fs::NativeAuthorityGuard(
            move |work: &acyclic_fs::NativeMaterializationWork| {
                let work = work.clone();
                let guard = Arc::clone(&guard);
                async move { guard.verify_creation_with_native_work(&work)?.await }
            },
        );
        let (result, recorded) = host
            .filesystem
            .create_workspace_with_operation::<_, _, Error>(
                name,
                acyclic_fs::VolumeConfig::portable(lifecycle),
                acyclic_fs::OperationId::from_bytes(operation_key(key, Some(io))?.into_bytes()),
                io,
                io.cancellation(),
                Some(&mut before),
            )
            .await;
        recorded?;
        result?;
        return Ok(());
    }
    if let Some(task) = preparation {
        task.verify_creation().await?;
    }
    if let Some(io) = io {
        io.sdk(
            host.filesystem
                .create_workspace_with_operation::<_, (), WorkspaceError>(
                    name,
                    acyclic_fs::VolumeConfig::portable(lifecycle),
                    acyclic_fs::OperationId::from_bytes(operation_key(key, Some(io))?.into_bytes()),
                    io,
                    io.cancellation(),
                    None,
                )
                .await,
        )?;
    } else {
        host.filesystem
            .create_workspace(name, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| map_error(failure.error))?;
    }
    if let Some(task) = preparation {
        task.verify_creation().await?;
    }
    Ok(())
}

pub(in crate::filesystem) async fn apply_generation_task_guarded<
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
>(
    host: &FilesystemHost<A, O>,
    reference: &WorkspaceRef,
    expected: Option<&crate::resources::GenerationRef>,
    mutations: &[WorkspaceMutation],
    key: &IdempotencyKey,
    io: Option<&PreparationIo>,
    preparation: Option<&TaskResourcePreparation>,
    #[cfg(not(target_arch = "wasm32"))] native_guard: Option<&Arc<TaskResourcePreparation>>,
) -> Result<Generation<A, O>> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
        let guard = Arc::clone(native_guard.ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "task-bound native mutation requires its original issued creation guard",
        })?);
        let mut before = move |work: &acyclic_fs::NativeMaterializationWork| {
            let work = work.clone();
            let guard = Arc::clone(&guard);
            async move { guard.verify_creation_with_native_work(&work)?.await }
        };
        return publish_native(host, reference, expected, mutations, key, io, &mut before).await;
    }
    publish_generation(
        host,
        reference,
        expected,
        mutations,
        key,
        io,
        &mut |_| async move {
            let result = match preparation {
                Some(task) => task.verify_creation().await,
                None => Ok(()),
            };
            result
                .map(|value| OperationReceipt {
                    value,
                    work: WorkCounters::default(),
                })
                .map_err(OperationFailure::before_work)
        },
    )
    .await
}

pub(in crate::filesystem) async fn apply_generation<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>,
    reference: &WorkspaceRef,
    expected: Option<&crate::resources::GenerationRef>,
    mutations: &[WorkspaceMutation],
    key: &IdempotencyKey,
    io: &PreparationIo,
) -> Result<Generation<A, O>> {
    #[cfg(not(target_arch = "wasm32"))]
    if io.original_native_work().is_some() {
        return publish_native(
            host,
            reference,
            expected,
            mutations,
            key,
            io,
            &mut |_: &acyclic_fs::NativeMaterializationWork| std::future::ready(Ok(())),
        )
        .await;
    }
    publish_generation(
        host,
        reference,
        expected,
        mutations,
        key,
        Some(io),
        &mut |_| {
            std::future::ready(Ok(OperationReceipt {
                value: (),
                work: WorkCounters::default(),
            }))
        },
    )
    .await
}

fn committed<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    outcome: TransactionCommit<A, O>,
    io: &PreparationIo,
) -> Result<Generation<A, O>> {
    use TransactionCommit::*;
    match outcome {
        Committed(generation) | AlreadyCommitted(generation) => Ok(generation),
        Conflict { .. } => Err(stock_boundary(
            Some(io),
            crate::wire::ErrorCode::Conflict,
            "workspace generation changed",
        )),
        Fenced => Err(stock_boundary(
            Some(io),
            crate::wire::ErrorCode::Conflict,
            "workspace writer was fenced",
        )),
        IdempotencyConflict => Err(stock_boundary(
            Some(io),
            crate::wire::ErrorCode::Conflict,
            "filesystem idempotency key belongs to another mutation",
        )),
    }
}

async fn mutation<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    transaction: &mut acyclic_fs::Transaction<A, O>,
    value: &WorkspaceMutation,
    io: &PreparationIo,
) -> Result<()> {
    io.remaining()?;
    match value {
        WorkspaceMutation::CreateDirectory { path } => io.sdk(
            transaction
                .create_dir_all(path, io, io.cancellation())
                .await,
        ),
        WorkspaceMutation::PutFile { path, bytes } => io.sdk(
            transaction
                .write(path, bytes.clone(), io, io.cancellation())
                .await,
        ),
        WorkspaceMutation::Remove { .. } => Err(stock_boundary(
            Some(io),
            crate::wire::ErrorCode::Unsupported,
            "bounded publication does not remove content",
        )),
    }
}

pub(in crate::filesystem) async fn publish_generation<A, O, F, Fut>(
    host: &FilesystemHost<A, O>,
    reference: &WorkspaceRef,
    expected: Option<&crate::resources::GenerationRef>,
    mutations: &[WorkspaceMutation],
    key: &IdempotencyKey,
    io: Option<&PreparationIo>,
    before: &mut F,
) -> Result<Generation<A, O>>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    F: FnMut(WorkBudget) -> Fut + Send,
    Fut: Future<Output = acyclic_fs::MeasuredResult<OperationReceipt<()>, Error>> + Send,
{
    let Some(io) = io else {
        return host
            .apply_generation_guarded(reference, expected, mutations, key, &mut || {
                let future = before(WorkBudget::UNBOUNDED);
                async move {
                    future
                        .await
                        .map(|receipt| receipt.value)
                        .map_err(|failure| failure.error)
                }
            })
            .await;
    };
    let workspace = open(host, reference, io).await?;
    io.remaining()?;
    let pinned = match expected {
        Some(reference) => generation(host, &workspace, reference, io).await?,
        None => io.sdk(workspace.head(io, io.cancellation()).await)?,
    };
    let mut transaction = io.sdk(
        workspace
            .begin_pinned_transaction(&pinned, filesystem_key(key), io, io.cancellation())
            .await,
    )?;
    for value in mutations {
        mutation(&mut transaction, value, io).await?;
    }
    committed(
        io.receive_content(
            transaction
                .commit(
                    None,
                    acyclic_fs::PublicationPermit::Unrestricted,
                    io.remaining()?,
                    io.cancellation(),
                    before,
                )
                .await,
        )?,
        io,
    )
}

#[cfg(not(target_arch = "wasm32"))]
pub(in crate::filesystem) async fn publish_native<A, O, F, Fut>(
    host: &FilesystemHost<A, O>,
    reference: &WorkspaceRef,
    expected: Option<&crate::resources::GenerationRef>,
    mutations: &[WorkspaceMutation],
    key: &IdempotencyKey,
    io: &PreparationIo,
    before: &mut F,
) -> Result<Generation<A, O>>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    F: for<'m> FnMut(&'m acyclic_fs::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    let work = io
        .original_native_work()
        .ok_or_else(|| io.invalid("native publication requires its original accounting scope"))?;
    before(work).await?;
    let workspace = open(host, reference, io).await?;
    let pinned = match expected {
        Some(reference) => generation(host, &workspace, reference, io).await?,
        None => io.sdk(workspace.head(io, io.cancellation()).await)?,
    };
    let key = operation_key(key, Some(io))?;
    let mut transaction = io.sdk(
        workspace
            .begin_pinned_transaction(&pinned, key, io, io.cancellation())
            .await,
    )?;
    for value in mutations {
        before(work).await?;
        mutation(&mut transaction, value, io).await?;
    }
    committed(
        transaction
            .commit_with_native_work(
                None,
                acyclic_fs::PublicationPermit::Unrestricted,
                work,
                io.cancellation(),
                before,
            )
            .await?,
        io,
    )
}

pub(in crate::filesystem) async fn retain<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    generation: &Generation<A, O>,
    io: Option<&PreparationIo>,
) -> std::result::Result<(), acyclic_fs::LazyWorkspaceError> {
    use acyclic_fs::LazyWorkspaceError as LazyError;
    let encoding = WorkCounters {
        bytes_encoded: 64,
        bytes_copied: 83,
        ..Default::default()
    };
    if let Some(io) = io {
        io.preflight(encoding).map_err(LazyError::Filesystem)?;
    }
    let hex = blake3::Hash::from_bytes(generation.id().digest().into_bytes()).to_hex();
    let mut label = [0_u8; 83];
    label[..19].copy_from_slice(b"harness-generation-");
    label[19..].copy_from_slice(hex.as_bytes());
    let label = std::str::from_utf8(&label).expect("ASCII generation identity");
    let Some(io) = io else {
        return generation
            .pin(label, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map(|_| ())
            .map_err(|failure| LazyError::WorkspaceCause(failure.error));
    };
    io.spent(encoding).map_err(LazyError::Filesystem)?;
    io.remaining().map_err(LazyError::Filesystem)?;
    let (result, recorded) = generation.pin(label, io, io.cancellation()).await;
    match result {
        Err(source) => Err(LazyError::WorkspaceCause(source)),
        Ok(_) => recorded.map_err(LazyError::Filesystem),
    }
}

pub(in crate::filesystem) async fn fork<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>,
    source: &VolumeRef,
    grant: &ContentGrant,
    selected: &crate::resources::GenerationRef,
    child: &VolumeRef,
    paths: Option<Vec<String>>,
    key: &IdempotencyKey,
    io: Option<&PreparationIo>,
    preparation: Option<&Arc<TaskResourcePreparation>>,
) -> Result<WorkspaceObservation> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(work) = io.and_then(PreparationIo::original_native_work) {
        preparation
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "task-bound native fork requires its original issued creation guard",
            })?
            .verify_creation_with_native_work(work)?
            .await?;
    } else if let Some(task) = preparation {
        task.verify_creation().await?;
    }
    #[cfg(target_arch = "wasm32")]
    if let Some(task) = preparation {
        task.verify_creation().await?;
    }
    grant.require(source, VolumeOperation::Read, io)?;
    let Some(io) = io else {
        let observed = host
            .fork_volume_pinned(source, selected, child, paths, key)
            .await?;
        if let Some(task) = preparation {
            task.verify_creation().await?;
        }
        return Ok(observed);
    };
    source.validate(&mut Some(&mut io.history_io()))?;
    child.validate(&mut Some(&mut io.history_io()))?;
    if source.provider() != &host.provider || child.provider() != &host.provider || source == child
    {
        return Err(io.invalid("volume fork has a foreign or identical destination"));
    }
    let reference = content_workspace(&host.provider, source, Some(io))?;
    let workspace = open(host, &reference, io).await?;
    let pinned = generation(host, &workspace, selected, io).await?;
    retain(&pinned, Some(io)).await?;
    let name = volume_storage_name(child, Some(io))?;
    let operation = operation_key(key, Some(io))?;
    let options = acyclic_fs::ForkOptions::from_generation(pinned, operation);
    let options = match paths {
        Some(paths) => options.inherit_paths(paths),
        None => options,
    };
    let forked = 'forked: {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.original_native_work() {
            let guard = Arc::clone(preparation.expect("native creation guard was verified"));
            let mut before = move |work: &acyclic_fs::NativeMaterializationWork| {
                let work = work.clone();
                let guard = Arc::clone(&guard);
                async move { guard.verify_creation_with_native_work(&work)?.await }
            };
            break 'forked workspace
                .fork_with_native_work(
                    &name,
                    options,
                    acyclic_fs::PublicationPermit::Unrestricted,
                    work,
                    io.cancellation(),
                    &mut before,
                )
                .await?;
        }
        io.receive(
            workspace
                .fork(
                    &name,
                    options,
                    acyclic_fs::PublicationPermit::Unrestricted,
                    io.remaining()?,
                    io.cancellation(),
                    |_| {
                        std::future::ready(Ok(OperationReceipt {
                            value: (),
                            work: WorkCounters::default(),
                        }))
                    },
                )
                .await,
        )?
    };
    io.remaining()?;
    let initial = io
        .sdk(
            forked
                .operation_generation(operation, io, io.cancellation())
                .await,
        )?
        .ok_or_else(|| {
            stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Storage,
                "fork creation has no durable original generation",
            )
        })?;
    retain(&initial, Some(io)).await?;
    let observed = WorkspaceObservation {
        workspace: WorkspaceRef::new(host.provider.clone_with_io(io)?, name.into_bytes(), None)?,
        generation: generation_ref(&host.provider, initial.id(), Some(io))?,
    };
    if io.original_native_work().is_none()
        && let Some(task) = preparation
    {
        task.verify_creation().await?;
    }
    Ok(observed)
}

pub(in crate::filesystem) fn operation_key(
    key: &IdempotencyKey,
    io: Option<&PreparationIo>,
) -> Result<acyclic_fs::IdempotencyKey> {
    let hashed = WorkCounters {
        bytes_hashed: key.as_str().len() as u64,
        ..Default::default()
    };
    if let Some(io) = io {
        io.preflight(hashed)?;
    }
    let key = crate::filesystem::filesystem_key(key);
    if let Some(io) = io {
        io.charge(hashed)?;
    }
    Ok(key)
}
pub(in crate::filesystem) fn content_workspace(
    provider: &ProviderRef,
    volume: &VolumeRef,
    io: Option<&PreparationIo>,
) -> Result<WorkspaceRef> {
    let name = volume_storage_name(volume, io)?;
    let provider = match io {
        Some(io) => provider.clone_with_io(io)?,
        None => provider.clone(),
    };
    WorkspaceRef::new(provider, name.into_bytes(), None)
}
pub(in crate::filesystem) fn volume_storage_name(
    volume: &VolumeRef,
    io: Option<&PreparationIo>,
) -> Result<String> {
    match io {
        Some(io) => io.capability(volume, "harness-"),
        None => volume.storage_name(&mut None),
    }
}

use crate::executor::stock_boundary;
