//! Filesystem operations on the preparation context.
use super::*;
use acyclic_fs::{Generation, GenerationId};

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
    #[doc(hidden)]
    pub fn sdk<T, E: Into<Error>>(
        &self,
        (result, recorded): (
            std::result::Result<T, E>,
            std::result::Result<(), acyclic_fs::FsError>,
        ),
    ) -> Result<T> {
        recorded?;
        result.map_err(|error| {
            let error = error.into();
            #[cfg(not(target_arch = "wasm32"))]
            if matches!(self, PreparationIo::Owned { budget, .. } if *budget == WorkBudget::UNBOUNDED) {
                return match error {
                    Error::NativeWorkspace { source, .. } => map_error(*source),
                    Error::NativeFilesystem { source, .. } => map_error(WorkspaceError::Filesystem(*source)),
                    error => error,
                };
            }
            error
        })
    }
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
        let reference = crate::resources::GenerationRef::from_resource(resource, quota.work())?;
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
    if io.is_some_and(|io| io.original_native_work().is_some()) && preparation.is_none() {
        return Err(stock_boundary(
            io,
            crate::wire::ErrorCode::Unauthorized,
            "native creation requires its original issued task guard",
        ));
    }
    if let Some(task) = preparation {
        task.verify(false, io)?.await?;
    }
    if let Some(io) = io {
        let mut guard = preparation.map(Arc::as_ref);
        io.sdk(
            host.filesystem
                .create_workspace_with_operation::<_, _, Error>(
                    name,
                    acyclic_fs::VolumeConfig::portable(lifecycle),
                    acyclic_fs::OperationId::from_bytes(operation_key(key, Some(io))?.into_bytes()),
                    io,
                    io.cancellation(),
                    guard.as_mut(),
                )
                .await,
        )?;
    } else {
        host.filesystem
            .create_workspace(name, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| map_error(failure.error))?;
    }
    if let Some(task) =
        preparation.filter(|_| io.is_none_or(|io| io.original_native_work().is_none()))
    {
        task.verify(false, io)?.await?;
    }
    Ok(())
}

pub(in crate::filesystem) async fn retain<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    generation: &Generation<A, O>,
    io: Option<&PreparationIo>,
) -> std::result::Result<(), acyclic_fs::LazyWorkspaceError> {
    #[cfg(all(test, not(target_arch = "wasm32")))]
    acyclic_test_support::differential::record(("retain", 0u64));
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
    let label = std::str::from_utf8(&label)
        .map_err(|error| LazyError::WorkspaceCause(WorkspaceError::Path(error.to_string())))?;
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
    let name = crate::executor::with_history_io(io, |work| volume.storage_name(work))?;
    let provider = match io {
        Some(io) => provider.clone_with_io(io)?,
        None => provider.clone(),
    };
    WorkspaceRef::new(provider, name.into_bytes(), None)
}

use crate::executor::stock_boundary;
