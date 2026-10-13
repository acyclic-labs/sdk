use super::*;

/// Filesystem portion of the fork-publication admission barrier.
pub struct FilesystemForkVerifier<A, O> {
    pub(in crate::filesystem) host: Arc<FilesystemHost<A, O>>,
    pub(in crate::filesystem) maximum_inherited_bytes: u64,
    pub(in crate::filesystem) maximum_total_reference_bytes: u64,
    #[cfg(not(target_arch = "wasm32"))]
    pub(in crate::filesystem) native_binding: Option<Arc<FilesystemContentVerifier<A, O>>>,
}

pub(in crate::filesystem) struct FilesystemForkPublicationGuard<A, O> {
    workspace: Workspace<A, O>,
    reservation: Option<PublicationReservation>,
    #[cfg(not(target_arch = "wasm32"))]
    native_binding: Option<Arc<FilesystemContentVerifier<A, O>>>,
}

impl<A, O> FilesystemForkVerifier<A, O> {
    /// Binds the provider, reference limits and optional original authority.
    pub fn new(
        host: Arc<FilesystemHost<A, O>>,
        maximum_inherited_bytes: u64,
        maximum_total_reference_bytes: Option<u64>,
        original: Option<(&AuthorityVerifier, &Scope, &PreparationIo)>,
    ) -> Result<Arc<Self>> {
        let maximum_total_reference_bytes = maximum_total_reference_bytes
            .unwrap_or_else(|| maximum_inherited_bytes.saturating_mul(1_024));
        let io = original.map(|(_, _, io)| io);
        if maximum_inherited_bytes == 0 || maximum_total_reference_bytes == 0 {
            return Err(crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "fork reference limits must be positive",
            ));
        }
        #[cfg(not(target_arch = "wasm32"))]
        let native_binding = original
            .map(|(verifier, scope, io)| -> Result<_> {
                io.original_native_work()
                    .ok_or_else(|| io.invalid("fork verifier requires original native work"))?;
                host.provider.validate(&mut Some(&mut io.history_io()))?;
                let mut binding = FilesystemContentVerifier::new(
                    Arc::clone(&host),
                    verifier,
                    scope,
                    maximum_inherited_bytes,
                    Some(io),
                )?;
                let mut header = io.original_native_work().unwrap().claim(
                    std::mem::size_of::<Self>() as u64,
                    1,
                    WorkCounters::UNCHARGED,
                )?;
                Arc::get_mut(&mut binding)
                    .expect("new verifier has no aliases")
                    ._native_metadata
                    .as_mut()
                    .expect("native verifier owns its metadata")
                    .absorb(&mut header)
                    .map_err(|cause| Error::NativeWork {
                        cause,
                        custody: Some(header),
                    })?;
                Ok(binding)
            })
            .transpose()?;
        #[cfg(target_arch = "wasm32")]
        if original.is_some() {
            return Err(crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "native fork verification is unavailable",
            ));
        }
        Ok(Arc::new(Self {
            host,
            maximum_inherited_bytes,
            maximum_total_reference_bytes,
            #[cfg(not(target_arch = "wasm32"))]
            native_binding,
        }))
    }
}

pub(in crate::filesystem) fn fork_io(
    work: &Option<&mut dyn crate::store::HistoryIo>,
) -> Result<PreparationIo> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = work.as_ref().and_then(|io| io.original_native_work()) {
        return PreparationIo::from_native_work(original);
    }
    let cancellation = work
        .as_ref()
        .map(|io| io.cancellation().clone())
        .unwrap_or_default();
    let budget = work
        .as_ref()
        .map(|io| io.remaining())
        .transpose()?
        .unwrap_or(acyclic_fs::WorkBudget::UNBOUNDED);
    if budget == acyclic_fs::WorkBudget::UNBOUNDED {
        PreparationIo::for_unmeasured(&cancellation)
    } else {
        PreparationIo::from_budget(budget, &cancellation)
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn require_seed<A, O>(
    binding: &FilesystemContentVerifier<A, O>,
    seed: &ForkSeed,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    binding.require_native_work(work)?;
    let audience = binding.verifier.audience();
    if audience != &seed.parent && audience != &seed.child {
        return Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "fork verifier belongs to another publication aggregate",
        });
    }
    if binding.scope.agent().is_none()
        || audience == &seed.child && binding.scope.agent() != Some(seed.child_agent)
    {
        return Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "fork verifier belongs to another publication agent",
        });
    }
    if audience == &seed.parent
        && !binding
            .scope
            .capabilities()
            .contains(capability::FORK_PUBLISH)
    {
        return Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "parent fork publication is not currently granted",
        });
    }
    Ok(())
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> ForkPublicationGuard
    for FilesystemForkPublicationGuard<A, O>
{
    fn release<'a, 'w: 'a>(
        &'a self,
        work: &'a mut Option<&'w mut dyn crate::store::HistoryIo>,
    ) -> Result<crate::store::history::HistoryContentFuture<'a, ()>> {
        crate::fork::admit_fork_future(work, |work| async move {
            let io = fork_io(work)?;
            #[cfg(not(target_arch = "wasm32"))]
            let io = if let Some(original) = io.original_native_work() {
                let binding = self.native_binding.as_ref().ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "fork publication guard has no original native authority",
                })?;
                binding.require_native_work(original)?;
                PreparationIo::from_native_work(
                    binding
                        ._native_metadata
                        .as_ref()
                        .expect("authenticated native metadata")
                        .work(),
                )?
            } else {
                io
            };
            let reservation = self
                .reservation
                .ok_or_else(|| io.invalid("fork publication guard has no reservation"))?;
            let result = io.sdk(
                self.workspace
                    .release_publication(reservation, &io, io.cancellation())
                    .await,
            );
            if io.original_native_work().is_none()
                && let Some(work) = work.as_deref_mut()
            {
                work.receive_work(io.receipt())?;
            }
            result
        })
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> ForkSeedVerifier
    for FilesystemForkVerifier<A, O>
{
    fn provider(&self) -> &ProviderRef {
        &self.host.provider
    }
    fn maximum_reference_bytes(&self) -> u64 {
        self.maximum_total_reference_bytes
    }

    fn verify<'a, 'w: 'a>(
        &'a self,
        seed: &'a ForkSeed,
        work: &'a mut Option<&'w mut dyn crate::store::HistoryIo>,
    ) -> Result<crate::store::history::HistoryContentFuture<'a, ()>> {
        crate::fork::admit_fork_future(work, |work| async move {
            let io = fork_io(work)?;
            let result = async {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(original) = io.original_native_work() {
                    require_seed(
                        self.native_binding.as_ref().ok_or(Error::NativeBoundary {
                            code: crate::wire::ErrorCode::Unauthorized,
                            reason: "Filesystem verifier has no native authority",
                        })?,
                        seed,
                        original,
                    )?;
                    for file in seed
                        .inherited_context
                        .iter()
                        .chain(seed.reference_grants.iter().map(|grant| &grant.file))
                        .chain(&seed.attachment_manifests)
                        .chain(seed.summary.iter().flat_map(|summary| {
                            std::iter::once(&summary.context)
                                .chain(&summary.payloads)
                                .chain(&summary.references)
                        }))
                    {
                        if file.volume().provider() == &self.host.provider {
                            self.require_seed_file(seed, file, &io).await?;
                        }
                    }
                }
                self.verify_seed(seed, &io).await
            }
            .await;
            if io.original_native_work().is_none()
                && let Some(work) = work.as_deref_mut()
            {
                work.receive_work(io.receipt())?;
            }
            result
        })
    }

    fn read_manifest<'a, 'w: 'a>(
        &'a self,
        seed: &'a ForkSeed,
        file: &'a FileRef,
        work: &'a mut Option<&'w mut dyn crate::store::HistoryIo>,
    ) -> Result<crate::store::history::HistoryContentFuture<'a, (acyclic_stream::OwnedBytes, u64)>>
    {
        crate::fork::admit_fork_future(work, |work| async move {
            let io = fork_io(work)?;
            let result = async {
                #[cfg(not(target_arch = "wasm32"))]
                if io.original_native_work().is_some() {
                    self.require_seed_file(seed, file, &io).await?;
                }
                if file.volume().provider() != &self.host.provider {
                    return Err(crate::executor::stock_boundary(
                        Some(&io),
                        crate::wire::ErrorCode::Unauthorized,
                        "fork manifest belongs to another provider",
                    ));
                }
                let bytes = self
                    .host
                    .read_pinned_io(file, self.maximum_inherited_bytes, Some(&io))
                    .await?;
                let retained = if io.original_native_work().is_some() {
                    0
                } else {
                    bytes.retained_capacity().ok_or_else(|| {
                        crate::executor::stock_boundary(
                            Some(&io),
                            crate::wire::ErrorCode::Unsupported,
                            "manifest producer does not expose its admitted owned capacity",
                        )
                    })? as u64
                };
                Ok((bytes, retained))
            }
            .await;
            if io.original_native_work().is_none()
                && let Some(work) = work.as_deref_mut()
            {
                work.receive_work(io.receipt())?;
            }
            result
        })
    }

    fn verify_file<'a, 'w: 'a>(
        &'a self,
        seed: &'a ForkSeed,
        file: &'a FileRef,
        work: &'a mut Option<&'w mut dyn crate::store::HistoryIo>,
    ) -> Result<crate::store::history::HistoryContentFuture<'a, ()>> {
        crate::fork::admit_fork_future(work, |work| async move {
            let io = fork_io(work)?;
            let result = async {
                #[cfg(not(target_arch = "wasm32"))]
                if io.original_native_work().is_some() {
                    self.require_seed_file(seed, file, &io).await?;
                }
                if file.volume().provider() != &self.host.provider {
                    return Err(crate::executor::stock_boundary(
                        Some(&io),
                        crate::wire::ErrorCode::Unauthorized,
                        "fork file belongs to another provider",
                    ));
                }
                let bytes = self
                    .host
                    .read_pinned_io(file, self.maximum_inherited_bytes, Some(&io))
                    .await?;
                let retained = bytes.retained_capacity().unwrap_or(0) as u64;
                drop(bytes);
                if io.original_native_work().is_none() {
                    io.release(retained)?;
                }
                Ok(())
            }
            .await;
            if io.original_native_work().is_none()
                && let Some(work) = work.as_deref_mut()
            {
                work.receive_work(io.receipt())?;
            }
            result
        })
    }

    fn acquire_private_fence<'a, 'w: 'a>(
        &'a self,
        seed: &'a ForkSeed,
        work: &'a mut Option<&'w mut dyn crate::store::HistoryIo>,
    ) -> Result<
        crate::store::history::HistoryContentFuture<'a, crate::fork::ObservedForkPublicationGuard>,
    > {
        crate::fork::admit_fork_future(work, |work| async move {
            let io = fork_io(work)?;
            let result = async {
                if seed.child_private_volume.provider() != &self.host.provider {
                    return Err(crate::executor::stock_boundary(Some(&io), crate::wire::ErrorCode::Unauthorized,
                        "fork private volume belongs to another provider"));
                }
                #[cfg(not(target_arch = "wasm32"))]
                let (native_binding, allocation) = if let Some(original) = io.original_native_work() {
                    let binding = self.native_binding.as_ref().ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                        reason: "Filesystem verifier has no native authority" })?;
                    require_seed(binding, seed, original)?;
                    if binding.verifier.audience() != &seed.parent
                        && !io.contains_capability(binding.scope.capabilities(), &seed.child_private_volume, "volume:write:")?
                    {
                        return Err(crate::executor::stock_boundary(Some(&io), crate::wire::ErrorCode::Unauthorized,
                            "fork private publication fence lacks the exact current provider write grant"));
                    }
                    self.host.read_fork_allocation(seed, &seed.child_private_volume,
                        crate::filesystem::fork_preparer::seed_digest(seed, 16 * 1024 * 1024, Some(&io))?,
                        16 * 1024 * 1024, Some(&io)).await?;
                    (Some(Arc::clone(binding)), Some(original.claim(
                        std::mem::size_of::<FilesystemForkPublicationGuard<A, O>>() as u64, 1, WorkCounters::UNCHARGED)?))
                } else { (None, None) };
                let reference = fork_preparer_io::content_workspace(&self.host.provider, &seed.child_private_volume, Some(&io))?;
                let workspace = fork_preparer_io::open(&self.host, &reference, &io).await?;
                if io.original_native_work().is_none() {
                    io.retain_owned(std::mem::size_of::<FilesystemForkPublicationGuard<A, O>>() as u64)?;
                    io.allocation_attempt(std::mem::size_of::<FilesystemForkPublicationGuard<A, O>>() as u64)?;
                }
                let mut guard = Box::new(FilesystemForkPublicationGuard { workspace, reservation: None,
                    #[cfg(not(target_arch = "wasm32"))] native_binding });
                let operation = acyclic_fs::OperationId::from_bytes(seed.operation_id.into_bytes());
                match io.sdk(guard.workspace.reserve_publication(operation, &io, io.cancellation()).await) {
                    Ok(reservation) => guard.reservation = Some(reservation),
                    Err(cause) => {
                        #[cfg(not(target_arch = "wasm32"))]
                        if let Some(work) = io.original_native_work() && let Ok(Some(reservation)) = guard.workspace.publication_reservation(operation, work, io.cancellation()).await {
                            let _cleanup = io.sdk(guard.workspace.release_publication(reservation, &io, io.cancellation()).await);
                        }
                        return Err(cause);
                    }
                }
                Ok(crate::fork::ObservedForkPublicationGuard { guard,
                    #[cfg(not(target_arch = "wasm32"))] _allocation: allocation,
                })
            }.await;
            if io.original_native_work().is_none()
                && let Some(work) = work.as_deref_mut()
            {
                work.receive_work(io.receipt())?;
            }
            result
        })
    }
}

mod ordered;
mod seed;
