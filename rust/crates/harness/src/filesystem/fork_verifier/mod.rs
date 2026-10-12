use super::*;

/// Filesystem portion of the fork-publication admission barrier.
pub struct FilesystemForkVerifier<A, O> {
    pub(in crate::filesystem) host: Arc<FilesystemHost<A, O>>,
    pub(in crate::filesystem) maximum_inherited_bytes: u64,
    pub(in crate::filesystem) maximum_total_reference_bytes: u64,
    #[cfg(not(target_arch = "wasm32"))]
    pub(in crate::filesystem) native_binding: Option<NativeFilesystemVerificationBinding>,
}

pub(in crate::filesystem) struct FilesystemForkPublicationGuard<A, O> {
    pub(in crate::filesystem) workspace: Workspace<A, O>,
    pub(in crate::filesystem) reservation: Option<PublicationReservation>,
    #[cfg(not(target_arch = "wasm32"))]
    pub(in crate::filesystem) native_binding: Option<NativeFilesystemVerificationBinding>,
}

#[cfg(not(target_arch = "wasm32"))]
pub(in crate::filesystem) struct NativeFilesystemVerificationBinding {
    pub(in crate::filesystem) verifier: AuthorityVerifier,
    pub(in crate::filesystem) scope: Scope,
    pub(in crate::filesystem) custody: acyclic_fs::NativeAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeFilesystemVerificationBinding {
    pub(in crate::filesystem) fn new(
        verifier: &AuthorityVerifier, scope: &Scope,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        verifier.verify(scope, &mut Some(&mut crate::store::history::native_work_io(work)?))?;
        let mut observer = crate::store::history::native_work_io(work)
            .map_err(Error::from)?;
        let verifier = verifier.copy(&mut Some(&mut observer))?;
        let scope = scope.copy(&mut Some(&mut observer))?;
        Ok(Self { verifier, scope, custody: observer.into_custody() })
    }

    pub(in crate::filesystem) fn authenticate(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        if !self.custody.matches_work(work) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "Filesystem verifier belongs to another original resource" });
        }
        self.verifier.verify(&self.scope, &mut Some(&mut crate::store::history::native_work_io(work)?))
    }

    pub(in crate::filesystem) fn require_seed(&self, seed: &ForkSeed, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        self.authenticate(work)?;
        let audience = self.verifier.audience();
        if audience != &seed.parent && audience != &seed.child {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "fork verifier belongs to another publication aggregate" });
        }
        if self.scope.agent().is_none()
            || (audience == &seed.child && self.scope.agent() != Some(seed.child_agent))
        {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "fork verifier belongs to another publication agent" });
        }
        if audience == &seed.parent && !self.scope.capabilities().contains(capability::FORK_PUBLISH) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "parent fork publication is not currently granted" });
        }
        Ok(())
    }

    pub(in crate::filesystem) fn require_file(&self, file: &FileRef, io: &PreparationIo) -> Result<()> {
        self.authenticate(io.original_native_work().expect("native verification IO"))?;
        require_file_read(&self.scope, file, io)
    }

}

impl<A, O> ForkPublicationGuard for FilesystemForkPublicationGuard<A, O>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn release<'a, 'w: 'a>(&'a self,
        work: &'a mut Option<&'w mut dyn crate::store::history::HistoryIo>,
    ) -> Result<crate::store::history::HistoryContentFuture<'a, ()>> {
        crate::fork::admit_fork_future(work, |work| async move {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(native) = work.as_ref().and_then(|io| io.original_native_work()) {
                let binding = self.native_binding.as_ref().ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "fork publication guard has no original native authority" })?;
                binding.authenticate(native)?;
                let original = binding.custody.work();
                let reservation = self.reservation.ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "fork publication guard has no reservation" })?;
                return self.workspace.release_publication(reservation, original, original.cancellation())
                    .await.map_err(|source| Error::NativeWorkspace { source, custody: None });
            }
            let reservation = self.reservation.ok_or_else(|| Error::Invalid("fork publication guard has no reservation".into()))?;
            let budget = work.as_ref().map(|io| io.remaining()).transpose()?.unwrap_or(acyclic_fs::WorkBudget::UNBOUNDED);
            let cancellation = work.as_ref().map(|io| io.cancellation().clone()).unwrap_or_default();
            let result = self.workspace.release_publication(reservation, budget, &cancellation).await;
            let counters = match &result { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
            if let Some(io) = work.as_deref_mut() { io.receive_work(counters)?; }
            result.map(|receipt| receipt.value).map_err(|failure| map_error(failure.error))
        })
    }
}

impl<A, O> FilesystemForkVerifier<A, O> {
    /// Binds one Filesystem provider and a per-file fork-reference limit.
    pub fn new(host: Arc<FilesystemHost<A, O>>, maximum_inherited_bytes: u64) -> Result<Self> {
        if maximum_inherited_bytes == 0 {
            return Err(Error::Invalid(
                "fork inherited-content limit must be positive".into(),
            ));
        }
        Ok(Self {
            host,
            maximum_inherited_bytes,
            maximum_total_reference_bytes: maximum_inherited_bytes.saturating_mul(1_024),
            #[cfg(not(target_arch = "wasm32"))]
            native_binding: None,
        })
    }

    /// Copies the authentic scoped verifier without creating a new issuer or grant.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new_with_native_work(
        host: Arc<FilesystemHost<A, O>>, maximum_inherited_bytes: u64,
        verifier: &AuthorityVerifier, scope: &Scope,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        if maximum_inherited_bytes == 0 {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "fork inherited-content limit must be positive" });
        }
        host.provider.validate(&mut Some(&mut crate::store::native_work_io(work)?))?;
        let binding = NativeFilesystemVerificationBinding::new(verifier, scope, work)?;
        Ok(Self { host, maximum_inherited_bytes,
            maximum_total_reference_bytes: maximum_inherited_bytes.saturating_mul(1_024),
            native_binding: Some(binding) })
    }



    /// Admits the one actual shared provider allocation before constructing it.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn into_native_shared(mut self, work: &acyclic_fs::NativeMaterializationWork) -> Result<Arc<Self>> {
        let binding = self.native_binding.as_mut().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized, reason: "Filesystem verifier has no native authority" })?;
        binding.authenticate(work)?;
        let mut header = work.claim(std::mem::size_of::<Self>() as u64, u64::from((std::mem::size_of::<Self>() as u64) != 0), acyclic_fs::WorkCounters::UNCHARGED)
            .map_err(Error::from)?;
        binding.custody.absorb(&mut header)
            .map_err(|cause| Error::NativeWork { cause, custody: Some(header) })?;
        Ok(Arc::new(self))
    }
    /// Narrows the aggregate bytes inspected during one fork publication.
    pub fn with_total_reference_bytes(mut self, maximum_bytes: u64) -> Result<Self> {
        if maximum_bytes == 0 {
            return Err(Error::Invalid(
                "fork total reference limit must be positive".into(),
            ));
        }
        self.maximum_total_reference_bytes = maximum_bytes;
        Ok(self)
    }
}


pub(in crate::filesystem) fn fork_io(work: &Option<&mut dyn crate::store::history::HistoryIo>) -> Result<PreparationIo> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = work.as_ref().and_then(|io| io.original_native_work()) {
        return PreparationIo::from_native_work(original);
    }
    let cancellation = work.as_ref().map(|io| io.cancellation().clone()).unwrap_or_default();
    let budget = work.as_ref().map(|io| io.remaining()).transpose()?.unwrap_or(acyclic_fs::WorkBudget::UNBOUNDED);
    if budget == acyclic_fs::WorkBudget::UNBOUNDED {
        PreparationIo::for_unmeasured(&cancellation)
    } else { PreparationIo::from_budget(budget, &cancellation) }
}

impl<A, O> ForkSeedVerifier for FilesystemForkVerifier<A, O>
where A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static,
{
    fn provider(&self) -> &ProviderRef { &self.host.provider }
    fn maximum_reference_bytes(&self) -> u64 { self.maximum_total_reference_bytes }
    fn verify<'a, 'w: 'a>(&'a self, seed: &'a ForkSeed,
        work: &'a mut Option<&'w mut dyn crate::store::history::HistoryIo>,
    ) -> Result<crate::store::history::HistoryContentFuture<'a, ()>> {
        crate::fork::admit_fork_future(work, |work| async move {
            #[cfg(not(target_arch = "wasm32"))]
            let native = work.as_ref().and_then(|io| io.original_native_work()).cloned();
            let io = fork_io(work)?;
            let result = async {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(native) = &native {
                    let binding = self.native_binding.as_ref().ok_or(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Unauthorized, reason: "Filesystem verifier has no native authority" })?;
                    binding.require_seed(seed, native)?;
                    for file in &seed.inherited_context {
                        if file.volume().provider() == &self.host.provider { self.require_seed_file_with_io(seed, file, &io).await?; }
                    }
                    for grant in &seed.reference_grants {
                        if grant.file.volume().provider() == &self.host.provider { self.require_seed_file_with_io(seed, &grant.file, &io).await?; }
                    }
                    for file in &seed.attachment_manifests {
                        if file.volume().provider() == &self.host.provider { self.require_seed_file_with_io(seed, file, &io).await?; }
                    }
                    if let Some(summary) = &seed.summary {
                        for file in std::iter::once(&summary.context).chain(&summary.payloads).chain(&summary.references) {
                            if file.volume().provider() == &self.host.provider { self.require_seed_file_with_io(seed, file, &io).await?; }
                        }
                    }
                }
                self.verify_with_io(seed, &io).await
            }.await;
            #[cfg(not(target_arch = "wasm32"))]
            if native.is_none() {
                if let Some(observer) = work.as_deref_mut() { observer.receive_work(io.receipt())?; }
            }
            #[cfg(target_arch = "wasm32")]
            if let Some(observer) = work.as_deref_mut() { observer.receive_work(io.receipt())?; }
            result
        })
    }
    fn read_manifest<'a, 'w: 'a>(&'a self, seed: &'a ForkSeed, file: &'a FileRef,
        work: &'a mut Option<&'w mut dyn crate::store::history::HistoryIo>,
    ) -> Result<crate::store::history::HistoryContentFuture<'a, (acyclic_stream::OwnedBytes, u64)>> {
        crate::fork::admit_fork_future(work, |work| async move {
            #[cfg(not(target_arch = "wasm32"))]
            let native = work.as_ref().and_then(|io| io.original_native_work()).cloned();
            let io = fork_io(work)?;
            let result = async {
                #[cfg(not(target_arch = "wasm32"))]
                if native.is_some() { self.require_seed_file_with_io(seed, file, &io).await?; }
                if file.volume().provider() != &self.host.provider {
                    return Err(fork_preparer_io::failure(Some(&io), crate::wire::ErrorCode::Unauthorized, "fork manifest belongs to another provider"));
                }
                let bytes = self.host.read_pinned_io(file, self.maximum_inherited_bytes, Some(&io)).await?;
                #[cfg(not(target_arch = "wasm32"))]
                if native.is_some() { return Ok((bytes, 0)); }
                let retained = bytes.retained_capacity().ok_or_else(|| fork_preparer_io::failure(Some(&io),
                    crate::wire::ErrorCode::Unsupported, "manifest producer does not expose its admitted owned capacity"))? as u64;
                Ok((bytes, retained))
            }.await;
            #[cfg(not(target_arch = "wasm32"))]
            if native.is_none() {
                if let Some(observer) = work.as_deref_mut() { observer.receive_work(io.receipt())?; }
            }
            #[cfg(target_arch = "wasm32")]
            if let Some(observer) = work.as_deref_mut() { observer.receive_work(io.receipt())?; }
            result
        })
    }
    fn verify_file<'a, 'w: 'a>(&'a self, seed: &'a ForkSeed, file: &'a FileRef,
        work: &'a mut Option<&'w mut dyn crate::store::history::HistoryIo>,
    ) -> Result<crate::store::history::HistoryContentFuture<'a, ()>> {
        crate::fork::admit_fork_future(work, |work| async move {
            #[cfg(not(target_arch = "wasm32"))]
            let native = work.as_ref().and_then(|io| io.original_native_work()).cloned();
            let io = fork_io(work)?;
            let result = async {
                #[cfg(not(target_arch = "wasm32"))]
                if native.is_some() { self.require_seed_file_with_io(seed, file, &io).await?; }
                if file.volume().provider() != &self.host.provider {
                    return Err(fork_preparer_io::failure(Some(&io), crate::wire::ErrorCode::Unauthorized, "fork file belongs to another provider"));
                }
                let bytes = self.host.read_pinned_io(file, self.maximum_inherited_bytes, Some(&io)).await?;
                let retained = bytes.retained_capacity().unwrap_or(0) as u64;
                drop(bytes);
                #[cfg(not(target_arch = "wasm32"))]
                if native.is_none() { io.release(retained)?; }
                #[cfg(target_arch = "wasm32")]
                io.release(retained)?;
                Ok(())
            }.await;
            #[cfg(not(target_arch = "wasm32"))]
            if native.is_none() {
                if let Some(observer) = work.as_deref_mut() { observer.receive_work(io.receipt())?; }
            }
            #[cfg(target_arch = "wasm32")]
            if let Some(observer) = work.as_deref_mut() { observer.receive_work(io.receipt())?; }
            result
        })
    }
    fn acquire_private_fence<'a, 'w: 'a>(&'a self, seed: &'a ForkSeed,
        work: &'a mut Option<&'w mut dyn crate::store::history::HistoryIo>,
    ) -> Result<crate::store::history::HistoryContentFuture<'a, crate::fork::ObservedForkPublicationGuard>> {
        crate::fork::admit_fork_future(work, |work| async move {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = work.as_ref().and_then(|io| io.original_native_work()) {
                return async {
            let io = PreparationIo::from_native_work(work)?;
            let binding = self.native_binding.as_ref().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized, reason: "Filesystem verifier has no native authority" })?;
            binding.require_seed(seed, work)?;
            if seed.child_private_volume.provider() != &self.host.provider
                || (binding.verifier.audience() != &seed.parent
                    && !io.contains_capability(binding.scope.capabilities(), &seed.child_private_volume, "volume:write:")?)
            {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "fork private publication fence lacks the exact current provider write grant" });
            }
            self.host.verify_fork_allocation_io(seed, &seed.child_private_volume, Some(&io)).await?;
            let reference = fork_preparer_io::content_workspace(&self.host.provider, &seed.child_private_volume, Some(&io))?;
            let workspace = fork_preparer_io::open(&self.host, &reference, &io).await?;
            let native_binding = NativeFilesystemVerificationBinding::new(&binding.verifier, &binding.scope, work)?;
            let header;
            let mut guard = {
                header = work.claim(std::mem::size_of::<FilesystemForkPublicationGuard<A, O>>() as u64, u64::from((std::mem::size_of::<FilesystemForkPublicationGuard<A, O>>() as u64) != 0), acyclic_fs::WorkCounters::UNCHARGED)
                    .map_err(Error::from)?;
                Box::new(FilesystemForkPublicationGuard {
                    workspace, reservation: None,
                    native_binding: Some(native_binding),
                })
            };
            let operation = acyclic_fs::OperationId::from_bytes(seed.operation_id.into_bytes());
            let reservation = match guard.workspace.reserve_publication(
                operation, work, work.cancellation()).await.map_err(|error| (error, None))
            {
                Ok(reservation) => reservation,
                Err((source, custody)) => {
                    let primary = Error::NativeWorkspace { source, custody };
                    if let Ok(Some(reservation)) = guard.workspace.publication_reservation_with_native_work(
                        operation, work, work.cancellation()).await
                    {
                        guard.reservation = Some(reservation);
                        let _cleanup = guard.workspace.release_publication(
                            reservation, work, work.cancellation()).await;
                    }
                    return Err(primary);
                }
            };
            guard.reservation = Some(reservation);
            Ok(crate::fork::ObservedForkPublicationGuard { guard: guard as Box<dyn ForkPublicationGuard>, _allocation: Some(header) })
                }.await;
            }
            let io = fork_io(work)?;
            let result = async {
                if seed.child_private_volume.provider() != &self.host.provider {
                    return Err(Error::Unauthorized("fork private volume belongs to another provider".into()));
                }
                let reference = fork_preparer_io::content_workspace(
                    &self.host.provider, &seed.child_private_volume, Some(&io),
                )?;
                let workspace = fork_preparer_io::open(&self.host, &reference, &io).await?;
                let guard_bytes = std::mem::size_of::<FilesystemForkPublicationGuard<A, O>>() as u64;
                io.retain_owned(guard_bytes)?;
                io.allocation_attempt(guard_bytes)?;
                let mut guard = Box::new(FilesystemForkPublicationGuard {
                    workspace, reservation: None,
                    #[cfg(not(target_arch = "wasm32"))]
                    native_binding: None,
                });
                let receipt = guard.workspace.reserve_publication(
                    acyclic_fs::OperationId::from_bytes(seed.operation_id.into_bytes()),
                    io.remaining()?,
                    io.cancellation(),
                ).await;
                let receipt = match receipt {
                    Ok(receipt) => receipt,
                    Err(failure) => {
                        io.spent(*failure.work)?;
                        return Err(map_error(failure.error));
                    }
                };
                guard.reservation = Some(receipt.value);
                io.charge(receipt.work)?;
                Ok(crate::fork::ObservedForkPublicationGuard { guard: guard as Box<dyn ForkPublicationGuard>,
                    #[cfg(not(target_arch = "wasm32"))] _allocation: None })
            }.await;
            if let Some(observer) = work.as_deref_mut() { observer.receive_work(io.receipt())?; }
            result
        })
    }
}


mod seed;
