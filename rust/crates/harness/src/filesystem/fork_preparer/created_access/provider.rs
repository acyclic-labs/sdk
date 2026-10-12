//! Resource creation through the existing provider contract.
use super::*;
impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> ResourceCreationProvider
    for FilesystemResourceCreationProvider<A, O>
{
    fn provider(&self) -> &ProviderRef {
        &self.host.provider
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn root_bootstrap_sources(&self, operation: OperationId)
        -> [Option<(&crate::durable_host::TaskVolumeSnapshot,
            &crate::durable_host::RootBootstrapBindingWitness)>; 2]
    {
        self.roots.each_ref().map(|slot|
            slot.get().filter(|allocation| allocation.witness.binding().original_task() == crate::runtime::task_id_for_admission(operation))
                .map(|allocation| (&allocation.request, allocation.witness.as_ref())))
    }
    fn verify<'a>(
        &'a self,
        request: &'a ResourcePreparationRequest,
        proof: &'a ResourceCreationProofRef,
    ) -> BoxFuture<'a, Result<CreatedResourceReceipt>> {
        Box::pin(async move {
            let io = PreparationIo::new(request, &CancellationToken::new())?;
            self.receipt(request, proof, &io).await
        })
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn verify_with_native_work<'a>(
        &'a self,
        request: &'a ResourcePreparationRequest,
        proof: &'a ResourceCreationProofRef,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (CreatedResourceReceipt, acyclic_fs::NativeAllocation)>>
    {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            let receipt = self.receipt(request, proof, &io).await?;
            let retained = io.owned_bytes()?;
            let custody = io.into_native_custody(retained)?;
            Ok((receipt, custody))
        })
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn verify_root_bootstrap_with_native_work<'a>(
        &'a self, context: crate::resource_preparation::RootBootstrapContentContext<'a>,
        request: &'a crate::durable_host::TaskVolumeSnapshot,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, crate::resource_preparation::CreatedResourceAccess>> {
        crate::NativeFutureBundle::new(work, || async move {
            use crate::resource_preparation::CreatedResourceAccess;
            let allocation = match request.volume.class() {
                VolumeClass::Project => self.roots[0].get(),
                VolumeClass::AgentPrivate => self.roots[1].get(),
                VolumeClass::SessionShared => None,
            }.ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, reason: "Root content has no configured protected original allocation" })?;
            work.remaining()?;
            let root = allocation.witness.binding();
            if context.owner() != allocation.owner() || context.audience() != root.audience()
                || context.agent() != root.agent() || context.original_root().parent.is_some()
                || context.original_root().task_id() != root.original_task()
                || context.original_admission_digest() != allocation.admission_digest
                || request.volume != allocation.request.volume
                || request.generation.as_resource().provider() != &self.host.provider
                || request.generation.as_resource().key() != allocation.request.generation.as_resource().key()
            {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, reason: "Root content request differs from its actual protected original association" });
            }
            let io = PreparationIo::from_native_work(work)?;
            let name_hold;
            let name = io.capability(&allocation.request.volume, "harness-")?;
            name_hold = io.guard_owned(name.capacity() as u64)?;
            let read = crate::executor::with_history_io(Some(&io), |work| {
                crate::contract::preparation_format(format_args!("volume:read:{}", &name["harness-".len()..]), work)
            })?;
            let write = crate::executor::with_history_io(Some(&io), |work| {
                crate::contract::preparation_format(format_args!("volume:write:{}", &name["harness-".len()..]), work)
            })?;
            if !context.original_root().grants.contains(&read) {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, reason: "Root content original admission does not declare Project or Scratch READ" });
            }
            let writable = allocation.publication_lease.is_some() && context.original_root().grants.contains(&write);
            allocation.reauthenticate_with_io(&io).await?;
            let id = crate::executor::with_history_io(Some(&io), |work| {
                crate::contract::preparation_format(format_args!("root-created:{name}"), work)
            })?;
            drop(name);
            drop(name_hold);
            io.retain_owned((if writable { 2 } else { 1 }) * std::mem::size_of::<String>() as u64)?;
            let capabilities = if writable { Capabilities::new([read, write]) } else {
                let discarded_capacity = write.capacity() as u64;
                drop(write);
                io.release(discarded_capacity)?;
                Capabilities::new([read])
            };
            let scope = {
                let mut observer = io.history_io();
                self.issuer.issue(&id, &capabilities, Some(context.agent()), None, &mut Some(&mut observer))?
            };
            let id_capacity = id.capacity() as u64;
            let capabilities_capacity = capabilities.logical_owned_bytes()?;
            drop(id);
            io.release(id_capacity)?;
            drop(capabilities);
            io.release(capabilities_capacity)?;
            let verifier = {
                let mut observer = io.history_io();
                self.issuer.verifier(&mut Some(&mut observer))?
            };
            io.retain_owned(std::mem::size_of::<CreatedFilesystemAccess<A, O>>() as u64)?;
            let custody;
            let writer = if writable {
                Some(FilesystemContentPublisher::new(self.host.clone(), &allocation.request.volume,
                    &verifier, &scope, context.original_root().limits.file_bytes, Some(&io))?)
            } else { None };
            let temporary = verifier.logical_owned_bytes()?.checked_add(scope.logical_owned_bytes()?)
                .ok_or_else(|| io.invalid("created reader metadata overflows"))?;
            let reader = FilesystemContentVerifier::new(self.host.clone(), &verifier, &scope,
                context.original_root().limits.file_bytes, Some(&io))?;
            drop((verifier, scope));
            io.release(temporary)?;
            let capture = io.owned_bytes()?;
            custody = io.into_native_custody(capture)?;
            work.charge(WorkCounters { allocation_operations: 1, ..WorkCounters::default() })?;
            let access = Arc::new(CreatedFilesystemAccess::Root {
                allocation: allocation.clone(), reader, writer, _custody: custody,
            });
            CreatedResourceAccess::root_bootstrap(context, request, ContentBindings {
                reader: access.clone(), writer: if writable { Some(access) } else { None },
            }, work)
        })
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    FilesystemResourceCreationProvider<A, O>
{
    async fn receipt(
        &self,
        request: &ResourcePreparationRequest,
        proof: &ResourceCreationProofRef,
        io: &PreparationIo,
    ) -> Result<CreatedResourceReceipt> {
        let allocation =
            authenticate(&self.host, self.issuer.audience(), request, proof, Some(io)).await?;
        crate::core::resource_creation_clone_reservation(
            request,
            proof,
            &allocation.origin,
            &mut Some(&mut io.history_io()),
        )?;
        let capture = allocation
            .volume
            .logical_owned_bytes()?
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(allocation.generation.logical_owned_bytes().ok()?))
            .and_then(|bytes| bytes.checked_add(proof.resource.logical_owned_bytes().ok()?))
            .and_then(|bytes| bytes.checked_add(proof.evidence.logical_owned_bytes().ok()?))
            .and_then(|bytes| {
                bytes.checked_add(std::mem::size_of::<CreatedFilesystemAccess<A, O>>() as u64)
            })
            .ok_or_else(|| io.invalid("creator capture ownership overflow"))?;
        io.retain_owned(capture)?;
        io.allocation_attempt(std::mem::size_of::<CreatedFilesystemAccess<A, O>>() as u64)?;
        let name = io.capability(&allocation.volume, "harness-")?;
        let read = crate::executor::with_history_io(Some(io), |work| crate::contract::preparation_format(format_args!("volume:read:{}", &name["harness-".len()..]), work))?;
        let write = crate::executor::with_history_io(Some(io), |work| crate::contract::preparation_format(format_args!("volume:write:{}", &name["harness-".len()..]), work))?;
        let id = crate::executor::with_history_io(Some(io), |work| crate::contract::preparation_format(format_args!("created:{}:{name}", proof.preparation), work))?;
        io.retain_owned((2 * std::mem::size_of::<String>()) as u64)?;
        let capabilities = Capabilities::new([read, write]);
        let temporary =
            name.capacity() as u64 + id.capacity() as u64 + capabilities.logical_owned_bytes()?;
        let scope = self.issuer.issue(
            &id,
            &capabilities,
            Some(allocation.agent),
            None,
            &mut Some(&mut io.history_io()),
        )?;
        drop((name, id, capabilities));
        io.release(temporary)?;
        let verifier = self.issuer.verifier(&mut Some(&mut io.history_io()))?;
        let temporary = verifier
            .logical_owned_bytes()?
            .checked_add(scope.logical_owned_bytes()?)
            .ok_or_else(|| io.invalid("created reader metadata overflows"))?;
        let writer = FilesystemContentPublisher::new(
            self.host.clone(),
            &allocation.volume,
            &verifier,
            &scope,
            request.maximum_bytes,
            Some(io),
        )?;
        let reader = FilesystemContentVerifier::new(
            self.host.clone(),
            &verifier,
            &scope,
            request.maximum_bytes,
            Some(io),
        )?;
        drop((verifier, scope));
        io.release(temporary)?;
        let access = Arc::new(CreatedFilesystemAccess::Task {
            request: request.clone(),
            proof: proof.clone(),
            origin: allocation.origin.clone(),
            generation: allocation.generation.clone(),
            prepared_generation: allocation.prepared_generation,
            creation_operation: allocation.creation_operation,
            reader,
            writer,
        });
        let count = allocation.reference_files.len();
        let operations = Vec::new();
        let mut references = bounded_io::vector(count, Some(io))?;
        for file in allocation.reference_files {
            let retained = file
                .logical_owned_bytes()?
                .checked_add(std::mem::size_of::<CreatedFilesystemAccess<A, O>>() as u64)
                .ok_or_else(|| io.invalid("creator reference ownership overflow"))?;
            io.retain_owned(retained)?;
            io.allocation_attempt(std::mem::size_of::<CreatedFilesystemAccess<A, O>>() as u64)?;
            let reader = Arc::new(CreatedFilesystemAccess::Reference {
                allocation: access.clone(),
                file: file.clone(),
            });
            references.push((file, reader as Arc<dyn crate::conversation::ContentResidencyVerifier>));
        }
        let mut content = bounded_io::vector(1, Some(io))?;
        content.push(CreatedContentBinding {
            default_writer: allocation.authority.is_some(),
            artifact_writer: allocation.authority.is_none()
                && allocation.volume.class() == VolumeClass::AgentPrivate,
            scratch_creation: allocation.authority.as_ref().map(|_| {
                access.clone() as crate::resource_preparation::ChildScratchCreationProof
            }),
            authority: allocation.authority,
            volume: allocation.volume,
            binding: ContentBindings {
                reader: access.clone(),
                writer: Some(access),
            },
        });
        Ok(CreatedResourceReceipt {
            origin: allocation.origin,
            resource: proof.resource.clone(),
            evidence: proof.evidence.clone(),
            allocation_generation: allocation.generation,
            operations,
            references,
            content,
        })
    }
}
