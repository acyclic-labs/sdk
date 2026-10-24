//! Resource creation through the existing provider contract.
use super::*;
impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> ResourceCreationProvider
    for FilesystemResourceCreationProvider<A, O>
{
    fn provider(&self) -> &ProviderRef {
        &self.host.provider
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
        // Verify immutable input bodies once when issuing the reader. Rechecking
        // its protected allocation identity must not reload those bodies on each read.
        if allocation.authority.is_some() {
            for file in &request.input_files {
                let bytes = self
                    .host
                    .read_pinned_io(file, request.maximum_bytes, Some(io))
                    .await?;
                let held = io.guard_bytes(&bytes)?;
                drop(bytes);
                drop(held);
            }
        }
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
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<CreatedProvenance>() as u64))
            .ok_or_else(|| io.invalid("creator capture ownership overflow"))?;
        io.retain_owned(capture)?;
        io.allocation_attempt(std::mem::size_of::<CreatedProvenance>() as u64)?;
        let name = allocation
            .volume
            .storage_name(&mut Some(&mut io.history_io()))?;
        let suffix = name
            .strip_prefix("harness-")
            .ok_or_else(|| io.invalid("content namespace has no harness prefix"))?;
        let read = crate::executor::with_history_io(Some(io), |work| {
            crate::executor::preparation_format(
                format_args!(
                    "{}{suffix}",
                    acyclic_fs::access::VolumeOperation::Read.capability_prefix()
                ),
                work,
            )
        })?;
        let write = crate::executor::with_history_io(Some(io), |work| {
            crate::executor::preparation_format(
                format_args!(
                    "{}{suffix}",
                    acyclic_fs::access::VolumeOperation::Write.capability_prefix()
                ),
                work,
            )
        })?;
        let id = crate::executor::with_history_io(Some(io), |work| {
            crate::executor::preparation_format(
                format_args!("created:{}:{name}", proof.preparation),
                work,
            )
        })?;
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
        let mut writer = FilesystemContentPublisher::new(
            self.host.clone(),
            &allocation.volume,
            &verifier,
            &scope,
            request.maximum_bytes,
            Some(io),
        )?;
        let mut reader = FilesystemContentVerifier::new(
            self.host.clone(),
            &verifier,
            &scope,
            request.maximum_bytes,
            Some(io),
        )?;
        drop((verifier, scope));
        io.release(temporary)?;
        let creation = Arc::new(CreatedProvenance {
            request: request.clone(),
            proof: proof.clone(),
            origin: allocation.origin.clone(),
            volume: allocation.volume.clone(),
            generation: allocation.generation.clone(),
            prepared_generation: allocation.prepared_generation,
        });
        Arc::get_mut(&mut reader)
            .ok_or_else(|| io.invalid("new content reader has aliases"))?
            .creation = Some(creation.clone());
        Arc::get_mut(&mut writer)
            .ok_or_else(|| io.invalid("new content writer has aliases"))?
            .creation = Some(creation.clone());
        let count = allocation.reference_files.len();
        let operations = Vec::new();
        let mut references = crate::model::vec_with_capacity(count, Some(io))?;
        for file in allocation.reference_files {
            let reference_reader = reader.reference_reader(&file, io)?;
            references.push((
                file,
                reference_reader as Arc<dyn crate::conversation::ContentResidencyVerifier>,
            ));
        }
        let mut content = crate::model::vec_with_capacity(1, Some(io))?;
        content.push(CreatedContentBinding {
            default_writer: allocation.authority.is_some(),
            artifact_writer: allocation.authority.is_none()
                && allocation.volume.class() == VolumeClass::AgentPrivate,
            scratch_creation: allocation.authority.as_ref().map(|_| {
                creation.clone() as crate::resource_preparation::ChildScratchCreationProof
            }),
            authority: allocation.authority,
            volume: allocation.volume,
            binding: ContentBindings {
                reader,
                writer: Some(writer),
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
