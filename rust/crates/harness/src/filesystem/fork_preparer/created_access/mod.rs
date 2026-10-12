//! Trusted created-resource bindings using the protected fork journals.
use super::*;
use crate::filesystem::{FilesystemContentPublisher, FilesystemContentVerifier};
use crate::{
    Capabilities,
    conversation::{
        BoundedContentFuture, ContentHold, ContentPublisher, FileRef, PrivateDirectoryPage,
        VolumeClass, VolumeOwner,
    },
    core::AuthorityIssuer,
    resource_preparation::{
        CreatedContentBinding, CreatedReferenceBinding, CreatedResourceOperation,
        CreatedResourceReceipt, ResourceCreationProofRef, ResourceCreationProvider,
        ResourcePreparationRequest,
    },
    resources::GenerationRef,
    runtime::ContentBindings,
};
use acyclic_fs::{CancellationToken, WorkCounters};
mod access;
mod allocation;
mod authentication;
mod lazy;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
use access::CreatedFilesystemAccess;
pub use allocation::FilesystemRootBootstrapAllocation;
use authentication::{AuthenticatedAllocation, authenticate};

/// Trusted creation verifier with host-managed signing authority.
pub struct FilesystemResourceCreationProvider<A, O> {
    host: Arc<FilesystemHost<A, O>>,
    issuer: AuthorityIssuer,
    #[cfg(not(target_arch = "wasm32"))]
    roots: [std::sync::OnceLock<Arc<FilesystemRootBootstrapAllocation<A, O>>>; 2],
    #[cfg(not(target_arch = "wasm32"))]
    native_custody: Option<acyclic_fs::NativeAllocation>,
}

impl<A, O> FilesystemResourceCreationProvider<A, O> {
    /// Copies the signing identity on the optional original native ledger.
    pub fn new(
        host: Arc<FilesystemHost<A, O>>,
        issuer: &AuthorityIssuer,
        #[cfg(not(target_arch = "wasm32"))] work: Option<&acyclic_fs::NativeMaterializationWork>,
    ) -> Result<Self> {
        #[cfg(not(target_arch = "wasm32"))]
        let io = work.map(PreparationIo::from_native_work).transpose()?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = &io {
            io.retain_owned(std::mem::size_of::<Self>() as u64)?;
        }
        #[cfg(target_arch = "wasm32")]
        let io = None;
        let mut observer = io.as_ref();
        let issuer = issuer.copy(
            &mut observer
                .as_mut()
                .map(|observer| observer as &mut dyn crate::store::history::HistoryIo),
        )?;
        #[cfg(not(target_arch = "wasm32"))]
        let native_custody = io
            .map(|io| {
                let retained = io.owned_bytes()?;
                io.into_native_custody(retained)
            })
            .transpose()?;
        Ok(Self {
            host,
            issuer,
            #[cfg(not(target_arch = "wasm32"))]
            roots: std::array::from_fn(|_| std::sync::OnceLock::new()),
            #[cfg(not(target_arch = "wasm32"))]
            native_custody,
        })
    }
    /// Shares the already paid creator on its original ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn into_native_shared(
        self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<Self>> {
        if !self
            .native_custody
            .as_ref()
            .is_some_and(|custody| custody.work().shares_allocation_context(work))
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "resource creator belongs to another original native resource",
            });
        }
        work.charge(WorkCounters {
            allocation_operations: 1,
            ..Default::default()
        })?;
        Ok(Arc::new(self))
    }
    /// Binds this configured creator's protected original Root allocation.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn bind_root_bootstrap_allocation_with_native_work(
        &self,
        proof: Arc<FilesystemRootBootstrapAllocation<A, O>>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        work.remaining()?;
        let configured = {
            let io = PreparationIo::from_native_work(work)?;
            let verifier = self.issuer.verifier(&mut Some(&mut io.history_io()))?;
            proof.creator == verifier
        };
        if !Arc::ptr_eq(&proof.host, &self.host)
            || proof.owner() != self.issuer.audience()
            || !configured
            || !proof.matches_native_work(work)
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "Root allocation is not this configured creator's protected original",
            });
        }
        let (slot, other) = match proof.request.role {
            crate::resource_preparation::RootBootstrapContentRole::Project => {
                (&self.roots[0], self.roots[1].get())
            }
            crate::resource_preparation::RootBootstrapContentRole::Scratch => {
                (&self.roots[1], self.roots[0].get())
            }
        };
        let root = proof.witness.binding();
        if other.is_some_and(|other| {
            other.witness.binding().original_task() != root.original_task()
                || other.owner() != proof.owner()
                || other.witness.binding().agent() != root.agent()
                || other.admission_digest != proof.admission_digest
        }) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "configured Root allocations belong to different original admissions",
            });
        }
        if let Err(proof) = slot.set(proof)
            && !slot.get().is_some_and(|prior| Arc::ptr_eq(prior, &proof))
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Conflict,
                reason: "configured original Root allocation changed",
            });
        }
        Ok(())
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> ResourceCreationProvider
    for FilesystemResourceCreationProvider<A, O>
{
    fn provider(&self) -> &ProviderRef {
        &self.host.provider
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn root_bootstrap_sources(
        &self,
        operation: OperationId,
    ) -> [Option<(
        &crate::resource_preparation::RootBootstrapContentRequest,
        &crate::durable_host::RootBootstrapBindingWitness,
    )>; 2] {
        self.roots.each_ref().map(|slot| {
            slot.get()
                .filter(|allocation| allocation.request.root_operation == operation)
                .map(|allocation| (&allocation.request, allocation.witness.as_ref()))
        })
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
            let bytes = io.owned_bytes()?;
            Ok((receipt, io.into_native_custody(bytes)?))
        })
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn verify_root_bootstrap_with_native_work<'a>(
        &'a self,
        context: crate::resource_preparation::RootBootstrapContentContext<'a>,
        request: &'a crate::resource_preparation::RootBootstrapContentRequest,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<
            'a,
            (
                crate::resource_preparation::RootBootstrapContentReceipt,
                acyclic_fs::NativeAllocation,
            ),
        >,
    > {
        use crate::resource_preparation::{RootBootstrapContentReceipt, RootBootstrapContentRole};
        use crate::store::history::HistoryIo as _;
        crate::resource_preparation::preflight_root_bootstrap_with_native_work(
            context, request, work,
        )?;
        crate::NativeFutureBundle::new(work, || async move {
            let index = match request.role {
                RootBootstrapContentRole::Project => 0,
                RootBootstrapContentRole::Scratch => 1,
            };
            let allocation = self.roots[index].get().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "Root content has no protected original allocation",
            })?;
            let root = allocation.witness.binding();
            if context.owner() != allocation.owner()
                || context.audience() != root.audience()
                || context.agent() != root.agent()
                || context.original_root().task_id() != root.original_task()
                || context.original_admission_digest() != allocation.admission_digest
                || request != &allocation.request
            {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "Root content differs from its protected original association",
                });
            }
            let io = PreparationIo::from_native_work(work)?;
            let [read, write] = content_grants(&request.volume, &io)?;
            if !context.original_root().grants.contains(&read) {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "Root admission does not declare content READ",
                });
            }
            let writable = allocation.publication_lease.is_some()
                && context.original_root().grants.contains(&write);
            allocation.reauthenticate_with_io(&io).await?;
            io.retain_owned(std::mem::size_of::<String>() as u64 * (1 + u64::from(writable)))?;
            let capabilities = if writable {
                Capabilities::new([read, write])
            } else {
                let bytes = write.capacity() as u64;
                drop(write);
                io.release(bytes)?;
                Capabilities::new([read])
            };
            let (reader, writer) = self.content_bindings(
                &request.volume,
                context.agent(),
                context.original_root().limits.file_bytes,
                capabilities,
                &io,
            )?;
            io.retain_owned(std::mem::size_of::<CreatedFilesystemAccess<A, O>>() as u64)?;
            io.allocation_attempt(std::mem::size_of::<CreatedFilesystemAccess<A, O>>() as u64)?;
            let access = Arc::new(CreatedFilesystemAccess::Root {
                allocation: Arc::clone(allocation),
                _custody: io.take_native_custody(io.owned_bytes()?)?,
                reader,
                writer,
            });
            io.retain_owned(
                (std::mem::size_of::<RootBootstrapContentReceipt>()
                    - std::mem::size_of::<crate::resource_preparation::RootBootstrapContentRequest>(
                    )) as u64,
            )?;
            let (request, custody) = request.copy(work)?;
            io.absorb_native_custody(custody)?;
            let audience = io.history_io().clone_authority(root.audience())?;
            let receipt = RootBootstrapContentReceipt {
                request,
                audience,
                agent: context.agent(),
                binding: ContentBindings {
                    reader: access.clone(),
                    writer: writable.then_some(access),
                },
            };
            let bytes = io.owned_bytes()?;
            Ok((receipt, io.into_native_custody(bytes)?))
        })
    }
}

fn content_grants(volume: &VolumeRef, io: &PreparationIo) -> Result<[String; 2]> {
    let name = io.capability(volume, "harness-")?;
    let held = io.guard_owned(name.capacity() as u64)?;
    let grants = crate::executor::with_history_io(Some(io), |work| {
        Ok([
            crate::contract::preparation_format(format_args!("volume:read:{}", &name[8..]), work)?,
            crate::contract::preparation_format(format_args!("volume:write:{}", &name[8..]), work)?,
        ])
    });
    drop(name);
    drop(held);
    grants
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    FilesystemResourceCreationProvider<A, O>
{
    fn content_bindings(
        &self,
        volume: &VolumeRef,
        agent: crate::AgentId,
        maximum: u64,
        capabilities: Capabilities,
        io: &PreparationIo,
    ) -> Result<(
        Arc<FilesystemContentVerifier<A, O>>,
        Option<Arc<FilesystemContentPublisher<A, O>>>,
    )> {
        let writable = capabilities
            .iter()
            .any(|grant| grant.starts_with("volume:write:"));
        let scope = self.issuer.issue(
            "created-content",
            &capabilities,
            Some(agent),
            None,
            &mut Some(&mut io.history_io()),
        )?;
        let verifier = self.issuer.verifier(&mut Some(&mut io.history_io()))?;
        let temporary = crate::core::owned_sum([
            capabilities.logical_owned_bytes()?,
            scope.logical_owned_bytes()?,
            verifier.logical_owned_bytes()?,
        ])?;
        let reader = FilesystemContentVerifier::new(
            self.host.clone(),
            &verifier,
            &scope,
            maximum,
            Some(io),
        )?;
        let writer = if writable {
            Some(FilesystemContentPublisher::new(
                self.host.clone(),
                volume,
                &verifier,
                &scope,
                maximum,
                Some(io),
            )?)
        } else {
            None
        };
        drop((capabilities, scope, verifier));
        io.release(temporary)?;
        Ok((reader, writer))
    }
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
        let volume_bytes = allocation.volume.logical_owned_bytes()?;
        io.retain_owned(crate::core::owned_sum([
            volume_bytes,
            volume_bytes,
            allocation.generation.logical_owned_bytes()?,
            proof.resource.logical_owned_bytes()?,
            proof.evidence.logical_owned_bytes()?,
            std::mem::size_of::<CreatedFilesystemAccess<A, O>>() as u64,
        ])?)?;
        io.allocation_attempt(std::mem::size_of::<CreatedFilesystemAccess<A, O>>() as u64)?;
        let grants = content_grants(&allocation.volume, io)?;
        io.retain_owned(2 * std::mem::size_of::<String>() as u64)?;
        let (reader, writer) = self.content_bindings(
            &allocation.volume,
            allocation.agent,
            request.maximum_bytes,
            Capabilities::new(grants),
            io,
        )?;
        let access = Arc::new(CreatedFilesystemAccess::Task {
            request: request.clone(),
            proof: proof.clone(),
            origin: allocation.origin.clone(),
            generation: allocation.generation.clone(),
            prepared_generation: allocation.prepared_generation,
            creation_operation: allocation.creation_operation,
            reader,
            writer: writer.expect("task creation grants WRITE"),
        });
        let count = allocation.reference_files.len();
        let mut operations = crate::model::vec_with_capacity(
            count
                .checked_add(2)
                .ok_or_else(|| io.invalid("creator receipt item count overflow"))?,
            Some(io),
        )?;
        let mut references = crate::model::vec_with_capacity(count, Some(io))?;
        operations.extend([
            CreatedResourceOperation::ReadVolume(allocation.volume.clone()),
            CreatedResourceOperation::WriteVolume(allocation.volume.clone()),
        ]);
        for file in allocation.reference_files {
            let bytes = file.logical_owned_bytes()?;
            io.retain_owned(crate::core::owned_sum([
                bytes,
                bytes,
                std::mem::size_of::<CreatedFilesystemAccess<A, O>>() as u64,
            ])?)?;
            io.allocation_attempt(std::mem::size_of::<CreatedFilesystemAccess<A, O>>() as u64)?;
            let reader = Arc::new(CreatedFilesystemAccess::Reference {
                allocation: access.clone(),
                file: file.clone(),
            });
            operations.push(CreatedResourceOperation::ReadFile(file.clone()));
            references.push(CreatedReferenceBinding { file, reader });
        }
        let mut content = crate::model::vec_with_capacity(1, Some(io))?;
        content.push(CreatedContentBinding {
            default_writer: allocation.authority.is_some(),
            artifact_writer: allocation.authority.is_none()
                && allocation.volume.class() == VolumeClass::AgentPrivate,
            scratch_creation: allocation.authority.as_ref().map(|_| {
                crate::resource_preparation::ChildScratchCreationProof::new(access.clone())
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
