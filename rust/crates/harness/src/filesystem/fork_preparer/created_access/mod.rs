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
mod provider;
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
        let mut observer = io.as_ref().map(PreparationIo::history_io);
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
    /// Borrows both configured allocations without copying proof payloads.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn bound_root_bootstrap_allocations(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<[&Arc<FilesystemRootBootstrapAllocation<A, O>>; 2]> {
        work.remaining()?;
        let project = self.roots[0].get().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "original Root Project allocation is not bound",
        })?;
        let scratch = self.roots[1].get().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "original Root Scratch allocation is not bound",
        })?;
        if !project.matches_native_work(work) || !scratch.matches_native_work(work) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "original Root allocations belong to another materialization ledger",
            });
        }
        Ok([project, scratch])
    }
}
