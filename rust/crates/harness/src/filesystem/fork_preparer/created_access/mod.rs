//! Trusted created-resource bindings using the protected fork journals.
use super::*;
use crate::filesystem::{FilesystemContentPublisher, FilesystemContentVerifier};
use crate::{
    Capabilities,
    conversation::{FileRef, VolumeClass},
    core::AuthorityIssuer,
    resource_preparation::{
        CreatedContentBinding, CreatedResourceReceipt, ResourceCreationProofRef,
        ResourceCreationProvider, ResourcePreparationRequest,
    },
    resources::GenerationRef,
    runtime::ContentBindings,
};
use acyclic_fs::CancellationToken;
#[cfg(not(target_arch = "wasm32"))]
use acyclic_fs::WorkCounters;
mod authentication;
mod provenance;
mod provider;
use authentication::authenticate;
pub(in crate::filesystem) use provenance::CreatedProvenance;

/// Trusted creation verifier with host-managed signing authority.
pub struct FilesystemResourceCreationProvider<A, O> {
    host: Arc<FilesystemHost<A, O>>,
    issuer: AuthorityIssuer,
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
}
