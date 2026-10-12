use super::*;
use crate::conversation::{FileRef, VolumeRef};
/// Authenticated content mapping for one created volume.
pub struct CreatedContentBinding {
    /// Exact provider-authenticated volume.
    pub volume: VolumeRef,
    /// Canonical aggregate audience, when supplied.
    pub authority: Option<Authority>,
    /// Existing provider reader and writer.
    pub binding: crate::runtime::ContentBindings,
    /// Ordinary task content destination.
    pub default_writer: bool,
    /// Explicit provider-authenticated result destination.
    pub artifact_writer: bool,
    /// Protected original scratch allocation identity.
    #[cfg(feature = "filesystem")]
    pub scratch_creation: Option<ChildScratchCreationProof>,
}
/// Authenticated verification result from a trusted registered creator.
pub struct CreatedResourceReceipt {
    /// Original allocation provenance.
    pub origin: ResourceCreationOrigin,
    /// Exact created identity.
    pub resource: ResourceRef,
    /// Protected original evidence.
    pub evidence: ResourceRef,
    /// Immutable allocation generation.
    pub allocation_generation: crate::resources::GenerationRef,
    /// Operations authenticated by the creator contract.
    pub operations: Vec<String>,
    /// Actual volume content mappings.
    pub content: Vec<CreatedContentBinding>,
    /// Exact immutable file readers.
    pub references: Vec<(
        FileRef,
        std::sync::Arc<dyn crate::conversation::ContentResidencyVerifier>,
    )>,
}
/// Creator verifier explicitly registered by the actual task owner.
pub trait ResourceCreationProvider: acyclic_stream::ProviderPlatform {
    /// Pinned creator namespace, family and revision.
    fn provider(&self) -> &ProviderRef;
    /// Authenticates the original evidence without allocating or replacing its fence.
    fn verify<'a>(
        &'a self,
        request: &'a ResourcePreparationRequest,
        proof: &'a ResourceCreationProofRef,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<CreatedResourceReceipt>>;
    /// Authenticates evidence on the caller's original native ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn verify_with_native_work<'a>(
        &'a self,
        _request: &'a ResourcePreparationRequest,
        _proof: &'a ResourceCreationProofRef,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (CreatedResourceReceipt, acyclic_fs::NativeAllocation)>>
    {
        Err(Error::NativeBoundary {
            code: Unsupported,
            reason: "provider has no original native-ledger creator verifier",
        })
    }
    /// Borrows protected original ROOT sources and their canonical witnesses.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn root_bootstrap_sources(
        &self,
        _operation: OperationId,
    ) -> [Option<(
        &TaskVolumeSnapshot,
        &crate::durable_host::RootBootstrapBindingWitness,
    )>; 2] {
        [None, None]
    }
    /// Authenticates original ROOT content on the original native ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn verify_root_bootstrap_with_native_work<'a>(
        &'a self,
        _context: RootBootstrapContentContext<'a>,
        _request: &'a TaskVolumeSnapshot,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, CreatedResourceAccess>> {
        Err(Error::NativeBoundary {
            code: Unsupported,
            reason: "provider has no original native-ledger ROOT bootstrap verifier",
        })
    }
}

/// Exact provider evidence for a created resource.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceCreationProofRef {
    /// Pinned creator contract.
    pub provider: ProviderRef,
    /// Preparation task retaining the evidence.
    pub preparation: TaskId,
    /// Exact created identity.
    pub resource: ResourceRef,
    /// Durable provider evidence.
    pub evidence: ResourceRef,
}
// Its private module and crate-only re-export keep implementation in the SDK.
#[cfg(feature = "filesystem")]
#[doc = "Authenticated original scratch allocation."]
pub trait ScratchCreationIdentity: acyclic_stream::ProviderPlatform {
    /// Exact created volume.
    fn volume(&self) -> &crate::conversation::VolumeRef;
    /// Original protected provenance.
    fn origin(&self) -> &ResourceCreationOrigin;
}
/// Shared authenticated scratch allocation.
#[cfg(feature = "filesystem")]
pub type ChildScratchCreationProof = std::sync::Arc<dyn ScratchCreationIdentity>;
