//! Task-bound resource creation requests and authenticated preparation leases.

use crate::core::Authority;
use crate::resources::{ProviderRef, ResourceRef};
use crate::runtime::TaskAdmissionRecord;
use crate::scheduler::LeaseFence;
use crate::{Capabilities, Error, OperationId, Result, TaskId};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;
#[cfg(feature = "filesystem")]
use std::sync::Arc;

mod root_bootstrap;
pub use root_bootstrap::{
    RootBootstrapContentReceipt, RootBootstrapContentRequest, RootBootstrapContentRole,
};
#[cfg(feature = "filesystem")]
pub use root_bootstrap::RootBootstrapContentContext;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) use root_bootstrap::preflight_root_bootstrap_with_native_work;

/// Exact provider evidence to authenticate before deriving child resource access.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceCreationProofRef {
    /// Trusted creation provider and immutable verifier contract revision.
    pub provider: ProviderRef,
    /// Ordinary preparation task whose allocation evidence must be retained.
    pub preparation: TaskId,
    /// Exact created resource whose access may be derived after verification.
    pub resource: ResourceRef,
    /// Provider-owned durable evidence, independent of a mutable resource head.
    pub evidence: ResourceRef,
}

impl ResourceCreationProofRef {
    /// Validates reference structure without authenticating provider evidence.
    pub(crate) fn validate(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(observer) = work.as_deref_mut() {
            observer.check_cancellation()?;
            observer.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
        }
        self.provider.validate_with_work(work)?;
        self.resource.validate_with_work(work)?;
        self.evidence.validate_with_work(work)
    }
}

/// One immutable resource request admitted as an ordinary preparation task's input.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourcePreparationRequest {
    /// Trusted creation provider and contract revision.
    pub provider: ProviderRef,
    /// Complete intended child admission, including its requested scope.
    pub child: TaskAdmissionRecord,
    /// Exact identities that this preparation may create.
    pub resources: Vec<ResourceRef>,
    /// Explicit creation capability already narrowed from the actual parent.
    pub creation_grant: String,
    /// Maximum resources in this request; no universal volume count is imposed.
    pub maximum_resources: u32,
    /// Maximum provider work units, interpreted by the pinned provider contract.
    pub maximum_work: u64,
    /// Maximum canonical input and provider IO bytes under this request.
    pub maximum_bytes: u64,
    /// Complete provider-specific request, never substituted on recovery.
    pub payload: Value,
    /// Optional ordinary resumable outbox command.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<crate::workflow::WorkflowCommand>,
    /// Immutable original caller checkpoint for source restoration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_checkpoint: Option<crate::conversation::FileRef>,
    /// Explicit immutable inputs for the intended child, not a reference scan or a grant.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub input_files: Vec<crate::conversation::FileRef>,
    /// Explicit selected Project authorization; the owner verifies and seals it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_authorization: Option<crate::source_authorization::ProjectSourceAuthorization>,
}

#[cfg(feature = "filesystem")]
impl crate::filesystem::PreparationRecord for ResourcePreparationRequest {
    fn maximum_slot_bytes() -> u64 {
        crate::filesystem::preparation_slot_bytes::<Self>().max(Self::decode_slot_bytes())
    }

    fn json_value_schema() -> crate::contract::JsonValueSchema {
        crate::contract::JsonValueSchema::ResourcePreparationRequest
    }
}

impl ResourcePreparationRequest {
    /// Every actually owned variable-capacity field, excluding the inline Self.
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        let overflow = || {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            { Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "resource preparation ownership overflows" } }
            #[cfg(any(not(feature = "filesystem"), target_arch = "wasm32"))]
            { Error::Invalid("resource preparation ownership overflows".into()) }
        };
        let mut bytes = 0_u64;
        let mut add = |owned: u64| -> Result<()> {
            bytes = bytes.checked_add(owned).ok_or_else(overflow)?;
            Ok(())
        };
        add(self.provider.logical_owned_bytes()?)?;
        add(self.child.logical_owned_bytes()?)?;
        add(self.creation_grant.capacity() as u64)?;
        add(crate::contract::json_value_logical_owned_bytes(&self.payload)?)?;
        add((self.resources.capacity() as u64)
            .checked_mul(std::mem::size_of::<ResourceRef>() as u64).ok_or_else(overflow)?)?;
        for resource in &self.resources { add(resource.logical_owned_bytes()?)?; }
        if let Some(command) = &self.command {
            add(command.kind.capacity() as u64)?;
            add(command.payload.logical_owned_bytes()?)?;
        }
        if let Some(checkpoint) = &self.source_checkpoint { add(checkpoint.logical_owned_bytes()?)?; }
        add((self.input_files.capacity() as u64)
            .checked_mul(std::mem::size_of::<crate::conversation::FileRef>() as u64)
            .ok_or_else(overflow)?)?;
        for file in &self.input_files { add(file.logical_owned_bytes()?)?; }
        if let Some(authorization) = &self.read_authorization { add(authorization.logical_owned_bytes()?)?; }
        Ok(bytes)
    }

    /// Decodes the closed preparation DTO from the caller's original retained value.
    pub(crate) fn from_borrowed_value_with_work(
        value: &Value,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(Self, u64)> {
        if let Some(observer) = work.as_deref_mut() { observer.check_cancellation()?; }
        let maximum = value.get("maximum_bytes").and_then(Value::as_u64)
            .filter(|maximum| *maximum != 0 && *maximum != u64::MAX)
            .ok_or_else(|| crate::scheduler::diagnostic(work, crate::wire::ErrorCode::Invalid, "retained preparation has no finite byte bound"))?;
        let mut quota = crate::wire_codec::CodecQuota::from_retained(work, 0);
        let encoded = crate::store::history::canonical_bytes_with_work(value, maximum, quota.work())?;
        let encoded_extent = encoded.capacity() as u64;
        quota.adopt(encoded_extent)?;
        let decoded: Result<_> = (|| {
            if let Some(observer) = quota.work().as_deref_mut() {
                observer.hashed_bytes(encoded.len() as u64)?;
            }
            let original = *blake3::hash(&encoded).as_bytes();
            let (request, reserved) = Self::decode_retained_bytes(&encoded, quota.work())?;
            Ok((request, reserved, original))
        })();
        if let Ok((_, reserved, _)) = &decoded { quota.adopt(*reserved)?; }
        drop(encoded);
        let released = quota.relinquish(encoded_extent);
        let (request, reserved, original) = decoded?;
        released?;
        request.validate(quota.work())?;
        let retained = crate::contract::canonical_json_digest_with_work(
            &request, request.maximum_bytes, quota.work())?;
        if original != retained {
            return Err(crate::scheduler::diagnostic(quota.work(), crate::wire::ErrorCode::Invalid,
                "retained preparation input changes its canonical identity on decode"));
        }
        let extent = crate::core::resource_preparation_owned_bytes(&request, quota.work())?;
        if extent > reserved {
            return Err(crate::scheduler::diagnostic(quota.work(), crate::wire::ErrorCode::Invalid,
                "closed preparation inventory exceeded its admitted reservation"));
        }
        quota.replace(extent)?;
        quota.transfer();
        Ok((request, extent))
    }

    fn decode_slot_bytes() -> u64 {
        [
            std::mem::size_of::<Self>(),
            std::mem::size_of::<TaskAdmissionRecord>(),
            std::mem::size_of::<ResourceCreationProofRef>(),
            std::mem::size_of::<ResourceRef>(),
            std::mem::size_of::<ProviderRef>(),
            std::mem::size_of::<crate::conversation::FileRef>(),
            std::mem::size_of::<crate::workflow::WorkflowCommand>(),
            std::mem::size_of::<crate::source_authorization::ProjectSourceAuthorization>(),
            std::mem::size_of::<(String, Value)>(),
        ].into_iter().max().unwrap_or(0) as u64
    }

    fn decode_retained_bytes(
        bytes: &[u8],
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(Self, u64)> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(original) = work.as_deref().and_then(|observer| observer.original_native_work()) {
            let io = crate::filesystem::PreparationIo::from_native_work(original)?;
            let decoded = io.decode_preparation::<Self>(bytes, crate::wire::ErrorCode::Invalid)?;
            let (request, custody) = crate::filesystem::native_decoded(decoded)?;
            let extent = custody.bytes();
            if let Some(observer) = work.as_deref_mut() {
                if let Err(error) = observer.absorb_native_custody(custody) {
                    drop(request);
                    return Err(error);
                }
            }
            return Ok((request, extent));
        }
        let bounds = crate::contract::JsonBounds {
            maximum_bytes: bytes.len() as u64,
            maximum_items: (bytes.len() as u64).saturating_add(1),
            maximum_depth: 128,
        };
        let mut items = 0_u64;
        let mut quota = crate::wire_codec::CodecQuota::from_retained(work, 0);
        let result = crate::contract::json_from_slice_bounded(std::marker::PhantomData,
            bytes, bounds, 0, None, None, &mut |next| {
                match next {
                    crate::contract::JsonCodecCharge::Work { items: count, .. } => {
                        items = items.checked_add(count)
                            .ok_or_else(|| Error::Invalid("preparation DTO item count overflow".into()))?;
                        if let Some(observer) = quota.work().as_deref_mut() { observer.codec_charge(next)?; }
                        Ok(())
                    }
                    crate::contract::JsonCodecCharge::Admission { maximum_owned_bytes, .. } => {
                        let owned = items.checked_mul(Self::decode_slot_bytes())
                            .and_then(|slots| slots.checked_mul(8))
                            .and_then(|slots| (bytes.len() as u64).checked_mul(4)
                                .and_then(|strings| slots.checked_add(strings)))
                            .and_then(|owned| owned.checked_add(maximum_owned_bytes))
                            .and_then(|owned| owned.checked_add(std::mem::size_of::<Self>() as u64))
                            .ok_or_else(|| Error::Invalid("preparation DTO ownership overflow".into()))?;
                        quota.replace(owned)?;
                        Ok(())
                    }
                    next => {
                        if let Some(observer) = quota.work().as_deref_mut() { observer.codec_charge(next)?; }
                        Ok(())
                    }
                }
            },
        );
        let request = result?;
        let retained = quota.retained();
        quota.transfer();
        Ok((request, retained))
    }

    /// Checks the complete request with the caller's optional work observer.
    pub fn validate(
        &self,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<()> {
        if let Some(observer) = work.as_deref_mut() { observer.check_cancellation()?; }
        self.provider.validate_with_work(work)?;
        if self.child.parent.is_none()
            || self.resources.is_empty()
            || self.resources.len() as u64 > u64::from(self.maximum_resources)
            || self.maximum_work == 0
            || self.maximum_work == u64::MAX
            || self.maximum_bytes == 0
            || self.maximum_bytes == u64::MAX
            || self.creation_grant.is_empty()
            || self.creation_grant.chars().any(char::is_control)
        {
            return Err(crate::scheduler::diagnostic(work, crate::wire::ErrorCode::Invalid, "resource preparation lacks a parent, authority or finite work bounds"));
        }
        if work.is_some() {
            let encoded = crate::store::history::canonical_bytes_with_work(self, self.maximum_bytes, work)?;
            let encoded_extent = encoded.capacity() as u64;
            drop(encoded);
            if let Some(observer) = work.as_deref_mut() { observer.release_owned(encoded_extent)?; }
        } else {
            crate::contract::validate_json_byte_bound(self, self.maximum_bytes)?;
        }
        if let Some(checkpoint) = &self.source_checkpoint {
            checkpoint.validate(work)?;
            if checkpoint.descriptor().byte_length() > self.maximum_bytes {
                return Err(crate::scheduler::diagnostic(work, crate::wire::ErrorCode::Invalid, "source checkpoint exceeds preparation byte bound"));
            }
        }
        if self.input_files.len() as u64 > self.maximum_work {
            return Err(crate::scheduler::diagnostic(work, crate::wire::ErrorCode::Invalid, "child input reference count exceeds preparation work bound"));
        }
        let mut input_bytes = 0_u64;
        for (index, file) in self.input_files.iter().enumerate() {
            if let Some(observer) = work.as_deref_mut() {
                observer.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: index as u64 + 1 })?;
            }
            file.validate(work)?;
            input_bytes = input_bytes.checked_add(file.descriptor().byte_length())
                .ok_or_else(|| crate::scheduler::diagnostic(work, crate::wire::ErrorCode::Invalid, "child input reference byte bound overflow"))?;
            if input_bytes > self.maximum_bytes || self.input_files[..index].contains(file) {
                return Err(crate::scheduler::diagnostic(work, crate::wire::ErrorCode::Invalid, "child input references exceed their bound or repeat an identity"));
            }
        }
        if let Some(authorization) = &self.read_authorization {
            authorization.validate(work)?;
        }
        if let Some(command) = &self.command {
            command.validate(work)?;
            if command.operation_id == self.child.operation_id {
                return Err(crate::scheduler::diagnostic(work, crate::wire::ErrorCode::Invalid, "preparation effect and intended child identities must differ"));
            }
        }
        self.child.validate_with_work(work)?;
        for (index, resource) in self.resources.iter().enumerate() {
            if let Some(observer) = work.as_deref_mut() {
                observer.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: index as u64 + 1 })?;
            }
            resource.validate_with_work(work)?;
            if self.resources[..index].contains(resource) {
                return Err(crate::scheduler::diagnostic(work, crate::wire::ErrorCode::Invalid, "resource preparation repeats an identity"));
            }
        }
        for proof in &self.child.resource_proofs {
            if let Some(observer) = work.as_deref_mut() {
                observer.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: self.resources.len() as u64 + 1 })?;
            }
            if self.resources.contains(&proof.resource) {
                if proof.provider != self.provider {
                    return Err(crate::scheduler::diagnostic(work, crate::wire::ErrorCode::Invalid, "child proof belongs to another creation provider"));
                }
            }
        }
        if let Some(observer) = work.as_deref_mut() {
            let comparisons = (self.resources.len() as u64)
                .checked_mul(self.child.resource_proofs.len() as u64)
                .ok_or_else(|| Error::Invalid("preparation proof comparison work overflows".into()))?;
            observer.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: comparisons })?;
        }
        if self.resources.iter().any(|resource| !self.child.resource_proofs.iter()
            .any(|proof| &proof.resource == resource)) {
            return Err(crate::scheduler::diagnostic(work, crate::wire::ErrorCode::Invalid, "child proofs differ from requested created resources"));
        }
        Ok(())
    }
}

/// Provenance retained inside a trusted provider's existing allocation record.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceCreationOrigin {
    /// Actual owner of the preparation task's coordinator.
    pub owner: Authority,
    /// Actual parent, shared by the preparation and intended child.
    pub parent_task: TaskId,
    /// Ordinary admitted task that owns this resource creation.
    pub preparation_task: TaskId,
    /// Exact intended child admission operation.
    pub intended_child: OperationId,
    /// Digest of the preparation task's complete retained admission.
    pub admission_digest: [u8; 32],
    /// Exact authenticated preparation-task donor grants, not provider-root scope.
    pub source_grants: Capabilities,
    /// Digest of the complete immutable resource request and requested child scope.
    pub request_digest: [u8; 32],
    /// Original creation lease, distinct from a later recovery lease.
    pub creation_fence: LeaseFence,
}

/// Sealed, process-local binding to an authenticated preparation task lease.
#[cfg(feature = "filesystem")]
pub struct TaskResourcePreparation {
    pub(crate) owner: std::sync::Arc<dyn ResourcePreparationLease>,
    pub(crate) request: ResourcePreparationRequest,
    pub(crate) origin: ResourceCreationOrigin,
    pub(crate) project_source: Option<crate::source_authorization::ProjectSourceReadHandle>,
    // Actual native producer's intrinsic custody follows every owned payload.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) native_custody: Option<acyclic_fs::NativeAllocation>,
}

#[cfg(feature = "filesystem")]
impl TaskResourcePreparation {
    /// Exact admitted request, including provider work bounds and intended child.
    #[must_use]
    pub const fn request(&self) -> &ResourcePreparationRequest {
        &self.request
    }

    /// Original provenance to retain with an allocation, never a bearer grant.
    #[must_use]
    pub const fn origin(&self) -> &ResourceCreationOrigin {
        &self.origin
    }

    /// Authenticated explicit Project source, sealed by the original task owner.
    #[must_use]
    pub fn project_source(&self) -> Option<&crate::source_authorization::ProjectSourceReadHandle> {
        self.project_source.as_ref()
    }

    /// Borrowed original preparation-task grants.
    #[must_use]
    pub fn grants(&self) -> &crate::Capabilities {
        &self.origin.source_grants
    }

    /// Borrowed actual preparation invocation token, if its owner was bound at the caller's live invocation boundary.
    #[must_use]
    pub fn cancellation(&self) -> Option<&acyclic_fs::CancellationToken> {
        self.owner.cancellation()
    }

    /// Requires the current uncancelled preparation lease before fresh allocation.
    pub async fn verify_creation(&self) -> Result<()> {
        self.owner.verify_lease(false).await
    }

    /// Verifies original native creation authority before allocating a future.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn verify_creation_with_native_work<'a>(
        &'a self, work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        self.require_native_custody(work)?;
        self.owner.verify_lease_with_native_work(false, work)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn require_native_custody(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        if !self.native_custody.as_ref().is_some_and(|custody| custody.matches_work(work)) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "preparation token has no original native metadata custody" });
        }
        Ok(())
    }

    /// Permits observing an existing uncertain allocation under a retained lease.
    pub async fn verify_reconciliation(&self) -> Result<()> {
        self.owner.verify_lease(true).await
    }

    /// Observes the same retained native lease; it authorizes no new creation.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn verify_reconciliation_with_native_work<'a>(
        &'a self, work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        self.require_native_custody(work)?;
        self.owner.verify_lease_with_native_work(true, work)
    }
}

#[cfg(feature = "filesystem")]
pub(crate) trait ResourcePreparationLease: acyclic_stream::ProviderPlatform {
    fn verify_lease(&self, settlement: bool) -> acyclic_stream::BoxProviderFuture<'_, Result<()>>;
    fn cancellation(&self) -> Option<&acyclic_fs::CancellationToken>;

    #[cfg(not(target_arch = "wasm32"))]
    fn verify_lease_with_native_work<'a>(
        &'a self, settlement: bool, work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>>;
}

#[cfg(feature = "filesystem")]
impl<P: acyclic_stream::StreamProvider> ResourcePreparationLease
    for crate::durable_host::TaskJournalOwner<P>
{
    fn cancellation(&self) -> Option<&acyclic_fs::CancellationToken> {
        self.invocation_cancellation()
    }

    fn verify_lease(&self, settlement: bool) -> acyclic_stream::BoxProviderFuture<'_, Result<()>> {
        Box::pin(async move { self.verify(settlement, &mut None).await })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn verify_lease_with_native_work<'a>(
        &'a self, settlement: bool, work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        if !self.native_shared_work_matches(work) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "preparation lease has no original shared native owner custody" });
        }
        crate::NativeFutureBundle::new(work, || async move {
            self.verify_with_native_work(settlement, work).await
        })
    }
}

/// An operation declared by a trusted creation provider for one exact resource.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CreatedResourceOperation {
    /// Provider-neutral operation on this resource only.
    Named(String),
    /// Actual filesystem content read authority for an exact volume.
    ReadVolume(crate::conversation::VolumeRef),
    /// Actual filesystem content publication authority for an exact volume.
    WriteVolume(crate::conversation::VolumeRef),
    /// Exact immutable inherited-file read delegated by an authenticated creator.
    ReadFile(crate::conversation::FileRef),
}

/// Provider-authenticated content handles for an exact created volume.
#[derive(Clone)]
pub struct CreatedContentBinding {
    /// Exact volume mapped by the provider's authenticated allocation record.
    pub volume: crate::conversation::VolumeRef,
    /// Exact aggregate audience authenticated with this allocation, if the provider supplies a canonical published child binding.
    pub authority: Option<crate::core::Authority>,
    /// Existing provider reader/writer machinery, not a second content engine.
    pub binding: crate::runtime::ContentBindings,
    /// Selects the task's ordinary content destination; at most one is allowed.
    pub default_writer: bool,
    /// Explicit result slot authenticated by the allocation provider; never inferred from private volume ordering.
    pub artifact_writer: bool,
    /// Original protected filesystem allocation identity, not a physical leaf receipt.
    #[cfg(feature = "filesystem")]
    pub scratch_creation: Option<ChildScratchCreationProof>,
}

#[cfg(feature = "filesystem")]
pub(crate) trait ScratchCreationIdentity: Send + Sync {
    fn original_task(&self) -> crate::TaskId;
    fn agent(&self) -> crate::AgentId;
    fn volume(&self) -> &crate::conversation::VolumeRef;
    fn creation_operation(&self) -> acyclic_fs::OperationId;
    fn initial_generation(&self) -> &crate::resources::GenerationRef;
    fn origin(&self) -> &ResourceCreationOrigin;
}

/// Opaque identity of the original child scratch allocation.
#[cfg(feature = "filesystem")]
#[derive(Clone)]
pub struct ChildScratchCreationProof {
    identity: Arc<dyn ScratchCreationIdentity>,
}

#[cfg(feature = "filesystem")]
impl ChildScratchCreationProof {
    pub(crate) fn new(identity: Arc<dyn ScratchCreationIdentity>) -> Self { Self { identity } }
    /// Original task identity authenticated by the allocation provider.
    pub fn original_task(&self) -> crate::TaskId { self.identity.original_task() }
    /// Agent authenticated as the owner of this scratch allocation.
    pub fn agent(&self) -> crate::AgentId { self.identity.agent() }
    /// Exact created scratch volume; its identity is not a read or write grant.
    pub fn volume(&self) -> &crate::conversation::VolumeRef { self.identity.volume() }
    /// Original filesystem creation operation retained by the provider.
    pub fn creation_operation(&self) -> acyclic_fs::OperationId { self.identity.creation_operation() }
    /// Immutable initial allocation generation, never a refreshed head.
    pub fn initial_generation(&self) -> &crate::resources::GenerationRef { self.identity.initial_generation() }
    pub(crate) fn origin(&self) -> &ResourceCreationOrigin { self.identity.origin() }
    pub(crate) fn same_identity(&self, other: &Self) -> bool {
        self.original_task() == other.original_task() && self.agent() == other.agent()
            && self.volume() == other.volume() && self.creation_operation() == other.creation_operation()
            && self.initial_generation() == other.initial_generation() && self.origin() == other.origin()
    }


}

/// One immutable inherited file and its authenticated exact-file reader.
#[derive(Clone)]
pub struct CreatedReferenceBinding {
    /// Complete exact file identity authenticated by the original creator seal.
    pub file: crate::conversation::FileRef,
    /// Read-only provider handle restricted to that file and immutable version.
    pub reader: std::sync::Arc<dyn crate::conversation::ContentResidencyVerifier>,
}

/// Authenticated result of one explicitly trusted provider verification.
#[derive(Clone)]
pub struct CreatedResourceReceipt {
    /// Provenance retained with the original allocation.
    pub origin: ResourceCreationOrigin,
    /// Exact created identity.
    pub resource: ResourceRef,
    /// Exact protected evidence identified in the admitted proof.
    pub evidence: ResourceRef,
    /// Immutable original allocation generation, never a mutable resource head.
    pub allocation_generation: crate::resources::GenerationRef,
    /// Finite operations declared by this provider's pinned contract.
    pub operations: Vec<CreatedResourceOperation>,
    /// Actual content mappings for filesystem-compatible resources.
    pub content: Vec<CreatedContentBinding>,
    /// Reference-only readers; these never describe a new volume allocation.
    pub references: Vec<CreatedReferenceBinding>,
}

/// Trusted provider-neutral verifier registered at the actual task owner.
pub trait ResourceCreationProvider: acyclic_stream::ProviderPlatform {
    /// Pinned provider namespace, family and immutable contract revision.
    fn provider(&self) -> &ProviderRef;
    /// Authenticates evidence without allocating, issuing another request or substituting a recovery fence for the receipt's original creation fence.
    fn verify<'a>(
        &'a self,
        request: &'a ResourcePreparationRequest,
        proof: &'a ResourceCreationProofRef,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<CreatedResourceReceipt>>;

    /// Authenticates the same creator proof using the caller's original native ledger/token.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn verify_with_native_work<'a>(
        &'a self, _request: &'a ResourcePreparationRequest,
        _proof: &'a ResourceCreationProofRef, _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (CreatedResourceReceipt, acyclic_fs::NativeAllocation)>> {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "provider has no original native-ledger creator verifier",
        })
    }

    /// Borrows the configured original ROOT sources and their opaque canonical witnesses.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn root_bootstrap_sources(
        &self,
        _operation: OperationId,
    ) -> [Option<(&RootBootstrapContentRequest, &crate::durable_host::RootBootstrapBindingWitness)>; 2] {
        [None, None]
    }

    /// Authenticates protected original ROOT content on the original native ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn verify_root_bootstrap_with_native_work<'a>(
        &'a self,
        _context: RootBootstrapContentContext<'a>,
        _request: &'a RootBootstrapContentRequest,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (RootBootstrapContentReceipt, acyclic_fs::NativeAllocation)>> {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "provider has no original native-ledger ROOT bootstrap verifier",
        })
    }
}

/// A private kernel-produced seal, never deserialized from user input.
pub(crate) struct CreatedResourceAccess {
    pub(crate) operation: OperationId,
    pub(crate) admission_digest: [u8; 32],
    pub(crate) grants: crate::Capabilities,
    pub(crate) content: Vec<CreatedContentBinding>,
    pub(crate) references: Vec<CreatedReferenceBinding>,
    pub(crate) proofs: Vec<ResourceCreationProofRef>,
    pub(crate) publication_lease: Option<std::sync::Arc<dyn CreatedTaskLease>>,
    // SDK-owned logical payload custody.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    native_custody: Option<acyclic_fs::NativeAllocation>,
}

impl CreatedResourceAccess {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn has_native_custody(&self) -> bool { self.native_custody.is_some() }

    pub(crate) fn current_content(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Option<&CreatedContentBinding>> {
        for binding in &self.content {
            created_access_step(work)?;
            if !binding.default_writer { continue; }
            if binding.volume.class() != crate::conversation::VolumeClass::AgentPrivate
                || !matches!(binding.volume.owner(), crate::conversation::VolumeOwner::Agent(_))
            {
                return Err(crate::scheduler::diagnostic(work, crate::wire::ErrorCode::Unauthorized, "current created scratch is not agent-owned private content"));
            }
            return Ok(Some(binding));
        }
        Ok(None)
    }

    pub(crate) fn consume(
        admission: &TaskAdmissionRecord,
        receipts: &mut Vec<CreatedResourceReceipt>,
        current_default_origin: Option<OperationId>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(Self, u64)> {
        created_access_step(work)?;
        if receipts.len() != admission.resource_proofs.len() || receipts.is_empty() {
            return Err(crate::scheduler::diagnostic(work, crate::wire::ErrorCode::Unauthorized, "created resource receipts are incomplete"));
        }
        let admission_digest = admission.canonical_digest_with_work(work)?;
        let mut labels = BTreeSet::new();
        let mut references = Vec::new();
        let mut reference_keys = BTreeSet::new();
        let mut content: Vec<CreatedContentBinding> = Vec::new();
        let reference_count = receipts.iter().try_fold(0_usize, |count, receipt|
            count.checked_add(receipt.references.len()).ok_or_else(created_access_overflow))?;
        let content_count = receipts.iter().try_fold(0_usize, |count, receipt|
            count.checked_add(receipt.content.len()).ok_or_else(created_access_overflow))?;
        created_access_reserve(&mut references, reference_count, work)?;
        created_access_reserve(&mut content, content_count, work)?;
        let mut default_writer = false;
        let mut artifact_writer = false;
        for (proof, mut receipt) in admission.resource_proofs.iter().zip(receipts.drain(..)) {
            created_access_step(work)?;
            if receipt.resource != proof.resource || receipt.evidence != proof.evidence
                || receipt.origin.preparation_task != proof.preparation
                || receipt.allocation_generation.as_resource().provider() != &proof.provider
            {
                return Err(crate::scheduler::diagnostic(work, crate::wire::ErrorCode::Unauthorized, "created resource receipt changed identity"));
            }
            let retired = created_access_receipt_temporary_bytes(&receipt)?;
            let mut reference_labels = BTreeSet::new();
            for operation in &receipt.operations {
                created_access_step(work)?;
                let label = match operation {
                    CreatedResourceOperation::Named(name) => {
                        created_resource_capability(&receipt.resource, name, work)?
                    }
                    CreatedResourceOperation::ReadFile(file) => {
                        let label = created_access_file_capability(file, work)?;
                        if reference_labels.contains(&label) {
                            return Err(crate::scheduler::diagnostic(work, crate::wire::ErrorCode::Unauthorized, "created exact-file operation is ambiguous"));
                        }
                        let copy = if let Some(io) = work.as_deref_mut() {
                            io.clone_text(&label)?
                        } else { label.clone() };
                        created_access_insert_label(&mut reference_labels, copy, work)?;
                        label
                    }
                    CreatedResourceOperation::ReadVolume(volume) => {
                        if volume.provider() != &proof.provider {
                            return Err(crate::scheduler::diagnostic(work, crate::wire::ErrorCode::Unauthorized, "created volume provider differs"));
                        }
                        created_access_volume_capability(volume, crate::conversation::VolumeOperation::Read, work)?
                    }
                    CreatedResourceOperation::WriteVolume(volume) => {
                        if volume.provider() != &proof.provider {
                            return Err(crate::scheduler::diagnostic(work, crate::wire::ErrorCode::Unauthorized, "created volume provider differs"));
                        }
                        created_access_volume_capability(volume, crate::conversation::VolumeOperation::Write, work)?
                    }
                };
                created_access_insert_label(&mut labels, label, work)?;
            }
            for binding in receipt.references.drain(..) {
                created_access_step(work)?;
                let label = created_access_file_capability(&binding.file, work)?;
                let Some(expected) = reference_labels.take(&label) else {
                    return Err(crate::scheduler::diagnostic(work, crate::wire::ErrorCode::Unauthorized, "created exact-file reader differs from this proof"));
                };
                let retired_label = created_access_label_bytes(&expected)?;
                drop(expected);
                created_access_release(work, retired_label)?;
                if reference_keys.contains(&label) {
                    return Err(crate::scheduler::diagnostic(work, crate::wire::ErrorCode::Unauthorized, "created exact-file reader differs from this proof"));
                }
                created_access_insert_label(&mut reference_keys, label, work)?;
                references.push(binding);
            }
            if !reference_labels.is_empty() {
                return Err(crate::scheduler::diagnostic(work, crate::wire::ErrorCode::Unauthorized, "created exact-file operation has no authenticated reader"));
            }
            drop(reference_labels);
            for mut binding in receipt.content.drain(..) {
                created_access_step(work)?;
                if (binding.default_writer || binding.artifact_writer) && binding.binding.writer.is_none() {
                    return Err(crate::scheduler::diagnostic(work, crate::wire::ErrorCode::Unauthorized, "created default content has no writer"));
                }
                binding.default_writer &= current_default_origin == Some(receipt.origin.intended_child);
                binding.artifact_writer &= current_default_origin == Some(receipt.origin.intended_child);
                #[cfg(feature = "filesystem")]
                if let Some(scratch) = &binding.scratch_creation {
                    if scratch.volume() != &binding.volume || scratch.origin() != &receipt.origin
                        || binding.artifact_writer
                    {
                        return Err(crate::scheduler::diagnostic(work, crate::wire::ErrorCode::Unauthorized, "scratch proof differs from created allocation"));
                    }
                }
                let read = created_access_volume_capability(&binding.volume, crate::conversation::VolumeOperation::Read, work)?;
                let write = created_access_volume_capability(&binding.volume, crate::conversation::VolumeOperation::Write, work)?;
                if !labels.contains(&read)
                    || binding.volume.provider() != &proof.provider
                    || content.iter().any(|existing| existing.volume == binding.volume)
                    || binding.binding.writer.as_ref().is_some_and(|writer|
                        writer.volume() != &binding.volume || !labels.contains(&write))
                    || (binding.artifact_writer && (artifact_writer || binding.default_writer
                        || binding.binding.writer.is_none()
                        || binding.volume.class() != crate::conversation::VolumeClass::AgentPrivate
                        || !matches!(binding.volume.owner(), crate::conversation::VolumeOwner::Agent(_))))
                    || (binding.default_writer && (default_writer || binding.binding.writer.is_none()))
                {
                    return Err(crate::scheduler::diagnostic(work, crate::wire::ErrorCode::Unauthorized, "created content mapping differs from receipt"));
                }
                let retired_labels = created_access_add(read.capacity() as u64, write.capacity() as u64)?;
                drop((read, write));
                created_access_release(work, retired_labels)?;
                default_writer |= binding.default_writer;
                artifact_writer |= binding.artifact_writer;
                content.push(binding);
            }
            drop(receipt);
            created_access_release(work, retired)?;
        }
        let retired_receipts = created_access_vector_bytes::<CreatedResourceReceipt>(receipts.capacity())?;
        drop(std::mem::take(receipts));
        created_access_release(work, retired_receipts)?;
        let retired_keys = reference_keys.iter().try_fold(0_u64, |bytes, label|
            created_access_add(bytes, created_access_label_bytes(label)?))?;
        drop(reference_keys);
        created_access_release(work, retired_keys)?;
        let mut proofs = Vec::new();
        created_access_reserve(&mut proofs, admission.resource_proofs.len(), work)?;
        for proof in &admission.resource_proofs {
            let (cloned, _) = crate::core::resource_creation_proof_clone_with_custody(proof, work)?;
            proofs.push(cloned);
        }
        if let Some(io) = work.as_deref_mut() {
            io.retain_owned(std::mem::size_of::<Self>() as u64)?;
        }
        let access = Self {
            operation: admission.operation_id,
            admission_digest,
            grants: crate::Capabilities::from_owned_set(labels),
            proofs,
            content,
            references,
            publication_lease: None,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_custody: None,
        };
        let owned = access.logical_owned_bytes()?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut access = access;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(io) = work.as_deref_mut() {
            if io.original_native_work().is_some() {
                access.native_custody = Some(io.take_native_custody(owned)?);
                return Ok((access, 0));
            }
        }
        Ok((access, owned))
    }

    /// Known SDK layout and owned buffer capacities only.
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        let mut bytes = created_access_add(std::mem::size_of::<Self>() as u64, self.grants.logical_owned_bytes()?)?;
        bytes = created_access_add(bytes, created_access_vector_bytes::<CreatedContentBinding>(self.content.capacity())?)?;
        bytes = created_access_add(bytes, created_access_vector_bytes::<CreatedReferenceBinding>(self.references.capacity())?)?;
        bytes = created_access_add(bytes, created_access_vector_bytes::<ResourceCreationProofRef>(self.proofs.capacity())?)?;
        for binding in &self.content {
            bytes = created_access_add(bytes, binding.volume.logical_owned_bytes()?)?;
            if let Some(authority) = &binding.authority {
                bytes = created_access_add(bytes, authority.id.capacity() as u64)?;
            }
        }
        for binding in &self.references {
            bytes = created_access_add(bytes, binding.file.logical_owned_bytes()?)?;
        }
        for proof in &self.proofs {
            bytes = created_access_add(bytes, created_access_proof_bytes(proof)?)?;
        }
        Ok(bytes)
    }
}

fn created_access_step(work: &mut Option<&mut dyn crate::store::history::HistoryIo>) -> Result<()> {
    if let Some(io) = work.as_deref_mut() {
        io.check_cancellation()?;
        io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
    }
    Ok(())
}

fn created_access_overflow() -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    { Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason: "created access logical ownership overflows" } }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    { Error::Invalid("created access logical ownership overflows".into()) }
}

fn created_access_add(left: u64, right: u64) -> Result<u64> {
    left.checked_add(right).ok_or_else(created_access_overflow)
}

fn created_access_vector_bytes<T>(capacity: usize) -> Result<u64> {
    capacity.checked_mul(std::mem::size_of::<T>()).map(|bytes| bytes as u64)
        .ok_or_else(created_access_overflow)
}

fn created_access_release(
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>, bytes: u64,
) -> Result<()> {
    if let Some(io) = work.as_deref_mut() { io.release_owned(bytes)?; }
    Ok(())
}

fn created_access_reserve<T>(
    values: &mut Vec<T>, capacity: usize,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    if capacity <= values.capacity() { return Ok(()); }
    let old = values.capacity();
    let bytes = created_access_vector_bytes::<T>(capacity - old)?;
    if let Some(io) = work.as_deref_mut() { io.admit_allocation(bytes)?; }
    values.try_reserve_exact(capacity - values.len()).map_err(|cause| match work.as_deref() {
        Some(io) => io.allocation_failure(cause),
        None => Error::Storage(cause.to_string()),
    })?;
    if let Some(io) = work.as_deref_mut() {
        io.retain_owned(created_access_vector_bytes::<T>(values.capacity() - old)?)?;
    }
    Ok(())
}

fn created_access_label_bytes(label: &String) -> Result<u64> {
    created_access_add(std::mem::size_of::<String>() as u64, label.capacity() as u64)
}

fn created_access_insert_label(
    labels: &mut BTreeSet<String>, label: String,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    if labels.contains(&label) {
        let bytes = label.capacity() as u64;
        drop(label);
        return created_access_release(work, bytes);
    }
    if let Some(io) = work.as_deref_mut() { io.retain_owned(std::mem::size_of::<String>() as u64)?; }
    labels.insert(label);
    Ok(())
}

fn created_access_proof_bytes(proof: &ResourceCreationProofRef) -> Result<u64> {
    created_access_add(created_access_add(proof.provider.logical_owned_bytes()?,
        proof.resource.logical_owned_bytes()?)?, proof.evidence.logical_owned_bytes()?)
}

fn created_access_receipt_temporary_bytes(receipt: &CreatedResourceReceipt) -> Result<u64> {
    let mut bytes = created_access_add(receipt.origin.owner.id.capacity() as u64,
        receipt.origin.source_grants.logical_owned_bytes()?)?;
    bytes = created_access_add(bytes, receipt.origin.creation_fence.reservation_id.capacity() as u64)?;
    bytes = created_access_add(bytes, receipt.origin.creation_fence.placement.capacity() as u64)?;
    bytes = created_access_add(bytes, receipt.resource.logical_owned_bytes()?)?;
    bytes = created_access_add(bytes, receipt.evidence.logical_owned_bytes()?)?;
    bytes = created_access_add(bytes, receipt.allocation_generation.logical_owned_bytes()?)?;
    bytes = created_access_add(bytes, created_access_vector_bytes::<CreatedResourceOperation>(receipt.operations.capacity())?)?;
    bytes = created_access_add(bytes, created_access_vector_bytes::<CreatedContentBinding>(receipt.content.capacity())?)?;
    bytes = created_access_add(bytes, created_access_vector_bytes::<CreatedReferenceBinding>(receipt.references.capacity())?)?;
    for operation in &receipt.operations {
        let owned = match operation {
            CreatedResourceOperation::Named(name) => name.capacity() as u64,
            CreatedResourceOperation::ReadVolume(volume) | CreatedResourceOperation::WriteVolume(volume) => volume.logical_owned_bytes()?,
            CreatedResourceOperation::ReadFile(file) => file.logical_owned_bytes()?,
        };
        bytes = created_access_add(bytes, owned)?;
    }
    Ok(bytes)
}

/// Derives a created-resource capability on the caller's optional observer.
pub fn created_resource_capability(
    resource: &ResourceRef, operation: &str,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<String> {
    resource.validate_with_work(work)?;
    if operation.is_empty() || operation.len() > crate::COMPONENT_LABEL_MAX_BYTES
        || operation.chars().any(char::is_control)
    {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if work.as_deref().is_some_and(|io| io.original_native_work().is_some()) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid, reason: "created resource operation is invalid",
            });
        }
        return Err(Error::Invalid("created resource operation is invalid".into()));
    }
    created_access_canonical_capability(&(resource, operation), "resource:created:", work)
}

fn created_access_volume_capability(
    volume: &crate::conversation::VolumeRef, operation: crate::conversation::VolumeOperation,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<String> {
    let prefix = match operation {
        crate::conversation::VolumeOperation::Read => "volume:read:",
        crate::conversation::VolumeOperation::Write => "volume:write:",
    };
    created_access_canonical_capability(volume, prefix, work)
}

fn created_access_file_capability(
    file: &crate::conversation::FileRef,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<String> {
    file.validate(work)?;
    created_access_canonical_capability(file, "file:read:", work)
}

fn created_access_canonical_capability<T: Serialize + ?Sized>(
    identity: &T, prefix: &'static str,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<String> {
    let digest = blake3::Hash::from_bytes(crate::contract::canonical_json_digest_with_work(
        identity, crate::store::default_projection_read_limits().maximum_bytes, work,
    )?);
    let mut label = String::new();
    let capacity = prefix.len() + 64;
    if let Some(io) = work.as_deref_mut() {
        io.reserve_text(&mut label, capacity)?;
        io.copy_bytes(capacity as u64)?;
    } else {
        label.try_reserve_exact(capacity).map_err(|cause| Error::Storage(cause.to_string()))?;
    }
    label.push_str(prefix);
    label.push_str(digest.to_hex().as_str());
    Ok(label)
}

pub(crate) trait CreatedTaskLease: acyclic_stream::ProviderPlatform {
    fn verify(&self) -> acyclic_stream::BoxProviderFuture<'_, Result<()>>;
    fn fence(&self) -> Result<crate::scheduler::LeaseFence>;

    /// Verifies the original lease on the caller's preparation observer.
    #[cfg(feature = "filesystem")]
    fn verify_with_io<'a>(
        &'a self,
        io: &'a crate::filesystem::PreparationIo,
    ) -> Result<crate::store::history::HistoryContentFuture<'a, ()>>;

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn verify_with_native_work<'a>(
        &'a self,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>>;
}
