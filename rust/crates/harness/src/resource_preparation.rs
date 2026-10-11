//! Task-bound resource creation requests and authenticated preparation leases.
//!
//! These values do not grant resource access. Providers must retain the origin
//! with their existing allocation records and authenticate those records before
//! deriving child authority or content bindings.

use crate::core::Authority;
use crate::resources::{ProviderRef, ResourceRef};
use crate::runtime::TaskAdmissionRecord;
use crate::scheduler::LeaseFence;
use crate::{Capabilities, Error, OperationId, Result, TaskId};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;

/// Exact provider evidence to authenticate before deriving child resource access.
/// These references are canonical admission data, never bearer authority.
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
    pub fn validate(&self) -> Result<()> {
        self.provider.validate()?;
        self.resource.validate()?;
        self.evidence.validate()
    }
}

/// One immutable resource request admitted as an ordinary preparation task's input.
/// Resource count and provider work/bytes are explicit per-request bounds; the
/// preparation task itself consumes the coordinator's ordinary session budget.
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
    /// Optional ordinary resumable outbox command. Its immutable payload refers
    /// to provider input, not recursively to this complete preparation request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<crate::workflow::WorkflowCommand>,
    /// Immutable original caller checkpoint for source restoration. The owner
    /// requires read authority; consumers must authenticate the snapshot itself.
    /// Direct configured creators may already retain the exact source separately.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_checkpoint: Option<crate::conversation::FileRef>,
    /// Explicit immutable inputs for the intended child, not a reference scan or
    /// a grant. The creator authenticates original donor rights and pinned bytes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub input_files: Vec<crate::conversation::FileRef>,
    /// Explicit selected Project authorization; the owner verifies and seals it.
    /// This never grants another task's history or private namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_authorization: Option<crate::source_authorization::ProjectSourceAuthorization>,
}

impl ResourcePreparationRequest {
    /// Checks the complete request before it can be bound to a task lease.
    pub fn validate(&self) -> Result<()> {
        self.provider.validate()?;
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
            return Err(Error::Invalid(
                "resource preparation lacks a parent, authority or finite work bounds".into(),
            ));
        }
        crate::contract::validate_json_byte_bound(self, self.maximum_bytes)?;
        if let Some(checkpoint) = &self.source_checkpoint {
            checkpoint.validate()?;
            if checkpoint.descriptor().byte_length() > self.maximum_bytes {
                return Err(Error::Invalid("source checkpoint exceeds preparation byte bound".into()));
            }
        }
        if self.input_files.len() as u64 > self.maximum_work {
            return Err(Error::Invalid("child input reference count exceeds preparation work bound".into()));
        }
        let mut input_bytes = 0_u64;
        let mut input_identities = BTreeSet::new();
        for file in &self.input_files {
            file.validate()?;
            input_bytes = input_bytes.checked_add(file.descriptor().byte_length())
                .ok_or_else(|| Error::Invalid("child input reference byte bound overflow".into()))?;
            if input_bytes > self.maximum_bytes || !input_identities.insert(file.read_capability()?) {
                return Err(Error::Invalid("child input references exceed their bound or repeat an identity".into()));
            }
        }
        if let Some(authorization) = &self.read_authorization {
            authorization.validate()?;
        }
        if let Some(command) = &self.command {
            command.validate()?;
            if command.operation_id == self.child.operation_id {
                return Err(Error::Invalid(
                    "preparation effect and intended child identities must differ".into(),
                ));
            }
        }
        self.child.validate()?;
        let mut identities = BTreeSet::new();
        for resource in &self.resources {
            resource.validate()?;
            if !identities.insert(crate::contract::canonical_json_bytes(resource)?) {
                return Err(Error::Invalid(
                    "resource preparation repeats an identity".into(),
                ));
            }
        }
        let mut proved = BTreeSet::new();
        for proof in &self.child.resource_proofs {
            let identity = crate::contract::canonical_json_bytes(&proof.resource)?;
            if identities.contains(&identity) {
                if proof.provider != self.provider {
                    return Err(Error::Invalid(
                        "child proof belongs to another creation provider".into(),
                    ));
                }
                proved.insert(identity);
            }
        }
        if proved != identities {
            return Err(Error::Invalid(
                "child proofs differ from requested created resources".into(),
            ));
        }
        Ok(())
    }
}

/// Provenance retained inside a trusted provider's existing allocation record.
/// Deserializing this record conveys no authority. Verification also requires
/// the bound task owner and the provider's authenticated allocation evidence.
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
    /// The coordinator verifies these against the original retained admission.
    pub source_grants: Capabilities,
    /// Digest of the complete immutable resource request and requested child scope.
    pub request_digest: [u8; 32],
    /// Original creation lease, distinct from a later recovery lease.
    pub creation_fence: LeaseFence,
}

/// Sealed, process-local binding to an authenticated preparation task lease.
/// Only `TaskJournalOwner::resource_preparation` constructs this value.
#[cfg(feature = "filesystem")]
pub struct TaskResourcePreparation {
    pub(crate) owner: std::sync::Arc<dyn ResourcePreparationLease>,
    pub(crate) request: ResourcePreparationRequest,
    pub(crate) origin: ResourceCreationOrigin,
    pub(crate) project_source: Option<crate::source_authorization::ProjectSourceReadHandle>,
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

    /// Borrowed original preparation-task grants. Broader provider credentials
    /// never substitute for the task's admitted source-read authority.
    #[must_use]
    pub fn grants(&self) -> &crate::Capabilities {
        self.owner.grants()
    }

    /// Borrowed actual preparation invocation token, if its owner was bound at
    /// the caller's live invocation boundary. It grants no resource authority.
    #[must_use]
    pub fn cancellation(&self) -> Option<&acyclic_fs::CancellationToken> {
        self.owner.cancellation()
    }

    /// Requires the current uncancelled preparation lease before fresh allocation.
    /// Providers must enforce the request's work/IO bounds at their own boundary.
    pub async fn verify_creation(&self) -> Result<()> {
        self.owner.verify_lease(false).await
    }

    /// Permits observing an existing uncertain allocation under a retained lease.
    /// This does not authorize another creation or grant child access.
    pub async fn verify_reconciliation(&self) -> Result<()> {
        self.owner.verify_lease(true).await
    }
}

// Keep the authenticated lease process-local while allowing creation providers
// to remain independent of the coordinator's Stream implementation.
#[cfg(feature = "filesystem")]
pub(crate) trait ResourcePreparationLease: acyclic_stream::ProviderPlatform {
    fn verify_lease(&self, settlement: bool) -> acyclic_stream::BoxProviderFuture<'_, Result<()>>;
    fn grants(&self) -> &crate::Capabilities;
    fn cancellation(&self) -> Option<&acyclic_fs::CancellationToken>;
}

#[cfg(feature = "filesystem")]
impl<P: acyclic_stream::StreamProvider> ResourcePreparationLease
    for crate::durable_host::TaskJournalOwner<P>
{
    fn grants(&self) -> &crate::Capabilities {
        self.input_grants()
    }

    fn cancellation(&self) -> Option<&acyclic_fs::CancellationToken> {
        self.invocation_cancellation()
    }

    fn verify_lease(&self, settlement: bool) -> acyclic_stream::BoxProviderFuture<'_, Result<()>> {
        Box::pin(async move { self.verify(settlement).await })
    }
}

/// An operation declared by a trusted creation provider for one exact resource.
/// These labels are requests, not bearer authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CreatedResourceOperation {
    /// Provider-neutral operation on this resource only.
    Named(String),
    /// Actual filesystem content read authority for an exact volume.
    ReadVolume(crate::conversation::VolumeRef),
    /// Actual filesystem content publication authority for an exact volume.
    WriteVolume(crate::conversation::VolumeRef),
    /// Exact immutable inherited-file read delegated by an authenticated creator.
    /// This does not authorize the source volume or any publication operation.
    ReadFile(crate::conversation::FileRef),
}

/// Canonical request label for an operation on one created resource.
pub fn created_resource_capability(resource: &ResourceRef, operation: &str) -> Result<String> {
    resource.validate()?;
    if operation.is_empty() || operation.len() > crate::COMPONENT_LABEL_MAX_BYTES
        || operation.chars().any(char::is_control)
    {
        return Err(Error::Invalid("created resource operation is invalid".into()));
    }
    let bytes = crate::contract::canonical_json_bytes(&(resource, operation))?;
    Ok(format!("resource:created:{}", blake3::hash(&bytes).to_hex()))
}

/// Provider-authenticated content handles for an exact created volume.
/// A configured provider must reauthenticate its retained allocation evidence
/// whenever these handles read or publish. Raw proof refs cannot create handles.
#[derive(Clone)]
pub struct CreatedContentBinding {
    /// Exact volume mapped by the provider's authenticated allocation record.
    pub volume: crate::conversation::VolumeRef,
    /// Exact aggregate audience authenticated with this allocation, if the
    /// provider supplies a canonical published child binding.
    pub authority: Option<crate::core::Authority>,
    /// Existing provider reader/writer machinery, not a second content engine.
    pub binding: crate::runtime::ContentBindings,
    /// Selects the task's ordinary content destination; at most one is allowed.
    pub default_writer: bool,
    /// Explicit result slot authenticated by the allocation provider; never inferred
    /// from private volume ordering. This slot is not the ordinary scratch writer.
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

/// Opaque identity of the original child scratch allocation. It shares the
/// already retained provider handle; cloning it allocates or copies no lease.
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

    /// Authenticates original allocation custody and current-fenced WRITE through
    /// the same task owner's settlement path. No native physical IO is claimed.
    pub async fn authorize_settlement_measured<P: acyclic_stream::StreamProvider + 'static>(
        &self, owner: &crate::durable_host::TaskJournalOwner<P>,
        budget: acyclic_fs::WorkBudget, cancellation: &acyclic_fs::CancellationToken,
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error> {
        owner.authorize_created_scratch_settlement_measured(self,budget,cancellation).await
    }

    /// Authorizes only cleanup of this original allocation under its authentic
    /// current owner, original WRITE custody and the caller's existing ledger.
    /// Recheck before every namespace mutation; this is not a deletion receipt.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn authorize_settlement_with_native_work<P: acyclic_stream::StreamProvider + 'static>(
        &self,
        owner: &crate::durable_host::TaskJournalOwner<P>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        owner.authorize_created_scratch_settlement_with_native_work(self, work).await
    }
}

/// One immutable inherited file and its authenticated exact-file reader.
/// This is not an allocated namespace, volume mapping or writable destination.
#[derive(Clone)]
pub struct CreatedReferenceBinding {
    /// Complete exact file identity authenticated by the original creator seal.
    pub file: crate::conversation::FileRef,
    /// Read-only provider handle restricted to that file and immutable version.
    pub reader: std::sync::Arc<dyn crate::conversation::ContentResidencyVerifier>,
}

/// Authenticated result of one explicitly trusted provider verification.
/// This public record is not authority: only the coordinator consumes it into a
/// private, operation-bound scope after authenticating the preparation admission.
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
/// Implementations authenticate protected allocation records, the immutable
/// original generation and complete request before returning access operations.
pub trait ResourceCreationProvider: acyclic_stream::ProviderPlatform {
    /// Pinned provider namespace, family and immutable contract revision.
    fn provider(&self) -> &ProviderRef;
    /// Authenticates evidence without allocating, issuing another request or
    /// substituting a recovery fence for the receipt's original creation fence.
    fn verify<'a>(
        &'a self,
        request: &'a ResourcePreparationRequest,
        proof: &'a ResourceCreationProofRef,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<CreatedResourceReceipt>>;

    /// Authenticates the same creator proof using the caller's original native
    /// ledger/token. Returned custody must cover the owned receipt until actual
    /// scope consumption transfers it; ordinary verification is not a fallback.
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
}

/// A private kernel-produced seal, never deserialized from user input.
#[derive(Clone)]
pub(crate) struct CreatedResourceAccess {
    pub(crate) operation: OperationId,
    pub(crate) admission_digest: [u8; 32],
    pub(crate) grants: crate::Capabilities,
    pub(crate) content: Vec<CreatedContentBinding>,
    pub(crate) references: Vec<CreatedReferenceBinding>,
    pub(crate) proofs: Vec<ResourceCreationProofRef>,
    pub(crate) publication_lease: Option<std::sync::Arc<dyn CreatedTaskLease>>,
}

impl CreatedResourceAccess {
    pub(crate) fn current_content(&self) -> Result<Option<&CreatedContentBinding>> {
        let Some(binding) = self.content.iter().find(|binding| binding.default_writer)
        else { return Ok(None); };
        if binding.volume.class() != crate::conversation::VolumeClass::AgentPrivate
            || !matches!(binding.volume.owner(), crate::conversation::VolumeOwner::Agent(_))
        {
            return Err(Error::Unauthorized("current created scratch is not agent-owned private content".into()));
        }
        Ok(Some(binding))
    }

    pub(crate) fn consume(
        admission: &TaskAdmissionRecord,
        receipts: Vec<CreatedResourceReceipt>,
        current_default_origin: Option<OperationId>,
    ) -> Result<Self> {
        if receipts.len() != admission.resource_proofs.len() || receipts.is_empty() {
            return Err(Error::Unauthorized("created resource receipts are incomplete".into()));
        }
        let mut labels = BTreeSet::new();
        let mut references = Vec::new();
        let mut reference_keys = BTreeSet::new();
        let mut content = Vec::new();
        let mut volumes = BTreeSet::new();
        let mut default_writer = false;
        let mut artifact_writer = false;
        for (proof, receipt) in admission.resource_proofs.iter().zip(receipts) {
            if receipt.resource != proof.resource || receipt.evidence != proof.evidence
                || receipt.origin.preparation_task != proof.preparation
                || receipt.allocation_generation.as_resource().provider() != &proof.provider
            {
                return Err(Error::Unauthorized("created resource receipt changed identity".into()));
            }
            let mut reference_labels = BTreeSet::new();
            for operation in &receipt.operations {
                let label = match operation {
                    CreatedResourceOperation::Named(name) => {
                        created_resource_capability(&receipt.resource, name)?
                    }
                    CreatedResourceOperation::ReadFile(file) => {
                        file.validate()?;
                        let label = file.read_capability()?;
                        if !reference_labels.insert(label.clone()) {
                            return Err(Error::Unauthorized("created exact-file operation is ambiguous".into()));
                        }
                        label
                    }
                    CreatedResourceOperation::ReadVolume(volume) => {
                        if volume.provider() != &proof.provider {
                            return Err(Error::Unauthorized("created volume provider differs".into()));
                        }
                        volume.capability(crate::conversation::VolumeOperation::Read)?
                    }
                    CreatedResourceOperation::WriteVolume(volume) => {
                        if volume.provider() != &proof.provider {
                            return Err(Error::Unauthorized("created volume provider differs".into()));
                        }
                        volume.capability(crate::conversation::VolumeOperation::Write)?
                    }
                };
                labels.insert(label);
            }
            for binding in receipt.references {
                binding.file.validate()?;
                let label = binding.file.read_capability()?;
                if !reference_labels.remove(&label) || !reference_keys.insert(label) {
                    return Err(Error::Unauthorized("created exact-file reader differs from this proof".into()));
                }
                references.push(binding);
            }
            if !reference_labels.is_empty() {
                return Err(Error::Unauthorized("created exact-file operation has no authenticated reader".into()));
            }
            for mut binding in receipt.content {
                if (binding.default_writer || binding.artifact_writer) && binding.binding.writer.is_none() {
                    return Err(Error::Unauthorized("created default content has no writer".into()));
                }
                binding.default_writer &= current_default_origin == Some(receipt.origin.intended_child);
                binding.artifact_writer &= current_default_origin == Some(receipt.origin.intended_child);
                #[cfg(feature = "filesystem")]
                if let Some(proof) = &binding.scratch_creation {
                    if proof.volume() != &binding.volume || proof.origin() != &receipt.origin
                        || binding.artifact_writer
                    {
                        return Err(Error::Unauthorized("scratch proof differs from created allocation".into()));
                    }
                }
                let read = binding.volume.capability(crate::conversation::VolumeOperation::Read)?;
                let write = binding.volume.capability(crate::conversation::VolumeOperation::Write)?;
                if !labels.contains(&read)
                    || binding.volume.provider() != &proof.provider
                    || !volumes.insert(crate::contract::canonical_json_bytes(&binding.volume)?)
                    || binding.binding.writer.as_ref().is_some_and(|writer|
                        writer.volume() != &binding.volume || !labels.contains(&write))
                    || (binding.artifact_writer && (artifact_writer || binding.default_writer
                        || binding.binding.writer.is_none()
                        || binding.volume.class() != crate::conversation::VolumeClass::AgentPrivate
                        || !matches!(binding.volume.owner(), crate::conversation::VolumeOwner::Agent(_))))
                    || (binding.default_writer && (default_writer || binding.binding.writer.is_none()))
                {
                    return Err(Error::Unauthorized("created content mapping differs from receipt".into()));
                }
                default_writer |= binding.default_writer;
                artifact_writer |= binding.artifact_writer;
                content.push(binding);
            }
        }
        Ok(Self {
            operation: admission.operation_id,
            admission_digest: crate::contract::canonical_json_digest(&admission.canonical_value())?,
            grants: crate::Capabilities::new(labels),
            proofs: admission.resource_proofs.clone(),
            content,
            references,
            publication_lease: None,
        })
    }
}

pub(crate) trait CreatedTaskLease: acyclic_stream::ProviderPlatform {
    fn verify(&self) -> acyclic_stream::BoxProviderFuture<'_, Result<()>>;
    fn fence(&self) -> Result<crate::scheduler::LeaseFence>;
}
