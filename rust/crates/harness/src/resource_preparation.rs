//! Task-bound resource creation requests and authenticated preparation leases.
//!
//! These values do not grant resource access. Providers must retain the origin
//! with their existing allocation records and authenticate those records before
//! deriving child authority or content bindings.

use crate::core::Authority;
use crate::resources::{ProviderRef, ResourceRef};
use crate::runtime::TaskAdmissionRecord;
use crate::scheduler::LeaseFence;
use crate::{Error, OperationId, Result, TaskId};
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
            if proof.provider != self.provider {
                return Err(Error::Invalid(
                    "child proof belongs to another creation provider".into(),
                ));
            }
            proved.insert(crate::contract::canonical_json_bytes(&proof.resource)?);
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
}

#[cfg(feature = "filesystem")]
impl<P: acyclic_stream::StreamProvider> ResourcePreparationLease
    for crate::durable_host::TaskJournalOwner<P>
{
    fn verify_lease(&self, settlement: bool) -> acyclic_stream::BoxProviderFuture<'_, Result<()>> {
        Box::pin(async move { self.verify(settlement).await })
    }
}
