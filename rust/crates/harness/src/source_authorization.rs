//! Operation-bound project reads issued only after trusted graph and snapshot verification.
//!
//! Serialized authorization is evidence, not a caller scope or transcript grant.

use crate::conversation::{ContentGrant, VolumeClass, VolumeRef};
use crate::core::{Authority, AuthorityVerifier, Scope};
use crate::resources::GenerationRef;
use crate::runtime::TaskAdmissionRecord;
use crate::scheduler::LeaseFence;
use crate::{Capabilities, Error, OperationId, Result, TaskId};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Fixed MAC purpose separating project-source reads from every other scope use.
pub const PROJECT_SOURCE_READ_PURPOSE: &str = "harness.project-source.read.v1";

/// Backend selection whose generation and optional native capture must be authenticated.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectSourceSnapshot {
    /// Exact project namespace, never a private or history namespace.
    pub project: VolumeRef,
    /// Immutable backend generation.
    pub generation: GenerationRef,
    /// Real backend snapshot descriptor; opaque metadata alone is not proof.
    pub native_snapshot: Option<Value>,
}

/// Actual retained graph facts supplied by the owning coordinator, not by a caller.
pub struct RootProjectSourceContext<'a> {
    /// Actual coordinator owner.
    pub owner: &'a Authority,
    /// Complete original caller admission.
    pub caller: &'a TaskAdmissionRecord,
    /// Complete actual selected task admission.
    pub source: &'a TaskAdmissionRecord,
    /// Root obtained by traversing both actual coordinator lineages.
    pub graph_root: TaskId,
    /// One original fork operation.
    pub operation_id: OperationId,
    /// Finite provider work bound.
    pub maximum_work: u64,
    /// Finite provider byte bound.
    pub maximum_bytes: u64,
    /// Actual invocation/driver cancellation, never a fresh verifier-local token.
    #[cfg(feature = "filesystem")]
    pub cancellation: &'a acyclic_fs::CancellationToken,
    /// True only for an exact authenticated creator-receipt content mapping.
    /// When false the source MUST be the actual graph root and the provider MUST
    /// authenticate its protected successful bootstrap allocation record.
    pub created_project_authenticated: bool,
}

/// Explicit trusted RootService policy and genuine backend snapshot verifier.
///
/// Success MUST prove the configured graph policy permits this exact selection,
/// authenticate its immutable generation and native capture (when present), and
/// authenticate the root bootstrap allocation if no ordinary creator receipt
/// exists. Implementations must bound their own work/IO by the supplied limits.
/// There is intentionally no permissive default implementation.
pub trait RootProjectSourceProvider: acyclic_stream::ProviderPlatform {
    /// Authenticates backend evidence and explicitly approves this project read.
    fn verify<'a>(
        &'a self,
        context: RootProjectSourceContext<'a>,
        snapshot: &'a ProjectSourceSnapshot,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<()>>;
}

/// MAC-bound, operation-specific project evidence retained in original admission input.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectSourceAuthorization {
    /// Actual trusted service owner.
    pub owner: Authority,
    /// Original caller, never the ordinary resource-preparation task.
    pub caller_task: TaskId,
    /// Complete original caller admission digest.
    pub caller_admission_digest: [u8; 32],
    /// Exact original uncancelled caller lease.
    pub caller_fence: LeaseFence,
    /// Original fork operation.
    pub operation_id: OperationId,
    /// Actual selected task.
    pub source_task: TaskId,
    /// Exact authenticated created project.
    pub project: VolumeRef,
    /// Pinned immutable generation.
    pub generation: GenerationRef,
    /// Authenticated genuine native snapshot descriptor, when needed.
    pub native_snapshot: Option<Value>,
    /// Actual shared coordinator session root.
    pub graph_root: TaskId,
    /// Finite authorization/provider work bound.
    pub maximum_work: u64,
    /// Finite authorization/provider byte bound.
    pub maximum_bytes: u64,
    /// Original issuer's opaque descriptor attestation, never a volume-read scope.
    pub policy_scope: Scope,
}

impl ProjectSourceAuthorization {
    /// Checks structure only; callers cannot turn this into authority themselves.
    pub fn validate(&self) -> Result<()> {
        self.project.validate()?;
        self.generation.validate()?;
        if self.project.class() != VolumeClass::Project
            || self.project.provider() != self.generation.as_resource().provider()
            || self.maximum_work == 0 || self.maximum_work == u64::MAX
            || self.maximum_bytes == 0 || self.maximum_bytes == u64::MAX
        {
            return Err(Error::Invalid("project source lacks an exact project or finite bounds".into()));
        }
        crate::contract::validate_json_byte_bound(self, self.maximum_bytes)
    }

    pub(crate) fn scope_label(&self) -> Result<String> {
        let descriptor = (
            PROJECT_SOURCE_READ_PURPOSE,
            &self.owner, self.caller_task, self.caller_admission_digest, &self.caller_fence,
            self.operation_id, self.source_task, &self.project, &self.generation,
            &self.native_snapshot, self.graph_root, self.maximum_work, self.maximum_bytes,
        );
        let bytes = crate::contract::canonical_json_bytes(&descriptor)?;
        Ok(format!("project-source:{}", blake3::hash(&bytes).to_hex()))
    }

    pub(crate) fn verify_scope(&self, verifier: &AuthorityVerifier) -> Result<()> {
        self.validate()?;
        let label = self.scope_label()?;
        if verifier.audience() != &self.owner
            || self.policy_scope.id() != label
            || self.policy_scope.agent().is_some()
            || self.policy_scope.capabilities() != &Capabilities::new([label])
        {
            return Err(Error::Unauthorized("project source descriptor or opaque policy attestation differs".into()));
        }
        verifier.verify(&self.policy_scope)
    }
}

/// Kernel-only witness carrying the complete authenticated original issuer,
/// audience, caller admission/fence, operation, source and pinned selection.
/// The owning host checks graph and creator evidence before the private sealed
/// path below verifies the purpose-separated RootMAC and constructs this proof.
pub(crate) struct VerifiedProjectSourceRead<'a> {
    authorization: &'a ProjectSourceAuthorization,
}

impl VerifiedProjectSourceRead<'_> {
    pub(crate) const fn project(&self) -> &VolumeRef { &self.authorization.project }
}

/// Private-constructor read token. Borrowing it never mutates the caller scope.
#[derive(Clone, Debug)]
pub struct ProjectSourceReadHandle {
    authorization: ProjectSourceAuthorization,
    grant: ContentGrant,
}

impl ProjectSourceReadHandle {
    // Only the owning kernel calls this after authenticating retained graph facts.
    pub(crate) fn seal(authorization: ProjectSourceAuthorization, verifier: &AuthorityVerifier) -> Result<Self> {
        authorization.verify_scope(verifier)?;
        let proof = VerifiedProjectSourceRead { authorization: &authorization };
        let grant = ContentGrant::from_verified_project_source(&proof);
        Ok(Self { authorization, grant })
    }

    /// Borrowed immutable authorization descriptor.
    #[must_use]
    pub const fn authorization(&self) -> &ProjectSourceAuthorization { &self.authorization }

    /// Borrowed exact project-read grant, never private/history authority.
    #[must_use]
    pub(crate) const fn grant(&self) -> &ContentGrant { &self.grant }

    /// Requires this exact operation, source task and pinned backend selection.
    pub fn require_selection(
        &self, operation_id: OperationId, source_task: TaskId, project: &VolumeRef,
        generation: &GenerationRef, native_snapshot: Option<&Value>,
    ) -> Result<()> {
        let record = &self.authorization;
        if record.operation_id != operation_id || record.source_task != source_task
            || &record.project != project || &record.generation != generation
            || record.native_snapshot.as_ref() != native_snapshot
        {
            return Err(Error::Unauthorized("project source read token names another selection".into()));
        }
        Ok(())
    }
}
