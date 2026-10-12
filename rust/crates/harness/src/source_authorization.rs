//! Operation-bound project reads issued only after trusted graph and snapshot verification.

use crate::conversation::{ContentGrant, VolumeClass, VolumeRef};
use crate::core::{Authority, AuthorityVerifier, Scope};
use crate::resources::GenerationRef;
use crate::runtime::TaskAdmissionRecord;
use crate::scheduler::LeaseFence;
use crate::{Error, OperationId, Result, TaskId};
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
    pub created_project_authenticated: bool,
}

/// Explicit trusted RootService policy and genuine backend snapshot verifier.
pub trait RootProjectSourceProvider: acyclic_stream::ProviderPlatform {
    /// Authenticates backend evidence and explicitly approves this project read.
    fn verify<'a>(
        &'a self,
        context: RootProjectSourceContext<'a>,
        snapshot: &'a ProjectSourceSnapshot,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<()>>;

    /// Advertises genuine original-ledger project verification, never ordinary verification hidden inside a funded frame.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_source(&self) -> bool { false }

    /// Authenticates that same exact selection on the supplied original native ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn verify_with_native_work<'a>(
        &'a self,
        _context: RootProjectSourceContext<'a>,
        _snapshot: &'a ProjectSourceSnapshot,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "Root project source provider has no original native-ledger verifier",
        })
    }
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
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        let overflow = || {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            { Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "project source authorization ownership overflows" } }
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            { Error::Invalid("project source authorization ownership overflows".into()) }
        };
        let snapshot = self.native_snapshot.as_ref().map(crate::core::value_owned_bytes).transpose()?.unwrap_or(0);
        [
            self.owner.id.capacity() as u64,
            self.caller_fence.reservation_id.capacity() as u64,
            self.caller_fence.placement.capacity() as u64,
            self.project.logical_owned_bytes()?,
            self.generation.as_resource().logical_owned_bytes()?,
            self.policy_scope.logical_owned_bytes()?, snapshot,
        ].into_iter().try_fold(0_u64, |total, bytes| total.checked_add(bytes).ok_or_else(overflow))
    }

    pub(crate) fn clone_with_work_and_custody(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(Self, u64)> {
        if work.is_none() { return Ok((self.clone(), 0)); }
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        quota.work().as_deref_mut().expect("source authorization copy observer")
            .codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
        let owner = crate::core::copy_authority(&self.owner, &mut quota)?;
        let caller_fence = LeaseFence {
            reservation_id: crate::core::copy_text(&self.caller_fence.reservation_id, &mut quota)?,
            placement: crate::core::copy_text(&self.caller_fence.placement, &mut quota)?,
        };
        let project = self.project.copy(quota.work())?;
        quota.adopt(project.logical_owned_bytes()?)?;
        let reference = self.generation.as_resource();
        let provider = crate::resources::ProviderRef::new(
            crate::core::copy_text(reference.provider().namespace(), &mut quota)?,
            crate::core::copy_text(reference.provider().family(), &mut quota)?,
            crate::core::copy_text(reference.provider().version(), &mut quota)?,
            quota.work(),
        )?;
        let key = crate::core::copy_copy_vector(reference.key(), &mut quota)?;
        let version = reference.version().map(|version| crate::core::copy_text(version, &mut quota)).transpose()?;
        let reference = crate::resources::ResourceRef::new(
            reference.kind(), provider, key, version, quota.work(),
        )?;
        let generation = GenerationRef::from_resource_with_work(reference, quota.work())?;
        let native_snapshot = match &self.native_snapshot {
            Some(snapshot) => {
                let (value, bytes) = crate::core::native_value_clone_with_custody(snapshot, quota.work())?;
                quota.adopt(bytes)?;
                Some(value)
            }
            None => None,
        };
        let policy_scope = self.policy_scope.copy(quota.work())?;
        quota.adopt(policy_scope.logical_owned_bytes()?)?;
        let value = Self {
            owner, caller_task: self.caller_task, caller_admission_digest: self.caller_admission_digest,
            caller_fence, operation_id: self.operation_id, source_task: self.source_task,
            project, generation, native_snapshot, graph_root: self.graph_root,
            maximum_work: self.maximum_work, maximum_bytes: self.maximum_bytes, policy_scope,
        };
        let retained = quota.retained();
        quota.transfer();
        Ok((value, retained))
    }
}
impl ProjectSourceAuthorization {
    /// Checks structure only; callers cannot turn this into authority themselves.
    pub(crate) fn validate(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.project.validate(work)?;
        self.generation.validate(work)?;
        if self.project.class() != VolumeClass::Project
            || self.project.provider() != self.generation.as_resource().provider()
            || self.maximum_work == 0 || self.maximum_work == u64::MAX
            || self.maximum_bytes == 0 || self.maximum_bytes == u64::MAX
        {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.as_ref().is_some_and(|observer| observer.original_native_work().is_some()) {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "project source lacks an exact project or finite bounds",
                });
            }
            return Err(Error::Invalid("project source lacks an exact project or finite bounds".into()));
        }
        crate::contract::validate_trusted_json_byte_bound(self, self.maximum_bytes, work)
    }

    pub(crate) fn scope_label(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<String> {
        const LABEL_BYTES: usize = "project-source:".len() + 64;
        let descriptor = (
            PROJECT_SOURCE_READ_PURPOSE,
            &self.owner, self.caller_task, self.caller_admission_digest, &self.caller_fence,
            self.operation_id, self.source_task, &self.project, &self.generation,
            &self.native_snapshot, self.graph_root, self.maximum_work, self.maximum_bytes,
        );
        let digest = crate::contract::canonical_json_digest_with_work(
            &descriptor, self.maximum_bytes, work,
        )?;
        let mut label = String::new();
        if let Some(observer) = work.as_deref_mut() {
            observer.reserve_text(&mut label, LABEL_BYTES)?;
            observer.admit_encoding(LABEL_BYTES as u64)?;
        } else {
            label.reserve_exact(LABEL_BYTES);
        }
        label.push_str("project-source:");
        label.push_str(blake3::Hash::from_bytes(digest).to_hex().as_str());
        if let Some(observer) = work.as_deref_mut() {
            observer.encoded_bytes(LABEL_BYTES as u64)?;
        }
        Ok(label)
    }

    pub(crate) fn verify_scope_with_work(
        &self, verifier: &AuthorityVerifier,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.validate(work)?;
        let label = self.scope_label(work)?;
        let mut capabilities = self.policy_scope.capabilities().iter();
        let matches = verifier.audience() == &self.owner
            && self.policy_scope.id() == label
            && self.policy_scope.agent().is_none()
            && capabilities.next() == Some(label.as_str())
            && capabilities.next().is_none();
        let result = if matches {
            verifier.verify(&self.policy_scope, work)
        } else {
            let denied = || {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                if work.as_ref().is_some_and(|observer| observer.original_native_work().is_some()) {
                    return Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                        reason: "project source descriptor or opaque policy attestation differs" };
                }
                Error::Unauthorized("project source descriptor or opaque policy attestation differs".into())
            };
            Err(denied())
        };
        let bytes = label.capacity() as u64;
        drop(label);
        if let Some(observer) = work.as_deref_mut() {
            let released = observer.release_owned(bytes);
            result?;
            released?;
            return Ok(());
        }
        result
    }
}

/// Private-constructor read token. Borrowing it never mutates the caller scope.
#[derive(Clone, Debug)]
pub struct ProjectSourceReadHandle {
    authorization: ProjectSourceAuthorization,
    grant: ContentGrant,
}

impl ProjectSourceReadHandle {
    // Only the owning kernel calls this after authenticating retained graph facts.
    pub(crate) fn seal(
        authorization: ProjectSourceAuthorization, verifier: &AuthorityVerifier,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        let grant = ContentGrant::from_project_source(&authorization, verifier, work)?;
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
        work: crate::NativeWork<'_>,
    ) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            work.remaining().map_err(|cause| Error::NativeWork { cause, custody: None })?;
        }
        let record = &self.authorization;
        if record.operation_id != operation_id || record.source_task != source_task
            || &record.project != project || &record.generation != generation
            || record.native_snapshot.as_ref() != native_snapshot
        {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.is_some() {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "project source read token names another selection",
                });
            }
            return Err(Error::Unauthorized("project source read token names another selection".into()));
        }
        let _ = work;
        Ok(())
    }
}
