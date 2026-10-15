//! Operation-bound reads of trusted project snapshots.

#[cfg(feature = "filesystem")]
use crate::conversation::ContentGrant;
use crate::conversation::{VolumeClass, VolumeRef};
#[cfg(feature = "filesystem")]
use crate::core::AuthorityVerifier;
use crate::core::{Authority, Scope};
use crate::resources::GenerationRef;
use crate::runtime::TaskAdmissionRecord;
use crate::scheduler::LeaseFence;
use crate::store::{guarded_boundary, history::HistoryIo};
use crate::wire::ErrorCode;
use crate::{Error, OperationId, Result, TaskId};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// MAC domain for project-source reads.
pub const PROJECT_SOURCE_READ_PURPOSE: &str = "harness.project-source.read.v1";

/// Exact backend snapshot to authenticate.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectSourceSnapshot {
    /// Selected project volume.
    pub project: VolumeRef,
    /// Immutable generation.
    pub generation: GenerationRef,
    /// Backend snapshot evidence.
    pub native_snapshot: Option<Value>,
}

/// Trusted coordinator facts for the original fork.
pub struct RootProjectSourceContext<'a> {
    /// Coordinator owner.
    pub owner: &'a Authority,
    /// Original caller admission.
    pub caller: &'a TaskAdmissionRecord,
    /// Selected task admission.
    pub source: &'a TaskAdmissionRecord,
    /// Common authenticated graph root.
    pub graph_root: TaskId,
    /// Original fork operation.
    pub operation_id: OperationId,
    /// Finite work allowance.
    pub maximum_work: u64,
    /// Finite byte allowance.
    pub maximum_bytes: u64,
    /// Original invocation cancellation.
    #[cfg(feature = "filesystem")]
    pub cancellation: &'a acyclic_fs::CancellationToken,
    /// Authenticated creator-receipt mapping.
    pub created_project_authenticated: bool,
}

/// Trusted backend snapshot policy.
pub trait RootProjectSourceProvider: acyclic_stream::ProviderPlatform {
    /// Authenticates and approves the exact snapshot.
    fn verify<'a>(
        &'a self,
        context: RootProjectSourceContext<'a>,
        snapshot: &'a ProjectSourceSnapshot,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<()>>;

    /// Verifies on the invocation's original native ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn verify_with_native_work<'a>(
        &'a self,
        _context: RootProjectSourceContext<'a>,
        _snapshot: &'a ProjectSourceSnapshot,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        Err(Error::NativeBoundary {
            code: ErrorCode::Unsupported,
            reason: "Root project source provider has no original native-ledger verifier",
        })
    }
}

/// Signed project selection retained with the original admission.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectSourceAuthorization {
    /// Trusted service owner.
    pub owner: Authority,
    /// Original caller task.
    pub caller_task: TaskId,
    /// Complete original admission digest.
    pub caller_admission_digest: [u8; 32],
    /// Exact original caller lease.
    pub caller_fence: LeaseFence,
    /// Original fork operation.
    pub operation_id: OperationId,
    /// Selected source task.
    pub source_task: TaskId,
    /// Authenticated project volume.
    pub project: VolumeRef,
    /// Pinned immutable generation.
    pub generation: GenerationRef,
    /// Authenticated backend evidence.
    pub native_snapshot: Option<Value>,
    /// Common coordinator graph root.
    pub graph_root: TaskId,
    /// Finite work allowance.
    pub maximum_work: u64,
    /// Finite byte allowance.
    pub maximum_bytes: u64,
    /// Issuer's opaque descriptor attestation.
    pub policy_scope: Scope,
}

impl ProjectSourceAuthorization {
    #[cfg(feature = "filesystem")]
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        let snapshot = self
            .native_snapshot
            .as_ref()
            .map(crate::executor::json::json_value_logical_owned_bytes)
            .transpose()?
            .unwrap_or(0);
        [
            self.owner.id.capacity() as u64,
            self.caller_fence.reservation_id.capacity() as u64,
            self.caller_fence.placement.capacity() as u64,
            self.project.logical_owned_bytes()?,
            self.generation.as_resource().logical_owned_bytes()?,
            self.policy_scope.logical_owned_bytes()?,
            snapshot,
        ]
        .into_iter()
        .try_fold(0_u64, |total, bytes| {
            total.checked_add(bytes).ok_or_else(|| {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                {
                    Error::NativeBoundary {
                        code: ErrorCode::Invalid,
                        reason: "project source authorization ownership overflows",
                    }
                }
                #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
                {
                    Error::Invalid("project source authorization ownership overflows".into())
                }
            })
        })
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn copy(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<(Self, u64)> {
        if work.is_none() {
            return Ok((self.clone(), 0));
        }
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        quota
            .observer()?
            .codec_charge(crate::executor::json::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
        let owner = crate::core::copy_authority(&self.owner, &mut quota)?;
        let caller_fence = LeaseFence {
            reservation_id: crate::core::copy_text(&self.caller_fence.reservation_id, &mut quota)?,
            placement: crate::core::copy_text(&self.caller_fence.placement, &mut quota)?,
        };
        let project = self.project.copy(quota.work())?;
        quota.adopt(project.logical_owned_bytes()?)?;
        let reference = crate::core::copy_resource(self.generation.as_resource(), &mut quota)?;
        let generation = GenerationRef::from_resource(reference, quota.work())?;
        let native_snapshot = self
            .native_snapshot
            .as_ref()
            .map(|snapshot| {
                crate::core::copy(snapshot, &mut quota, |value: &Value, _| {
                    crate::executor::json::json_value_logical_owned_bytes(value)
                })
            })
            .transpose()?;
        let policy_scope = self.policy_scope.copy(quota.work())?;
        quota.adopt(policy_scope.logical_owned_bytes()?)?;
        let value = Self {
            owner,
            caller_task: self.caller_task,
            caller_admission_digest: self.caller_admission_digest,
            caller_fence,
            operation_id: self.operation_id,
            source_task: self.source_task,
            project,
            generation,
            native_snapshot,
            graph_root: self.graph_root,
            maximum_work: self.maximum_work,
            maximum_bytes: self.maximum_bytes,
            policy_scope,
        };
        let retained = quota.retained();
        quota.transfer();
        Ok((value, retained))
    }

    pub(crate) fn validate(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<()> {
        self.project.validate(work)?;
        self.generation.validate(work)?;
        if self.project.class() != VolumeClass::Project
            || self.project.provider() != self.generation.as_resource().provider()
            || self.maximum_work == 0
            || self.maximum_work == u64::MAX
            || self.maximum_bytes == 0
            || self.maximum_bytes == u64::MAX
        {
            return Err(guarded_boundary(
                work,
                ErrorCode::Invalid,
                "project source lacks an exact project or finite bounds",
            ));
        }
        crate::executor::json::json_byte_length(self, self.maximum_bytes, work).map(|_| ())
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn scope_label(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<String> {
        const LABEL_BYTES: usize = "project-source:".len() + 64;
        let digest = crate::executor::json::canonical_json_digest(
            &(
                PROJECT_SOURCE_READ_PURPOSE,
                &self.owner,
                self.caller_task,
                self.caller_admission_digest,
                &self.caller_fence,
                self.operation_id,
                self.source_task,
                &self.project,
                &self.generation,
                &self.native_snapshot,
                self.graph_root,
                self.maximum_work,
                self.maximum_bytes,
            ),
            self.maximum_bytes,
            work,
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

    #[cfg(feature = "filesystem")]
    pub(crate) fn verify_scope(
        &self,
        verifier: &AuthorityVerifier,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        self.validate(work)?;
        let label = self.scope_label(work)?;
        let mut capabilities = self.policy_scope.capabilities().iter();
        let result = if verifier.audience() == &self.owner
            && self.policy_scope.id() == label
            && self.policy_scope.agent().is_none()
            && capabilities.next() == Some(label.as_str())
            && capabilities.next().is_none()
        {
            verifier.verify(&self.policy_scope, work)
        } else {
            Err(guarded_boundary(
                work,
                ErrorCode::Unauthorized,
                "project source descriptor or opaque policy attestation differs",
            ))
        };
        let bytes = label.capacity() as u64;
        drop(label);
        let release = work
            .as_deref_mut()
            .map(|observer| observer.release_owned(bytes))
            .transpose();
        result?;
        release?;
        Ok(())
    }
}

/// Kernel-sealed read grant for one immutable selection.
#[derive(Clone, Debug)]
pub struct ProjectSourceReadHandle {
    authorization: ProjectSourceAuthorization,
    #[cfg(feature = "filesystem")]
    grant: ContentGrant,
}

impl ProjectSourceReadHandle {
    #[cfg(feature = "filesystem")]
    pub(crate) fn seal(
        authorization: ProjectSourceAuthorization,
        verifier: &AuthorityVerifier,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Self> {
        let grant = ContentGrant::from_project_source(&authorization, verifier, work)?;
        Ok(Self {
            authorization,
            grant,
        })
    }

    /// Borrows the authenticated descriptor.
    #[must_use]
    pub const fn authorization(&self) -> &ProjectSourceAuthorization {
        &self.authorization
    }

    #[cfg(feature = "filesystem")]
    pub(crate) const fn grant(&self) -> &ContentGrant {
        &self.grant
    }

    /// Requires the original operation, task and pinned snapshot.
    pub fn require_selection(
        &self,
        operation_id: OperationId,
        source_task: TaskId,
        project: &VolumeRef,
        generation: &GenerationRef,
        native_snapshot: Option<&Value>,
        work: crate::NativeWork<'_>,
    ) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            work.remaining().map_err(|cause| Error::NativeWork {
                cause,
                custody: None,
            })?;
        }
        let record = &self.authorization;
        if record.operation_id != operation_id
            || record.source_task != source_task
            || &record.project != project
            || &record.generation != generation
            || record.native_snapshot.as_ref() != native_snapshot
        {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.is_some() {
                return Err(Error::NativeBoundary {
                    code: ErrorCode::Unauthorized,
                    reason: "project source read token names another selection",
                });
            }
            return Err(Error::Unauthorized(
                "project source read token names another selection".into(),
            ));
        }
        let _ = work;
        Ok(())
    }
}
