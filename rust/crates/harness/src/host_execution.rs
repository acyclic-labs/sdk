//! Approved host execution provider.
//!
//! This module is deliberately a host adapter.  Harness owns admission and
//! effect recovery; this adapter only validates an immutable command approval
//! and runs one already-dispatched attempt.  It does not provide a sandbox or
//! claim that a workspace route confines a process.

use crate::{
    Capabilities, EffectAttemptId, EffectId, Error, InteractionId, OperationId, Result, SessionId,
    conversation::{ContentPublisher, ContentResidencyVerifier, FileRef, VolumeRef},
    core::{AuthorityIssuer, EffectGuarantee, EffectStatus, Scope},
    effects::{EffectDispatch, EffectObservation, EffectProvider},
};
#[cfg(all(feature = "native-process-tree", not(target_arch = "wasm32")))]
use acyclic_native_runtime::{ProcessTree, spawn_process_tree};
use futures::FutureExt as _;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    io::Read,
    path::Path,
    process::{Child, ChildStderr, ChildStdout, Command, ExitStatus, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError},
    },
    thread,
    time::{Duration, Instant},
};

fn execution_request_locator_digest(volume: &VolumeRef, path: &str) -> Result<[u8; 32]> {
    let bytes = crate::contract::canonical_json_bytes(&(volume, path))?;
    Ok(*blake3::hash(&bytes).as_bytes())
}

const REQUEST_DOMAIN: &[u8] = b"acyclic:harness:approved-execution:v1";
const MAX_ARGUMENTS: usize = 1024;
const MAX_ARGUMENT_BYTES: usize = 64 * 1024;
const MAX_ENVIRONMENT_ENTRIES: usize = 256;
const MAX_OUTPUT_BYTES: usize = 4 * 1024 * 1024;
const MAX_FAILURE_BYTES: usize = 4096;
const READER_GRACE: Duration = Duration::from_millis(250);
const MAX_TIMEOUT_MS: u64 = i64::MAX as u64;

/// Environment values admitted for a process.
///
/// Both variants call `env_clear` before adding values.  There is intentionally
/// no inherited-environment variant: a host credential must never arrive by
/// default environment inheritance.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExecutionEnvironment {
    /// Run with no environment variables.
    #[default]
    Clear,
    /// Run with exactly these key/value pairs after clearing the host env.
    Explicit {
        /// Exact key/value pairs passed to the process.
        variables: BTreeMap<String, String>,
    },
}

impl ExecutionEnvironment {
    /// Creates a validated explicit environment.
    pub fn explicit(variables: BTreeMap<String, String>) -> Result<Self> {
        let value = Self::Explicit { variables };
        value.validate()?;
        Ok(value)
    }

    /// Validates keys, values, and bounded size.
    pub fn validate(&self) -> Result<()> {
        let Self::Explicit { variables } = self else {
            return Ok(());
        };
        if variables.len() > MAX_ENVIRONMENT_ENTRIES {
            return Err(Error::Invalid(
                "execution environment has too many entries".into(),
            ));
        }
        for (key, value) in variables {
            if key.is_empty() || key.contains('=') || key.contains('\0') || value.contains('\0') {
                return Err(Error::Invalid(
                    "execution environment contains an invalid key or value".into(),
                ));
            }
            if key.len() + value.len() > MAX_ARGUMENT_BYTES {
                return Err(Error::Invalid(
                    "execution environment entry is too large".into(),
                ));
            }
        }
        Ok(())
    }

    fn apply(&self, command: &mut Command) {
        command.env_clear();
        if let Self::Explicit { variables } = self {
            command.envs(variables);
        }
    }
}

/// Exact process request.  The executable and working directory are absolute
/// paths so PATH and the caller's current directory cannot reinterpret an
/// approval later.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionSpec {
    /// Absolute executable path.
    pub executable: String,
    /// Ordered argv entries, excluding argv[0].
    pub arguments: Vec<String>,
    /// Absolute working directory.
    pub working_directory: String,
    /// Exact environment policy.
    pub environment: ExecutionEnvironment,
    /// Maximum wall-clock execution time. `None` means no provider timeout.
    pub timeout_ms: Option<u64>,
    /// Maximum combined stdout and stderr bytes retained in the result.
    pub max_output_bytes: u32,
}

impl ExecutionSpec {
    /// Validates an exact command before it can be approved.
    pub fn validate(&self) -> Result<()> {
        if self.executable.is_empty() || self.executable.contains('\0') {
            return Err(Error::Invalid(
                "execution executable is empty or contains NUL".into(),
            ));
        }
        if !Path::new(&self.executable).is_absolute() {
            return Err(Error::Invalid(
                "execution executable must be absolute".into(),
            ));
        }
        if self.arguments.len() > MAX_ARGUMENTS
            || self.arguments.iter().map(String::len).sum::<usize>() > MAX_ARGUMENT_BYTES
            || self
                .arguments
                .iter()
                .any(|argument| argument.contains('\0'))
        {
            return Err(Error::Invalid(
                "execution arguments are invalid or too large".into(),
            ));
        }
        if self.working_directory.is_empty() || self.working_directory.contains('\0') {
            return Err(Error::Invalid(
                "execution working directory is invalid".into(),
            ));
        }
        if !Path::new(&self.working_directory).is_absolute() {
            return Err(Error::Invalid(
                "execution working directory must be absolute".into(),
            ));
        }
        if let Some(timeout_ms) = self.timeout_ms {
            if timeout_ms == 0 {
                return Err(Error::Invalid("execution timeout must be positive".into()));
            }
            // Keep Instant arithmetic fallible below.  A serialized u64 must
            // never be able to panic the host adapter after admission.
            if timeout_ms > MAX_TIMEOUT_MS {
                return Err(Error::Invalid("execution timeout is too large".into()));
            }
        }
        if self.max_output_bytes == 0 || self.max_output_bytes as usize > MAX_OUTPUT_BYTES {
            return Err(Error::Invalid("execution output limit is invalid".into()));
        }
        self.environment.validate()
    }

    /// Stable approval digest over every model-independent execution field.
    pub fn digest(&self) -> Result<[u8; 32]> {
        self.validate()?;
        let bytes = serde_json::to_vec(self).map_err(|error| Error::Invalid(error.to_string()))?;
        Ok(*blake3::keyed_hash(blake3::hash(REQUEST_DOMAIN).as_bytes(), &bytes).as_bytes())
    }
}

/// A host approval bound to one operation and one exact command.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionApproval {
    /// Session whose authenticated owner made this decision.
    pub session_id: SessionId,
    /// Durable approval interaction resolved by the owner.
    pub interaction_id: InteractionId,
    /// Operation identity allocated before admission.
    pub operation_id: OperationId,
    /// Exact approved command.
    pub request: ExecutionSpec,
    /// Digest of `request` at approval time.
    pub request_digest: [u8; 32],
    /// Digest of the exact host volume and logical path approved for dispatch.
    /// The locator is bound before staging so the approval record is not
    /// self-referential. The authenticated interaction separately binds the
    /// dispatch digest, which includes the immutable staged file reference.
    #[serde(default)]
    pub request_locator_digest: Option<[u8; 32]>,
    /// Whether the owner approved dispatch.
    pub approved: bool,
    /// Bounded owner reason for a denial, if any.
    pub denial_reason: Option<String>,
}

impl ExecutionApproval {
    /// Creates an approval for one exact command.
    pub fn approve(operation_id: OperationId, request: ExecutionSpec) -> Result<Self> {
        Self::approve_for(
            SessionId::new(),
            InteractionId::new(),
            operation_id,
            request,
        )
    }

    /// Creates an approval explicitly bound to an authenticated session and
    /// durable owner interaction. Production callers should use this form so
    /// the verifier can compare these identities with its journal record.
    pub fn approve_for(
        session_id: SessionId,
        interaction_id: InteractionId,
        operation_id: OperationId,
        request: ExecutionSpec,
    ) -> Result<Self> {
        let request_digest = request.digest()?;
        Ok(Self {
            session_id,
            interaction_id,
            operation_id,
            request,
            request_digest,
            request_locator_digest: None,
            approved: true,
            denial_reason: None,
        })
    }

    /// Creates a durable denial without exposing a host command to execution.
    pub fn deny(
        operation_id: OperationId,
        request: ExecutionSpec,
        reason: impl Into<String>,
    ) -> Result<Self> {
        Self::deny_for(
            SessionId::new(),
            InteractionId::new(),
            operation_id,
            request,
            reason,
        )
    }

    /// Creates a durable denial bound to an authenticated session and owner
    /// interaction.
    pub fn deny_for(
        session_id: SessionId,
        interaction_id: InteractionId,
        operation_id: OperationId,
        request: ExecutionSpec,
        reason: impl Into<String>,
    ) -> Result<Self> {
        let request_digest = request.digest()?;
        let denial_reason = reason.into();
        if denial_reason.is_empty() || denial_reason.len() > MAX_FAILURE_BYTES {
            return Err(Error::Invalid("execution denial reason is invalid".into()));
        }
        Ok(Self {
            session_id,
            interaction_id,
            operation_id,
            request,
            request_digest,
            request_locator_digest: None,
            approved: false,
            denial_reason: Some(denial_reason),
        })
    }

    /// Checks that persisted approval bytes still name the exact request.
    pub fn validate(&self) -> Result<()> {
        if self.session_id.into_bytes() == [0; 16]
            || self.interaction_id.into_bytes() == [0; 16]
            || self.operation_id.into_bytes() == [0; 16]
        {
            return Err(Error::Invalid(
                "execution approval must identify its session, interaction, and operation".into(),
            ));
        }
        let digest = self.request.digest()?;
        if digest != self.request_digest {
            return Err(Error::Conflict(
                "execution approval request digest mismatch".into(),
            ));
        }
        if self.approved && self.denial_reason.is_some() {
            return Err(Error::Invalid(
                "approved execution carries a denial reason".into(),
            ));
        }
        if !self.approved {
            let reason = self.denial_reason.as_deref().unwrap_or_default();
            if reason.is_empty() || reason.len() > MAX_FAILURE_BYTES {
                return Err(Error::Invalid(
                    "denied execution has no bounded reason".into(),
                ));
            }
        }
        if self
            .request_locator_digest
            .is_some_and(|digest| digest == [0; 32])
        {
            return Err(Error::Invalid(
                "execution approval request location digest cannot be empty".into(),
            ));
        }
        Ok(())
    }

    /// Binds this approval to the exact host volume and path later staged.
    pub fn bind_request_location(&mut self, volume: &VolumeRef, path: &str) -> Result<()> {
        volume.validate()?;
        if path.is_empty() || path.contains('\0') {
            return Err(Error::Invalid(
                "execution approval request path is invalid".into(),
            ));
        }
        self.request_locator_digest = Some(execution_request_locator_digest(volume, path)?);
        Ok(())
    }
}

/// Typed receipt persisted as the effect artifact.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExecutionReceipt {
    /// Process exited successfully.
    Succeeded {
        /// Process exit code, guaranteed to be zero.
        status_code: i32,
        /// Captured standard output.
        stdout: Vec<u8>,
        /// Captured standard error.
        stderr: Vec<u8>,
    },
    /// Process exited nonzero.
    Failed {
        /// Native exit code, if the process supplied one.
        status_code: Option<i32>,
        /// Captured standard output.
        stdout: Vec<u8>,
        /// Captured standard error.
        stderr: Vec<u8>,
    },
    /// Host timeout killed the process.
    TimedOut {
        /// Output captured before termination.
        stdout: Vec<u8>,
        /// Error output captured before termination.
        stderr: Vec<u8>,
    },
    /// Host cancellation killed the process.
    Cancelled {
        /// Output captured before termination.
        stdout: Vec<u8>,
        /// Error output captured before termination.
        stderr: Vec<u8>,
    },
    /// Approval or host policy rejected dispatch.
    Denied {
        /// Bounded owner or policy reason.
        reason: String,
    },
    /// The host cannot determine whether the process completed.
    ///
    /// This receipt is durable so a retry after a crash observes uncertainty
    /// again instead of dispatching the command a second time.
    Unknown {
        /// Bounded diagnostic retained for recovery.
        reason: String,
    },
}

impl ExecutionReceipt {
    /// Validates the bounded receipt persisted by a host adapter before it is
    /// exposed to Harness recovery.
    pub fn validate(&self) -> Result<()> {
        let validate_output = |stdout: &[u8], stderr: &[u8]| {
            if stdout.len().saturating_add(stderr.len()) > MAX_OUTPUT_BYTES {
                return Err(Error::Invalid(
                    "execution receipt output exceeds its bound".into(),
                ));
            }
            Ok(())
        };
        match self {
            Self::Succeeded {
                status_code,
                stdout,
                stderr,
            } if *status_code == 0 => validate_output(stdout, stderr),
            Self::Succeeded { .. } => Err(Error::Invalid(
                "successful execution receipt has a nonzero status".into(),
            )),
            Self::Failed {
                status_code: Some(0),
                ..
            } => Err(Error::Invalid(
                "failed execution receipt has a successful status".into(),
            )),
            Self::Failed { stdout, stderr, .. }
            | Self::TimedOut { stdout, stderr }
            | Self::Cancelled { stdout, stderr } => validate_output(stdout, stderr),
            Self::Denied { reason } | Self::Unknown { reason }
                if reason.is_empty() || reason.len() > MAX_FAILURE_BYTES =>
            {
                Err(Error::Invalid("execution receipt reason is invalid".into()))
            }
            Self::Denied { .. } | Self::Unknown { .. } => Ok(()),
        }
    }
}

/// Immutable identity of one host receipt slot. Implementations must resolve
/// this key through host-owned storage; a model-visible or workspace-relative
/// path is not an authority for a receipt.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionReceiptKey {
    /// Operation identity allocated before admission.
    pub operation_id: OperationId,
    /// Effect identity under which the operation was dispatched.
    pub effect_id: EffectId,
    /// Unique dispatch attempt identity.
    pub attempt_id: EffectAttemptId,
    /// Provider identity.
    pub provider: String,
    /// Provider operation kind.
    pub effect_kind: String,
    /// Pinned delivery guarantee.
    pub guarantee: EffectGuarantee,
    /// Digest of the immutable request content.
    pub request_digest: [u8; 32],
}

impl ExecutionReceiptKey {
    /// Validates the canonical identity and provider tuple of one receipt slot.
    ///
    /// The operation/effect relationship is checked when the key is matched
    /// to a dispatch.  Keeping this check local to the key also lets durable
    /// stores reject malformed journal records before exposing them to a
    /// provider restart path.
    pub fn validate(&self) -> Result<()> {
        if self.operation_id.into_bytes() == [0; 16]
            || self.effect_id.into_bytes() == [0; 16]
            || self.attempt_id.into_bytes() == [0; 16]
            || self.request_digest == [0; 32]
        {
            return Err(Error::Invalid(
                "execution receipt key contains an empty identity or request digest".into(),
            ));
        }
        if self.provider != "harness.native-execution.v1"
            || self.effect_kind != "host.process"
            || self.guarantee != EffectGuarantee::AtMostOnce
        {
            return Err(Error::Unauthorized(
                "execution receipt key is not for the authenticated native provider".into(),
            ));
        }
        Ok(())
    }
}

/// Durable receipt plus its host-owned result artifact.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionReceiptRecord {
    /// Identity of the dispatch that produced this receipt.
    pub key: ExecutionReceiptKey,
    /// Verified JSON result artifact exposed to Harness.
    pub result: FileRef,
    /// Typed host outcome.
    pub receipt: ExecutionReceipt,
    /// Authenticated principal that resolved an uncertain outcome, when any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operator_principal: Option<String>,
    /// Whether the terminal unknown outcome was authorized by an external
    /// operator capability rather than an internal test-only owner path.
    #[serde(default)]
    pub operator_authenticated: bool,
}

/// Opaque owner handle returned for the one caller that acquired a claim.
///
/// The handle binds the full receipt key, a journal generation, and a secret
/// owner token. It is deliberately not serializable or model-visible.
#[derive(Clone, Eq, PartialEq)]
pub struct ExecutionClaimHandle {
    key: ExecutionReceiptKey,
    token: [u8; 32],
    generation: u64,
    operator: bool,
    operator_principal: Option<String>,
    operator_authenticated: bool,
}

/// Authenticated host capability required to resolve an uncertain native
/// execution. This is separate from the workspace write grant so a model or
/// ordinary editor cannot mint an operator resolution handle.
#[derive(Clone, Eq, PartialEq)]
pub struct ExecutionResolutionCapability {
    session_id: SessionId,
    volume: crate::conversation::VolumeRef,
    token: [u8; 32],
    principal: String,
    operation_id: Option<OperationId>,
    operator_authenticated: bool,
}

/// Host-only signer for one explicitly approved execution resolution.
///
/// The configured issuer stays inside the host composition. Callers receive a
/// narrow, operation-bound scope only after their host-side approval path has
/// selected the exact session, volume, and operation. Model tools never see
/// this signer or its key material.
#[derive(Clone)]
pub struct ExecutionOperatorAuthorizer {
    issuer: AuthorityIssuer,
}

impl ExecutionOperatorAuthorizer {
    pub(crate) fn new(issuer: AuthorityIssuer) -> Self {
        Self { issuer }
    }

    /// Signs the smallest operator scope for one exact execution attempt.
    pub fn issue_scope(
        &self,
        principal: impl Into<String>,
        session_id: SessionId,
        volume: &VolumeRef,
        operation_id: OperationId,
    ) -> Result<Scope> {
        let capability =
            ExecutionResolutionCapability::capability_for(session_id, volume, operation_id)?;
        Ok(self.issuer.root(
            principal,
            Capabilities::new(vec!["execution:resolve".to_owned(), capability]),
        ))
    }

    /// Authenticates a host-issued narrow grant into a resolution handle.
    pub fn authenticate(
        &self,
        principal: impl Into<String>,
        session_id: SessionId,
        volume: &VolumeRef,
        operation_id: OperationId,
    ) -> Result<ExecutionResolutionCapability> {
        let scope = self.issue_scope(principal, session_id, volume, operation_id)?;
        ExecutionResolutionCapability::authenticate(
            &self.issuer.verifier(),
            &scope,
            session_id,
            volume,
            operation_id,
        )
    }
}

impl std::fmt::Debug for ExecutionResolutionCapability {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExecutionResolutionCapability")
            .field("session_id", &self.session_id)
            .field("volume", &self.volume)
            .field("principal", &self.principal)
            .field("operation_id", &self.operation_id)
            .field("token", &"[redacted]")
            .finish()
    }
}

impl ExecutionResolutionCapability {
    #[cfg(test)]
    pub(crate) fn for_test_owner(
        session_id: SessionId,
        volume: &VolumeRef,
        owner_scope: &crate::core::Scope,
    ) -> Result<Self> {
        Ok(Self {
            session_id,
            volume: volume.clone(),
            token: Self::owner_token(session_id, volume, owner_scope)?,
            principal: "owner".into(),
            operation_id: None,
            operator_authenticated: false,
        })
    }

    /// Returns the canonical grant name for one exact session, volume, and
    /// operation tuple. The volume digest covers provider, class, and owner;
    /// the short provider-owned volume label is not sufficient for a grant.
    pub fn capability_for(
        session_id: SessionId,
        volume: &VolumeRef,
        operation_id: OperationId,
    ) -> Result<String> {
        volume.validate()?;
        if session_id.into_bytes() == [0; 16] || operation_id.into_bytes() == [0; 16] {
            return Err(Error::Invalid(
                "execution resolution identity cannot be zero".into(),
            ));
        }
        let volume_digest =
            blake3::Hash::from_bytes(crate::contract::canonical_json_digest(volume)?).to_hex();
        Ok(format!(
            "execution:resolve:{session_id}:{volume_digest}:{operation_id}"
        ))
    }

    pub(crate) fn owner_token(
        session_id: SessionId,
        volume: &VolumeRef,
        owner_scope: &crate::core::Scope,
    ) -> Result<[u8; 32]> {
        if session_id.into_bytes() == [0; 16] {
            return Err(Error::Invalid(
                "execution resolution session identity cannot be zero".into(),
            ));
        }
        let mut input = Vec::with_capacity(64);
        input.extend_from_slice(b"acyclic:harness:execution-resolution:v1");
        input.extend_from_slice(&session_id.into_bytes());
        // Keep the v1 owner token stable for pending claims written by older
        // local sessions. Externally issued operator capabilities below use
        // the canonical full volume identity and are versioned separately.
        input.extend_from_slice(volume.id().as_bytes());
        input.extend_from_slice(owner_scope.proof());
        Ok(*blake3::hash(&input).as_bytes())
    }

    /// Authenticates an externally issued operator scope for one operation.
    /// The signed scope is retained only as a principal proof; the model
    /// cannot mint this capability from a public session or volume identity.
    pub fn authenticate(
        verifier: &crate::core::AuthorityVerifier,
        operator: &crate::core::Scope,
        session_id: SessionId,
        volume: &VolumeRef,
        operation_id: OperationId,
    ) -> Result<Self> {
        volume.validate()?;
        verifier.verify(operator)?;
        let target_capability = Self::capability_for(session_id, volume, operation_id)?;
        if session_id.into_bytes() == [0; 16]
            || operation_id.into_bytes() == [0; 16]
            || operator.id().is_empty()
            || !operator.capabilities().contains("execution:resolve")
            || !operator.capabilities().contains(&target_capability)
        {
            return Err(Error::Unauthorized(
                "operator scope lacks the exact execution session, volume, and operation capabilities".into(),
            ));
        }
        let mut input = Vec::with_capacity(96);
        input.extend_from_slice(b"acyclic:harness:execution-resolution:v2");
        input.extend_from_slice(&session_id.into_bytes());
        input.extend_from_slice(&crate::contract::canonical_json_digest(volume)?);
        input.extend_from_slice(&operation_id.into_bytes());
        input.extend_from_slice(operator.proof());
        Ok(Self {
            session_id,
            volume: volume.clone(),
            token: *blake3::hash(&input).as_bytes(),
            principal: operator.id().to_owned(),
            operation_id: Some(operation_id),
            operator_authenticated: true,
        })
    }

    pub(crate) fn matches(
        &self,
        session_id: SessionId,
        volume: &crate::conversation::VolumeRef,
        token: &[u8; 32],
        operation_id: Option<OperationId>,
    ) -> bool {
        self.session_id == session_id
            && self.volume == *volume
            && &self.token == token
            && match (self.operation_id, operation_id) {
                (Some(expected), Some(actual)) => expected == actual,
                (Some(_), None) => false,
                (None, _) => true,
            }
    }

    pub(crate) fn principal(&self) -> &str {
        &self.principal
    }

    pub(crate) fn is_operator_for(
        &self,
        session_id: SessionId,
        volume: &crate::conversation::VolumeRef,
        operation_id: OperationId,
    ) -> bool {
        self.session_id == session_id
            && self.volume == *volume
            && self.operation_id == Some(operation_id)
            && self.operator_authenticated
    }
}

impl std::fmt::Debug for ExecutionClaimHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExecutionClaimHandle")
            .field("key", &self.key)
            .field("generation", &self.generation)
            .field("operator", &self.operator)
            .field("operator_principal", &self.operator_principal)
            .field("token", &"[redacted]")
            .finish()
    }
}

impl ExecutionClaimHandle {
    pub(crate) fn issue(key: ExecutionReceiptKey, generation: u64) -> Self {
        let nonce = OperationId::new().into_bytes();
        let mut input = Vec::with_capacity(32 + 16 + 8);
        input.extend_from_slice(&nonce);
        input.extend_from_slice(&key.attempt_id.into_bytes());
        input.extend_from_slice(&generation.to_le_bytes());
        Self {
            key,
            token: *blake3::hash(&input).as_bytes(),
            generation,
            operator: false,
            operator_principal: None,
            operator_authenticated: false,
        }
    }

    #[cfg(test)]
    pub(crate) fn from_owner_parts(
        key: ExecutionReceiptKey,
        token: [u8; 32],
        generation: u64,
    ) -> Self {
        Self {
            key,
            token,
            generation,
            operator: true,
            operator_principal: None,
            operator_authenticated: false,
        }
    }

    pub(crate) fn from_operator_parts(
        key: ExecutionReceiptKey,
        token: [u8; 32],
        generation: u64,
        principal: String,
    ) -> Result<Self> {
        if principal.is_empty() {
            return Err(Error::Unauthorized(
                "operator principal cannot be empty".into(),
            ));
        }
        Ok(Self {
            key,
            token,
            generation,
            operator: true,
            operator_principal: Some(principal),
            operator_authenticated: true,
        })
    }

    pub(crate) fn matches(&self, key: &ExecutionReceiptKey) -> bool {
        self.key == *key
    }

    pub(crate) const fn token(&self) -> &[u8; 32] {
        &self.token
    }

    pub(crate) const fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) const fn is_operator(&self) -> bool {
        self.operator
    }

    pub(crate) fn operator_principal(&self) -> Option<&str> {
        self.operator_principal.as_deref()
    }

    pub(crate) const fn operator_authenticated(&self) -> bool {
        self.operator_authenticated
    }
}

/// Result of an atomic host-journal claim before a process is spawned.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutionClaim {
    /// This caller owns the first dispatch reservation.
    Acquired {
        /// Opaque owner handle required to publish the terminal receipt.
        handle: ExecutionClaimHandle,
    },
    /// Another process or provider instance already owns the reservation.
    Pending,
    /// A durable terminal receipt already exists for this exact key.
    Completed(ExecutionReceiptRecord),
}

impl ExecutionReceiptRecord {
    /// Validates the record independently of a dispatch lookup.
    pub fn validate(&self) -> Result<()> {
        self.key.validate()?;
        self.result.validate()?;
        if self.result.descriptor().media_type() != "application/json" {
            return Err(Error::Invalid(
                "execution result must be JSON content".into(),
            ));
        }
        if self
            .operator_principal
            .as_deref()
            .is_some_and(str::is_empty)
            || (self.operator_authenticated
                && (!matches!(self.receipt, ExecutionReceipt::Unknown { .. })
                    || self.operator_principal.is_none()))
            || (!self.operator_authenticated && self.operator_principal.is_some())
        {
            return Err(Error::Invalid(
                "execution receipt operator principal does not match its outcome".into(),
            ));
        }
        self.receipt.validate()
    }

    /// Validates the typed record against a dispatch before replay.
    pub fn validate_for(&self, dispatch: &EffectDispatch) -> Result<()> {
        if self.key.operation_id.into_bytes() != dispatch.effect_id.into_bytes()
            || self.key.effect_id != dispatch.effect_id
            || self.key.attempt_id != dispatch.attempt_id
            || self.key.provider != dispatch.provider
            || self.key.effect_kind != dispatch.effect_kind
            || self.key.guarantee != dispatch.guarantee
            || self.key.request_digest != dispatch.request_digest
        {
            return Err(Error::Conflict(
                "execution receipt identity does not match dispatch".into(),
            ));
        }
        self.validate()
    }
}

/// Host-owned receipt persistence boundary.
///
/// The provider never accepts an arbitrary workspace path as a receipt store.
/// Native integrations should implement this with a private system journal
/// whose records bind the full key and are inaccessible to model tools.
pub trait ExecutionReceiptStore: Send + Sync {
    /// Atomically reserves this exact attempt before any host process starts.
    /// Implementations must persist `Pending` before returning `Acquired` and
    /// return `Pending` after restart until a receipt is published or an
    /// operator explicitly resolves the claim.
    fn claim<'a>(&'a self, _key: &'a ExecutionReceiptKey) -> BoxFuture<'a, Result<ExecutionClaim>> {
        async {
            Err(Error::Unsupported(
                "atomic host execution claim is unavailable".into(),
            ))
        }
        .boxed()
    }

    /// Resolves an existing immutable receipt for exactly this dispatch.
    fn load<'a>(
        &'a self,
        key: &'a ExecutionReceiptKey,
    ) -> BoxFuture<'a, Result<Option<ExecutionReceiptRecord>>>;

    /// Finds a previously admitted attempt during restart reconciliation.
    /// Implementations must return only a record whose key is authenticated
    /// by the host journal. The default is fail-closed.
    fn load_attempt<'a>(
        &'a self,
        _attempt_id: EffectAttemptId,
    ) -> BoxFuture<'a, Result<Option<ExecutionReceiptRecord>>> {
        async {
            Err(Error::Unsupported(
                "host receipt attempt reconciliation is unavailable".into(),
            ))
        }
        .boxed()
    }

    /// Persists a receipt before exposing its observation to Harness.
    fn publish<'a>(
        &'a self,
        key: &'a ExecutionReceiptKey,
        handle: &'a ExecutionClaimHandle,
        receipt: &'a ExecutionReceipt,
    ) -> BoxFuture<'a, Result<FileRef>>;

    /// Durably records cancellation intent for an admitted pending attempt.
    ///
    /// A restart must retain this fence even when the process outcome was not
    /// observed. Implementations should append intent through the same
    /// compare-and-swap journal used by `claim` and `publish`.
    fn request_cancel<'a>(&'a self, _key: &'a ExecutionReceiptKey) -> BoxFuture<'a, Result<()>> {
        async {
            Err(Error::Unsupported(
                "durable execution cancellation is unavailable".into(),
            ))
        }
        .boxed()
    }
}

/// Provider-neutral runner outcome, useful for deterministic fault injection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunnerOutcome {
    /// A process completed with captured output.
    Exited {
        /// Native exit code, if the process supplied one.
        status_code: Option<i32>,
        /// Captured standard output.
        stdout: Vec<u8>,
        /// Captured standard error.
        stderr: Vec<u8>,
    },
    /// Provider-side timeout.
    TimedOut {
        /// Output captured before termination.
        stdout: Vec<u8>,
        /// Error output captured before termination.
        stderr: Vec<u8>,
    },
    /// Provider-side cancellation.
    Cancelled {
        /// Output captured before termination.
        stdout: Vec<u8>,
        /// Error output captured before termination.
        stderr: Vec<u8>,
    },
    /// The process outcome cannot be known by this host.
    Unknown {
        /// Bounded diagnostic retained for the recovery owner.
        reason: String,
    },
}

/// Cooperative cancellation signal owned by an admitted operation.
#[derive(Clone, Debug, Default)]
pub struct ExecutionCancellation {
    cancelled: Arc<AtomicBool>,
}

impl ExecutionCancellation {
    /// Creates a fresh signal in the not-cancelled state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Requests cancellation of the running operation.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    /// Returns whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

/// Replaceable process runner boundary.  A sandbox provider can implement this
/// later without changing approval or Harness effect semantics.
pub trait ExecutionRunner: Send + Sync {
    /// Runs the exact admitted command.
    fn run(&self, request: &ExecutionSpec) -> Result<RunnerOutcome>;

    /// Runs with a cooperative cancellation signal.
    fn run_with_cancellation(
        &self,
        request: &ExecutionSpec,
        cancellation: &ExecutionCancellation,
    ) -> Result<RunnerOutcome> {
        if cancellation.is_cancelled() {
            return Ok(RunnerOutcome::Cancelled {
                stdout: Vec::new(),
                stderr: Vec::new(),
            });
        }
        self.run(request)
    }
}

/// Native host process runner.  It never inherits the caller environment.
#[derive(Clone, Copy, Debug, Default)]
pub struct NativeExecutionRunner;

impl ExecutionRunner for NativeExecutionRunner {
    fn run(&self, request: &ExecutionSpec) -> Result<RunnerOutcome> {
        self.run_with_cancellation(request, &ExecutionCancellation::new())
    }

    fn run_with_cancellation(
        &self,
        request: &ExecutionSpec,
        cancellation: &ExecutionCancellation,
    ) -> Result<RunnerOutcome> {
        request.validate()?;
        if cancellation.is_cancelled() {
            return Ok(RunnerOutcome::Cancelled {
                stdout: Vec::new(),
                stderr: Vec::new(),
            });
        }
        // Compute the deadline before spawning. An admitted timeout must not
        // discover an unrepresentable clock instant after a child exists.
        let deadline = request
            .timeout_ms
            .map(|ms| {
                Instant::now()
                    .checked_add(Duration::from_millis(ms))
                    .ok_or_else(|| {
                        Error::Invalid(
                            "execution timeout cannot be represented by the host clock".into(),
                        )
                    })
            })
            .transpose()?;
        let mut command = Command::new(&request.executable);
        command
            .args(&request.arguments)
            .current_dir(&request.working_directory)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        request.environment.apply(&mut command);
        let mut child = ManagedChild::spawn(&mut command).map_err(|error| {
            Error::Storage(format!("failed to start approved process: {error}"))
        })?;
        let stdout = child
            .take_stdout()
            .ok_or_else(|| Error::Storage("approved process stdout pipe missing".into()))?;
        let stderr = child
            .take_stderr()
            .ok_or_else(|| Error::Storage("approved process stderr pipe missing".into()))?;
        let overflow = Arc::new(AtomicBool::new(false));
        let remaining = Arc::new(std::sync::atomic::AtomicUsize::new(
            request.max_output_bytes as usize,
        ));
        let stdout_thread = spawn_reader(stdout, Arc::clone(&remaining), Arc::clone(&overflow));
        let stderr_thread = spawn_reader(stderr, Arc::clone(&remaining), Arc::clone(&overflow));
        let termination;
        loop {
            if overflow.load(Ordering::Acquire) {
                let _ = child.terminate();
                termination = Termination::Overflow;
                break;
            }
            if cancellation.is_cancelled() {
                let _ = child.terminate();
                termination = Termination::Cancelled;
                break;
            }
            if let Some(status) = child.try_wait().map_err(|error| {
                Error::Storage(format!("failed waiting for approved process: {error}"))
            })? {
                let stdout = receive_reader(&stdout_thread)?;
                let stderr = receive_reader(&stderr_thread)?;
                if overflow.load(Ordering::Acquire) {
                    return Err(Error::Invalid(
                        "approved process output exceeded its limit".into(),
                    ));
                }
                let (Some(stdout), Some(stderr)) = (stdout, stderr) else {
                    return Ok(RunnerOutcome::Unknown {
                        reason: "process descendants retained output handles".into(),
                    });
                };
                return Ok(RunnerOutcome::Exited {
                    status_code: status.code(),
                    stdout,
                    stderr,
                });
            }
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                let _ = child.terminate();
                termination = Termination::TimedOut;
                break;
            }
            thread::sleep(Duration::from_millis(5));
        }
        let stdout = receive_reader(&stdout_thread)?;
        let stderr = receive_reader(&stderr_thread)?;
        if overflow.load(Ordering::Acquire) {
            return Err(Error::Invalid(
                "approved process output exceeded its limit".into(),
            ));
        }
        let (Some(stdout), Some(stderr)) = (stdout, stderr) else {
            return Ok(RunnerOutcome::Unknown {
                reason: "process descendants retained output handles".into(),
            });
        };
        if !child.controls_process_tree()
            && matches!(termination, Termination::TimedOut | Termination::Cancelled)
        {
            return Ok(RunnerOutcome::Unknown {
                reason:
                    "native runner cannot prove descendant termination without process-tree support"
                        .into(),
            });
        }
        match termination {
            Termination::TimedOut => Ok(RunnerOutcome::TimedOut { stdout, stderr }),
            Termination::Cancelled => Ok(RunnerOutcome::Cancelled { stdout, stderr }),
            Termination::Overflow => unreachable!("overflow is returned above"),
        }
    }
}

/// Child handle used by the native adapter. The optional process-tree feature
/// gives timeout and cancellation ownership of inherited descendants; the
/// direct fallback preserves compilation for WASM and minimal hosts.
enum ManagedChild {
    Direct(Child),
    #[cfg(all(feature = "native-process-tree", not(target_arch = "wasm32")))]
    Tree(ProcessTree),
}

impl ManagedChild {
    fn spawn(command: &mut Command) -> std::io::Result<Self> {
        #[cfg(all(feature = "native-process-tree", not(target_arch = "wasm32")))]
        {
            return spawn_process_tree(command).map(Self::Tree);
        }
        #[allow(unreachable_code)]
        command.spawn().map(Self::Direct)
    }

    fn take_stdout(&mut self) -> Option<ChildStdout> {
        match self {
            Self::Direct(child) => child.stdout.take(),
            #[cfg(all(feature = "native-process-tree", not(target_arch = "wasm32")))]
            Self::Tree(tree) => tree.take_stdout(),
        }
    }

    fn take_stderr(&mut self) -> Option<ChildStderr> {
        match self {
            Self::Direct(child) => child.stderr.take(),
            #[cfg(all(feature = "native-process-tree", not(target_arch = "wasm32")))]
            Self::Tree(tree) => tree.take_stderr(),
        }
    }

    fn try_wait(&mut self) -> std::io::Result<Option<ExitStatus>> {
        match self {
            Self::Direct(child) => child.try_wait(),
            #[cfg(all(feature = "native-process-tree", not(target_arch = "wasm32")))]
            Self::Tree(tree) => tree.try_wait(),
        }
    }

    fn terminate(&mut self) -> std::io::Result<()> {
        match self {
            Self::Direct(child) => {
                let _ = child.kill();
                child.wait().map(|_| ())
            }
            #[cfg(all(feature = "native-process-tree", not(target_arch = "wasm32")))]
            Self::Tree(tree) => tree.terminate(),
        }
    }

    fn controls_process_tree(&self) -> bool {
        match self {
            Self::Direct(_) => false,
            #[cfg(all(feature = "native-process-tree", not(target_arch = "wasm32")))]
            Self::Tree(_) => true,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Termination {
    TimedOut,
    Cancelled,
    Overflow,
}

fn spawn_reader<R: Read + Send + 'static>(
    mut reader: R,
    remaining: Arc<std::sync::atomic::AtomicUsize>,
    overflow: Arc<AtomicBool>,
) -> Receiver<Result<Vec<u8>>> {
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let result = (|| {
            let mut bytes = Vec::new();
            let mut buffer = [0_u8; 8192];
            loop {
                let read = reader.read(&mut buffer).map_err(|error| {
                    Error::Storage(format!("failed reading process output: {error}"))
                })?;
                if read == 0 {
                    break;
                }
                let consumed =
                    remaining.fetch_update(Ordering::AcqRel, Ordering::Acquire, |available| {
                        available.checked_sub(read)
                    });
                if consumed.is_err() {
                    overflow.store(true, Ordering::Release);
                    break;
                }
                bytes.extend_from_slice(buffer.get(..read).ok_or_else(|| {
                    Error::Storage("process reader returned an invalid length".into())
                })?);
            }
            Ok(bytes)
        })();
        let _ = sender.send(result);
    });
    receiver
}

fn receive_reader(receiver: &Receiver<Result<Vec<u8>>>) -> Result<Option<Vec<u8>>> {
    match receiver.recv_timeout(READER_GRACE) {
        Ok(result) => result.map(Some),
        Err(RecvTimeoutError::Timeout) => Ok(None),
        Err(RecvTimeoutError::Disconnected) => Err(Error::Storage(
            "process output reader disconnected without a result".into(),
        )),
    }
}

/// Immutable dispatch identity presented to the approval authority.
///
/// The serialized `approved` bit is request data. A production verifier must
/// resolve the interaction in its authenticated session journal and compare
/// every field here before allowing the process to run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionApprovalContext<'a> {
    /// Session claimed by the persisted approval.
    pub session_id: SessionId,
    /// Durable interaction claimed by the persisted approval.
    pub interaction_id: InteractionId,
    /// Operation identity allocated before admission.
    pub operation_id: OperationId,
    /// Effect identity under which the process is dispatched.
    pub effect_id: EffectId,
    /// Dispatch attempt identity.
    pub attempt_id: EffectAttemptId,
    /// Provider identity selected by durable effect state.
    pub provider: &'a str,
    /// Provider operation kind.
    pub effect_kind: &'a str,
    /// Pinned delivery guarantee.
    pub guarantee: EffectGuarantee,
    /// Digest of the exact immutable dispatch request content, including its
    /// staged `FileRef` identity and descriptor.
    pub request_digest: [u8; 32],
    /// Digest of the exact host volume and logical path being dispatched.
    pub request_locator_digest: [u8; 32],
}

/// Verifies that an execution request was approved by the durable owner.
///
/// The serialized `approved` flag is request data and is never sufficient on
/// its own.  Implementations should authenticate the owner record and bind it
/// to the operation, request digest, and current session before returning.
pub trait ExecutionApprovalVerifier: Send + Sync {
    /// Authenticates the persisted approval or denial record.
    fn verify<'a>(
        &'a self,
        context: ExecutionApprovalContext<'a>,
        approval: &'a ExecutionApproval,
    ) -> BoxFuture<'a, Result<()>>;
}

/// Host-bound provider that adapts approved processes to Harness effects.
pub struct NativeExecutionProvider {
    resolver: Arc<dyn ContentResidencyVerifier>,
    /// Legacy content publisher retained for compatibility with existing
    /// tests and callers. Production composition should provide
    /// `receipt_store`, which is host-owned and request-bound.
    publisher: Option<Arc<dyn ContentPublisher>>,
    receipt_store: Option<Arc<dyn ExecutionReceiptStore>>,
    runner: Arc<dyn ExecutionRunner>,
    approval_verifier: Arc<dyn ExecutionApprovalVerifier>,
    /// In-process reservation for an admitted operation.  A single semantic
    /// operation may have many durable attempts over its lifetime, but only
    /// one attempt can be dispatched by this provider at a time.  Keeping the
    /// attempt identity with the cancellation signal prevents a late cleanup
    /// from deleting a newer reservation.
    active:
        Mutex<BTreeMap<OperationId, (ExecutionReceiptKey, EffectAttemptId, ExecutionCancellation)>>,
    provider_id: String,
}

impl NativeExecutionProvider {
    /// Compatibility constructor for in-crate legacy tests.
    ///
    /// Production composition must use [`Self::new_with_receipt_store`],
    /// because this path resolves receipts through an agent-private content
    /// publisher and cannot establish a host-only system journal boundary.
    #[cfg(test)]
    pub fn new(
        resolver: Arc<dyn ContentResidencyVerifier>,
        publisher: Arc<dyn ContentPublisher>,
        runner: Arc<dyn ExecutionRunner>,
        approval_verifier: Arc<dyn ExecutionApprovalVerifier>,
    ) -> Result<Self> {
        if publisher.volume().class() != crate::conversation::VolumeClass::AgentPrivate {
            return Err(Error::Unauthorized(
                "execution results require an agent-private output volume".into(),
            ));
        }
        Ok(Self {
            resolver,
            publisher: Some(publisher),
            receipt_store: None,
            runner,
            approval_verifier,
            active: Mutex::new(BTreeMap::new()),
            provider_id: "harness.native-execution.v1".into(),
        })
    }

    /// Binds a host-owned, request-bound receipt journal. This is the
    /// production constructor: receipt records are not resolved through a
    /// model-writable workspace path.
    pub fn new_with_receipt_store(
        resolver: Arc<dyn ContentResidencyVerifier>,
        receipt_store: Arc<dyn ExecutionReceiptStore>,
        runner: Arc<dyn ExecutionRunner>,
        approval_verifier: Arc<dyn ExecutionApprovalVerifier>,
    ) -> Result<Self> {
        Ok(Self {
            resolver,
            publisher: None,
            receipt_store: Some(receipt_store),
            runner,
            approval_verifier,
            active: Mutex::new(BTreeMap::new()),
            provider_id: "harness.native-execution.v1".into(),
        })
    }

    /// Compatibility constructor for in-crate legacy tests.
    #[cfg(test)]
    pub fn native(
        resolver: Arc<dyn ContentResidencyVerifier>,
        publisher: Arc<dyn ContentPublisher>,
        approval_verifier: Arc<dyn ExecutionApprovalVerifier>,
    ) -> Result<Self> {
        Self::new(
            resolver,
            publisher,
            Arc::new(NativeExecutionRunner),
            approval_verifier,
        )
    }

    /// Uses the native runner with a host-owned receipt journal.
    pub fn native_with_receipt_store(
        resolver: Arc<dyn ContentResidencyVerifier>,
        receipt_store: Arc<dyn ExecutionReceiptStore>,
        approval_verifier: Arc<dyn ExecutionApprovalVerifier>,
    ) -> Result<Self> {
        Self::new_with_receipt_store(
            resolver,
            receipt_store,
            Arc::new(NativeExecutionRunner),
            approval_verifier,
        )
    }

    /// Requests cancellation of a currently running operation.
    ///
    /// This compatibility entry point signals the process immediately and
    /// schedules the matching durable cancellation intent. Call
    /// [`Self::cancel_and_persist`] when the caller must observe persistence
    /// before proceeding.
    pub fn cancel(&self, operation_id: OperationId) -> bool {
        let Ok(active) = self.active.lock() else {
            return false;
        };
        let Some((key, _, cancellation)) = active.get(&operation_id).cloned() else {
            return false;
        };
        cancellation.cancel();
        if let Some(store) = self.receipt_store.as_ref().map(Arc::clone) {
            thread::spawn(move || {
                let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                else {
                    return;
                };
                let _ = runtime.block_on(store.request_cancel(&key));
            });
        }
        true
    }

    /// Signals and durably records cancellation for the currently running
    /// attempt. A successful return means a restarted provider will retain the
    /// cancellation fence instead of spawning the operation again.
    pub async fn cancel_and_persist(&self, operation_id: OperationId) -> Result<bool> {
        let (key, cancellation) = {
            let active = self
                .active
                .lock()
                .map_err(|_| Error::Storage("active execution registry is poisoned".into()))?;
            let Some((key, _, cancellation)) = active.get(&operation_id).cloned() else {
                return Ok(false);
            };
            (key, cancellation)
        };
        cancellation.cancel();
        if let Some(store) = &self.receipt_store {
            store.request_cancel(&key).await?;
        }
        Ok(true)
    }

    fn reserve_attempt(&self, key: ExecutionReceiptKey) -> Result<ExecutionCancellation> {
        let operation_id = key.operation_id;
        let attempt_id = key.attempt_id;
        let mut active = self
            .active
            .lock()
            .map_err(|_| Error::Storage("active execution registry is poisoned".into()))?;
        if active.contains_key(&operation_id) {
            return Err(Error::Conflict(
                "execution operation already has an active attempt".into(),
            ));
        }
        let cancellation = ExecutionCancellation::new();
        active.insert(operation_id, (key, attempt_id, cancellation.clone()));
        Ok(cancellation)
    }

    fn release_attempt(
        &self,
        operation_id: OperationId,
        attempt_id: EffectAttemptId,
    ) -> Result<()> {
        let mut active = self
            .active
            .lock()
            .map_err(|_| Error::Storage("active execution registry is poisoned".into()))?;
        if active
            .get(&operation_id)
            .is_some_and(|(_, active_attempt, _)| *active_attempt == attempt_id)
        {
            active.remove(&operation_id);
        }
        Ok(())
    }

    fn receipt_key(request: &EffectDispatch, operation_id: OperationId) -> ExecutionReceiptKey {
        ExecutionReceiptKey {
            operation_id,
            effect_id: request.effect_id,
            attempt_id: request.attempt_id,
            provider: request.provider.clone(),
            effect_kind: request.effect_kind.clone(),
            guarantee: request.guarantee,
            request_digest: request.request_digest,
        }
    }

    async fn persisted_receipt(
        &self,
        dispatch: &EffectDispatch,
        operation_id: OperationId,
    ) -> Result<Option<(FileRef, ExecutionReceipt)>> {
        let key = Self::receipt_key(dispatch, operation_id);
        if let Some(store) = &self.receipt_store {
            let record = store.load(&key).await?;
            if let Some(record) = record {
                record.validate_for(dispatch)?;
                return Ok(Some((record.result, record.receipt)));
            }
            return Ok(None);
        }
        let publisher = self.publisher.as_ref().ok_or_else(|| {
            Error::Unsupported("host execution receipt store is not configured".into())
        })?;
        let path = format!(
            ".harness/execution/{}/attempt-{}.json",
            operation_id, dispatch.attempt_id
        );
        let resolved = match self
            .resolver
            .read_private_path(publisher.volume(), "", &path, None)
            .await
        {
            Ok(resolved) => resolved,
            Err(Error::NotFound(_)) => return Ok(None),
            Err(error) => return Err(error),
        };
        let (reference, bytes) = resolved;
        reference.descriptor().verify(&bytes)?;
        let receipt: ExecutionReceipt = serde_json::from_slice(&bytes).map_err(|error| {
            Error::Invalid(format!("persisted execution receipt is invalid: {error}"))
        })?;
        receipt.validate()?;
        Ok(Some((reference, receipt)))
    }

    fn status_for_receipt(receipt: ExecutionReceipt, result: FileRef) -> EffectStatus {
        match receipt {
            ExecutionReceipt::Succeeded { .. } => EffectStatus::Succeeded { result },
            ExecutionReceipt::Failed { status_code, .. } => EffectStatus::FailedWithReceipt {
                message: format!("process exited with status {status_code:?}"),
                result,
            },
            ExecutionReceipt::TimedOut { .. } => EffectStatus::FailedWithReceipt {
                message: "process timed out".into(),
                result,
            },
            ExecutionReceipt::Cancelled { .. } => EffectStatus::FailedWithReceipt {
                message: "process cancelled".into(),
                result,
            },
            ExecutionReceipt::Denied { reason } => EffectStatus::FailedWithReceipt {
                message: format!("execution denied: {reason}"),
                result,
            },
            ExecutionReceipt::Unknown { .. } => EffectStatus::Indeterminate,
        }
    }

    fn bounded_unknown(reason: impl Into<String>) -> ExecutionReceipt {
        let mut reason = reason.into();
        if reason.is_empty() {
            reason = "host execution outcome is unknown".into();
        }
        if reason.len() > MAX_FAILURE_BYTES {
            let mut end = MAX_FAILURE_BYTES.min(reason.len());
            while end > 0 && !reason.is_char_boundary(end) {
                end -= 1;
            }
            reason.truncate(end);
        }
        ExecutionReceipt::Unknown { reason }
    }

    fn enforce_output_limit(request: &ExecutionSpec, outcome: RunnerOutcome) -> RunnerOutcome {
        let output_len = |stdout: &[u8], stderr: &[u8]| {
            stdout.len().saturating_add(stderr.len()) > request.max_output_bytes as usize
        };
        match outcome {
            RunnerOutcome::Exited {
                status_code: _,
                stdout,
                stderr,
            } if output_len(&stdout, &stderr) => RunnerOutcome::Unknown {
                reason: "execution runner exceeded the approved output limit".into(),
            },
            RunnerOutcome::TimedOut { stdout, stderr } if output_len(&stdout, &stderr) => {
                RunnerOutcome::Unknown {
                    reason: "execution runner exceeded the approved output limit".into(),
                }
            }
            RunnerOutcome::Cancelled { stdout, stderr } if output_len(&stdout, &stderr) => {
                RunnerOutcome::Unknown {
                    reason: "execution runner exceeded the approved output limit".into(),
                }
            }
            outcome => outcome,
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the provider boundary validates, executes, stages, and maps one exact effect attempt"
    )]
    async fn dispatch_inner(&self, request: EffectDispatch) -> Result<EffectObservation> {
        if request.provider.as_str() != self.provider_id {
            return Err(Error::Unauthorized(
                "execution dispatch names a different provider".into(),
            ));
        }
        if request.effect_kind != "host.process" {
            return Err(Error::Invalid(
                "native execution provider only accepts host.process effects".into(),
            ));
        }
        if request.guarantee != EffectGuarantee::AtMostOnce {
            return Err(Error::Invalid(
                "native execution requires the at-most-once guarantee".into(),
            ));
        }
        let expected_request_digest = crate::core::effect_request_digest(
            &request.provider,
            request.guarantee,
            &request.effect_kind,
            &request.request,
        )?;
        if request.request_digest != expected_request_digest {
            return Err(Error::Conflict(
                "execution dispatch request digest is stale or forged".into(),
            ));
        }
        if request.request.descriptor().media_type() != "application/json" {
            return Err(Error::Invalid(
                "approved execution request must be JSON content".into(),
            ));
        }
        let bytes = self.resolver.read(&request.request).await?;
        request.request.descriptor().verify(&bytes)?;
        let approval: ExecutionApproval = serde_json::from_slice(&bytes).map_err(|error| {
            Error::Invalid(format!("approved execution request is invalid: {error}"))
        })?;
        approval.validate()?;
        if self.receipt_store.is_some() {
            let locator =
                execution_request_locator_digest(request.request.volume(), request.request.path())?;
            if approval.request_locator_digest != Some(locator) {
                return Err(Error::Conflict(
                    "execution approval is not bound to this host request location".into(),
                ));
            }
        }
        if EffectId::from_bytes(approval.operation_id.into_bytes()) != request.effect_id {
            return Err(Error::Conflict(
                "execution approval operation does not match effect identity".into(),
            ));
        }
        self.approval_verifier
            .verify(
                ExecutionApprovalContext {
                    session_id: approval.session_id,
                    interaction_id: approval.interaction_id,
                    operation_id: approval.operation_id,
                    effect_id: request.effect_id,
                    attempt_id: request.attempt_id,
                    provider: &request.provider,
                    effect_kind: &request.effect_kind,
                    guarantee: request.guarantee,
                    request_digest: request.request_digest,
                    request_locator_digest: execution_request_locator_digest(
                        request.request.volume(),
                        request.request.path(),
                    )?,
                },
                &approval,
            )
            .await?;
        let key = Self::receipt_key(&request, approval.operation_id);
        // Reserve before looking up the receipt so two concurrent dispatches
        // cannot both observe a miss and run the same host command.
        let cancellation = self.reserve_attempt(key.clone())?;
        let claim_handle = if let Some(store) = &self.receipt_store {
            match store.claim(&key).await {
                Ok(ExecutionClaim::Acquired { handle }) => handle,
                Ok(ExecutionClaim::Pending) => {
                    self.release_attempt(approval.operation_id, request.attempt_id)?;
                    return Err(Error::Indeterminate(approval.operation_id));
                }
                Ok(ExecutionClaim::Completed(record)) => {
                    self.release_attempt(approval.operation_id, request.attempt_id)?;
                    record.validate_for(&request)?;
                    return Ok(EffectObservation {
                        provider: request.provider,
                        effect_id: request.effect_id,
                        attempt_id: request.attempt_id,
                        request_digest: request.request_digest,
                        guarantee: request.guarantee,
                        status: Self::status_for_receipt(record.receipt, record.result),
                    });
                }
                Err(error) => {
                    self.release_attempt(approval.operation_id, request.attempt_id)?;
                    return Err(error);
                }
            }
        } else {
            match self
                .persisted_receipt(&request, approval.operation_id)
                .await
            {
                Ok(Some((result, receipt))) => {
                    self.release_attempt(approval.operation_id, request.attempt_id)?;
                    return Ok(EffectObservation {
                        provider: request.provider,
                        effect_id: request.effect_id,
                        attempt_id: request.attempt_id,
                        request_digest: request.request_digest,
                        guarantee: request.guarantee,
                        status: Self::status_for_receipt(receipt, result),
                    });
                }
                Ok(None) => {}
                Err(error) => {
                    self.release_attempt(approval.operation_id, request.attempt_id)?;
                    return Err(error);
                }
            }
            ExecutionClaimHandle::issue(Self::receipt_key(&request, approval.operation_id), 0)
        };
        let receipt = if approval.approved {
            let runner = Arc::clone(&self.runner);
            let execution_request = approval.request.clone();
            let outcome = tokio::task::spawn_blocking(move || {
                runner.run_with_cancellation(&execution_request, &cancellation)
            })
            .await;
            match outcome {
                Err(error) => Self::bounded_unknown(format!(
                    "approved process task failed before its outcome was durable: {error}"
                )),
                Ok(Err(error)) => Self::bounded_unknown(format!(
                    "approved process runner failed before its outcome was durable: {error}"
                )),
                Ok(Ok(outcome)) => match Self::enforce_output_limit(&approval.request, outcome) {
                    RunnerOutcome::Exited {
                        status_code: Some(0),
                        stdout,
                        stderr,
                    } => ExecutionReceipt::Succeeded {
                        status_code: 0,
                        stdout,
                        stderr,
                    },
                    RunnerOutcome::Exited {
                        status_code,
                        stdout,
                        stderr,
                    } => ExecutionReceipt::Failed {
                        status_code,
                        stdout,
                        stderr,
                    },
                    RunnerOutcome::TimedOut { stdout, stderr } => {
                        ExecutionReceipt::TimedOut { stdout, stderr }
                    }
                    RunnerOutcome::Cancelled { stdout, stderr } => {
                        ExecutionReceipt::Cancelled { stdout, stderr }
                    }
                    RunnerOutcome::Unknown { reason } => Self::bounded_unknown(reason),
                },
            }
        } else {
            ExecutionReceipt::Denied {
                reason: approval
                    .denial_reason
                    .unwrap_or_else(|| "owner denied execution".into()),
            }
        };
        if let Err(error) = receipt.validate() {
            self.release_attempt(approval.operation_id, request.attempt_id)?;
            return Err(error);
        }
        // A native runner can finish with an unknown external outcome. Keep
        // the durable claim pending and require operator resolution; publishing
        // an Unknown receipt from the dispatcher would incorrectly clear the
        // retry fence.
        if self.receipt_store.is_some() && matches!(&receipt, ExecutionReceipt::Unknown { .. }) {
            self.release_attempt(approval.operation_id, request.attempt_id)?;
            return Ok(EffectObservation {
                provider: request.provider,
                effect_id: request.effect_id,
                attempt_id: request.attempt_id,
                request_digest: request.request_digest,
                guarantee: request.guarantee,
                status: EffectStatus::Indeterminate,
            });
        }
        let receipt_bytes = match serde_json::to_vec(&receipt) {
            Ok(bytes) => bytes,
            Err(error) => {
                self.release_attempt(approval.operation_id, request.attempt_id)?;
                return Err(Error::Invalid(error.to_string()));
            }
        };
        let result = if let Some(store) = &self.receipt_store {
            match store.publish(&key, &claim_handle, &receipt).await {
                Ok(result) => result,
                Err(error) => {
                    self.release_attempt(approval.operation_id, request.attempt_id)?;
                    return Err(error);
                }
            }
        } else {
            let publisher = self.publisher.as_ref().ok_or_else(|| {
                Error::Unsupported("host execution receipt store is not configured".into())
            })?;
            let path = format!(
                ".harness/execution/{}/attempt-{}.json",
                approval.operation_id, request.attempt_id
            );
            match publisher
                .stage(
                    approval.operation_id,
                    &path,
                    &receipt_bytes,
                    "application/json",
                    "execution-result.json",
                )
                .await
            {
                Ok(result) => result,
                Err(error) => {
                    self.release_attempt(approval.operation_id, request.attempt_id)?;
                    return Err(error);
                }
            }
        };
        // Keep the reservation until the receipt publication has returned.
        // A second dispatch must never race the first attempt between process
        // completion and durable result publication.
        self.release_attempt(approval.operation_id, request.attempt_id)?;
        let status = Self::status_for_receipt(receipt, result);
        Ok(EffectObservation {
            provider: request.provider,
            effect_id: request.effect_id,
            attempt_id: request.attempt_id,
            request_digest: request.request_digest,
            guarantee: request.guarantee,
            status,
        })
    }
}

impl EffectProvider for NativeExecutionProvider {
    fn id(&self) -> &str {
        &self.provider_id
    }

    fn guarantees(&self, effect_kind: &str) -> std::collections::BTreeSet<EffectGuarantee> {
        if effect_kind == "host.process" {
            [EffectGuarantee::AtMostOnce].into_iter().collect()
        } else {
            Default::default()
        }
    }

    fn linearizable_reconciliation(&self) -> bool {
        false
    }

    fn dispatch<'a>(&'a self, request: EffectDispatch) -> BoxFuture<'a, Result<EffectObservation>> {
        self.dispatch_inner(request).boxed()
    }

    fn reconcile<'a>(
        &'a self,
        attempt_id: EffectAttemptId,
    ) -> BoxFuture<'a, Result<Option<EffectObservation>>> {
        async move {
            let Some(store) = &self.receipt_store else {
                return Ok(None);
            };
            let Some(record) = store.load_attempt(attempt_id).await? else {
                return Ok(None);
            };
            record.validate()?;
            Ok(Some(EffectObservation {
                provider: record.key.provider.clone(),
                effect_id: record.key.effect_id,
                attempt_id: record.key.attempt_id,
                request_digest: record.key.request_digest,
                guarantee: record.key.guarantee,
                status: Self::status_for_receipt(record.receipt, record.result),
            }))
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AgentId, Capabilities,
        conversation::{FileDescriptor, FileRef, VolumeClass, VolumeOwner, VolumeRef},
        core::{AggregateKind, Authority, AuthorityIssuer},
        resources::ProviderRef,
    };
    use std::sync::Mutex;

    struct MemoryContent {
        volume: VolumeRef,
        request: Vec<u8>,
        staged: Mutex<Vec<Vec<u8>>>,
    }

    impl ContentResidencyVerifier for MemoryContent {
        fn verify<'a>(
            &'a self,
            reference: &'a FileRef,
        ) -> futures::future::BoxFuture<'a, Result<()>> {
            async move {
                if reference.descriptor().media_type() != "application/json" {
                    return Err(Error::Invalid("test content media type mismatch".into()));
                }
                reference.descriptor().verify(&self.request)
            }
            .boxed()
        }

        fn read<'a>(
            &'a self,
            reference: &'a FileRef,
        ) -> futures::future::BoxFuture<'a, Result<Vec<u8>>> {
            async move {
                reference.descriptor().verify(&self.request)?;
                Ok(self.request.clone())
            }
            .boxed()
        }

        fn read_private_path<'a>(
            &'a self,
            volume: &'a VolumeRef,
            _granted_prefix: &'a str,
            path: &'a str,
            _expected_generation: Option<&'a crate::resources::GenerationRef>,
        ) -> crate::conversation::ContentFuture<'a, Result<(FileRef, Vec<u8>)>> {
            Box::pin(async move {
                let bytes = self
                    .staged
                    .lock()
                    .map_err(|_| Error::Storage("test staged content lock poisoned".into()))?
                    .last()
                    .cloned()
                    .ok_or_else(|| Error::NotFound(path.into()))?;
                let descriptor = FileDescriptor::from_bytes(&bytes, "application/json")?;
                let reference = FileRef::new(
                    volume.clone(),
                    path,
                    "replayed-generation",
                    descriptor,
                    "execution-result.json",
                )?;
                Ok((reference, bytes))
            })
        }
    }

    impl ContentPublisher for MemoryContent {
        fn volume(&self) -> &VolumeRef {
            &self.volume
        }

        fn stage<'a>(
            &'a self,
            _operation_id: OperationId,
            path: &'a str,
            bytes: &'a [u8],
            media_type: &'a str,
            display_name: &'a str,
        ) -> futures::future::BoxFuture<'a, Result<FileRef>> {
            async move {
                let version = format!("generation-{}", self.staged.lock().unwrap().len() + 1);
                let descriptor = FileDescriptor::from_bytes(bytes, media_type)?;
                let reference =
                    FileRef::new(self.volume.clone(), path, version, descriptor, display_name)?;
                self.staged.lock().unwrap().push(bytes.to_vec());
                Ok(reference)
            }
            .boxed()
        }
    }

    #[derive(Default)]
    struct MemoryReceiptState {
        records: Vec<ExecutionReceiptRecord>,
        pending: Vec<(ExecutionReceiptKey, ExecutionClaimHandle)>,
        cancellation_requested: Vec<ExecutionReceiptKey>,
    }

    #[derive(Default)]
    struct MemoryReceiptStore {
        volume: Option<VolumeRef>,
        state: Mutex<MemoryReceiptState>,
    }

    impl ExecutionReceiptStore for MemoryReceiptStore {
        fn claim<'a>(
            &'a self,
            key: &'a ExecutionReceiptKey,
        ) -> futures::future::BoxFuture<'a, Result<ExecutionClaim>> {
            Box::pin(async move {
                let mut state = self
                    .state
                    .lock()
                    .map_err(|_| Error::Storage("test receipt lock poisoned".into()))?;
                if let Some(record) = state
                    .records
                    .iter()
                    .find(|record| record.key == *key)
                    .cloned()
                {
                    return Ok(ExecutionClaim::Completed(record));
                }
                if state
                    .cancellation_requested
                    .iter()
                    .any(|candidate| candidate == key)
                {
                    return Ok(ExecutionClaim::Pending);
                }
                if state.pending.iter().any(|(candidate, _)| candidate == key) {
                    return Ok(ExecutionClaim::Pending);
                }
                let handle = ExecutionClaimHandle::issue(key.clone(), state.pending.len() as u64);
                state.pending.push((key.clone(), handle.clone()));
                Ok(ExecutionClaim::Acquired { handle })
            })
        }

        fn load<'a>(
            &'a self,
            key: &'a ExecutionReceiptKey,
        ) -> futures::future::BoxFuture<'a, Result<Option<ExecutionReceiptRecord>>> {
            Box::pin(async move {
                Ok(self
                    .state
                    .lock()
                    .map_err(|_| Error::Storage("test receipt lock poisoned".into()))?
                    .records
                    .iter()
                    .find(|record| record.key == *key)
                    .cloned())
            })
        }

        fn publish<'a>(
            &'a self,
            key: &'a ExecutionReceiptKey,
            handle: &'a ExecutionClaimHandle,
            receipt: &'a ExecutionReceipt,
        ) -> futures::future::BoxFuture<'a, Result<FileRef>> {
            Box::pin(async move {
                receipt.validate()?;
                let volume = self
                    .volume
                    .as_ref()
                    .ok_or_else(|| Error::Storage("test receipt volume missing".into()))?
                    .clone();
                let bytes = serde_json::to_vec(receipt)
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                let descriptor = FileDescriptor::from_bytes(&bytes, "application/json")?;
                let result = FileRef::new(
                    volume,
                    format!("system-receipts/{}", key.attempt_id),
                    "receipt-generation",
                    descriptor,
                    "execution-result.json",
                )?;
                let mut state = self
                    .state
                    .lock()
                    .map_err(|_| Error::Storage("test receipt lock poisoned".into()))?;
                let Some((_, pending_handle)) =
                    state.pending.iter().find(|(candidate, _)| candidate == key)
                else {
                    return Err(Error::Conflict("test receipt claim is missing".into()));
                };
                if pending_handle != handle {
                    return Err(Error::Conflict("test receipt claim handle is stale".into()));
                }
                state.records.push(ExecutionReceiptRecord {
                    key: key.clone(),
                    result: result.clone(),
                    receipt: receipt.clone(),
                    operator_principal: None,
                    operator_authenticated: false,
                });
                state.pending.retain(|(candidate, _)| candidate != key);
                Ok(result)
            })
        }

        fn load_attempt<'a>(
            &'a self,
            attempt_id: EffectAttemptId,
        ) -> futures::future::BoxFuture<'a, Result<Option<ExecutionReceiptRecord>>> {
            Box::pin(async move {
                Ok(self
                    .state
                    .lock()
                    .map_err(|_| Error::Storage("test receipt lock poisoned".into()))?
                    .records
                    .iter()
                    .find(|record| record.key.attempt_id == attempt_id)
                    .cloned())
            })
        }

        fn request_cancel<'a>(
            &'a self,
            key: &'a ExecutionReceiptKey,
        ) -> futures::future::BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                let mut state = self
                    .state
                    .lock()
                    .map_err(|_| Error::Storage("test receipt lock poisoned".into()))?;
                if state.records.iter().any(|record| record.key == *key)
                    || state
                        .cancellation_requested
                        .iter()
                        .any(|candidate| candidate == key)
                {
                    return Ok(());
                }
                if !state.pending.iter().any(|(candidate, _)| candidate == key) {
                    return Err(Error::Conflict(
                        "test receipt cancellation claim is missing".into(),
                    ));
                }
                state.cancellation_requested.push(key.clone());
                Ok(())
            })
        }
    }

    #[derive(Default)]
    struct TestApprovalVerifier;

    impl ExecutionApprovalVerifier for TestApprovalVerifier {
        fn verify<'a>(
            &'a self,
            _context: ExecutionApprovalContext<'a>,
            _approval: &'a ExecutionApproval,
        ) -> futures::future::BoxFuture<'a, Result<()>> {
            async { Ok(()) }.boxed()
        }
    }

    fn approval_verifier() -> Arc<dyn ExecutionApprovalVerifier> {
        Arc::new(TestApprovalVerifier)
    }

    struct RejectApprovalVerifier;

    impl ExecutionApprovalVerifier for RejectApprovalVerifier {
        fn verify<'a>(
            &'a self,
            _context: ExecutionApprovalContext<'a>,
            _: &'a ExecutionApproval,
        ) -> futures::future::BoxFuture<'a, Result<()>> {
            async {
                Err(Error::Unauthorized(
                    "durable owner approval is absent".into(),
                ))
            }
            .boxed()
        }
    }

    #[derive(Clone)]
    struct FixedRunner(RunnerOutcome);

    impl ExecutionRunner for FixedRunner {
        fn run(&self, _request: &ExecutionSpec) -> Result<RunnerOutcome> {
            Ok(self.0.clone())
        }
    }

    #[derive(Clone)]
    struct CountingFixedRunner {
        calls: Arc<std::sync::atomic::AtomicUsize>,
        outcome: RunnerOutcome,
    }

    impl ExecutionRunner for CountingFixedRunner {
        fn run(&self, _request: &ExecutionSpec) -> Result<RunnerOutcome> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.outcome.clone())
        }
    }

    struct FaultAfterExitRunner;

    impl ExecutionRunner for FaultAfterExitRunner {
        fn run(&self, request: &ExecutionSpec) -> Result<RunnerOutcome> {
            let _ = NativeExecutionRunner.run(request)?;
            Err(Error::Storage(
                "fault injected after child exit before receipt persistence".into(),
            ))
        }
    }

    struct BlockingRunner {
        started: Arc<AtomicBool>,
        release: Arc<AtomicBool>,
    }

    struct CancellationUnknownRunner {
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl ExecutionRunner for CancellationUnknownRunner {
        fn run(&self, _request: &ExecutionSpec) -> Result<RunnerOutcome> {
            Ok(RunnerOutcome::Unknown {
                reason: "cancellation test runner was not admitted with a signal".into(),
            })
        }

        fn run_with_cancellation(
            &self,
            _request: &ExecutionSpec,
            cancellation: &ExecutionCancellation,
        ) -> Result<RunnerOutcome> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            while !cancellation.is_cancelled() {
                thread::sleep(Duration::from_millis(1));
            }
            Ok(RunnerOutcome::Unknown {
                reason: "cancellation left the external outcome uncertain".into(),
            })
        }
    }

    impl ExecutionRunner for BlockingRunner {
        fn run(&self, _request: &ExecutionSpec) -> Result<RunnerOutcome> {
            self.started.store(true, Ordering::Release);
            while !self.release.load(Ordering::Acquire) {
                thread::sleep(Duration::from_millis(1));
            }
            Ok(RunnerOutcome::Exited {
                status_code: Some(0),
                stdout: Vec::new(),
                stderr: Vec::new(),
            })
        }
    }

    fn content_fixture(approval: &ExecutionApproval) -> Result<(Arc<MemoryContent>, FileRef)> {
        let volume = VolumeRef::new(
            ProviderRef::new("test", "filesystem", "2")?,
            "private",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([7; 16])),
        )?;
        let mut approval = approval.clone();
        approval.bind_request_location(&volume, "requests/approved.json")?;
        let bytes =
            serde_json::to_vec(&approval).map_err(|error| Error::Invalid(error.to_string()))?;
        let descriptor = FileDescriptor::from_bytes(&bytes, "application/json")?;
        let reference = FileRef::new(
            volume.clone(),
            "requests/approved.json",
            "generation-1",
            descriptor,
            "approved.json",
        )?;
        Ok((
            Arc::new(MemoryContent {
                volume,
                request: bytes,
                staged: Mutex::new(Vec::new()),
            }),
            reference,
        ))
    }

    #[test]
    fn operator_resolution_requires_exact_grant_issuer_audience_and_identity() -> Result<()> {
        let session_id = SessionId::from_bytes([3; 16]);
        let operation_id = OperationId::from_bytes([4; 16]);
        let volume = VolumeRef::new(
            ProviderRef::new("test", "filesystem", "2")?,
            "private",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([7; 16])),
        )?;
        let audience = Authority {
            kind: AggregateKind::Session,
            id: "session-audience".into(),
        };
        let issuer = AuthorityIssuer::new("configured-issuer", [9; 32], audience.clone());
        let target =
            ExecutionResolutionCapability::capability_for(session_id, &volume, operation_id)?;
        let exact = issuer.root(
            "operator-exact",
            Capabilities::new(vec!["execution:resolve".to_owned(), target.clone()]),
        );
        assert!(
            ExecutionResolutionCapability::authenticate(
                &issuer.verifier(),
                &exact,
                session_id,
                &volume,
                operation_id,
            )
            .is_ok()
        );

        let missing_capability = issuer.root(
            "operator-missing-operation",
            Capabilities::new(["execution:resolve"]),
        );
        assert!(
            ExecutionResolutionCapability::authenticate(
                &issuer.verifier(),
                &missing_capability,
                session_id,
                &volume,
                operation_id,
            )
            .is_err()
        );

        let foreign_issuer = AuthorityIssuer::new("foreign-issuer", [10; 32], audience.clone());
        let foreign_scope = foreign_issuer.root(
            "operator-foreign-issuer",
            Capabilities::new(vec!["execution:resolve".to_owned(), target.clone()]),
        );
        assert!(
            ExecutionResolutionCapability::authenticate(
                &issuer.verifier(),
                &foreign_scope,
                session_id,
                &volume,
                operation_id,
            )
            .is_err()
        );

        let foreign_audience = AuthorityIssuer::new(
            "configured-issuer",
            [9; 32],
            Authority {
                kind: AggregateKind::Session,
                id: "different-session-audience".into(),
            },
        );
        let wrong_audience = foreign_audience.root(
            "operator-foreign-audience",
            Capabilities::new(vec!["execution:resolve".to_owned(), target]),
        );
        assert!(
            ExecutionResolutionCapability::authenticate(
                &issuer.verifier(),
                &wrong_audience,
                session_id,
                &volume,
                operation_id,
            )
            .is_err()
        );

        let other_session = SessionId::from_bytes([5; 16]);
        let other_session_target =
            ExecutionResolutionCapability::capability_for(other_session, &volume, operation_id)?;
        let other_session_scope = issuer.root(
            "operator-cross-session",
            Capabilities::new([other_session_target]),
        );
        assert!(
            ExecutionResolutionCapability::authenticate(
                &issuer.verifier(),
                &other_session_scope,
                session_id,
                &volume,
                operation_id,
            )
            .is_err()
        );

        let other_volume = VolumeRef::new(
            ProviderRef::new("test", "filesystem", "2")?,
            "other-private",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([7; 16])),
        )?;
        assert!(
            ExecutionResolutionCapability::authenticate(
                &issuer.verifier(),
                &exact,
                session_id,
                &other_volume,
                operation_id,
            )
            .is_err()
        );
        Ok(())
    }

    fn spec() -> ExecutionSpec {
        let executable = if cfg!(windows) {
            std::env::var("ComSpec").unwrap_or_else(|_| r"C:\Windows\System32\cmd.exe".into())
        } else {
            "/bin/sh".into()
        };
        let working_directory = std::env::current_dir()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        ExecutionSpec {
            executable,
            arguments: Vec::new(),
            working_directory,
            environment: ExecutionEnvironment::Clear,
            timeout_ms: Some(10_000),
            max_output_bytes: 4096,
        }
    }

    #[test]
    fn approval_digest_binds_every_process_field() -> Result<()> {
        let operation = OperationId::from_bytes([1; 16]);
        let mut request = spec();
        let mut environment = BTreeMap::new();
        environment.insert("GRAPH_CODER_APPROVAL_KEY".into(), "exact".into());
        request.environment = ExecutionEnvironment::explicit(environment)?;
        let approval = ExecutionApproval::approve(operation, request.clone())?;
        approval.validate()?;
        let mut changed = approval.clone();
        changed.request.arguments.push("changed".into());
        assert!(matches!(changed.validate(), Err(Error::Conflict(_))));
        let mut changed_environment = approval.clone();
        if let ExecutionEnvironment::Explicit { variables } =
            &mut changed_environment.request.environment
        {
            variables.insert("GRAPH_CODER_APPROVAL_KEY".into(), "changed".into());
        }
        assert!(matches!(
            changed_environment.validate(),
            Err(Error::Conflict(_))
        ));
        let mut changed_timeout = approval.clone();
        changed_timeout.request.timeout_ms = Some(1);
        assert!(matches!(
            changed_timeout.validate(),
            Err(Error::Conflict(_))
        ));
        let mut changed_output = approval.clone();
        changed_output.request.max_output_bytes = 1;
        assert!(matches!(changed_output.validate(), Err(Error::Conflict(_))));
        let mut changed_directory = approval.clone();
        changed_directory.request.working_directory.push('x');
        assert!(matches!(
            changed_directory.validate(),
            Err(Error::Conflict(_))
        ));
        let mut relative = request;
        relative.executable = "cmd.exe".into();
        assert!(relative.digest().is_err());
        Ok(())
    }

    #[test]
    fn timeout_and_unknown_reasons_are_bounded_without_unicode_corruption() -> Result<()> {
        let mut request = spec();
        request.timeout_ms = Some(u64::MAX);
        assert!(
            matches!(request.validate(), Err(Error::Invalid(message)) if message.contains("timeout"))
        );

        let receipt = NativeExecutionProvider::bounded_unknown("🦀".repeat(MAX_FAILURE_BYTES));
        receipt.validate()?;
        if let ExecutionReceipt::Unknown { reason } = receipt {
            assert!(reason.len() <= MAX_FAILURE_BYTES);
            assert!(std::str::from_utf8(reason.as_bytes()).is_ok());
        } else {
            unreachable!("bounded unknown must preserve its receipt kind");
        }
        Ok(())
    }

    #[test]
    fn native_runner_clears_host_environment_and_captures_real_output() -> Result<()> {
        let sentinel = if cfg!(windows) { "SystemRoot" } else { "PATH" };
        assert!(std::env::var_os(sentinel).is_some());
        let mut request = spec();
        if cfg!(windows) {
            request.arguments = vec![
                "/C".into(),
                format!("if defined {sentinel} (exit /b 7) else (echo graphcoder-approved)"),
            ];
        } else {
            request.arguments = vec![
                "-c".into(),
                format!(
                    "if [ -n \"${sentinel}:-\" ]; then exit 7; else printf graphcoder-approved; fi"
                ),
            ];
        }
        let output = NativeExecutionRunner.run(&request)?;
        let expected = if cfg!(windows) {
            b"graphcoder-approved\r\n".to_vec()
        } else {
            b"graphcoder-approved".to_vec()
        };
        assert_eq!(
            output,
            RunnerOutcome::Exited {
                status_code: Some(0),
                stdout: expected,
                stderr: Vec::new()
            }
        );
        Ok(())
    }

    #[test]
    fn native_runner_passes_only_explicit_environment_values() -> Result<()> {
        const SENTINEL: &str = "GRAPH_CODER_EXPLICIT_SENTINEL_8A4D";
        let mut request = spec();
        let mut variables = BTreeMap::new();
        variables.insert(SENTINEL.into(), "exact-value".into());
        request.environment = ExecutionEnvironment::explicit(variables)?;
        if cfg!(windows) {
            request.arguments = vec!["/C".into(), format!("echo %{SENTINEL}%")];
        } else {
            request.arguments = vec!["-c".into(), format!("printf \"%${SENTINEL}\"")];
        }
        let output = NativeExecutionRunner.run(&request)?;
        let expected = if cfg!(windows) {
            b"exact-value\r\n".to_vec()
        } else {
            b"exact-value".to_vec()
        };
        assert_eq!(
            output,
            RunnerOutcome::Exited {
                status_code: Some(0),
                stdout: expected,
                stderr: Vec::new()
            }
        );
        Ok(())
    }

    #[test]
    fn native_runner_times_out_without_claiming_success() -> Result<()> {
        let mut request = spec();
        request.timeout_ms = Some(100);
        if cfg!(windows) {
            request.executable = format!(
                r"{}\System32\ping.exe",
                std::env::var("SystemRoot").unwrap_or_else(|_| r"C:\Windows".into())
            );
            request.arguments = vec!["-n".into(), "10".into(), "127.0.0.1".into()];
        } else {
            request.arguments = vec!["-c".into(), "sleep 1".into()];
        }
        let outcome = NativeExecutionRunner.run(&request)?;
        assert!(
            matches!(outcome, RunnerOutcome::TimedOut { .. })
                || (!cfg!(feature = "native-process-tree")
                    && matches!(outcome, RunnerOutcome::Unknown { ref reason } if reason.contains("process-tree support")))
        );
        Ok(())
    }

    #[test]
    fn native_runner_rejects_output_overflow_before_publication() -> Result<()> {
        let mut request = spec();
        request.max_output_bytes = 16;
        if cfg!(windows) {
            request.arguments = vec!["/C".into(), "echo 12345678901234567890".into()];
        } else {
            request.arguments = vec!["-c".into(), "printf 12345678901234567890".into()];
        }
        assert!(matches!(
            NativeExecutionRunner.run(&request),
            Err(Error::Invalid(_))
        ));
        Ok(())
    }

    #[test]
    fn native_runner_rechecks_overflow_after_fast_process_exit() -> Result<()> {
        for _ in 0..64 {
            let mut request = spec();
            request.max_output_bytes = 1;
            if cfg!(windows) {
                request.arguments = vec!["/C".into(), "echo overflow".into()];
            } else {
                request.arguments = vec!["-c".into(), "printf overflow".into()];
            }
            assert!(matches!(
                NativeExecutionRunner.run(&request),
                Err(Error::Invalid(message)) if message.contains("output")
            ));
        }
        Ok(())
    }

    /// The direct Windows adapter must classify a parent that exits while a
    /// hidden descendant still owns the inherited output pipe as uncertain.
    /// The process-tree adapter owns descendants and has a separate cleanup
    /// contract, so this fixture targets the direct native path explicitly.
    #[cfg(all(windows, not(feature = "native-process-tree")))]
    #[test]
    fn native_runner_reports_unknown_for_hidden_descendant_held_pipe() -> Result<()> {
        let marker = std::env::temp_dir().join(format!(
            "graphcoder-held-pipe-{}.marker",
            OperationId::new()
        ));
        let mut request = spec();
        request.timeout_ms = Some(10_000);
        request.executable =
            std::env::var("ComSpec").unwrap_or_else(|_| r"C:\Windows\System32\cmd.exe".into());
        request.arguments = vec![
            "/D".into(),
            "/S".into(),
            "/C".into(),
            format!(
                r#"echo marker>>"{}" & start "" /B powershell.exe -NoProfile -NonInteractive -WindowStyle Hidden -Command "Start-Sleep -Seconds 3""#,
                marker.display()
            ),
        ];
        let outcome = NativeExecutionRunner.run(&request)?;
        assert!(matches!(
            outcome,
            RunnerOutcome::Unknown { ref reason }
                if reason.contains("retained output handles")
        ));
        std::thread::sleep(Duration::from_secs(4));
        let markers = std::fs::read_to_string(&marker).map_err(|error| {
            Error::Storage(format!("held-pipe marker was not written: {error}"))
        })?;
        assert_eq!(markers.lines().count(), 1);
        std::fs::remove_file(marker).map_err(|error| Error::Storage(error.to_string()))?;
        Ok(())
    }

    #[test]
    fn native_runner_cancellation_kills_a_running_process() -> Result<()> {
        let mut request = spec();
        request.timeout_ms = Some(10_000);
        if cfg!(windows) {
            request.executable = format!(
                r"{}\System32\ping.exe",
                std::env::var("SystemRoot").unwrap_or_else(|_| r"C:\Windows".into())
            );
            request.arguments = vec!["-n".into(), "30".into(), "127.0.0.1".into()];
        } else {
            request.arguments = vec!["-c".into(), "sleep 5".into()];
        }
        let cancellation = ExecutionCancellation::new();
        let signal = cancellation.clone();
        let handle =
            thread::spawn(move || NativeExecutionRunner.run_with_cancellation(&request, &signal));
        cancellation.cancel();
        let outcome = handle.join().expect("runner thread panicked")?;
        assert!(
            matches!(outcome, RunnerOutcome::Cancelled { .. })
                || (!cfg!(feature = "native-process-tree")
                    && matches!(outcome, RunnerOutcome::Unknown { ref reason } if reason.contains("process-tree support")))
        );
        Ok(())
    }

    #[tokio::test]
    async fn provider_stages_success_and_never_retries_unknown_outcomes() -> Result<()> {
        let operation = OperationId::from_bytes([13; 16]);
        let approval = ExecutionApproval::approve(operation, spec())?;
        let (content, request_file) = content_fixture(&approval)?;
        let provider = NativeExecutionProvider::new(
            content.clone(),
            content.clone(),
            Arc::new(FixedRunner(RunnerOutcome::Exited {
                status_code: Some(0),
                stdout: b"ok".to_vec(),
                stderr: Vec::new(),
            })),
            approval_verifier(),
        )?;
        let request_digest = crate::core::effect_request_digest(
            provider.id(),
            EffectGuarantee::AtMostOnce,
            "host.process",
            &request_file,
        )?;
        let dispatch = EffectDispatch {
            provider: provider.id().into(),
            effect_id: EffectId::from_bytes(operation.into_bytes()),
            attempt_id: EffectAttemptId::from_bytes([14; 16]),
            effect_kind: "host.process".into(),
            request: request_file,
            guarantee: EffectGuarantee::AtMostOnce,
            request_digest,
        };
        let observation = provider.dispatch(dispatch).await?;
        assert!(matches!(observation.status, EffectStatus::Succeeded { .. }));
        let staged = content.staged.lock().unwrap();
        let receipt: ExecutionReceipt = serde_json::from_slice(staged.first().unwrap()).unwrap();
        assert_eq!(
            receipt,
            ExecutionReceipt::Succeeded {
                status_code: 0,
                stdout: b"ok".to_vec(),
                stderr: Vec::new()
            }
        );
        drop(staged);

        let approval = ExecutionApproval::approve(operation, spec())?;
        let (unknown_content, unknown_request) = content_fixture(&approval)?;
        let unknown = NativeExecutionProvider::new(
            unknown_content.clone(),
            unknown_content.clone(),
            Arc::new(FixedRunner(RunnerOutcome::Unknown {
                reason: "host restarted".into(),
            })),
            approval_verifier(),
        )?;
        let unknown_request_digest = crate::core::effect_request_digest(
            unknown.id(),
            EffectGuarantee::AtMostOnce,
            "host.process",
            &unknown_request,
        )?;
        let observation = unknown
            .dispatch(EffectDispatch {
                provider: unknown.id().into(),
                effect_id: EffectId::from_bytes(operation.into_bytes()),
                attempt_id: EffectAttemptId::from_bytes([16; 16]),
                effect_kind: "host.process".into(),
                request: unknown_request.clone(),
                guarantee: EffectGuarantee::AtMostOnce,
                request_digest: unknown_request_digest,
            })
            .await?;
        assert_eq!(observation.status, EffectStatus::Indeterminate);
        let unknown_staged = unknown_content.staged.lock().unwrap();
        assert_eq!(unknown_staged.len(), 1);
        let unknown_receipt: ExecutionReceipt =
            serde_json::from_slice(unknown_staged.first().unwrap()).unwrap();
        assert_eq!(
            unknown_receipt,
            ExecutionReceipt::Unknown {
                reason: "host restarted".into()
            }
        );
        drop(unknown_staged);
        assert!(
            unknown
                .reconcile(EffectAttemptId::from_bytes([16; 16]))
                .await?
                .is_none()
        );

        let denied = ExecutionApproval::deny(operation, spec(), "owner declined")?;
        let (denied_content, denied_request) = content_fixture(&denied)?;
        let denied_provider = NativeExecutionProvider::new(
            denied_content.clone(),
            denied_content.clone(),
            Arc::new(FixedRunner(RunnerOutcome::Unknown {
                reason: "must not run".into(),
            })),
            approval_verifier(),
        )?;
        let denied_request_digest = crate::core::effect_request_digest(
            denied_provider.id(),
            EffectGuarantee::AtMostOnce,
            "host.process",
            &denied_request,
        )?;
        let observation = denied_provider
            .dispatch(EffectDispatch {
                provider: denied_provider.id().into(),
                effect_id: EffectId::from_bytes(operation.into_bytes()),
                attempt_id: EffectAttemptId::from_bytes([18; 16]),
                effect_kind: "host.process".into(),
                request: denied_request,
                guarantee: EffectGuarantee::AtMostOnce,
                request_digest: denied_request_digest,
            })
            .await?;
        assert!(matches!(
            observation.status,
            EffectStatus::FailedWithReceipt { ref message, .. } if message.contains("owner declined")
        ));
        let staged = denied_content.staged.lock().unwrap();
        let receipt: ExecutionReceipt = serde_json::from_slice(staged.first().unwrap()).unwrap();
        assert_eq!(
            receipt,
            ExecutionReceipt::Denied {
                reason: "owner declined".into()
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn provider_rejects_unauthenticated_approval_before_running() -> Result<()> {
        let operation = OperationId::from_bytes([51; 16]);
        let approval = ExecutionApproval::approve(operation, spec())?;
        let (content, request_file) = content_fixture(&approval)?;
        let provider = NativeExecutionProvider::new(
            content.clone(),
            content.clone(),
            Arc::new(FixedRunner(RunnerOutcome::Exited {
                status_code: Some(0),
                stdout: b"must not run".to_vec(),
                stderr: Vec::new(),
            })),
            Arc::new(RejectApprovalVerifier),
        )?;
        let request_digest = crate::core::effect_request_digest(
            provider.id(),
            EffectGuarantee::AtMostOnce,
            "host.process",
            &request_file,
        )?;
        let result = provider
            .dispatch(EffectDispatch {
                provider: provider.id().into(),
                effect_id: EffectId::from_bytes(operation.into_bytes()),
                attempt_id: EffectAttemptId::from_bytes([52; 16]),
                effect_kind: "host.process".into(),
                request: request_file,
                guarantee: EffectGuarantee::AtMostOnce,
                request_digest,
            })
            .await;
        assert!(matches!(result, Err(Error::Unauthorized(_))));
        assert!(content.staged.lock().unwrap().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn provider_persists_unknown_runner_failure_without_redispatch() -> Result<()> {
        let operation = OperationId::from_bytes([53; 16]);
        let approval = ExecutionApproval::approve(operation, spec())?;
        let (content, request_file) = content_fixture(&approval)?;
        let provider = NativeExecutionProvider::new(
            content.clone(),
            content.clone(),
            Arc::new(FaultAfterExitRunner),
            approval_verifier(),
        )?;
        let request_digest = crate::core::effect_request_digest(
            provider.id(),
            EffectGuarantee::AtMostOnce,
            "host.process",
            &request_file,
        )?;
        let dispatch = EffectDispatch {
            provider: provider.id().into(),
            effect_id: EffectId::from_bytes(operation.into_bytes()),
            attempt_id: EffectAttemptId::from_bytes([54; 16]),
            effect_kind: "host.process".into(),
            request: request_file.clone(),
            guarantee: EffectGuarantee::AtMostOnce,
            request_digest,
        };
        let first = provider.dispatch(dispatch.clone()).await?;
        assert_eq!(first.status, EffectStatus::Indeterminate);
        let receipt: ExecutionReceipt =
            serde_json::from_slice(content.staged.lock().unwrap().first().unwrap()).unwrap();
        assert!(matches!(receipt, ExecutionReceipt::Unknown { .. }));

        let restarted = NativeExecutionProvider::new(
            content.clone(),
            content.clone(),
            Arc::new(FixedRunner(RunnerOutcome::Exited {
                status_code: Some(0),
                stdout: b"must not rerun".to_vec(),
                stderr: Vec::new(),
            })),
            approval_verifier(),
        )?;
        let second = restarted.dispatch(dispatch).await?;
        assert_eq!(second.status, EffectStatus::Indeterminate);
        assert_eq!(content.staged.lock().unwrap().len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn typed_receipt_store_replays_only_the_bound_dispatch() -> Result<()> {
        let operation = OperationId::from_bytes([55; 16]);
        let approval = ExecutionApproval::approve(operation, spec())?;
        let (content, request_file) = content_fixture(&approval)?;
        let store = Arc::new(MemoryReceiptStore {
            volume: Some(content.volume.clone()),
            state: Mutex::new(MemoryReceiptState::default()),
        });
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let provider = NativeExecutionProvider::new_with_receipt_store(
            content.clone(),
            store.clone(),
            Arc::new(CountingFixedRunner {
                calls: Arc::clone(&calls),
                outcome: RunnerOutcome::Exited {
                    status_code: Some(0),
                    stdout: b"typed".to_vec(),
                    stderr: Vec::new(),
                },
            }),
            approval_verifier(),
        )?;
        let request_digest = crate::core::effect_request_digest(
            provider.id(),
            EffectGuarantee::AtMostOnce,
            "host.process",
            &request_file,
        )?;
        let dispatch = EffectDispatch {
            provider: provider.id().into(),
            effect_id: EffectId::from_bytes(operation.into_bytes()),
            attempt_id: EffectAttemptId::from_bytes([56; 16]),
            effect_kind: "host.process".into(),
            request: request_file.clone(),
            guarantee: EffectGuarantee::AtMostOnce,
            request_digest,
        };
        assert!(matches!(
            provider.dispatch(dispatch.clone()).await?.status,
            EffectStatus::Succeeded { .. }
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(matches!(
            provider
                .reconcile(dispatch.attempt_id)
                .await?
                .map(|observation| observation.status),
            Some(EffectStatus::Succeeded { .. })
        ));

        let replay_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let restarted = NativeExecutionProvider::new_with_receipt_store(
            content,
            store,
            Arc::new(CountingFixedRunner {
                calls: Arc::clone(&replay_calls),
                outcome: RunnerOutcome::Unknown {
                    reason: "must not rerun".into(),
                },
            }),
            approval_verifier(),
        )?;
        let replay = restarted.dispatch(dispatch).await?;
        assert!(matches!(replay.status, EffectStatus::Succeeded { .. }));
        assert_eq!(replay_calls.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn typed_store_claim_blocks_cross_provider_redispatch() -> Result<()> {
        let operation = OperationId::from_bytes([57; 16]);
        let approval = ExecutionApproval::approve(operation, spec())?;
        let (content, request_file) = content_fixture(&approval)?;
        let store = Arc::new(MemoryReceiptStore {
            volume: Some(content.volume.clone()),
            state: Mutex::new(MemoryReceiptState::default()),
        });
        let started = Arc::new(AtomicBool::new(false));
        let release = Arc::new(AtomicBool::new(false));
        let first = Arc::new(NativeExecutionProvider::new_with_receipt_store(
            content.clone(),
            store.clone(),
            Arc::new(BlockingRunner {
                started: Arc::clone(&started),
                release: Arc::clone(&release),
            }),
            approval_verifier(),
        )?);
        let second = NativeExecutionProvider::new_with_receipt_store(
            content,
            store,
            Arc::new(FixedRunner(RunnerOutcome::Exited {
                status_code: Some(0),
                stdout: b"must not rerun".to_vec(),
                stderr: Vec::new(),
            })),
            approval_verifier(),
        )?;
        let request_digest = crate::core::effect_request_digest(
            first.id(),
            EffectGuarantee::AtMostOnce,
            "host.process",
            &request_file,
        )?;
        let dispatch = EffectDispatch {
            provider: first.id().into(),
            effect_id: EffectId::from_bytes(operation.into_bytes()),
            attempt_id: EffectAttemptId::from_bytes([58; 16]),
            effect_kind: "host.process".into(),
            request: request_file,
            guarantee: EffectGuarantee::AtMostOnce,
            request_digest,
        };
        let task = tokio::spawn({
            let first = Arc::clone(&first);
            let dispatch = dispatch.clone();
            async move { first.dispatch(dispatch).await }
        });
        for _ in 0..2_000 {
            if started.load(Ordering::Acquire) {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(started.load(Ordering::Acquire));
        assert!(matches!(
            second.dispatch(dispatch).await,
            Err(Error::Indeterminate(_))
        ));
        release.store(true, Ordering::Release);
        assert!(matches!(
            task.await
                .map_err(|error| Error::Storage(error.to_string()))??
                .status,
            EffectStatus::Succeeded { .. }
        ));
        Ok(())
    }

    #[tokio::test]
    async fn durable_cancellation_intent_blocks_restart_redispatch() -> Result<()> {
        let operation = OperationId::from_bytes([65; 16]);
        let approval = ExecutionApproval::approve(operation, spec())?;
        let (content, request_file) = content_fixture(&approval)?;
        let store = Arc::new(MemoryReceiptStore {
            volume: Some(content.volume.clone()),
            state: Mutex::new(MemoryReceiptState::default()),
        });
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let provider = Arc::new(NativeExecutionProvider::new_with_receipt_store(
            content.clone(),
            store.clone(),
            Arc::new(CancellationUnknownRunner {
                calls: Arc::clone(&calls),
            }),
            approval_verifier(),
        )?);
        let request_digest = crate::core::effect_request_digest(
            provider.id(),
            EffectGuarantee::AtMostOnce,
            "host.process",
            &request_file,
        )?;
        let dispatch = EffectDispatch {
            provider: provider.id().into(),
            effect_id: EffectId::from_bytes(operation.into_bytes()),
            attempt_id: EffectAttemptId::from_bytes([66; 16]),
            effect_kind: "host.process".into(),
            request: request_file.clone(),
            guarantee: EffectGuarantee::AtMostOnce,
            request_digest,
        };
        let task = tokio::spawn({
            let provider = Arc::clone(&provider);
            let dispatch = dispatch.clone();
            async move { provider.dispatch(dispatch).await }
        });
        for _ in 0..2_000 {
            if calls.load(Ordering::Acquire) != 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(calls.load(Ordering::Acquire) != 0);
        assert!(provider.cancel_and_persist(operation).await?);
        assert_eq!(
            task.await
                .map_err(|error| Error::Storage(error.to_string()))??
                .status,
            EffectStatus::Indeterminate
        );

        let replay_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let restarted = NativeExecutionProvider::new_with_receipt_store(
            content,
            store,
            Arc::new(CountingFixedRunner {
                calls: Arc::clone(&replay_calls),
                outcome: RunnerOutcome::Exited {
                    status_code: Some(0),
                    stdout: b"must not rerun".to_vec(),
                    stderr: Vec::new(),
                },
            }),
            approval_verifier(),
        )?;
        assert!(matches!(
            restarted.dispatch(dispatch).await,
            Err(Error::Indeterminate(_))
        ));
        assert_eq!(replay_calls.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn provider_cancellation_is_nonblocking_and_persisted_as_a_receipt() -> Result<()> {
        let operation = OperationId::from_bytes([31; 16]);
        let mut request = spec();
        request.timeout_ms = Some(10_000);
        if cfg!(windows) {
            request.executable = format!(
                r"{}\System32\ping.exe",
                std::env::var("SystemRoot").unwrap_or_else(|_| r"C:\Windows".into())
            );
            request.arguments = vec!["-n".into(), "30".into(), "127.0.0.1".into()];
        } else {
            request.arguments = vec!["-c".into(), "sleep 5".into()];
        }
        let approval = ExecutionApproval::approve(operation, request)?;
        let (content, request_file) = content_fixture(&approval)?;
        let provider = Arc::new(NativeExecutionProvider::new(
            content.clone(),
            content.clone(),
            Arc::new(NativeExecutionRunner),
            approval_verifier(),
        )?);
        let request_digest = crate::core::effect_request_digest(
            provider.id(),
            EffectGuarantee::AtMostOnce,
            "host.process",
            &request_file,
        )?;
        let dispatch = EffectDispatch {
            provider: provider.id().into(),
            effect_id: EffectId::from_bytes(operation.into_bytes()),
            attempt_id: EffectAttemptId::from_bytes([32; 16]),
            effect_kind: "host.process".into(),
            request: request_file,
            guarantee: EffectGuarantee::AtMostOnce,
            request_digest,
        };
        let task = tokio::spawn({
            let provider = Arc::clone(&provider);
            async move { provider.dispatch(dispatch).await }
        });
        let mut cancelled = false;
        for _ in 0..100 {
            if provider.cancel(operation) {
                cancelled = true;
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            cancelled,
            "dispatch never exposed its active cancellation token"
        );
        let observation = task
            .await
            .map_err(|error| Error::Storage(error.to_string()))??;
        assert!(matches!(
            observation.status,
            EffectStatus::FailedWithReceipt { ref message, .. } if message.contains("cancelled")
        ));
        assert_eq!(content.staged.lock().unwrap().len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn provider_rejects_concurrent_attempts_for_one_operation() -> Result<()> {
        let operation = OperationId::from_bytes([61; 16]);
        let approval = ExecutionApproval::approve(operation, spec())?;
        let (content, request_file) = content_fixture(&approval)?;
        let started = Arc::new(AtomicBool::new(false));
        let release = Arc::new(AtomicBool::new(false));
        let provider = Arc::new(NativeExecutionProvider::new(
            content.clone(),
            content.clone(),
            Arc::new(BlockingRunner {
                started: Arc::clone(&started),
                release: Arc::clone(&release),
            }),
            approval_verifier(),
        )?);
        let request_digest = crate::core::effect_request_digest(
            provider.id(),
            EffectGuarantee::AtMostOnce,
            "host.process",
            &request_file,
        )?;
        let first = EffectDispatch {
            provider: provider.id().into(),
            effect_id: EffectId::from_bytes(operation.into_bytes()),
            attempt_id: EffectAttemptId::from_bytes([62; 16]),
            effect_kind: "host.process".into(),
            request: request_file.clone(),
            guarantee: EffectGuarantee::AtMostOnce,
            request_digest,
        };
        let second = EffectDispatch {
            attempt_id: EffectAttemptId::from_bytes([63; 16]),
            ..first.clone()
        };
        let task = tokio::spawn({
            let provider = Arc::clone(&provider);
            async move { provider.dispatch(first).await }
        });
        for _ in 0..2_000 {
            if started.load(Ordering::Acquire) {
                break;
            }
            tokio::task::yield_now().await;
        }
        if !started.load(Ordering::Acquire) {
            return Err(Error::Storage("blocking runner did not start".into()));
        }
        assert!(
            matches!(provider.dispatch(second).await, Err(Error::Conflict(message)) if message.contains("active attempt"))
        );
        release.store(true, Ordering::Release);
        task.await
            .map_err(|error| Error::Storage(error.to_string()))??;
        Ok(())
    }

    #[test]
    fn operation_identity_is_stable_for_effect_adaptation() {
        let operation = OperationId::from_bytes([9; 16]);
        assert_eq!(
            EffectId::from_bytes(operation.into_bytes()).into_bytes(),
            [9; 16]
        );
    }

    #[allow(dead_code)]
    fn _file_ref_fixture() -> Result<FileRef> {
        FileRef::new(
            VolumeRef::new(
                ProviderRef::new("test", "filesystem", "2")?,
                "private",
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(AgentId::from_bytes([7; 16])),
            )?,
            "request.json",
            "generation-1",
            FileDescriptor::from_bytes(br#"{}"#, "application/json")?,
            "request.json",
        )
    }
}

#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
#[cfg(test)]
mod local_provider_tests {
    use super::*;
    use crate::{
        InteractionId,
        conversation::Limits,
        filesystem::PersistentLocalHarness,
        interaction::{Interaction, InteractionResponse},
        model::{Model, ModelAttempt, ModelEvent, ModelProvider, ModelRequest},
    };
    use futures::{future::BoxFuture, stream::BoxStream};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct NoopModel;

    impl ModelProvider for NoopModel {
        fn generate<'a>(&'a self, _: crate::model_input::PreparedModelInput) -> BoxStream<'a, Result<ModelEvent>> {
            Box::pin(futures::stream::iter([Ok(ModelEvent::Completed {
                metadata: serde_json::Value::Null,
            })]))
        }

        fn reconcile<'a>(
            &'a self,
            _: ModelAttempt,
        ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
            Box::pin(async { Ok(None) })
        }
    }

    struct LocalApprovalVerifier;

    impl ExecutionApprovalVerifier for LocalApprovalVerifier {
        fn verify<'a>(
            &'a self,
            _context: ExecutionApprovalContext<'a>,
            _: &'a ExecutionApproval,
        ) -> BoxFuture<'a, Result<()>> {
            Box::pin(async { Ok(()) })
        }
    }

    fn local_spec() -> ExecutionSpec {
        let executable = if cfg!(windows) {
            std::env::var("ComSpec").unwrap_or_else(|_| r"C:\Windows\System32\cmd.exe".into())
        } else {
            "/bin/sh".into()
        };
        ExecutionSpec {
            executable,
            arguments: Vec::new(),
            working_directory: std::env::current_dir()
                .expect("current directory")
                .to_string_lossy()
                .into_owned(),
            environment: ExecutionEnvironment::Clear,
            timeout_ms: Some(10_000),
            max_output_bytes: 4096,
        }
    }

    #[derive(Clone)]
    struct CountingRunner {
        calls: Arc<AtomicUsize>,
        outcome: RunnerOutcome,
    }

    impl ExecutionRunner for CountingRunner {
        fn run(&self, _: &ExecutionSpec) -> Result<RunnerOutcome> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.outcome.clone())
        }
    }

    #[derive(Clone)]
    struct CountingNativeRunner {
        calls: Arc<AtomicUsize>,
    }

    impl ExecutionRunner for CountingNativeRunner {
        fn run(&self, request: &ExecutionSpec) -> Result<RunnerOutcome> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            NativeExecutionRunner.run(request)
        }
    }

    /// Runs the real native adapter, then loses the publication acknowledgement
    /// so the durable provider must leave its claim indeterminate.
    struct NativeExitThenFaultRunner;

    impl ExecutionRunner for NativeExitThenFaultRunner {
        fn run(&self, request: &ExecutionSpec) -> Result<RunnerOutcome> {
            let _ = NativeExecutionRunner.run(request)?;
            Err(Error::Storage(
                "fault injected after native child exit".into(),
            ))
        }
    }

    struct FailAfterReceiptPublish {
        inner: Arc<dyn ExecutionReceiptStore>,
        fail_after_publish: Arc<AtomicBool>,
    }

    impl ExecutionReceiptStore for FailAfterReceiptPublish {
        fn claim<'a>(
            &'a self,
            key: &'a ExecutionReceiptKey,
        ) -> futures::future::BoxFuture<'a, Result<ExecutionClaim>> {
            self.inner.claim(key)
        }

        fn load<'a>(
            &'a self,
            key: &'a ExecutionReceiptKey,
        ) -> futures::future::BoxFuture<'a, Result<Option<ExecutionReceiptRecord>>> {
            self.inner.load(key)
        }

        fn load_attempt<'a>(
            &'a self,
            attempt_id: EffectAttemptId,
        ) -> futures::future::BoxFuture<'a, Result<Option<ExecutionReceiptRecord>>> {
            self.inner.load_attempt(attempt_id)
        }

        fn publish<'a>(
            &'a self,
            key: &'a ExecutionReceiptKey,
            handle: &'a ExecutionClaimHandle,
            receipt: &'a ExecutionReceipt,
        ) -> futures::future::BoxFuture<'a, Result<FileRef>> {
            Box::pin(async move {
                let reference = self.inner.publish(key, handle, receipt).await?;
                if self.fail_after_publish.swap(false, Ordering::SeqCst) {
                    return Err(Error::Storage(
                        "fault injected after durable receipt publication".into(),
                    ));
                }
                Ok(reference)
            })
        }
    }

    #[tokio::test]
    async fn durable_local_receipt_is_reconciled_without_redispatch() -> Result<()> {
        let root = std::env::temp_dir().join(format!("harness-execution-{}", OperationId::new()));
        let model = Model::new("mock", "execution", "1", serde_json::Value::Null)?;
        let session =
            PersistentLocalHarness::open(&root, model, Arc::new(NoopModel), Limits::default())
                .await?;
        let operation = OperationId::from_bytes([41; 16]);
        let mut approval = ExecutionApproval::approve(operation, local_spec())?;
        approval.bind_request_location(session.storage().volume(), "requests/approved.json")?;
        let approval_bytes =
            serde_json::to_vec(&approval).map_err(|error| Error::Invalid(error.to_string()))?;
        let request_file = session
            .storage()
            .stage(
                operation,
                "requests/approved.json",
                &approval_bytes,
                "application/json",
                "approved.json",
            )
            .await?;
        let calls = Arc::new(AtomicUsize::new(0));
        let receipt_store: Arc<dyn ExecutionReceiptStore> = session.execution_receipt_store()?;
        let fail_after_publish = Arc::new(AtomicBool::new(true));
        let provider = NativeExecutionProvider::new_with_receipt_store(
            session.storage().content_verifier(),
            Arc::new(FailAfterReceiptPublish {
                inner: Arc::clone(&receipt_store),
                fail_after_publish,
            }),
            Arc::new(CountingNativeRunner {
                calls: Arc::clone(&calls),
            }),
            Arc::new(LocalApprovalVerifier),
        )?;
        let request_digest = crate::core::effect_request_digest(
            provider.id(),
            EffectGuarantee::AtMostOnce,
            "host.process",
            &request_file,
        )?;
        let dispatch = EffectDispatch {
            provider: provider.id().into(),
            effect_id: EffectId::from_bytes(operation.into_bytes()),
            attempt_id: EffectAttemptId::from_bytes([42; 16]),
            effect_kind: "host.process".into(),
            request: request_file.clone(),
            guarantee: EffectGuarantee::AtMostOnce,
            request_digest,
        };
        assert!(matches!(
            provider.dispatch(dispatch.clone()).await,
            Err(Error::Storage(_))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let second_calls = Arc::new(AtomicUsize::new(0));
        let restarted = NativeExecutionProvider::new_with_receipt_store(
            session.storage().content_verifier(),
            session.execution_receipt_store()?,
            Arc::new(CountingRunner {
                calls: Arc::clone(&second_calls),
                outcome: RunnerOutcome::Unknown {
                    reason: "redispatch must not happen".into(),
                },
            }),
            Arc::new(LocalApprovalVerifier),
        )?;
        let second = restarted.dispatch(dispatch).await?;
        assert!(matches!(second.status, EffectStatus::Succeeded { .. }));
        assert_eq!(second_calls.load(Ordering::SeqCst), 0);

        let unknown_operation = OperationId::from_bytes([43; 16]);
        let unknown_approval = ExecutionApproval::approve(unknown_operation, local_spec())?;
        let unknown_bytes = serde_json::to_vec(&unknown_approval)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let unknown_request = session
            .storage()
            .stage(
                unknown_operation,
                "requests/unknown.json",
                &unknown_bytes,
                "application/json",
                "unknown.json",
            )
            .await?;
        let unknown_calls = Arc::new(AtomicUsize::new(0));
        let unknown_provider = NativeExecutionProvider::new(
            session.storage().content_verifier(),
            session.storage().content_publisher(),
            Arc::new(CountingRunner {
                calls: Arc::clone(&unknown_calls),
                outcome: RunnerOutcome::Unknown {
                    reason: "native host restarted".into(),
                },
            }),
            Arc::new(LocalApprovalVerifier),
        )?;
        let unknown_digest = crate::core::effect_request_digest(
            unknown_provider.id(),
            EffectGuarantee::AtMostOnce,
            "host.process",
            &unknown_request,
        )?;
        let unknown_dispatch = EffectDispatch {
            provider: unknown_provider.id().into(),
            effect_id: EffectId::from_bytes(unknown_operation.into_bytes()),
            attempt_id: EffectAttemptId::from_bytes([44; 16]),
            effect_kind: "host.process".into(),
            request: unknown_request.clone(),
            guarantee: EffectGuarantee::AtMostOnce,
            request_digest: unknown_digest,
        };
        let unknown_observation = unknown_provider.dispatch(unknown_dispatch.clone()).await?;
        assert_eq!(unknown_observation.status, EffectStatus::Indeterminate);
        assert_eq!(unknown_calls.load(Ordering::SeqCst), 1);
        let unknown_restarted_calls = Arc::new(AtomicUsize::new(0));
        let unknown_restarted = NativeExecutionProvider::new(
            session.storage().content_verifier(),
            session.storage().content_publisher(),
            Arc::new(CountingRunner {
                calls: Arc::clone(&unknown_restarted_calls),
                outcome: RunnerOutcome::Exited {
                    status_code: Some(0),
                    stdout: b"must not rerun".to_vec(),
                    stderr: Vec::new(),
                },
            }),
            Arc::new(LocalApprovalVerifier),
        )?;
        let unknown_replay = unknown_restarted.dispatch(unknown_dispatch).await?;
        assert_eq!(unknown_replay.status, EffectStatus::Indeterminate);
        assert_eq!(unknown_restarted_calls.load(Ordering::SeqCst), 0);
        std::fs::remove_dir_all(root).map_err(|error| Error::Storage(error.to_string()))?;
        Ok(())
    }

    #[tokio::test]
    async fn authenticated_native_provider_reopens_and_replays_without_redispatch() -> Result<()> {
        let root = std::env::temp_dir().join(format!(
            "harness-authenticated-native-{}",
            OperationId::new()
        ));
        let marker = root.join("marker.txt");
        let model = Model::new("mock", "execution-auth", "1", serde_json::Value::Null)?;
        let operation = OperationId::from_bytes([91; 16]);
        let interaction_id = InteractionId::new();
        let mut request = local_spec();
        request.working_directory = root.to_string_lossy().into_owned();
        if cfg!(windows) {
            request.arguments = vec!["/C".into(), "echo graphcoder-approved>>marker.txt".into()];
        } else {
            request.arguments = vec![
                "-c".into(),
                "printf graphcoder-approved >> marker.txt".into(),
            ];
        }
        let request_file;
        let dispatch;
        {
            let session = PersistentLocalHarness::open(
                &root,
                model.clone(),
                Arc::new(NoopModel),
                Limits::default(),
            )
            .await?;
            let mut approval = ExecutionApproval::approve_for(
                session.storage().session_id(),
                interaction_id,
                operation,
                request.clone(),
            )?;
            approval
                .bind_request_location(session.storage().volume(), "requests/authenticated.json")?;
            let bytes =
                serde_json::to_vec(&approval).map_err(|error| Error::Invalid(error.to_string()))?;
            request_file = session
                .storage()
                .stage(
                    operation,
                    "requests/authenticated.json",
                    &bytes,
                    "application/json",
                    "authenticated.json",
                )
                .await?;
            let provider = session.native_execution_provider()?;
            let digest = crate::core::effect_request_digest(
                provider.id(),
                EffectGuarantee::AtMostOnce,
                "host.process",
                &request_file,
            )?;
            session
                .storage()
                .open_interaction(
                    interaction_id,
                    Interaction::Approval {
                        prompt: "approve exact marker command".into(),
                        operation_id: operation,
                        action_digest: digest,
                    },
                )
                .await?;
            session
                .storage()
                .resolve_interaction(
                    interaction_id,
                    InteractionResponse::Approval {
                        approved: true,
                        reason: None,
                    },
                    &session.storage().test_interaction_responder(interaction_id),
                )
                .await?;
            dispatch = EffectDispatch {
                provider: provider.id().into(),
                effect_id: EffectId::from_bytes(operation.into_bytes()),
                attempt_id: EffectAttemptId::from_bytes([92; 16]),
                effect_kind: "host.process".into(),
                request: request_file.clone(),
                guarantee: EffectGuarantee::AtMostOnce,
                request_digest: digest,
            };
            let observation = provider.dispatch(dispatch.clone()).await?;
            assert!(matches!(observation.status, EffectStatus::Succeeded { .. }));

            let copied_file = session
                .storage()
                .stage(
                    OperationId::from_bytes([99; 16]),
                    "requests/copied.json",
                    &bytes,
                    "application/json",
                    "copied.json",
                )
                .await?;
            let copied_digest = crate::core::effect_request_digest(
                provider.id(),
                EffectGuarantee::AtMostOnce,
                "host.process",
                &copied_file,
            )?;
            assert!(matches!(
                provider
                    .dispatch(EffectDispatch {
                        provider: provider.id().into(),
                        effect_id: EffectId::from_bytes([100; 16]),
                        attempt_id: EffectAttemptId::from_bytes([101; 16]),
                        effect_kind: "host.process".into(),
                        request: copied_file,
                        guarantee: EffectGuarantee::AtMostOnce,
                        request_digest: copied_digest,
                    })
                    .await,
                Err(Error::Conflict(_))
            ));

            let denied_operation = OperationId::from_bytes([96; 16]);
            let denied_interaction = InteractionId::from_bytes([97; 16]);
            let mut denied = ExecutionApproval::deny_for(
                session.storage().session_id(),
                denied_interaction,
                denied_operation,
                request.clone(),
                "owner declined",
            )?;
            denied.bind_request_location(session.storage().volume(), "requests/denied.json")?;
            let denied_bytes =
                serde_json::to_vec(&denied).map_err(|error| Error::Invalid(error.to_string()))?;
            let denied_file = session
                .storage()
                .stage(
                    denied_operation,
                    "requests/denied.json",
                    &denied_bytes,
                    "application/json",
                    "denied.json",
                )
                .await?;
            let denied_digest = crate::core::effect_request_digest(
                provider.id(),
                EffectGuarantee::AtMostOnce,
                "host.process",
                &denied_file,
            )?;
            session
                .storage()
                .open_interaction(
                    denied_interaction,
                    Interaction::Approval {
                        prompt: "decline exact marker command".into(),
                        operation_id: denied_operation,
                        action_digest: denied_digest,
                    },
                )
                .await?;
            session
                .storage()
                .resolve_interaction(
                    denied_interaction,
                    InteractionResponse::Approval {
                        approved: false,
                        reason: Some("owner declined".into()),
                    },
                    &session.storage().test_interaction_responder(denied_interaction),
                )
                .await?;
            let denied_observation = provider
                .dispatch(EffectDispatch {
                    provider: provider.id().into(),
                    effect_id: EffectId::from_bytes(denied_operation.into_bytes()),
                    attempt_id: EffectAttemptId::from_bytes([98; 16]),
                    effect_kind: "host.process".into(),
                    request: denied_file,
                    guarantee: EffectGuarantee::AtMostOnce,
                    request_digest: denied_digest,
                })
                .await?;
            assert!(matches!(
                denied_observation.status,
                EffectStatus::FailedWithReceipt { ref message, .. }
                    if message.contains("owner declined")
            ));

            let mut forged = ExecutionApproval::approve_for(
                session.storage().session_id(),
                interaction_id,
                operation,
                request.clone(),
            )?;
            forged
                .bind_request_location(session.storage().volume(), "requests/authenticated.json")?;
            let forged_bytes =
                serde_json::to_vec(&forged).map_err(|error| Error::Invalid(error.to_string()))?;
            let forged_file = session
                .storage()
                .stage(
                    OperationId::from_bytes([93; 16]),
                    "requests/authenticated.json",
                    &forged_bytes,
                    "application/json",
                    "forged-session.json",
                )
                .await?;
            let forged_digest = crate::core::effect_request_digest(
                provider.id(),
                EffectGuarantee::AtMostOnce,
                "host.process",
                &forged_file,
            )?;
            assert!(matches!(
                provider
                    .dispatch(EffectDispatch {
                        provider: provider.id().into(),
                        effect_id: EffectId::from_bytes(operation.into_bytes()),
                        attempt_id: EffectAttemptId::from_bytes([95; 16]),
                        effect_kind: "host.process".into(),
                        request: forged_file,
                        guarantee: EffectGuarantee::AtMostOnce,
                        request_digest: forged_digest,
                    })
                    .await,
                Err(Error::Unauthorized(message))
                    if message.contains("owner interaction does not authorize")
            ));
        }
        let first_marker =
            std::fs::read(&marker).map_err(|error| Error::Storage(error.to_string()))?;
        assert!(!first_marker.is_empty());
        {
            let session =
                PersistentLocalHarness::open(&root, model, Arc::new(NoopModel), Limits::default())
                    .await?;
            let provider = session.native_execution_provider()?;
            let observation = provider.dispatch(dispatch).await?;
            assert!(matches!(observation.status, EffectStatus::Succeeded { .. }));
        }
        let replayed_marker =
            std::fs::read(&marker).map_err(|error| Error::Storage(error.to_string()))?;
        assert_eq!(replayed_marker, first_marker);
        std::fs::remove_dir_all(root).map_err(|error| Error::Storage(error.to_string()))?;
        Ok(())
    }

    #[tokio::test]
    async fn protected_local_store_unknown_reopens_for_operator_resolution_without_rerun()
    -> Result<()> {
        let root =
            std::env::temp_dir().join(format!("harness-protected-unknown-{}", OperationId::new()));
        let model = Model::new("mock", "execution-unknown", "1", serde_json::Value::Null)?;
        let operation = OperationId::from_bytes([111; 16]);
        let interaction_id = InteractionId::from_bytes([112; 16]);
        let request = local_spec();
        let dispatch;
        let first_calls = Arc::new(AtomicUsize::new(0));
        {
            let session = PersistentLocalHarness::open(
                &root,
                model.clone(),
                Arc::new(NoopModel),
                Limits::default(),
            )
            .await?;
            let mut approval = ExecutionApproval::approve_for(
                session.storage().session_id(),
                interaction_id,
                operation,
                request.clone(),
            )?;
            approval
                .bind_request_location(session.storage().volume(), "requests/uncertain.json")?;
            let approval_bytes =
                serde_json::to_vec(&approval).map_err(|error| Error::Invalid(error.to_string()))?;
            let request_file = session
                .storage()
                .stage(
                    operation,
                    "requests/uncertain.json",
                    &approval_bytes,
                    "application/json",
                    "uncertain.json",
                )
                .await?;
            let provider = NativeExecutionProvider::new_with_receipt_store(
                session.storage().content_verifier(),
                session.execution_receipt_store()?,
                Arc::new(CountingRunner {
                    calls: Arc::clone(&first_calls),
                    outcome: RunnerOutcome::Unknown {
                        reason: "host process outcome was interrupted".into(),
                    },
                }),
                session.storage().execution_approval_verifier(),
            )?;
            let request_digest = crate::core::effect_request_digest(
                provider.id(),
                EffectGuarantee::AtMostOnce,
                "host.process",
                &request_file,
            )?;
            session
                .storage()
                .open_interaction(
                    interaction_id,
                    Interaction::Approval {
                        prompt: "approve uncertain process".into(),
                        operation_id: operation,
                        action_digest: request_digest,
                    },
                )
                .await?;
            session
                .storage()
                .resolve_interaction(
                    interaction_id,
                    InteractionResponse::Approval {
                        approved: true,
                        reason: None,
                    },
                    &session.storage().test_interaction_responder(interaction_id),
                )
                .await?;
            dispatch = EffectDispatch {
                provider: provider.id().into(),
                effect_id: EffectId::from_bytes(operation.into_bytes()),
                attempt_id: EffectAttemptId::from_bytes([114; 16]),
                effect_kind: "host.process".into(),
                request: request_file,
                guarantee: EffectGuarantee::AtMostOnce,
                request_digest,
            };
            let observation = provider.dispatch(dispatch.clone()).await?;
            assert_eq!(observation.status, EffectStatus::Indeterminate);
            assert_eq!(first_calls.load(Ordering::SeqCst), 1);
        }

        let second_calls = Arc::new(AtomicUsize::new(0));
        {
            let session =
                PersistentLocalHarness::open(&root, model, Arc::new(NoopModel), Limits::default())
                    .await?;
            let key = ExecutionReceiptKey {
                operation_id: operation,
                effect_id: dispatch.effect_id,
                attempt_id: dispatch.attempt_id,
                provider: dispatch.provider.clone(),
                effect_kind: dispatch.effect_kind.clone(),
                guarantee: dispatch.guarantee,
                request_digest: dispatch.request_digest,
            };
            session
                .resolve_unknown_execution(&key, "operator reviewed after restart")
                .await?;
            let provider = NativeExecutionProvider::new_with_receipt_store(
                session.storage().content_verifier(),
                session.execution_receipt_store()?,
                Arc::new(CountingRunner {
                    calls: Arc::clone(&second_calls),
                    outcome: RunnerOutcome::Exited {
                        status_code: Some(0),
                        stdout: b"must not rerun".to_vec(),
                        stderr: Vec::new(),
                    },
                }),
                session.storage().execution_approval_verifier(),
            )?;
            let replay = provider.dispatch(dispatch).await?;
            assert_eq!(replay.status, EffectStatus::Indeterminate);
            assert_eq!(second_calls.load(Ordering::SeqCst), 0);
        }
        std::fs::remove_dir_all(root).map_err(|error| Error::Storage(error.to_string()))?;
        Ok(())
    }

    #[tokio::test]
    async fn real_native_unknown_restart_keeps_marker_and_claim_fenced() -> Result<()> {
        let root = std::env::temp_dir().join(format!(
            "harness-native-unknown-marker-{}",
            OperationId::new()
        ));
        let model = Model::new(
            "mock",
            "execution-native-unknown",
            "1",
            serde_json::Value::Null,
        )?;
        let operation = OperationId::from_bytes([121; 16]);
        let mut request = local_spec();
        request.working_directory = root.to_string_lossy().into_owned();
        if cfg!(windows) {
            request.arguments = vec!["/C".into(), "echo native-marker>>marker.txt".into()];
        } else {
            request.arguments = vec!["-c".into(), "printf native-marker >> marker.txt".into()];
        }

        let dispatch;
        let first_marker;
        {
            let session = PersistentLocalHarness::open(
                &root,
                model.clone(),
                Arc::new(NoopModel),
                Limits::default(),
            )
            .await?;
            let mut approval = ExecutionApproval::approve(operation, request.clone())?;
            approval.bind_request_location(
                session.storage().volume(),
                "requests/native-unknown.json",
            )?;
            let bytes =
                serde_json::to_vec(&approval).map_err(|error| Error::Invalid(error.to_string()))?;
            let request_file = session
                .storage()
                .stage(
                    operation,
                    "requests/native-unknown.json",
                    &bytes,
                    "application/json",
                    "native-unknown.json",
                )
                .await?;
            let provider = NativeExecutionProvider::new_with_receipt_store(
                session.storage().content_verifier(),
                session.execution_receipt_store()?,
                Arc::new(NativeExitThenFaultRunner),
                Arc::new(LocalApprovalVerifier),
            )?;
            let request_digest = crate::core::effect_request_digest(
                provider.id(),
                EffectGuarantee::AtMostOnce,
                "host.process",
                &request_file,
            )?;
            dispatch = EffectDispatch {
                provider: provider.id().into(),
                effect_id: EffectId::from_bytes(operation.into_bytes()),
                attempt_id: EffectAttemptId::from_bytes([122; 16]),
                effect_kind: "host.process".into(),
                request: request_file,
                guarantee: EffectGuarantee::AtMostOnce,
                request_digest,
            };
            let observation = provider.dispatch(dispatch.clone()).await?;
            assert_eq!(observation.status, EffectStatus::Indeterminate);
            first_marker = std::fs::read(root.join("marker.txt"))
                .map_err(|error| Error::Storage(error.to_string()))?;
            assert!(!first_marker.is_empty());
        }

        {
            let session =
                PersistentLocalHarness::open(&root, model, Arc::new(NoopModel), Limits::default())
                    .await?;
            let replay_calls = Arc::new(AtomicUsize::new(0));
            let provider = NativeExecutionProvider::new_with_receipt_store(
                session.storage().content_verifier(),
                session.execution_receipt_store()?,
                Arc::new(CountingRunner {
                    calls: Arc::clone(&replay_calls),
                    outcome: RunnerOutcome::Exited {
                        status_code: Some(0),
                        stdout: b"must not rerun".to_vec(),
                        stderr: Vec::new(),
                    },
                }),
                Arc::new(LocalApprovalVerifier),
            )?;
            let replay = provider.dispatch(dispatch).await?;
            assert_eq!(replay.status, EffectStatus::Indeterminate);
            assert_eq!(replay_calls.load(Ordering::SeqCst), 0);
            let replayed_marker = std::fs::read(root.join("marker.txt"))
                .map_err(|error| Error::Storage(error.to_string()))?;
            assert_eq!(replayed_marker, first_marker);
        }
        std::fs::remove_dir_all(root).map_err(|error| Error::Storage(error.to_string()))?;
        Ok(())
    }
}
