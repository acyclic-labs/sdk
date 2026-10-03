//! Approved host execution provider.
//!
//! This module is deliberately a host adapter.  Harness owns admission and
//! effect recovery; this adapter only validates an immutable command approval
//! and runs one already-dispatched attempt.  It does not provide a sandbox or
//! claim that a workspace route confines a process.

use crate::{
    EffectAttemptId, EffectId, Error, OperationId, Result,
    conversation::{ContentPublisher, ContentResidencyVerifier},
    core::{EffectGuarantee, EffectStatus},
    effects::{EffectDispatch, EffectObservation, EffectProvider},
};
use futures::FutureExt as _;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    io::Read,
    path::Path,
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError},
        Mutex,
    },
    thread,
    time::{Duration, Instant},
};

const REQUEST_DOMAIN: &[u8] = b"acyclic:harness:approved-execution:v1";
const MAX_ARGUMENTS: usize = 1024;
const MAX_ARGUMENT_BYTES: usize = 64 * 1024;
const MAX_ENVIRONMENT_ENTRIES: usize = 256;
const MAX_OUTPUT_BYTES: usize = 4 * 1024 * 1024;
const MAX_FAILURE_BYTES: usize = 4096;
const READER_GRACE: Duration = Duration::from_millis(250);

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
        if self.timeout_ms == Some(0) {
            return Err(Error::Invalid("execution timeout must be positive".into()));
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
    /// Operation identity allocated before admission.
    pub operation_id: OperationId,
    /// Exact approved command.
    pub request: ExecutionSpec,
    /// Digest of `request` at approval time.
    pub request_digest: [u8; 32],
    /// Whether the owner approved dispatch.
    pub approved: bool,
    /// Bounded owner reason for a denial, if any.
    pub denial_reason: Option<String>,
}

impl ExecutionApproval {
    /// Creates an approval for one exact command.
    pub fn approve(operation_id: OperationId, request: ExecutionSpec) -> Result<Self> {
        let request_digest = request.digest()?;
        Ok(Self {
            operation_id,
            request,
            request_digest,
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
        let request_digest = request.digest()?;
        let denial_reason = reason.into();
        if denial_reason.is_empty() || denial_reason.len() > MAX_FAILURE_BYTES {
            return Err(Error::Invalid("execution denial reason is invalid".into()));
        }
        Ok(Self {
            operation_id,
            request,
            request_digest,
            approved: false,
            denial_reason: Some(denial_reason),
        })
    }

    /// Checks that persisted approval bytes still name the exact request.
    pub fn validate(&self) -> Result<()> {
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
        Ok(())
    }
}

/// Typed result persisted as the successful effect artifact.
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
        let mut command = Command::new(&request.executable);
        command
            .args(&request.arguments)
            .current_dir(&request.working_directory)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        request.environment.apply(&mut command);
        let mut child = command.spawn().map_err(|error| {
            Error::Storage(format!("failed to start approved process: {error}"))
        })?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| Error::Storage("approved process stdout pipe missing".into()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| Error::Storage("approved process stderr pipe missing".into()))?;
        let overflow = Arc::new(AtomicBool::new(false));
        let remaining = Arc::new(std::sync::atomic::AtomicUsize::new(
            request.max_output_bytes as usize,
        ));
        let stdout_thread = spawn_reader(stdout, Arc::clone(&remaining), Arc::clone(&overflow));
        let stderr_thread = spawn_reader(stderr, Arc::clone(&remaining), Arc::clone(&overflow));
        let deadline = request
            .timeout_ms
            .map(|ms| Instant::now() + Duration::from_millis(ms));
        let termination;
        loop {
            if overflow.load(Ordering::Acquire) {
                let _ = child.kill();
                let _ = child.wait();
                termination = Termination::Overflow;
                break;
            }
            if cancellation.is_cancelled() {
                let _ = child.kill();
                let _ = child.wait();
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
                let _ = child.kill();
                let _ = child.wait();
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
        match termination {
            Termination::TimedOut => Ok(RunnerOutcome::TimedOut { stdout, stderr }),
            Termination::Cancelled => Ok(RunnerOutcome::Cancelled { stdout, stderr }),
            Termination::Overflow => unreachable!("overflow is returned above"),
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

/// Verifies that an execution request was approved by the durable owner.
///
/// The serialized `approved` flag is request data and is never sufficient on
/// its own.  Implementations should authenticate the owner record and bind it
/// to the operation, request digest, and current session before returning.
pub trait ExecutionApprovalVerifier: Send + Sync {
    /// Authenticates the persisted approval or denial record.
    fn verify<'a>(
        &'a self,
        approval: &'a ExecutionApproval,
    ) -> BoxFuture<'a, Result<()>>;
}

/// Host-bound provider that adapts approved processes to Harness effects.
pub struct NativeExecutionProvider {
    resolver: Arc<dyn ContentResidencyVerifier>,
    publisher: Arc<dyn ContentPublisher>,
    runner: Arc<dyn ExecutionRunner>,
    approval_verifier: Arc<dyn ExecutionApprovalVerifier>,
    active: Mutex<BTreeMap<OperationId, ExecutionCancellation>>,
    provider_id: String,
}

impl NativeExecutionProvider {
    /// Binds owner-authorized content access and a host execution runner.
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
            publisher,
            runner,
            approval_verifier,
            active: Mutex::new(BTreeMap::new()),
            provider_id: "harness.native-execution.v1".into(),
        })
    }

    /// Uses the default native process runner.
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

    /// Requests cancellation of a currently running operation.
    ///
    /// Durable cancellation admission remains the Harness owner's job; this
    /// method only signals the already admitted native attempt.
    pub fn cancel(&self, operation_id: OperationId) -> bool {
        let Ok(active) = self.active.lock() else {
            return false;
        };
        let Some(cancellation) = active.get(&operation_id).cloned() else {
            return false;
        };
        cancellation.cancel();
        true
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
        if EffectId::from_bytes(approval.operation_id.into_bytes()) != request.effect_id {
            return Err(Error::Conflict(
                "execution approval operation does not match effect identity".into(),
            ));
        }
        self.approval_verifier.verify(&approval).await?;
        let receipt = if approval.approved {
            let cancellation = ExecutionCancellation::new();
            self.active
                .lock()
                .map_err(|_| Error::Storage("active execution registry is poisoned".into()))?
                .insert(approval.operation_id, cancellation.clone());
            let runner = Arc::clone(&self.runner);
            let execution_request = approval.request.clone();
            let outcome = tokio::task::spawn_blocking(move || {
                runner.run_with_cancellation(&execution_request, &cancellation)
            })
            .await
            .map_err(|error| Error::Storage(format!("approved process task failed: {error}")));
            self.active
                .lock()
                .map_err(|_| Error::Storage("active execution registry is poisoned".into()))?
                .remove(&approval.operation_id);
            match outcome?? {
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
                RunnerOutcome::Unknown { reason: _ } => {
                    return Ok(EffectObservation {
                        provider: request.provider,
                        effect_id: request.effect_id,
                        attempt_id: request.attempt_id,
                        request_digest: request.request_digest,
                        guarantee: request.guarantee,
                        status: EffectStatus::Indeterminate,
                    });
                }
            }
        } else {
            ExecutionReceipt::Denied {
                reason: approval
                    .denial_reason
                    .unwrap_or_else(|| "owner denied execution".into()),
            }
        };
        let receipt_bytes =
            serde_json::to_vec(&receipt).map_err(|error| Error::Invalid(error.to_string()))?;
        let path = format!(
            ".harness/execution/{}/attempt-{}.json",
            approval.operation_id, request.attempt_id
        );
        let result = self
            .publisher
            .stage(
                approval.operation_id,
                &path,
                &receipt_bytes,
                "application/json",
                "execution-result.json",
            )
            .await?;
        let status = match receipt {
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
        };
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
        _attempt_id: EffectAttemptId,
    ) -> BoxFuture<'a, Result<Option<EffectObservation>>> {
        async { Ok(None) }.boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AgentId,
        conversation::{FileDescriptor, FileRef, VolumeClass, VolumeOwner, VolumeRef},
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
    struct TestApprovalVerifier;

    impl ExecutionApprovalVerifier for TestApprovalVerifier {
        fn verify<'a>(
            &'a self,
            _approval: &'a ExecutionApproval,
        ) -> futures::future::BoxFuture<'a, Result<()>> {
            async { Ok(()) }.boxed()
        }
    }

    fn approval_verifier() -> Arc<dyn ExecutionApprovalVerifier> {
        Arc::new(TestApprovalVerifier)
    }

    #[derive(Clone)]
    struct FixedRunner(RunnerOutcome);

    impl ExecutionRunner for FixedRunner {
        fn run(&self, _request: &ExecutionSpec) -> Result<RunnerOutcome> {
            Ok(self.0.clone())
        }
    }

    fn content_fixture(approval: &ExecutionApproval) -> Result<(Arc<MemoryContent>, FileRef)> {
        let volume = VolumeRef::new(
            ProviderRef::new("test", "filesystem", "2")?,
            "private",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([7; 16])),
        )?;
        let bytes =
            serde_json::to_vec(approval).map_err(|error| Error::Invalid(error.to_string()))?;
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
        let request = spec();
        let approval = ExecutionApproval::approve(operation, request.clone())?;
        approval.validate()?;
        let mut changed = approval.clone();
        changed.request.arguments.push("changed".into());
        assert!(matches!(changed.validate(), Err(Error::Conflict(_))));
        let mut relative = request;
        relative.executable = "cmd.exe".into();
        assert!(relative.digest().is_err());
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
                format!(
                    "if defined {sentinel} (exit /b 7) else (echo graphcoder-approved)"
                ),
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
        assert!(matches!(
            NativeExecutionRunner.run(&request)?,
            RunnerOutcome::TimedOut { .. }
        ));
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
        let handle = thread::spawn(move || {
            NativeExecutionRunner.run_with_cancellation(&request, &signal)
        });
        cancellation.cancel();
        assert!(matches!(handle.join().expect("runner thread panicked")?, RunnerOutcome::Cancelled { .. }));
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
                request: unknown_request,
                guarantee: EffectGuarantee::AtMostOnce,
                request_digest: unknown_request_digest,
            })
            .await?;
        assert_eq!(observation.status, EffectStatus::Indeterminate);
        assert!(unknown_content.staged.lock().unwrap().is_empty());
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
        assert!(cancelled, "dispatch never exposed its active cancellation token");
        let observation = task.await.map_err(|error| Error::Storage(error.to_string()))??;
        assert!(matches!(
            observation.status,
            EffectStatus::FailedWithReceipt { ref message, .. } if message.contains("cancelled")
        ));
        assert_eq!(content.staged.lock().unwrap().len(), 1);
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
