#![doc = include_str!("../README.md")]

use sha2::{Digest, Sha256};

pub mod grpc;
pub mod http;

/// Generated Workers v1 wire types. The documented schema is `proto/workers/v1/workers.proto`.
pub mod wire {
    #![allow(missing_docs, reason = "generated from the public Workers schema")]
    #![allow(clippy::all, clippy::pedantic, reason = "generated protobuf bindings")]
    include!("generated/acyclic.workers.v1.rs");
}

/// Canonical version-one descriptor set.
pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("generated/acyclic-workers-v1.bin");
/// Largest inline JavaScript module admitted by the first public contract.
pub const MAX_MODULE_BYTES: usize = 1024 * 1024;
/// Largest inline job input or result.
pub const MAX_INLINE_BYTES: usize = 1024 * 1024;
/// Largest bounded retry count.
pub const MAX_JOB_ATTEMPTS: u32 = 8;

/// Public JavaScript entrypoint declaration emitted into the TypeScript package.
/// A module may implement either handler or both; durable jobs never call `fetch`.
pub const MODULE_TYPESCRIPT_CONTRACT: &str = r#"// Generated from acyclic-workers::MODULE_TYPESCRIPT_CONTRACT. Do not edit.
export interface WorkerJobContext {
  readonly jobId: string;
  /** Starts at 1 and increases when an accepted job is retried. */
  readonly attempt: number;
  readonly signal: AbortSignal;
}
export interface WorkerModule {
  fetch?(request: Request): Response | Promise<Response>;
  run?(input: Uint8Array, context: WorkerJobContext): Uint8Array | Promise<Uint8Array>;
}
"#;

/// Rust-owned route names used by the TypeScript transport generator.
pub const HTTP_ROUTES: &[(&str, &str)] = &[
    ("publishVersion", "v1/workers/versions/publish"),
    ("selectDeployment", "v1/workers/deployments/select"),
    ("submitJob", "v1/workers/jobs/submit"),
    ("inspectJob", "v1/workers/jobs/inspect"),
    ("cancelJob", "v1/workers/jobs/cancel"),
    ("invokeVersion", "v1/workers/versions/{sha256hex}/invoke"),
    ("invokeDeployment", "v1/workers/deployments/{alias}/invoke"),
];

/// Invalid customer-authored Workers request.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ContractError {
    /// A required field is absent or malformed.
    #[error("required field is absent or malformed")]
    InvalidArgument,
    /// An authored byte payload exceeds its contract bound.
    #[error("inline payload exceeds the Workers v1 limit")]
    LimitExceeded,
    /// The supplied digest does not identify the exact module bytes.
    #[error("module SHA-256 does not match exact bytes")]
    DigestMismatch,
}

fn digest(value: &[u8]) -> bool {
    value.len() == 32 && value.iter().any(|byte| *byte != 0)
}

fn name(value: &str) -> bool {
    !value.is_empty()
        && value != "."
        && value != ".."
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

/// Validates an immutable JavaScript module publication before service admission.
pub fn validate_publish(request: &wire::PublishVersionRequest) -> Result<(), ContractError> {
    if request.javascript_module.is_empty() || !name(&request.idempotency_key) {
        return Err(ContractError::InvalidArgument);
    }
    if request.javascript_module.len() > MAX_MODULE_BYTES {
        return Err(ContractError::LimitExceeded);
    }
    if !digest(&request.expected_sha256) {
        return Err(ContractError::InvalidArgument);
    }
    if Sha256::digest(&request.javascript_module).as_slice() != request.expected_sha256 {
        return Err(ContractError::DigestMismatch);
    }
    Ok(())
}

/// Validates a compare-and-select deployment alias mutation.
pub fn validate_select(request: &wire::SelectDeploymentRequest) -> Result<(), ContractError> {
    if !name(&request.alias)
        || !digest(&request.version_sha256)
        || !name(&request.idempotency_key)
        || request.expected_revision == Some(0)
    {
        return Err(ContractError::InvalidArgument);
    }
    Ok(())
}

/// Validates a durable job request before authoritative resolution of its target.
pub fn validate_submit(request: &wire::SubmitJobRequest) -> Result<(), ContractError> {
    let target = request
        .target
        .as_ref()
        .and_then(|value| value.target.as_ref());
    let valid_target = match target {
        Some(wire::job_target::Target::DeploymentAlias(alias)) => name(alias),
        Some(wire::job_target::Target::VersionSha256(value)) => digest(value),
        None => false,
    };
    if !valid_target || !name(&request.idempotency_key) {
        return Err(ContractError::InvalidArgument);
    }
    let Some(input) = request
        .input
        .as_ref()
        .and_then(|value| value.source.as_ref())
    else {
        return Err(ContractError::InvalidArgument);
    };
    match input {
        wire::payload::Source::InlineBytes(bytes) if bytes.len() > MAX_INLINE_BYTES => {
            return Err(ContractError::LimitExceeded);
        }
        wire::payload::Source::ObjectVersion(reference)
            if reference.bucket_id.is_empty()
                || reference.object_key.is_empty()
                || reference.version_id.is_empty() =>
        {
            return Err(ContractError::InvalidArgument);
        }
        _ => {}
    }
    let Some(retry) = request.retry.as_ref() else {
        return Err(ContractError::InvalidArgument);
    };
    if retry.max_attempts == 0
        || retry.max_attempts > MAX_JOB_ATTEMPTS
        || !request.limits.as_ref().is_some_and(|limits| {
            limits.timeout_millis > 0
                && limits.memory_bytes > 0
                && limits.output_bytes > 0
                && limits.output_bytes <= MAX_INLINE_BYTES as u64
        })
    {
        return Err(ContractError::InvalidArgument);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alias_selection_requires_positive_revision_when_present() {
        let mut request = wire::SelectDeploymentRequest {
            alias: "current".into(),
            version_sha256: vec![1; 32],
            idempotency_key: "select-a".into(),
            expected_revision: None,
        };
        assert_eq!(validate_select(&request), Ok(()));
        request.expected_revision = Some(7);
        assert_eq!(validate_select(&request), Ok(()));
        request.expected_revision = Some(0);
        assert_eq!(validate_select(&request), Err(ContractError::InvalidArgument));
    }

    #[test]
    fn publication_is_bound_to_exact_bytes() {
        let bytes = b"export default { fetch() { return new Response('ok') } }";
        let mut request = wire::PublishVersionRequest {
            javascript_module: bytes.to_vec(),
            expected_sha256: Sha256::digest(bytes).to_vec(),
            idempotency_key: "publish-a".into(),
        };
        assert_eq!(validate_publish(&request), Ok(()));
        request.javascript_module.push(b' ');
        assert_eq!(
            validate_publish(&request),
            Err(ContractError::DigestMismatch)
        );
    }

    #[test]
    fn job_is_pinned_or_resolved_at_acceptance() {
        let request = wire::SubmitJobRequest {
            target: Some(wire::JobTarget {
                target: Some(wire::job_target::Target::DeploymentAlias("current".into())),
            }),
            input: Some(wire::Payload {
                source: Some(wire::payload::Source::InlineBytes(vec![])),
            }),
            limits: Some(wire::JobLimits {
                timeout_millis: 1000,
                memory_bytes: 1024,
                output_bytes: 1024,
            }),
            retry: Some(wire::RetryPolicy {
                max_attempts: 2,
                backoff_millis: 0,
            }),
            idempotency_key: "job-a".into(),
        };
        assert_eq!(validate_submit(&request), Ok(()));
    }
}
