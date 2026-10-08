//! Existing Workers admission predicates shared by library and schema build.
use crate::wire;
use sha2::{Digest, Sha256};

/// Largest inline JavaScript module admitted by the first public contract.
pub const MAX_MODULE_BYTES: usize = 1024 * 1024;
/// Largest inline job input or result.
pub const MAX_INLINE_BYTES: usize = 1024 * 1024;
/// Largest bounded retry count.
pub const MAX_JOB_ATTEMPTS: u32 = 8;

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
        wire::payload::Source::Object(reference)
            if reference.bucket.is_empty()
                || reference.bucket.len() > 63
                || reference.key.is_empty()
                || reference.key.len() > 1024 =>
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

/// Validates exact job output against the accepted job's bounded output budget.
pub fn validate_result(
    result: &wire::JobResult,
    limits: &wire::JobLimits,
) -> Result<(), ContractError> {
    if limits.output_bytes == 0 || limits.output_bytes > MAX_INLINE_BYTES as u64 {
        return Err(ContractError::InvalidArgument);
    }
    if result.body.len() as u64 > limits.output_bytes {
        return Err(ContractError::LimitExceeded);
    }
    Ok(())
}
