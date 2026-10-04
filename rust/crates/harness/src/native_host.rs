//! Thin native command host over the existing Harness effect provider.
//!
//! This adapter owns no second journal and runs no process itself. The
//! composition supplies a task-scoped admission boundary that persists the
//! exact approval and effect dispatch before the provider is called.

use crate::{
    EffectId, Error, OperationId, Result,
    conversation::{ContentResidencyVerifier, FileRef},
    effects::{EffectDispatch, EffectProvider},
    host_execution::{ExecutionReceipt, ExecutionSpec, NativeExecutionProvider},
    native_tool::NativeCommandHost,
    tool::ToolResult,
};
use futures::future::BoxFuture;
use serde_json::{Value, json};
use std::sync::Arc;

/// Owner admission and restart reconstruction for one task-scoped command.
pub trait NativeCommandAdmission: Send + Sync {
    /// Persists the exact admission and returns its immutable dispatch.
    fn admit<'a>(
        &'a self,
        operation_id: OperationId,
        request: &'a ExecutionSpec,
    ) -> BoxFuture<'a, Result<EffectDispatch>>;

    /// Reconstructs the persisted dispatch without creating a new attempt.
    fn reopen<'a>(
        &'a self,
        operation_id: OperationId,
        request: &'a ExecutionSpec,
    ) -> BoxFuture<'a, Result<EffectDispatch>>;
}

/// Maximum command timeout accepted by this host composition by default.
///
/// `ExecutionSpec` keeps a wide wire-level bound for compatibility, while a
/// live session must always apply a finite host policy before admission.
pub const DEFAULT_MAX_SESSION_TIMEOUT_MS: u64 = 24 * 60 * 60 * 1_000;

/// Host adapter that routes native commands through the durable effect
/// provider and the task's private admission boundary.
pub struct ProviderNativeCommandHost {
    provider: Arc<NativeExecutionProvider>,
    resolver: Arc<dyn ContentResidencyVerifier>,
    admission: Arc<dyn NativeCommandAdmission>,
    max_session_timeout_ms: u64,
}

impl ProviderNativeCommandHost {
    /// Constructs a host with the default one-day session deadline.
    pub fn new(
        provider: Arc<NativeExecutionProvider>,
        resolver: Arc<dyn ContentResidencyVerifier>,
        admission: Arc<dyn NativeCommandAdmission>,
    ) -> Self {
        Self {
            provider,
            resolver,
            admission,
            max_session_timeout_ms: DEFAULT_MAX_SESSION_TIMEOUT_MS,
        }
    }

    /// Applies the owner-selected finite session deadline.
    pub fn with_max_session_timeout_ms(mut self, maximum: u64) -> Result<Self> {
        if maximum == 0 {
            return Err(Error::Invalid(
                "native command session timeout must be positive".into(),
            ));
        }
        self.max_session_timeout_ms = maximum;
        Ok(self)
    }

    fn validate_session_timeout(&self, request: &ExecutionSpec) -> Result<()> {
        if request
            .timeout_ms
            .is_some_and(|timeout| timeout > self.max_session_timeout_ms)
        {
            return Err(Error::Invalid(
                "native command timeout exceeds the admitted session limit".into(),
            ));
        }
        Ok(())
    }

    async fn read_result(&self, reference: &FileRef) -> Result<ToolResult> {
        let bytes = self.resolver.read(reference).await?;
        reference.descriptor().verify(&bytes)?;
        let receipt: ExecutionReceipt = serde_json::from_slice(&bytes).map_err(|error| {
            Error::Invalid(format!("native execution receipt is invalid: {error}"))
        })?;
        receipt.validate()?;
        receipt_to_tool_result(receipt)
    }

    async fn execute_inner(
        &self,
        operation_id: OperationId,
        request: ExecutionSpec,
    ) -> Result<ToolResult> {
        self.validate_session_timeout(&request)?;
        let dispatch = self.admission.admit(operation_id, &request).await?;
        validate_dispatch_identity(operation_id, &dispatch)?;
        let observation = self.provider.dispatch(dispatch).await?;
        observation_to_tool_result(self, operation_id, observation.status).await
    }

    async fn reconcile_inner(
        &self,
        operation_id: OperationId,
        request: ExecutionSpec,
    ) -> Result<Option<ToolResult>> {
        self.validate_session_timeout(&request)?;
        let dispatch = self.admission.reopen(operation_id, &request).await?;
        validate_dispatch_identity(operation_id, &dispatch)?;
        let observation = self.provider.reconcile(dispatch.attempt_id).await?;
        let Some(observation) = observation else {
            return Ok(None);
        };
        Ok(Some(
            observation_to_tool_result(self, operation_id, observation.status).await?,
        ))
    }
}

impl NativeCommandHost for ProviderNativeCommandHost {
    fn execute<'a>(
        &'a self,
        operation_id: OperationId,
        request: ExecutionSpec,
    ) -> BoxFuture<'a, Result<ToolResult>> {
        Box::pin(self.execute_inner(operation_id, request))
    }

    fn reconcile<'a>(
        &'a self,
        operation_id: OperationId,
        request: ExecutionSpec,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(self.reconcile_inner(operation_id, request))
    }

    fn project(
        &self,
        _operation_id: OperationId,
        _request: &ExecutionSpec,
        result: &ToolResult,
    ) -> Result<Value> {
        result
            .value
            .get("status")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::Invalid("native command result has no status".into()))?;
        Ok(result.value.clone())
    }
}

fn validate_dispatch_identity(operation_id: OperationId, dispatch: &EffectDispatch) -> Result<()> {
    if dispatch.effect_id != EffectId::from_bytes(operation_id.into_bytes()) {
        return Err(Error::Conflict(
            "native command admission changed effect identity".into(),
        ));
    }
    Ok(())
}

async fn observation_to_tool_result(
    host: &ProviderNativeCommandHost,
    operation_id: OperationId,
    status: crate::core::EffectStatus,
) -> Result<ToolResult> {
    match status {
        crate::core::EffectStatus::Succeeded { result }
        | crate::core::EffectStatus::FailedWithReceipt { result, .. } => {
            host.read_result(&result).await
        }
        crate::core::EffectStatus::Indeterminate => Err(Error::Indeterminate(operation_id)),
        crate::core::EffectStatus::Failed { message } => Err(Error::Invalid(message)),
        crate::core::EffectStatus::Planned | crate::core::EffectStatus::Dispatched => {
            Err(Error::Indeterminate(operation_id))
        }
    }
}

fn receipt_to_tool_result(receipt: ExecutionReceipt) -> Result<ToolResult> {
    let (status, exit_code, stdout, stderr, reason) = match receipt {
        ExecutionReceipt::Succeeded {
            status_code,
            stdout,
            stderr,
        } => ("exited", Some(status_code), stdout, stderr, None),
        ExecutionReceipt::Failed {
            status_code,
            stdout,
            stderr,
        } => ("exited", status_code, stdout, stderr, None),
        ExecutionReceipt::TimedOut { stdout, stderr } => ("timed_out", None, stdout, stderr, None),
        ExecutionReceipt::Cancelled { stdout, stderr } => ("cancelled", None, stdout, stderr, None),
        ExecutionReceipt::Denied { reason } => {
            ("denied", None, Vec::new(), Vec::new(), Some(reason))
        }
        ExecutionReceipt::Unknown { reason } => {
            ("unknown", None, Vec::new(), Vec::new(), Some(reason))
        }
    };
    Ok(ToolResult {
        value: json!({
            "status": status,
            "exit_code": exit_code,
            "stdout": String::from_utf8_lossy(&stdout),
            "stderr": String::from_utf8_lossy(&stderr),
            "reason": reason,
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn denied_receipts_remain_denied() -> Result<()> {
        let result = receipt_to_tool_result(ExecutionReceipt::Denied {
            reason: "operator rejected".into(),
        })?;
        assert_eq!(result.value["status"], "denied");
        assert_eq!(result.value["reason"], "operator rejected");
        Ok(())
    }

    #[test]
    fn unknown_receipts_remain_uncertain() -> Result<()> {
        let result = receipt_to_tool_result(ExecutionReceipt::Unknown {
            reason: "runner outcome unavailable".into(),
        })?;
        assert_eq!(result.value["status"], "unknown");
        assert_eq!(result.value["reason"], "runner outcome unavailable");
        Ok(())
    }
}
