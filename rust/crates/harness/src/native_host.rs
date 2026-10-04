//! Local native command host backed by the Harness effect provider.
//!
//! This adapter deliberately contains no process runner.  A composition gives
//! it the already configured [`NativeExecutionProvider`] and an admission
//! boundary which stages the exact, owner approved request.  Dispatch and
//! reconciliation therefore use the same receipt and recovery semantics as
//! every other Harness native effect.

use crate::{
    Error, OperationId, Result,
    conversation::{ContentResidencyVerifier, FileRef},
    effects::{EffectDispatch, EffectProvider},
    host_execution::{ExecutionReceipt, ExecutionSpec, NativeExecutionProvider},
    native_tool::NativeCommandHost,
    tool::ToolResult,
};
use futures::future::BoxFuture;
use serde_json::{Value, json};
use std::sync::Arc;

/// The host-owned admission boundary for one exact native command.
///
/// Implementations stage an [`ExecutionApproval`] and its authenticated
/// interaction before returning the immutable effect dispatch.  They also
/// reconstruct that same dispatch after restart; this keeps reconciliation
/// from redispatching or deriving request bytes from mutable workspace paths.
pub trait NativeCommandAdmission: Send + Sync {
    /// Persists owner approval and returns the immutable first dispatch.
    fn admit<'a>(
        &'a self,
        operation_id: OperationId,
        request: &'a ExecutionSpec,
    ) -> BoxFuture<'a, Result<EffectDispatch>>;

    /// Reopens the persisted dispatch for reconciliation after a restart.
    fn reopen<'a>(
        &'a self,
        operation_id: OperationId,
        request: &'a ExecutionSpec,
    ) -> BoxFuture<'a, Result<EffectDispatch>>;
}

/// Thin command host over the existing native effect provider.
pub struct ProviderNativeCommandHost {
    provider: Arc<NativeExecutionProvider>,
    resolver: Arc<dyn ContentResidencyVerifier>,
    admission: Arc<dyn NativeCommandAdmission>,
}

impl ProviderNativeCommandHost {
    /// Binds the existing native provider to a host-owned admission and
    /// immutable result resolver.
    #[must_use]
    pub fn new(
        provider: Arc<NativeExecutionProvider>,
        resolver: Arc<dyn ContentResidencyVerifier>,
        admission: Arc<dyn NativeCommandAdmission>,
    ) -> Self {
        Self {
            provider,
            resolver,
            admission,
        }
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
        let dispatch = self.admission.admit(operation_id, &request).await?;
        if dispatch.effect_id != crate::EffectId::from_bytes(operation_id.into_bytes()) {
            return Err(Error::Conflict(
                "native command admission changed effect identity".into(),
            ));
        }
        let observation = self.provider.dispatch(dispatch).await?;
        observation_to_tool_result(self, operation_id, observation.status).await
    }

    async fn reconcile_inner(
        &self,
        operation_id: OperationId,
        request: ExecutionSpec,
    ) -> Result<Option<ToolResult>> {
        let dispatch = self.admission.reopen(operation_id, &request).await?;
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
        ExecutionReceipt::Denied { reason } | ExecutionReceipt::Unknown { reason } => {
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
    fn receipt_projection_preserves_terminal_status_and_output() -> Result<()> {
        let result = receipt_to_tool_result(ExecutionReceipt::Succeeded {
            status_code: 0,
            stdout: b"ok\n".to_vec(),
            stderr: Vec::new(),
        })?;
        assert_eq!(result.value["status"], "exited");
        assert_eq!(result.value["exit_code"], 0);
        assert_eq!(result.value["stdout"], "ok\n");
        assert_eq!(result.value["stderr"], "");
        assert!(result.value["reason"].is_null());
        Ok(())
    }

    #[test]
    fn uncertain_receipts_are_explicit_and_have_no_output() -> Result<()> {
        let result = receipt_to_tool_result(ExecutionReceipt::Unknown {
            reason: "operator review required".into(),
        })?;
        assert_eq!(result.value["status"], "unknown");
        assert!(result.value["exit_code"].is_null());
        assert_eq!(result.value["reason"], "operator review required");
        assert_eq!(result.value["stdout"], "");
        Ok(())
    }
}
