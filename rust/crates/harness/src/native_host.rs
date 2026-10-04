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

#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
use crate::{
    EffectAttemptId, EffectId,
    core::EffectGuarantee,
    filesystem::PersistentLocalHarness,
    host_execution::ExecutionApproval,
    interaction::{Interaction, InteractionResponse},
};

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
        validate_dispatch(&dispatch, operation_id)?;
        let observation = self.provider.dispatch(dispatch).await?;
        observation_to_tool_result(self, operation_id, observation.status).await
    }

    async fn reconcile_inner(
        &self,
        operation_id: OperationId,
        request: ExecutionSpec,
    ) -> Result<Option<ToolResult>> {
        let dispatch = self.admission.reopen(operation_id, &request).await?;
        validate_dispatch(&dispatch, operation_id)?;
        let observation = self.provider.reconcile(dispatch.attempt_id).await?;
        let Some(observation) = observation else {
            return Ok(None);
        };
        Ok(Some(
            observation_to_tool_result(self, operation_id, observation.status).await?,
        ))
    }
}

fn validate_dispatch(dispatch: &EffectDispatch, operation_id: OperationId) -> Result<()> {
    if dispatch.effect_id != crate::EffectId::from_bytes(operation_id.into_bytes()) {
        return Err(Error::Conflict(
            "native command admission changed effect identity".into(),
        ));
    }
    if dispatch.attempt_id.into_bytes() == [0; 16]
        || dispatch.request_digest == [0; 32]
        || dispatch.provider != "harness.native-execution.v1"
        || dispatch.effect_kind != "host.process"
        || dispatch.guarantee != crate::core::EffectGuarantee::AtMostOnce
    {
        return Err(Error::Conflict(
            "native command admission returned an unbound effect dispatch".into(),
        ));
    }
    Ok(())
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

#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
/// Host decision for a durable native command approval.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NativeCommandDecision {
    /// Keep the durable approval unresolved.
    Pending,
    /// Resolve the approval and execute the exact command.
    Approved,
    /// Resolve the approval as declined.
    Denied(String),
}

/// Durable local admission for the native command host.
///
/// Approval bytes are staged into the session's private volume before the
/// interaction is opened. The provider receives only the immutable reference
/// returned by that stage operation. Reopen reads that same owner-private
/// request and reconstructs its dispatch without redispatching or consulting a
/// mutable workspace file.
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
pub struct PersistentNativeCommandAdmission {
    session: Arc<PersistentLocalHarness>,
    decisions:
        Arc<std::sync::Mutex<std::collections::BTreeMap<OperationId, NativeCommandDecision>>>,
}

#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
impl PersistentNativeCommandAdmission {
    /// Binds admission to one persistent session.
    #[must_use]
    pub fn new(session: Arc<PersistentLocalHarness>) -> Self {
        Self {
            session,
            decisions: Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new())),
        }
    }

    /// Records the host decision for one operation. Unlisted operations stay
    /// pending and cannot spawn a process.
    pub fn set_decision(&self, operation_id: OperationId, decision: NativeCommandDecision) {
        if let Ok(mut decisions) = self.decisions.lock() {
            decisions.insert(operation_id, decision);
        }
    }

    fn interaction_id(operation: OperationId) -> crate::InteractionId {
        let digest = blake3::hash(
            &[
                b"harness:native-command-approval:v1".as_slice(),
                operation.into_bytes().as_slice(),
            ]
            .concat(),
        );
        let mut bytes = [0_u8; 16];
        bytes.copy_from_slice(&digest.as_bytes()[..16]);
        crate::InteractionId::from_bytes(bytes)
    }

    fn attempt_id(operation: OperationId, request: &ExecutionSpec) -> Result<EffectAttemptId> {
        let digest = request.digest()?;
        let hash = blake3::hash(
            &[
                b"harness:native-command-attempt:v1".as_slice(),
                operation.into_bytes().as_slice(),
                &digest,
            ]
            .concat(),
        );
        let mut bytes = [0_u8; 16];
        bytes.copy_from_slice(&hash.as_bytes()[..16]);
        Ok(EffectAttemptId::from_bytes(bytes))
    }

    async fn stage_and_resolve(
        &self,
        operation: OperationId,
        request: &ExecutionSpec,
        approval: &ExecutionApproval,
        interaction: crate::InteractionId,
        decision: NativeCommandDecision,
    ) -> Result<EffectDispatch> {
        let path = format!(".harness/execution/{operation}/approved-request.json");
        let bytes = serde_json::to_vec(approval).map_err(|error| {
            Error::Invalid(format!("native approval is not serializable: {error}"))
        })?;
        let file = self
            .session
            .storage()
            .stage(
                operation,
                &path,
                &bytes,
                "application/json",
                "approved-execution.json",
            )
            .await?;
        let request_digest = crate::core::effect_request_digest(
            "harness.native-execution.v1",
            EffectGuarantee::AtMostOnce,
            "host.process",
            &file,
        )?;
        self.session
            .storage()
            .open_interaction(
                interaction,
                Interaction::Approval {
                    prompt: "approve exact native command".into(),
                    operation_id: operation,
                    action_digest: request_digest,
                },
            )
            .await?;
        match decision {
            NativeCommandDecision::Pending => return Err(Error::Indeterminate(operation)),
            NativeCommandDecision::Approved | NativeCommandDecision::Denied(_) => {
                let approved = matches!(decision, NativeCommandDecision::Approved);
                let reason = match decision {
                    NativeCommandDecision::Denied(reason) => Some(reason),
                    _ => None,
                };
                let authorizer = self.session.storage().interaction_operator_authorizer()?;
                let scope = authorizer
                    .issue_scope(&crate::filesystem::InteractionApprovalAuthorization {
                        interaction_id: interaction,
                        operation_id: operation,
                        action_digest: request_digest,
                        approved,
                    })
                    .await?;
                self.session
                    .resolve_interaction(
                        interaction,
                        InteractionResponse::Approval { approved, reason },
                        &scope,
                    )
                    .await?;
            }
        }
        Ok(EffectDispatch {
            provider: "harness.native-execution.v1".into(),
            effect_id: EffectId::from_bytes(operation.into_bytes()),
            attempt_id: Self::attempt_id(operation, request)?,
            effect_kind: "host.process".into(),
            request: file,
            guarantee: EffectGuarantee::AtMostOnce,
            request_digest,
        })
    }
}

#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
impl NativeCommandAdmission for PersistentNativeCommandAdmission {
    fn admit<'a>(
        &'a self,
        operation: OperationId,
        request: &'a ExecutionSpec,
    ) -> BoxFuture<'a, Result<EffectDispatch>> {
        Box::pin(async move {
            request.validate()?;
            let interaction = Self::interaction_id(operation);
            let decision = self
                .decisions
                .lock()
                .map_err(|_| Error::Storage("native decision lock is poisoned".into()))?
                .get(&operation)
                .cloned()
                .unwrap_or(NativeCommandDecision::Pending);
            let mut approval = match &decision {
                NativeCommandDecision::Denied(reason) => ExecutionApproval::deny_for(
                    self.session.storage().session_id(),
                    interaction,
                    operation,
                    request.clone(),
                    reason.clone(),
                )?,
                _ => ExecutionApproval::approve_for(
                    self.session.storage().session_id(),
                    interaction,
                    operation,
                    request.clone(),
                )?,
            };
            let path = format!(".harness/execution/{operation}/approved-request.json");
            approval.bind_request_location(self.session.storage().volume(), &path)?;
            self.stage_and_resolve(operation, request, &approval, interaction, decision)
                .await
        })
    }

    fn reopen<'a>(
        &'a self,
        operation: OperationId,
        request: &'a ExecutionSpec,
    ) -> BoxFuture<'a, Result<EffectDispatch>> {
        Box::pin(async move {
            request.validate()?;
            let path = format!(".harness/execution/{operation}/approved-request.json");
            let (file, bytes) = self
                .session
                .storage()
                .read_private_path(&path, None)
                .await?;
            file.descriptor().verify(&bytes)?;
            let approval: ExecutionApproval = serde_json::from_slice(&bytes)
                .map_err(|error| Error::Invalid(format!("native approval is invalid: {error}")))?;
            approval.validate()?;
            if approval.operation_id != operation || approval.request != *request {
                return Err(Error::Conflict(
                    "native command approval changed after admission".into(),
                ));
            }
            let request_digest = crate::core::effect_request_digest(
                "harness.native-execution.v1",
                EffectGuarantee::AtMostOnce,
                "host.process",
                &file,
            )?;
            Ok(EffectDispatch {
                provider: "harness.native-execution.v1".into(),
                effect_id: EffectId::from_bytes(operation.into_bytes()),
                attempt_id: Self::attempt_id(operation, request)?,
                effect_kind: "host.process".into(),
                request: file,
                guarantee: EffectGuarantee::AtMostOnce,
                request_digest,
            })
        })
    }
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
