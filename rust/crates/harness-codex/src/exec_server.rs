//! Original-bound mediation for the pinned native Codex exec-server protocol.
use acyclic_harness::{Error, OperationId, Result};
/// One immutable registry binding for an upstream exec-server method. The
/// registered tool receives the complete upstream request without rewriting its
/// arguments, process IDs, write IDs, or optional telemetry attribution.
pub struct BuiltinToolBinding {
    /// Exact method exported by the pinned exec-server protocol.
    pub method: String,
    /// Exact admitted registry revision, including hidden host-only tools.
    pub tool: acyclic_harness::runtime::ToolRef<serde_json::Value, serde_json::Value>,
}

/// Durable dispatch gate for the real Codex builtin execution transport.
///
/// This opens no listener and grants no peer authority. The process host must
/// attach it to the owned, original-bound exec-server transport, forward genuine
/// provider notifications, and never install an unmediated/local fallback.
/// Root/provider authorization and financial permits belong inside the pinned
/// durable tool executors, before they forward any upstream request.
pub struct BuiltinMediator {
    context: acyclic_harness::runtime::TaskContext,
    bindings: std::collections::BTreeMap<String, acyclic_harness::runtime::ToolRef<serde_json::Value, serde_json::Value>>,
}

impl BuiltinMediator {
    /// Binds only actual admitted tools, never a claimed receipt or live-only
    /// executor. The caller supplies revisions from the accepted factory.
    pub fn new(
        context: acyclic_harness::runtime::TaskContext,
        bindings: Vec<BuiltinToolBinding>,
    ) -> Result<Self> {
        if context.durable_task_id().is_none() {
            return Err(Error::Unauthorized("builtin mediation requires an admitted durable task".into()));
        }
        let mut pinned = std::collections::BTreeMap::new();
        for binding in bindings {
            if !builtin_method(&binding.method) {
                return Err(Error::Unsupported(format!("unsupported exec-server method {}", binding.method)));
            }
            let definition = binding.tool.definition();
            let admitted = context.tool::<serde_json::Value, serde_json::Value>(
                &format!("{}@{}", definition.name, definition.revision),
            )?;
            if admitted.definition() != definition {
                return Err(Error::Conflict("builtin tool differs from its admitted revision".into()));
            }
            if pinned.insert(binding.method, binding.tool).is_some() {
                return Err(Error::Conflict("duplicate builtin method binding".into()));
            }
        }
        Ok(Self { context, bindings: pinned })
    }

    /// Original SDK parent, retained across transport reconnects.
    #[must_use]
    pub fn operation_id(&self) -> OperationId { self.context.id() }

    /// Admits/reconciles one actual upstream request through the SDK durable
    /// runner. A repeated ID with different method/arguments conflicts; it does
    /// not acquire a fresh mutation identity after a lost ACK.
    ///
    /// A successful SDK tool outcome is not a correlated protocol receipt until
    /// its actual response envelope validates. Missing, malformed, mismatched,
    /// or notification-only replies remain indeterminate under the original
    /// operation; this gate never dispatches a replacement invocation.
    pub async fn dispatch(
        &self,
        request: codex_exec_server_protocol::JSONRPCRequest,
    ) -> Result<codex_exec_server_protocol::JSONRPCMessage> {
        use acyclic_harness::{Outcome, executor::encode_json, tool::ToolInvocation};
        let binding = self.bindings.get(&request.method)
            .ok_or_else(|| Error::Unauthorized(format!("exec-server method {} is not admitted", request.method)))?;
        // Hash the tagged canonical ID: integer 1 and string "1" remain distinct.
        // Optional thread/tool attribution is telemetry, never a capability.
        let call_id = format!("codex-exec-{}", blake3::hash(&encode_json(&request.id)?).to_hex());
        let request_id = request.id.clone();
        let arguments = serde_json::to_value(request).map_err(|error| Error::Invalid(error.to_string()))?;
        let invocation = ToolInvocation::for_model_call(
            self.context.id(), 0, call_id, binding.definition().name.clone(), arguments,
        );
        let value = match self.context.call_durable(invocation.operation_id, binding, invocation.arguments).await? {
            Outcome::Succeeded(value) => value,
            Outcome::Failed { message } => return Err(Error::Invalid(message)),
            Outcome::Cancelled => return Err(Error::InteractionRejected(acyclic_harness::InteractionRejection::Cancelled)),
            Outcome::Indeterminate { operation_id } => return Err(Error::Indeterminate(operation_id)),
        };
        correlated_reply(value, &request_id, invocation.operation_id)
    }
}

fn correlated_reply(
    value: serde_json::Value,
    request_id: &codex_exec_server_protocol::RequestId,
    operation_id: OperationId,
) -> Result<codex_exec_server_protocol::JSONRPCMessage> {
    use codex_exec_server_protocol::JSONRPCMessage;
    // The durable runtime already owns decoded JSON. Consume that value through
    // the upstream bounded envelope decoder; do not encode and copy the entire
    // file/process/HTTP response merely to parse it a second time.
    let response: JSONRPCMessage = serde_json::from_value(value)
        .map_err(|_| Error::Indeterminate(operation_id))?;
    match &response {
        JSONRPCMessage::Response(reply) if &reply.id == request_id => Ok(response),
        JSONRPCMessage::Error(reply) if &reply.id == request_id => Ok(response),
        _ => Err(Error::Indeterminate(operation_id)),
    }
}

fn builtin_method(method: &str) -> bool {
    use codex_exec_server_protocol::*;
    matches!(method, INITIALIZE_METHOD | EXEC_METHOD | EXEC_READ_METHOD |
        EXEC_WRITE_METHOD | EXEC_SIGNAL_METHOD | EXEC_TERMINATE_METHOD |
        ENVIRONMENT_INFO_METHOD | ENVIRONMENT_STATUS_METHOD |
        ENVIRONMENT_CONFIG_READ_METHOD | FS_READ_FILE_METHOD | FS_OPEN_METHOD |
        FS_READ_BLOCK_METHOD | FS_CLOSE_METHOD | FS_WRITE_FILE_METHOD |
        FS_CREATE_DIRECTORY_METHOD | FS_GET_METADATA_METHOD | FS_CANONICALIZE_METHOD |
        FS_READ_DIRECTORY_METHOD | FS_WALK_METHOD | FS_REMOVE_METHOD | FS_COPY_METHOD |
        CAPABILITY_ROOTS_DISCOVER_METHOD | HTTP_REQUEST_METHOD)
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_exec_server_protocol::{
        JSONRPCError, JSONRPCErrorError, JSONRPCMessage, JSONRPCNotification,
        JSONRPCResponse, RequestId,
    };

    #[test]
    fn actual_upstream_response_and_error_keep_the_original_request_identity() -> Result<()> {
        let operation = OperationId::from_bytes([7; 16]);
        let id = RequestId::String("original-process-start".into());
        let response = JSONRPCMessage::Response(JSONRPCResponse {
            id: id.clone(),
            result: serde_json::json!({"processId": "actual-original-process"}),
        });
        let encoded = serde_json::to_value(&response)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(correlated_reply(encoded, &id, operation)?, response);
        let rejected = JSONRPCMessage::Error(JSONRPCError {
            id: id.clone(),
            error: JSONRPCErrorError {
                code: -32602,
                data: None,
                message: "original request rejected".into(),
            },
        });
        let encoded = serde_json::to_value(&rejected)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(correlated_reply(encoded, &id, operation)?, rejected);
        Ok(())
    }

    #[test]
    fn malformed_mismatched_and_notification_replies_remain_original_indeterminate() -> Result<()> {
        let operation = OperationId::from_bytes([11; 16]);
        let id = RequestId::Integer(1);
        let mismatched = JSONRPCMessage::Response(JSONRPCResponse {
            id: RequestId::String("1".into()),
            result: serde_json::json!({"processId": "original-process"}),
        });
        let notification = JSONRPCMessage::Notification(JSONRPCNotification {
            method: codex_exec_server_protocol::EXEC_EXITED_METHOD.into(),
            params: Some(serde_json::json!({"processId": "original-process", "exitCode": 0})),
        });
        let values = [
            serde_json::Value::Null,
            serde_json::json!({"id": 1, "error": {"code": "not-an-integer", "message": "invalid provider reply"}}),
            serde_json::to_value(mismatched).map_err(|error| Error::Invalid(error.to_string()))?,
            serde_json::to_value(notification).map_err(|error| Error::Invalid(error.to_string()))?,
        ];
        for value in values {
            assert!(matches!(
                correlated_reply(value, &id, operation),
                Err(Error::Indeterminate(actual)) if actual == operation
            ));
        }
        Ok(())
    }
}
