//! Model-facing native command contract.
//!
//! This module contains only the typed tool boundary.  Process ownership,
//! approval verification, durable receipts, and recovery remain in the host
//! execution provider.  A composition supplies a [`NativeCommandHost`] when
//! it wants to expose the command tool; the default local composition keeps
//! the optional integration unloaded.

use crate::{
    Error, OperationId, Result,
    host_execution::ExecutionSpec,
    tool::{
        Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolProjection, ToolRegistry,
        ToolResult,
    },
};
use futures::future::BoxFuture;
use serde_json::{Value, json};
use std::sync::Arc;

/// Stable model-facing native command name.
pub const NATIVE_COMMAND_TOOL: &str = "acyclic.shell";
/// Revision of the native command contract.
pub const NATIVE_COMMAND_REVISION: &str = "1";

/// Host boundary for one already admitted native command invocation.
///
/// The host receives the exact model operation identity and validated
/// [`ExecutionSpec`].  Implementations should route this request through the
/// existing native effect provider, including its approval, receipt, process
/// cleanup, and reconciliation rules.  This interface deliberately has no
/// workspace or ambient environment access.
pub trait NativeCommandHost: Send + Sync {
    /// Dispatches an admitted command after Harness policy and interaction
    /// checks have succeeded.
    fn execute<'a>(
        &'a self,
        operation_id: OperationId,
        request: ExecutionSpec,
    ) -> BoxFuture<'a, Result<ToolResult>>;

    /// Reconciles a previously started command without spawning it again.
    fn reconcile<'a>(
        &'a self,
        operation_id: OperationId,
        request: ExecutionSpec,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>>;

    /// Projects a private provider result into the bounded model contract.
    fn project(
        &self,
        operation_id: OperationId,
        request: &ExecutionSpec,
        result: &ToolResult,
    ) -> Result<Value>;
}

/// One optional native command binding for a local Harness composition.
///
/// Policy is supplied by the host composition, so this type cannot mint an
/// operator authority or silently decide whether a command is allowed.
#[derive(Clone)]
pub struct NativeCommandBinding {
    host: Arc<dyn NativeCommandHost>,
    policy: Arc<dyn crate::runtime::ToolPolicy>,
}

impl NativeCommandBinding {
    /// Binds a host execution adapter and its owner-selected policy.
    #[must_use]
    pub fn new(
        host: Arc<dyn NativeCommandHost>,
        policy: Arc<dyn crate::runtime::ToolPolicy>,
    ) -> Self {
        Self { host, policy }
    }

    /// Returns the process/effect adapter retained by this binding.
    #[must_use]
    pub fn host(&self) -> Arc<dyn NativeCommandHost> {
        self.host.clone()
    }

    /// Returns the policy retained by this binding.
    #[must_use]
    pub fn policy(&self) -> Arc<dyn crate::runtime::ToolPolicy> {
        self.policy.clone()
    }
}

struct NativeCommandTool {
    host: Arc<dyn NativeCommandHost>,
}

impl ToolExecutor for NativeCommandTool {
    fn execute<'a>(&'a self, invocation: ToolInvocation) -> BoxFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            let request: ExecutionSpec = serde_json::from_value(invocation.arguments.clone())
                .map_err(|error| {
                    Error::Invalid(format!("invalid native command request: {error}"))
                })?;
            request.validate()?;
            self.host.execute(invocation.operation_id, request).await
        })
    }

    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move {
            let request: ExecutionSpec = serde_json::from_value(invocation.arguments.clone())
                .map_err(|error| {
                    Error::Invalid(format!("invalid native command request: {error}"))
                })?;
            request.validate()?;
            self.host.reconcile(invocation.operation_id, request).await
        })
    }
}

impl ToolProjection for NativeCommandTool {
    fn project(&self, invocation: &ToolInvocation, result: &ToolResult) -> Result<Value> {
        let request: ExecutionSpec = serde_json::from_value(invocation.arguments.clone())
            .map_err(|error| Error::Invalid(format!("invalid native command request: {error}")))?;
        request.validate()?;
        self.host.project(invocation.operation_id, &request, result)
    }
}

/// Constructs the exact native command tool registry for one composition.
pub fn native_command_tools(binding: &NativeCommandBinding) -> Result<ToolRegistry> {
    let implementation = Arc::new(NativeCommandTool {
        host: binding.host(),
    });
    let mut registry = ToolRegistry::new();
    registry.register(Tool {
        definition: native_command_definition(),
        executor: implementation.clone(),
        projection: implementation,
    })?;
    Ok(registry)
}

/// Returns the pinned model-visible native command contract.
#[must_use]
pub fn native_command_definition() -> ToolDefinition {
    ToolDefinition {
        name: NATIVE_COMMAND_TOOL.into(),
        revision: NATIVE_COMMAND_REVISION.into(),
        description: "Execute one owner-admitted native command with an explicit executable, arguments, working directory, environment, timeout, and output bound.".into(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "executable": {"type": "string", "minLength": 1},
                "arguments": {"type": "array", "items": {"type": "string"}},
                "working_directory": {"type": "string", "minLength": 1},
                "environment": {
                    "type": "object",
                    "properties": {
                        "kind": {"enum": ["clear", "explicit"]},
                        "variables": {"type": "object", "additionalProperties": {"type": "string"}}
                    },
                    "required": ["kind"],
                    "additionalProperties": false
                },
                "timeout_ms": {"type": ["integer", "null"]},
                "max_output_bytes": {"type": "integer", "minimum": 1}
            },
            "required": ["executable", "arguments", "working_directory", "environment", "max_output_bytes"],
            "additionalProperties": false
        }),
        output_schema: native_command_output_schema(),
        model_output_schema: native_command_output_schema(),
    }
}

fn native_command_output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "status": {"enum": ["exited", "timed_out", "cancelled", "unknown"]},
            "exit_code": {"type": ["integer", "null"]},
            "stdout": {"type": "string"},
            "stderr": {"type": "string"},
            "reason": {"type": ["string", "null"]}
        },
        "required": ["status", "exit_code", "stdout", "stderr", "reason"],
        "additionalProperties": false
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        registry::ComponentIdentity,
        runtime::{RuntimeScope, ToolPolicy, ToolPolicyDecision},
    };
    use futures::FutureExt;
    use std::sync::Mutex;

    struct AllowPolicy;

    impl ToolPolicy for AllowPolicy {
        fn identity(&self) -> ComponentIdentity {
            ComponentIdentity {
                name: "test.native-policy".into(),
                version: "1".into(),
                digest: [3; 32],
            }
        }

        fn evaluate<'a>(
            &'a self,
            _invocation: &'a ToolInvocation,
            _scope: &'a RuntimeScope,
        ) -> BoxFuture<'a, Result<ToolPolicyDecision>> {
            async { Ok(ToolPolicyDecision::Allow) }.boxed()
        }
    }

    struct RecordingHost {
        requests: Mutex<Vec<(OperationId, ExecutionSpec)>>,
    }

    impl NativeCommandHost for RecordingHost {
        fn execute<'a>(
            &'a self,
            operation_id: OperationId,
            request: ExecutionSpec,
        ) -> BoxFuture<'a, Result<ToolResult>> {
            self.requests
                .lock()
                .expect("recording host mutex")
                .push((operation_id, request));
            async { Ok(ToolResult { value: json!({
                "status": "exited", "exit_code": 0, "stdout": "ok", "stderr": "", "reason": null
            }) }) }.boxed()
        }

        fn reconcile<'a>(
            &'a self,
            _operation_id: OperationId,
            _request: ExecutionSpec,
        ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
            async { Ok(None) }.boxed()
        }

        fn project(
            &self,
            _operation_id: OperationId,
            _request: &ExecutionSpec,
            result: &ToolResult,
        ) -> Result<Value> {
            Ok(result.value.clone())
        }
    }

    fn request() -> Value {
        json!({
            "executable": if cfg!(windows) { r"C:\\Windows\\System32\\cmd.exe" } else { "/bin/echo" },
            "arguments": if cfg!(windows) { json!(["/C", "echo", "ok"]) } else { json!(["ok"]) },
            "working_directory": if cfg!(windows) { r"C:\\Windows" } else { "/tmp" },
            "environment": {"kind": "clear"},
            "timeout_ms": 1000,
            "max_output_bytes": 4096
        })
    }

    #[test]
    fn definition_is_closed_and_versioned() -> Result<()> {
        let definition = native_command_definition();
        definition.validate()?;
        assert_eq!(definition.name, NATIVE_COMMAND_TOOL);
        assert_eq!(definition.revision, NATIVE_COMMAND_REVISION);
        assert_eq!(
            definition.input_schema["additionalProperties"],
            Value::Bool(false)
        );
        Ok(())
    }

    #[tokio::test]
    async fn tool_passes_exact_operation_and_request_to_host() -> Result<()> {
        let host = Arc::new(RecordingHost {
            requests: Mutex::new(Vec::new()),
        });
        let binding = NativeCommandBinding::new(host.clone(), Arc::new(AllowPolicy));
        let registry = native_command_tools(&binding)?;
        let tool = registry
            .get(NATIVE_COMMAND_TOOL)
            .ok_or_else(|| Error::NotFound("native command".into()))?;
        let operation = OperationId::from_bytes([7; 16]);
        let invocation = ToolInvocation {
            operation_id: operation,
            call_id: "native-call".into(),
            name: NATIVE_COMMAND_TOOL.into(),
            arguments: request(),
        };
        let result = tool.executor.execute(invocation.clone()).await?;
        assert_eq!(result.value["status"], "exited");
        let requests = host.requests.lock().expect("recording host mutex");
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].0, operation);
        // The host sees the exact validated provider request represented by
        // the model arguments; no defaults or ambient fields are inserted.
        assert_eq!(
            serde_json::to_value(&requests[0].1)
                .map_err(|error| Error::Invalid(error.to_string()))?,
            invocation.arguments
        );
        Ok(())
    }

    #[test]
    fn malformed_request_cannot_reach_host() -> Result<()> {
        let definition = native_command_definition();
        let mut value = request();
        value["unexpected"] = Value::Bool(true);
        assert!(crate::tool::validate_value(&definition.input_schema, &value, "input").is_err());
        let relative = serde_json::from_value::<ExecutionSpec>(json!({
            "executable": "relative.exe",
            "arguments": [],
            "working_directory": ".",
            "environment": {"kind": "clear"},
            "max_output_bytes": 1
        }))
        .map_err(|error| Error::Invalid(format!("invalid test request: {error}")))?;
        assert!(relative.validate().is_err());
        Ok(())
    }
}
