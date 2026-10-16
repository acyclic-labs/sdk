use crate::mcp::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};
use crate::{Error, OperationId, Result, mcp::McpToolTransport, runtime::ToolContext};
use acyclic_native_runtime::{ProcessTree, run_blocking_io};
use acyclic_stream::BoxProviderFuture;
use futures::StreamExt as _;
use rmcp::{
    RoleClient, ServiceExt as _,
    model::{
        CallToolRequest, CallToolRequestParams, ClientConfig, ClientJsonRpcMessage, ClientRequest,
        Implementation, ProtocolVersion, ServerJsonRpcMessage, ServerResult,
    },
    service::{PeerRequestOptions, RunningService, RunningServiceCancellationToken},
};
use serde_json::Value;
#[cfg(test)]
use serde_json::json;
use std::{
    process::{Command, Stdio},
    time::Duration,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{FramedRead, FramedWrite};

mod codec;
use codec::BoundedCodec;

/// One initialized, bounded MCP connection and its owned native process tree.
pub struct StdioMcpTransport {
    service: RunningService<RoleClient, ClientConfig>,
    timeout: Duration,
    maximum_bytes: Arc<AtomicU32>,
    _process: Option<ProcessOwner>,
}

impl StdioMcpTransport {
    /// Selects the independent tool-exchange bounds after catalog discovery.
    pub fn with_tool_limits(mut self, maximum_bytes: u32, timeout_ms: u32) -> Result<Self> {
        validate_bounds(maximum_bytes, timeout_ms)?;
        self.maximum_bytes.store(maximum_bytes, Ordering::Release);
        self.timeout = Duration::from_millis(u64::from(timeout_ms));
        Ok(self)
    }

    /// Lists one typed catalog page without an intermediate JSON representation.
    pub async fn list_tools(
        &self,
        operation: OperationId,
        cursor: Option<String>,
    ) -> Result<rmcp::model::ListToolsResult> {
        let request = rmcp::model::ListToolsRequest::with_param(
            rmcp::model::PaginatedRequestParams::default().with_cursor(cursor),
        );
        match self
            .exchange(
                operation,
                "tools/list",
                ClientRequest::ListToolsRequest(request),
            )
            .await?
        {
            ServerResult::ListToolsResult(result) => Ok(result),
            _ => Err(Error::Invalid(
                "MCP tools/list returned another result type".into(),
            )),
        }
    }

    /// Starts one host-selected executable and keeps it for the connection's lifetime.
    /// Arguments, environment and workspace are supplied through `command`.
    pub async fn spawn(mut command: Command, maximum_bytes: u32, timeout_ms: u32) -> Result<Self> {
        validate_bounds(maximum_bytes, timeout_ms)?;
        acyclic_native_runtime::initialize_native_io_services().map_err(Error::process_io)?;
        let timeout = Duration::from_millis(u64::from(timeout_ms));
        let runtime = tokio::runtime::Handle::current();
        tokio::time::timeout(timeout, async move {
            let mut process = run_blocking_io(
                move || {
                    command
                        .stdin(Stdio::piped())
                        .stdout(Stdio::piped())
                        .stderr(Stdio::inherit());
                    ProcessTree::spawn(&mut command).map(|tree| ProcessOwner {
                        tree: std::sync::Mutex::new(Some(tree)),
                        runtime,
                    })
                },
                None,
            )
            .map_err(Error::process_io)?
            .await
            .map_err(Error::process_io)?
            .map_err(Error::process_io)?;
            let tree = process
                .tree
                .get_mut()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_mut()
                .ok_or_else(|| Error::Invalid("MCP child missing".into()))?;
            let input = tree
                .take_stdin()
                .ok_or_else(|| Error::Invalid("MCP stdin pipe missing".into()))?;
            let output = tree
                .take_stdout()
                .ok_or_else(|| Error::Invalid("MCP stdout pipe missing".into()))?;
            let input = tokio::process::ChildStdin::from_std(input).map_err(Error::process_io)?;
            let output =
                tokio::process::ChildStdout::from_std(output).map_err(Error::process_io)?;
            Self::bind(output, input, maximum_bytes, timeout, Some(process)).await
        })
        .await
        .map_err(|_| Error::Process {
            kind: "timeout",
            message: "MCP initialization deadline elapsed".into(),
        })?
    }

    async fn bind<R, W>(
        read: R,
        write: W,
        maximum_bytes: u32,
        timeout: Duration,
        process: Option<ProcessOwner>,
    ) -> Result<Self>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let maximum_bytes = Arc::new(AtomicU32::new(maximum_bytes));
        let read = FramedRead::new(
            read,
            BoundedCodec::<ServerJsonRpcMessage>::new(Arc::clone(&maximum_bytes)),
        )
        .take_while(|message| std::future::ready(message.is_ok()))
        .filter_map(|message| std::future::ready(message.ok()));
        let write = FramedWrite::new(
            write,
            BoundedCodec::<ClientJsonRpcMessage>::new(Arc::clone(&maximum_bytes)),
        );
        let client = ClientConfig::new(
            Default::default(),
            Implementation::new("acyclic-harness", env!("CARGO_PKG_VERSION")),
        )
        .with_protocol_version(ProtocolVersion::V_2025_11_25);
        let service = client
            .serve((write, read))
            .await
            .map_err(|error| Error::Invalid(format!("MCP initialization: {error}")))?;
        if service
            .peer_info()
            .is_none_or(|info| info.protocol_version != ProtocolVersion::V_2025_11_25)
        {
            return Err(Error::Unsupported(
                "MCP protocol version differs from the selected binding".into(),
            ));
        }
        Ok(Self {
            service,
            timeout,
            maximum_bytes,
            _process: process,
        })
    }

    /// Exchanges a supported service request without reconnecting or retrying it.
    pub async fn rpc(
        &self,
        operation: OperationId,
        method: &str,
        params: Value,
        context: Option<ToolContext>,
    ) -> Result<Value> {
        crate::mcp::validate_operation_context(operation, context.as_ref())?;
        let request = requests::request(method, params)?;
        let result = self.exchange(operation, method, request).await?;
        requests::result(method, result)
    }

    async fn exchange(
        &self,
        operation: OperationId,
        method: &str,
        request: ClientRequest,
    ) -> Result<ServerResult> {
        let deadline = tokio::time::Instant::now() + self.timeout;
        let mut cancellation = CancelOnDrop {
            service: Some(self.service.cancellation_token()),
            process: self._process.as_ref(),
        };
        let mut handle = tokio::time::timeout_at(
            deadline,
            self.service
                .send_request_with_option(request, PeerRequestOptions::with_timeout(self.timeout)),
        )
        .await
        .map_err(|_| Error::Indeterminate(operation))?
        .map_err(|_| Error::Indeterminate(operation))?;
        handle.options.timeout =
            Some(deadline.saturating_duration_since(tokio::time::Instant::now()));
        // rmcp sends notifications/cancelled and retires its pending responder.
        // If notification delivery stalls, close the connection within a bounded grace period.
        let result = tokio::time::timeout_at(
            deadline + self.timeout.min(Duration::from_millis(100)),
            handle.await_response(),
        )
        .await
        .map_err(|_| Error::Indeterminate(operation))?;
        if result.is_ok()
            || matches!(
                &result,
                Err(rmcp::ServiceError::Timeout { .. } | rmcp::ServiceError::McpError(_))
            )
        {
            cancellation.service.take();
        }
        result.map_err(|error| match error {
            rmcp::ServiceError::McpError(error) => Error::Invalid(format!("MCP {method}: {error}")),
            _ => Error::Indeterminate(operation),
        })
    }
}

impl McpToolTransport for StdioMcpTransport {
    fn call<'a>(
        &'a self,
        operation: OperationId,
        name: &'a str,
        arguments: Value,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<rmcp::model::CallToolResult>> {
        Box::pin(async move {
            crate::mcp::validate_operation_context(operation, context.as_ref())?;
            let Value::Object(arguments) = arguments else {
                return Err(Error::Invalid(
                    "MCP tool arguments must be an object".into(),
                ));
            };
            let request = CallToolRequest::new(
                CallToolRequestParams::new(name.to_owned()).with_arguments(arguments),
            );
            match self
                .exchange(
                    operation,
                    "tools/call",
                    ClientRequest::CallToolRequest(request),
                )
                .await?
            {
                ServerResult::CallToolResult(result) => Ok(result),
                _ => Err(Error::Invalid(
                    "MCP tools/call returned another result type".into(),
                )),
            }
        })
    }

    fn reconcile<'a>(
        &'a self,
        operation: OperationId,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<rmcp::model::CallToolResult>>> {
        Box::pin(async move {
            crate::mcp::validate_operation_context(operation, context.as_ref())?;
            // A protocol request ID is not a durable server receipt.
            Ok(None)
        })
    }
}

struct CancelOnDrop<'a> {
    service: Option<RunningServiceCancellationToken>,
    process: Option<&'a ProcessOwner>,
}
impl Drop for CancelOnDrop<'_> {
    fn drop(&mut self) {
        if let Some(token) = self.service.take() {
            token.cancel();
            if let Some(process) = self.process {
                process.close();
            }
        }
    }
}

struct ProcessOwner {
    tree: std::sync::Mutex<Option<ProcessTree>>,
    runtime: tokio::runtime::Handle,
}
impl ProcessOwner {
    fn close(&self) {
        let tree = self
            .tree
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(tree) = tree {
            drop_on_worker(&self.runtime, tree);
        }
    }
}
impl Drop for ProcessOwner {
    fn drop(&mut self) {
        if let Some(tree) = self
            .tree
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            drop_on_worker(&self.runtime, tree);
        }
    }
}

fn drop_on_worker<T: Send + 'static>(runtime: &tokio::runtime::Handle, value: T) {
    drop(runtime.spawn_blocking(move || drop(value)));
}

mod requests;

fn validate_bounds(maximum_bytes: u32, timeout_ms: u32) -> Result<()> {
    if maximum_bytes == 0
        || timeout_ms == 0
        || usize::try_from(maximum_bytes)
            .ok()
            .and_then(|bytes| bytes.checked_add(1))
            .is_none()
    {
        return Err(Error::Invalid(
            "MCP stdio needs positive finite bounds".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
