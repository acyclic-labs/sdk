//! Task-bound delegation for the stock workspace commands.
use crate::{
    Error, Result,
    runtime::{RuntimeScope, ToolContext},
    tool::{ToolExecutor, ToolInvocation, ToolResult},
};
use acyclic_stream::BoxProviderFuture;
use std::sync::Arc;

pub(super) fn executor(name: &'static str) -> Arc<dyn ToolExecutor> {
    Arc::new(WorkspaceExecutor { name })
}

fn workspace<'a>(
    context: &'a ToolContext,
    work: crate::NativeWork<'_>,
) -> Result<&'a Arc<dyn ToolExecutor>> {
    context
        .task()
        .harness
        .workspace_tools
        .as_ref()
        .ok_or_else(|| {
            boundary(
                work,
                crate::wire::ErrorCode::Unsupported,
                "workspace tools are not bound",
            )
        })
}

struct WorkspaceExecutor {
    name: &'static str,
}

impl ToolExecutor for WorkspaceExecutor {
    fn authorize(
        &self,
        scope: Option<&RuntimeScope>,
        context: Option<&ToolContext>,
        invocation: &ToolInvocation,
        work: crate::NativeWork<'_>,
    ) -> Result<()> {
        let context = context.ok_or_else(|| {
            boundary(
                work,
                crate::wire::ErrorCode::Unauthorized,
                "workspace tool requires its task context",
            )
        })?;
        if invocation.name != self.name
            || context.operation_id() != invocation.operation_id
            || context.call_id() != invocation.call_id
        {
            return Err(boundary(
                work,
                crate::wire::ErrorCode::Unauthorized,
                "workspace tool call identity changed",
            ));
        }
        workspace(context, work)?.authorize(scope, Some(context), invocation, work)
    }

    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            let context = context.ok_or_else(|| {
                boundary(
                    None,
                    crate::wire::ErrorCode::Unauthorized,
                    "workspace tool requires its task context",
                )
            })?;
            self.authorize(None, Some(&context), &invocation, None)?;
            if invocation.name == "fs" {
                if let Some(command) = invocation
                    .arguments
                    .get("command")
                    .and_then(serde_json::Value::as_str)
                    && let Some(result) = super::agent_files::inspect(&context, command).await?
                {
                    return Ok(result);
                }
            }
            let executor = workspace(&context, None)?.clone();
            executor.execute(invocation, Some(context)).await
        })
    }

    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move {
            let context = context.ok_or_else(|| {
                boundary(
                    None,
                    crate::wire::ErrorCode::Unauthorized,
                    "workspace tool requires its task context",
                )
            })?;
            self.authorize(None, Some(&context), &invocation, None)?;
            if invocation.name == "fs" {
                if let Some(command) = invocation
                    .arguments
                    .get("command")
                    .and_then(serde_json::Value::as_str)
                    && let Some(result) = super::agent_files::inspect(&context, command).await?
                {
                    return Ok(Some(result));
                }
            }
            let executor = workspace(&context, None)?.clone();
            executor.reconcile(invocation, Some(context)).await
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_execution(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        work.remaining()?;
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn execute_with_native_context<'a>(
        &'a self,
        context: &'a ToolContext,
        invocation: &'a ToolInvocation,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (ToolResult, acyclic_fs::NativeAllocation)>> {
        self.authorize(None, Some(context), invocation, Some(work))?;
        workspace(context, Some(work))?.execute_with_native_context(context, invocation, work)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn reconcile_with_native_context<'a>(
        &'a self,
        context: &'a ToolContext,
        invocation: &'a ToolInvocation,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<ToolResult>, acyclic_fs::NativeAllocation)>>
    {
        self.authorize(None, Some(context), invocation, Some(work))?;
        workspace(context, Some(work))?.reconcile_with_native_context(context, invocation, work)
    }
}

fn boundary(
    work: crate::NativeWork<'_>,
    code: crate::wire::ErrorCode,
    reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.is_some() {
        return Error::NativeBoundary { code, reason };
    }
    let _ = work;
    match code {
        crate::wire::ErrorCode::Unsupported => Error::Unsupported(reason.into()),
        _ => Error::Unauthorized(reason.into()),
    }
}
