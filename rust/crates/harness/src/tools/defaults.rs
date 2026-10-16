use crate::{
    Error, Result,
    runtime::{RuntimeScope, ToolContext},
    tool::{
        JsonToolProjection, Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolRegistry,
        ToolResult,
    },
};
use acyclic_stream::BoxProviderFuture;
use serde_json::json;
use std::sync::Arc;

/// The standard coding toolset. Custom tools can be added to this registry.
pub fn defaults() -> Result<ToolRegistry> {
    let mut registry = ToolRegistry::default();
    bind_defaults(&mut registry, &mut Vec::new(), None, true, true)?;
    Ok(registry)
}

pub(crate) fn bind_defaults(
    registry: &mut ToolRegistry,
    grants: &mut Vec<String>,
    bound: Option<&RuntimeScope>,
    volume: bool,
    workspace: bool,
) -> Result<()> {
    for (name, available) in [
        ("bash", workspace),
        ("fs", workspace),
        ("read", volume),
        ("write", volume),
        ("edit", volume),
    ] {
        if !available {
            continue;
        }
        let capability = crate::contract::capability::tool_call(name);
        if bound.is_some_and(|scope| !scope.grants().contains(&capability)) {
            continue;
        }
        let tool = match name {
            "bash" => command(
                name,
                "Run a bash command in the calling task's composed workspace view. No OS sandbox in release 1; writes outside the task view are not captured",
            )?,
            "fs" => command(
                name,
                "Run one git facade command, including diff and merge of agents/<ref>; --volume <name> selects a granted mount for diff, log, show, status or merge",
            )?,
            "read" => crate::tool::files::read()?,
            "write" => crate::tool::files::write()?,
            "edit" => crate::tool::files::edit()?,
            _ => unreachable!(),
        };
        registry.register((tool, None), &mut None, None)?;
        if bound.is_none() && !grants.contains(&capability) {
            grants.push(capability);
        }
    }
    Ok(())
}

fn command(name: &'static str, description: &str) -> Result<Tool> {
    let output = json!({"type":"object"});
    Ok(Tool {
        hook_names: if name == "bash" { &["Bash"] } else { &[] },
        definition: ToolDefinition {
            name: name.into(),
            revision: "1".into(),
            description: description.into(),
            input_schema: if name == "bash" {
                crate::tool::schema::input::<super::BashInput>()?
            } else {
                json!({
                    "type":"object", "required":["command"],
                    "properties":{"command":{"type":"string","minLength":1}},
                    "additionalProperties":false
                })
            },
            output_schema: output.clone(),
            projection_schema: crate::tool::json_projection_schema(output),
        },
        executor: Arc::new(WorkspaceExecutor { name }),
        projection: Arc::new(JsonToolProjection),
    })
}

pub(crate) fn workspace<'a>(
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
