//! Task-bound delegation for the stock workspace commands. Every host's
//! `shell` and `fs` pass here, so their bounds ([`super::bounds`]) hold for
//! each backend without a per-backend limit.
use crate::{
    Error, Result,
    runtime::{FilesystemExecutor, RuntimeScope, Target, ToolContext},
    tool::{ToolExecutor, ToolInvocation, ToolResult},
};
use acyclic_stream::BoxProviderFuture;
use std::sync::Arc;

pub(super) fn executor(name: &'static str) -> Arc<dyn ToolExecutor> {
    Arc::new(WorkspaceExecutor { name })
}

enum Workspace {
    Target(Arc<Target>),
    Filesystem(Arc<dyn FilesystemExecutor>),
}

impl Workspace {
    fn executor(&self) -> &dyn ToolExecutor {
        match self {
            Self::Target(target) => &*target.executor,
            Self::Filesystem(executor) => &**executor,
        }
    }
}

/// The exact machine or provider-backed filesystem this call runs on.
fn target(
    context: &ToolContext,
    invocation: &ToolInvocation,
    work: crate::NativeWork<'_>,
    binding: crate::tool::WorkspaceBinding,
) -> Result<Workspace> {
    if let Some(target) = &context.execution_target {
        return Ok(Workspace::Target(Arc::clone(target)));
    }
    if invocation.name == "fs"
        && let Some(filesystem) = &context.task().harness.filesystem
    {
        return Ok(Workspace::Filesystem(Arc::clone(filesystem)));
    }
    let unbound = || {
        boundary(
            work,
            crate::wire::ErrorCode::Unsupported,
            "workspace tools are not bound",
        )
    };
    let targets = context.task().harness.targets().ok_or_else(unbound)?;
    crate::runtime::call_target(&**targets, invocation, Some(binding))
        .ok_or_else(unbound)?
        .map(Workspace::Target)
}

/// Every shell result names the target and command language it ran under.
/// Also returns the retained bytes the result grew by, which a native
/// caller charges to the result's allocation.
fn echo(name: &str, target: &Workspace, mut result: ToolResult) -> Result<(ToolResult, u64)> {
    let mut grown = 0u64;
    if name == super::shell::PLATFORM.name
        && let Workspace::Target(target) = target
        && let serde_json::Value::Object(fields) = &mut result.value
    {
        for (key, value) in [
            ("target", target.record.name.clone()),
            ("shell", target.record.shell.clone()),
        ] {
            let bytes = crate::executor::json::json_object_insert(fields, key, value.into())?;
            grown = grown
                .checked_add(bytes)
                .ok_or_else(|| Error::Invalid("shell result size overflows".into()))?;
        }
    }
    Ok((result, grown))
}

/// [`echo`] on a native result, its growth charged to `allocation`.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn echo_charged(
    name: &str,
    target: &Workspace,
    result: ToolResult,
    allocation: &mut acyclic_fs::NativeAllocation,
) -> Result<ToolResult> {
    let (result, grown) = echo(name, target, result)?;
    allocation.grow(grown)?;
    Ok(result)
}

struct WorkspaceExecutor {
    name: &'static str,
}

impl WorkspaceExecutor {
    fn binding(&self) -> crate::tool::WorkspaceBinding {
        if self.name == super::shell::PLATFORM.name {
            crate::tool::WorkspaceBinding::Named
        } else {
            crate::tool::WorkspaceBinding::Home
        }
    }
}

impl ToolExecutor for WorkspaceExecutor {
    fn authorize_replay<'a>(
        &'a self,
        context: Option<&'a ToolContext>,
        invocation: &'a ToolInvocation,
        work: crate::NativeWork<'a>,
    ) -> Result<Option<crate::NativeFutureBundle<'a, ()>>> {
        let Some(context) = context else {
            return Ok(None);
        };
        if let Some(target) = &context.execution_target {
            target
                .executor
                .authorize_replay(Some(context), invocation, work)
        } else if invocation.name == "fs" {
            let Some(executor) = &context.task().harness.filesystem else {
                return Ok(None);
            };
            executor.authorize_replay(Some(context), invocation, work)
        } else {
            Ok(None)
        }
    }

    fn workspace_binding(&self) -> Option<crate::tool::WorkspaceBinding> {
        Some(self.binding())
    }

    fn prepare_shell_script(
        &self,
        source: &str,
        input: &serde_json::Value,
        context: Option<&ToolContext>,
        work: crate::NativeWork<'_>,
    ) -> Result<super::ShellInput> {
        if self.name != "shell" {
            return Err(boundary(
                work,
                crate::wire::ErrorCode::Unsupported,
                "script projection requires shell; recovery: select the admitted shell tool",
            ));
        }
        let context = context.ok_or_else(|| {
            boundary(
                work,
                crate::wire::ErrorCode::Unauthorized,
                "script projection lacks its task; recovery: retain the original tool context",
            )
        })?;
        context
            .task()
            .harness
            .target(None)?
            .executor
            .prepare_shell_script(source, input, Some(context), work)
    }

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
        match target(context, invocation, work, self.binding()) {
            Ok(target) => target
                .executor()
                .authorize(scope, Some(context), invocation, work),
            // A target that cannot take the call is no authorization failure:
            // the call settles with that refusal and the run continues.
            Err(Error::Target(_)) => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn dispatch(
        &self,
        invocation: &ToolInvocation,
        context: Option<&ToolContext>,
    ) -> Result<Option<crate::tool::Dispatch>> {
        let Some(context) = context else {
            return Ok(None);
        };
        if super::bounds::operands(&invocation.arguments, super::bounds::limits(context).1)
        .is_err()
        {
            // Ordinary execution returns this same rejection to the model.
            // Nothing may be handed to a remote target before that verdict.
            return Ok(None);
        }
        target(context, invocation, None, self.binding())?
            .executor()
            .dispatch(invocation, Some(context))
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
            let limits = super::bounds::limits(&context);
            if let Err(error) = super::bounds::operands(&invocation.arguments, limits.1) {
                return Ok(super::rejected_input(error));
            }
            let target = target(&context, &invocation, None, self.binding())?;
            let result = target.executor().execute(invocation, Some(context)).await?;
            super::bounds::result(self.name, echo(self.name, &target, result)?.0, limits.0)
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
            if context.execution_target.is_none()
                && !(invocation.name == "fs" && context.task().harness.filesystem.is_some())
            {
                return Err(Error::Indeterminate(invocation.operation_id));
            }
            self.authorize(None, Some(&context), &invocation, None)?;
            let limits = super::bounds::limits(&context);
            if let Err(error) = super::bounds::operands(&invocation.arguments, limits.1) {
                return Ok(Some(super::rejected_input(error)));
            }
            let target = target(&context, &invocation, None, self.binding())?;
            target
                .executor()
                .reconcile(invocation, Some(context))
                .await?
                .map(|result| {
                    super::bounds::result(self.name, echo(self.name, &target, result)?.0, limits.0)
                })
                .transpose()
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
        let target = target(context, invocation, Some(work), self.binding())?;
        let name = self.name;
        crate::NativeFutureBundle::new(work, move || async move {
            let (result, mut allocation) = target
                .executor()
                .execute_with_native_context(context, invocation, work)?
                .await?;
            let result = echo_charged(name, &target, result, &mut allocation)?;
            Ok((result, allocation))
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn reconcile_with_native_context<'a>(
        &'a self,
        context: &'a ToolContext,
        invocation: &'a ToolInvocation,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<ToolResult>, acyclic_fs::NativeAllocation)>>
    {
        if context.execution_target.is_none()
            && !(invocation.name == "fs" && context.task().harness.filesystem.is_some())
        {
            return Err(Error::Indeterminate(invocation.operation_id));
        }
        self.authorize(None, Some(context), invocation, Some(work))?;
        let target = target(context, invocation, Some(work), self.binding())?;
        let name = self.name;
        crate::NativeFutureBundle::new(work, move || async move {
            let (result, mut allocation) = target
                .executor()
                .reconcile_with_native_context(context, invocation, work)?
                .await?;
            let result = result
                .map(|result| echo_charged(name, &target, result, &mut allocation))
                .transpose()?;
            Ok((result, allocation))
        })
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
