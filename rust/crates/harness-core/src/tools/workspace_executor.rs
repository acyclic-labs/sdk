//! Task-bound delegation for the stock workspace commands. Every host's
//! `shell` and `fs` pass here, so their bounds ([`super::bounds`]) hold for
//! each backend without a per-backend limit.
use crate::{
    Error, Result,
    runtime::{RuntimeScope, Target, ToolContext},
    tool::{ToolExecutor, ToolInvocation, ToolResult},
};
use acyclic_stream::BoxProviderFuture;
use std::sync::Arc;

pub(super) fn executor(name: &'static str) -> Arc<dyn ToolExecutor> {
    Arc::new(WorkspaceExecutor { name })
}

/// The target this call runs on ([`crate::runtime::call_target`]).
fn target(
    context: &ToolContext,
    invocation: &ToolInvocation,
    work: crate::NativeWork<'_>,
) -> Result<Arc<Target>> {
    let unbound = || {
        boundary(
            work,
            crate::wire::ErrorCode::Unsupported,
            "workspace tools are not bound",
        )
    };
    let targets = context.task().harness.targets().ok_or_else(unbound)?;
    crate::runtime::call_target(&**targets, invocation).ok_or_else(unbound)?
}

/// Every shell result names the target and command language it ran under.
/// Also returns the retained bytes the result grew by, which a native
/// caller charges to the result's allocation.
fn echo(name: &str, target: &Target, mut result: ToolResult) -> Result<(ToolResult, u64)> {
    let mut grown = 0u64;
    if name == super::shell::PLATFORM.name
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
    target: &Target,
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
        match target(context, invocation, work) {
            Ok(target) => target
                .executor
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
        if super::bounds::operands(
            self.name,
            &invocation.arguments,
            super::bounds::limits(context),
        )
        .is_err()
        {
            // Ordinary execution returns this same rejection to the model.
            // Nothing may be handed to a remote target before that verdict.
            return Ok(None);
        }
        target(context, invocation, None)?
            .executor
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
            let target = target(&context, &invocation, None)?;
            let limits = super::bounds::limits(&context);
            if let Err(error) = super::bounds::operands(self.name, &invocation.arguments, limits) {
                return Ok(super::rejected_input(error));
            }
            let result = target.executor.execute(invocation, Some(context)).await?;
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
            self.authorize(None, Some(&context), &invocation, None)?;
            let target = target(&context, &invocation, None)?;
            let maximum = super::bounds::limits(&context).0;
            target
                .executor
                .reconcile(invocation, Some(context))
                .await?
                .map(|result| {
                    super::bounds::result(self.name, echo(self.name, &target, result)?.0, maximum)
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
        let target = target(context, invocation, Some(work))?;
        let name = self.name;
        crate::NativeFutureBundle::new(work, move || async move {
            let (result, mut allocation) = target
                .executor
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
        self.authorize(None, Some(context), invocation, Some(work))?;
        let target = target(context, invocation, Some(work))?;
        let name = self.name;
        crate::NativeFutureBundle::new(work, move || async move {
            let (result, mut allocation) = target
                .executor
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
