use super::native_io::{accepted_builtin_io, decoded_tool_request};
use super::*;

/// This executor owns no session, runtime, provider, credentials, or view.
struct PiBuiltinToolExecutor;

pub(crate) fn builtin_tool_executor_with_native_work(
    work: &NativeMaterializationWork,
) -> Result<std::sync::Arc<dyn acyclic_harness::tool::ToolExecutor>> {
    work.charge(WorkCounters {
        allocation_operations: 1,
        ..WorkCounters::default()
    })?;
    Ok(std::sync::Arc::new(PiBuiltinToolExecutor))
}

impl acyclic_harness::tool::ToolExecutor for PiBuiltinToolExecutor {
    fn execute<'a>(
        &'a self,
        _invocation: acyclic_harness::tool::ToolInvocation,
        _context: Option<acyclic_harness::runtime::ToolContext>,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<acyclic_harness::tool::ToolResult>> {
        Box::pin(async {
            Err(denied(
                "builtin IO requires the original accepted native tool callback",
            ))
        })
    }

    fn reconcile<'a>(
        &'a self,
        _invocation: acyclic_harness::tool::ToolInvocation,
        _context: Option<acyclic_harness::runtime::ToolContext>,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<Option<acyclic_harness::tool::ToolResult>>>
    {
        Box::pin(async {
            Err(denied(
                "builtin reconciliation requires the original accepted native tool callback",
            ))
        })
    }

    fn supports_native_execution(&self, work: &NativeMaterializationWork) -> Result<()> {
        work.remaining()?;
        Ok(())
    }

    fn authorize(
        &self,
        _scope: Option<&acyclic_harness::runtime::RuntimeScope>,
        context: Option<&acyclic_harness::runtime::ToolContext>,
        invocation: &acyclic_harness::tool::ToolInvocation,
        work: acyclic_harness::NativeWork<'_>,
    ) -> Result<()> {
        if let Some(work) = work {
            let context = context.ok_or_else(|| denied(
                "builtin IO requires the original accepted native tool callback",
            ))?;
            decoded_tool_request(context, invocation, work)?;
        }
        Ok(())
    }

    fn execute_with_native_context<'a>(
        &'a self,
        context: &'a acyclic_harness::runtime::ToolContext,
        invocation: &'a acyclic_harness::tool::ToolInvocation,
        work: &'a NativeMaterializationWork,
    ) -> Result<
        acyclic_harness::NativeFutureBundle<
            'a,
            (acyclic_harness::tool::ToolResult, NativeAllocation),
        >,
    > {
        acyclic_harness::NativeFutureBundle::new(work, || async move {
            accepted_builtin_io(context, invocation, work).await
        })
    }

    fn reconcile_with_native_context<'a>(
        &'a self,
        context: &'a acyclic_harness::runtime::ToolContext,
        invocation: &'a acyclic_harness::tool::ToolInvocation,
        work: &'a NativeMaterializationWork,
    ) -> Result<
        acyclic_harness::NativeFutureBundle<
            'a,
            (Option<acyclic_harness::tool::ToolResult>, NativeAllocation),
        >,
    > {
        acyclic_harness::NativeFutureBundle::new(work, || async move {
            let decoded = decoded_tool_request(context, invocation, work)?;
            context.validate_native_mcp_reconciliation_context(work)?;
            drop(decoded.0);
            drop(decoded.1);
            let custody = work.claim(
                std::mem::size_of::<Option<acyclic_harness::tool::ToolResult>>() as u64,
                0,
                acyclic_fs::WorkCounters::UNCHARGED,
            )?;
            Ok((None, custody))
        })
    }
}
