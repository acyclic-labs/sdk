use super::*;

/// Replaceable execution behavior for a tool.
pub trait ToolExecutor: acyclic_stream::ProviderPlatform {
    /// Borrows the pinned resumable identity on the supplied original ledger.
    fn resumable_machine_identity(
        &self,
        work: crate::NativeWork<'_>,
    ) -> Result<Option<&crate::workflow::MachineIdentity>> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if work.is_some() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "tool executor has no native resumable identity producer",
            });
        }
        let _ = work;
        Ok(None)
    }

    /// Checks invocation-specific resource grants before a result is replayed, dispatched, or reconciled.
    fn authorize(
        &self,
        _scope: Option<&crate::runtime::RuntimeScope>,
        _invocation: &ToolInvocation,
    ) -> Result<()> {
        Ok(())
    }

    /// Checks original task-bound resources before replay, dispatch or reconciliation.
    fn authorize_with_context(
        &self,
        context: &crate::runtime::ToolContext,
        invocation: &ToolInvocation,
    ) -> Result<()> {
        self.authorize(Some(context.task().scope()), invocation)
    }

    /// Executes an already admitted invocation.
    fn execute<'a>(&'a self, invocation: ToolInvocation) -> BoxFuture<'a, Result<ToolResult>>;

    /// Executes with the scoped task/tool context when admitted by the typed runtime.
    fn execute_with_context<'a>(
        &'a self,
        _context: crate::runtime::ToolContext,
        invocation: ToolInvocation,
    ) -> BoxFuture<'a, Result<ToolResult>> {
        self.execute(invocation)
    }

    /// Reconciles a previously started invocation without executing it again.
    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>>;

    /// Reconciles under the same owner-authenticated task context used for dispatch.
    fn reconcile_with_context<'a>(
        &'a self,
        _context: crate::runtime::ToolContext,
        invocation: ToolInvocation,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        self.reconcile(invocation)
    }

    /// Rejects an unaudited implementation before the caller constructs native invocation metadata or a future.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_execution(
        &self,
        _work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "tool executor has no native producer",
        })
    }

    /// Checks the same original context before native replay or dispatch.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn authorize_with_native_context(
        &self,
        _context: &crate::runtime::ToolContext,
        _invocation: &ToolInvocation,
        _work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "tool executor has no native authorization producer",
        })
    }

    /// Executes the exact borrowed invocation on its original native ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn execute_with_native_context<'a>(
        &'a self,
        _context: &'a crate::runtime::ToolContext,
        _invocation: &'a ToolInvocation,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (ToolResult, acyclic_fs::NativeAllocation)>> {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "tool executor has no native execution producer",
        })
    }

    /// Observes the already-dispatched effect; never calls ordinary execution.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn reconcile_with_native_context<'a>(
        &'a self,
        _context: &'a crate::runtime::ToolContext,
        _invocation: &'a ToolInvocation,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<ToolResult>, acyclic_fs::NativeAllocation)>>
    {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "tool executor has no native reconciliation producer",
        })
    }
}
