use super::*;

/// Replaceable execution behavior for a tool.
pub trait ToolExecutor: acyclic_stream::ProviderPlatform {
    /// Runs configured hooks inside this executor's selected, owner-fenced workspace.
    #[cfg(all(feature = "hooks", feature = "filesystem", not(target_arch = "wasm32")))]
    fn run_hooks<'a>(
        &'a self,
        _configuration: (&'a crate::hooks::Hooks, [u8; 32]),
        _event: crate::hooks::HookEvent,
        _input: Value,
        _registry: &'a ToolRegistry,
        _context: crate::runtime::ToolContext,
        reconcile_only: bool,
    ) -> BoxFuture<'a, Result<crate::hooks::HookOutput>> {
        Box::pin(async move {
            if reconcile_only {
                return Ok(crate::hooks::HookOutput::default());
            }
            Err(Error::Unsupported(
                "workspace hook execution is not bound".into(),
            ))
        })
    }
    /// Checks grants and the original context before replay, dispatch or reconciliation.
    fn authorize(
        &self,
        _scope: Option<&crate::runtime::RuntimeScope>,
        _context: Option<&crate::runtime::ToolContext>,
        _invocation: &ToolInvocation,
        work: crate::NativeWork<'_>,
    ) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if work.is_some() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "tool executor has no native authorization producer",
            });
        }
        let _ = work;
        Ok(())
    }

    /// The file version a successful durable result of this tool showed the
    /// model, if it names one. Later calls derive what the model has seen
    /// from these results alone.
    fn observed_file(&self, _result: &ToolResult) -> Option<crate::conversation::FileRef> {
        None
    }

    /// Whether this tool checks its input against the versions the model
    /// observed earlier in its turn.
    fn uses_observed_files(&self) -> bool {
        false
    }

    /// Executes an already admitted invocation.
    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<crate::runtime::ToolContext>,
    ) -> BoxFuture<'a, Result<ToolResult>>;

    /// Reconciles a previously started invocation without executing it again.
    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<crate::runtime::ToolContext>,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>>;

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
