use super::*;

/// Records appended atomically with a tool call's claim
/// ([`ToolExecutor::dispatch`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Dispatch {
    /// Original delivery and primitive wake records. Each value fits one
    /// protective Stream record; they are never published separately.
    pub records: Vec<(acyclic_stream::StreamPath, acyclic_stream::OwnedBytes)>,
}

/// Replaceable execution behavior for a tool.
pub trait ToolExecutor: acyclic_stream::ProviderPlatform {
    /// Runs configured hooks inside this executor's selected, owner-fenced workspace.
    #[cfg(all(feature = "hooks", feature = "filesystem", not(target_arch = "wasm32")))]
    fn run_hooks<'a>(
        &'a self,
        _configuration: (&'a crate::hooks::Hooks, [u8; 32]),
        _event: crate::hooks::HookEvent,
        _input: crate::hooks::HookInput<'a>,
        _registry: &'a ToolRegistry,
        _context: crate::runtime::ToolContext,
        reconcile_only: bool,
    ) -> BoxFuture<'a, Result<Option<crate::hooks::HookOutput>>> {
        Box::pin(async move {
            if reconcile_only {
                return Ok(None);
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

    /// The record that hands this invocation to another host, appended in the
    /// same commit as the journal entry that claims it; `None` when the
    /// invocation runs here. A dispatch that is never claimed was never sent,
    /// and a claimed one was sent exactly once.
    fn dispatch(
        &self,
        _invocation: &ToolInvocation,
        _context: Option<&crate::runtime::ToolContext>,
    ) -> Result<Option<Dispatch>> {
        Ok(None)
    }

    /// Whether dropping an invocation future retires all its I/O before return.
    /// Opt in only when cancellation leaves no independently running effect.
    /// The stock journal records interruption before settling the turn, and
    /// never retries an interrupted invocation. Other executors retain their
    /// existing reconciliation and effect-fencing behavior.
    fn cancellable_on_drop(&self) -> bool {
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
