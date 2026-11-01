use super::*;

/// Records appended atomically with a tool call's claim
/// ([`ToolExecutor::dispatch`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Dispatch {
    /// The original command, fitting one protective Stream record.
    pub delivery: (acyclic_stream::StreamPath, acyclic_stream::OwnedBytes),
    /// The owning Actor's activation, committed with its input and tool claim.
    pub activation: (acyclic_stream::StreamPath, acyclic_stream::OwnedBytes),
}

/// Workspace selection retained by an executor's immutable registry binding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkspaceBinding {
    /// Always use the task's home target.
    Home,
    /// Use the call's optional `target`, otherwise the task's home target.
    Named,
}

/// Replaceable execution behavior for a tool.
pub trait ToolExecutor: acyclic_stream::ProviderPlatform {
    /// Selects the workspace before authorization and original start publication.
    /// Adapters retain their underlying executor's binding; model-visible names
    /// do not choose execution placement or grant another capability.
    fn workspace_binding(&self) -> Option<WorkspaceBinding> {
        None
    }

    /// Projects pinned skill source and its JSON input into this executor's
    /// admitted target shell. This executes nothing: the returned ordinary
    /// shell invocation still needs its original approval, journal and custody.
    fn prepare_shell_script(
        &self,
        _source: &str,
        _input: &serde_json::Value,
        _context: Option<&crate::runtime::ToolContext>,
        work: crate::NativeWork<'_>,
    ) -> Result<crate::tools::ShellInput> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if work.is_some() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "target shell projection is not bound; recovery: bind the original admitted workspace executor",
            });
        }
        let _ = work;
        Err(Error::Unsupported(
            "target shell projection is not bound; recovery: bind the original admitted workspace executor".into(),
        ))
    }

    /// Resolves an already validated content path in this exact workspace.
    /// Selection and read policy do not require a publication binding. The
    /// task's original reader still authenticates the selected volume/grants.
    fn read_path<'a>(
        &'a self,
        _path: &'a str,
        work: crate::NativeWork<'_>,
    ) -> Result<(
        &'a crate::conversation::VolumeRef,
        std::borrow::Cow<'a, str>,
    )> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if work.is_some() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "workspace read-path resolution is unavailable; recovery: bind the original workspace path resolver",
            });
        }
        let _ = work;
        Err(Error::Unsupported("workspace read-path resolution is unavailable; recovery: bind the original workspace path resolver".into()))
    }
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

    /// Rechecks mutable source authority before exposing a completed result.
    /// The invocation has already passed `authorize` and its recorded identity
    /// check. This must only authorize reads: it cannot execute or reconcile the
    /// command. Executors whose synchronous authorization is sufficient return
    /// `None`, without constructing a future.
    fn authorize_replay<'a>(
        &'a self,
        _context: Option<&'a crate::runtime::ToolContext>,
        _invocation: &'a ToolInvocation,
        _work: crate::NativeWork<'a>,
    ) -> Result<Option<crate::NativeFutureBundle<'a, ()>>> {
        Ok(None)
    }

    /// The record that hands this invocation to another host, appended in the
    /// same commit as the journal entry that claims it; `None` when the
    /// invocation runs here. A dispatch that is never claimed was never sent,
    /// and a claimed one was sent exactly once. Adapters use the original work
    /// for argument preparation; an unfunded native handoff must refuse before
    /// constructing its records.
    fn dispatch(
        &self,
        _invocation: &ToolInvocation,
        _context: Option<&crate::runtime::ToolContext>,
        _work: crate::NativeWork<'_>,
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
    /// The caller must authenticate the complete invocation against its original
    /// start record; an operation ID alone does not admit replacement arguments.
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
    /// The caller authenticates the complete invocation against its start record,
    /// as for portable reconciliation.
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
