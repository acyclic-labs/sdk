use super::{
    arguments::{Adapted, Capability},
    *,
};
use crate::{
    runtime::{RuntimeScope, ToolContext},
    tool::{ToolExecutor, ToolInvocation, ToolResult},
};
use acyclic_stream::BoxProviderFuture;

pub(super) struct Adapter {
    pub profile: NativeToolProfile,
    pub capability: Capability,
    pub original: Arc<Tool>,
}

impl ToolExecutor for Adapter {
    fn workspace_binding(&self) -> Option<crate::tool::WorkspaceBinding> {
        self.original.executor.workspace_binding()
    }
    #[cfg(all(feature = "hooks", feature = "filesystem", not(target_arch = "wasm32")))]
    fn run_hooks<'a>(
        &'a self,
        configuration: (&'a crate::hooks::Hooks, [u8; 32]),
        event: crate::hooks::HookEvent,
        input: crate::hooks::HookInput<'a>,
        registry: &'a crate::tool::ToolRegistry,
        context: ToolContext,
        reconcile_only: bool,
    ) -> BoxProviderFuture<'a, Result<Option<crate::hooks::HookOutput>>> {
        self.original.executor.run_hooks(
            configuration,
            event,
            input,
            registry,
            context,
            reconcile_only,
        )
    }
    fn read_path<'a>(
        &'a self,
        path: &'a str,
        work: crate::NativeWork<'_>,
    ) -> Result<(
        &'a crate::conversation::VolumeRef,
        std::borrow::Cow<'a, str>,
    )> {
        self.original.executor.read_path(path, work)
    }

    fn authorize(
        &self,
        scope: Option<&RuntimeScope>,
        context: Option<&ToolContext>,
        invocation: &ToolInvocation,
        work: crate::NativeWork<'_>,
    ) -> Result<()> {
        let adapted = self.adapt(invocation, work)?;
        self.original
            .executor
            .authorize(scope, context, &adapted.invocation, work)
    }

    fn cancellable_on_drop(&self) -> bool {
        self.original.executor.cancellable_on_drop()
    }

    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            let Adapted { invocation, .. } = self.adapt(&invocation, None)?;
            self.original.executor.execute(invocation, context).await
        })
    }

    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move {
            let Adapted { invocation, .. } = self.adapt(&invocation, None)?;
            self.original.executor.reconcile(invocation, context).await
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_execution(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        self.original.executor.supports_native_execution(work)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn execute_with_native_context<'a>(
        &'a self,
        context: &'a ToolContext,
        invocation: &'a ToolInvocation,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (ToolResult, acyclic_fs::NativeAllocation)>> {
        self.supports_native_execution(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            let adapted = self.adapt(invocation, Some(work))?;
            self.original
                .executor
                .execute_with_native_context(context, &adapted.invocation, work)?
                .await
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
        self.supports_native_execution(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            let adapted = self.adapt(invocation, Some(work))?;
            self.original
                .executor
                .reconcile_with_native_context(context, &adapted.invocation, work)?
                .await
        })
    }
}

impl crate::tool::ToolProjection for Adapter {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_projection(&self, work: crate::NativeWork<'_>) -> Result<()> {
        self.original.projection.supports_native_projection(work)
    }

    fn project(
        &self,
        invocation: &ToolInvocation,
        result: &ToolResult,
        work: crate::NativeWork<'_>,
    ) -> Result<(Value, crate::NativeCustody)> {
        let adapted = self.adapt(invocation, work)?;
        self.original
            .projection
            .project(&adapted.invocation, result, work)
    }
}
