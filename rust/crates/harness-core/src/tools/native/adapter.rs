//! One generic executor for every native declaration: it rewrites the call
//! with the declaration's template and hands it to the canonical capability.
//! Grants, policy and approval see the canonical call; the model keeps its
//! own call and result under the native name.
use super::arguments::Arguments;
use crate::{
    Result,
    runtime::{RuntimeScope, ToolContext},
    tool::{Tool, ToolExecutor, ToolInvocation, ToolProjection, ToolResult},
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use serde_json::Value;
use std::sync::Arc;

pub(super) struct Adapter {
    pub(super) arguments: Arc<Arguments>,
    pub(super) original: Arc<Tool>,
}

impl Adapter {
    fn adapt(&self, invocation: &ToolInvocation) -> Result<ToolInvocation> {
        Ok(ToolInvocation {
            operation_id: invocation.operation_id,
            call_id: invocation.call_id.clone(),
            name: self.original.definition.name.clone(),
            arguments: self
                .arguments
                .adapt(&invocation.call_id, &invocation.arguments)?,
        })
    }
}

impl ToolExecutor for Adapter {
    fn capability(&self) -> Option<&Tool> {
        Some(&self.original)
    }

    fn canonical(&self, invocation: &ToolInvocation) -> Result<Option<ToolInvocation>> {
        self.adapt(invocation).map(Some)
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

    #[cfg(all(feature = "hooks", feature = "filesystem", not(target_arch = "wasm32")))]
    fn run_hooks<'a>(
        &'a self,
        configuration: (&'a crate::hooks::Hooks, [u8; 32]),
        event: crate::hooks::HookEvent,
        input: crate::hooks::HookInput<'a>,
        registry: &'a crate::tool::ToolRegistry,
        context: ToolContext,
        reconcile_only: bool,
    ) -> BoxFuture<'a, Result<Option<crate::hooks::HookOutput>>> {
        self.original.executor.run_hooks(
            configuration,
            event,
            input,
            registry,
            context,
            reconcile_only,
        )
    }

    fn authorize(
        &self,
        scope: Option<&RuntimeScope>,
        context: Option<&ToolContext>,
        invocation: &ToolInvocation,
        work: crate::NativeWork<'_>,
    ) -> Result<()> {
        self.original
            .executor
            .authorize(scope, context, &self.adapt(invocation)?, work)
    }

    fn authorize_replay<'a>(
        &'a self,
        context: Option<&'a ToolContext>,
        invocation: &'a ToolInvocation,
    ) -> Option<BoxFuture<'a, Result<()>>> {
        Some(Box::pin(async move {
            let adapted = self.adapt(invocation)?;
            match self.original.executor.authorize_replay(context, &adapted) {
                Some(authorization) => authorization.await,
                None => Ok(()),
            }
        }))
    }

    fn dispatch(
        &self,
        invocation: &ToolInvocation,
        context: Option<&ToolContext>,
    ) -> Result<Option<crate::tool::Dispatch>> {
        self.original
            .executor
            .dispatch(&self.adapt(invocation)?, context)
    }

    fn cancellable_on_drop(&self) -> bool {
        self.original.executor.cancellable_on_drop()
    }

    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            let adapted = self.adapt(&invocation)?;
            self.original.executor.execute(adapted, context).await
        })
    }

    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move {
            let adapted = self.adapt(&invocation)?;
            self.original.executor.reconcile(adapted, context).await
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
        crate::NativeFutureBundle::new(work, || async move {
            let adapted = self.adapt(invocation)?;
            self.original
                .executor
                .execute_with_native_context(context, &adapted, work)?
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
        crate::NativeFutureBundle::new(work, || async move {
            let adapted = self.adapt(invocation)?;
            self.original
                .executor
                .reconcile_with_native_context(context, &adapted, work)?
                .await
        })
    }
}

impl ToolProjection for Adapter {
    fn project(
        &self,
        invocation: &ToolInvocation,
        result: &ToolResult,
        work: crate::NativeWork<'_>,
    ) -> Result<(Value, crate::NativeCustody)> {
        self.original
            .projection
            .project(&self.adapt(invocation)?, result, work)
    }

    fn supports_native_projection(&self, work: crate::NativeWork<'_>) -> Result<()> {
        self.original.projection.supports_native_projection(work)
    }
}
