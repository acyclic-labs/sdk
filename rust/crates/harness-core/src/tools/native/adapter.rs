//! One generic executor for every native declaration: it rewrites the call
//! with the declaration's template and hands it to the canonical capability.
//! Grants, policy and approval see the canonical call; the model keeps its
//! own call and result under the native name.
use super::arguments::Arguments;
use crate::{
    Result,
    runtime::{RuntimeScope, ToolContext},
    tool::{CanonicalInvocation, Tool, ToolExecutor, ToolInvocation, ToolProjection, ToolResult},
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use serde_json::Value;
use std::sync::Arc;

pub(super) struct Adapter {
    pub(super) arguments: Arc<Arguments>,
    /// The capability whose grant admits the call.
    pub(super) capability: Arc<Tool>,
    /// The canonical tool that runs it: the capability's own, or a variant.
    pub(super) original: Arc<Tool>,
}

impl Adapter {
    fn adapt(
        &self,
        invocation: &ToolInvocation,
        work: crate::NativeWork<'_>,
    ) -> Result<CanonicalInvocation> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let io = work
            .map(crate::executor::PreparationIo::from_native_work)
            .transpose()?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let observer = io.as_ref();
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let observer = None;
        let (arguments, arguments_custody) = self.arguments.adapt(invocation, work)?;
        let mut adapted = CanonicalInvocation::new(
            ToolInvocation {
                operation_id: invocation.operation_id,
                call_id: String::new(),
                name: String::new(),
                arguments,
            },
            arguments_custody,
            None,
        );
        adapted.invocation.call_id =
            crate::executor::stock_string_copy(&invocation.call_id, observer)?;
        adapted.invocation.name =
            crate::executor::stock_string_copy(&self.original.definition.name, observer)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let metadata_custody = io
            .map(|io| {
                io.into_native_custody(crate::core::owned_sum([
                    adapted.invocation.call_id.capacity() as u64,
                    adapted.invocation.name.capacity() as u64,
                ])?)
            })
            .transpose()?;
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let metadata_custody = None;
        adapted.custody[1] = metadata_custody;
        Ok(adapted)
    }
}

impl ToolExecutor for Adapter {
    fn capability(&self) -> Option<&Tool> {
        Some(&self.capability)
    }

    fn canonical(
        &self,
        invocation: &ToolInvocation,
        work: crate::NativeWork<'_>,
    ) -> Result<Option<CanonicalInvocation>> {
        self.adapt(invocation, work).map(Some)
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
        let adapted = self.adapt(invocation, work)?;
        self.original
            .executor
            .authorize(scope, context, &adapted.invocation, work)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn authorize_execution_grant<'a>(
        &'a self,
        invocation: &'a ToolInvocation,
        context: &'a ToolContext,
        grant: &'a acyclic_native_runtime::execution::ExecutionGrant,
        work: crate::NativeWork<'a>,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let adapted = self.adapt(invocation, work)?;
            self.original
                .executor
                .authorize_execution_grant(&adapted.invocation, context, grant, work)
                .await
        })
    }

    fn authorize_replay<'a>(
        &'a self,
        context: Option<&'a ToolContext>,
        invocation: &'a ToolInvocation,
        work: crate::NativeWork<'a>,
    ) -> Option<BoxFuture<'a, Result<()>>> {
        Some(Box::pin(async move {
            let adapted = self.adapt(invocation, work)?;
            match self
                .original
                .executor
                .authorize_replay(context, &adapted.invocation, work)
            {
                Some(authorization) => authorization.await,
                None => Ok(()),
            }
        }))
    }

    fn dispatch(
        &self,
        invocation: &ToolInvocation,
        context: Option<&ToolContext>,
        work: crate::NativeWork<'_>,
    ) -> Result<Option<crate::tool::Dispatch>> {
        self.original
            .executor
            .dispatch(&self.adapt(invocation, work)?.invocation, context, work)
    }

    fn cancellable_on_drop(&self) -> bool {
        self.original.executor.cancellable_on_drop()
    }

    fn cancellation(
        &self,
        invocation: &ToolInvocation,
        context: Option<&ToolContext>,
        work: crate::NativeWork<'_>,
    ) -> Result<Option<BoxFuture<'static, Result<()>>>> {
        self.original.executor.cancellation(
            &self.adapt(invocation, work)?.invocation,
            context,
            work,
        )
    }

    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            let adapted = self.adapt(&invocation, None)?;
            self.original
                .executor
                .execute(adapted.invocation, context)
                .await
        })
    }

    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move {
            let adapted = self.adapt(&invocation, None)?;
            self.original
                .executor
                .reconcile(adapted.invocation, context)
                .await
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
        crate::NativeFutureBundle::new(work, || async move {
            let adapted = self.adapt(invocation, Some(work))?;
            self.original
                .executor
                .reconcile_with_native_context(context, &adapted.invocation, work)?
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
        let adapted = self.adapt(invocation, work)?;
        self.original
            .projection
            .project(&adapted.invocation, result, work)
    }

    fn supports_native_projection(&self, work: crate::NativeWork<'_>) -> Result<()> {
        self.original.projection.supports_native_projection(work)
    }
}
