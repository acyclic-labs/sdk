//! Standard tool facade over the host-owned search request files.
use super::*;
use crate::{
    runtime::{RuntimeScope, ToolContext},
    tool::{
        JsonToolProjection, Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolResult, schema,
    },
};
use acyclic_fs::{NativeAllocation, NativeMaterializationWork};
use acyclic_stream::StreamProvider;
use std::{marker::PhantomData, sync::Arc};

type Resolve = dyn Fn() -> Result<Option<Arc<dyn SearchProvider>>> + Send + Sync;

/// Binds search to the same stream-provider type as its original task owner.
/// Configuration and credentials are resolved privately for each dispatch.
pub fn bind<P: StreamProvider + 'static>(
    resolve: impl Fn() -> Result<Option<Arc<dyn SearchProvider>>> + Send + Sync + 'static,
) -> Result<Tool> {
    let output = schema::output::<files::SearchOutcome>()?;
    let definition = ToolDefinition {
        name: "web_search".into(),
        revision: "1".into(),
        description: "Search the web and return ranked sources for citations".into(),
        input_schema: schema::input::<SearchRequest>()?,
        output_schema: output,
        projection_schema: schema::ProjectionMode::Full.schema::<files::SearchOutcome>()?,
    };
    definition.validate(&mut None)?;
    Ok(Tool {
        hook_names: &[],
        definition,
        executor: Arc::new(SearchTool::<P> {
            resolve: Arc::new(resolve),
            provider: PhantomData,
        }),
        projection: Arc::new(JsonToolProjection),
    })
}

#[cfg(test)]
#[path = "tool/tests.rs"]
mod tests;

struct SearchTool<P> {
    resolve: Arc<Resolve>,
    provider: PhantomData<P>,
}

impl<P: StreamProvider + 'static> SearchTool<P> {
    async fn run(
        &self,
        context: &ToolContext,
        invocation: &ToolInvocation,
        work: crate::NativeWork<'_>,
        execute: bool,
    ) -> Result<Option<(ToolResult, crate::NativeCustody)>> {
        let io = work
            .map(crate::filesystem::PreparationIo::from_native_work)
            .transpose()?;
        let owner = if let Some(work) = work {
            let (_, fence) = context.owned_task_binding(io.as_ref())?;
            context.journal_owner::<P>(fence, work)?
        } else {
            context.task().ordinary_journal_owner::<P>()?
        };
        let volume = &context
            .task()
            .task_files()
            .ok_or_else(|| super::rejection(work, "search task files are not bound"))?
            .inner;
        let input = if execute {
            Some(crate::tool::decode::<SearchRequest>(
                &invocation.arguments,
                work,
            )?)
        } else {
            None
        };
        let bytes = files::dispatch(
            volume,
            &owner,
            invocation.operation_id,
            input.as_ref().map(|input| &input.0),
            || (self.resolve)(),
            work,
        )
        .await?;
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        let (value, custody) = if let Some(io) = io {
            let value: serde_json::Value = io.decode_json(&bytes)?;
            let size = (std::mem::size_of::<serde_json::Value>() as u64)
                .checked_add(crate::executor::json::json_value_logical_owned_bytes(
                    &value,
                )?)
                .ok_or_else(|| super::rejection(work, "search tool result ownership overflow"))?;
            (value, Some(io.into_native_custody(size)?))
        } else {
            (
                serde_json::from_slice(&bytes)
                    .map_err(|_| super::error(work, "invalid retained search result"))?,
                None,
            )
        };
        Ok(Some((ToolResult { value }, custody)))
    }
}

impl<P: StreamProvider + 'static> ToolExecutor for SearchTool<P> {
    fn authorize(
        &self,
        _scope: Option<&RuntimeScope>,
        context: Option<&ToolContext>,
        invocation: &ToolInvocation,
        work: crate::NativeWork<'_>,
    ) -> Result<()> {
        let context = context
            .ok_or_else(|| super::rejection(work, "search requires its original tool context"))?;
        if invocation.name != "web_search"
            || invocation.operation_id != context.operation_id()
            || invocation.call_id != context.call_id()
        {
            return Err(super::rejection(work, "search invocation identity changed"));
        }
        if let Some(work) = work {
            context.validate_native_mcp_reconciliation_context(work)?;
        }
        Ok(())
    }
    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            self.authorize(None, context.as_ref(), &invocation, None)?;
            let context = context
                .as_ref()
                .ok_or_else(|| super::rejection(None, "search context is absent"))?;
            self.run(context, &invocation, None, true)
                .await?
                .map(|result| result.0)
                .ok_or_else(|| super::error(None, "search result was not published"))
        })
    }
    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move {
            self.authorize(None, context.as_ref(), &invocation, None)?;
            let context = context
                .as_ref()
                .ok_or_else(|| super::rejection(None, "search context is absent"))?;
            Ok(self
                .run(context, &invocation, None, false)
                .await?
                .map(|result| result.0))
        })
    }
    fn resumable_machine_identity(
        &self,
        work: crate::NativeWork<'_>,
    ) -> Result<Option<&crate::workflow::MachineIdentity>> {
        if let Some(work) = work {
            work.remaining()?;
        }
        Ok(None)
    }
    fn supports_native_execution(&self, work: &NativeMaterializationWork) -> Result<()> {
        work.remaining()?;
        Ok(())
    }
    fn execute_with_native_context<'a>(
        &'a self,
        context: &'a ToolContext,
        invocation: &'a ToolInvocation,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (ToolResult, NativeAllocation)>> {
        self.authorize(None, Some(context), invocation, Some(work))?;
        context.validate_native_mcp_context(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            let (result, custody) = self
                .run(context, invocation, Some(work), true)
                .await?
                .ok_or_else(|| super::error(Some(work), "search result was not published"))?;
            Ok((
                result,
                custody.ok_or_else(|| {
                    super::error(Some(work), "native search result lacks custody")
                })?,
            ))
        })
    }
    fn reconcile_with_native_context<'a>(
        &'a self,
        context: &'a ToolContext,
        invocation: &'a ToolInvocation,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<ToolResult>, NativeAllocation)>> {
        self.authorize(None, Some(context), invocation, Some(work))?;
        crate::NativeFutureBundle::new(work, || async move {
            match self.run(context, invocation, Some(work), false).await? {
                Some((value, held)) => Ok((
                    Some(value),
                    held.ok_or_else(|| {
                        super::error(Some(work), "native search result lacks custody")
                    })?,
                )),
                None => Ok((None, work.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)?)),
            }
        })
    }
}
