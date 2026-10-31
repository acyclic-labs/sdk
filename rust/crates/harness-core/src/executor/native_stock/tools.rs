use super::*;

pub(in crate::executor) fn stock_tool_definitions(
    registry: &ToolRegistry,
    scope: &RuntimeScope,
    io: Option<&PreparationIo>,
) -> Result<Vec<crate::tool::ToolDefinition>> {
    registry.definitions_for_grants(scope.grants(), io)
}

impl StockExecution<'_> {
    pub(in crate::executor) fn tool_definitions(
        &self,
        io: Option<&PreparationIo>,
    ) -> Result<Vec<crate::tool::ToolDefinition>> {
        #[cfg(feature = "filesystem")]
        if let Some((profiles, pin, key)) = &self.stock.profile {
            let profile = profiles.for_model(pin, key)?;
            let bound = profile.bind_tools(&self.stock.tools, io)?;
            let mut definitions = crate::model::vec_with_capacity(bound.declarations().len(), io)?;
            for binding in bound.declarations() {
                definitions.push(crate::model::clone_tool_definition(
                    &binding.tool().definition,
                    io,
                )?);
            }
            return Ok(definitions);
        }
        stock_tool_definitions(&self.stock.tools, &self.stock.tool_scope, io)
    }
}

pub(in crate::executor) fn stock_is_native(io: Option<&PreparationIo>) -> bool {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    {
        io.and_then(PreparationIo::original_native_work).is_some()
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    {
        let _ = io;
        false
    }
}

pub(in crate::executor) fn stock_authorize_tool(
    tool: &crate::tool::Tool,
    context: Option<&crate::runtime::ToolContext>,
    scope: &RuntimeScope,
    invocation: &ToolInvocation,
    io: Option<&PreparationIo>,
) -> Result<()> {
    let _ = io;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = io.and_then(PreparationIo::original_native_work) {
        let context = context.ok_or_else(|| crate::Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "native tool authorization has no issued context",
        })?;
        return tool
            .executor
            .authorize(None, Some(context), invocation, Some(work));
    }
    if let Some(context) = context {
        tool.executor
            .authorize(None, Some(context), invocation, None)
    } else {
        tool.executor.authorize(Some(scope), None, invocation, None)
    }
}

pub(in crate::executor) async fn stock_tool_call<'a, T>(
    context: &'a mut Option<crate::runtime::ToolContext>,
    io: Option<&'a PreparationIo>,
    ordinary: impl FnOnce(Option<crate::runtime::ToolContext>) -> BoxFuture<'a, Result<T>>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))] native: impl FnOnce(
        &'a crate::runtime::ToolContext,
        &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<'a, (T, acyclic_fs::NativeAllocation)>,
    >,
) -> Result<T> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io
        && let Some(work) = io.original_native_work()
    {
        let context = context.as_ref().ok_or_else(|| {
            stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Unauthorized,
                "native tool call has no issued context",
            )
        })?;
        let (result, custody) = native(context, work)?.await?;
        io.absorb_native_custody(custody)?;
        return Ok(result);
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let _ = io;
    ordinary(context.take()).await
}

pub(in crate::executor) fn stock_project_tool(
    tool: &crate::tool::Tool,
    invocation: &ToolInvocation,
    result: &ToolResult,
    limits: Limits,
    io: Option<&PreparationIo>,
) -> Result<Result<(Value, crate::model::ToolResultContent)>> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let work = io.and_then(PreparationIo::original_native_work);
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let work = None;
    let (value, custody) = tool.projection.project(invocation, result, work)?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io.filter(|_| work.is_some()) {
        io.absorb_native_custody(custody.ok_or_else(|| {
            stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Unsupported,
                "tool projection has no native producer",
            )
        })?)?;
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let _ = custody;
    let content = match tool.definition.check_projection(&value, limits, io)? {
        Ok(content) => content,
        Err(violation) => return Ok(Err(violation)),
    };
    stock_json_bound(&value, limits.render_bytes, io)?;
    Ok(Ok((value, content)))
}
