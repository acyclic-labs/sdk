use super::{SearchProvider, SearchRequest, SearchResponse};
use crate::{
    Error, Result,
    runtime::ToolContext,
    tool::{
        BuiltinTool, JsonToolProjection, Tool, ToolDefinition, ToolEffect, ToolExecutor,
        ToolInvocation, ToolResult, ToolSpec, schema,
    },
};
use acyclic_stream::BoxProviderFuture;
use std::sync::Arc;

/// The web-search catalog entry; all schemas come from the actual DTOs.
pub fn spec() -> Result<ToolSpec> {
    Ok(ToolSpec {
        definition: ToolDefinition {
            name: "web_search".into(),
            revision: "1".into(),
            description: "Search the web for current information. Returns ranked source URLs, titles and snippets for citations.".into(),
            input_schema: schema::input::<SearchRequest>()?,
            output_schema: schema::output::<SearchResponse>()?,
            projection_schema: schema::ProjectionMode::Full.schema::<SearchResponse>()?,
        },
        executor: BuiltinTool::WebSearch,
        effects: vec![ToolEffect::Network],
    })
}

/// Binds an explicitly supplied backend, including a credential-free scripted backend.
pub fn web_search(provider: Arc<dyn SearchProvider>) -> Result<Tool> {
    spec()?.bind(
        Arc::new(SearchExecutor(provider)),
        Arc::new(JsonToolProjection),
    )
}

struct SearchExecutor(Arc<dyn SearchProvider>);

#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
mod native_tests;

impl ToolExecutor for SearchExecutor {
    fn authorize(
        &self,
        scope: Option<&crate::runtime::RuntimeScope>,
        context: Option<&ToolContext>,
        invocation: &ToolInvocation,
        work: crate::NativeWork<'_>,
    ) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            work.charge(acyclic_fs::WorkCounters {
                items_examined: (invocation.name.len() + invocation.call_id.len()) as u64,
                ..Default::default()
            })?;
            context
                .ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "native search requires its issued tool context",
                })?
                .validate_native_bound_context(work)?;
        }
        if invocation.name != "web_search"
            || context.is_some_and(|c| {
                c.operation_id() != invocation.operation_id || c.call_id() != invocation.call_id
            })
            || context
                .map(ToolContext::effective_scope)
                .or(scope)
                .is_some_and(|s| !s.grants().contains("tool:call:web_search"))
        {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.is_some() {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "search requires its exact admitted call and grant",
                });
            }
            return Err(Error::Unauthorized(
                "search requires its exact admitted call and grant".into(),
            ));
        }
        let _ = work;
        Ok(())
    }

    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            self.authorize(None, context.as_ref(), &invocation, None)?;
            let request: SearchRequest = serde_json::from_value(invocation.arguments)
                .map_err(|_| Error::Invalid("invalid web search arguments".into()))?;
            request.validate()?;
            let response = self.0.search(&request).await?;
            if response.results.len() > request.max_results as usize {
                return Err(Error::Invalid(
                    "search backend exceeded result limit".into(),
                ));
            }
            Ok(ToolResult {
                value: serde_json::to_value(response)
                    .map_err(|_| Error::Invalid("invalid web search response".into()))?,
            })
        })
    }

    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        // A lost response cannot be recovered from either search API. Reissuing
        // would consume another request and could return a different result.
        Box::pin(async { Ok(None) })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_execution(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        if self.0.supports_native_search(work)? {
            Ok(())
        } else {
            Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "search provider has no native producer",
            })
        }
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn execute_with_native_context<'a>(
        &'a self,
        context: &'a ToolContext,
        invocation: &'a ToolInvocation,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (ToolResult, acyclic_fs::NativeAllocation)>> {
        self.authorize(None, Some(context), invocation, Some(work))?;
        context.task().require_original_native_dispatch(work)?;
        self.supports_native_execution(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            context.task().verify_original_execution_owner(work).await?;
            let input = crate::tool::decode::<SearchRequest>(&invocation.arguments, Some(work))?;
            input.0.validate_with_work(Some(work))?;
            let response = match self.0.search_with_native_work(&input.0, work)?.await {
                Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Storage,
                    ..
                }) => return Err(Error::Indeterminate(invocation.operation_id)),
                result => result?,
            };
            if !response.1.work().shares_allocation_context(work)
                || response.1.bytes() < super::native::response_bytes(&response.0)?
            {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Conflict,
                    reason: "search response lacks original-ledger custody",
                });
            }
            if response.0.results.len() > input.0.max_results as usize {
                return Err(super::rejection(
                    Some(work),
                    "search backend exceeded result limit",
                ));
            }
            let encoded = crate::tool::value(
                &response.0,
                context.effective_scope().limits().file_bytes,
                Some(work),
            )?;
            context.task().verify_original_execution_owner(work).await?;
            Ok((
                ToolResult { value: encoded.0 },
                encoded.1.ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Storage,
                    reason: "native search result lacks custody",
                })?,
            ))
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
        self.authorize(None, Some(context), invocation, Some(work))?;
        crate::NativeFutureBundle::new(work, || async move {
            Ok((
                None,
                work.claim(
                    std::mem::size_of::<Option<ToolResult>>() as u64,
                    0,
                    Default::default(),
                )?,
            ))
        })
    }
}
