//! Host-bound web tools. The stock execution journal owns result replay.
use crate::{
    Error, NativeCustody, NativeWork, Result, SearchProvider, SearchRequest,
    runtime::{RuntimeScope, ToolContext},
    tool::{ToolExecutor, ToolInvocation, ToolRegistry, ToolResult},
    url_fetch::{FetchProvider, FetchRequest, HttpFetch},
    wire::ErrorCode,
};
use acyclic_fs::NativeMaterializationWork;
use acyclic_stream::BoxProviderFuture;
use serde::Serialize;
use std::sync::Arc;

/// Real host capabilities, independent of checkout selection and auxiliary mounts.
pub struct WebExecutor {
    search: Option<Arc<dyn SearchProvider>>,
    fetch: Arc<dyn FetchProvider>,
}

impl WebExecutor {
    /// Registers only configured capabilities before the host issues tool-call grants.
    pub fn bind(
        registry: &mut ToolRegistry,
        search: Option<Arc<dyn SearchProvider>>,
    ) -> Result<()> {
        let definitions = crate::web::definitions(search.is_some())?;
        let executor: Arc<dyn ToolExecutor> = Arc::new(Self {
            search,
            fetch: Arc::new(HttpFetch::new()?),
        });
        for definition in definitions {
            registry.register(
                (
                    crate::tool::Tool {
                        definition,
                        hook_names: &[],
                        executor: executor.clone(),
                        projection: Arc::new(crate::tool::JsonToolProjection),
                    },
                    None,
                ),
                &mut None,
                None,
            )?;
        }
        Ok(())
    }

    async fn dispatch(
        &self,
        invocation: &ToolInvocation,
        maximum: u64,
        work: NativeWork<'_>,
    ) -> Result<(ToolResult, NativeCustody)> {
        if invocation.name == "url_fetch" {
            let (request, _input) = decode::<FetchRequest>(&invocation.arguments, work)?;
            request.validate_with_work(work)?;
            if let Some(work) = work {
                if !self.fetch.supports_native_fetch(work)? {
                    return Err(Error::NativeBoundary {
                        code: ErrorCode::Unsupported,
                        reason: "fetch provider has no native producer",
                    });
                }
                let (response, held) = self.fetch.fetch_with_native_work(&request, work)?.await?;
                if !held.work().shares_allocation_context(work)
                    || held.bytes() < crate::url_fetch::response_bytes(&response)?
                {
                    return Err(Error::NativeBoundary {
                        code: ErrorCode::Storage,
                        reason: "fetch response lacks original-ledger custody",
                    });
                }
                return output(&response, maximum, Some(work));
            }
            output(&self.fetch.fetch(&request).await?, maximum, work)
        } else {
            let (request, _input) = decode::<SearchRequest>(&invocation.arguments, work)?;
            request.validate_with_work(work)?;
            let provider = self.search.as_ref().ok_or(Error::ToolRejected {
                code: ErrorCode::Unavailable,
                reason: "search is not configured",
            })?;
            if let Some(work) = work {
                if !provider.supports_native_search(work)? {
                    return Err(Error::NativeBoundary {
                        code: ErrorCode::Unsupported,
                        reason: "search provider has no native producer",
                    });
                }
                let (response, held) = provider.search_with_native_work(&request, work)?.await?;
                if !held.work().shares_allocation_context(work)
                    || held.bytes() < crate::native::response_bytes(&response)?
                {
                    return Err(Error::NativeBoundary {
                        code: ErrorCode::Storage,
                        reason: "search response lacks original-ledger custody",
                    });
                }
                if response.results.len() > request.max_results as usize {
                    return Err(crate::rejection(
                        Some(work),
                        "search backend exceeded result limit",
                    ));
                }
                return output(&response, maximum, Some(work));
            }
            let response = provider.search(&request).await?;
            if response.results.len() > request.max_results as usize {
                return Err(crate::rejection(
                    work,
                    "search backend exceeded result limit",
                ));
            }
            output(&response, maximum, work)
        }
    }

    async fn run(
        &self,
        context: &ToolContext,
        invocation: &ToolInvocation,
        work: NativeWork<'_>,
    ) -> Result<(ToolResult, NativeCustody)> {
        self.authorize(None, Some(context), invocation, work)?;
        if let Some(work) = work {
            context.validate_native_mcp_context(work)?;
        }
        let maximum = context.effective_scope().limits().file_bytes;
        match self.dispatch(invocation, maximum, work).await {
            Ok(result) => Ok(result),
            Err(error) => rejection(error, maximum, work),
        }
    }
}

fn decode<T: crate::filesystem::UploadRecord>(
    value: &serde_json::Value,
    work: NativeWork<'_>,
) -> Result<(T, NativeCustody)> {
    crate::tool::decode(value, work).map_err(|error| match error {
        Error::NativeJsonAdmission { .. } | Error::NativeJson { .. } | Error::Invalid(_) => {
            crate::url_fetch::rejection(work, "invalid web request")
        }
        error => error,
    })
}

fn output(
    value: &impl Serialize,
    maximum: u64,
    work: NativeWork<'_>,
) -> Result<(ToolResult, NativeCustody)> {
    let (value, held) = crate::tool::value(value, maximum, work)?;
    Ok((ToolResult { value }, held))
}

fn rejection(
    error: Error,
    maximum: u64,
    work: NativeWork<'_>,
) -> Result<(ToolResult, NativeCustody)> {
    use crate::web::WebRejectionCode;
    #[derive(Serialize)]
    struct Rejection<'a> {
        status: &'static str,
        code: WebRejectionCode,
        reason: &'a str,
    }
    let (code, reason) = match error {
        Error::ToolRejected {
            code: ErrorCode::Invalid,
            reason,
        } => (WebRejectionCode::Invalid, reason),
        Error::ToolRejected {
            code: ErrorCode::Unavailable,
            reason,
        } => (WebRejectionCode::Unavailable, reason),
        error => return Err(error),
    };
    output(
        &Rejection {
            status: "rejected",
            code,
            reason,
        },
        maximum,
        work,
    )
}

impl ToolExecutor for WebExecutor {
    fn authorize(
        &self,
        _: Option<&RuntimeScope>,
        context: Option<&ToolContext>,
        invocation: &ToolInvocation,
        work: NativeWork<'_>,
    ) -> Result<()> {
        let denied = || Error::NativeBoundary {
            code: ErrorCode::Unauthorized,
            reason: "web capability differs from its task authority",
        };
        let context = context.ok_or_else(denied)?;
        let grant = match invocation.name.as_str() {
            "web_search" => "tool:call:web_search",
            "url_fetch" => "tool:call:url_fetch",
            _ => return Err(denied()),
        };
        if context.operation_id() != invocation.operation_id
            || context.call_id() != invocation.call_id
            || !context.effective_scope().grants().contains(grant)
        {
            return Err(denied());
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
            let context = context.ok_or(Error::NativeBoundary {
                code: ErrorCode::Unauthorized,
                reason: "web capability requires its original task context",
            })?;
            Ok(self.run(&context, &invocation, None).await?.0)
        })
    }

    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move {
            self.authorize(None, context.as_ref(), &invocation, None)?;
            // Completed results replay from the execution journal. An interrupted
            // GET has no definitive receipt; reconciliation must never redispatch.
            Ok(None)
        })
    }

    fn supports_native_execution(&self, work: &NativeMaterializationWork) -> Result<()> {
        work.remaining().map(|_| ()).map_err(Into::into)
    }

    fn execute_with_native_context<'a>(
        &'a self,
        context: &'a ToolContext,
        invocation: &'a ToolInvocation,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (ToolResult, acyclic_fs::NativeAllocation)>> {
        self.authorize(None, Some(context), invocation, Some(work))?;
        crate::NativeFutureBundle::new(work, || async move {
            let (result, held) = self.run(context, invocation, Some(work)).await?;
            Ok((
                result,
                held.ok_or(Error::NativeBoundary {
                    code: ErrorCode::Storage,
                    reason: "web result lacks native custody",
                })?,
            ))
        })
    }

    fn reconcile_with_native_context<'a>(
        &'a self,
        context: &'a ToolContext,
        invocation: &'a ToolInvocation,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<ToolResult>, acyclic_fs::NativeAllocation)>>
    {
        self.authorize(None, Some(context), invocation, Some(work))?;
        crate::NativeFutureBundle::new(work, || async move {
            let held =
                crate::tool::edit::allocation(Some(work), 0)?.ok_or(Error::NativeBoundary {
                    code: ErrorCode::Storage,
                    reason: "web reconciliation lacks native custody",
                })?;
            Ok((None, held))
        })
    }
}

#[cfg(test)]
mod tests;
