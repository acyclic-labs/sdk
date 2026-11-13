use super::*;

pub(in crate::executor) fn stock_tool_definitions(
    registry: &ToolRegistry,
    scope: &RuntimeScope,
    io: Option<&PreparationIo>,
) -> Result<Vec<crate::tool::ToolDefinition>> {
    registry.definitions_for_grants(scope.grants(), io)
}

pub(in crate::executor) fn stock_is_native(io: Option<&PreparationIo>) -> bool {
    #[cfg(not(target_arch = "wasm32"))]
    {
        io.and_then(PreparationIo::original_native_work).is_some()
    }
    #[cfg(target_arch = "wasm32")]
    {
        let _ = io;
        false
    }
}

pub(in crate::executor) fn stock_recorded_tool_result<'a>(
    provider: &dyn crate::model::ModelProvider,
    operation: OperationId,
    call_id: &str,
    io: Option<&'a PreparationIo>,
) -> Result<
    Option<(
        crate::model::RecordedToolOutcome,
        Option<crate::conversation::ContentHold<'a>>,
    )>,
> {
    #[cfg(not(target_arch = "wasm32"))]
    let work = io.and_then(PreparationIo::original_native_work);
    #[cfg(target_arch = "wasm32")]
    let work = None;
    let Some((outcome, custody)) = provider.recorded_tool_result(operation, call_id, work)? else {
        return Ok(None);
    };
    #[cfg(not(target_arch = "wasm32"))]
    let hold = if let Some(io) = io.filter(|_| work.is_some()) {
        Some(io.hold_native_custody(custody.ok_or_else(|| {
            stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Unsupported,
                "recorded tool settlement has no native custody",
            )
        })?)?)
    } else {
        None
    };
    #[cfg(target_arch = "wasm32")]
    let hold = {
        let _ = (io, custody);
        None
    };
    Ok(Some((outcome, hold)))
}

pub(in crate::executor) fn stock_authorize_tool(
    tool: &crate::tool::Tool,
    context: Option<&crate::runtime::ToolContext>,
    scope: &RuntimeScope,
    invocation: &ToolInvocation,
    io: Option<&PreparationIo>,
) -> Result<()> {
    let _ = io;
    #[cfg(not(target_arch = "wasm32"))]
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
    #[cfg(not(target_arch = "wasm32"))] native: impl FnOnce(
        &'a crate::runtime::ToolContext,
        &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<
        acyclic_fs::NativeFutureBundle<'a, (T, acyclic_fs::NativeAllocation), crate::Error>,
    >,
) -> Result<T> {
    #[cfg(not(target_arch = "wasm32"))]
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
    #[cfg(target_arch = "wasm32")]
    let _ = io;
    ordinary(context.take()).await
}

pub(in crate::executor) async fn stock_project_tool(
    tool: &crate::tool::Tool,
    invocation: &ToolInvocation,
    result: &ToolResult,
    limits: Limits,
    io: Option<&PreparationIo>,
    context: Option<&crate::runtime::TaskContext>,
) -> Result<Result<(Value, crate::model::ToolResultContent)>> {
    #[cfg(not(target_arch = "wasm32"))]
    let work = io.and_then(PreparationIo::original_native_work);
    #[cfg(target_arch = "wasm32")]
    let work = None;
    let (value, custody) = tool.projection.project(invocation, result, work)?;
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(io) = io.filter(|_| work.is_some()) {
        io.absorb_native_custody(custody.ok_or_else(|| {
            stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Unsupported,
                "tool projection has no native producer",
            )
        })?)?;
    }
    #[cfg(target_arch = "wasm32")]
    let _ = custody;
    // Check the authored projection before the harness adds its common spill
    // envelope. Large output is retained, never silently replaced with a prefix.
    if let Err(violation) = with_history_io(io, |history| {
        crate::executor::check_json_schema_value(
            &tool.definition.projection_schema,
            &value,
            "tool projection",
            history,
        )
    })? {
        return Ok(Err(violation));
    }
    let maximum = context.map_or(limits.render_bytes, |_| {
        Some(crate::tool::output::view_bytes(limits.render_bytes))
    });
    let spill = maximum
        .map(|maximum| {
            with_history_io(io, |history| {
                crate::executor::json::json_byte_length(&value, None, history)
            })
            .map(|length| length > maximum)
        })
        .transpose()?
        .unwrap_or(false);
    let value = if spill {
        let context = context.ok_or_else(|| {
            stock_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "large tool output requires its original volume context",
            )
        })?;
        let (bounded, custody) = crate::tool::output::spill(
            &value,
            context,
            invocation.operation_id,
            limits.render_bytes,
            io,
        )
        .await?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io.filter(|_| work.is_some()) {
            io.absorb_native_custody(custody.ok_or_else(|| {
                stock_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Unsupported,
                    "output spill has no native producer",
                )
            })?)?;
        }
        #[cfg(target_arch = "wasm32")]
        let _ = custody;
        bounded
    } else {
        value
    };
    let content = match tool.definition.check_projection(&value, limits, io)? {
        Ok(content) => content,
        Err(violation) => return Ok(Err(violation)),
    };
    stock_json_bound(&value, limits.render_bytes, io)?;
    Ok(Ok((value, content)))
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::model::{FileProjectionPolicy, ModelDataPart, ToolResultContent};

    struct Parts;
    impl crate::tool::ToolProjection for Parts {
        fn project(
            &self,
            _invocation: &ToolInvocation,
            result: &ToolResult,
            work: acyclic_fs::NativeWork<'_>,
        ) -> Result<(Value, acyclic_fs::NativeCustody)> {
            crate::tool::value(&result.value, None, work)
        }
    }

    #[tokio::test]
    async fn media_references_exceed_the_text_display_page_without_losing_content() -> Result<()> {
        let storage =
            crate::filesystem::MemoryHarnessStorage::new(crate::AgentId::new(), 256_000).await?;
        let file = storage
            .stage(
                OperationId::new(),
                "image.png",
                include_bytes!("../../../tests/fixtures/notebook-output.png"),
                "image/png",
                "image.png",
            )
            .await?;
        let original = ToolResultContent::Parts {
            parts: (0..128)
                .map(|_| ModelDataPart::File {
                    file: file.clone(),
                    policy: FileProjectionPolicy::for_attachment(&file),
                })
                .collect(),
        };
        let result = ToolResult {
            value: serde_json::to_value(&original).unwrap(),
        };
        let encoded = serde_json::to_vec(&result.value).unwrap();
        assert!(encoded.len() as u64 > crate::tool::VIEW_BYTES);
        let mut tool = crate::tool::files::read()?;
        tool.definition.projection_schema = Value::Bool(true);
        tool.projection = Arc::new(Parts);
        for render_bytes in [None, Some(encoded.len() as u64 + 10_000), Some(1_024)] {
            let limits = Limits {
                render_bytes,
                ..Limits::default()
            };
            let task = crate::runtime::Bindings {
                scope: RuntimeScope::new(storage.owner_scope().capabilities().clone(), limits)?,
                content: Some(storage.content_bindings()),
                ..crate::runtime::Bindings::local()
            }
            .build()?
            .agent_context(&crate::Capabilities::default(), storage.content_bindings())?;
            let invocation = ToolInvocation {
                operation_id: OperationId::new(),
                call_id: "media".into(),
                name: tool.definition.name.clone(),
                arguments: Value::Null,
            };
            let projected =
                stock_project_tool(&tool, &invocation, &result, limits, None, Some(&task)).await;
            if render_bytes == Some(1_024) {
                assert!(!matches!(projected, Ok(Ok(_))));
                continue;
            }
            let (value, content) = projected??;
            let ToolResultContent::Parts { parts: expected } = &original else {
                unreachable!()
            };
            let ToolResultContent::Parts { parts: actual } = content else {
                return Err(Error::Invalid("spill lost multipart content".into()));
            };
            assert_eq!(&actual[1..], expected.as_slice());
            let preview: Value = serde_json::from_str(
                value["parts"][0]["text"]
                    .as_str()
                    .ok_or_else(|| Error::Invalid("spill has no preview".into()))?,
            )
            .unwrap();
            assert_eq!(
                preview["omitted_bytes"].as_u64(),
                Some(encoded.len() as u64)
            );
            let context = crate::runtime::ToolContext::new(
                Arc::new(task),
                invocation.operation_id,
                &invocation.call_id,
            )?;
            let (_, retained, _) = context
                .read_tool_path(
                    preview["output_spill"]["path"]
                        .as_str()
                        .ok_or_else(|| Error::Invalid("spill has no retained path".into()))?,
                    None,
                    None,
                )
                .await?;
            assert_eq!(retained.as_ref(), encoded.as_slice());
        }
        Ok(())
    }
}
