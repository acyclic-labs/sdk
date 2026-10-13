use super::*;

pub(in crate::executor) fn stock_tool_definitions(
    registry: &ToolRegistry,
    scope: &RuntimeScope,
    io: Option<&PreparationIo>,
) -> Result<Vec<crate::tool::ToolDefinition>> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io
        && io.original_native_work().is_some()
    {
        let definitions = registry.definitions(Some(io))?;
        let mut selected = Vec::new();
        for definition in definitions {
            let key = with_history_io(Some(io), |work| {
                crate::contract::preparation_format(
                    format_args!("tool:call:{}", definition.name),
                    work,
                )
            })?;
            if scope.grants().contains(&key) {
                stock_vec_push(&mut selected, definition, Some(io))?;
            }
        }
        return Ok(selected);
    }
    Ok(registry
        .definitions(None)?
        .into_iter()
        .filter(|definition| {
            scope
                .grants()
                .contains(&capability::tool_call(&definition.name))
        })
        .collect())
}

pub(in crate::executor) fn stock_is_native(io: Option<&PreparationIo>) -> bool {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    {
        return io.and_then(PreparationIo::original_native_work).is_some();
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    {
        let _ = io;
        false
    }
}

pub(in crate::executor) fn stock_native_failure(error: &Error) -> bool {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    {
        return error.is_native_failure();
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    {
        let _ = error;
        false
    }
}

pub(in crate::executor) fn stock_model_call(
    parent: OperationId,
    step: u32,
    call: String,
    name: String,
    arguments: Value,
    io: Option<&PreparationIo>,
) -> Result<ToolInvocation> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = io.and_then(PreparationIo::original_native_work) {
        return ToolInvocation::for_model_call(parent, step, call, name, arguments, Some(work));
    }
    ToolInvocation::for_model_call(parent, step, call, name, arguments, None)
}

pub(in crate::executor) fn stock_tool_lookup<'a>(
    registry: &'a ToolRegistry,
    name: &str,
    io: Option<&PreparationIo>,
) -> Result<&'a crate::tool::Tool> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if io.and_then(PreparationIo::original_native_work).is_some() {
        return registry
            .get(name, None, io)?
            .ok_or_else(|| crate::Error::NativeBoundary {
                code: crate::wire::ErrorCode::NotFound,
                reason: "tool is not bound to this stock composition",
            });
    }
    registry
        .get(name, None, io)?
        .ok_or_else(|| Error::NotFound(format!("tool {name}")))
}

pub(in crate::executor) fn stock_tool_support(
    tool: &crate::tool::Tool,
    io: Option<&PreparationIo>,
) -> Result<()> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = io.and_then(PreparationIo::original_native_work) {
        tool.executor.supports_native_execution(work)?;
        tool.projection.supports_native_projection(Some(work))?;
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let _ = (tool, io);
    Ok(())
}

pub(in crate::executor) fn stock_authorize_tool(
    tool: &crate::tool::Tool,
    context: Option<&crate::runtime::ToolContext>,
    scope: &RuntimeScope,
    invocation: &ToolInvocation,
    io: Option<&PreparationIo>,
) -> Result<()> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = io.and_then(PreparationIo::original_native_work) {
        let context = context.ok_or_else(|| crate::Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "native tool authorization has no issued context",
        })?;
        return tool
            .executor
            .authorize_with_native_context(context, invocation, work);
    }
    if let Some(context) = context {
        tool.executor.authorize_with_context(context, invocation)
    } else {
        tool.executor.authorize(Some(scope), invocation)
    }
}

pub(in crate::executor) async fn stock_execute_tool(
    tool: &crate::tool::Tool,
    context: &mut Option<crate::runtime::ToolContext>,
    invocation: &ToolInvocation,
    io: Option<&PreparationIo>,
) -> Result<ToolResult> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io
        && let Some(work) = io.original_native_work()
    {
        let context = context
            .as_ref()
            .ok_or_else(|| crate::Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "native tool execution has no issued context",
            })?;
        let (result, custody) = tool
            .executor
            .execute_with_native_context(context, invocation, work)?
            .await?;
        io.absorb_native_custody(custody)?;
        return Ok(result);
    }
    if let Some(context) = context.take() {
        tool.executor
            .execute_with_context(context, invocation.clone())
            .await
    } else {
        tool.executor.execute(invocation.clone()).await
    }
}

pub(in crate::executor) async fn stock_reconcile_tool(
    tool: &crate::tool::Tool,
    context: &mut Option<crate::runtime::ToolContext>,
    invocation: &ToolInvocation,
    io: Option<&PreparationIo>,
) -> Result<Option<ToolResult>> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io
        && let Some(work) = io.original_native_work()
    {
        let context = context
            .as_ref()
            .ok_or_else(|| crate::Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "native tool reconciliation has no issued context",
            })?;
        let (result, custody) = tool
            .executor
            .reconcile_with_native_context(context, invocation, work)?
            .await?;
        io.absorb_native_custody(custody)?;
        return Ok(result);
    }
    if let Some(context) = context.take() {
        tool.executor
            .reconcile_with_context(context, invocation.clone())
            .await
    } else {
        tool.executor.reconcile(invocation.clone()).await
    }
}

pub(in crate::executor) fn stock_project_tool(
    tool: &crate::tool::Tool,
    invocation: &ToolInvocation,
    result: &ToolResult,
    limits: Limits,
    io: Option<&PreparationIo>,
) -> Result<(Value, crate::model::ToolResultContent)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io
        && let Some(work) = io.original_native_work()
    {
        let (value, custody) = tool.projection.project(invocation, result, Some(work))?;
        let custody = custody.ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "tool projection has no native producer",
        })?;
        io.absorb_native_custody(custody)?;
        let content = tool
            .definition
            .validate_projection(&value, limits, Some(io))?;
        io.check_json_byte_bound(&value, limits.render_bytes)?;
        return Ok((value, content));
    }
    let value = tool.projection.project(invocation, result, None)?.0;
    let content = tool.definition.validate_projection(&value, limits, io)?;
    if crate::contract::canonical_json_bytes(&value)?.len() as u64 > limits.render_bytes {
        return Err(Error::Invalid(
            "tool projection exceeds render limit".into(),
        ));
    }
    Ok((value, content))
}

pub(in crate::executor) fn stock_task_interaction_id(
    task: TaskId,
    operation: OperationId,
    io: Option<&PreparationIo>,
) -> Result<InteractionId> {
    #[cfg(feature = "filesystem")]
    let hashed = acyclic_fs::WorkCounters {
        bytes_hashed: (b"harness/v2/task-interaction\0".len() + 32) as u64,
        ..Default::default()
    };
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        io.preflight(hashed)?;
    }
    let id = crate::durable_host::task_interaction_id(task, operation, &mut None)?;
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        io.spent(hashed)?;
    }
    #[cfg(not(feature = "filesystem"))]
    let _ = io;
    Ok(id)
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl StockExecution<'_> {
    pub(in crate::executor) async fn settle_native_tool_policy(
        &self,
        journal: &dyn ExecutionJournal,
        operation: OperationId,
        invocation: &ToolInvocation,
        definition: &crate::tool::ToolDefinition,
        started: bool,
    ) -> Result<Option<ExecutionProgress<()>>> {
        let Some(work) = self.native_work() else {
            return Ok(None);
        };
        let (context, _) = self.context.ok_or(crate::Error::NativeBoundary {
            code: crate::wire::ErrorCode::Storage,
            reason: "stock policy has no issued task",
        })?;
        let io = self.io.expect("native execution ledger");
        for (_, policy) in context
            .stock_tool_policy_binding()
            .into_iter()
            .chain(context.stock_tool_policy_overrides())
        {
            let Some(((approval_operation, request), custody)) = context
                .stock_policy_approval(
                    policy.as_ref(),
                    &self.stock.tool_scope,
                    definition,
                    invocation,
                    work,
                )
                .await?
            else {
                continue;
            };
            io.absorb_native_custody(custody)?;
            if !self
                .stock
                .tool_scope
                .grants()
                .contains(capability::INTERACTION_ROUTE)
            {
                return Err(crate::Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "stock tool approval requires interaction:route",
                });
            }
            let task = self.stock.task.as_ref().map_or(
                TaskId::from_bytes(operation.into_bytes()),
                |(_, task, _)| *task,
            );
            let approval = stock_task_interaction_id(task, approval_operation, Some(io))?;
            journal
                .open_interaction_with_native_work(approval, &request, work)?
                .await?;
            let (outcome, custody) = journal
                .interaction_outcome_with_native_work(approval, work)?
                .await?;
            io.absorb_native_custody(custody)?;
            let Some(outcome) = outcome else {
                return if started {
                    Err(Error::Indeterminate(invocation.operation_id))
                } else {
                    Ok(Some(ExecutionProgress::Pending(invocation.operation_id)))
                };
            };
            if matches!(outcome, InteractionOutcome::Answered { .. }) {
                return Err(crate::Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Conflict,
                    reason: "approval resolved with a question answer",
                });
            }
            match check_tool_approval(outcome) {
                Ok(()) => {}
                Err(Error::InteractionRejected(reason)) if !started => {
                    return Ok(Some(ExecutionProgress::Rejected(reason)));
                }
                Err(Error::InteractionRejected(_)) => {
                    return Err(Error::Indeterminate(invocation.operation_id));
                }
                Err(error) => return Err(error),
            }
        }
        Ok(None)
    }
}

pub(in crate::executor) fn stock_validate_invocation(
    invocation: &ToolInvocation,
    io: Option<&PreparationIo>,
) -> Result<()> {
    crate::executor::with_history_io(io, |work| {
        ToolInvocation::validate_identity(&invocation.call_id, Some(&invocation.name), work)
    })?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io
        && io.original_native_work().is_some()
    {
        crate::contract::json_owned_value_shape_bounded(
            &invocation.arguments,
            crate::contract::JsonBounds {
                maximum_bytes: u64::MAX,
                maximum_items: io.remaining()?.items_examined,
                maximum_depth: 256,
            },
            crate::contract::JsonValueSchema::None,
            &mut |next| io.codec_charge(next, false),
        )?;
        return Ok(());
    }
    Ok(())
}

pub(in crate::executor) async fn stock_tool_rejection(
    session: &StockExecution<'_>,
    journal: &dyn ExecutionJournal,
    operation: OperationId,
    step: u32,
    call: &str,
    kind: ToolFailureKind,
    error: Error,
) -> Result<ExecutionProgress<()>> {
    if stock_native_failure(&error) {
        return Err(error);
    }
    session
        .record_tool_failure(journal, operation, step, call, kind)
        .await?;
    Err(stock_boundary(
        session.io,
        crate::wire::ErrorCode::Invalid,
        kind.message(),
    ))
}
