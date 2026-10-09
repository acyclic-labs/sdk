//! Host composition for the example's one exactly approved MCP invocation.
//! The ordinary tool/effect journals and native provider own all execution.

use super::*;
use acyclic_harness::{
    Error, Outcome,
    durable_tool::DurableToolRunner,
    executor::{ExecutionEvent, ExecutionRecord},
    mcp::{
        McpCatalog, McpDiscoveryPolicy, McpSchemaExposure, McpToolDefinition, McpToolResult,
        McpToolTransport,
    },
    runtime::{TaskContext, ToolContext},
    tool::{ToolInvocation, ToolProjection, ToolRegistry, ToolResult},
};
use acyclic_stream::BoxProviderFuture;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(super) struct Approved<P> {
    pub recovery: Recovery<P>,
    pub task: TaskId,
    pub command: OperationId,
    pub plan: TaskEffectPlan,
    pub request: NativeProcessRequest,
}

struct Transport<P> {
    approved: Approved<P>,
    calls: AtomicUsize,
    reconciliations: AtomicUsize,
}

impl<P: StreamProvider> Transport<P> {
    fn descriptor(&self) -> Result<&McpStdioRequest> {
        self.approved
            .request
            .mcp_stdio
            .as_ref()
            .ok_or_else(|| Error::Unsupported("MCP descriptor required".into()))
    }

    fn authorize(&self, context: &ToolContext) -> Result<()> {
        let descriptor = self.descriptor()?;
        if context.task().durable_task_id() != Some(self.approved.task)
            || context.operation_id() != descriptor.operation
            || context.call_id() != descriptor.operation.to_string()
            || !context.task().scope().grants().contains("mcp:call:example")
            || !context
                .task()
                .scope()
                .grants()
                .contains("tool:call:mcp.example.echo")
        {
            return Err(Error::Unauthorized(
                "approved native tool context differs".into(),
            ));
        }
        Ok(())
    }

    async fn result(&self, status: EffectStatus) -> Result<McpToolResult> {
        if let EffectStatus::Failed { message } = status {
            return Ok(McpToolResult {
                content: vec![json!({"type":"text","text":message})],
                structured_content: None,
                is_error: true,
                meta: None,
            });
        }
        let EffectStatus::Succeeded { result } = status else {
            return Err(Error::Indeterminate(self.descriptor()?.operation));
        };
        let output: NativeProcessResult =
            serde_json::from_slice(&self.approved.recovery.content.read(&result).await?)
                .map_err(|error| Error::Invalid(error.to_string()))?;
        serde_json::from_value(self.approved.request.mcp_response(&output)?)
            .map_err(|error| Error::Invalid(error.to_string()))
    }
}

impl<P: StreamProvider> McpToolTransport for Transport<P> {
    fn call<'a>(
        &'a self,
        _: OperationId,
        _: &'a str,
        _: Value,
    ) -> BoxProviderFuture<'a, Result<McpToolResult>> {
        Box::pin(async { Err(Error::Unauthorized("admitted task context required".into())) })
    }
    fn reconcile<'a>(
        &'a self,
        _: OperationId,
    ) -> BoxProviderFuture<'a, Result<Option<McpToolResult>>> {
        Box::pin(async { Err(Error::Unauthorized("admitted task context required".into())) })
    }
    fn call_with_context<'a>(
        &'a self,
        context: ToolContext,
        name: &'a str,
        arguments: Value,
    ) -> BoxProviderFuture<'a, Result<McpToolResult>> {
        Box::pin(async move {
            self.authorize(&context)?;
            let descriptor = self.descriptor()?;
            if descriptor.method != McpStdioMethod::CallTool
                || descriptor.params != json!({"name":name,"arguments":arguments})
            {
                return Err(Error::Unauthorized(
                    "MCP arguments differ from exact native approval".into(),
                ));
            }
            self.calls.fetch_add(1, Ordering::SeqCst);
            let binding = &self.approved;
            self.result(
                binding
                    .recovery
                    .effects
                    .run_task_effect(
                        &binding.recovery.owner,
                        binding.command,
                        binding.plan.clone(),
                    )
                    .await?,
            )
            .await
        })
    }
    fn reconcile_with_context<'a>(
        &'a self,
        context: ToolContext,
    ) -> BoxProviderFuture<'a, Result<Option<McpToolResult>>> {
        Box::pin(async move {
            self.authorize(&context)?;
            self.reconciliations.fetch_add(1, Ordering::SeqCst);
            let binding = &self.approved;
            match binding
                .recovery
                .effects
                .reconcile_task_effect(&binding.recovery.owner, binding.command, &binding.plan)
                .await
            {
                Ok(status @ (EffectStatus::Succeeded { .. } | EffectStatus::Failed { .. })) => {
                    self.result(status).await.map(Some)
                }
                Ok(_) | Err(Error::NotFound(_)) => Ok(None),
                Err(error) => Err(error),
            }
        })
    }
}

struct Projection;
impl ToolProjection for Projection {
    fn project(&self, _: &ToolInvocation, result: &ToolResult) -> Result<Value> {
        Ok(result.value.clone())
    }
}

fn catalog() -> McpCatalog {
    let schema = json!({"type":"object","required":["text","sequence"],"additionalProperties":false,
        "properties":{"text":{"type":"string"},"sequence":{"type":"integer"}}});
    McpCatalog {
        server: "example".into(),
        revision: "approved-1".into(),
        schema_exposure: McpSchemaExposure::Eager,
        discovery: McpDiscoveryPolicy::Search,
        tools: vec![McpToolDefinition {
            name: "echo".into(),
            title: None,
            description: "Approved native echo".into(),
            input_schema: schema.clone(),
            output_schema: Some(schema),
        }],
    }
}

pub(super) async fn run<P: StreamProvider>(
    approved: Approved<P>,
    task: TaskContext,
    journal: Arc<dyn ExecutionJournal>,
    replay: bool,
    completed: bool,
    interrupted: bool,
) -> Result<()> {
    let descriptor = approved
        .request
        .mcp_stdio
        .as_ref()
        .ok_or_else(|| Error::Unsupported("MCP descriptor required".into()))?;
    let operation = descriptor.operation;
    let arguments = descriptor
        .params
        .get("arguments")
        .cloned()
        .ok_or_else(|| Error::Invalid("MCP arguments missing".into()))?;
    let task_id = approved.task;
    let transport = Arc::new(Transport {
        approved,
        calls: AtomicUsize::new(0),
        reconciliations: AtomicUsize::new(0),
    });
    let bound: Arc<dyn McpToolTransport> = transport.clone();
    let projection: Arc<dyn ToolProjection> = Arc::new(Projection);
    let mut tools = ToolRegistry::new();
    catalog().install(&mut tools, None, &bound, &projection, 1, 8192)?;
    let definition = tools
        .get("mcp.example.echo")
        .ok_or_else(|| Error::NotFound("native MCP tool".into()))?
        .definition
        .clone();
    let context = ToolContext::new(task, operation, operation.to_string())?;
    if !replay {
        let mut changed = arguments.clone();
        *changed
            .get_mut("text")
            .ok_or_else(|| Error::Invalid("approved text missing".into()))? = json!("unapproved");
        if !matches!(
            transport
                .call_with_context(context.clone(), "echo", changed)
                .await,
            Err(Error::Unauthorized(_))
        ) {
            return Err(Error::Invalid(
                "changed native MCP arguments were accepted".into(),
            ));
        }
    }
    let runner = DurableToolRunner::new(tools, journal.clone());
    for _ in 0..if interrupted && !replay { 1 } else { 2 } {
        let result = runner
            .run_with_context(
                task_id,
                operation,
                definition.clone(),
                arguments.clone(),
                context.clone(),
            )
            .await?;
        verify_result(result, &arguments, operation, completed)?;
    }
    verify_journal(journal.as_ref(), operation, completed).await?;
    if transport.calls.load(Ordering::SeqCst) != usize::from(!replay)
        || transport.reconciliations.load(Ordering::SeqCst)
            != if completed {
                usize::from(interrupted && replay)
            } else if replay {
                2
            } else {
                usize::from(!interrupted)
            }
    {
        return Err(Error::Invalid(
            "native tool replay repeated dispatch or skipped required reconciliation".into(),
        ));
    }
    Ok(())
}

async fn verify_journal(
    journal: &dyn ExecutionJournal,
    operation: OperationId,
    completed: bool,
) -> Result<()> {
    let mut records = Vec::new();
    for _ in 0..4 {
        let after = records
            .last()
            .map_or(0, |record: &ExecutionRecord| record.sequence);
        let batch = journal.replay(operation, after, 4).await?;
        if batch.is_empty() {
            break;
        }
        records.extend(batch);
        if records.len() > 3 {
            return Err(Error::Invalid(
                "native MCP tool journal has excess events".into(),
            ));
        }
    }
    if !matches!(
        records.first().map(|record| &record.event),
        Some(ExecutionEvent::Started { .. })
    ) || !matches!(
        records.get(1).map(|record| &record.event),
        Some(ExecutionEvent::ToolStarted { .. })
    ) || records.len() != if completed { 3 } else { 2 }
        || (completed
            && !matches!(
                records.get(2).map(|record| &record.event),
                Some(ExecutionEvent::ToolCompleted { .. })
            ))
    {
        return Err(Error::Invalid(
            "native MCP tool journal changed its dispatch or completion boundary".into(),
        ));
    }
    Ok(())
}

fn verify_result(
    outcome: Outcome<Value>,
    arguments: &Value,
    operation: OperationId,
    completed: bool,
) -> Result<()> {
    if !completed {
        return if matches!(outcome, Outcome::Indeterminate { operation_id } if operation_id == operation)
        {
            Ok(())
        } else {
            Err(Error::Invalid(format!(
                "unknown native MCP outcome became authoritative: {outcome:?}"
            )))
        };
    }
    let Outcome::Succeeded(value) = outcome else {
        return Err(Error::Invalid(format!(
            "native MCP tool did not succeed: {outcome:?}"
        )));
    };
    let result: McpToolResult =
        serde_json::from_value(value).map_err(|error| Error::Invalid(error.to_string()))?;
    if result.structured_content.as_ref() != Some(arguments) || result.is_error {
        return Err(Error::Invalid("native MCP tool result changed".into()));
    }
    Ok(())
}
