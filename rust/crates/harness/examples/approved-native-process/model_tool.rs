//! Host composition for the example's one exactly approved MCP invocation.

use super::*;
use acyclic_harness::{
    Error, Outcome,
    durable_tool::DurableToolRunner,
    mcp::{McpCatalog, McpDiscoveryPolicy, McpSchemaExposure, McpToolDefinition, McpToolTransport},
    runtime::{TaskContext, ToolContext},
    tool::{ToolProjection, ToolRegistry},
};
use acyclic_stream::BoxProviderFuture;

pub(super) struct Approved<P> {
    pub owner: Arc<TaskJournalOwner<P>>,
    pub effects: Arc<ConversationEffectHost<P>>,
    pub content: Arc<dyn ContentResidencyVerifier>,
    pub task: TaskId,
    pub command: OperationId,
    pub plan: TaskEffectPlan,
    pub request: NativeProcessRequest,
}

struct Transport<P> {
    approved: Approved<P>,
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

    async fn result(&self, status: EffectStatus) -> Result<rmcp::model::CallToolResult> {
        if let EffectStatus::Failed { message } = status {
            return Ok(rmcp::model::CallToolResult::error(vec![
                rmcp::model::ContentBlock::text(message),
            ]));
        }
        let EffectStatus::Succeeded { result } = status else {
            return Err(Error::Indeterminate(self.descriptor()?.operation));
        };
        let output: NativeProcessResult = serde_json::from_slice(
            &self
                .approved
                .content
                .read(&result, 1_000_000, None)?
                .await?,
        )
        .map_err(|error| Error::Invalid(error.to_string()))?;
        serde_json::from_value(self.approved.request.mcp_response(&output)?)
            .map_err(|error| Error::Invalid(error.to_string()))
    }
}

impl<P: StreamProvider> McpToolTransport for Transport<P> {
    fn call<'a>(
        &'a self,
        operation: OperationId,
        name: &'a str,
        arguments: Value,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<rmcp::model::CallToolResult>> {
        Box::pin(async move {
            let context = context
                .ok_or_else(|| Error::Unauthorized("admitted task context required".into()))?;
            if context.operation_id() != operation {
                return Err(Error::Unauthorized("admitted operation changed".into()));
            }
            self.authorize(&context)?;
            let descriptor = self.descriptor()?;
            if descriptor.method != "tools/call"
                || descriptor.params != json!({"name":name,"arguments":arguments})
            {
                return Err(Error::Unauthorized(
                    "MCP arguments differ from exact native approval".into(),
                ));
            }
            let binding = &self.approved;
            self.result(
                binding
                    .effects
                    .run_task_effect(&binding.owner, binding.command, &binding.plan, None, false)
                    .await
                    .map(|result| result.0)?,
            )
            .await
        })
    }
    fn reconcile<'a>(
        &'a self,
        operation: OperationId,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<rmcp::model::CallToolResult>>> {
        Box::pin(async move {
            let context = context
                .ok_or_else(|| Error::Unauthorized("admitted task context required".into()))?;
            if context.operation_id() != operation {
                return Err(Error::Unauthorized("admitted operation changed".into()));
            }
            self.authorize(&context)?;
            let binding = &self.approved;
            match binding
                .effects
                .run_task_effect(&binding.owner, binding.command, &binding.plan, None, true)
                .await
                .map(|result| result.0)
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

fn catalog() -> McpCatalog {
    let schema = json!({"type":"object","required":["text","sequence"],"additionalProperties":false,
        "properties":{"text":{"type":"string"},"sequence":{"type":"integer"}}});
    McpCatalog {
        server: "example".into(),
        revision: "approved-1".into(),
        schema_exposure: McpSchemaExposure::Eager,
        projection_schema: acyclic_harness::tool::json_projection_schema(json!({})),
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
    task: Arc<TaskContext>,
    journal: Arc<dyn ExecutionJournal>,
    replay: bool,
    completed: bool,
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
    let transport = Arc::new(Transport { approved });
    let bound: Arc<dyn McpToolTransport> = transport.clone();
    let projection: Arc<dyn ToolProjection> = Arc::new(acyclic_harness::tool::JsonToolProjection);
    let mut tools = ToolRegistry::default();
    catalog().install(&mut tools, None, &bound, &projection, 1, 8192)?;
    let definition = tools
        .get("mcp.example.echo", None, None)?
        .ok_or_else(|| Error::NotFound("native MCP tool".into()))?
        .definition
        .clone();
    let context = ToolContext::new(task, operation, &(operation.to_string()), None)?;
    if !replay {
        let mut changed = arguments.clone();
        *changed
            .get_mut("text")
            .ok_or_else(|| Error::Invalid("approved text missing".into()))? = json!("unapproved");
        if !matches!(
            transport
                .call(
                    context.operation_id(),
                    "echo",
                    changed,
                    Some(context.clone())
                )
                .await,
            Err(Error::Unauthorized(_))
        ) {
            return Err(Error::Invalid(
                "changed native MCP arguments were accepted".into(),
            ));
        }
    }
    let runner = DurableToolRunner::new(&tools, journal.clone(), None)?;
    let result = runner
        .run(
            task_id,
            &definition,
            &acyclic_harness::tool::ToolInvocation {
                operation_id: operation,
                call_id: operation.to_string(),
                name: definition.name.clone(),
                arguments: arguments.clone(),
            },
            &context,
            None,
            false,
        )?
        .await?
        .0;
    verify_result(result, &arguments, operation, completed)?;
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
    if value.get("structuredContent") != Some(arguments)
        || value.get("isError") == Some(&Value::Bool(true))
    {
        return Err(Error::Invalid("native MCP tool result changed".into()));
    }
    Ok(())
}
