use super::*;

/// Concrete recorded model/interaction adapter.
pub struct McpRuntimeCallbacks {
    executor: StockExecutor,
    journal: Arc<dyn ExecutionJournal>,
    model_name: String,
    continuation: ModelContent,
    image: Option<NativeMediaPolicy>,
    audio: Option<NativeMediaPolicy>,
}
impl McpRuntimeCallbacks {
    /// Selects the existing execution owner and model display identity.
    pub fn new(
        executor: StockExecutor,
        journal: Arc<dyn ExecutionJournal>,
        model_name: String,
        continuation: ModelContent,
        image: Option<NativeMediaPolicy>,
        audio: Option<NativeMediaPolicy>,
    ) -> Result<Self> {
        if model_name.is_empty() {
            return Err(Error::Invalid("MCP sampling model name is empty".into()));
        }
        continuation.validate_user_input(&mut None)?;
        if image
            .as_ref()
            .is_some_and(|policy| !matches!(policy.intent, NativeMediaIntent::Image { .. }))
            || audio
                .as_ref()
                .is_some_and(|policy| !matches!(policy.intent, NativeMediaIntent::Audio { .. }))
        {
            return Err(Error::Invalid(
                "MCP sampling media intent differs from selected modality".into(),
            ));
        }
        Ok(Self {
            executor: executor.without_tool_authority(),
            journal,
            model_name,
            continuation,
            image,
            audio,
        })
    }
    async fn pin(&self, operation: OperationId, request: &McpCallbackRequest) -> Result<()> {
        self.journal
            .stage(
                operation,
                "mcp:callback:request",
                &crate::contract::canonical_json_bytes(&(
                    request,
                    &self.model_name,
                    json!({"continuation":self.continuation,"image":self.image,"audio":self.audio}),
                ))?,
                "application/json",
                None,
            )?
            .await?;
        Ok(())
    }
}
impl McpRuntimeCallbacks {
    /// Admits and replays a callback through its recorded model owner.
    pub async fn admit(
        &self,
        context: ToolContext,
        request: &McpCallbackRequest,
    ) -> Result<(OperationId, Value)> {
        crate::executor::json::json_byte_length(
            request,
            context.scope().limits().file_bytes,
            &mut None,
        )
        .map(|_| ())?;
        let (operation, decoded) = request.validate()?;
        if request.parent != context.operation_id() {
            return Err(Error::Unauthorized(
                "MCP callback parent differs from admitted call".into(),
            ));
        }
        let result = match decoded {
            rmcp::model::ServerRequest::CreateMessageRequest(sampling) => {
                self.admit_sampling(&context, request, operation, sampling.params)
                    .await?
            }
            _ => {
                return Err(Error::Unsupported(
                    "MCP callback not enabled by pinned host policy".into(),
                ));
            }
        };
        let reply = json!({"jsonrpc":"2.0","id":request.message.get("id"),"result":result});
        Ok((operation, reply))
    }
    async fn admit_sampling(
        &self,
        context: &ToolContext,
        request: &McpCallbackRequest,
        operation: OperationId,
        params: rmcp::model::CreateMessageRequestParams,
    ) -> Result<Value> {
        if !context.scope().grants().contains("model:generate") {
            return Err(Error::Unauthorized(
                "MCP sampling lacks model:generate".into(),
            ));
        }
        if request
            .message
            .pointer("/params/tools")
            .is_some_and(|tools| tools.as_array().is_none_or(|tools| !tools.is_empty()))
        {
            return Err(Error::Unsupported(
                "MCP sampling tool projection was not selected by this host".into(),
            ));
        }
        let max_tokens = params.max_tokens;
        self.pin(operation, request).await?;
        let (input, prior) = sampling_messages(
            params,
            context,
            operation,
            &self.continuation,
            self.image.as_ref(),
            self.audio.as_ref(),
        )
        .await?;
        let stage = Arc::new(SourceStage::new(
            "mcp-sampling",
            operation.to_string(),
            Arc::new(Context {
                messages: prior,
                ..Context::default()
            }),
            ContextPlacement::Prepend,
        ));
        let executor = self
            .executor
            .clone()
            .with_max_output_tokens(max_tokens)?
            .with_context_stage(stage)?
            .with_task_context(context.task(), operation)?;
        let events = executor
            .model_step(
                self.journal.as_ref(),
                &TurnInput {
                    operation_id: operation,
                    input,
                    selected_context: None,
                    max_steps: 1,
                },
                0,
                &[],
            )
            .await?;
        let completed_message = events
            .iter()
            .any(|event| matches!(event, ModelEvent::MessageCompleted { .. }));
        let mut text = String::new();
        for event in events {
            match event {
                ModelEvent::Content { delta } if !completed_message => text.push_str(&delta),
                ModelEvent::MessageCompleted { text: completed } => text = completed,
                ModelEvent::ToolCall(_) => {
                    return Err(Error::Unsupported(
                        "MCP sampling tools require host-selected tool projection".into(),
                    ));
                }
                ModelEvent::Content { .. }
                | ModelEvent::Reasoning { .. }
                | ModelEvent::ReasoningBlock { .. }
                | ModelEvent::Completed { .. } => {}
            }
        }
        Ok(
            json!({"role":"assistant","content":{"type":"text","text":text},"model":self.model_name,"stopReason":"endTurn"}),
        )
    }
}
