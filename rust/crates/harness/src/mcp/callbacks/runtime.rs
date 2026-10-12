use super::*;

/// Concrete recorded model/interaction adapter.
pub struct McpRuntimeCallbacks {
    executor: StockExecutor,
    journal: Arc<dyn ExecutionJournal>,
    model_name: String,
    sampling: McpSamplingPolicy,
}
impl McpRuntimeCallbacks {
    /// Selects the existing execution owner and model display identity.
    pub fn new(
        executor: StockExecutor,
        journal: Arc<dyn ExecutionJournal>,
        model_name: String,
        sampling: McpSamplingPolicy,
    ) -> Result<Self> {
        if model_name.is_empty() {
            return Err(Error::Invalid("MCP sampling model name is empty".into()));
        }
        sampling.continuation.validate_user_input(&mut None)?;
        if sampling
            .image
            .as_ref()
            .is_some_and(|policy| !matches!(policy.intent, NativeMediaIntent::Image { .. }))
            || sampling
                .audio
                .as_ref()
                .is_some_and(|policy| !matches!(policy.intent, NativeMediaIntent::Audio { .. }))
        {
            return Err(Error::Invalid(
                "MCP sampling media intent differs from selected modality".into(),
            ));
        }
        Ok(Self {
            executor: executor.with_tool_registry(crate::tool::ToolRegistry::default()),
            journal,
            model_name,
            sampling,
        })
    }
    async fn pin(&self, operation: OperationId, request: &McpCallbackRequest) -> Result<()> {
        self.journal
            .stage(
                operation,
                "mcp:callback:request".into(),
                crate::contract::canonical_json_bytes(&(
                    request,
                    &self.model_name,
                    &self.sampling,
                ))?,
                "application/json",
            )
            .await?;
        Ok(())
    }
}
impl McpRuntimeCallbacks {
    /// Admits and replays a callback through its recorded model or interaction owner.
    pub fn admit<'a>(
        &'a self,
        context: ToolContext,
        request: &'a McpCallbackRequest,
    ) -> BoxProviderFuture<'a, Result<Value>> {
        Box::pin(async move {
            crate::contract::json_byte_length(
                request,
                context.scope().limits().file_bytes,
                &mut None,
            )
            .map(|_| ())?;
            let operation = request.validate()?;
            if request.parent != context.operation_id() {
                return Err(Error::Unauthorized(
                    "MCP callback parent differs from admitted call".into(),
                ));
            }
            let method = request
                .message
                .get("method")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::Invalid("MCP callback method absent".into()))?;
            let params = request
                .message
                .get("params")
                .ok_or_else(|| Error::Invalid("MCP callback parameters absent".into()))?;
            let result = match method {
                "sampling/createMessage" if request.policy.sampling => {
                    if !context.scope().grants().contains("model:generate") {
                        return Err(Error::Unauthorized(
                            "MCP sampling lacks model:generate".into(),
                        ));
                    }
                    if params
                        .get("tools")
                        .is_some_and(|tools| tools.as_array().is_none_or(|tools| !tools.is_empty()))
                    {
                        return Err(Error::Unsupported(
                            "MCP sampling tool projection was not selected by this host".into(),
                        ));
                    }
                    let max_tokens = params
                        .get("maxTokens")
                        .and_then(Value::as_u64)
                        .and_then(|tokens| u32::try_from(tokens).ok())
                        .ok_or_else(|| Error::Invalid("invalid MCP sampling token bound".into()))?;
                    self.pin(operation, request).await?;
                    let (input, prior) =
                        sampling_messages(params, max_tokens, &context, operation, &self.sampling)
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
                    let mut text = String::new();
                    for event in events {
                        match event {
                            ModelEvent::Content { delta } => text.push_str(&delta),
                            ModelEvent::ToolCall { .. } => {
                                return Err(Error::Unsupported(
                                    "MCP sampling tools require host-selected tool projection"
                                        .into(),
                                ));
                            }
                            ModelEvent::Reasoning { .. } | ModelEvent::Completed { .. } => {}
                        }
                    }
                    json!({"role":"assistant","content":{"type":"text","text":text},"model":self.model_name,"stopReason":"endTurn"})
                }
                "elicitation/create" if request.policy.elicitation => {
                    if !context.scope().grants().contains("interaction:route") {
                        return Err(Error::Unauthorized(
                            "MCP elicitation lacks interaction:route".into(),
                        ));
                    }
                    if params
                        .get("mode")
                        .and_then(Value::as_str)
                        .is_some_and(|mode| mode != "form")
                    {
                        return Err(Error::Unsupported(
                            "MCP URL elicitation requires a separately installed host interaction"
                                .into(),
                        ));
                    }
                    let interaction = Interaction::Form {
                        title: params
                            .get("message")
                            .and_then(Value::as_str)
                            .ok_or_else(|| Error::Invalid("MCP form message absent".into()))?
                            .into(),
                        schema: params
                            .get("requestedSchema")
                            .cloned()
                            .ok_or_else(|| Error::Invalid("MCP form schema absent".into()))?,
                    };
                    interaction.validate(&mut None)?;
                    self.pin(operation, request).await?;
                    match context.interact(operation, interaction.clone()).await? {
                        InteractionOutcome::Answered { answer } => {
                            let bytes = self.journal.load(&answer).await?;
                            answer.descriptor().verify(&bytes, &mut None)?;
                            let response: InteractionResponse =
                                crate::contract::json_from_slice(&bytes)
                                    .map_err(|error| Error::Invalid(error.to_string()))?;
                            interaction.validate_response(&response, &mut None)?;
                            let InteractionResponse::Form { value } = response else {
                                return Err(Error::Invalid("MCP form answer kind differs".into()));
                            };
                            json!({"action":"accept","content":value})
                        }
                        InteractionOutcome::Declined | InteractionOutcome::Denied => {
                            json!({"action":"decline"})
                        }
                        InteractionOutcome::Cancelled | InteractionOutcome::Expired => {
                            json!({"action":"cancel"})
                        }
                        InteractionOutcome::Indeterminate { .. } => {
                            return Err(Error::Indeterminate(operation));
                        }
                        InteractionOutcome::Approved => {
                            return Err(Error::Invalid("MCP form received approval".into()));
                        }
                    }
                }
                _ => {
                    return Err(Error::Unsupported(
                        "MCP callback not enabled by pinned host policy".into(),
                    ));
                }
            };
            Ok(json!({"jsonrpc":"2.0","id":request.message.get("id"),"result":result}))
        })
    }
}
