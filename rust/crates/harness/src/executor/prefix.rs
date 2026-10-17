use super::*;

impl StockExecution<'_> {
    pub(super) async fn completed_tool_prefix(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        through_call: Option<&str>,
    ) -> Result<crate::model::PreparedModelRequest> {
        self.verify_task_context_binding(input.operation_id)?;
        let records = self
            .prefix_records(journal, input.operation_id, step)
            .await?;
        self.verify_prefix_identity(&records, input)?;
        let retained = crate::executor::retain_preparation(
            retained_model_step(
                journal,
                &records,
                step,
                ModelPurpose::Response,
                self.stock.limits,
                self.io,
            )
            .await?,
        )?;
        let prepared = retained
            .request
            .ok_or(Error::Indeterminate(input.operation_id))?;
        let mut request = crate::model::clone_request(prepared.request(), self.io)?;
        if !retained.admission.completed
            || retained.admission.calls.is_empty()
            || through_call
                .is_some_and(|id| !retained.admission.calls.iter().any(|call| call == id))
        {
            return Err(Error::Indeterminate(input.operation_id));
        }
        let mut results = Vec::new();
        for event in retained.events {
            let (invocation, metadata) = match event {
                ModelEvent::ReasoningBlock { block } => {
                    self.append_assistant_part(
                        &mut request.messages,
                        ModelContentPart::Reasoning { block },
                    )?;
                    continue;
                }
                ModelEvent::ToolCall(call) => (
                    ToolInvocation::for_model_call(
                        input.operation_id,
                        step,
                        call.call_id,
                        call.name,
                        call.arguments,
                        self.native_work(),
                    )?,
                    call.metadata,
                ),
                _ => continue,
            };
            let projection = completed_tool_projection(
                journal,
                &records,
                step,
                crate::tool::RecordedInvocation {
                    invocation: &invocation,
                    metadata: &metadata,
                },
                &request.tools,
                self.stock.limits,
                self.io,
            )
            .await?;
            stock_vec_push(
                &mut request.messages,
                ModelMessage {
                    role: ModelRole::Assistant,
                    content: ModelContent::Part(ModelContentPart::ToolCall(
                        crate::model::ToolCall::new(
                            stock_string_copy(&invocation.call_id, self.io)?,
                            stock_string_copy(&invocation.name, self.io)?,
                            invocation.arguments,
                        )
                        .with_metadata(metadata),
                    )),
                },
                self.io,
            )?;
            stock_vec_push(
                &mut results,
                ModelMessage {
                    role: ModelRole::Tool,
                    content: ModelContent::Part(ModelContentPart::ToolResult {
                        call_id: stock_string_copy(&invocation.call_id, self.io)?,
                        name: invocation.name,
                        content: projection,
                    }),
                },
                self.io,
            )?;
            if through_call.is_some_and(|id| invocation.call_id == id) {
                break;
            }
        }
        for result in results {
            stock_vec_push(&mut request.messages, result, self.io)?;
        }
        let prepared =
            crate::model::PreparedModelRequest::prepare(request, self.stock.limits, self.io)?;
        self.verify_model_request_content(journal, &prepared)
            .await?;
        Ok(prepared)
    }

    fn verify_prefix_identity(&self, records: &[ExecutionRecord], input: &TurnInput) -> Result<()> {
        let Some(ExecutionRecord {
            sequence: 1,
            event: ExecutionEvent::Started { request_digest },
            ..
        }) = records.first()
        else {
            return Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Conflict,
                "tool prefix has another execution identity",
            ));
        };
        if *request_digest != self.request_digest(input)? {
            return Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Conflict,
                "tool prefix has another execution identity",
            ));
        }
        Ok(())
    }
}
