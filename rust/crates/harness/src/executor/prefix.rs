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
        let retained = retained_model_step(
            journal,
            &records,
            step,
            ModelPurpose::Response,
            self.stock.limits,
            self.io,
        )
        .await?;
        let prepared = retained
            .request
            .ok_or(Error::Indeterminate(input.operation_id))?;
        let mut request = crate::model::clone_request(prepared.request(), self.io)?;
        let mut calls = Vec::new();
        for event in retained.events {
            if let ModelEvent::ToolCall {
                call_id,
                name,
                arguments,
            } = event
            {
                let invocation = ToolInvocation::for_model_call(
                    input.operation_id,
                    step,
                    call_id,
                    name,
                    arguments,
                    self.native_work(),
                )?;
                stock_vec_push(&mut calls, invocation, self.io)?;
            }
        }
        if !retained.admission.completed
            || calls.is_empty()
            || through_call.is_some_and(|id| !calls.iter().any(|call| call.call_id == id))
        {
            return Err(Error::Indeterminate(input.operation_id));
        }
        for invocation in calls {
            let projection = completed_tool_projection(
                journal,
                &records,
                step,
                &invocation,
                &request.tools,
                self.stock.limits,
                self.io,
            )
            .await?;
            stock_vec_push(
                &mut request.messages,
                ModelMessage {
                    role: ModelRole::Assistant,
                    content: ModelContent::Part(ModelContentPart::ToolCall {
                        call_id: stock_string_copy(&invocation.call_id, self.io)?,
                        name: stock_string_copy(&invocation.name, self.io)?,
                        arguments: invocation.arguments,
                    }),
                },
                self.io,
            )?;
            stock_vec_push(
                &mut request.messages,
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
        let prepared =
            crate::model::PreparedModelRequest::prepare(request, self.stock.limits, self.io)?;
        self.verify_model_request_content(journal, &prepared)
            .await?;
        Ok(prepared)
    }
}
