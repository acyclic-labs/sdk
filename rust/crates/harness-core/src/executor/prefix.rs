use super::records::RecordedCall;
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
        let mut hook_context = Vec::new();
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
            let (_, call_records) = self
                .tool_records(journal, input.operation_id, step, &invocation.call_id)
                .await?;
            let call = RecordedCall::classify(&call_records).map_err(|reason| {
                stock_boundary(self.io, crate::wire::ErrorCode::Invalid, reason)
            })?;
            #[cfg(all(feature = "hooks", feature = "filesystem", not(target_arch = "wasm32")))]
            let mut invocation = invocation;
            #[cfg(all(feature = "hooks", feature = "filesystem", not(target_arch = "wasm32")))]
            let guard = self
                .tool_hook_output(
                    input.operation_id,
                    crate::hooks::HookSite::BeforeTool,
                    &invocation,
                    None,
                    true,
                )
                .await?;
            #[cfg(all(feature = "hooks", feature = "filesystem", not(target_arch = "wasm32")))]
            let mut guard = match guard {
                Some(output) => output,
                None if matches!(call, RecordedCall::Failed { started: None, .. }) => {
                    crate::hooks::HookOutput::default()
                }
                None => return Err(Error::Indeterminate(input.operation_id)),
            };
            #[cfg(all(feature = "hooks", feature = "filesystem", not(target_arch = "wasm32")))]
            let denied = guard.blocks();
            #[cfg(all(feature = "hooks", feature = "filesystem", not(target_arch = "wasm32")))]
            let original_arguments = if denied {
                None
            } else {
                guard
                    .updated_input
                    .take()
                    .map(|arguments| std::mem::replace(&mut invocation.arguments, arguments))
            };
            if let Some(reference) = call.started() {
                let recorded =
                    load_json::<crate::tool::RecordedInvocation>(journal, reference, self.io)
                        .await?;
                if recorded.0.invocation != invocation || recorded.0.metadata != metadata {
                    return Err(stock_boundary(
                        self.io,
                        crate::wire::ErrorCode::Conflict,
                        "tool call identity is bound to another invocation",
                    ));
                }
            }
            #[cfg(all(feature = "hooks", feature = "filesystem", not(target_arch = "wasm32")))]
            let completed = matches!(call, RecordedCall::Completed { .. });
            let projection = match call {
                RecordedCall::Failed { reason, cause, .. } => {
                    tool_failure_content(reason, cause, self.io)?
                }
                _ => {
                    completed_tool_projection(
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
                    .await?
                }
            };
            #[cfg(all(feature = "hooks", feature = "filesystem", not(target_arch = "wasm32")))]
            let feedback = if !completed {
                crate::hooks::HookOutput::default()
            } else {
                self.tool_hook(
                    input.operation_id,
                    crate::hooks::HookSite::AfterTool,
                    &invocation,
                    None,
                    true,
                )
                .await?
            };
            #[cfg(all(feature = "hooks", feature = "filesystem", not(target_arch = "wasm32")))]
            if let Some(original) = original_arguments {
                invocation.arguments = original;
            }
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
            #[cfg(all(feature = "hooks", feature = "filesystem", not(target_arch = "wasm32")))]
            {
                // The refusal consumed the explicit guard decision into its
                // tool result. Replay must consume it at the same point.
                let _ = guard.take_block_reason();
                self.append_hook_context(&mut hook_context, guard)?;
                self.append_hook_context(&mut hook_context, feedback)?;
            }
            if through_call.is_some_and(|id| invocation.call_id == id) {
                break;
            }
        }
        for result in results {
            stock_vec_push(&mut request.messages, result, self.io)?;
        }
        super::turn::append_step_context(&mut request.messages, &mut hook_context, self.io)?;
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
