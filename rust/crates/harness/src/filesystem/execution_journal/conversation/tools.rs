use super::*;

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn append_turn_history(
        &self,
        aggregate: &mut StreamAggregate<P>,
        operation_id: OperationId,
        user_id: Uuid,
        limits: Limits,
    ) -> Result<Value> {
        let mut usage = Value::Null;
        let mut calls = BTreeMap::new();
        let mut results = Vec::new();
        let mut assistant = std::collections::VecDeque::new();
        let mut current_step = None;
        let mut replay = crate::executor::ExecutionReplay::new(operation_id);
        loop {
            let page = self
                .replay_page(
                    operation_id,
                    replay.tail(),
                    crate::executor::EXECUTION_REPLAY_PAGE_RECORDS,
                    true,
                    None,
                )
                .await?;
            replay.observe_page(page.iter().map(|(record, _)| record), None)?;
            if page.is_empty() {
                break;
            }
            for (record, model) in page {
                if let ExecutionEvent::ModelStarted {
                    step,
                    purpose: crate::executor::ModelPurpose::Response,
                    ..
                } = &record.event
                {
                    if current_step != Some(*step) {
                        self.append_reasoning_prefix(
                            aggregate,
                            operation_id,
                            user_id,
                            limits,
                            &mut assistant,
                            None,
                        )
                        .await?;
                        for result in results.drain(..) {
                            self.append_tool_record(
                                aggregate,
                                operation_id,
                                user_id,
                                limits,
                                &mut calls,
                                result,
                            )
                            .await?;
                        }
                        calls.clear();
                        current_step = Some(*step);
                    }
                }
                match (&record.event, model) {
                    (_, Some(ModelEvent::Completed { metadata })) => {
                        crate::model::accumulate_usage(&mut usage, metadata)?
                    }
                    (
                        ExecutionEvent::Model {
                            purpose: crate::executor::ModelPurpose::Response,
                            event,
                            ..
                        },
                        Some(ModelEvent::ReasoningBlock { .. }),
                    ) => {
                        assistant.push_back((record.sequence, None, event.clone()));
                    }
                    (
                        ExecutionEvent::Model {
                            purpose: crate::executor::ModelPurpose::Response,
                            event,
                            ..
                        },
                        Some(ModelEvent::ToolCall(call)),
                    ) => {
                        assistant.push_back((record.sequence, Some(call.call_id), event.clone()));
                    }
                    _ => {}
                }
                if let ExecutionEvent::ToolStarted { call_id, .. } = &record.event {
                    self.append_reasoning_prefix(
                        aggregate,
                        operation_id,
                        user_id,
                        limits,
                        &mut assistant,
                        Some(call_id),
                    )
                    .await?;
                }
                if matches!(record.event, ExecutionEvent::ToolCompleted { .. }) {
                    results.push(record);
                } else {
                    self.append_tool_record(
                        aggregate,
                        operation_id,
                        user_id,
                        limits,
                        &mut calls,
                        record,
                    )
                    .await?;
                }
            }
        }
        self.append_reasoning_prefix(
            aggregate,
            operation_id,
            user_id,
            limits,
            &mut assistant,
            None,
        )
        .await?;
        for result in results {
            self.append_tool_record(aggregate, operation_id, user_id, limits, &mut calls, result)
                .await?;
        }
        Ok(usage)
    }

    async fn append_reasoning_prefix(
        &self,
        aggregate: &mut StreamAggregate<P>,
        operation_id: OperationId,
        user_id: Uuid,
        limits: Limits,
        assistant: &mut std::collections::VecDeque<(u64, Option<String>, FileRef)>,
        through: Option<&str>,
    ) -> Result<()> {
        while let Some((sequence, call, content)) = assistant.pop_front() {
            if let Some(call) = call {
                return if through == Some(call.as_str()) {
                    Ok(())
                } else {
                    Err(Error::Storage("canonical model/tool order differs".into()))
                };
            }
            let publication = derived_operation_id(
                operation_id,
                format!("conversation-reasoning:{sequence}").as_bytes(),
            );
            let id = Uuid::from_bytes(publication.into_bytes());
            let extensions = BTreeMap::from([("acyclic.model.reasoning".into(), content.clone())]);
            if let Some(existing) = self
                .indexed_conversation_message(aggregate, id, limits)
                .await?
            {
                if existing.kind != MessageKind::Assistant
                    || existing.content != content
                    || existing.extensions != extensions
                    || existing.reply_to != Some(user_id)
                {
                    return Err(Error::Conflict(
                        "reasoning publication changed on replay".into(),
                    ));
                }
                continue;
            }
            self.append_conversation(
                aggregate,
                publication,
                "reasoning",
                Action::AppendConversationMessage {
                    message: Box::new(ConversationMessage {
                        id,
                        sequence: Self::next_conversation_sequence(aggregate)?,
                        kind: MessageKind::Assistant,
                        content,
                        attachments: ReferencedAttachments::Inline { items: Vec::new() },
                        reply_to: Some(user_id),
                        tool_call_id: None,
                        extensions,
                    }),
                },
            )
            .await?;
        }
        if through.is_some() {
            return Err(Error::Storage("tool has no canonical model call".into()));
        }
        Ok(())
    }

    async fn append_tool_record(
        &self,
        aggregate: &mut StreamAggregate<P>,
        operation_id: OperationId,
        user_id: Uuid,
        limits: Limits,
        calls: &mut BTreeMap<(u32, String), Uuid>,
        record: crate::executor::ExecutionRecord,
    ) -> Result<()> {
        let (kind, content, attachments, reply_to, call_id, step) = match record.event {
            ExecutionEvent::ToolStarted {
                step,
                call_id,
                invocation,
            } => (
                MessageKind::ToolCall,
                invocation,
                ReferencedAttachments::Inline { items: Vec::new() },
                Some(user_id),
                call_id,
                step,
            ),
            ExecutionEvent::ToolCompleted {
                step,
                call_id,
                result,
                projection,
            } => {
                let call = calls
                    .get(&(step, call_id.clone()))
                    .copied()
                    .ok_or_else(|| Error::Storage("tool result has no durable start".into()))?;
                (
                    MessageKind::ToolResult,
                    result,
                    ReferencedAttachments::Inline {
                        items: vec![Attachment {
                            file: projection,
                            label: Some("model_projection".into()),
                        }],
                    },
                    Some(call),
                    call_id,
                    step,
                )
            }
            _ => return Ok(()),
        };
        let publication = derived_operation_id(
            operation_id,
            format!("conversation-tool:{}", record.sequence).as_bytes(),
        );
        let id = Uuid::from_bytes(publication.into_bytes());
        if kind == MessageKind::ToolCall && calls.insert((step, call_id.clone()), id).is_some() {
            return Err(Error::Storage(
                "duplicate durable tool call identity".into(),
            ));
        }
        let existing = self
            .indexed_conversation_message(aggregate, id, limits)
            .await?;
        if let Some(existing) = existing {
            if existing.kind != kind
                || existing.content != content
                || existing.attachments != attachments
                || existing.reply_to != reply_to
                || existing.tool_call_id.as_deref() != Some(call_id.as_str())
            {
                return Err(Error::Conflict(
                    "tool publication identity belongs to another message".into(),
                ));
            }
            return Ok(());
        }
        let message = ConversationMessage {
            id,
            sequence: Self::next_conversation_sequence(aggregate)?,
            kind,
            content,
            attachments,
            reply_to,
            tool_call_id: Some(call_id),
            extensions: BTreeMap::new(),
        };
        self.append_conversation(
            aggregate,
            publication,
            "tool",
            Action::AppendConversationMessage {
                message: Box::new(message),
            },
        )
        .await?;
        Ok(())
    }
}
