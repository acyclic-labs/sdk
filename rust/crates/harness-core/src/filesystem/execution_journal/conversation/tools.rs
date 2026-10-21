use super::*;

/// One response step's model output and tool settlements, published together once the step
/// is closed. Every call the model emitted is settled by exactly one record: a completion, a
/// failure after its start, or a refusal recorded without a start.
#[derive(Default)]
struct StepExchanges {
    /// Reasoning blocks and calls in model order: record sequence, the call if any, event file.
    assistant: Vec<(u64, Option<crate::model::ToolCall>, FileRef)>,
    /// Start records by call identity.
    started: BTreeMap<String, crate::executor::ExecutionRecord>,
    /// Settlement records by call identity.
    settled: BTreeMap<String, crate::executor::ExecutionRecord>,
}

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
        let mut exchanges = StepExchanges::default();
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
                    && current_step != Some(*step)
                {
                    if let Some(closed) = current_step {
                        let step = std::mem::take(&mut exchanges);
                        self.publish_step(aggregate, operation_id, user_id, limits, closed, step)
                            .await?;
                    }
                    current_step = Some(*step);
                }
                let model = match model {
                    Some(ModelEvent::Completed { metadata }) => {
                        crate::model::accumulate_usage(&mut usage, metadata)?;
                        None
                    }
                    model => model,
                };
                match (record.event, model) {
                    (
                        ExecutionEvent::Model {
                            purpose: crate::executor::ModelPurpose::Response,
                            event,
                            ..
                        },
                        Some(ModelEvent::ReasoningBlock { .. }),
                    ) => exchanges.assistant.push((record.sequence, None, event)),
                    (
                        ExecutionEvent::Model {
                            purpose: crate::executor::ModelPurpose::Response,
                            event,
                            ..
                        },
                        Some(ModelEvent::ToolCall(call)),
                    ) => exchanges
                        .assistant
                        .push((record.sequence, Some(call), event)),
                    (event @ ExecutionEvent::ToolStarted { .. }, _) => {
                        let ExecutionEvent::ToolStarted { call_id, .. } = &event else {
                            unreachable!("matched above")
                        };
                        let call_id = call_id.clone();
                        Self::record_once(
                            &mut exchanges.started,
                            call_id,
                            crate::executor::ExecutionRecord { event, ..record },
                        )?;
                    }
                    (
                        event @ (ExecutionEvent::ToolCompleted { .. }
                        | ExecutionEvent::ToolFailed { .. }),
                        _,
                    ) => {
                        let (ExecutionEvent::ToolCompleted { call_id, .. }
                        | ExecutionEvent::ToolFailed { call_id, .. }) = &event
                        else {
                            unreachable!("matched above")
                        };
                        let call_id = call_id.clone();
                        Self::record_once(
                            &mut exchanges.settled,
                            call_id,
                            crate::executor::ExecutionRecord { event, ..record },
                        )?;
                    }
                    _ => {}
                }
            }
        }
        if let Some(closed) = current_step {
            self.publish_step(aggregate, operation_id, user_id, limits, closed, exchanges)
                .await?;
        }
        Ok(usage)
    }

    fn record_once(
        records: &mut BTreeMap<String, crate::executor::ExecutionRecord>,
        call_id: String,
        record: crate::executor::ExecutionRecord,
    ) -> Result<()> {
        if records.insert(call_id, record).is_some() {
            return Err(Error::Storage(
                "duplicate durable tool call identity".into(),
            ));
        }
        Ok(())
    }

    /// Publishes a closed step: its reasoning and calls in model order, then each call's result
    /// in the same order. Every exchange takes this one path, however it was settled.
    async fn publish_step(
        &self,
        aggregate: &mut StreamAggregate<P>,
        operation_id: OperationId,
        user_id: Uuid,
        limits: Limits,
        step: u32,
        mut exchanges: StepExchanges,
    ) -> Result<()> {
        let mut calls = Vec::new();
        for (sequence, call, content) in exchanges.assistant {
            let Some(call) = call else {
                self.publish_reasoning(aggregate, operation_id, user_id, limits, sequence, content)
                    .await?;
                continue;
            };
            let started = exchanges.started.remove(&call.call_id);
            let (publication, content) = match started {
                Some(crate::executor::ExecutionRecord {
                    sequence,
                    event: ExecutionEvent::ToolStarted { invocation, .. },
                    ..
                }) => (format!("conversation-tool:{sequence}"), invocation),
                // A call refused before dispatch has no start: its exact invocation is the
                // model's own call, staged once under its model record.
                _ => {
                    let invocation = crate::tool::RecordedInvocation {
                        invocation: crate::tool::ToolInvocation::for_model_call(
                            operation_id,
                            step,
                            call.call_id.clone(),
                            call.name.clone(),
                            call.arguments.clone(),
                            None,
                        )?,
                        metadata: call.metadata.clone(),
                    };
                    let bytes = crate::contract::canonical_json_bytes(&invocation)?;
                    let path = format!("turns/{operation_id}/tool-calls/{sequence}.json");
                    let file = self
                        .stage_conversation(
                            operation_id,
                            &path,
                            &bytes,
                            "application/json",
                            "invocation.json",
                        )
                        .await?;
                    (format!("conversation-tool-call:{sequence}"), file)
                }
            };
            let id = self
                .publish_tool_message(
                    aggregate,
                    operation_id,
                    limits,
                    &publication,
                    ConversationMessageDraft {
                        kind: MessageKind::ToolCall,
                        content,
                        attachments: Vec::new(),
                        reply_to: user_id,
                        call_id: call.call_id.clone(),
                    },
                )
                .await?;
            calls.push((call, id));
        }
        if !exchanges.started.is_empty() {
            return Err(Error::Storage("tool has no canonical model call".into()));
        }
        for (call, id) in calls {
            let settled = exchanges
                .settled
                .remove(&call.call_id)
                .ok_or_else(|| Error::Storage("canonical tool call has no settlement".into()))?;
            let (content, projection) = match settled.event {
                ExecutionEvent::ToolCompleted {
                    result, projection, ..
                } => (result, projection),
                ExecutionEvent::ToolFailed { reason, cause, .. } => {
                    // The exact model-visible failure the executor returned and replays.
                    let failure = crate::executor::tool_failure_content(reason, &cause, None)?;
                    let bytes = crate::contract::canonical_json_bytes(&failure)?;
                    let path = format!(
                        "turns/{operation_id}/tool-failures/{}.json",
                        settled.sequence
                    );
                    let file = self
                        .stage_conversation(
                            operation_id,
                            &path,
                            &bytes,
                            "application/json",
                            "failure.json",
                        )
                        .await?;
                    (file.clone(), file)
                }
                _ => return Err(Error::Storage("tool settlement kind is invalid".into())),
            };
            self.publish_tool_message(
                aggregate,
                operation_id,
                limits,
                &format!("conversation-tool:{}", settled.sequence),
                ConversationMessageDraft {
                    kind: MessageKind::ToolResult,
                    content,
                    attachments: vec![Attachment {
                        file: projection,
                        label: Some("model_projection".into()),
                    }],
                    reply_to: id,
                    call_id: call.call_id,
                },
            )
            .await?;
        }
        if !exchanges.settled.is_empty() {
            return Err(Error::Storage(
                "tool settlement has no canonical model call".into(),
            ));
        }
        Ok(())
    }

    async fn publish_reasoning(
        &self,
        aggregate: &mut StreamAggregate<P>,
        operation_id: OperationId,
        user_id: Uuid,
        limits: Limits,
        sequence: u64,
        content: FileRef,
    ) -> Result<()> {
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
            return Ok(());
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
        .await
    }

    /// Appends one tool message under its derived identity, or verifies the replayed one.
    async fn publish_tool_message(
        &self,
        aggregate: &mut StreamAggregate<P>,
        operation_id: OperationId,
        limits: Limits,
        publication: &str,
        draft: ConversationMessageDraft,
    ) -> Result<Uuid> {
        let publication = derived_operation_id(operation_id, publication.as_bytes());
        let id = Uuid::from_bytes(publication.into_bytes());
        let attachments = ReferencedAttachments::Inline {
            items: draft.attachments,
        };
        if let Some(existing) = self
            .indexed_conversation_message(aggregate, id, limits)
            .await?
        {
            if existing.kind != draft.kind
                || existing.content != draft.content
                || existing.attachments != attachments
                || existing.reply_to != Some(draft.reply_to)
                || existing.tool_call_id.as_deref() != Some(draft.call_id.as_str())
            {
                return Err(Error::Conflict(
                    "tool publication identity belongs to another message".into(),
                ));
            }
            return Ok(id);
        }
        let message = ConversationMessage {
            id,
            sequence: Self::next_conversation_sequence(aggregate)?,
            kind: draft.kind,
            content: draft.content,
            attachments,
            reply_to: Some(draft.reply_to),
            tool_call_id: Some(draft.call_id),
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
        Ok(id)
    }
}

/// A tool message before its derived identity and sequence are assigned.
struct ConversationMessageDraft {
    kind: MessageKind,
    content: FileRef,
    attachments: Vec<Attachment>,
    reply_to: Uuid,
    call_id: String,
}
