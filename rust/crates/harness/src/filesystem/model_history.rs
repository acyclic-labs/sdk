//! Publish the exact completed model exchange into ref-only conversation storage.
use super::*;
use crate::{
    executor::{ExecutionRecord, load_json},
    model::{ModelContent, ModelContentPart, ModelEvent, ModelMessage, ModelRequest, ModelRole},
    model_input::CompletedModelBoundary,
    tool::{ToolInvocation, ToolResult},
};

struct HistoryMessage {
    publication: OperationId,
    kind: MessageKind,
    content: FileRef,
    attachments: ReferencedAttachments,
    reply_to: Option<Uuid>,
    call_id: Option<String>,
}

struct HistoryBatch {
    step: u32,
    messages: Vec<ModelMessage>,
    tools: Vec<crate::tool::ToolDefinition>,
}

impl HistoryMessage {
    fn id(&self) -> Uuid {
        Uuid::from_bytes(self.publication.into_bytes())
    }
}

impl<P, A, O> HarnessStorage<P, A, O>
where
    P: acyclic_stream::StreamProvider + Send + Sync + 'static,
    A: acyclic_fs::AsyncAuthorityStore + Send + Sync + 'static,
    O: acyclic_fs::AsyncObjectStore + Send + Sync + 'static,
{
    /// Binds the parent's pinned model and byte-exact prefix to a child's
    /// private storage. Tool registration and execution grants remain explicit;
    /// inherited transcript content does not expand this storage owner's scope.
    pub fn inherited_builder(
        &self,
        boundary: CompletedModelBoundary,
        suffix: Vec<ModelMessage>,
        provider: Arc<dyn ModelProvider>,
        limits: Limits,
    ) -> Result<crate::bundle::HarnessBuilder> {
        let model = boundary.request.model.clone();
        provider.admit_model(&model)?;
        let context =
            crate::model_input::InheritedModelContext::new(boundary.clone(), suffix, limits)?;
        let guarded = Arc::new(crate::model_input::PrefixBoundModelProvider::new(
            boundary.prefix,
            limits,
            provider,
        )?);
        Ok(self
            .builder()
            .model(model, guarded)
            .grant("model:generate")
            .context(crate::context::ContextPipeline::default().with(Arc::new(context)))
            .limits(limits))
    }

    /// Publishes all complete exchanges through a pinned model step before fork
    /// preparation. No unfinished effect becomes a conversation result.
    /// Later authoritative messages cause a stale-boundary refusal.
    pub async fn completed_conversation(
        &self,
        operation: OperationId,
        step: u32,
        limits: Limits,
    ) -> Result<StreamAggregate<P>> {
        self.completed_model_boundary(operation, step, limits)
            .await?
            .ok_or_else(|| Error::Conflict("model batch is not complete".into()))?;
        let mut aggregate = self.open_conversation(limits).await?;
        let selection = aggregate
            .reducer()
            .context_selection_for_operation(operation)
            .ok_or_else(|| Error::Conflict("batch has no authoritative selection".into()))?;
        let user = *selection
            .message_ids
            .last()
            .ok_or_else(|| Error::Storage("batch selection has no user".into()))?;
        let records = self.journal.replay(operation).await?;
        let batches = self.completed_batches(&records, Some(step), limits).await?;
        Self::validate_history_tail(
            &aggregate,
            user,
            &Self::history_ids(operation, &batches),
            true,
        )?;
        let messages = self
            .materialize_history(operation, user, &records, batches)
            .await?;
        for message in messages {
            self.publish_history_message(&mut aggregate, message)
                .await?;
        }
        Ok(aggregate)
    }

    pub(super) async fn append_tool_history(
        &self,
        aggregate: &mut StreamAggregate<P>,
        operation: OperationId,
        user: Uuid,
        limits: Limits,
    ) -> Result<()> {
        let records = self.journal.replay(operation).await?;
        let batches = self.completed_batches(&records, None, limits).await?;
        Self::validate_history_tail(
            aggregate,
            user,
            &Self::history_ids(operation, &batches),
            false,
        )?;
        let messages = self
            .materialize_history(operation, user, &records, batches)
            .await?;
        for message in messages {
            self.publish_history_message(aggregate, message).await?;
        }
        Ok(())
    }

    fn validate_history_tail(
        aggregate: &StreamAggregate<P>,
        user: Uuid,
        messages: &[Uuid],
        exact_boundary: bool,
    ) -> Result<()> {
        let state = aggregate
            .reducer()
            .conversation()
            .ok_or_else(|| Error::Storage("conversation projection is missing".into()))?;
        let start = state
            .messages
            .iter()
            .position(|message| message.id == user)
            .ok_or_else(|| Error::Conflict("batch user is absent from conversation".into()))?
            + 1;
        let tail = state
            .messages
            .get(start..)
            .ok_or_else(|| Error::Storage("history boundary exceeds conversation".into()))?;
        if tail
            .iter()
            .zip(messages)
            .any(|(actual, expected)| actual.id != *expected)
            || (exact_boundary && tail.len() > messages.len())
        {
            return Err(Error::Conflict(
                "completed batch has a stale conversation boundary".into(),
            ));
        }
        Ok(())
    }

    async fn completed_batches(
        &self,
        records: &[ExecutionRecord],
        through: Option<u32>,
        limits: Limits,
    ) -> Result<Vec<HistoryBatch>> {
        let mut history = Vec::new();
        let mut seen = std::collections::BTreeSet::new();
        for record in records {
            let ExecutionEvent::ToolBatchCompleted { step, boundary } = &record.event else {
                continue;
            };
            if through.is_some_and(|end| *step > end) {
                continue;
            }
            if !seen.insert(*step) {
                return Err(Error::Storage("duplicate completed batch".into()));
            }
            let boundary: CompletedModelBoundary =
                load_json(self.journal.as_ref(), boundary).await?;
            boundary.verify(limits)?;
            let original = records
                .iter()
                .find_map(|record| match &record.event {
                    ExecutionEvent::ModelInputPrepared {
                        step: candidate,
                        request,
                        ..
                    } if candidate == step => Some(request),
                    _ => None,
                })
                .ok_or_else(|| Error::Storage("completed batch lacks original request".into()))?;
            let original: ModelRequest = load_json(self.journal.as_ref(), original).await?;
            let mut prefix = boundary.request.clone();
            prefix.messages.truncate(original.messages.len());
            if prefix != original {
                return Err(Error::Conflict(
                    "completed batch changed its original request".into(),
                ));
            }
            let suffix = boundary
                .request
                .messages
                .get(original.messages.len()..)
                .ok_or_else(|| Error::Storage("completed batch lacks request prefix".into()))?;
            history.push(HistoryBatch {
                step: *step,
                messages: suffix.to_vec(),
                tools: original.tools,
            });
        }
        Ok(history)
    }

    fn history_publication(operation: OperationId, step: u32, position: usize) -> OperationId {
        derived_operation_id(
            operation,
            format!("conversation-batch:{step}:{position}").as_bytes(),
        )
    }

    fn history_ids(operation: OperationId, batches: &[HistoryBatch]) -> Vec<Uuid> {
        batches
            .iter()
            .flat_map(|batch| {
                (0..batch.messages.len()).map(move |position| {
                    Uuid::from_bytes(
                        Self::history_publication(operation, batch.step, position).into_bytes(),
                    )
                })
            })
            .collect()
    }

    async fn materialize_history(
        &self,
        operation: OperationId,
        user: Uuid,
        records: &[ExecutionRecord],
        batches: Vec<HistoryBatch>,
    ) -> Result<Vec<HistoryMessage>> {
        let mut history = Vec::new();
        for batch in batches {
            let mut calls = BTreeMap::new();
            for (position, message) in batch.messages.iter().enumerate() {
                let publication = Self::history_publication(operation, batch.step, position);
                let path = format!("turns/{operation}/batches/{}/{position}", batch.step);
                history.push(
                    self.history_message(
                        operation,
                        batch.step,
                        user,
                        publication,
                        &path,
                        message,
                        records,
                        &batch.tools,
                        &mut calls,
                    )
                    .await?,
                );
            }
        }
        Ok(history)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "exact batch and message identities remain explicit"
    )]
    async fn history_message(
        &self,
        operation: OperationId,
        step: u32,
        user: Uuid,
        publication: OperationId,
        path: &str,
        message: &ModelMessage,
        records: &[ExecutionRecord],
        tools: &[crate::tool::ToolDefinition],
        calls: &mut BTreeMap<String, (Uuid, crate::tool::ToolInvocation)>,
    ) -> Result<HistoryMessage> {
        let id = Uuid::from_bytes(publication.into_bytes());
        let (kind, content, attachments, reply_to, call_id) =
            match (&message.role, &message.content) {
                (ModelRole::Assistant, ModelContent::Text(text)) => (
                    MessageKind::Assistant,
                    self.stage_model_text(operation, &format!("{path}.json"), text)
                        .await?,
                    ReferencedAttachments::Inline { items: Vec::new() },
                    Some(user),
                    None,
                ),
                (
                    ModelRole::Assistant,
                    ModelContent::Part(ModelContentPart::ToolCall {
                        call_id,
                        name,
                        arguments,
                    }),
                ) => {
                    let invocation = ToolInvocation::for_model_call(
                        operation,
                        step,
                        call_id.clone(),
                        name.clone(),
                        arguments.clone(),
                    );
                    if calls
                        .insert(call_id.clone(), (id, invocation.clone()))
                        .is_some()
                    {
                        return Err(Error::Storage("duplicate completed call identity".into()));
                    }
                    (
                        MessageKind::ToolCall,
                        self.stage_history_json(operation, &format!("{path}.json"), &invocation)
                            .await?,
                        ReferencedAttachments::Inline { items: Vec::new() },
                        Some(user),
                        Some(call_id.clone()),
                    )
                }
                (
                    ModelRole::Tool,
                    ModelContent::Part(ModelContentPart::ToolResult {
                        call_id,
                        name,
                        value,
                    }),
                ) => {
                    let (call, invocation) = calls
                        .get(call_id)
                        .ok_or_else(|| Error::Storage("completed result lacks call".into()))?;
                    if invocation.name != *name {
                        return Err(Error::Conflict("completed result changed tool".into()));
                    }
                    let (result, projection) = self
                        .history_result(operation, step, invocation, path, value, tools, records)
                        .await?;
                    (
                        MessageKind::ToolResult,
                        result,
                        ReferencedAttachments::Inline {
                            items: vec![Attachment {
                                file: projection,
                                label: Some("model_projection".into()),
                            }],
                        },
                        Some(*call),
                        Some(call_id.clone()),
                    )
                }
                _ => {
                    return Err(Error::Storage(
                        "unexpected content in completed tool batch".into(),
                    ));
                }
            };
        Ok(HistoryMessage {
            publication,
            kind,
            content,
            attachments,
            reply_to,
            call_id,
        })
    }

    pub(super) async fn stage_final_assistant(
        &self,
        operation: OperationId,
        output: &TurnOutput,
    ) -> Result<FileRef> {
        let text = self.final_model_text(operation, &output.text).await?;
        if output.attachments.is_empty() {
            self.stage_model_text(
                operation,
                &format!("turns/{operation}/assistant.json"),
                &text,
            )
            .await
        } else {
            self.stage(
                operation,
                &format!("turns/{operation}/assistant.txt"),
                text.as_bytes(),
                "text/plain",
                "assistant.txt",
            )
            .await
        }
    }

    pub(super) async fn stage_model_text(
        &self,
        operation: OperationId,
        path: &str,
        text: &str,
    ) -> Result<FileRef> {
        let bytes = crate::contract::canonical_json_bytes(&text)?;
        self.stage(
            operation,
            path,
            &bytes,
            crate::model::MODEL_TEXT_MEDIA_TYPE,
            "assistant.json",
        )
        .await
    }

    async fn stage_history_json<T: serde::Serialize>(
        &self,
        operation: OperationId,
        path: &str,
        value: &T,
    ) -> Result<FileRef> {
        let bytes = crate::contract::canonical_json_bytes(value)?;
        self.stage(operation, path, &bytes, "application/json", "exchange.json")
            .await
    }

    async fn history_result(
        &self,
        operation: OperationId,
        step: u32,
        invocation: &ToolInvocation,
        path: &str,
        value: &Value,
        tools: &[crate::tool::ToolDefinition],
        records: &[ExecutionRecord],
    ) -> Result<(FileRef, FileRef)> {
        let mut completed = None;
        for record in records {
            let ExecutionEvent::ToolCompleted {
                step: candidate,
                call_id: call,
                result,
                projection,
                invocation_digest,
                ..
            } = &record.event
            else {
                continue;
            };
            if *candidate != step || call != &invocation.call_id {
                continue;
            }
            if completed.is_some() {
                return Err(Error::Storage("duplicate completed tool result".into()));
            }
            completed = Some((result, projection, invocation_digest));
        }
        if let Some((result, projection, invocation_digest)) = completed {
            let definition = tools
                .iter()
                .find(|definition| definition.name == invocation.name)
                .ok_or_else(|| {
                    Error::Storage("completed result lacks pinned tool definition".into())
                })?;
            if *invocation_digest != crate::contract::canonical_json_digest(invocation)? {
                return Err(Error::Conflict(
                    "completed result is bound to another invocation".into(),
                ));
            }
            let actual_result: crate::tool::ToolResult =
                load_json(self.journal.as_ref(), result).await?;
            crate::tool::validate_value(
                &definition.output_schema,
                &actual_result.value,
                "durable tool result",
            )?;
            let actual: Value = load_json(self.journal.as_ref(), projection).await?;
            if &actual != value {
                return Err(Error::Conflict(
                    "completed result changed its projection".into(),
                ));
            }
            crate::tool::validate_value(
                &definition.model_output_schema,
                &actual,
                "durable tool projection",
            )?;
            // Execution records remain host-only. Authoritative conversation
            // artifacts contain only the validated model projection; retaining
            // the raw result here would disclose private executor output.
            return Ok((
                self.stage_history_json(
                    operation,
                    &format!("{path}.json"),
                    &ToolResult {
                        value: actual.clone(),
                    },
                )
                .await?,
                self.stage_history_json(operation, &format!("{path}-projection.json"), &actual)
                    .await?,
            ));
        }
        let mut rejection = None;
        for record in records {
            let ExecutionEvent::ToolAdmissionRejected {
                step: candidate,
                invocation: rejected,
                reason: crate::executor::ToolRejectionKind::InvalidArguments,
                feedback,
            } = &record.event
            else {
                continue;
            };
            if *candidate != step {
                continue;
            }
            let candidate_invocation: ToolInvocation =
                load_json(self.journal.as_ref(), rejected).await?;
            if candidate_invocation.call_id != invocation.call_id {
                continue;
            }
            if candidate_invocation != *invocation {
                return Err(Error::Conflict(
                    "rejection record changed its model invocation".into(),
                ));
            }
            if rejection.is_some() {
                return Err(Error::Storage(
                    "duplicate invalid-argument rejection for model call".into(),
                ));
            }
            rejection = Some(feedback.clone());
        }
        let Some(feedback) = rejection else {
            return Err(Error::Storage(
                "completed result lacks durable admission outcome".into(),
            ));
        };
        let feedback = feedback.as_ref().ok_or_else(|| {
            Error::Storage("invalid-argument rejection lacks durable feedback".into())
        })?;
        let durable: crate::tool::ToolRejectionFeedback =
            load_json(self.journal.as_ref(), feedback).await?;
        let definition = tools
            .iter()
            .find(|definition| definition.name == invocation.name)
            .ok_or_else(|| Error::Storage("rejection lacks pinned tool definition".into()))?;
        let expected = crate::tool::ToolRejectionFeedback::invalid_arguments(
            invocation,
            &definition.input_schema,
            value
                .get("error")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::Invalid("rejection error is missing".into()))?,
        )?;
        if durable != expected {
            return Err(Error::Conflict(
                "rejection feedback is not bound to the durable invocation".into(),
            ));
        }
        let Some(model_feedback) = crate::tool::ToolRejectionFeedback::from_model_value(value)?
        else {
            return Err(Error::Conflict(
                "model rejection result lacks durable feedback envelope".into(),
            ));
        };
        if model_feedback != durable {
            return Err(Error::Conflict(
                "model rejection feedback differs from durable feedback".into(),
            ));
        }
        // Rejection envelopes have their own authenticated contract. They
        // carry bounded diagnostic text and durable call/schema/error
        // digests, so they are intentionally validated through
        // `ToolRejectionFeedback` above rather than the successful result's
        // model projection schema.
        Ok((
            self.stage_history_json(
                operation,
                &format!("{path}.json"),
                &ToolResult {
                    value: value.clone(),
                },
            )
            .await?,
            self.stage_history_json(operation, &format!("{path}-projection.json"), value)
                .await?,
        ))
    }

    async fn publish_history_message(
        &self,
        aggregate: &mut StreamAggregate<P>,
        item: HistoryMessage,
    ) -> Result<()> {
        let id = item.id();
        let state = aggregate
            .reducer()
            .conversation()
            .ok_or_else(|| Error::Storage("conversation projection is missing".into()))?;
        let message = ConversationMessage {
            id,
            sequence: state.messages.len() as u64 + 1,
            kind: item.kind,
            content: item.content,
            attachments: item.attachments,
            reply_to: item.reply_to,
            tool_call_id: item.call_id,
            extensions: BTreeMap::new(),
        };
        if let Some(existing) = state.messages.iter().find(|message| message.id == id) {
            let mut expected = message;
            expected.sequence = existing.sequence;
            if existing != &expected {
                return Err(Error::Conflict(
                    "batch publication changed an existing message".into(),
                ));
            }
            return Ok(());
        }
        self.append_conversation(
            aggregate,
            item.publication,
            "batch-message",
            Action::AppendConversationMessage {
                message: Box::new(message),
            },
        )
        .await
    }

    pub(super) async fn final_model_text(
        &self,
        operation: OperationId,
        fallback: &str,
    ) -> Result<String> {
        let records = self.journal.replay(operation).await?;
        let Some(last) = records
            .iter()
            .filter_map(|record| match record.event {
                ExecutionEvent::ModelInputPrepared { step, .. } => Some(step),
                _ => None,
            })
            .max()
        else {
            return Ok(fallback.to_owned());
        };
        let mut text = String::new();
        for record in records {
            if let ExecutionEvent::Model { step, event } = record.event
                && step == last
                && let ModelEvent::Content { delta } =
                    load_json(self.journal.as_ref(), &event).await?
            {
                text.push_str(&delta);
            }
        }
        Ok(text)
    }
}
