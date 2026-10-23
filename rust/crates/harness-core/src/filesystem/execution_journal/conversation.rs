//! The canonical conversation loop, shared by memory and durable bindings.

mod tools;

use super::*;
use crate::{
    conversation::{
        Attachment, ConversationMessage, MessageKind, ReferencedAttachments, assistant_message_id,
        derived_operation_id,
    },
    core::{Action, Command},
    executor::{TurnInput, TurnOutput},
    filesystem::InternalContentClass,
};
use serde_json::Value;
use uuid::Uuid;

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    async fn stage_conversation(
        &self,
        operation: OperationId,
        path: &str,
        bytes: &[u8],
        media_type: &str,
        display_name: &str,
    ) -> Result<FileRef> {
        let retry = IdempotencyKey::new(format!(
            "filesystem-upload:{operation}:{}",
            blake3::hash(path.as_bytes()).to_hex()
        ))?;
        self.host
            .put_internal_content(
                &self.volume,
                &ContentGrant::verify(
                    &self.verifier,
                    &self.scope,
                    &self.volume,
                    VolumeOperation::Write,
                    u64::MAX,
                    &mut None,
                )?,
                "turns/.canonical/payload",
                bytes,
                media_type,
                display_name,
                self.maximum_payload_bytes,
                &retry,
                InternalContentClass::Conversation,
                None,
            )
            .await
    }
    /// Runs a canonical conversation turn from already staged primary content and ordered attachments.
    #[allow(
        clippy::too_many_lines,
        reason = "one complete idempotent conversation turn"
    )]
    pub async fn run_conversation(
        &self,
        bundle: &crate::bundle::HarnessBundle,
        operation_id: OperationId,
        content: FileRef,
        attachments: Vec<Attachment>,
        max_steps: u32,
    ) -> Result<TurnOutput> {
        let limits = bundle.limits();
        limits.validate_file(&content, &mut None)?;
        if attachments.len() > limits.attachments {
            return Err(Error::Invalid(
                "turn attachments exceed harness limits".into(),
            ));
        }
        for attachment in &attachments {
            attachment.validate(&mut None)?;
            limits.validate_file(&attachment.file, &mut None)?;
        }
        let referenced = if attachments.len() <= 128 {
            ReferencedAttachments::Inline { items: attachments }
        } else {
            let bytes = crate::conversation::encode_attachment_manifest(&attachments)?;
            let manifest = self
                .host
                .put_internal_content(
                    &self.volume,
                    &ContentGrant::verify(
                        &self.verifier,
                        &self.scope,
                        &self.volume,
                        VolumeOperation::Write,
                        u64::MAX,
                        &mut None,
                    )?,
                    "turns/.canonical/payload",
                    &bytes,
                    "application/vnd.acyclic.harness.attachments+json",
                    "attachments.json",
                    self.maximum_payload_bytes,
                    &IdempotencyKey::new(format!("conversation:{operation_id}:attachments"))?,
                    InternalContentClass::Conversation,
                    None,
                )
                .await?;
            ReferencedAttachments::Manifest {
                manifest,
                item_count: u32::try_from(attachments.len())
                    .map_err(|_| Error::Invalid("attachment count exceeds limit".into()))?,
            }
        };
        let user_operation = derived_operation_id(operation_id, b"conversation-user");
        let mut aggregate = self.conversation_projection(limits).await?;
        let state = aggregate
            .reducer()
            .conversation()
            .ok_or_else(|| Error::Storage("conversation projection is missing".into()))?;
        let existing_selection = aggregate
            .context_selection_for_operation(operation_id)
            .await?;
        let retry_view = if let Some(selection) = &existing_selection {
            if selection
                .message_ids
                .iter()
                .any(|id| state.message(*id).is_none())
            {
                let reader = aggregate.history_reader()?;
                let cursor = crate::store::HistoryCursor {
                    authority: aggregate.reducer().authority().clone(),
                    after_revision: 0,
                    through_revision: aggregate.reducer().revision(),
                };
                Some(
                    reader
                        .selected_conversation(
                            &cursor,
                            selection,
                            self.context_history_limits(limits)?,
                        )
                        .await?,
                )
            } else {
                None
            }
        } else {
            None
        };
        let checkpoint_reference = aggregate.reducer().latest_context_checkpoint().cloned();
        let checkpoint = if existing_selection.is_none() {
            match &checkpoint_reference {
                Some(reference) => Some(
                    crate::executor::load_canonical_checkpoint(self, reference, limits, None, None)
                        .await?
                        .0,
                ),
                None => None,
            }
        } else {
            None
        };
        let preparation = crate::turn::prepare_turn(
            retry_view.as_ref().unwrap_or(state),
            operation_id,
            content,
            referenced,
            limits,
            existing_selection,
            false,
            true,
            None,
            checkpoint.as_ref().zip(checkpoint_reference),
        )?;
        let user_id = preparation.user_id;
        if let Some(message) = preparation.user_message.clone() {
            self.append_conversation(
                &mut aggregate,
                user_operation,
                "user",
                Action::AppendConversationMessage {
                    message: Box::new(message),
                },
            )
            .await?;
        }
        if preparation.selection_is_new {
            self.append_conversation(
                &mut aggregate,
                operation_id,
                "context",
                Action::SelectModelContext {
                    selection: (preparation.selection.clone()).into(),
                },
            )
            .await?;
        }
        let selection = preparation.selection;
        let historical = aggregate
            .reducer()
            .conversation()
            .ok_or_else(|| Error::Storage("conversation projection is missing".into()))?;
        let historical = retry_view.as_ref().unwrap_or(historical);
        if selection.conversation_revision > historical.logical_revision() {
            return Err(Error::Storage(
                "selection revision exceeds conversation history".into(),
            ));
        }
        if selection.message_ids.last() != Some(&user_id) {
            return Err(Error::Conflict(
                "turn identity is bound to another context selection".into(),
            ));
        }
        let selected = select_model_context(
            historical,
            selection.conversation_revision,
            selection,
            self.input_verifier.as_deref().ok_or_else(|| {
                Error::Unsupported("canonical conversation reader is not bound".into())
            })?,
            limits,
            limits.attachments,
            None,
        )
        .await?;
        drop(aggregate);
        let output = bundle
            .run(TurnInput::from_selected_context(
                operation_id,
                selected,
                max_steps,
            )?)
            .await?;
        self.publish_canonical_checkpoint(operation_id, limits)
            .await?;
        self.append_assistant(operation_id, user_id, &output, limits)
            .await?;
        self.conversation_projection(limits)
            .await?
            .compact_conversation_projection()?;
        Ok(output)
    }
    async fn publish_canonical_checkpoint(
        &self,
        operation_id: OperationId,
        limits: Limits,
    ) -> Result<()> {
        let Some(reference) =
            crate::executor::canonical_checkpoint_for_operation(self, operation_id, limits, None)
                .await?
        else {
            return Ok(());
        };
        let (envelope, _) =
            crate::executor::load_canonical_checkpoint(self, &reference, limits, None, None)
                .await?;
        let mut selection = envelope.selection;
        selection.checkpoint = Some(reference);
        let mut aggregate = self.conversation_projection(limits).await?;
        let publication = derived_operation_id(operation_id, b"conversation-checkpoint");
        if let Some(committed) = aggregate
            .context_selection_for_operation(publication)
            .await?
        {
            return if committed == selection {
                Ok(())
            } else {
                Err(Error::Conflict(
                    "canonical checkpoint publication changed on retry".into(),
                ))
            };
        }
        self.append_conversation(
            &mut aggregate,
            publication,
            "checkpoint",
            Action::SelectModelContext {
                selection: (selection).into(),
            },
        )
        .await
    }
    async fn append_conversation(
        &self,
        aggregate: &mut StreamAggregate<P>,
        operation_id: OperationId,
        label: &str,
        action: Action,
    ) -> Result<()> {
        aggregate
            .execute(
                Command {
                    operation_id,
                    idempotency_key: IdempotencyKey::new(format!(
                        "conversation:{operation_id}:{label}"
                    ))?,
                    expected_revision: aggregate.reducer().revision(),
                    scope: self.scope.clone(),
                    causal_parent: None,
                    action,
                },
                #[cfg(feature = "filesystem")]
                None,
                &mut None,
            )
            .await?;
        Ok(())
    }

    async fn indexed_conversation_message(
        &self,
        aggregate: &StreamAggregate<P>,
        id: Uuid,
        limits: Limits,
    ) -> Result<Option<ConversationMessage>> {
        let state = aggregate
            .reducer()
            .conversation()
            .ok_or_else(|| Error::Storage("conversation projection is missing".into()))?;
        if let Some(message) = state.message(id) {
            return Ok(Some(message.clone()));
        }
        let cursor = crate::store::HistoryCursor {
            authority: aggregate.reducer().authority().clone(),
            after_revision: 0,
            through_revision: aggregate.reducer().revision(),
        };
        aggregate
            .history_reader()?
            .conversation_message(
                &cursor,
                id,
                self.context_history_limits(limits)?.maximum_bytes,
            )
            .await
    }

    fn next_conversation_sequence(aggregate: &StreamAggregate<P>) -> Result<u64> {
        aggregate
            .reducer()
            .conversation()
            .ok_or_else(|| Error::Storage("conversation projection is missing".into()))?
            .logical_revision()
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("conversation sequence exhausted".into()))
    }

    async fn append_assistant(
        &self,
        operation_id: OperationId,
        user_id: Uuid,
        output: &TurnOutput,
        limits: Limits,
    ) -> Result<()> {
        if output.attachments.len() > limits.attachments {
            return Err(Error::Invalid(
                "assistant attachments exceed harness limits".into(),
            ));
        }
        for attachment in &output.attachments {
            attachment.validate(&mut None)?;
            limits.validate_file(&attachment.file, &mut None)?;
        }
        let assistant_id = assistant_message_id(operation_id);
        let assistant_operation = OperationId::from_bytes(*assistant_id.as_bytes());
        let content = self
            .stage_conversation(
                operation_id,
                &format!("turns/{operation_id}/assistant.txt"),
                output.text.as_bytes(),
                "text/plain",
                "assistant.txt",
            )
            .await?;
        let attachments = if output.attachments.len() <= 128 {
            ReferencedAttachments::Inline {
                items: output.attachments.clone(),
            }
        } else {
            let bytes = crate::conversation::encode_attachment_manifest(&output.attachments)?;
            let manifest = self
                .host
                .put_internal_content(
                    &self.volume,
                    &ContentGrant::verify(
                        &self.verifier,
                        &self.scope,
                        &self.volume,
                        VolumeOperation::Write,
                        u64::MAX,
                        &mut None,
                    )?,
                    "turns/.canonical/payload",
                    &bytes,
                    "application/vnd.acyclic.harness.attachments+json",
                    "attachments.json",
                    self.maximum_payload_bytes,
                    &IdempotencyKey::new(format!(
                        "conversation:{operation_id}:assistant-attachments"
                    ))?,
                    InternalContentClass::Conversation,
                    None,
                )
                .await?;
            ReferencedAttachments::Manifest {
                manifest,
                item_count: u32::try_from(output.attachments.len())
                    .map_err(|_| Error::Invalid("attachment count exceeds limit".into()))?,
            }
        };
        let mut aggregate = self.conversation_projection(limits).await?;
        let usage = self
            .append_turn_history(&mut aggregate, operation_id, user_id, limits)
            .await?;
        let metadata =
            serde_json::to_vec(&usage).map_err(|error| Error::Invalid(error.to_string()))?;
        let metadata = self
            .stage_conversation(
                operation_id,
                &format!("turns/{operation_id}/metadata.json"),
                &metadata,
                "application/json",
                "metadata.json",
            )
            .await?;
        let mut extensions = BTreeMap::new();
        extensions.insert("acyclic.model.metadata".to_owned(), metadata);
        let existing = self
            .indexed_conversation_message(&aggregate, assistant_id, limits)
            .await?;
        if let Some(existing) = existing {
            if existing.content != content
                || existing.attachments != attachments
                || existing.extensions != extensions
                || existing.reply_to != Some(user_id)
                || existing.kind != MessageKind::Assistant
            {
                return Err(Error::Conflict(
                    "turn identity belongs to another assistant message".into(),
                ));
            }
            return Ok(());
        }
        let message = ConversationMessage {
            id: assistant_id,
            sequence: Self::next_conversation_sequence(&aggregate)?,
            kind: MessageKind::Assistant,
            content,
            attachments,
            reply_to: Some(user_id),
            tool_call_id: None,
            extensions,
        };
        self.append_conversation(
            &mut aggregate,
            assistant_operation,
            "assistant",
            Action::AppendConversationMessage {
                message: Box::new(message),
            },
        )
        .await
    }
}

#[cfg(test)]
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
mod tests;
