//! Real canonical history for the compaction cancellation scenario.
use super::*;
use crate::{
    conversation::{
        ConversationMessage, MessageKind, ModelContextSelection, ReferencedAttachments,
    },
    core::{Action, Command, SchemaRegistry},
    filesystem::FilesystemContentVerifier,
    projection::SelectedModelContext,
    store::StreamAggregate,
};

const OPERATION: OperationId = OperationId::from_bytes([13; 16]);
const ADMISSION: OperationId = OperationId::from_bytes([14; 16]);

pub(super) async fn initialize(harness: &Harness) -> Result<SelectedModelContext> {
    let reader = FilesystemContentVerifier::new(
        harness.files.clone(),
        &harness.verifier,
        &harness.signed,
        harness.limits.file_bytes,
        None,
    )?;
    let publisher = FilesystemContentPublisher::new(
        harness.files.clone(),
        &harness.scratch,
        &harness.verifier,
        &harness.signed,
        harness.limits.file_bytes,
        None,
    )?;
    let mut aggregate = StreamAggregate::open(
        &harness.runtime.task_host().workflow_stream(),
        (
            harness.verifier.audience().clone(),
            harness.verifier.clone(),
            SchemaRegistry::new(),
        ),
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?
    .with_content_verifier(reader.clone());
    append(
        &mut aggregate,
        harness,
        OperationId::new(),
        Action::BindConversation {
            agent: harness
                .signed
                .agent()
                .ok_or_else(|| Error::Invalid("agent".into()))?,
        },
    )
    .await?;
    let mut ids = Vec::new();
    for (index, (kind, text)) in [
        (MessageKind::User, "old-user"),
        (MessageKind::Assistant, "old-assistant"),
        (MessageKind::User, PROMPT),
    ]
    .into_iter()
    .enumerate()
    {
        let file = publisher
            .stage_once(
                OperationId::new(),
                &format!("history/{index}.txt"),
                text.as_bytes(),
                "text/plain",
                "history.txt",
                None,
            )?
            .await?
            .0;
        let id = uuid::Uuid::new_v4();
        append(
            &mut aggregate,
            harness,
            OperationId::new(),
            Action::AppendConversationMessage {
                message: Box::new(ConversationMessage {
                    id,
                    sequence: index as u64 + 1,
                    kind,
                    content: file,
                    attachments: ReferencedAttachments::Inline { items: vec![] },
                    reply_to: (index == 1).then(|| ids[0]),
                    tool_call_id: None,
                    extensions: Default::default(),
                }),
            },
        )
        .await?;
        ids.push(id);
    }
    let selection = ModelContextSelection {
        conversation_revision: 3,
        message_ids: ids,
        checkpoint: None,
    };
    append(
        &mut aggregate,
        harness,
        OPERATION,
        Action::SelectModelContext {
            selection: (selection.clone()).into(),
        },
    )
    .await?;
    crate::projection::select_model_context(
        aggregate
            .reducer()
            .conversation()
            .ok_or_else(|| Error::Invalid("history".into()))?,
        3,
        selection,
        reader.as_ref(),
        harness.limits,
        harness.limits.attachments,
        None,
    )
    .await
}

async fn append(
    aggregate: &mut StreamAggregate<LocalStream>,
    harness: &Harness,
    operation: OperationId,
    action: Action,
) -> Result<()> {
    aggregate
        .execute(
            Command {
                operation_id: operation,
                idempotency_key: crate::IdempotencyKey::new(operation.to_string())?,
                expected_revision: aggregate.reducer().revision(),
                scope: harness.signed.clone(),
                causal_parent: None,
                action,
            },
            None,
            &mut None,
        )
        .await?;
    Ok(())
}

pub(super) async fn run(
    harness: &Harness,
    history: SelectedModelContext,
) -> Result<crate::executor::TurnOutput> {
    let admission = serialized(&harness.admission, async || {
        tracing::trace!(target: "acyclic_harness::read_profile", phase = "manifest:begin");
        let command = crate::filesystem::ModelTaskCommand {
            summary_source: None,
            context_manifest: harness.stage_context_manifest(OPERATION).await?,
            input: history
                .messages
                .last()
                .ok_or_else(|| Error::Invalid("history".into()))?
                .content
                .clone(),
            selected_context: Some(history),
            max_steps: u32::try_from(harness.limits.model_steps)
                .map_err(|_| Error::Invalid("step bound".into()))?,
            max_output_tokens: None,
            mailbox_operation: None,
            selected_output_volume: None,
        };
        tracing::trace!(target: "acyclic_harness::read_profile", phase = "manifest:end");
        let bytes = serde_json::to_vec(&command).map_err(storage)?;
        let publisher = FilesystemContentPublisher::new(
            harness.files.clone(),
            &harness.journal,
            &harness.verifier,
            &harness.signed,
            harness.limits.file_bytes,
            None,
        )?;
        let payload = publisher
            .stage_once(
                OPERATION,
                &format!("inputs/{OPERATION}.json"),
                &bytes,
                "application/json",
                "model input",
                None,
            )?
            .await?
            .0;
        let inputs = harness
            .finish_turn_inputs(OPERATION, payload, publisher.as_ref())
            .await?;
        tracing::trace!(target: "acyclic_harness::read_profile", phase = "inputs:end");
        harness
            .publish_turn(
                super::super::Turn {
                    operation: ADMISSION,
                    root: ADMISSION,
                    automatic_turns: 0,
                    goal_revision: None,
                    receive: inputs.0,
                    model: inputs.1,
                },
                None,
            )
            .await
    })
    .await?;
    tracing::trace!(target: "acyclic_harness::read_profile", phase = "admission:end");
    let output = harness.resume(admitted_task(admission)?).await;
    tracing::trace!(target: "acyclic_harness::read_profile", phase = "execution:end");
    output
}
