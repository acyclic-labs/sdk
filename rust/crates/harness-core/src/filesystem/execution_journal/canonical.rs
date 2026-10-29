use super::*;

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: crate::store::JournalProvider + ?Sized,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Admits a compaction publication. Its projection must stand alone: a
    /// canonical checkpoint names committed conversation history that agent
    /// streams do not keep, so no journal can prove its source.
    pub(super) async fn verify_canonical_publication_with_io(
        &self,
        event: &ExecutionEvent,
        limits: Limits,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        if let Some(io) = io {
            io.check_cancellation()?;
        }
        let ExecutionEvent::ContextCompacted { projection, .. } = event else {
            return Ok(());
        };
        crate::executor::with_history_io(io, |work| limits.validate_file(projection, work))?;
        let projection =
            crate::executor::load_response_projection(self, projection, limits, io).await?;
        crate::context::validate_projected_context(&projection.0.context, limits, io)?;
        if projection.0.checkpoint.is_some() || projection.0.canonical.is_some() {
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "checkpoint source selection is not the committed admission",
            ));
        }
        Ok(())
    }
}

#[cfg(all(test, not(loom), not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::{
        context::{ContextPipeline, ModelContextCapacity, ModelTokenCount},
        executor::{Executor, StockExecutor, TurnInput},
        model::{Model, ModelContent, ModelMessage, ModelRole},
        test_support::{ScriptedModelProvider, ScriptedTurn},
        tool::ToolRegistry,
    };

    #[tokio::test]
    async fn selected_compaction_publishes_only_the_signed_working_cut() -> Result<()> {
        let (journal, _) = super::super::test_support::journal().await?;
        let selected = crate::projection::SelectedModelContext {
            selection: crate::conversation::ModelContextSelection {
                conversation_revision: 3,
                message_ids: (1..=3).map(uuid::Uuid::from_u128).collect(),
                checkpoint: None,
            },
            messages: vec![
                ModelMessage {
                    role: ModelRole::User,
                    content: ModelContent::Text("old user".repeat(4_000)),
                },
                ModelMessage {
                    role: ModelRole::Assistant,
                    content: ModelContent::Text("old answer".repeat(2_800)),
                },
                ModelMessage {
                    role: ModelRole::User,
                    content: ModelContent::Text("current question".into()),
                },
            ],
        };
        let journal = journal.with_bound_selection(selected.clone());
        let provider = Arc::new(
            ScriptedModelProvider::new([
                ScriptedTurn::Text("bounded summary".into()),
                ScriptedTurn::Text("done".into()),
            ])
            .with_context_capacity(ModelContextCapacity {
                context_tokens: 65_536,
                output_tokens: Some(4_096),
            })
            .with_token_counter(|request| {
                let messages = request
                    .request()
                    .messages
                    .iter()
                    .map(|message| {
                        u32::try_from(
                            crate::contract::canonical_json_bytes(&message.content)?.len(),
                        )
                        .map_err(|_| Error::Invalid("message token bound exceeds u32".into()))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(ModelTokenCount::from_upper_bounds(
                    request.request_digest(),
                    100,
                    messages,
                ))
            }),
        );
        let executor = StockExecutor::new(
            Model::new(
                "synthetic".into(),
                "counter".into(),
                "1".into(),
                serde_json::Value::Null,
            )?,
            provider.clone(),
            ContextPipeline::default(),
            ToolRegistry::default(),
        )
        .with_compaction_policy(crate::context::CompactionPolicy::Threshold(
            crate::context::ThresholdCompaction {
                recent_tokens: 1,
                ..Default::default()
            },
        ));
        let input = TurnInput {
            operation_id: OperationId::new(),
            input: selected.messages.last().unwrap().content.clone(),
            selected_context: Some(selected),
            max_steps: 3,
        };
        let output = executor.execute(input.clone(), &journal).await?;
        assert_eq!(output.text, "done");
        assert_eq!(provider.request_count(), 2);
        assert_eq!(executor.execute(input.clone(), &journal).await?, output);
        assert_eq!(
            provider.request_count(),
            2,
            "replay dispatched another model request"
        );

        let records = journal.replay(input.operation_id, 0, 128, None)?.await?;
        let event = records
            .iter()
            .find_map(|record| match &record.event {
                event @ ExecutionEvent::ContextCompacted { .. } => Some(event),
                _ => None,
            })
            .expect("selected history was compacted");
        let ExecutionEvent::ContextCompacted { projection, .. } = event else {
            unreachable!()
        };
        let (mut projected, _) = crate::executor::load_response_projection(
            &journal,
            projection,
            Limits::default(),
            None,
        )
        .await?;
        assert!(projected.canonical.is_none());
        assert!(projected.checkpoint.is_none());
        assert!(projected.working.is_some());

        // A valid working cut cannot authorize a claim about aggregate history.
        projected.canonical = Some(projection.clone());
        let invalid = crate::executor::stage_json(
            &journal,
            input.operation_id,
            "unprovable-canonical",
            &projected,
            None,
        )
        .await?;
        let mut invalid_event = event.clone();
        if let ExecutionEvent::ContextCompacted { projection, .. } = &mut invalid_event {
            *projection = invalid;
        }
        assert!(matches!(
            journal.verify_canonical_publication_with_io(&invalid_event, Limits::default(), None).await,
            Err(Error::Conflict(reason)) if reason == "checkpoint source selection is not the committed admission"
        ));
        Ok(())
    }
}
