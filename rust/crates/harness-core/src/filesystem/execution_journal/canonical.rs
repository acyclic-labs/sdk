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
        runtime::RuntimeScope,
        test_support::{ScriptedModelProvider, ScriptedTurn},
        tool::ToolRegistry,
    };

    #[tokio::test]
    async fn task_summary_uses_response_publication_and_replays_without_dispatch() -> Result<()> {
        let (journal, _issuer) = super::super::test_support::journal().await?;
        let private = crate::runtime::test_context::private_volume(7)?;
        let scope = RuntimeScope::new(
            crate::Capabilities::new([
                "model:generate".to_owned(),
                private.capability(VolumeOperation::Write, &mut None)?,
            ]),
            Limits::default(),
        )?;
        let context = crate::runtime::test_context::agent_context(
            scope.clone(),
            Some(crate::runtime::ContentBindings {
                volume: private.clone(),
                reader: Arc::new(crate::runtime::test_context::NoReads),
                // Admission retains the original task writer. Actual publication
                // must use the real journal; this writer cannot publish bytes.
                writer: Some(Arc::new(crate::runtime::test_context::PrivateVolume(
                    private,
                ))),
            }),
        )?;
        let operation = OperationId::new();
        let provider = Arc::new(
            ScriptedModelProvider::new([
                ScriptedTurn::Text("portable summary".into()),
                ScriptedTurn::Text("response".into()),
            ])
            .with_context_capacity(ModelContextCapacity {
                context_tokens: 4_096,
                output_tokens: Some(64),
            })
            .with_token_counter(|request| {
                Ok(ModelTokenCount::from_upper_bounds(
                    request.request_digest(),
                    8,
                    vec![8; request.request().messages.len()],
                ))
            }),
        );
        let executor = StockExecutor::new(
            Model::new("test".into(), "summary".into(), "1".into())?,
            provider.clone(),
            ContextPipeline::default(),
            ToolRegistry::default(),
        )
        .with_max_output_tokens(64)?
        .with_compaction_policy(crate::context::CompactionPolicy::Disabled)
        .with_tool_authority(
            scope,
            Arc::new(crate::test_support::FixedToolPolicy::new(
                crate::runtime::ToolPolicyDecision::Allow,
            )?),
        )?
        .with_task_context(&context, operation)?;
        let input = TurnInput {
            max_steps: None,
            operation_id: operation,
            input: ModelContent::Text("respond".into()),
            selected_context: None,
        };
        let source = crate::context::Context {
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("earlier history".into()),
            }],
            ..Default::default()
        };
        let instruction = ModelContent::Text("summarize".into());
        let summary = executor
            .summarize_in_turn(&journal, &input, 0, source.clone(), instruction.clone())
            .await?;
        assert_eq!(summary.output.volume(), &journal.volume);
        assert!(journal.authenticate_summary(&summary.output, None)?.await?);
        assert_eq!(
            journal.load(&summary.output, None)?.await?.as_ref(),
            b"portable summary"
        );

        // Readable but unrelated content cannot inherit the summary receipt.
        let unrelated = journal
            .stage(
                operation,
                "unrelated",
                b"portable summary",
                "text/plain",
                None,
            )?
            .await?;
        assert!(!journal.authenticate_summary(&unrelated, None)?.await?);
        let response = executor.execute(input.clone(), &journal).await?;
        assert_eq!(response.text, "response");
        assert_eq!(executor.execute(input.clone(), &journal).await?, response);
        assert_eq!(
            executor
                .summarize_in_turn(&journal, &input, 0, source, instruction)
                .await?,
            summary,
        );
        assert_eq!(
            provider.request_count(),
            2,
            "accepted calls were dispatched again"
        );
        Ok(())
    }

    #[tokio::test]
    async fn selected_compaction_publishes_only_the_signed_working_cut() -> Result<()> {
        let (journal, _issuer) = super::super::test_support::journal().await?;
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
            ScriptedModelProvider::new([ScriptedTurn::Text("done".into())])
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
            Model::new("synthetic".into(), "counter".into(), "1".into())?,
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
            max_steps: None,
            operation_id: OperationId::new(),
            input: selected.messages.last().unwrap().content.clone(),
            selected_context: Some(selected),
        };
        let output = executor.execute(input.clone(), &journal).await?;
        assert_eq!(output.text, "done");
        assert_eq!(provider.request_count(), 1);
        assert_eq!(executor.execute(input.clone(), &journal).await?, output);
        assert_eq!(
            provider.request_count(),
            1,
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
