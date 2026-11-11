use super::*;

#[derive(Clone, Copy)]
enum Dispatch {
    Model,
    Tool,
    InterleaveStage,
}

struct BarrierJournal(Journal, Dispatch, AtomicUsize);

impl BarrierJournal {
    fn records(&self) -> Result<std::sync::MutexGuard<'_, Vec<ExecutionRecord>>> {
        self.0
            .0
            .lock()
            .map_err(|_| Error::Storage("fixture journal lock poisoned".into()))
    }

    fn is_dispatch(&self, event: &ExecutionEvent) -> bool {
        matches!(
            (self.1, event),
            (Dispatch::Model, ExecutionEvent::ModelStarted { .. })
                | (Dispatch::Tool, ExecutionEvent::ToolStarted { .. })
        )
    }
}

impl ExecutionJournal for BarrierJournal {
    fn supports_deferred_durability(&self) -> bool {
        true
    }

    fn durability_barrier<'a>(
        &'a self,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        if self
            .records()?
            .last()
            .is_some_and(|record| self.is_dispatch(&record.event))
        {
            self.2.fetch_add(1, Ordering::SeqCst);
            return Err(Error::Conflict("fixture barrier admission refused".into()));
        }
        crate::conversation::BoundedContentFuture::new(io, || async { Ok(()) })
    }

    fn acquire_execution<'a>(
        &'a self,
        operation: OperationId,
        io: Option<&PreparationIo>,
    ) -> Result<crate::executor::ExecutionAttempt<'a>> {
        self.0.acquire_execution(operation, io)
    }
    fn replay<'a>(
        &'a self,
        operation: OperationId,
        after: u64,
        maximum: u32,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<ExecutionRecord>>> {
        self.0.replay(operation, after, maximum, io)
    }
    fn append<'a>(
        &'a self,
        operation: OperationId,
        key: &'a str,
        event: &'a ExecutionEvent,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        self.0.append(operation, key, event, io)
    }
    fn append_if_tail<'a>(
        &'a self,
        operation: OperationId,
        tail: u64,
        key: &'a str,
        event: &'a ExecutionEvent,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, bool>> {
        self.0.append_if_tail(operation, tail, key, event, io)
    }
    fn stage<'a>(
        &'a self,
        operation: OperationId,
        key: &'a str,
        bytes: &'a [u8],
        media: &'static str,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, FileRef>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            if matches!(self.1, Dispatch::InterleaveStage) {
                tokio::task::yield_now().await;
            }
            self.0.stage(operation, key, bytes, media, io)?.await
        })
    }
    fn load<'a>(
        &'a self,
        reference: &'a FileRef,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        self.0.load(reference, io)
    }
    fn supports_native_execution(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<bool> {
        self.0.supports_native_execution(work)
    }
    fn open_interaction<'a>(
        &'a self,
        id: InteractionId,
        interaction: &'a Interaction,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        self.0.open_interaction(id, interaction, io)
    }
    fn interaction_outcome<'a>(
        &'a self,
        id: InteractionId,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Option<InteractionOutcome>>> {
        self.0.interaction_outcome(id, io)
    }
}

#[tokio::test]
async fn concurrent_summary_and_response_preserve_both_dispatch_claims() -> Result<()> {
    for shared in [true, false] {
        let journal = BarrierJournal(
            Journal::default(),
            Dispatch::InterleaveStage,
            AtomicUsize::new(0),
        );
        let provider = Arc::new(ScriptedModelProvider::new([
            ScriptedTurn::Text("first".into()),
            ScriptedTurn::Text("second".into()),
        ]));
        let executor = uncompacted_executor(
            Model::new("test".into(), "concurrent-context".into(), "1".into())?,
            provider.clone(),
            ContextPipeline::default(),
            ToolRegistry::default(),
        );
        let execution = StockExecution::ordinary(&executor);
        let independent = StockExecution::ordinary(&executor);
        let input = TurnInput {
            operation_id: OperationId::new(),
            max_steps: None,
            input: ModelContent::Text("continue".into()),
            selected_context: None,
        };
        execution.ensure_started(&journal, &input).await?;
        let source = crate::context::Context {
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("history".into()),
            }],
            ..Default::default()
        };
        let summary_owner = if shared { &execution } else { &independent };
        let mut phase = DispatchPhase::new();
        let summary = summary_owner.summarize(
            &journal,
            Some(&input),
            input.operation_id,
            0,
            (source, ModelContent::Text("summarize".into())),
            &mut phase,
        );
        let response = execution.model_step(&journal, &input, 0, &[]);
        let (summary, response) = tokio::join!(summary, response);
        if shared {
            summary?;
            response?;
            assert_eq!(provider.requests().len(), 2);
            assert_eq!(
                journal
                    .records()?
                    .iter()
                    .filter(|record| matches!(record.event, ExecutionEvent::ModelStarted { .. }))
                    .count(),
                2
            );
        } else {
            assert!(
                summary.is_err() || response.is_err(),
                "independent writers did not expose the stale-tail counterexample"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn dispatch_claims_have_no_fallible_post_publication_barrier() -> Result<()> {
    for dispatch in [Dispatch::Model, Dispatch::Tool] {
        let tool = Arc::new(FakeTool(AtomicUsize::new(0)));
        let provider = Arc::new(ScriptedModelProvider::new([
            ScriptedTurn::Events(vec![
                ScriptedEvent::Event(ModelEvent::ToolCall(crate::model::ToolCall::new(
                    "call".into(),
                    "example.echo".into(),
                    json!({}),
                ))),
                ScriptedEvent::Event(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ]),
            ScriptedTurn::Text("answer".into()),
        ]));
        let executor = uncompacted_executor(
            Model::new("test".into(), "claim".into(), "1".into())?,
            provider.clone(),
            ContextPipeline::default(),
            echo_tools(tool.clone(), json!(true), json!(true), json!(true))?,
        )
        .with_tool_authority(
            RuntimeScope::new(
                Capabilities::new(["model:generate", "tool:call:example.echo"]),
                Limits::default(),
            )?,
            allowed()?,
        )?;
        let journal = BarrierJournal(Journal::default(), dispatch, AtomicUsize::new(0));
        let operation = OperationId::new();
        let result = executor
            .execute(
                TurnInput {
                    max_steps: None,
                    operation_id: operation,
                    input: ModelContent::Text("input".into()),
                    selected_context: None,
                },
                &journal,
            )
            .await?;
        assert_eq!(result.text, "answer");
        assert_eq!(tool.0.load(Ordering::SeqCst), 1);
        assert_eq!(provider.requests().len(), 2);
        assert_eq!(journal.2.load(Ordering::SeqCst), 0);
        let dispatch = journal
            .records()?
            .iter()
            .find(|record| journal.is_dispatch(&record.event))
            .map(|record| record.event.clone())
            .ok_or_else(|| Error::Storage("fixture dispatch missing".into()))?;
        // The former deferred claim commits its start, then fails admission
        // before the executor can issue the corresponding external effect.
        let old = BarrierJournal(Journal::default(), journal.1, AtomicUsize::new(0));
        let result = acyclic_stream::deferring_durability(async {
            old.append(operation, "old-claim", &dispatch, None)?.await?;
            old.durability_barrier(None)?.await
        })
        .await;
        assert!(result.is_err());
        assert_eq!(old.records()?.len(), 1);
        assert_eq!(old.2.load(Ordering::SeqCst), 1);
    }
    Ok(())
}
