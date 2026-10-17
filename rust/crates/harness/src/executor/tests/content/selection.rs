use super::*;
use crate::{conversation::ModelContextSelection, projection::SelectedModelContext};
use proptest::prelude::*;

#[derive(Default)]
struct Checks {
    // Single-thread test observations; never nested or held across an await.
    contents: Mutex<Vec<ModelContent>>,
    selections: AtomicUsize,
    files: Journal,
}

impl ExecutionJournal for Checks {
    fn replay<'a>(
        &'a self,
        _: OperationId,
        _: u64,
        _: u32,
        _: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<ExecutionRecord>>> {
        Err(Error::Unsupported(
            "unexpected preflight journal replay".into(),
        ))
    }
    fn append<'a>(
        &'a self,
        _: OperationId,
        _: &'a str,
        _: &'a ExecutionEvent,
        _: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        Err(Error::Unsupported(
            "unexpected preflight journal append".into(),
        ))
    }
    fn stage<'a>(
        &'a self,
        _: OperationId,
        _: &'a str,
        _: &'a [u8],
        _: &'static str,
        _: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, FileRef>> {
        Err(Error::Unsupported(
            "unexpected preflight journal publication".into(),
        ))
    }
    fn load<'a>(
        &'a self,
        _: &'a FileRef,
        _: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        Err(Error::Unsupported(
            "unexpected preflight journal load".into(),
        ))
    }
    fn open_interaction<'a>(
        &'a self,
        _: InteractionId,
        _: &'a Interaction,
        _: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        Err(Error::Unsupported(
            "unexpected preflight interaction".into(),
        ))
    }
    fn interaction_outcome<'a>(
        &'a self,
        _: InteractionId,
        _: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Option<InteractionOutcome>>> {
        Err(Error::Unsupported(
            "unexpected preflight interaction read".into(),
        ))
    }

    fn verify_selected_context<'a>(
        &'a self,
        _: OperationId,
        selected: &'a SelectedModelContext,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, move || async move {
            self.selections.fetch_add(1, Ordering::SeqCst);
            for message in &selected.messages {
                self.verify_model_content(&message.content, io, None, &[])?
                    .await?;
            }
            Ok(())
        })
    }

    fn verify_model_content<'a>(
        &'a self,
        content: &'a ModelContent,
        io: Option<&'a PreparationIo>,
        _: Option<&'a crate::runtime::TaskContext>,
        summaries: &'a [&'a FileRef],
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, move || async move {
            self.contents.lock().unwrap().push(content.clone());
            self.files
                .verify_model_content(content, io, None, summaries)?
                .await
        })
    }
}

fn input(count: usize, text: &str) -> TurnInput {
    let messages: Vec<_> = (0..count)
        .map(|index| ModelMessage {
            role: if index + 1 == count {
                ModelRole::User
            } else {
                ModelRole::Assistant
            },
            content: ModelContent::Text(format!("{index}:{text}")),
        })
        .collect();
    TurnInput {
        operation_id: OperationId::from_bytes([9; 16]),
        input: messages.last().unwrap().content.clone(),
        selected_context: Some(SelectedModelContext {
            selection: ModelContextSelection {
                conversation_revision: count as u64,
                message_ids: (1..=count)
                    .map(|id| uuid::Uuid::from_u128(id as u128))
                    .collect(),
                checkpoint: None,
            },
            messages,
        }),
        max_steps: 1,
    }
}

fn check_once(journal: &Checks, expected: &[ModelContent]) -> bool {
    journal.selections.load(Ordering::SeqCst) == 1
        && journal.contents.lock().unwrap().as_slice() == expected
        && journal.files.4.load(Ordering::SeqCst) == expected.len()
}

fn rejected_parent<T>(result: &Result<T>) -> bool {
    matches!(result, Err(Error::Invalid(reason))
        if reason == "selected context must end with the current user input")
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    // The last selected message is the input itself: authenticate every selected
    // message exactly once, and reject a different input before journal I/O.
    #[test]
    fn selected_input_is_verified_once(count in 1usize..9, text in "[a-z]{1,24}") {
        let executor = uncompacted_executor(
            Model::new("test".into(), "reads".into(), "1".into(), Value::Null).unwrap(),
            Arc::new(ScriptedModelProvider::new([])),
            ContextPipeline::default(), ToolRegistry::default(),
        );
        let journal = Checks::default();
        let mut input = input(count, &text);
        futures::executor::block_on(async {
            for (index, message) in input.selected_context.as_mut().unwrap().messages.iter_mut().enumerate() {
                let file = journal.files.stage(input.operation_id, &format!("history-{index}"),
                    text.as_bytes(), "text/plain", None)?.await?;
                message.content = ModelContent::Part(ModelContentPart::File {
                    file, policy: crate::model::FileProjectionPolicy::Reference,
                });
            }
            Ok::<_, Error>(())
        }).unwrap();
        input.input = input.selected_context.as_ref().unwrap().messages.last().unwrap().content.clone();
        let expected: Vec<_> = input.selected_context.as_ref().unwrap().messages.iter()
            .map(|message| message.content.clone()).collect();
        futures::executor::block_on(StockExecution::ordinary(&executor)
            .validate_turn_input(&journal, &input)).unwrap();
        prop_assert!(check_once(&journal, &expected));
        // The former path re-reads every authenticated history file. Both paths
        // load stored bytes and verify their descriptors, not just callbacks.
        futures::executor::block_on(verify_model_contents_scoped(&journal,
            expected.iter(), Limits::default(), None, None, None)).unwrap();
        prop_assert!(!check_once(&journal, &expected));
        prop_assert_eq!(journal.files.4.load(Ordering::SeqCst), 2 * count);

        input.input = ModelContent::Text("different input".into());
        let rejected = Checks::default();
        prop_assert!(futures::executor::block_on(StockExecution::ordinary(&executor)
            .validate_turn_input(&rejected, &input)).is_err());
        prop_assert_eq!(rejected.selections.load(Ordering::SeqCst), 0);
        prop_assert!(rejected.contents.lock().unwrap().is_empty());
    }

    // Standalone in-turn summaries retain parent validation at their public
    // boundary; only automatic compaction reuses its already validated parent.
    #[test]
    fn public_summary_rejects_an_invalid_parent_before_dispatch(count in 1usize..9, text in "[a-z]{1,24}") {
        let provider = Arc::new(ScriptedModelProvider::new([]));
        let executor = uncompacted_executor(
            Model::new("test".into(), "reads".into(), "1".into(), Value::Null).unwrap(),
            provider.clone(), ContextPipeline::default(), ToolRegistry::default(),
        ).with_max_output_tokens(64).unwrap();
        let mut parent = input(count, &text);
        parent.input = ModelContent::Text("different input".into());
        let journal = Checks::default();
        let result = futures::executor::block_on(executor.summarize_in_turn(&journal,
            &parent, 0, crate::context::Context::default(), ModelContent::Text("summarize".into())));
        prop_assert!(rejected_parent(&result));
        prop_assert!(provider.requests().is_empty());
        prop_assert_eq!(journal.selections.load(Ordering::SeqCst), 0);
        prop_assert!(journal.contents.lock().unwrap().is_empty());
        // Broken public boundary: call the inner summary path without validating
        // its parent. A later, unrelated rejection must not satisfy this check.
        let broken = futures::executor::block_on(StockExecution::ordinary(&executor)
            .summarize(&journal, Some(&parent), parent.operation_id, 0,
                crate::context::Context::default(), ModelContent::Text("summarize".into())));
        prop_assert!(!rejected_parent(&broken));
    }

    // Staging already authenticates the exact encoded proof. Checkpoint creation
    // must validate that held proof without loading a second copy from storage.
    #[test]
    fn canonical_checkpoint_does_not_reload_its_held_proof(text in "[a-z]{1,24}") {
        futures::executor::block_on(async {
            let executor = uncompacted_executor(
                Model::new("test".into(), "reads".into(), "1".into(), Value::Null)?,
                Arc::new(ScriptedModelProvider::new([])),
                ContextPipeline::default(), ToolRegistry::default(),
            );
            let journal = Journal::default();
            let input = input(3, &text);
            let source = crate::context::Context {
                messages: input.selected_context.as_ref().unwrap().messages.clone(),
                current_input_index: Some(2),
                ..Default::default()
            };
            let (prefix, _) = crate::context::clone_context(&source, Some(2), &mut None)?;
            let output = journal.stage(input.operation_id, "summary", b"summary", "text/plain", None)?.await?;
            let summary = crate::context::ContextSummary {
                        operation_id: input.operation_id,
                step: 0,
                source_messages: 2,
                source_digest: crate::executor::json::canonical_json_digest(&prefix, u64::MAX, &mut None)?,
                output,
            };
            let (retained, proof) = crate::context::DurableContextProvider::compact(
                &source, 2, Some(summary), &Default::default(), None)?;
            let source_file = stage_json(&journal, input.operation_id, "source", &source, None).await?;
            let proof_file = stage_json(&journal, input.operation_id, "proof", &proof, None).await?;
            let before = journal.4.load(Ordering::SeqCst);
            assert!(StockExecution::ordinary(&executor).stage_canonical_checkpoint(
                &journal, &input, Some(&source_file), &source, &retained,
                (&proof_file, &proof)).await?.is_some());
            assert_eq!(journal.4.load(Ordering::SeqCst), before);
            // Real former read: the same no-reload check rejects this variant.
            load_json::<crate::context::CompactionReference>(&journal, &proof_file, None).await?;
            assert_ne!(journal.4.load(Ordering::SeqCst), before);
            Ok::<_, Error>(())
        }).unwrap();
    }
}
