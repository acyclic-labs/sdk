use super::*;

#[tokio::test]
async fn canonical_cut_survives_followup_steps_and_reopen_before_stages() -> Result<()> {
    let mut journal = Journal::default();
    let (provider, _) = counted_model(
        65_536,
        None,
        [ScriptedTurn::Text("bounded summary".into())],
        [],
    );
    let stage = crate::context::Context {
        messages: vec![ModelMessage {
            role: ModelRole::System,
            content: ModelContent::Text("one stage instruction".into()),
        }],
        ..Default::default()
    };
    let executor = StockExecutor::new(
        Model::new(
            "synthetic".into(),
            "counter".into(),
            "1".into(),
            Value::Null,
        )?,
        provider.clone(),
        ContextPipeline::new([Arc::new(crate::context::SourceStage::new(
            "instructions",
            "1",
            Arc::new(stage),
            crate::context::ContextPlacement::Prepend,
        )) as Arc<dyn crate::context::ContextStage>]),
        ToolRegistry::default(),
    )
    .with_compaction_policy(crate::context::CompactionPolicy::Threshold(
        crate::context::ThresholdCompaction {
            recent_tokens: 1,
            ..Default::default()
        },
    ));
    let selected = vec![
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
    ];
    let input = TurnInput {
        operation_id: OperationId::new(),
        input: selected.last().unwrap().content.clone(),
        selected_context: Some(SelectedModelContext {
            selection: crate::conversation::ModelContextSelection {
                conversation_revision: 3,
                message_ids: (1..=3).map(uuid::Uuid::from_u128).collect(),
                checkpoint: None,
            },
            messages: selected,
        }),
        max_steps: 3,
    };
    let execution = StockExecution::ordinary(&executor);
    execution.ensure_started(&journal, &input).await?;
    let first = execution
        .prepared_response(&journal, &input, 0, &[], &mut DispatchPhase::new())
        .await?;
    assert!(first.bytes().len() < 2_000);
    let tail = vec![ModelMessage {
        role: ModelRole::Assistant,
        content: ModelContent::Text("tool result after the cut".into()),
    }];
    // A fresh stock session must discover the same authenticated cut without
    // redispatching its summary. The original selected input remains unchanged.
    for session in [&execution, &StockExecution::ordinary(&executor)] {
        let mut context_input = ContextInput::copy_for_turn(&input, 1, &tail, None)?;
        let original_input = context_input.clone();
        let base = session
            .canonical_base_for_turn(&journal, input.operation_id, &mut context_input)
            .await?;
        assert_eq!(context_input, original_input);
        assert_eq!(base.messages.last(), tail.last());
        let current = base.current_input_index.unwrap() as usize;
        assert_eq!(base.messages[current].content, input.input);
        assert_eq!(
            base.messages
                .iter()
                .filter(|message| message.content == input.input)
                .count(),
            1
        );
        let original = session
            .canonical_base_for_input(&journal, &mut context_input)
            .await?;
        println!(
            "working cut: original={} retained={} bytes; request={} bytes",
            crate::contract::canonical_json_bytes(&original)?.len(),
            crate::contract::canonical_json_bytes(&base)?.len(),
            first.bytes().len()
        );
        assert!(
            preparation_digest(&base, None)? != preparation_digest(&original, None)?,
            "negative control rebuilt the old selected history"
        );
        let projected = session.transform_projection(&context_input, base).await?;
        assert_eq!(
            projected
                .messages
                .iter()
                .filter(|message| {
                    message.content == ModelContent::Text("one stage instruction".into())
                })
                .count(),
            1
        );
        assert!(crate::contract::canonical_json_bytes(&projected)?.len() < 2_000);
        let reads = journal.4.load(Ordering::SeqCst);
        let _ = session
            .canonical_base_for_turn(&journal, input.operation_id, &mut context_input)
            .await?;
        assert_eq!(
            journal.4.load(Ordering::SeqCst),
            reads,
            "warm continuation reloaded old checkpoint payloads"
        );
        let mut wrong = context_input.clone();
        wrong
            .selected_context
            .as_mut()
            .unwrap()
            .selection
            .conversation_revision += 1;
        assert!(
            session
                .canonical_base_for_turn(&journal, input.operation_id, &mut wrong)
                .await
                .is_err()
        );
        let mut wrong_input = context_input.clone();
        wrong_input.input = ModelContent::Text("a different admitted input".into());
        assert!(
            session
                .canonical_base_for_turn(&journal, input.operation_id, &mut wrong_input)
                .await
                .is_err()
        );
        assert!(
            session
                .canonical_base_for_turn(&journal, OperationId::new(), &mut context_input)
                .await
                .is_err()
        );
        assert!(
            session
                .canonical_base_for_turn(
                    &Journal::default(),
                    input.operation_id,
                    &mut context_input
                )
                .await
                .is_err()
        );
    }
    assert_eq!(provider.requests().len(), 1);
    let address = std::ptr::from_ref(&journal) as usize;
    journal = Journal::default();
    assert_eq!(std::ptr::from_ref(&journal) as usize, address);
    let mut context_input = ContextInput::copy_for_turn(&input, 1, &tail, None)?;
    assert!(
        execution
            .canonical_base_for_turn(&journal, input.operation_id, &mut context_input)
            .await
            .is_err(),
        "negative control reused a canonical cut after replacing its journal"
    );
    Ok(())
}
