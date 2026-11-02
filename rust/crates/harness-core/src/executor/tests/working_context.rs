use super::*;

#[tokio::test]
async fn selected_working_cut_survives_followup_steps_and_reopen_before_stages() -> Result<()> {
    for signed in [false, true] {
        selected_working_cut_case(signed).await?;
    }
    Ok(())
}

async fn selected_working_cut_case(signed: bool) -> Result<()> {
    let mut journal = Journal::default();
    let issuer = crate::core::AuthorityRoot::new(
        "selected-working-cut",
        [21; 32],
        crate::core::Authority {
            kind: crate::core::AggregateKind::Conversation,
            id: crate::ConversationId::new().to_string(),
        },
    );
    journal.8 = if signed {
        Some(issuer.verifier(&mut None)?)
    } else {
        None
    };
    let (provider, _) = counted_model(65_536, None, [], []);
    let stage = crate::context::Context {
        messages: vec![ModelMessage {
            role: ModelRole::System,
            content: ModelContent::Text("one stage instruction".into()),
        }],
        ..Default::default()
    };
    let executor = StockExecutor::new(
        Model::new("synthetic".into(), "counter".into(), "1".into())?,
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
        max_steps: None,
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
    };
    let execution = StockExecution::ordinary(&executor);
    execution.ensure_started(&journal, &input).await?;
    let first = execution
        .prepared_response(&journal, &input, 0, &[])
        .await?;
    assert!(first.bytes().len() < 2_000);
    let restarted = StockExecution::ordinary(&executor);
    let replayed = restarted
        .prepared_response(&journal, &input, 0, &[])
        .await?;
    assert_eq!(replayed.bytes(), first.bytes());
    assert_eq!(provider.request_count(), 0, "the backstop called a model");
    let tail = vec![ModelMessage {
        role: ModelRole::Assistant,
        content: ModelContent::Text("tool result after the cut".into()),
    }];
    // A fresh stock session must discover the same authenticated cut without
    // evicting again. The original selected input remains unchanged.
    for session in [&execution, &StockExecution::ordinary(&executor)] {
        let original_input = ContextInput::copy_for_turn(&input, 1, &tail, None)?;
        let (mut context_input, base, working) =
            session.response_base(&journal, &input, 1, &tail).await?;
        let reference = working.as_ref().expect("selected base has a working cut");
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
        let (_, repeated, repeated_working) =
            session.response_base(&journal, &input, 1, &tail).await?;
        assert_eq!(repeated_working.as_ref(), Some(reference));
        assert_eq!(repeated.messages.last(), tail.last());
        assert_eq!(
            journal.4.load(Ordering::SeqCst),
            reads + 1,
            "warm continuation read more than its current projection"
        );
        let cold_reads = journal.4.load(Ordering::SeqCst);
        session.load_working_cut(&journal, reference).await?;
        assert_eq!(
            journal.4.load(Ordering::SeqCst) - cold_reads,
            if signed { 2 } else { 4 },
            "authenticated reopening must read only the cut and retained context"
        );
        let mut wrong = input.clone();
        wrong
            .selected_context
            .as_mut()
            .unwrap()
            .selection
            .conversation_revision += 1;
        assert!(
            session
                .response_base(&journal, &wrong, 1, &tail)
                .await
                .is_err()
        );
        let mut wrong_input = input.clone();
        wrong_input.input = ModelContent::Text("a different admitted input".into());
        assert!(
            session
                .response_base(&journal, &wrong_input, 1, &tail)
                .await
                .is_err()
        );
        let mut foreign = input.clone();
        foreign.operation_id = OperationId::new();
        assert!(
            session
                .synchronize_working_cut(&journal, &foreign, 1, tail.len(), Some(reference))
                .await
                .is_err()
        );
        assert!(
            session
                .load_working_cut(&Journal::default(), reference)
                .await
                .is_err()
        );
        working_cut_admission_controls(session, &journal, &input, reference, signed).await?;
    }
    assert!(provider.requests().is_empty());
    let address = std::ptr::from_ref(&journal) as usize;
    journal = Journal::default();
    assert_eq!(std::ptr::from_ref(&journal) as usize, address);
    let (_, original, working) = execution.response_base(&journal, &input, 1, &tail).await?;
    assert!(
        working.is_none(),
        "replacing the journal retained an authenticated working cut"
    );
    assert!(crate::contract::canonical_json_bytes(&original)?.len() > 50_000);
    Ok(())
}

async fn working_cut_admission_controls(
    execution: &StockExecution<'_>,
    journal: &Journal,
    input: &TurnInput,
    reference: &FileRef,
    signed: bool,
) -> Result<()> {
    let cut = load_json::<Value>(journal, reference, None).await?.0;
    let source_ref: FileRef = serde_json::from_value(cut["source"].clone()).unwrap();
    let retained_ref: FileRef = serde_json::from_value(cut["retained"].clone()).unwrap();
    let proof_ref: FileRef = serde_json::from_value(cut["compaction"].clone()).unwrap();
    let source = load_json::<crate::context::Context>(journal, &source_ref, None)
        .await?
        .0;
    let retained = load_json::<crate::context::Context>(journal, &retained_ref, None)
        .await?
        .0;
    let proof = load_json::<crate::context::CompactionReference>(journal, &proof_ref, None)
        .await?
        .0;
    let mut fresh = input.clone();
    fresh.operation_id = OperationId::new();
    let staged = journal.1.lock().unwrap().len();
    for invalid in 0..3 {
        let mut proof = proof.clone();
        proof.summary.as_mut().unwrap().operation_id = fresh.operation_id;
        let mut retained = retained.clone();
        match invalid {
            0 => proof.source_digest[0] ^= 1,
            1 => retained.current_input_index = Some(u32::MAX),
            _ => proof.summary.as_mut().unwrap().step += 1,
        }
        assert!(
            execution
                .stage_working_cut(
                    journal,
                    crate::executor::working_tail::WorkingCompaction {
                        input: &fresh,
                        step: 0,
                        prior: 0,
                        source: &source,
                        retained: &retained,
                        proof: &proof,
                    }
                )
                .await
                .is_err()
        );
        assert_eq!(
            journal.1.lock().unwrap().len(),
            staged,
            "invalid semantic admission staged content before refusal"
        );
    }
    for mutation in 0..3 {
        let mut changed = cut.clone();
        match mutation {
            0 => changed["version"] = serde_json::json!(2),
            1 => {
                changed.as_object_mut().unwrap().remove("version");
            }
            _ => changed["retained"] = changed["source"].clone(),
        }
        let changed =
            stage_json(journal, OperationId::new(), "changed-cut", &changed, None).await?;
        assert!(
            execution.load_working_cut(journal, &changed).await.is_err(),
            "changed format or retained reference inherited an admitted cut"
        );
    }
    // Original source loss cannot turn a signed retained read into hydration.
    // An unsigned adapter still needs that source to validate its claim.
    let key = journal
        .1
        .lock()
        .unwrap()
        .iter()
        .find(|(_, (file, _))| file == &source_ref)
        .unwrap()
        .0
        .clone();
    let removed = journal.1.lock().unwrap().remove(&key).unwrap();
    let reopened = execution.load_working_cut(journal, reference).await;
    journal.1.lock().unwrap().insert(key, removed);
    assert_eq!(
        reopened.is_ok(),
        signed,
        "source loss must distinguish authenticated cuts from unsigned claims"
    );
    Ok(())
}
