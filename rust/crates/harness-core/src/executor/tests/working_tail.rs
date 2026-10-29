use super::*;

#[tokio::test]
async fn later_compaction_replaces_pre_stage_history_and_survives_reopen() -> Result<()> {
    let mut journal = Journal::default();
    let issuer = crate::core::AuthorityRoot::new(
        "working-cut",
        [19; 32],
        crate::core::Authority {
            kind: crate::core::AggregateKind::Conversation,
            id: crate::ConversationId::new().to_string(),
        },
    );
    journal.8 = Some(issuer.verifier(&mut None)?);
    let (provider, _) = counted_model(
        65_536,
        None,
        [ScriptedTurn::Text("bounded summary".into())],
        [],
    );
    let instruction = ModelContent::Text("fresh stage instruction".into());
    let stage = crate::context::Context {
        messages: vec![ModelMessage {
            role: ModelRole::System,
            content: instruction.clone(),
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
    let input = TurnInput {
        operation_id: OperationId::new(),
        input: ModelContent::Text("current question".into()),
        selected_context: None,
        max_steps: 4,
    };
    let mut prior = vec![
        ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text("old user".repeat(4_000)),
        },
        ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Text("old answer".repeat(2_000)),
        },
        ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Text("recent answer".into()),
        },
    ];
    let execution = StockExecution::ordinary(&executor);
    execution.ensure_started(&journal, &input).await?;
    let compacted = execution
        .prepared_response(&journal, &input, 1, &prior, &mut DispatchPhase::new())
        .await?;
    assert!(compacted.bytes().len() < 2_000);
    assert_eq!(provider.requests().len(), 1);
    for step in 2..4 {
        prior.push(ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Text(format!("new tail {step}")),
        });
        // A new execution has no in-memory working checkpoint to rely on.
        let session = StockExecution::ordinary(&executor);
        let (context_input, base, working) = session
            .response_base(&journal, &input, step, &prior)
            .await?;
        assert!(working.is_some());
        if step == 2 {
            let reference = working
                .as_ref()
                .ok_or_else(|| Error::Invalid("missing working cut".into()))?;
            let bytes = journal.load(reference, None)?.await?;
            let original: Value = serde_json::from_slice(&bytes)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            for (label, pointer, replacement) in [
                ("missing signature", "/proof", Value::Null),
                ("consumed count", "/prior_messages", Value::from(4)),
                ("step", "/step", Value::from(0)),
                (
                    "input binding",
                    "/input_digest/0",
                    Value::from(
                        original
                            .pointer("/input_digest/0")
                            .and_then(Value::as_u64)
                            .ok_or_else(|| Error::Invalid("missing input digest".into()))?
                            ^ 1,
                    ),
                ),
                (
                    "proof",
                    "/proof/0/0",
                    Value::from(
                        original
                            .pointer("/proof/0/0")
                            .and_then(Value::as_u64)
                            .ok_or_else(|| Error::Invalid("missing proof".into()))?
                            ^ 1,
                    ),
                ),
                (
                    "retained source",
                    "/retained",
                    original
                        .get("source")
                        .cloned()
                        .ok_or_else(|| Error::Invalid("missing source".into()))?,
                ),
            ] {
                let mut forged = original.clone();
                *forged
                    .pointer_mut(pointer)
                    .ok_or_else(|| Error::Invalid("missing cut field".into()))? = replacement;
                let reference = stage_json(
                    &journal,
                    input.operation_id,
                    &format!("forged-cut:{label}"),
                    &forged,
                    None,
                )
                .await?;
                assert!(
                    session
                        .load_working_cut(&journal, &reference)
                        .await
                        .is_err(),
                    "forged {label} accepted"
                );
            }
        }
        assert_eq!(context_input.prior_messages.len(), prior.len() - 3);
        assert_eq!(base.messages.last(), prior.last());
        let marker = base
            .current_input_index
            .ok_or_else(|| Error::Invalid("missing current input".into()))?;
        let current = base
            .messages
            .get(marker as usize)
            .ok_or_else(|| Error::Invalid("invalid current input".into()))?;
        assert_eq!(current.content, input.input);
        assert!(
            crate::contract::canonical_json_bytes(&base)?.len() < 2_000,
            "later working checkpoint resurrected the consumed history"
        );
        let mut original_input = ContextInput::copy_for_turn(&input, step, &prior, None)?;
        let old = session
            .canonical_base_for_input(&journal, &mut original_input)
            .await?;
        assert!(
            crate::contract::canonical_json_bytes(&old)?.len() > 50_000,
            "the original-history negative control did not restore the consumed prefix"
        );
        let projected = session.transform_projection(&context_input, base).await?;
        assert_eq!(
            projected
                .messages
                .iter()
                .filter(|message| message.content == instruction)
                .count(),
            1,
            "working history retained stale stage output"
        );
        let response = session
            .prepared_response(&journal, &input, step, &prior, &mut DispatchPhase::new())
            .await?;
        assert!(response.bytes().len() < 2_000);
        assert_eq!(
            provider.requests().len(),
            1,
            "a consumed prefix was summarized again"
        );
        let mut changed = input.clone();
        changed.input = ModelContent::Text("different question".into());
        assert!(
            session
                .response_base(&journal, &changed, step, &prior)
                .await
                .is_err()
        );
        assert!(
            session
                .response_base(&journal, &input, step, &[])
                .await
                .is_err()
        );
        let mut tail = prior.clone();
        session.retire_working_history(&mut tail).await?;
        assert_eq!(tail.as_slice(), &prior[3..]);
        assert_eq!(
            session.absolute_prior_count(tail.len()).await?,
            prior.len() as u64
        );
        let (_, retired_base, _) = session.response_base(&journal, &input, step, &tail).await?;
        assert_eq!(retired_base.messages.last(), prior.last());
        assert!(crate::contract::canonical_json_bytes(&retired_base)?.len() < 2_000);
        assert!(
            session
                .response_base(&journal, &input, 0, &tail)
                .await
                .is_err(),
            "a missing checkpoint silently discarded the retired prefix"
        );
        session.begin_working_history().await;
        assert_eq!(session.absolute_prior_count(0).await?, 0);
        assert!(
            session
                .response_base(&journal, &input, step, &prior)
                .await
                .is_ok()
        );
    }
    Ok(())
}

#[tokio::test]
async fn working_checkpoint_reads_require_only_the_public_key() -> Result<()> {
    let authority = crate::core::Authority {
        kind: crate::core::AggregateKind::Conversation,
        id: crate::ConversationId::new().to_string(),
    };
    let issuer = crate::core::AuthorityRoot::new("working-cut", [19; 32], authority);
    let verifier = issuer.verifier(&mut None)?;
    let proof = verifier
        .working_checkpoint_proof([31; 32], &mut None)
        .await?;
    let public = crate::core::AuthorityVerifier::from_public(
        "working-cut".into(),
        verifier.audience().clone(),
        issuer.public_workload_trust()?,
        verifier.public_checkpoint_key(),
        issuer.trust_changes(),
    )?;
    drop(issuer);
    drop(verifier);
    public.verify_working_checkpoint([31; 32], &proof, &mut None)?;
    assert!(
        public
            .verify_working_checkpoint([32; 32], &proof, &mut None)
            .is_err()
    );
    assert!(
        public
            .working_checkpoint_proof([31; 32], &mut None)
            .await
            .is_err()
    );
    Ok(())
}

#[path = "working_tail/native.rs"]
mod native;

#[path = "working_tail/driver.rs"]
mod driver;
