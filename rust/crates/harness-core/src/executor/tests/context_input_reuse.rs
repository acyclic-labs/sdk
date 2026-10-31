use super::*;

#[test]
fn base_and_stages_reuse_the_same_input_without_a_history_copy() {
    let executor = uncompacted_executor(
        Model::new("test".into(), "context-input".into(), "1".into()).unwrap(),
        Arc::new(ScriptedModelProvider::new([])),
        ContextPipeline::default(),
        ToolRegistry::default(),
    );
    let execution = StockExecution::ordinary(&executor);
    let journal = Journal::default();
    let turn = TurnInput {
        operation_id: OperationId::from_bytes([29; 16]),
        input: ModelContent::Text("input".into()),
        selected_context: None,
        max_steps: 1,
    };
    for count in [1, 10, 100, 1000] {
        let prior: Vec<_> = (0..count)
            .map(|index| ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Text(format!("history-{index}")),
            })
            .collect();
        let mut input = ContextInput::copy_for_turn(&turn, 0, &prior, None).unwrap();
        let expected_input = input.clone();
        let mut copied = None;
        let copying = acyclic_test_support::measure(|| {
            copied = Some(ContextInput::copy_for_turn(&turn, 0, &prior, None).unwrap());
        });
        drop(copied);
        let mut old_base = None;
        let old = acyclic_test_support::measure(|| {
            old_base = Some(
                futures::executor::block_on(execution.canonical_base(&journal, &turn, 0, &prior))
                    .unwrap(),
            );
        });
        let mut new_base = None;
        let new = acyclic_test_support::measure(|| {
            new_base = Some(
                futures::executor::block_on(
                    execution.canonical_base_for_input(&journal, &mut input),
                )
                .unwrap(),
            );
        });
        assert_eq!(new_base, old_base);
        assert_eq!(input, expected_input);
        let no_copy = |observed: acyclic_test_support::AllocationInfo| {
            observed.count_total + copying.count_total <= old.count_total
                && observed.bytes_total + copying.bytes_total <= old.bytes_total
        };
        assert!(
            no_copy(new),
            "count={count}: {new:?} + {copying:?} != {old:?}"
        );
        assert!(
            !no_copy(old),
            "negative control did not detect the input copy"
        );
    }
}

#[tokio::test]
async fn base_assembly_restores_the_stage_checkpoint_before_returning_an_error() -> Result<()> {
    let executor = uncompacted_executor(
        Model::new("test".into(), "context-input".into(), "1".into())?,
        Arc::new(ScriptedModelProvider::new([])),
        ContextPipeline::default(),
        ToolRegistry::default(),
    );
    let execution = StockExecution::ordinary(&executor);
    let journal = Journal::default();
    let operation = OperationId::from_bytes([30; 16]);
    let checkpoint = stage_json(
        &journal,
        operation,
        "invalid-checkpoint",
        &Value::Null,
        None,
    )
    .await?;
    let mut input = ContextInput {
        input: ModelContent::Text("input".into()),
        step: 0,
        prior_messages: vec![],
        selected_context: Some(SelectedModelContext {
            selection: crate::conversation::ModelContextSelection {
                conversation_revision: 2,
                message_ids: vec![uuid::Uuid::from_u128(1)],
                checkpoint: Some(checkpoint),
            },
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("input".into()),
            }],
        }),
    };
    let expected = input.clone();
    assert!(
        execution
            .canonical_base_for_input(&journal, &mut input)
            .await
            .is_err()
    );
    assert_eq!(
        input, expected,
        "checkpoint was removed from the stage input"
    );
    input
        .selected_context
        .as_mut()
        .ok_or_else(|| Error::Invalid("fixture selection absent".into()))?
        .selection
        .checkpoint
        .take();
    assert_ne!(
        input, expected,
        "negative control omitted checkpoint restoration"
    );
    Ok(())
}
