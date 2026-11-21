use super::*;

/// A turn's history carries its base admission: extending it by one message
/// measures only that message, whether 8 or 512 came before. A rewrite drops
/// the admission, so the next base is measured in full again.
#[cfg(not(target_arch = "wasm32"))]
#[test]
fn turn_history_bounds_only_appended_messages() {
    use crate::test_support::bounds::{measure, notice_message};
    let turn = TurnInput {
        max_steps: None,
        operation_id: OperationId::from_bytes([31; 16]),
        input: ModelContent::Text("input".into()),
        selected_context: None,
    };
    let limits = Limits {
        file_bytes: Some(1_048_576),
        ..Limits::default()
    };
    let measured = |count: usize| {
        let messages = (0..count).map(|_| notice_message()).collect::<Vec<_>>();
        let mut history = crate::executor::TurnHistory::from(messages);
        history.base(&turn, 0, limits, None).unwrap();
        crate::executor::AppendHistory::push_message(&mut history, notice_message(), None).unwrap();
        let (carried, appended) = measure(async { history.base(&turn, 1, limits, None) });
        let (input, carried) = carried.unwrap();
        let assembled = ContextPipeline::base_context(&input, limits, None).unwrap();
        assert_eq!(carried, assembled, "the carried base is the assembled one");
        history.rewrite();
        let (rewritten, full) = measure(async { history.base(&turn, 1, limits, None) });
        assert_eq!(rewritten.unwrap().1, assembled);
        (appended, full)
    };
    let (small, small_full) = measured(8);
    let (large, large_full) = measured(512);
    assert_ne!(small, 0, "the appended notice batch is bounded");
    assert_eq!(large, small);
    assert!(small < small_full && small_full < large_full);
}

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
        max_steps: None,
        operation_id: OperationId::from_bytes([29; 16]),
        input: ModelContent::Text("input".into()),
        selected_context: None,
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
