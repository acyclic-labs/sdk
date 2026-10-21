use super::*;
use proptest::prelude::*;

proptest! {
    // Both UTF-8 boundaries and replay use the production StockExecutor.
    // The filesystem-backed boundary scenarios remain in the e2e catalog.
    #[test]
    fn streamed_output_budget_counts_utf8_bytes_across_every_delta(
        mut chunks in prop::collection::vec(
            prop::collection::vec(any::<char>(), 0..9)
                .prop_map(|chars| chars.into_iter().collect::<String>()),
            1..9,
        ),
    ) {
        chunks[0].insert_str(0, "xy");
        let text = chunks.concat();
        for at in [true, false] {
            let mut events = chunks.iter().map(|delta| ScriptedEvent::Event(
                ModelEvent::Content { delta: delta.clone() }
            )).collect::<Vec<_>>();
            events.push(ScriptedEvent::Event(ModelEvent::Completed { metadata: Value::Null }));
            let provider = Arc::new(ScriptedModelProvider::new([ScriptedTurn::Events(events)]));
            let executor = uncompacted_executor(
                Model::new("scripted".into(), "scenario".into(), "1".into(), Value::Null)?,
                provider.clone(), ContextPipeline::default(), ToolRegistry::default(),
            ).with_limits(Limits { file_bytes: (text.len() - usize::from(!at)) as u64, ..Limits::default() });
            let journal = Journal::default();
            let input = TurnInput { operation_id: OperationId::new(), input: ModelContent::Text("x".into()), selected_context: None, max_steps: 1 };
            let result = futures::executor::block_on(executor.execute(input.clone(), &journal));
            if at {
                let actual = result?;
                prop_assert_eq!(&actual.text, &text);
                prop_assert_eq!(actual.steps, 1);
                prop_assert_eq!(actual, futures::executor::block_on(executor.execute(input, &journal))?);
            } else {
                let error = result.expect_err("one-byte-under budget accepted");
                prop_assert!(error.to_string().contains("assistant output exceeds file limit"), "{error}");
            }
            prop_assert_eq!(provider.requests().len(), 1);
            prop_assert_eq!(provider.remaining(), 0);
        }
    }
}
