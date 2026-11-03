use super::*;
use acyclic_test_support::differential;
use proptest::prelude::*;
use proptest::test_runner::TestCaseResult;

type State = (Journal, StockExecutor, TurnInput, Arc<AtomicUsize>);
type Output = (
    std::result::Result<(ModelRequest, Vec<u8>, [u8; 32]), String>,
    Vec<u8>,
    std::collections::BTreeMap<String, (FileRef, Vec<u8>)>,
    usize,
    acyclic_test_support::AllocationInfo,
);

async fn initialize(text: &[String; 3], invalid: bool) -> State {
    let journal = Journal::default();
    let (provider, calls) = counted_model(131_072, None, [], []);
    let model = Model::new("test".into(), "request-reuse".into(), "1".into()).unwrap();
    let messages = vec![
        ModelMessage {
            role: ModelRole::System,
            content: ModelContent::Text(format!("history:{}", text[0])),
        },
        ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Text(format!("prior:{}", text[1])),
        },
    ];
    let mut executor = StockExecutor::new(
        model,
        provider,
        ContextPipeline::new([Arc::new(crate::context::SourceStage::new(
            "history",
            "1",
            Arc::new(crate::context::Context {
                messages,
                ..Default::default()
            }),
            crate::context::ContextPlacement::Prepend,
        )) as Arc<dyn crate::context::ContextStage>]),
        ToolRegistry::default(),
    )
    .with_max_output_tokens(512)
    .unwrap();
    let input = TurnInput {
        max_steps: None,
        operation_id: OperationId::from_bytes([3; 16]),
        input: ModelContent::Text(format!("current:{}", text[2])),
        selected_context: None,
    };
    StockExecution::ordinary(&executor)
        .ensure_started(&journal, &input)
        .await
        .unwrap();
    if invalid {
        executor.limits.context_messages = 1;
    }
    (journal, executor, input, calls)
}

fn pair(
    text: &[String; 3],
    invalid: bool,
    restore_old_factory: bool,
) -> [(Output, Vec<(&'static str, u64)>); 2] {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            crate::executor::json::baseline::LEGACY.set(false);
        }
    }
    let _reset = Reset;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let mut first = true;
    differential::pair::<_, _, (&'static str, u64)>(
        || {
            crate::executor::json::baseline::LEGACY.set(first);
            first = false;
            runtime.block_on(initialize(text, invalid))
        },
        |(journal, executor, input, calls), old| {
            crate::executor::json::baseline::LEGACY.set(old);
            let mut result = None;
            let allocations = acyclic_test_support::measure(|| {
                // Pinned here so the runtime never boxes it: the measured
                // allocations must not depend on the state machine's layout.
                let future = std::pin::pin!(async {
                    let execution = StockExecution::ordinary(&executor);
                    if old || restore_old_factory {
                        execution
                            .prepared_response_original(&journal, &input, 0, &[])
                            .await
                    } else {
                        execution
                            .prepared_response(&journal, &input, 0, &[], None)
                            .await
                    }
                });
                result = Some(runtime.block_on(future));
            });
            let result = result
                .unwrap()
                .map(|prepared| {
                    (
                        prepared.request().clone(),
                        prepared.bytes().to_vec(),
                        prepared.request_digest(),
                    )
                })
                .map_err(|error| format!("{error:?}"));
            // Compare every persisted field and byte. No IDs, digests or payloads
            // are normalized: the two independent fixtures use the same input IDs.
            let records = serde_json::to_vec(&*journal.0.lock().unwrap()).unwrap();
            let staged = journal
                .1
                .lock()
                .unwrap()
                .iter()
                .map(|(key, (file, bytes))| (key.clone(), (file.clone(), bytes.to_vec())))
                .collect();
            (
                result,
                records,
                staged,
                calls.load(Ordering::SeqCst),
                allocations,
            )
        },
    )
}

fn check(text: &[String; 3], invalid: bool, restore: bool) -> TestCaseResult {
    let [(old, old_trace), (new, new_trace)] = pair(text, invalid, restore);
    let semantic = |trace: &Vec<(&'static str, u64)>| {
        trace
            .iter()
            .copied()
            .filter(|(op, _)| op.starts_with("journal ") || *op == "count tokens")
            .collect::<Vec<_>>()
    };
    let preparations = |trace: &Vec<(&'static str, u64)>| {
        trace
            .iter()
            .filter(|(op, _)| *op == "prepare request")
            .count()
    };
    let publications = new_trace
        .iter()
        .filter(|(op, _)| *op == "journal stage")
        .count();
    prop_assert!(
        old.0 == new.0
            && old.1 == new.1
            && old.2 == new.2
            && old.3 == new.3
            && (invalid || new.4.count_total < old.4.count_total)
            && semantic(&old_trace) == semantic(&new_trace)
            && preparations(&old_trace) == if invalid { 0 } else { 4 }
            && preparations(&new_trace) == if invalid { 0 } else { 1 }
            && publications == if invalid { 0 } else { 1 },
        "independent preparation differs or repeats removed work: old={old:?}, new={new:?}, old_trace={old_trace:?}, new_trace={new_trace:?}"
    );
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn preparations_keep_full_requests_journal_bytes_errors_and_operation_trace(
        text in prop::array::uniform3("[a-z0-9\\x00é🦀]{0,16}"),
    ) {
        for invalid in [false, true] { check(&text, invalid, false)?; }
    }
}

#[test]
fn restoring_redundant_request_preparations_is_detected() {
    let text = std::array::from_fn(|_| String::new());
    check(&text, false, false).unwrap();
    let error = check(&text, false, true).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("independent preparation differs or repeats removed work")
    );
}
