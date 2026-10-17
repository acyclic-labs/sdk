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
    let model = Model::new(
        "test".into(),
        "request-reuse".into(),
        "1".into(),
        Value::Null,
    )
    .unwrap();
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
        operation_id: OperationId::from_bytes([3; 16]),
        input: ModelContent::Text(format!("current:{}", text[2])),
        selected_context: None,
        max_steps: 1,
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
                result = Some(runtime.block_on(async {
                    let execution = StockExecution::ordinary(&executor);
                    if old || restore_old_factory {
                        execution
                            .prepared_response_original(&journal, &input, 0, &[])
                            .await
                    } else {
                        execution.prepared_response(&journal, &input, 0, &[]).await
                    }
                }));
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
    prop_assert!(
        old.0 == new.0
            && old.1 == new.1
            && old.2 == new.2
            && old.3 == new.3
            && semantic(&old_trace) == semantic(&new_trace)
            && preparations(&old_trace) == if invalid { 0 } else { 3 }
            && preparations(&new_trace) == if invalid { 0 } else { 1 },
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

#[test]
fn preparation_accounting_measurements() {
    let text = std::array::from_fn(|_| String::new());
    for invalid in [false, true] {
        let [(old, _), (new, _)] = pair(&text, invalid, false);
        assert!(old.0 == new.0 && old.1 == new.1 && old.2 == new.2 && old.3 == new.3);
        eprintln!(
            "PERF_ACCOUNTING {}",
            serde_json::json!({"case": format!("preparation.{invalid}"), "allocations": differential::allocation_counts(new.4)})
        );
    }
}

fn exact(invalid: bool, restore: bool) -> TestCaseResult {
    let text = std::array::from_fn(|_| String::new());
    let [(old, _), (new, _)] = pair(&text, invalid, restore);
    let expected: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("src/executor/tests/preparation_expected.json"),
        )
        .unwrap(),
    )
    .unwrap();
    let expected = &expected[format!("preparation.{invalid}")];
    let actual = serde_json::json!(differential::allocation_counts(new.4));
    prop_assert!(
        old.0 == new.0 && old.1 == new.1 && old.2 == new.2 && actual == *expected,
        "exact preparation allocation differs: actual={actual}, expected={expected}"
    );
    Ok(())
}

#[test]
fn request_preparation_has_exact_physical_allocations() {
    for invalid in [false, true] {
        exact(invalid, false).unwrap();
    }
}

#[test]
fn restoring_redundant_preparations_fails_the_exact_allocation_guard() {
    let error = exact(false, true).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("exact preparation allocation differs")
    );
}
