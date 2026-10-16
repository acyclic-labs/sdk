use super::{TestResult, drivers, scenario};
use proptest::{prelude::*, test_runner::TestRunner};
use serde_json::json;
use std::path::Path;

#[tokio::test]
async fn limit_oracles_reject_changed_allowances_counts_and_deltas() -> TestResult {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/scenarios");
    for (file, pointer, replacement) in [
        ("input-utf8-at", "/limits/render_bytes", json!(3)),
        ("output-utf8-at", "/limits/file_bytes", json!(3)),
        ("accumulated-at", "/turns/0/0/delta", json!("")),
        ("context-1-at", "/limits/context_messages", json!(1)),
        ("invalid/zero-file-bytes", "/limits/file_bytes", json!(1)),
        ("input-utf8-over", "/expect/request_count", json!(1)),
    ] {
        let path = root.join(format!("limits/bounds/{file}.json"));
        let mut value = scenario::load_value(&root, &path)?;
        drivers::run(value.clone()).await?;
        *value
            .pointer_mut(pointer)
            .ok_or("missing mutation target")? = replacement;
        assert!(
            drivers::run(value).await.is_err(),
            "accepted changed {file}"
        );
    }
    Ok(())
}

#[test]
fn streamed_output_budget_counts_utf8_bytes_across_every_delta() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    // Arbitrary Unicode chunks succeed at their complete UTF-8 byte length and
    // fail one byte below. Check both boundaries for every generated history.
    TestRunner::default()
        .run(
            &proptest::collection::vec(
                proptest::collection::vec(any::<char>(), 0..9)
                    .prop_map(|chars| chars.into_iter().collect::<String>()),
                1..9,
            ),
            |mut chunks| {
                chunks[0].insert_str(0, "xy");
                let text = chunks.concat();
                let mut events = chunks
                    .iter()
                    .map(|delta| json!({"type":"event","kind":"content","delta":delta}))
                    .collect::<Vec<_>>();
                events.push(json!({"type":"event","kind":"completed","metadata":null}));
                for at in [true, false] {
                    let expect = if at {
                        json!({"text":text,"request_count":1,"result":{"steps":1}})
                    } else {
                        json!({"error":"assistant output exceeds file limit","request_count":1})
                    };
                    let value = json!({"driver":"executor","prompt":"x","max_steps":1,
                        "limits":{"file_bytes":text.len() - usize::from(!at)},"replay":true,
                        "turns":[events],"expect":expect});
                    runtime
                        .block_on(drivers::run(value))
                        .map_err(|error| TestCaseError::fail(format!("at={at}: {error}")))?;
                }
                Ok(())
            },
        )
        .unwrap();
}
