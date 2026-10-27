use super::run;
use crate::TestResult;
use serde_json::json;

#[tokio::test]
async fn unbound_tool_oracles_reject_corrupted_names_and_arguments() -> TestResult {
    let root = acyclic_test_support::source_path("rust/crates/harness").join("tests/scenarios");
    for path in [
        "plain",
        "pinned",
        "array-arguments",
        "file-facade",
        "multiple-calls",
    ] {
        let baseline =
            crate::scenario::load_value(&root, &root.join(format!("errors/unbound/{path}.json")))?;
        run(baseline.clone()).await?;
        let calls = baseline["turns"][0]
            .as_array()
            .ok_or("missing events")?
            .iter()
            .filter(|event| event["kind"] == "tool_call")
            .count();
        for index in 0..calls {
            for (field, value) in [
                ("name", json!("corrupted")),
                ("arguments", json!({"corrupted":true})),
            ] {
                let mut case = baseline.clone();
                case["turns"][0][index][field] = value;
                assert!(run(case).await.is_err(), "accepted {path}[{index}].{field}");
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn unbound_tool_oracles_reject_inserted_arguments() -> TestResult {
    let root = acyclic_test_support::source_path("rust/crates/harness").join("tests/scenarios");
    for (path, arguments) in [
        (
            "array-arguments",
            json!([1, {"path":"unbound-output.txt"}, "injected"]),
        ),
        (
            "array-arguments",
            json!([1, {"path":"unbound-output.txt", "injected":true}]),
        ),
        ("plain", json!({"name":"No route", "injected":true})),
    ] {
        let baseline =
            crate::scenario::load_value(&root, &root.join(format!("errors/unbound/{path}.json")))?;
        run(baseline.clone()).await?;
        let mut case = baseline;
        case["turns"][0][0]["arguments"] = arguments;
        assert!(
            run(case).await.is_err(),
            "accepted inserted arguments in {path}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn markdown_oracles_reject_coalescing_reordering_and_reasoning_leaks() -> TestResult {
    let root = acyclic_test_support::source_path("rust/crates/harness").join("tests/scenarios");
    for (name, mutations) in [
        (
            "fence-split",
            &[
                "coalesced",
                "reversed",
                "missing",
                "completion",
                "delta",
                "metadata",
                "final",
                "insert-empty",
                "insert-reasoning",
            ][..],
        ),
        (
            "empty-deltas",
            &["missing", "insert-empty", "insert-reasoning"][..],
        ),
        (
            "reasoning",
            &[
                "leaked",
                "missing",
                "reversed",
                "insert-empty",
                "insert-reasoning",
            ][..],
        ),
    ] {
        let baseline =
            crate::scenario::load_value(&root, &root.join(format!("markdown/{name}.json")))?;
        run(baseline.clone()).await?;
        for mutation in mutations {
            let mut case = baseline.clone();
            match *mutation {
                "coalesced" => {
                    let events = case["turns"][0].as_array().ok_or("events")?;
                    let text: String = events
                        .iter()
                        .filter_map(|event| event["delta"].as_str())
                        .collect();
                    case["turns"][0] = json!([
                        {"type":"event","kind":"content","delta":text},
                        {"type":"event","kind":"completed","metadata":null}
                    ]);
                }
                "reversed" => case["turns"][0].as_array_mut().ok_or("events")?.swap(0, 1),
                "missing" => {
                    case["turns"][0].as_array_mut().ok_or("events")?.remove(0);
                }
                "completion" => {
                    case["turns"][0].as_array_mut().ok_or("events")?.pop();
                }
                "delta" => case["turns"][0][0]["delta"] = json!("corrupted"),
                "leaked" => case["turns"][0][0]["kind"] = json!("content"),
                "metadata" => {
                    case["turns"][0]
                        .as_array_mut()
                        .ok_or("events")?
                        .last_mut()
                        .ok_or("completion")?["metadata"] = json!({"injected":true});
                }
                "final" => case["expect"]["text"] = json!("corrupted"),
                "insert-empty" | "insert-reasoning" => {
                    case["turns"][0].as_array_mut().ok_or("events")?.insert(
                        1,
                        json!({"type":"event", "kind":if *mutation == "insert-empty" {"content"} else {"reasoning"}, "delta":""}),
                    );
                }
                _ => return Err("unknown Markdown mutation".into()),
            }
            assert!(run(case).await.is_err(), "accepted {name}: {mutation}");
        }
    }
    Ok(())
}

#[test]
fn dispatch_retains_only_the_selected_driver_frame() -> TestResult {
    let baseline = serde_json::from_slice::<serde_json::Value>(include_bytes!(
        "../../../harness-fuzz/corpus/client_diff/local-read"
    ))?;
    let direct = super::local_execution::run(serde_json::from_value(baseline.clone())?);
    let dispatched = run(baseline);
    let bytes = std::mem::size_of_val(&dispatched);
    let inline = std::mem::size_of_val(&direct);
    println!("dispatch future: {bytes} bytes; inline native driver: {inline} bytes");
    // A dispatch frame holds its JSON recipe and a pointer to the selected driver,
    // not that driver's nested filesystem futures. Bound is independent of inputs.
    let bounded = |bytes| bytes <= 1024;
    assert!(
        bounded(bytes),
        "every scenario retained a large driver frame: {bytes}"
    );
    // Former implementation: inline the real native driver without changing cases.
    assert!(
        !bounded(inline),
        "the inline-driver negative control escaped the bound"
    );
    Ok(())
}
