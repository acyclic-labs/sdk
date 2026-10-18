use super::run;
use crate::TestResult;
use serde_json::json;
use std::path::Path;

#[tokio::test(start_paused = true)]
async fn direct_swarm_dispatch_rejects_a_permanently_pending_response() -> TestResult {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/scenarios");
    let mut value =
        crate::scenario::load_value(&root, &root.join("swarm/prefetch-promotion.json"))?;
    run(value.clone()).await?;
    value.as_object_mut().ok_or("scenario")?.remove("driver");
    let case = serde_json::from_value(value)?;
    let error = run(super::Case::SwarmFault(
        Box::new(case),
        crate::swarm_wire::Fault::PermanentPending,
    ))
    .await
    .expect_err("an async driver owner hid a permanent response stall");
    assert!(error.to_string().contains("stalled: driver"), "{error}");
    Ok(())
}

#[tokio::test]
async fn plan_query_oracles_check_every_file_and_rendered_revision() -> TestResult {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/scenarios");
    for name in [
        "lifecycle",
        "multiple-raw-and-code",
        "filenames-and-paragraphs",
        "empty-stem",
    ] {
        let mut baseline =
            crate::scenario::load_value(&root, &root.join(format!("plan-query/{name}.json")))?;
        baseline
            .as_object_mut()
            .ok_or("missing scenario")?
            .remove("driver");
        super::super::plan_query::PlanQuery::parse(baseline)?
            .negative_oracles()
            .await?;
    }
    Ok(())
}

#[tokio::test]
async fn plans_oracles_reject_wrong_bytes_states_raw_fallback_and_missing_files() -> TestResult {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/scenarios");
    for name in ["lifecycle", "multiple-files", "malformed-raw"] {
        let mut baseline =
            crate::scenario::load_value(&root, &root.join(format!("plans/{name}.json")))?;
        baseline
            .as_object_mut()
            .ok_or("missing scenario")?
            .remove("driver");
        super::super::plans::Plans::parse(baseline)?
            .negative_oracles()
            .await?;
    }
    Ok(())
}

#[tokio::test]
async fn unbound_tool_oracles_reject_corrupted_names_and_arguments() -> TestResult {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/scenarios");
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
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/scenarios");
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
async fn driver_schemas_reject_unknown_and_inapplicable_fields() {
    let base = json!({"driver":"composer","prompt":"/plan",
        "expect":{"result":{"kind":"select","query":"plan"}}});
    assert!(run(base.clone()).await.is_ok());
    for (field, value) in [
        ("driver", json!("unknown")),
        ("driver", json!(null)),
        ("turns", json!([])),
        ("kind", json!("summary")),
        ("maximum_bytes", json!(-1)),
        ("commands", json!([{"scope":"project","typo":true}])),
        ("expect", json!({})),
        ("expect", json!({"result":{}})),
        ("expect", json!({"error":""})),
        ("expect", json!({"result":{"kind":"select"},"error":"x"})),
        ("expect", json!({"text":"wrong driver"})),
        (
            "expect",
            json!({"result":{"kind":"select"},"completions":[]}),
        ),
        ("expect", json!({"error":"x","mention_files":[]})),
    ] {
        let mut case = base.clone();
        case[field] = value;
        assert!(run(case).await.is_err(), "accepted invalid {field}");
    }
}

#[tokio::test]
async fn composer_oracles_reject_changed_results_completions_bytes_and_snapshots() -> TestResult {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/scenarios");
    let path = root.join("composer/mentions/pinned.json");
    run(crate::scenario::load_value(&root, &path)?).await?;
    let baseline = crate::scenario::load_value(&root, &path)?;
    for field in [
        "result",
        "mention_files",
        "files",
        "snapshot",
        "workspace",
        "error",
    ] {
        let mut case = baseline.clone();
        match field {
            "result" => case["expect"]["result"]["text"] = json!("wrong"),
            "mention_files" => case["expect"]["mention_files"] = json!(["after"]),
            "files" => case["expect"]["files"]["project/a.txt"] = json!("before"),
            "snapshot" => {
                case["after_capture"] = json!({});
            }
            "workspace" => case["workspace"] = json!("other"),
            "error" => {
                case["expect"] = json!({"error":"wrong expected failure"});
            }
            _ => return Err("unknown mutation".into()),
        }
        assert!(run(case).await.is_err(), "accepted wrong {field}");
    }
    for path in [
        "composer/builtins/completion-all.json",
        "composer/builtins/completion-empty.json",
    ] {
        let path = root.join(path);
        run(crate::scenario::load_value(&root, &path)?).await?;
        let mut case = crate::scenario::load_value(&root, &path)?;
        case["expect"]["completions"] = json!(["wrong"]);
        assert!(run(case).await.is_err(), "accepted wrong completion");
    }
    Ok(())
}

#[tokio::test]
async fn markdown_oracles_reject_coalescing_reordering_and_reasoning_leaks() -> TestResult {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/scenarios");
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

#[tokio::test]
async fn composer_byte_boundary_oracles_reject_character_counts_and_expansion_overflow()
-> TestResult {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/scenarios");
    for (name, maximum) in [
        ("ascii-exact", 4),
        ("ascii-over", 5),
        ("unicode-exact", 2),
        ("unicode-over", 6),
        ("builtin-exact", 4),
        ("builtin-over", 5),
        ("templates/appended-arguments-exact", 14),
        ("templates/appended-arguments-over", 15),
        ("templates/unicode-body-exact", 15),
        ("templates/unicode-body-over", 16),
    ] {
        let baseline =
            crate::scenario::load_value(&root, &root.join(format!("composer/limits/{name}.json")))?;
        run(baseline.clone()).await?;
        let mut changed = baseline;
        changed["maximum_bytes"] = json!(maximum);
        assert!(
            run(changed).await.is_err(),
            "accepted wrong bound for {name}"
        );
    }
    Ok(())
}

#[test]
fn dispatch_retains_only_the_selected_driver_frame() -> TestResult {
    let baseline = serde_json::from_slice::<serde_json::Value>(include_bytes!(
        "../../../harness-fuzz/corpus/client_diff/local-read"
    ))?;
    let direct = crate::local_execution::run(serde_json::from_value(baseline.clone())?);
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
