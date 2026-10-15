use super::run;
use crate::TestResult;
use serde_json::json;
use std::path::Path;

#[tokio::test]
async fn plans_oracles_reject_wrong_bytes_states_raw_fallback_and_missing_files() -> TestResult {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/scenarios");
    for name in ["lifecycle", "multiple-files", "malformed-raw"] {
        let baseline =
            crate::scenario::load_value(&root, &root.join(format!("plans/{name}.json")))?;
        run(baseline.clone()).await?;
        let path = baseline["expect"]
            .as_object()
            .ok_or("missing files")?
            .keys()
            .next()
            .ok_or("empty files")?
            .clone();
        for field in ["bytes", "tasks", "missing"] {
            let mut case = baseline.clone();
            match field {
                "bytes" => case["expect"][&path]["markdown"] = json!("wrong bytes"),
                "tasks" => {
                    case["expect"][&path]["tasks"] = json!([{"state":"[ ]","text":"wrong task"}])
                }
                "missing" => {
                    case["expect"]
                        .as_object_mut()
                        .ok_or("missing files")?
                        .remove(&path);
                }
                _ => return Err("unknown plan mutation".into()),
            }
            assert!(run(case).await.is_err(), "accepted wrong {name} {field}");
        }
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
