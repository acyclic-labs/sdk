use crate::{TestResult, drivers, scenario};
use serde_json::{Value, json};
use std::path::Path;

async fn baseline(path: &str) -> TestResult<Value> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/scenarios");
    let case = scenario::load_value(&root, &root.join(path))?;
    drivers::run(case.clone()).await?;
    Ok(case)
}

#[tokio::test]
async fn instruction_oracles_reject_wrong_order_roots_bytes_and_missing_discovery() -> TestResult {
    let baseline = baseline("discovery/instructions/ancestors.json").await?;
    for field in ["active", "order", "files", "paths", "missing", "forbidden"] {
        let mut case = baseline.clone();
        match field {
            "active" => case["discovery"]["instructions"] = json!([{}]),
            "order" => {
                case["discovery"]["policy"]["instruction_names"] = json!(["CLAUDE.md", "AGENTS.md"])
            }
            "files" => case["expect"]["request_files"][0][0] = json!("wrong bytes"),
            "paths" => {
                case["expect"]["requests"][0]["messages"][0]["content"]["file"]["path"] =
                    json!("wrong.md")
            }
            "missing" => {
                case.as_object_mut()
                    .ok_or("missing case object")?
                    .remove("discovery");
            }
            // Instructions are scoped user-role frames (#849): forbidding that
            // frame must reject the discovered request.
            "forbidden" => {
                case["expect"]["not_requests"] = json!([{"messages":[{"role":"user","content":{"policy":{"project_instructions":{}}}}]}])
            }
            _ => return Err("unknown mutation".into()),
        }
        assert!(drivers::run(case).await.is_err(), "accepted wrong {field}");
    }
    Ok(())
}

#[tokio::test]
async fn discovery_oracles_distinguish_pinned_and_reloaded_generations() -> TestResult {
    for path in [
        "discovery/instructions/pinned.json",
        "discovery/instructions/explicit.json",
    ] {
        let baseline = baseline(path).await?;
        for field in ["update", "reload", "files"] {
            let mut case = baseline.clone();
            match field {
                "update" => case["discovery"]["after_capture"] = json!({}),
                "reload" => case["discovery"]["reload"] = json!("next_request"),
                "files" => case["expect"]["files"]["AGENTS.md"] = json!("Old rules"),
                _ => return Err("unknown mutation".into()),
            }
            assert!(
                drivers::run(case).await.is_err(),
                "accepted wrong {field} for {path}"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn skill_oracles_reject_missing_metadata_wrong_descriptions_and_body_injection() -> TestResult
{
    let initial = baseline("discovery/skills/lazy.json").await?;
    for field in ["description", "source", "disabled", "forbidden"] {
        let mut case = initial.clone();
        match field {
            "description" => {
                case["expect"]["requests"][0]["messages"][0]["content"]["$json"][0]["description"] =
                    json!("wrong description")
            }
            "source" => {
                case["expect"]["requests"][0]["messages"][0]["content"]["$json"][0]["source"]["path"] =
                    json!("wrong/SKILL.md")
            }
            "disabled" => case["discovery"]["policy"]["skill_name"] = Value::Null,
            "forbidden" => {
                case["expect"]["not_requests"] = json!([{"messages":[{"role":"system"}]}])
            }
            _ => return Err("unknown mutation".into()),
        }
        assert!(drivers::run(case).await.is_err(), "accepted wrong {field}");
    }
    let mut pinned = baseline("discovery/skills/pinned-description.json").await?;
    pinned["discovery"]["reload"] = json!("next_request");
    assert!(
        drivers::run(pinned).await.is_err(),
        "accepted reloaded metadata as pinned"
    );
    Ok(())
}

#[tokio::test]
async fn skill_body_guards_reject_plain_wrapped_and_structured_text() -> TestResult {
    let initial = baseline("discovery/skills/lazy.json").await?;
    let file = initial["seed"]["skills/inspect/SKILL.md"]
        .as_str()
        .ok_or("missing skill")?;
    for content in [
        json!("SECRET BODY"),
        json!(format!("before {file} after")),
        json!({"kind":"text","text":"before SECRET BODY after"}),
        json!([{"kind":"text","text":"before SECRET BODY after"}]),
    ] {
        let mut injected = initial.clone();
        injected["context"] = json!([{"role":"system","content":content}]);
        let mut observable = injected.clone();
        observable["expect"]["not_requests"] = json!([]);
        observable["expect"]["requests"][0]["messages"] = json!([
            {"role":"system","content":{"$json":[{"name":"inspect"}]}},
            {"role":"system","content":content},
            {"role":"user","content":"Inspect files"}
        ]);
        // With the guard removed, the exact injected content reaches the recorded request.
        drivers::run(observable).await?;
        let error = drivers::run(injected)
            .await
            .expect_err("accepted leaked skill body");
        assert!(error.to_string().contains("forbidden request"), "{error}");
    }
    Ok(())
}

#[tokio::test]
async fn discovery_errors_reject_ignored_bounds_and_unused_turns() -> TestResult {
    let baseline = baseline("discovery/errors/zero-entries.json").await?;
    let mut changed = baseline.clone();
    changed["discovery"]["limits"]["entries"] = json!(4096);
    assert!(
        drivers::run(changed).await.is_err(),
        "ignored discovery bounds"
    );
    let mut changed = baseline;
    changed["turns"] = json!(["unused"]);
    assert!(
        drivers::run(changed).await.is_err(),
        "ignored unused model script"
    );
    Ok(())
}
