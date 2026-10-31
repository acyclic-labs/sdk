use super::local_case::LocalCase;

proptest::proptest! {
    #[test]
    fn local_case_codec_preserves_script_and_file_expectations(
        text in ".{0,64}", deleted in proptest::bool::ANY, indeterminate in proptest::bool::ANY,
    ) {
        for bytes in [
            include_bytes!("../../../harness-fuzz/corpus/client_diff/local-harness").as_slice(),
            include_bytes!("../../../harness-fuzz/corpus/client_diff/local-remove").as_slice(),
            include_bytes!("../../../harness-fuzz/corpus/client_diff/local-read").as_slice(),
            include_bytes!("../../../harness-fuzz/corpus/client_diff/local-indeterminate").as_slice(),
        ] {
            let mut value: serde_json::Value = serde_json::from_slice(bytes)?;
            if value["turns"][0][0]["name"] == "write" {
                value["turns"][0][0]["arguments"]["text"] = text.clone().into();
            }
            value["expect"]["files"]["plans/a.md"] = if deleted { serde_json::Value::Null } else { text.clone().into() };
            if indeterminate {
                value["expect"]["text"] = serde_json::Value::Null;
                value["expect"]["outcome"] = "indeterminate".into();
            }
            let case: LocalCase = serde_json::from_value(value)?;
            let encoded = serde_json::to_vec(&case)?;
            let decoded: LocalCase = serde_json::from_slice(&encoded)?;
            proptest::prop_assert_eq!(serde_json::to_value(case)?, serde_json::to_value(decoded)?);
        }
    }
}

proptest::proptest! {
    #[test]
    fn seeded_reference_binding_preserves_other_arguments_and_text(
        text in ".{0,48}", pinned in proptest::bool::ANY,
    ) {
        use acyclic_harness::{model::{ModelEvent, ToolCall}, test_support::{ScriptedEvent, ScriptedTurn}};
        use serde_json::json;
        let name = if pinned { "read@portable-7" } else { "read" };
        let script = vec![ScriptedTurn::Events(vec![ScriptedEvent::Event(ModelEvent::ToolCall(
            ToolCall::new("call".into(), name.into(), json!({"path":"seed","version":"seed","range":{"start":2,"end":4},"text":text})),
        ))]), ScriptedTurn::Text(text.clone())];
        let reference = json!({"immutable":"generation", "path":"seed", "text":text});
        let references = std::collections::BTreeMap::from([("seed".into(), reference.clone())]);
        let bound = super::local_case::bind_seed_files(script.clone(), &references).unwrap();
        let mut expected = serde_json::to_value(&script)?;
        expected[0][0]["arguments"]["version"] = serde_json::to_string(&reference)?.into();
        proptest::prop_assert_eq!(serde_json::to_value(bound)?, expected);
        proptest::prop_assert!(super::local_case::bind_seed_files(script, &Default::default()).is_err());
    }

    #[test]
    fn model_result_oracle_rejects_loss_duplicates_identity_and_content(text in ".{0,48}") {
        use serde_json::json;
        let result = json!({"name":"read", "content":{"kind":"json","value":text}});
        let expected = std::collections::BTreeMap::from([("call".into(), result.clone())]);
        proptest::prop_assert!(super::drivers::check_results(&expected, &expected).is_ok());
        let mut broken = expected.clone();
        broken.get_mut("call").unwrap()["content"]["value"] = json!(format!("{text}\0"));
        proptest::prop_assert!(super::drivers::check_results(&expected, &broken).is_err());
        proptest::prop_assert!(super::drivers::check_results(&expected, &Default::default()).is_err());
        broken = expected.clone(); broken.insert("unexpected".into(), result);
        proptest::prop_assert!(super::drivers::check_results(&expected, &broken).is_err());
        broken = expected.clone(); broken.get_mut("call").unwrap()["name"] = "write".into();
        proptest::prop_assert!(super::drivers::check_results(&expected, &broken).is_err());
        use acyclic_harness::model::{Model, ModelContent, ModelContentPart, ModelMessage, ModelRequest, ModelRole, ToolResultContent};
        let part = ModelContentPart::ToolResult {call_id:"call".into(), name:"read".into(), content:ToolResultContent::Json {value:json!(text)}};
        let request = ModelRequest {model:Model::new("scripted".into(),"local".into(),"1".into(),json!(null))?,
            messages:vec![ModelMessage {role:ModelRole::Tool,content:ModelContent::Parts(vec![part.clone(),part])}], tools:vec![], max_output_tokens:None};
        proptest::prop_assert!(super::drivers::model_results(&request).is_err());
    }
}

#[test]
fn local_schema_requires_outcome_files_and_known_fields() -> crate::TestResult {
    use serde_json::json;
    let baseline: serde_json::Value = serde_json::from_slice(include_bytes!(
        "../../../harness-fuzz/corpus/client_diff/local-harness"
    ))?;
    let case: LocalCase = serde_json::from_value(baseline.clone())?;
    case.validate()?;
    for (key, value) in [
        ("prompt", json!("")),
        ("turns", json!([])),
        ("unknown", json!(true)),
    ] {
        let mut broken = baseline.clone();
        broken[key] = value;
        assert!(
            serde_json::from_value::<LocalCase>(broken)
                .map_or(true, |case| case.validate().is_err())
        );
    }
    let mut broken = baseline.clone();
    broken["expect"]["files"] = json!({});
    assert!(
        serde_json::from_value::<LocalCase>(broken)?
            .validate()
            .is_err()
    );
    for (outcome, text) in [
        (json!("succeeded"), json!(null)),
        (json!("indeterminate"), json!("done")),
        (json!("unknown"), json!(null)),
    ] {
        let mut broken = baseline.clone();
        broken["expect"]["outcome"] = outcome;
        broken["expect"]["text"] = text;
        assert!(
            serde_json::from_value::<LocalCase>(broken)
                .map_or(true, |case| case.validate().is_err())
        );
    }
    Ok(())
}

#[tokio::test]
async fn read_family_rejects_a_valid_but_wrong_selected_range() -> crate::TestResult {
    let root = acyclic_test_support::source_path("rust/crates/harness").join("tests/scenarios");
    let baseline =
        super::scenario::load_value(&root, &root.join("default-tools/read/middle.json"))?;
    super::drivers::run(baseline.clone()).await?;
    let mut broken = baseline;
    broken["turns"][0][0]["arguments"]["range"]["start"] = 2.into();
    let error = super::drivers::run(broken)
        .await
        .expect_err("accepted the wrong selected bytes");
    assert!(error.to_string().contains("model tool result"), "{error}");
    Ok(())
}

#[tokio::test]
async fn bash_family_rejects_corrupted_stdout_with_the_same_persisted_bytes() -> crate::TestResult {
    reject_changed_command(
        "default-tools/bash/stdout.json",
        if cfg!(windows) {
            "[IO.File]::WriteAllText('plans/a.md', 'changed', [Text.UTF8Encoding]::new($false)); [Console]::Write('CORRUPTED')"
        } else {
            "printf changed > plans/a.md; printf CORRUPTED"
        },
    )
    .await
}

#[tokio::test]
async fn fs_family_rejects_executing_a_rejected_command() -> crate::TestResult {
    reject_changed_command("default-tools/fs/whitespace.json", "add plans/a.md").await
}

#[tokio::test]
async fn fs_family_rejects_bypassing_schema_rejection() -> crate::TestResult {
    let root = acyclic_test_support::source_path("rust/crates/harness").join("tests/scenarios");
    let baseline = super::scenario::load_value(&root, &root.join("default-tools/fs/empty.json"))?;
    super::drivers::run(baseline.clone()).await?;
    let mut broken = baseline;
    broken["turns"][0][0]["arguments"]["command"] = " \t\n".into();
    let error = super::drivers::run(broken)
        .await
        .expect_err("parser dispatch passed unchanged schema-rejection expectations");
    assert!(error.to_string().contains("model tool result"), "{error}");
    Ok(())
}

async fn reject_changed_command(path: &str, command: &str) -> crate::TestResult {
    let root = acyclic_test_support::source_path("rust/crates/harness").join("tests/scenarios");
    let baseline = super::scenario::load_value(&root, &root.join(path))?;
    super::drivers::run(baseline.clone()).await?;
    let mut broken = baseline;
    broken["turns"][0][0]["arguments"]["command"] = command.into();
    let error = super::drivers::run(broken)
        .await
        .expect_err("a different command passed unchanged family expectations");
    assert!(error.to_string().contains("model tool result"), "{error}");
    Ok(())
}
