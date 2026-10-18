use super::local_case::{ExpectedOutcome, LocalCase};

proptest::proptest! {
    #[test]
    fn local_case_codec_preserves_script_files_and_wire_expectations(
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
        let name = if pinned { "read@portable-6" } else { "read" };
        let script = vec![ScriptedTurn::Events(vec![ScriptedEvent::Event(ModelEvent::ToolCall(
            ToolCall::new("call".into(), name.into(), json!({"file":"seed","range":{"start":2,"end":4},"text":text})),
        ))]), ScriptedTurn::Text(text.clone())];
        let reference = json!({"immutable":"generation", "path":"seed", "text":text});
        let references = std::collections::BTreeMap::from([("seed".into(), reference.clone())]);
        let bound = super::local_case::bind_seed_files(script.clone(), &references).unwrap();
        let mut expected = serde_json::to_value(&script)?;
        expected[0][0]["arguments"]["file"] = reference;
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
fn local_schema_requires_outcome_frames_files_and_known_fields() -> crate::TestResult {
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
    for key in ["files", "frames", "statuses"] {
        let mut broken = baseline.clone();
        broken["expect"][key] = if key == "files" { json!({}) } else { json!([]) };
        assert!(
            serde_json::from_value::<LocalCase>(broken)?
                .validate()
                .is_err()
        );
    }
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
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/scenarios");
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

proptest::proptest! {
    #[test]
    fn local_wire_oracle_requires_one_bound_root_outcome_in_order(id in proptest::array::uniform16(proptest::num::u8::ANY)) {
        use serde_json::json;
        let operation = acyclic_harness::OperationId::from_bytes(id);
        let expected = vec![json!({"kind":"declared"}),json!({"kind":"admitted"}),json!({"kind":"completed"})];
        let file = json!({"volume":{"provider":{"namespace":"fixture","family":"filesystem","version":"1"},
            "id":"project","class":"project","owner":{"kind":"project","id":"fixture"}},
            "path":"result","version":"v1","descriptor":{"sha256":vec![0;32],"byte_length":0,"media_type":"text/plain"},"display_name":"result"});
        for outcome in [ExpectedOutcome::Succeeded,ExpectedOutcome::Indeterminate] {
            let completed = match outcome {
                ExpectedOutcome::Succeeded => json!({"Succeeded":file}),
                ExpectedOutcome::Indeterminate => json!({"Indeterminate":{"operation_id":operation}}),
            };
            let frames = vec![json!({"kind":"declared"}),json!({"kind":"admitted"}),json!({"kind":"completed","operation_id":operation,"outcome":completed,"fence":null})];
            let check = |frames:&[serde_json::Value]| super::drivers::check_frames(&expected,frames,operation,outcome);
            proptest::prop_assert!(check(&frames).is_ok());
            let mut duplicate = frames.clone();duplicate.push(frames[2].clone());
            proptest::prop_assert!(check(&duplicate).is_err());
            proptest::prop_assert!(check(&frames[..2]).is_err());
            let mut failed = frames.clone();failed[2]["outcome"]=json!({"Failed":{"message":"expected failure"}});
            proptest::prop_assert!(check(&failed).is_err());
            let mut foreign = id;foreign[0]^=1;
            proptest::prop_assert!(super::drivers::check_frames(&expected,&frames,acyclic_harness::OperationId::from_bytes(foreign),outcome).is_err());
            let other = if outcome == ExpectedOutcome::Succeeded {ExpectedOutcome::Indeterminate} else {ExpectedOutcome::Succeeded};
            proptest::prop_assert!(super::drivers::check_frames(&expected,&frames,operation,other).is_err());
            if outcome == ExpectedOutcome::Indeterminate {
                let mut unbound = frames.clone();
                unbound[2]["outcome"]["Indeterminate"]["operation_id"] = json!(acyclic_harness::OperationId::from_bytes(foreign));
                proptest::prop_assert!(check(&unbound).is_err());
            }
            let mut reordered = frames;reordered.swap(0,1);
            proptest::prop_assert!(check(&reordered).is_err());
        }
    }
}

proptest::proptest! {
    // A status view must preserve the exact ordered publication list and bind
    // every state to its admitted task. Extra, missing and reordered states fail.
    #[test]
    fn status_oracle_binds_identity_attention_and_every_publication(
        id in proptest::array::uniform16(proptest::num::u8::ANY),
    ) {
        use serde_json::json;
        let task = acyclic_harness::TaskId::from_bytes(id);
        let expected = vec![json!({"state":"pending","attention":false}),json!({"state":"completed","attention":true})];
        let actual = vec![json!({"task":task,"state":"pending","attention":false}),json!({"task":task,"state":"completed","attention":true})];
        proptest::prop_assert!(super::local_admission::check_statuses(&expected,&actual,task).is_ok());
        proptest::prop_assert!(super::local_admission::check_statuses(&expected,&actual[..1],task).is_err());
        let mut duplicate = actual.clone();duplicate.insert(1,actual[0].clone());
        proptest::prop_assert!(super::local_admission::check_statuses(&expected,&duplicate,task).is_err());
        let mut reordered = actual.clone();reordered.swap(0,1);
        proptest::prop_assert!(super::local_admission::check_statuses(&expected,&reordered,task).is_err());
        let mut foreign = id;foreign[0]^=1;
        proptest::prop_assert!(super::local_admission::check_statuses(&expected,&actual,acyclic_harness::TaskId::from_bytes(foreign)).is_err());
        for (field,value) in [("attention",json!(false)),("state",json!("pending")),("task",json!(null))] {
            let mut broken = actual.clone();broken[1][field]=value;
            proptest::prop_assert!(super::local_admission::check_statuses(&expected,&broken,task).is_err());
        }
    }
}

#[tokio::test]
async fn bash_family_rejects_corrupted_stdout_with_the_same_persisted_bytes() -> crate::TestResult {
    reject_changed_command(
        "default-tools/bash/stdout.json",
        "printf changed > plans/a.md; printf CORRUPTED",
    )
    .await
}

#[tokio::test]
async fn fs_family_rejects_executing_a_rejected_command() -> crate::TestResult {
    reject_changed_command("default-tools/fs/whitespace.json", "add plans/a.md").await
}

#[tokio::test]
async fn fs_family_rejects_bypassing_schema_rejection() -> crate::TestResult {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/scenarios");
    let baseline = super::scenario::load_value(&root, &root.join("default-tools/fs/empty.json"))?;
    super::drivers::run(baseline.clone()).await?;
    let mut broken = baseline;
    broken["turns"][0][0]["arguments"]["command"] = " \t\n".into();
    let error = super::drivers::run(broken)
        .await
        .expect_err("parser dispatch passed unchanged schema-rejection expectations");
    assert!(
        error.to_string().contains("scripted model turns"),
        "{error}"
    );
    Ok(())
}

async fn reject_changed_command(path: &str, command: &str) -> crate::TestResult {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/scenarios");
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

proptest::proptest! {
    #[test]
    fn task_output_gap_is_rejected_even_when_concatenated_bytes_are_unchanged(
        id in proptest::array::uniform16(proptest::num::u8::ANY),
    ) {
        use acyclic_harness::task_volume::{FileKey,TaskFile,ToolFile};
        let task=acyclic_harness::TaskId::from_bytes(id);
        let operation=acyclic_harness::OperationId::from_bytes(id);
        let (files,calls,results)=super::local_tool_files_tests::fixture(task,operation,"out","err")
            .map_err(|error| proptest::test_runner::TestCaseError::fail(error.to_string()))?;
        for stdout in [true,false] {
            let mut broken=files.clone();
            let original=broken.keys().find(|key| matches!(key,
                FileKey::Task {file:TaskFile::Tool {file:ToolFile::Stdout(0),..},..} if stdout
            ) || matches!(key,
                FileKey::Task {file:TaskFile::Tool {file:ToolFile::Stderr(0),..},..} if !stdout
            )).unwrap().clone();
            let mut value=broken.remove(&original).unwrap();
            let mut shifted=original;
            let FileKey::Task {file:TaskFile::Tool {file,..},..}=&mut shifted else {unreachable!()};
            *file=if stdout {ToolFile::Stdout(1)} else {ToolFile::Stderr(1)};
            value.key=shifted.clone();
            broken.insert(shifted,value);
            let error=super::local_tool_files_check::check(&broken,task,operation,&calls,&results)
                .expect_err("a sequence gap with identical payloads passed");
            proptest::prop_assert!(error.to_string().contains("sequence has a gap"));
        }
    }
}
