use super::scenario::Scenario;

#[test]
fn scenarios_reject_typos_and_vacuous_results() {
    for input in [
        r#"{"prompt":"x","turns":[],"expect":{}}"#,
        r#"{"prompt":"x","turns":[],"expect":{"text":"x","error":"x"}}"#,
        r#"{"prompt":"x","turns":[],"expect":{"text":"x","typo":1}}"#,
        r#"{"prompt":"x","turns":[],"typo":1,"expect":{"text":"x"}}"#,
        r#"{"prompt":"x","turns":[],"expect":{"error":""}}"#,
        r#"{"prompt":"x","turns":[],"replay":true,"expect":{"error":"x"}}"#,
        r#"{"prompt":"x","turns":[],"expect":{"error":"x","result":{}}}"#,
        r#"{"prompt":"x","turns":[],"resume":[""],"expect":{"text":"x"}}"#,
        r#"{"prompt":"x","turns":[],"capacity":{"context_tokens":60,"output_tokens":10,"typo":1},"expect":{"text":"x"}}"#,
        r#"{"prompt":"x","turns":[],"compaction":{"kind":"typo"},"expect":{"text":"x"}}"#,
        r#"{"prompt":"x","turns":[],"message_tokens":-1,"expect":{"text":"x"}}"#,
        r#"{"summary":true,"prompt":"x","turns":[],"expect":{"text":"x"}}"#,
        r#"{"kind":"summary","prompt":"x","instruction":"x","turns":[],"expect":{"text":"x"}}"#,
        r#"{"kind":"summary","instruction":"x","max_steps":16,"turns":[],"expect":{"text":"x"}}"#,
        r#"{"kind":"summary","instruction":"x","max_steps":null,"turns":[],"expect":{"text":"x"}}"#,
        r#"{"kind":"summary","instruction":"x","discovery":null,"turns":[],"expect":{"text":"x"}}"#,
        r#"{"prompt":"x","discovery":{"typo":true},"turns":[],"expect":{"text":"x"}}"#,
        r#"{"prompt":"x","discovery":{"instructions":[{"typo":true}]},"turns":[],"expect":{"text":"x"}}"#,
        r#"{"prompt":"x","discovery":{"reload":"typo"},"turns":[],"expect":{"text":"x"}}"#,
        r#"{"prompt":"x","discovery":{"limits":{"entries":1,"typo":true}},"turns":[],"expect":{"text":"x"}}"#,
        r#"{"kind":"summary","turns":[],"expect":{"text":"x"}}"#,
        r#"{"kind":"turn","prompt":"x","instruction":"x","turns":[],"expect":{"text":"x"}}"#,
        r#"{"kind":"unknown","prompt":"x","turns":[],"expect":{"text":"x"}}"#,
    ] {
        assert!(
            Scenario::parse(input.as_bytes()).is_err(),
            "accepted {input}"
        );
    }
}

#[test]
fn limit_overrides_preserve_defaults_and_reject_unknown_keys() -> Result<(), String> {
    let case = Scenario::parse(
        br#"{"prompt":"x","turns":[],"limits":{"model_events_per_step":2},"expect":{"text":"x"}}"#,
    )?;
    let limits = case.execution_limits()?;
    assert_eq!(limits.model_events_per_step, 2);
    assert_eq!(
        limits.file_bytes,
        acyclic_harness::conversation::Limits::default().file_bytes
    );
    let typo =
        Scenario::parse(br#"{"prompt":"x","turns":[],"limits":{"typo":2},"expect":{"text":"x"}}"#)?;
    assert!(typo.execution_limits().is_err());
    Ok(())
}

#[test]
fn defaults_merge_from_root_to_family_and_keep_case_overrides() -> super::TestResult {
    let temp = tempfile::tempdir()?;
    let family = temp.path().join("family");
    std::fs::create_dir(&family)?;
    std::fs::write(
        temp.path().join("_defaults.json"),
        r#"{
        "prompt":"root", "turns":["root"], "capacity":{"context_tokens":60,"output_tokens":10},
        "max_output_tokens":10, "expect":{"text":"root","request_count":1},
        "seed":{"a":"root","b":"kept"},
        "platforms":{
            "unix":{"turns":["unix program"]},
            "windows":{"turns":["windows program"]}
        }
    }"#,
    )?;
    std::fs::write(
        family.join("_defaults.json"),
        r#"{
        "prompt":"family", "expect":{"text":"family"}, "capacity":{"context_tokens":80},
        "seed":{"a":"family"}
    }"#,
    )?;
    let path = family.join("case.json");
    std::fs::write(
        &path,
        r#"{"max_output_tokens":null,"expect":{"text":"case"}}"#,
    )?;
    let case = Scenario::load(temp.path(), &path)?;
    assert_eq!(case.prompt.as_deref(), Some("family"));
    assert_eq!(
        serde_json::to_value(&case.turns)?,
        serde_json::json!([if cfg!(windows) {
            "windows program"
        } else {
            "unix program"
        }]),
        "scenario loading must select the current platform program"
    );
    assert_eq!(case.max_output_tokens, None);
    assert_eq!(case.expect.text.as_deref(), Some("case"));
    assert_eq!(case.expect.request_count, Some(1));
    assert_eq!(case.seed.get("a").map(String::as_str), Some("family"));
    assert_eq!(case.seed.get("b").map(String::as_str), Some("kept"));
    let capacity = case.capacity.ok_or("missing capacity")?;
    assert_eq!(capacity.context_tokens, 80);
    assert_eq!(capacity.output_tokens, Some(10));
    std::fs::write(
        &path,
        r#"{"turns":[],"expect":{"text":"case","request_count":null}}"#,
    )?;
    let case = Scenario::load(temp.path(), &path)?;
    assert!(case.turns.is_empty());
    assert_eq!(case.expect.request_count, None);
    for platforms in [
        serde_json::json!({"windwos":{}}),
        serde_json::json!({"windows":[]}),
        serde_json::json!({"unix":"program"}),
    ] {
        std::fs::write(
            &path,
            serde_json::to_vec(&serde_json::json!({"platforms":platforms}))?,
        )?;
        assert!(Scenario::load(temp.path(), &path).is_err());
    }
    std::fs::write(&path, r#"{"typo":1}"#)?;
    assert!(Scenario::load(temp.path(), &path).is_err());
    std::fs::write(family.join("_defaults.json"), "{")?;
    assert!(Scenario::load(temp.path(), &path).is_err());
    Ok(())
}
