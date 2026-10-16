use super::{TestResult, provider_case::ProviderCase, providers, scenario};
use acyclic_harness::model::ModelEvent;
use proptest::prelude::*;
use std::path::Path;

proptest! {
    #[test]
    fn recorded_provider_case_codec_and_exact_event_oracle(text in ".{0,48}", position in 0usize..4, protocol in 0usize..4) {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/scenarios");
        let mut value = scenario::load_value(&root, &root.join(format!("providers/{}/text.json", ["anthropic", "responses", "chat", "gemini"][protocol]))).map_err(TestCaseError::fail)?;
        value.as_object_mut().unwrap().remove("driver");
        value["prompt"] = text.clone().into();
        let case: ProviderCase = serde_json::from_value(value)?;
        let encoded = serde_json::to_vec(&case)?;
        prop_assert_eq!(serde_json::from_slice::<ProviderCase>(&encoded)?, case);
        let events = vec![ModelEvent::Content { delta: text.clone() }, ModelEvent::Completed { metadata: serde_json::Value::Null }];
        let expected: Vec<_> = events.iter().map(|event| serde_json::to_value(event).unwrap()).collect();
        prop_assert!(providers::check_events(&expected, &events).is_ok());
        let mut broken = events;
        broken.insert(position.min(broken.len()), ModelEvent::Content { delta: String::new() });
        prop_assert!(providers::check_events(&expected, &broken).is_err());
    }
}

#[test]
fn provider_case_rejects_ignored_fields_zero_chunks_and_unused_response_shapes() -> TestResult {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/scenarios");
    let mut value = scenario::load_value(&root, &root.join("providers/chat/text.json"))?;
    value.as_object_mut().ok_or("case")?.remove("driver");
    let case: ProviderCase = serde_json::from_value(value.clone())?;
    case.validate()?;
    for key in ["unknown", "protocol", "chunk_bytes", "responses"] {
        let mut broken = value.clone();
        broken[key] = match key {
            "unknown" => true.into(),
            "protocol" => "typo".into(),
            "chunk_bytes" => 0.into(),
            _ => serde_json::json!([]),
        };
        assert!(
            serde_json::from_value::<ProviderCase>(broken)
                .map_or(true, |case| case.validate().is_err()),
            "accepted {key}"
        );
    }
    Ok(())
}

#[test]
fn provider_scenario_fuzz_seeds_round_trip() -> TestResult {
    let root =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../harness-fuzz/corpus/provider_scenario");
    for entry in std::fs::read_dir(root)? {
        let case: ProviderCase = serde_json::from_slice(&std::fs::read(entry?.path())?)?;
        case.validate()?;
        assert_eq!(
            serde_json::from_slice::<ProviderCase>(&serde_json::to_vec(&case)?)?,
            case
        );
    }
    Ok(())
}
