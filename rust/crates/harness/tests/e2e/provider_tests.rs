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

    #[test]
    fn reasoning_oracle_binds_every_continuity_byte_and_credential_scope(
        bytes in prop::collection::vec(any::<u8>(), 0..64),
        inserted in any::<u8>(), position in 0usize..65, scope_byte in 0usize..32,
    ) {
        use acyclic_harness::model::{ReasoningBlock, ReasoningKind, ReasoningOrigin, ReasoningOutcome};
        let event = ModelEvent::ReasoningBlock { block: ReasoningBlock {
            kind: ReasoningKind::Opaque, text: None,
            origin: ReasoningOrigin { provider: "fixture".into(), model: "model".into(), credential_scope: Some([73; 32]) },
            continuity: bytes, outcome: ReasoningOutcome::PreservedOpaque,
        }};
        let expected = vec![serde_json::to_value(&event)?];
        prop_assert!(providers::check_events(&expected, std::slice::from_ref(&event)).is_ok());
        for scope in [false, true] {
            let mut broken = event.clone();
            let ModelEvent::ReasoningBlock { block } = &mut broken else { unreachable!() };
            if scope {
                block.origin.credential_scope.as_mut().unwrap()[scope_byte] ^= 1;
            } else {
                block.continuity.insert(position.min(block.continuity.len()), inserted);
            }
            prop_assert!(providers::check_events(&expected, &[broken]).is_err());
        }
    }

    #[test]
    fn request_oracle_rejects_lost_results_and_changed_provider_ids(id in "[a-z]{1,16}") {
        use serde_json::json;
        let body = json!({"history":[{"call_id":id,"output":"result"}]});
        let expected = std::collections::BTreeMap::from([("/history/0".into(), body["history"][0].clone())]);
        prop_assert!(providers::check_request(&expected, &body).is_ok());
        prop_assert!(providers::check_request(&expected, &json!({"history":[]})).is_err(), "accepted a missing result");
        prop_assert!(providers::check_request(&expected, &json!({"history":[{"call_id":format!("{id}!"),"output":"result"}]})).is_err(), "accepted a mismatched ID");
        prop_assert!(providers::check_request(&expected, &json!({"history":[{"call_id":id,"output":"corrupted"}]})).is_err(), "accepted corrupted result bytes");
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
    for statuses in [
        serde_json::json!([]),
        serde_json::json!([{}, {}]),
        serde_json::json!([{"state":"pending","attention":0},{"state":"completed","attention":true}]),
    ] {
        let mut broken = value.clone();
        broken["expect"]["statuses"] = statuses;
        assert!(
            serde_json::from_value::<ProviderCase>(broken)?
                .validate()
                .is_err()
        );
    }
    Ok(())
}

#[test]
fn provider_scenario_fuzz_seeds_round_trip() -> TestResult {
    for bytes in [
        include_bytes!("../../../harness-fuzz/corpus/provider_scenario/chat.json").as_slice(),
        include_bytes!("../../../harness-fuzz/corpus/provider_scenario/gemini.json").as_slice(),
    ] {
        let case: ProviderCase = serde_json::from_slice(bytes)?;
        case.validate()?;
        assert_eq!(
            serde_json::from_slice::<ProviderCase>(&serde_json::to_vec(&case)?)?,
            case
        );
    }
    Ok(())
}
