use super::{TestResult, provider_case::ProviderCase, providers, scenario_data};
use acyclic_harness::model::ModelEvent;
use proptest::prelude::*;

proptest! {
    #[test]
    fn recorded_provider_case_codec_and_exact_event_oracle(text in ".{0,48}", position in 0usize..4, protocol in prop::sample::select(vec!["anthropic", "responses", "chat", "gemini"])) {
        let root = acyclic_test_support::crate_path!("tests/scenarios");
        let mut value = scenario_data::load_value(&root, &root.join(format!("providers/{protocol}/text.json"))).map_err(TestCaseError::fail)?;
        let object = value.as_object_mut().ok_or_else(|| TestCaseError::fail("case"))?;
        object.remove("driver");
        object.insert("prompt".into(), text.clone().into());
        let case: ProviderCase = serde_json::from_value(value)?;
        if case.validate().is_ok() {
            futures::executor::block_on(providers::parsed(
                &case,
                super::provider_http::Recorded::new(&case),
            )).map_err(|error| TestCaseError::fail(error.to_string()))?;
        }
        let encoded = serde_json::to_vec(&case)?;
        prop_assert_eq!(serde_json::from_slice::<ProviderCase>(&encoded)?, case);
        let events = vec![ModelEvent::Content { delta: text }, ModelEvent::Completed { metadata: serde_json::Value::Null }];
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
        credential_scope in prop::option::of(any::<[u8; 32]>()),
    ) {
        use acyclic_harness::model::{ReasoningBlock, ReasoningKind, ReasoningOrigin, ReasoningOutcome};
        let event = ModelEvent::ReasoningBlock { block: ReasoningBlock {
            kind: ReasoningKind::Opaque, text: None,
            origin: ReasoningOrigin { provider: "fixture".into(), model: "model".into(), credential_scope },
            continuity: bytes, outcome: ReasoningOutcome::PreservedOpaque,
        }};
        let expected = vec![serde_json::to_value(&event)?];
        prop_assert!(providers::check_events(&expected, std::slice::from_ref(&event)).is_ok());
        for mutation in 0..3 {
            let mut broken = event.clone();
            let ModelEvent::ReasoningBlock { block } = &mut broken else { unreachable!() };
            match mutation {
                0 => block.continuity.insert(position.min(block.continuity.len()), inserted),
                1 => block.origin.credential_scope.get_or_insert([73; 32])[scope_byte] ^= 1,
                _ => block.origin.credential_scope = credential_scope.is_none().then_some([73; 32]),
            }
            prop_assert!(providers::check_events(&expected, &[broken]).is_err());
        }
    }

    #[test]
    fn request_oracle_rejects_lost_results_and_changed_provider_ids(id in "[a-z]{1,16}", notices in 0usize..4) {
        use serde_json::json;
        let arguments = json!({"query":id});
        let entry = json!({"call_id":id,"output":"result","arguments":serde_json::to_string(&arguments)?});
        let mut history = vec![json!({"notice":"tools.changed"}); notices];
        history.push(entry.clone());
        let body = json!({"history":history});
        let expected = std::collections::BTreeMap::from([("/history".into(), json!([{"call_id":id,"output":"result","arguments":{"$json":arguments}}]))]);
        prop_assert!(providers::check_request(&expected, &body).is_ok());
        prop_assert!(providers::check_request(&expected, &json!({"history":[]})).is_err(), "accepted a missing result");
        let mut changed = body.clone();
        changed["history"][notices]["call_id"] = format!("{id}!").into();
        prop_assert!(providers::check_request(&expected, &changed).is_err(), "accepted a mismatched ID");
        let mut changed = body.clone();
        changed["history"][notices]["output"] = "corrupted".into();
        prop_assert!(providers::check_request(&expected, &changed).is_err(), "accepted corrupted result bytes");
        let mut injected = body;
        injected["history"][notices]["arguments"] = serde_json::to_string(&json!({"query":id,"injected":true}))?.into();
        prop_assert!(providers::check_request(&expected, &injected).is_err(), "accepted extra encoded arguments");
    }

}

#[test]
fn provider_case_rejects_ignored_fields_zero_chunks_and_unused_response_shapes() -> TestResult {
    let root = acyclic_test_support::crate_path!("tests/scenarios");
    let mut value = scenario_data::load_value(&root, &root.join("providers/chat/text.json"))?;
    value.as_object_mut().ok_or("case")?.remove("driver");
    let case: ProviderCase = serde_json::from_value(value.clone())?;
    case.validate()?;
    for key in ["unknown", "protocol", "chunk_bytes", "responses"] {
        let mut broken = value.clone();
        broken.as_object_mut().ok_or("case")?.insert(
            key.into(),
            match key {
                "unknown" => true.into(),
                "protocol" => "typo".into(),
                "chunk_bytes" => 0.into(),
                _ => serde_json::json!([]),
            },
        );
        assert!(
            serde_json::from_value::<ProviderCase>(broken)
                .map_or(true, |case| case.validate().is_err()),
            "accepted {key}"
        );
    }
    // A refused last response cannot complete; statuses must be HTTP and one per response.
    for statuses in [
        serde_json::json!([500]),
        serde_json::json!([99]),
        serde_json::json!([200, 200]),
    ] {
        let mut broken = value.clone();
        broken
            .as_object_mut()
            .ok_or("case")?
            .insert("statuses".into(), statuses);
        assert!(
            serde_json::from_value::<ProviderCase>(broken)?
                .validate()
                .is_err()
        );
    }
    // A stalled last response cannot complete; stalls must name distinct, ascending responses.
    for stalls in [
        serde_json::json!([0]),
        serde_json::json!([1]),
        serde_json::json!([0, 0]),
    ] {
        let mut broken = value.clone();
        broken
            .as_object_mut()
            .ok_or("case")?
            .insert("stalls".into(), stalls);
        assert!(
            serde_json::from_value::<ProviderCase>(broken)?
                .validate()
                .is_err()
        );
    }
    for statuses in [
        serde_json::json!([]),
        serde_json::json!([{}, {}]),
        serde_json::json!([{"state":"pending","attention":0},{"state":"completed","attention":true}]),
    ] {
        let mut broken = value.clone();
        broken
            .pointer_mut("/expect")
            .and_then(serde_json::Value::as_object_mut)
            .ok_or("expected object")?
            .insert("statuses".into(), statuses);
        assert!(serde_json::from_value::<ProviderCase>(broken).is_err());
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
