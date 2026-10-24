use super::*;
use crate::{tests::tool_name_tests::request_with, tool_names::Names};
use proptest::prelude::*;
use std::collections::BTreeSet;

thread_local! {
    pub(crate) static DROP_PINNED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn declarations(request: &ModelRequest) -> Result<()> {
    let names = Names::new(request);
    let expected = request
        .tools
        .iter()
        .flat_map(|tool| {
            [
                tool.name.clone(),
                format!("{}@{}", tool.name, tool.revision),
            ]
        })
        .collect::<BTreeSet<_>>();
    for (adapter, _) in fixtures() {
        let (provider, _) = provider(adapter, 200);
        let body = crate::projection::body(&provider, request)?;
        assert!(
            crate::validate_input(adapter, &body).is_empty(),
            "valid provider grammar"
        );
        let tools = match adapter {
            crate::Adapter::Gemini => body["tools"][0]["functionDeclarations"].as_array(),
            _ => body["tools"].as_array(),
        }
        .map(Vec::as_slice)
        .unwrap_or(&[]);
        let mut seen = BTreeSet::new();
        for tool in tools {
            let tool = if adapter == crate::Adapter::Chat {
                &tool["function"]
            } else {
                tool
            };
            let alias = tool["name"].as_str().unwrap();
            let canonical = names.canonical(alias)?.unwrap_or(alias);
            assert!(
                seen.insert(canonical.to_owned()),
                "binding advertised twice"
            );
            assert_eq!(names.wire(canonical)?, alias);
        }
        assert_eq!(
            seen, expected,
            "every granted binding must be advertised exactly once"
        );
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn granted_names_and_pins_are_declared_once_and_round_trip_on_every_provider(
        tools in prop::collection::btree_set("[a-z][a-z_.-]{0,72}", 0..8),
        revision in "[a-z0-9.-]{1,24}",
    ) {
        let mut candidate = request_with(&tools.iter().map(String::as_str).collect::<Vec<_>>());
        for tool in &mut candidate.tools { tool.revision.clone_from(&revision); }
        // A retained, no-longer-granted call is replayable, never advertised.
        candidate.messages.push(acyclic_harness_core::model::ModelMessage {
            role: acyclic_harness_core::model::ModelRole::Assistant,
            content: acyclic_harness_core::model::ModelContent::Part(
                acyclic_harness_core::model::ModelContentPart::ToolCall(
                    acyclic_harness_core::model::ToolCall::new("old".into(), "hidden.tool@old".into(), serde_json::json!({})),
                )
            ),
        });
        candidate.messages.push(acyclic_harness_core::model::ModelMessage {
            role: acyclic_harness_core::model::ModelRole::Tool,
            content: acyclic_harness_core::model::ModelContent::Part(
                acyclic_harness_core::model::ModelContentPart::ToolResult {
                    call_id: "old".into(), name: "hidden.tool@old".into(),
                    content: acyclic_harness_core::model::ToolResultContent::Json { value: serde_json::json!({}) },
                }
            ),
        });
        declarations(&candidate).map_err(|error| TestCaseError::fail(error.to_string()))?;
        let decoded: ModelRequest = serde_json::from_slice(&serde_json::to_vec(&candidate)?)?;
        declarations(&decoded).map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}

#[test]
fn negative_control_dropping_pins_fails_the_same_declaration_oracle() {
    let request = request_with(&["read"]);
    DROP_PINNED.with(|flag| flag.set(true));
    let failed = std::panic::catch_unwind(|| declarations(&request));
    DROP_PINNED.with(|flag| flag.set(false));
    let panic = failed.expect_err("old declaration path must lose granted pins");
    assert!(
        panic
            .downcast_ref::<String>()
            .unwrap()
            .contains("every granted binding")
    );
}

#[test]
fn pinned_binding_corpus_runs_the_declaration_and_round_trip_oracles() {
    let seed = include_bytes!("../fuzz/corpus/provider_stream/pinned-bindings.json");
    let request: ModelRequest = serde_json::from_slice(&seed[1..]).unwrap();
    declarations(&request).unwrap();
    Names::assert_round_trip(&request);
    crate::fuzzing::assert_stream_invariants(seed);
}
