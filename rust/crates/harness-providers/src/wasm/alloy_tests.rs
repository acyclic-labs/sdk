use super::*;
use crate::{AlloySelection, ModelSpec};
use acyclic_harness::context::ModelContextCapacity;
use proptest::{
    prelude::*,
    test_runner::{Config, RngSeed, TestRunner},
};
use wasm_bindgen_test::wasm_bindgen_test;

fn parity(
    capacities: Vec<u32>,
    weights: Vec<u32>,
    ordinal: u64,
    policy: u8,
    operation: [u8; 16],
    role: bool,
    broken: bool,
) -> bool {
    let mut browser = WasmProviderRegistry::new(
        to_js(&serde_json::json!({})).unwrap(),
        to_js(&Limits::default()).unwrap(),
        None,
        None,
    )
    .unwrap();
    let models: BTreeMap<_, _> = capacities
        .iter()
        .enumerate()
        .map(|(index, &capacity)| {
            (
                format!("model-{index}"),
                ModelSpec {
                    capacity: ModelContextCapacity {
                        context_tokens: capacity,
                        output_tokens: capacity / 4,
                    },
                    features: ModelFeatures {
                        tools: true,
                        images: true,
                    },
                },
            )
        })
        .collect();
    browser.register("fixture".into(), to_js(&serde_json::json!({
        "adapter":"chat","authentication":"optional","base_url":"http://localhost/v1","models":models,
        "max_event_bytes":4096,"token_counter":{"path":"count","adapter":"chat","response_field":"input_tokens","framing_tokens":0},
    })).unwrap()).unwrap();
    let count = capacities.len();
    let spec = AlloySpec {
        models: models
            .keys()
            .map(|name| format!("fixture/{name}"))
            .collect(),
        select: match policy {
            0 => AlloySelection::First,
            1 => AlloySelection::RoundRobin,
            2 => AlloySelection::Weighted {
                weights: (0..count)
                    .map(|index| weights[index % weights.len()])
                    .collect(),
            },
            _ => AlloySelection::PerRole {
                roles: BTreeMap::from([("worker".into(), count - 1)]),
                fallback: 0,
            },
        },
    };
    let required = ModelFeatures {
        tools: true,
        images: true,
    };
    let native = browser
        .registry
        .resolve_alloy(spec.clone(), required)
        .unwrap();
    let browser = browser
        .resolve_alloy(to_js(&spec).unwrap(), to_js(&required).unwrap())
        .unwrap();
    let operation_id = acyclic_harness::OperationId::from_bytes(operation);
    let role = if role { "worker" } else { "other" };
    let turn = AlloyTurn {
        operation_id,
        ordinal,
        role,
    };
    let expected = native.select(turn);
    let selected = browser
        .select(
            &operation_id.to_string(),
            if broken { 0 } else { ordinal },
            role,
        )
        .unwrap();
    let actual: Model = from_js(selected.model().unwrap()).unwrap();
    let capacity: serde_json::Value = from_js(browser.capacity().unwrap()).unwrap();
    let expected_capacity = native.capacity();
    actual == *expected.model()
        && capacity
            == serde_json::json!({"contextTokens":expected_capacity.context_tokens,"outputTokens":expected_capacity.output_tokens})
        && selected.provider.context_capacity(&actual).unwrap() == expected_capacity
        && browser
            .select(
                &operation_id.to_string(),
                if broken { 0 } else { ordinal },
                role,
            )
            .unwrap()
            .provider
            .model()
            == &actual
}

#[wasm_bindgen_test]
fn random_alloys_have_native_binding_parity_and_resume_stability() {
    let mut runner = TestRunner::new(Config {
        cases: 96,
        failure_persistence: None,
        rng_seed: RngSeed::Fixed(508),
        ..Config::default()
    });
    runner
        .run(
            &(
                prop::collection::vec(1024u32..65536, 1..7),
                prop::collection::vec(1u32..16, 1..7),
                any::<u64>(),
                0u8..4,
                any::<[u8; 16]>(),
                any::<bool>(),
            ),
            |(capacities, weights, ordinal, policy, operation, role)| {
                prop_assert!(parity(
                    capacities, weights, ordinal, policy, operation, role, false
                ));
                Ok(())
            },
        )
        .unwrap();
}

#[wasm_bindgen_test]
fn parity_oracle_rejects_a_lost_turn_ordinal() {
    assert!(!parity(
        vec![4096, 8192],
        vec![1, 1],
        u64::MAX,
        1,
        [7; 16],
        false,
        true
    ));
}
