use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // Task-owned forks have no conversation witness; other output contracts
    // retain the previously required witness field, including its nullable form.
    #[test]
    fn output_contract_distinguishes_task_forks_without_weakening_conversation_reports(
        task in any::<bool>(), witness in 0u8..3, digest in any::<[u8;32]>(),
    ) {
        let schema: Value = serde_json::from_slice(PREPARATION_OUTPUT_SCHEMA).unwrap();
        let validator = jsonschema::validator_for(&schema).unwrap();
        let mut report = serde_json::json!({
            "request":{"parent":{"kind":if task {"task"} else {"conversation"}}},
            "captures":[], "child_private_volume":{}, "child_private_generation":{},
            "inherited_context":[], "summary":null, "inherited_through_sequence":0,
            "shared_grants":[], "reference_grants":[], "attachment_manifests":[],
        });
        if witness != 0 {
            report["history_witness"] = if witness == 1 { Value::Null } else { serde_json::json!(digest) };
        }
        prop_assert_eq!(validator.is_valid(&report), if task { witness == 0 } else { witness != 0 });
        report["unexpected"] = Value::Null;
        prop_assert!(!validator.is_valid(&report));
    }
}
