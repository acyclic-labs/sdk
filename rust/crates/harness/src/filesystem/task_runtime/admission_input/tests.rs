use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    // Only the two registered command-input schemas select a payload to pin.
    // Decoding preserves the full command and rejects executor override fields.
    #[test]
    fn initial_command_round_trip_is_exact_and_scoped_to_its_registered_schema(
        operation in any::<[u8;16]>(), kind in "[a-z]{1,24}", direct in any::<bool>(),
    ) {
        let seed:Value = serde_json::from_str(include_str!("../../../../../harness-fuzz/corpus/external-hooks/session-next")).unwrap();
        let mut command = seed[1]["command"].clone();
        command["operation_id"] = serde_json::json!(OperationId::from_bytes(operation));
        command["kind"] = serde_json::json!(kind);
        let mut input = TaskAdmitCommand {
            name: if direct {"acyclic.direct_tool"} else {"acyclic.command_session"}.into(),
            version:"1".into(), input:serde_json::json!({"command":command,"terminal":false}),
        };
        let decoded = input.initial_command(None)?.unwrap();
        prop_assert_eq!(serde_json::to_value(&decoded).unwrap(),command);
        let encoded = serde_json::to_vec(&input).unwrap();
        let replay:TaskAdmitCommand = serde_json::from_slice(&encoded).unwrap();
        prop_assert_eq!(replay.initial_command(None)?,Some(decoded));
        input.input["command"]["executor"] = serde_json::json!("override");
        prop_assert!(input.initial_command(None).is_err());
        input.name = "user.machine".into();
        prop_assert!(input.initial_command(None)?.is_none());
    }
}
