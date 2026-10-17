use super::*;
use crate::conversation::{FileDescriptor, FileRef, VolumeClass, VolumeOwner, VolumeRef};
use proptest::prelude::*;

fn input(operation: crate::OperationId) -> Value {
    let payload = FileRef::new(
        VolumeRef::new(
            crate::resources::ProviderRef::new(
                "direct-test".into(),
                "filesystem".into(),
                "2".into(),
                &mut None,
            )
            .unwrap(),
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(crate::AgentId::from_bytes([4; 16])),
            &mut None,
        )
        .unwrap(),
        "inputs/tool.json".into(),
        "immutable-version".into(),
        FileDescriptor::from_bytes(b"{}", "application/json", &mut None).unwrap(),
        "tool.json".into(),
        &mut None,
    )
    .unwrap();
    json!({"command": WorkflowCommand { operation_id: operation, kind: filesystem::TOOL_TASK_COMMAND_KIND.into(), payload }})
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // A persisted initialized/sent state can be reconstructed and emits exactly
    // one matching command; a terminal response must carry its exact identity.
    #[test]
    fn direct_tool_state_round_trip_and_result_identity(operation in any::<[u8;16]>(), text in ".{0,80}", succeeds in any::<bool>()) {
        let machine = DirectTool::new();
        let operation = crate::OperationId::from_bytes(operation);
        let state = machine.initialize(&input(operation)).unwrap();
        let encoded = serde_json::to_vec(&state).unwrap();
        let restored: Value = serde_json::from_slice(&encoded).unwrap();
        prop_assert_eq!(&restored, &state);
        let sent = machine.transition(&restored, &Value::Null).unwrap();
        prop_assert_eq!(sent.commands.len(), 1);
        prop_assert_eq!(sent.commands[0].operation_id, operation);
        let value = if succeeds { json!({"Succeeded":{"text":text}}) } else { json!({"Failed":{"message":text}}) };
        let result = json!({"commands":[{"operation_id":operation,"value":value}]});
        let terminal = machine.transition(&sent.state, &result).unwrap();
        prop_assert!(terminal.commands.is_empty());
        let expected = if succeeds { MachineStatus::Completed { value: json!({"value":{"text":text}}) } } else { MachineStatus::Failed { message: text } };
        prop_assert_eq!(terminal.status, expected);
        let mut wrong = result;
        wrong["commands"][0]["operation_id"] = json!("mismatched");
        prop_assert!(machine.transition(&sent.state, &wrong).is_err());
    }
}
