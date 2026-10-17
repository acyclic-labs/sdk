use super::*;
use crate::model::ToolCall;
use proptest::prelude::*;
use serde_json::json;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn provisional_admission_is_exact_for_every_authority_identity(
        id in any::<[u8;16]>(), arguments in ".{0,64}", signature in ".{0,64}", offset in 0u64..3,
    ) {
        let invocation = ToolInvocation { operation_id:OperationId::from_bytes(id), call_id:"call".into(), name:"echo".into(), arguments:json!({"payload":arguments}) };
        let metadata = json!({"signature":signature});
        let size = serde_json::to_vec(&crate::tool::RecordedInvocation { invocation:&invocation, metadata:&metadata }).unwrap().len() as u64;
        let maximum = size + offset - 1;
        let event = ModelEvent::ToolCall(ToolCall::new(invocation.call_id, invocation.name, invocation.arguments).with_metadata(metadata));
        let limits = Limits { file_bytes:maximum, render_bytes:maximum, ..Limits::default() };
        let mut admission = ModelEventAdmission::default();
        prop_assert_eq!(admission.observe(&event, limits, None).is_ok(), maximum >= size);
    }
}

#[test]
fn individually_bounded_signature_negative_control_cannot_reach_a_tool_handler() {
    let metadata = json!({"signature":"x".repeat(1000)});
    let arguments = json!({});
    let limits = Limits {
        file_bytes: 1024,
        render_bytes: 1024,
        ..Limits::default()
    };
    assert!(serde_json::to_vec(&metadata).unwrap().len() < limits.file_bytes as usize);
    assert!(serde_json::to_vec(&arguments).unwrap().len() < limits.file_bytes as usize);
    let event = ModelEvent::ToolCall(
        ToolCall::new("call".into(), "echo".into(), arguments).with_metadata(metadata),
    );
    let mut admission = ModelEventAdmission::default();
    assert!(admission.observe(&event, limits, None).is_err());
    assert_eq!(admission.count, 0);
    assert!(admission.calls.is_empty());
}
