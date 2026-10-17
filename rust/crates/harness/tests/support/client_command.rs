use acyclic_harness::{wire, wire_api::current_protocol};

pub fn owner() -> wire::Authority {
    wire::Authority {
        kind: wire::AggregateKind::Task as i32,
        id: "task".into(),
    }
}
pub fn command() -> wire::CommandEnvelope {
    wire::CommandEnvelope {
        protocol: Some(current_protocol()),
        authority: Some(owner()),
        operation: Some(wire::OperationIdentity {
            operation_id: "00000000-0000-0000-0000-000000000001".into(),
            idempotency_key: "submit-1".into(),
        }),
        action_type: "transition_lifecycle".into(),
        canonical_action_json: b"{}".to_vec(),
        ..Default::default()
    }
}
