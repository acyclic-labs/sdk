//! Regenerates a persisted native admission for cross-language replay checks.
use acyclic_harness::{
    AgentId, Capabilities, IdempotencyKey, OperationId,
    core::{
        Action, AggregateKind, ApplyResult, Authority, AuthorityIssuer, EventPayload, Reducer,
        SchemaRegistry,
    },
    wire::EventEnvelope,
};
use prost::Message;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("expected canonical vector path")?;
    let EventPayload::ConversationMessageAppended { message } = fixture_payload(&path)? else {
        return Err("expected conversation message fixture".into());
    };
    let authority = Authority {
        kind: AggregateKind::Conversation,
        id: "conversation-1".into(),
    };
    let runtime = AuthorityIssuer::new("test", [7; 32], authority.clone());
    let scope = runtime.issue(
        "root",
        &Capabilities::new(["conversation:bind", "conversation:append"]),
        None,
        None,
        &mut None,
    )?;
    let verifier = runtime.verifier(&mut None)?;
    let mut reducer = Reducer::new(authority.clone(), verifier, SchemaRegistry::new());
    let command = |index, action| -> Result<_, acyclic_harness::Error> {
        Ok(acyclic_harness::core::Command {
            operation_id: OperationId::from_bytes([index; 16]),
            idempotency_key: IdempotencyKey::new(if index == 1 { "bind-1" } else { "append-2" })?,
            expected_revision: u64::from(index - 1),
            scope: scope.clone(),
            causal_parent: None,
            action,
        })
    };
    reducer.apply(&command(
        1,
        Action::BindConversation {
            agent: AgentId::from_bytes([4; 16]),
        },
    )?)?;
    let ApplyResult::Applied { event } =
        reducer.apply(&command(2, Action::AppendConversationMessage { message })?)?
    else {
        return Err("expected a fresh admission".into());
    };
    let envelope =
        acyclic_harness::wire_codec::encode_event_envelope(&authority, &event, &mut None)?;
    let wire = hex::encode(envelope.encode_to_vec());
    let (snapshot, _) = reducer.snapshot(2, &mut None)?;
    let vector = serde_json::json!({"version": 2, "event_wire_hex": wire, "snapshot": snapshot});
    std::fs::write(
        path,
        format!("{}\n", serde_json::to_string_pretty(&vector)?),
    )?;
    Ok(())
}

fn fixture_payload(path: &str) -> Result<EventPayload, Box<dyn std::error::Error>> {
    let previous: serde_json::Value = serde_json::from_slice(&std::fs::read(path)?)?;
    let encoded = previous
        .get("event_wire_hex")
        .and_then(serde_json::Value::as_str)
        .ok_or("missing wire fixture")?;
    let bytes = hex::decode(encoded)?;
    let old = EventEnvelope::decode(bytes.as_slice())?;
    Ok(serde_json::from_slice(&old.canonical_payload_json)?)
}
