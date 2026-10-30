//! Oracles over the actual retained agent record and checkpoint codecs.
use super::{
    AgentId, AgentPath, AgentRecord, AgentState, Closed, ConversationEntry, ForkAdopted,
    InboxRecord, IntentId, Phase, Sender, StatusRecord, TurnEffect, checkpoint, records,
};

fn record<T: AgentRecord + std::fmt::Debug + PartialEq>(bytes: &[u8]) {
    if let Ok(value) = T::decode(bytes) {
        let encoded = value.encode().expect("admitted agent record encodes");
        let restored = T::decode(&encoded).expect("encoded agent record decodes");
        assert_eq!(restored, value);
        assert_eq!(restored.encode().expect("stable record encoding"), encoded);
    }
}

/// Every accepted record or checkpoint must preserve its typed value.
/// Parsers are synchronous; completion requires no producer or event.
pub fn check(bytes: &[u8]) {
    record::<InboxRecord>(bytes);
    record::<StatusRecord>(bytes);
    record::<Closed>(bytes);
    record::<ConversationEntry>(bytes);
    if let Ok(effect) = records::open::<TurnEffect>(bytes) {
        let encoded = records::seal(&effect).expect("admitted effect encodes");
        let restored: TurnEffect =
            records::open(&encoded).unwrap_or_else(|_| panic!("encoded effect does not decode"));
        assert_eq!(restored, effect);
    }
    if let Ok(state) = checkpoint::decode(bytes) {
        let encoded = checkpoint::encode(&state).expect("admitted checkpoint encodes");
        assert_eq!(
            checkpoint::decode(&encoded).expect("checkpoint decodes"),
            state
        );
    }
}

/// Seed bytes come from the same writers used by committed actor transitions.
pub fn seeds() -> Result<Vec<(&'static str, Vec<u8>)>, Box<dyn std::error::Error>> {
    let agent = AgentId::from_bytes([7; 16]);
    let path = AgentPath::root(&agent);
    let adopted = ForkAdopted {
        agent,
        parent: None,
        forked_at: 0,
        conversation_tail: 0,
        checkpoint: None,
        volumes: Vec::new(),
        brief: None,
        policy: None,
    };
    let state = AgentState::adopted(path.clone(), &adopted)?;
    let id = IntentId::try_from("captured-intent")?;
    Ok(vec![
        (
            "agent-adopted",
            InboxRecord::ForkAdopted(Box::new(adopted)).encode()?,
        ),
        ("agent-awaited", InboxRecord::Awaited.encode()?),
        (
            "agent-status",
            StatusRecord {
                phase: Phase::Idle,
                cursor: 0,
                conversation: 0,
            }
            .encode()?,
        ),
        (
            "agent-closed",
            Closed {
                by: Sender::Agent { path: path.clone() },
                id: id.clone(),
            }
            .encode()?,
        ),
        (
            "agent-effect",
            records::seal(&TurnEffect::Archive { target: path, id })?,
        ),
        ("agent-checkpoint", checkpoint::encode(&state)?),
    ])
}
