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
    if let Ok(value) = serde_json::from_slice::<acyclic_actors::guest::Checkpoint>(bytes) {
        if let Ok(state) = checkpoint::checkpoint_state(&value) {
            let encoded = acyclic_actors::guest::Checkpoint {
                codec: checkpoint::codec(),
                bytes: checkpoint::encode(&state)
                    .expect("admitted state encodes")
                    .into(),
            };
            assert_eq!(
                checkpoint::checkpoint_state(&encoded).expect("checkpoint identity admits"),
                state
            );
        }
    }
}

/// Seed bytes come from the same writers used by committed actor transitions.
pub fn seeds() -> Result<Vec<(&'static str, Vec<u8>)>, Box<dyn std::error::Error>> {
    let agent = AgentId::from_bytes([7; 16]);
    let path = AgentPath::root(&agent);
    let adopted = ForkAdopted {
        profile: None,
        agent,
        parent: None,
        forked_at: 0,
        conversation_tail: 0,
        checkpoint: None,
        volumes: Vec::new(),
        brief: None,
        policy: None,
    };
    let mut state = AgentState::adopted(path.clone(), &adopted)?;
    let encoded = checkpoint::encode(&state)?;
    let envelope = checkpoint::from_encoded(encoded.clone())?;
    let id = IntentId::try_from("captured-intent")?;
    let mut seeds = vec![
        (
            "adopted",
            InboxRecord::ForkAdopted(Box::new(adopted)).encode()?,
        ),
        ("awaited", InboxRecord::Awaited.encode()?),
        (
            "status",
            StatusRecord {
                phase: Phase::Idle,
                cursor: 0,
                conversation: 0,
            }
            .encode()?,
        ),
        (
            "closed",
            Closed {
                by: Sender::Agent { path: path.clone() },
                id: id.clone(),
            }
            .encode()?,
        ),
        (
            "effect",
            records::seal(&TurnEffect::Archive { target: path, id })?,
        ),
        ("checkpoint", encoded),
        (
            "checkpoint-envelope",
            crate::contract::canonical_json_bytes(&envelope)?,
        ),
    ];
    for (names, binding) in [
        (
            [
                "status-approval-bound",
                "checkpoint-approval-bound",
                "checkpoint-envelope-approval-bound",
            ],
            Some(crate::interaction::ApprovalBinding {
                operation_id: crate::OperationId::from_bytes([11; 16]),
                action_digest: [13; 32],
            }),
        ),
        (
            [
                "status-approval-external",
                "checkpoint-approval-external",
                "checkpoint-envelope-approval-external",
            ],
            None,
        ),
    ] {
        state.phase = Phase::WaitingApproval {
            id: IntentId::try_from("captured-approval")?,
            binding,
        };
        let status = StatusRecord {
            phase: state.phase.clone(),
            cursor: 0,
            conversation: 0,
        };
        let status_bytes = status.encode()?;
        assert_eq!(StatusRecord::decode(&status_bytes)?, status);
        let encoded = checkpoint::encode(&state)?;
        let envelope = checkpoint::from_encoded(encoded.clone())?;
        assert_eq!(checkpoint::checkpoint_state(&envelope)?, state);
        seeds.extend([
            (names[0], status_bytes),
            (names[1], encoded),
            (names[2], crate::contract::canonical_json_bytes(&envelope)?),
        ]);
    }
    Ok(seeds)
}
