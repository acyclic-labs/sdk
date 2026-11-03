//! Oracles over the actual retained agent record and checkpoint codecs.
use super::{
    AgentId, AgentPath, AgentRecord, AgentState, Closed, ConversationEntry, ForkAdopted,
    InboxRecord, IntentId, Phase, Progress, Sender, StatusRecord, TurnEffect, checkpoint, records,
};

// Compare with the writer's input, before any decoder can discard a field.
fn preserves_written(
    state: &AgentState,
    encoded: &[u8],
    decode: impl FnOnce(&[u8]) -> Result<AgentState, super::Error>,
) -> Result<bool, super::Error> {
    Ok(decode(encoded)? == *state)
}

fn record<T: AgentRecord + std::fmt::Debug + PartialEq>(bytes: &[u8]) {
    if let Ok(value) = T::decode(bytes) {
        let encoded = value.encode().expect("admitted agent record encodes");
        let restored = T::decode(&encoded).expect("encoded agent record decodes");
        assert_eq!(restored, value);
        assert_eq!(restored.encode().expect("stable record encoding"), encoded);
        header(&value);
    }
}

fn header<T: serde::Serialize + for<'de> serde::Deserialize<'de>>(value: &T) {
    fn refuses<T: for<'de> serde::Deserialize<'de>>(bytes: &[u8], found: u32) {
        let refused = match records::decode_record::<T>(bytes) {
            Err(super::Error::Record("record exceeds MAX_RECORD_BYTES")) => {
                bytes.len() > records::MAX_RECORD_BYTES
            }
            Err(super::Error::UnsupportedRecordVersion(version)) => {
                bytes.len() <= records::MAX_RECORD_BYTES
                    && found != records::RECORD_VERSION
                    && version.found == found
                    && version.expected == records::RECORD_VERSION
            }
            Err(super::Error::Record("record is malformed")) => {
                bytes.len() <= records::MAX_RECORD_BYTES && found == records::RECORD_VERSION
            }
            _ => false,
        };
        assert!(refused, "agent record header refusal changed");
    }
    let encoded = records::seal(value).expect("admitted record writer");
    // Only inspect the canonical writer's output here. Parsing arbitrary input
    // through Value would lose duplicate keys before the real header sees them.
    let mut written: serde_json::Value = serde_json::from_slice(&encoded).expect("writer JSON");
    let fields = written.as_object_mut().expect("writer header");
    for version in (0..=records::RECORD_VERSION + 1).chain(std::iter::once(u32::MAX)) {
        fields.insert("version".into(), version.into());
        let encoded = crate::contract::canonical_json_bytes(fields).expect("altered header");
        if version != records::RECORD_VERSION {
            refuses::<T>(&encoded, version);
        }
        // Both a valid body and one that cannot decode as any of these record
        // types must report a foreign version before payload decoding.
        let body = fields.insert("record".into(), serde_json::Value::Null);
        let malformed = crate::contract::canonical_json_bytes(fields).expect("altered payload");
        refuses::<T>(&malformed, version);
        fields.remove("record");
        let missing = crate::contract::canonical_json_bytes(fields).expect("missing payload");
        refuses::<T>(&missing, version);
        fields.insert("record".into(), body.expect("writer record"));
        for suffix in [
            br#", "extra":0}"#.as_slice(),
            br#", "record":null}"#.as_slice(),
        ] {
            let mut changed = encoded[..encoded.len() - 1].to_vec();
            changed.extend_from_slice(suffix);
            refuses::<T>(&changed, version);
        }
        // The first selected version wins; a repeated version marks the
        // current header malformed, never a new format selection.
        let mut duplicate = encoded[..encoded.len() - 1].to_vec();
        let second_version = if version == records::RECORD_VERSION {
            0
        } else {
            records::RECORD_VERSION
        };
        duplicate.extend_from_slice(format!(r#", "version":{second_version}}}"#).as_bytes());
        refuses::<T>(&duplicate, version);
    }
    for version in [
        serde_json::Value::Null,
        (-1).into(),
        u64::MAX.into(),
        "unknown".into(),
    ] {
        fields.insert("version".into(), version);
        let invalid = crate::contract::canonical_json_bytes(fields).expect("invalid version");
        refuses::<T>(&invalid, records::RECORD_VERSION);
    }
    fields.remove("version");
    let headerless = crate::contract::canonical_json_bytes(fields).expect("headerless record");
    refuses::<T>(&headerless, 1);
}

fn envelope(value: &acyclic_actors::guest::Checkpoint) {
    fn refuses_before_payload(mut foreign: acyclic_actors::guest::Checkpoint) {
        for clear_payload in [false, true] {
            if clear_payload {
                foreign.bytes = Vec::new().into();
            }
            assert!(
                matches!(
                    checkpoint::checkpoint_state(&foreign),
                    Err(super::Error::IncompatibleCheckpointCodec)
                ),
                "checkpoint codec refusal did not precede payload decoding"
            );
        }
    }
    let decoded = checkpoint::checkpoint_state(value);
    if value.codec != checkpoint::codec() {
        assert!(
            matches!(decoded, Err(super::Error::IncompatibleCheckpointCodec)),
            "checkpoint admitted a foreign codec identity"
        );
    } else if let Ok(state) = decoded {
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
        // Exercise refusal with both decodable and malformed payloads. A
        // payload error cannot stand in for refusing the selected identity.
        for version in (0..super::records::RECORD_VERSION)
            .map(|version| version.to_string())
            .chain(std::iter::once("unknown".to_owned()))
        {
            let mut foreign = value.clone();
            foreign.codec.version = version;
            refuses_before_payload(foreign);
        }
        let mut foreign = value.clone();
        foreign.codec.name.push_str(".other");
        refuses_before_payload(foreign);
        let malformed = acyclic_actors::guest::Checkpoint {
            codec: checkpoint::codec(),
            bytes: Vec::new().into(),
        };
        assert!(matches!(
            checkpoint::checkpoint_state(&malformed),
            Err(super::Error::Record(_))
        ));
    }
}

/// Every accepted record or checkpoint must preserve its typed value.
/// Parsers are synchronous; completion requires no producer or event.
pub fn check(bytes: &[u8]) {
    // The state JSON seeds are generated by the real state serializer. They
    // drive the checkpoint writer as well as its reader, with an independent
    // expected value; decode/encode/decode alone cannot detect field loss.
    if let Ok(state) = serde_json::from_slice::<AgentState>(bytes) {
        if let Ok(encoded) = checkpoint::encode(&state) {
            assert!(
                preserves_written(&state, &encoded, checkpoint::decode)
                    .expect("written checkpoint decodes"),
                "checkpoint lost the writer's state"
            );
        }
    }
    record::<InboxRecord>(bytes);
    record::<StatusRecord>(bytes);
    record::<Closed>(bytes);
    record::<ConversationEntry>(bytes);
    if let Ok(effect) = records::open::<TurnEffect>(bytes) {
        let encoded = records::seal(&effect).expect("admitted effect encodes");
        let restored: TurnEffect =
            records::open(&encoded).unwrap_or_else(|_| panic!("encoded effect does not decode"));
        assert_eq!(restored, effect);
        header(&effect);
    }
    if let Ok(state) = checkpoint::decode(bytes) {
        let encoded = checkpoint::encode(&state).expect("admitted checkpoint encodes");
        assert_eq!(
            checkpoint::decode(&encoded).expect("checkpoint decodes"),
            state
        );
    }
    if let Ok(value) = serde_json::from_slice::<acyclic_actors::guest::Checkpoint>(bytes) {
        envelope(&value);
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
    // Cross a bitmap byte boundary, retaining a paused turn's slots and raw
    // checkpoint. A child path also exercises reconstruction of its parent.
    state.path = state.path.child(&AgentId::from_bytes([8; 16]))?;
    state.parent = state.path.parent();
    state.conversation_tail = 23;
    state.status_tail = 29;
    for n in 0..9 {
        state
            .open_children
            .insert(state.path.child(&AgentId::from_bytes([n; 16]))?);
    }
    state.awaiting = state
        .open_children
        .iter()
        .enumerate()
        .filter(|(index, _)| matches!(index, 0 | 7 | 8))
        .map(|(_, child)| child.clone())
        .collect();
    state.need = 2;
    state.turn = Some(Progress {
        inbox: 17,
        checkpoint: vec![0, 1, 255],
    });
    state.reserved_children = 3;
    state.phase = Phase::Waiting {
        deadline_unix_ms: Some(1_700_000_000_000),
    };
    for (names, archived) in [
        (
            ["checkpoint-wait", "checkpoint-envelope-wait", "state-wait"],
            false,
        ),
        (
            [
                "checkpoint-archive",
                "checkpoint-envelope-archive",
                "state-archive",
            ],
            true,
        ),
    ] {
        if archived {
            state.closed = true;
            state.archive_pending = state.open_children.iter().skip(7).cloned().collect();
            state.awaiting.clear();
            state.need = 0;
            state.turn = None;
            state.reserved_children = 0;
            state.phase = Phase::Idle;
        }
        let encoded = checkpoint::encode(&state)?;
        assert!(preserves_written(&state, &encoded, checkpoint::decode)?);
        // Planted reader defects must fail that SAME writer-input comparison.
        // Each changes one nonempty field, after the real decoder ran.
        for loss in 0..5 {
            assert!(!preserves_written(&state, &encoded, |bytes| {
                let mut changed = checkpoint::decode(bytes)?;
                match loss {
                    0 => changed.conversation_tail = 0,
                    1 if archived => changed.archive_pending.clear(),
                    1 => changed.awaiting.clear(),
                    2 if archived => changed.closed = false,
                    2 => changed.turn = None,
                    3 if archived => changed.reported = true,
                    3 => changed.reserved_children = 0,
                    _ => changed.phase = Phase::Running,
                }
                // The former decode/encode/decode oracle still accepts it.
                assert_eq!(checkpoint::decode(&checkpoint::encode(&changed)?)?, changed);
                Ok(changed)
            })?);
        }
        let envelope = checkpoint::from_encoded(encoded.clone())?;
        assert_eq!(checkpoint::checkpoint_state(&envelope)?, state);
        seeds.extend([
            (names[0], encoded),
            (names[1], crate::contract::canonical_json_bytes(&envelope)?),
            (names[2], crate::contract::canonical_json_bytes(&state)?),
        ]);
    }
    Ok(seeds)
}

#[cfg(test)]
mod tests {
    #[test]
    fn writer_corpus_satisfies_the_shared_agent_oracle() {
        for (_, bytes) in super::seeds().expect("production agent writers") {
            super::check(&bytes);
        }
    }
}
