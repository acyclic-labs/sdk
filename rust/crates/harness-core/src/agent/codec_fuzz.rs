//! Oracles over the actual retained agent record and checkpoint codecs.
use super::{
    AgentId, AgentPath, AgentRecord, AgentState, Closed, ConversationEntry, ForkAdopted,
    HistoryCheckpoint, InboxRecord, IntentId, Phase, Progress, Sender, StatusRecord, TurnEffect,
    checkpoint, records,
};

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
    assert!(records::decode_record::<T>(&encoded).is_ok());
    // Struct records and the struct payload of an externally tagged variant
    // refuse unknown fields. Unit variants have no fields to extend.
    let mut extended = serde_json::to_value(value).expect("admitted record fields");
    if let serde_json::Value::Object(fields) = &mut extended {
        let fields = if fields.len() == 1 {
            fields
                .values_mut()
                .next()
                .and_then(serde_json::Value::as_object_mut)
        } else {
            Some(fields)
        };
        if let Some(fields) = fields {
            fields.insert("__unknown_record_field".into(), serde_json::Value::Null);
            let bytes = records::seal(&extended).expect("extended record writer");
            assert!(
                matches!(
                    records::decode_record::<T>(&bytes),
                    Err(super::Error::Record(_))
                ),
                "agent record discarded an unknown payload field"
            );
        }
    }
    for version in (0..=records::RECORD_VERSION + 1).chain(std::iter::once(u32::MAX)) {
        let mut changed = encoded.clone();
        changed[..4].copy_from_slice(&version.to_be_bytes());
        if version != records::RECORD_VERSION {
            refuses::<T>(&changed, version);
        }
        // A foreign version wins even when its payload is absent or cannot
        // decode as the selected record. The fixed header has no duplicate keys.
        for body in [b"".as_slice(), &[0xc0][..]] {
            let mut malformed = version.to_be_bytes().to_vec();
            malformed.extend_from_slice(body);
            refuses::<T>(&malformed, version);
        }
        // Complete consumption is required: never accept a second value,
        // a second header, or a truncated current value.
        changed.push(0);
        refuses::<T>(&changed, version);
        let mut duplicate = version.to_be_bytes().to_vec();
        duplicate.extend_from_slice(&encoded);
        refuses::<T>(&duplicate, version);
    }
    for length in 0..4 {
        assert!(matches!(
            records::decode_record::<T>(&encoded[..length]),
            Err(super::Error::Record("record is malformed"))
        ));
    }
    refuses::<T>(&encoded[..encoded.len() - 1], records::RECORD_VERSION);
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
    history(bytes);
    roster(bytes);
    // The state JSON seeds are generated by the real state serializer. They
    // drive the checkpoint writer as well as its reader, with an independent
    // expected value; decode/encode/decode alone cannot detect field loss.
    if let Ok(state) = serde_json::from_slice::<AgentState>(bytes) {
        if let Ok(encoded) = checkpoint::encode(&state) {
            assert_eq!(
                checkpoint::decode(&encoded).expect("written checkpoint decodes"),
                state,
                "checkpoint lost the writer's state"
            );
        }
    }
    record::<InboxRecord>(bytes);
    // Read the selection independently of SpawnRequest's field deserializer:
    // dropping it there otherwise leaves a stable decode/encode fixed point.
    if let Ok(InboxRecord::Spawn(spawn)) = InboxRecord::decode(bytes) {
        let input =
            records::decode_record::<serde_json::Value>(bytes).expect("admitted spawn payload");
        // The production serde reader admits both named and positional
        // structs. Keep this projection independent of SpawnRequest while
        // reading its fifth field in either representation.
        let spawn_input = input.get("spawn").expect("admitted spawn variant");
        let selected: Option<super::HistorySelection> = serde_json::from_value(
            spawn_input
                .get("history")
                .or_else(|| spawn_input.get(4))
                .cloned()
                .unwrap_or(serde_json::Value::Null),
        )
        .expect("admitted spawn history selection");
        assert_eq!(
            spawn.history, selected,
            "spawn record lost the input's history selection"
        );
    }
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
    checkpoint::check_wire(bytes);
    if let Ok(value) = serde_json::from_slice::<acyclic_actors::guest::Checkpoint>(bytes) {
        envelope(&value);
    }
}

fn roster(bytes: &[u8]) {
    let Ok(value) = serde_json::from_slice::<super::Roster>(bytes) else {
        return;
    };
    // Read original wire fields independently of Roster's admitted indexes.
    let input: serde_json::Value = serde_json::from_slice(bytes).unwrap();
    let targets: Vec<super::TargetRecord> =
        serde_json::from_value(input["targets"].clone()).unwrap();
    assert_eq!(value.home_name(), input["home"].as_str().unwrap());
    assert_eq!(value.targets(), targets);
    assert_eq!(
        value.canonical().0,
        crate::contract::canonical_json_bytes(&value).unwrap()
    );
    let mut owners = std::collections::BTreeMap::new();
    for (position, record) in targets.iter().enumerate() {
        owners.entry(record.owner.as_str()).or_insert(position);
    }
    for (position, record) in targets.iter().enumerate() {
        for (query, expected) in [
            (record.name.clone(), position),
            (
                format!("{}{}", super::targets::USER_PREFIX, record.owner),
                owners[record.owner.as_str()],
            ),
        ] {
            if targets[expected].state == super::TargetState::Online {
                assert_eq!(value.select_position(Some(&query)).unwrap(), expected);
            } else {
                assert_eq!(
                    value.select_position(Some(&query)).unwrap_err().refused,
                    super::targets::Refused::Offline
                );
            }
        }
    }
    let mut extended = input;
    extended["positions"] = true.into();
    assert!(serde_json::from_value::<super::Roster>(extended).is_err());
}

fn history(bytes: &[u8]) {
    if let Ok(summary) = HistoryCheckpoint::summary(bytes) {
        assert_ne!(summary.conversation, 0);
        assert!(summary.context_tail <= summary.conversation);
    }
    let Ok(value) = HistoryCheckpoint::decode(bytes) else {
        return;
    };
    assert_eq!(value.version, 1);
    assert_ne!(value.conversation, 0);
    assert!(value.context.tail <= value.conversation);
    assert!(value.checkpoint.is_none_or(|cut| cut < value.conversation));
    assert!(
        value
            .context
            .checkpoint
            .is_none_or(|cut| cut < value.context.tail)
    );
    assert!(value.volumes.len() <= records::MAX_VOLUMES);
    let mut names = std::collections::BTreeSet::new();
    for saved in &value.volumes {
        assert_eq!(
            saved.binding.volume.class(),
            crate::conversation::VolumeClass::Project
        );
        assert!(names.insert(&saved.binding.name));
    }
    let summary = HistoryCheckpoint::summary(bytes).expect("admitted history projects");
    assert_eq!(summary.conversation, value.conversation);
    assert_eq!(summary.context_tail, value.context.tail);
    assert_eq!(summary.volume_count, value.volumes.len() as u64);
    let encode = |value: &HistoryCheckpoint| serde_json::to_vec(value).unwrap();
    assert_eq!(HistoryCheckpoint::decode(&encode(&value)).unwrap(), value);

    // Refusals are derived from an admitted writer record, so they cannot pass
    // just because an unrelated malformed field kept the decoder unreachable.
    let mut invalid = value.clone();
    for version in [0, 2, u32::MAX] {
        invalid.version = version;
        let bytes = encode(&invalid);
        assert!(HistoryCheckpoint::decode(&bytes).is_err());
        assert!(HistoryCheckpoint::summary(&bytes).is_err());
    }
    invalid = value.clone();
    invalid.conversation = 0;
    assert!(HistoryCheckpoint::decode(&encode(&invalid)).is_err());
    assert!(HistoryCheckpoint::summary(&encode(&invalid)).is_err());
    invalid = value.clone();
    invalid.checkpoint = Some(value.conversation);
    assert!(HistoryCheckpoint::decode(&encode(&invalid)).is_err());
    invalid = value.clone();
    invalid.context.checkpoint = Some(value.context.tail);
    assert!(HistoryCheckpoint::decode(&encode(&invalid)).is_err());
    if value.context.tail > 0 {
        invalid = value.clone();
        invalid.conversation = value.context.tail - 1;
        invalid.checkpoint = None;
        invalid.context.checkpoint = None;
        assert!(HistoryCheckpoint::decode(&encode(&invalid)).is_err());
        assert!(HistoryCheckpoint::summary(&encode(&invalid)).is_err());
    }
    if let Some(saved) = value.volumes.first() {
        invalid = value.clone();
        invalid.volumes.push(saved.clone());
        assert!(HistoryCheckpoint::decode(&encode(&invalid)).is_err());
        let overlong = "x".repeat(records::MAX_INTENT_BYTES + 1);
        for name in ["", "invalid\nname", &overlong] {
            invalid = value.clone();
            invalid.volumes[0].binding.name = name.into();
            assert!(HistoryCheckpoint::decode(&encode(&invalid)).is_err());
        }
        let mut wrong_class = serde_json::to_value(&value).unwrap();
        wrong_class["volumes"][0]["binding"]["volume"]["class"] = "session_shared".into();
        assert!(HistoryCheckpoint::decode(&serde_json::to_vec(&wrong_class).unwrap()).is_err());
    }
    let mut unknown = serde_json::to_value(&value).unwrap();
    unknown["unknown_history_field"] = true.into();
    assert!(HistoryCheckpoint::decode(&serde_json::to_vec(&unknown).unwrap()).is_err());
}

/// Seed bytes come from the same writers used by committed actor transitions.
pub fn seeds() -> Result<Vec<(&'static str, Vec<u8>)>, Box<dyn std::error::Error>> {
    fn written<T: AgentRecord + PartialEq + std::fmt::Debug>(
        value: &T,
    ) -> Result<Vec<u8>, super::Error> {
        let bytes = value.encode()?;
        assert_eq!(
            T::decode(&bytes)?,
            *value,
            "agent record lost the writer's input"
        );
        Ok(bytes)
    }
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
    // These records carry distinct nested history types, not the history
    // descriptor format. Capture them through their original record writer.
    let volume = crate::conversation::VolumeRef::new(
        crate::resources::ProviderRef::new(
            "agents".into(),
            "filesystem".into(),
            "2".into(),
            &mut None,
        )?,
        "project".into(),
        crate::conversation::VolumeClass::Project,
        crate::conversation::VolumeOwner::Project("project".into()),
        &mut None,
    )?;
    let file = crate::conversation::FileRef::new(
        volume.clone(),
        "inputs/brief.txt".into(),
        "captured-revision".into(),
        crate::conversation::FileDescriptor::new([19; 32], 37, "text/plain".into())?,
        "brief".into(),
        &mut None,
    )?;
    let spawn = InboxRecord::Spawn(super::SpawnRequest {
        by: Sender::Agent {
            path: state.path.clone(),
        },
        id: IntentId::try_from("captured-spawn")?,
        brief: Box::new(file.clone()),
        profile: super::CreationConfig {
            context: super::SpawnContext::Fork,
            template: Some(file.clone()),
            peers: Default::default(),
        },
        history: Some(super::HistorySelection {
            conversation: 23,
            checkpoint: Some(17),
        }),
    });
    seeds.push(("spawn-history", written(&spawn)?));
    // Capture every named/positional combination at the two struct boundaries
    // used by the independent history projection. All fields come from the
    // same typed record and every byte goes through the actual record writer.
    for (name, positional_spawn, positional_history) in [
        ("spawn-positional", true, false),
        ("spawn-history-positional", false, true),
        ("spawn-and-history-positional", true, true),
    ] {
        let mut input = serde_json::to_value(&spawn)?;
        let fields = input["spawn"].as_object_mut().expect("named spawn writer");
        if positional_history {
            let history = fields.get_mut("history").expect("written history");
            *history = serde_json::json!([history["conversation"], history["checkpoint"]]);
        }
        if positional_spawn {
            input["spawn"] = ["by", "id", "brief", "profile", "history"]
                .map(|field| fields.remove(field).expect("written spawn field"))
                .into();
        }
        let bytes = records::seal(&input)?;
        assert_eq!(InboxRecord::decode(&bytes)?, spawn);
        seeds.push((name, bytes));
    }
    for (name, mode) in [
        ("restore-files", super::RestoreMode::Files),
        ("restore-context", super::RestoreMode::Context),
        ("restore-both", super::RestoreMode::Both),
    ] {
        let action = super::ContextAction::Restore {
            at: 23,
            mode,
            expected: if mode.files() {
                vec![super::HistoryVolume {
                    binding: super::VolumeBinding {
                        name: "project".into(),
                        volume: volume.clone(),
                    },
                    generation: acyclic_fs::GenerationId::from_root(acyclic_fs::ObjectId {
                        kind: acyclic_fs::ObjectKind::GenerationRoot,
                        digest: acyclic_fs::Digest::from_bytes([23; 32]),
                        storage: Some(acyclic_fs::ObjectStorage {
                            locator: acyclic_fs::Digest::from_bytes([29; 32]),
                            root_file_id: acyclic_fs::FileId::from_bytes([31; 16]),
                            origin_generation: acyclic_fs::Digest::from_bytes([37; 32]),
                        }),
                    }),
                }]
            } else {
                Vec::new()
            },
        };
        seeds.push((
            name,
            written(&InboxRecord::Context(super::ContextRequest {
                by: Sender::Agent {
                    path: state.path.clone(),
                },
                id: IntentId::try_from("captured-restore")?,
                action: action.clone(),
            }))?,
        ));
        if mode == super::RestoreMode::Both {
            seeds.push((
                "conversation-restore",
                written(&ConversationEntry {
                    inbox: 29,
                    input: super::TurnInput::Context {
                        from: Sender::Agent {
                            path: state.path.clone(),
                        },
                        id: IntentId::try_from("captured-restore")?,
                        action,
                    },
                    output: file.clone(),
                    outcome: super::TurnOutcome::RestoreConflict,
                    goal: Some(crate::OperationId::from_bytes([41; 16])),
                    targets: Some(file.clone()),
                    tool_results: Some("tools/.results/captured-turn".into()),
                    steering: Some(super::SteeringTranscript {
                        through: 31,
                        file: file.clone(),
                    }),
                    notices: Some(super::NoticeSelection {
                        source: state.path.clone(),
                        range: 7..11,
                    }),
                })?,
            ));
        }
    }
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
        assert_eq!(
            checkpoint::decode(&encoded)?,
            state,
            "checkpoint lost the writer's state"
        );
        let envelope = checkpoint::from_encoded(encoded.clone())?;
        assert_eq!(checkpoint::checkpoint_state(&envelope)?, state);
        seeds.extend([
            (names[0], encoded),
            (names[1], crate::contract::canonical_json_bytes(&envelope)?),
            (names[2], crate::contract::canonical_json_bytes(&state)?),
        ]);
    }
    seeds.extend(history_seeds()?);
    seeds.push(("roster", roster_seed()?));
    Ok(seeds)
}

fn roster_seed() -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    use super::targets::{DeviceKind, PathStyle, TargetClass, TargetRecord, TargetState};
    let owner = format!("{}a", "é".repeat(127));
    let maximum_name = "n".repeat(super::targets::MAX_TARGET_NAME_BYTES);
    let targets = [
        ("laptop", owner.as_str(), TargetState::Online),
        ("desk", "bo", TargetState::Offline),
        ("ci", "bo", TargetState::Online),
        (maximum_name.as_str(), "ana", TargetState::Online),
    ]
    .into_iter()
    .map(|(name, owner, state)| TargetRecord {
        name: name.into(),
        owner: owner.into(),
        class: TargetClass::Attached,
        os: "linux".into(),
        shell: "bash".into(),
        path_style: PathStyle::Posix,
        device_kind: DeviceKind::Desktop,
        state,
    })
    .collect();
    let value = super::Roster::new("laptop", targets)?;
    let bytes = value.canonical().0.to_vec();
    assert_eq!(serde_json::from_slice::<super::Roster>(&bytes)?, value);
    Ok(bytes)
}

/// History uses the same typed JSON writer as filesystem::history. These seeds
/// qualify descriptor syntax, not the existence or authority of retained roots.
pub fn history_seeds() -> Result<Vec<(&'static str, Vec<u8>)>, Box<dyn std::error::Error>> {
    use super::{ContextCut, SavedVolume, VolumeBinding};
    use crate::conversation::{VolumeClass, VolumeOwner, VolumeRef};
    let mut checkpoint = HistoryCheckpoint {
        version: 1,
        conversation: 1,
        checkpoint: None,
        context: ContextCut {
            path: AgentPath::root(&AgentId::from_bytes([7; 16])).conversation()?,
            tail: 0,
            checkpoint: None,
        },
        volumes: Vec::new(),
    };
    let mut seeds = vec![("history-empty", serde_json::to_vec(&checkpoint)?)];
    checkpoint.conversation = u64::MAX;
    checkpoint.checkpoint = Some(u64::MAX - 1);
    checkpoint.context.tail = u64::MAX;
    checkpoint.context.checkpoint = Some(u64::MAX - 1);
    for index in 0..records::MAX_VOLUMES {
        checkpoint.volumes.push(SavedVolume {
            binding: VolumeBinding {
                name: format!("project-{index}"),
                volume: VolumeRef::new(
                    crate::resources::ProviderRef::new(
                        "agents".into(),
                        "filesystem".into(),
                        "2".into(),
                        &mut None,
                    )?,
                    format!("workspace-{index}"),
                    VolumeClass::Project,
                    VolumeOwner::Project("owner".into()),
                    &mut None,
                )?,
            },
            generation: acyclic_fs::GenerationId::new(acyclic_fs::Digest::from_bytes([1; 32])),
            retained_workspace: format!("retained-{index}"),
            retained_generation: acyclic_fs::GenerationId::new(acyclic_fs::Digest::from_bytes(
                [2; 32],
            )),
        });
        if index == 0 {
            seeds.push(("history-project", serde_json::to_vec(&checkpoint)?));
        }
    }
    seeds.push(("history-max-bindings", serde_json::to_vec(&checkpoint)?));
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
