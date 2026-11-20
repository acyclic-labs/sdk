use super::*;
use acyclic_stream::{CommitCondition, CommitMutation, StreamPath};

fn transition() -> Result<Transition, Box<dyn std::error::Error>> {
    let path = StreamPath::new("agents/a/inbox")?;
    Ok(Transition {
        pause: false,
        response: Some(Bytes::new()),
        checkpoint: Checkpoint {
            codec: CodecIdentity {
                name: "codec".into(),
                version: "1".into(),
            },
            bytes: Bytes::from_static(b"state"),
        },
        alarm: AlarmChange::Set {
            deadline_unix_ms: 7,
        },
        effects: vec![acyclic_stream::CommitEffect {
            conditions: vec![CommitCondition::Tail {
                path: path.clone(),
                expected: 3,
            }],
            mutations: vec![CommitMutation::Append {
                path,
                records: vec![Bytes::from_static(b"record").into()],
            }],
            soft: true,
        }],
        participant: Some(Participant {
            kind: "spawn".into(),
            plan: Bytes::from_static(b"plan"),
        }),
        subscriptions: vec![
            SubscriptionChange::Add {
                id: "watch".into(),
                path: StreamPath::new("agents/b/outbox")?,
                from: None,
            },
            SubscriptionChange::Add {
                id: "replay".into(),
                path: StreamPath::new("agents/c/outbox")?,
                from: Some(4),
            },
            SubscriptionChange::Remove { id: "old".into() },
            SubscriptionChange::Pause { id: "watch".into() },
            SubscriptionChange::Resume { id: "watch".into() },
        ],
    })
}

#[test]
fn transition_round_trips_and_refuses_non_canonical_bytes() -> Result<(), Box<dyn std::error::Error>>
{
    let transition = transition()?;
    let bytes = encode_transition(&transition)?;
    assert_eq!(decode_transition(&bytes)?, transition);
    let admitted: TransitionWire = canonical(&bytes, "transition", TRANSITION_DOMAIN)?;
    for deadline in [0, u64::MAX] {
        let mut forged = admitted.clone();
        forged
            .effects
            .as_mut()
            .expect("actual writer effects")
            .deadline_unix_millis = Some(deadline);
        assert!(matches!(
            decode_transition(&encode(&forged, TRANSITION_DOMAIN)),
            Err(GuestError::Decode(_))
        ));
    }
    let mut orphaned = admitted;
    orphaned
        .effects
        .as_mut()
        .expect("actual writer effects")
        .effects[0]
        .mutations
        .clear();
    assert!(matches!(
        decode_transition(&encode(&orphaned, TRANSITION_DOMAIN)),
        Err(GuestError::Decode(_))
    ));
    // A hard guard joins the host's commit, so it is admissible as written.
    let mut guarded = transition.clone();
    guarded.effects[0].mutations.clear();
    guarded.effects[0].soft = false;
    assert_eq!(decode_transition(&encode_transition(&guarded)?)?, guarded);
    // No-effect transitions are representable; only soft effects without
    // mutations refuse, since replay could not show whether they applied.
    let mut empty = transition;
    empty.effects.clear();
    assert_eq!(decode_transition(&encode_transition(&empty)?)?, empty);
    // An unknown field the host would otherwise ignore.
    let mut unknown = bytes.clone();
    // The greatest legal protobuf field number is outside this schema.
    unknown.extend_from_slice(&[0xf8, 0xff, 0xff, 0xff, 0x0f, 0x01]);
    assert!(decode_transition(&unknown).is_err());
    // A subscription change names its id and exactly one canonical change.
    for change in [
        SubscriptionChangeWire {
            id: String::new(),
            change: Some(ChangeWire::Remove(true)),
        },
        SubscriptionChangeWire {
            id: "watch".into(),
            change: None,
        },
        SubscriptionChangeWire {
            id: "watch".into(),
            change: Some(ChangeWire::Add(AddWire {
                path: "/not/a/path".into(),
                from: None,
            })),
        },
    ] {
        let mut forged: TransitionWire = canonical(&bytes, "transition", TRANSITION_DOMAIN)?;
        forged.subscriptions = vec![change];
        assert!(matches!(
            decode_transition(&encode(&forged, TRANSITION_DOMAIN)),
            Err(GuestError::Decode(_))
        ));
    }
    // A repeated message field decodes (merged), but is not canonical.
    let mut duplicated = bytes;
    duplicated.extend_from_slice(&[0x0a, 0x03, 0x0a, 0x01, b'x']);
    assert!(decode_transition(&duplicated).is_err());
    Ok(())
}

#[test]
fn version_refusal_precedes_guest_payload_decode() {
    type Decoder = fn(&[u8]) -> Result<(), GuestError>;
    let formats: [(&[u8], Decoder); 3] = [
        (MANIFEST_DOMAIN, |bytes| decode_manifest(bytes).map(|_| ())),
        (DELIVERY_DOMAIN, |bytes| decode_delivery(bytes).map(|_| ())),
        (TRANSITION_DOMAIN, |bytes| {
            decode_transition(bytes).map(|_| ())
        }),
    ];
    for (domain, decode) in formats {
        for refused in [0, 1, 2, u8::MAX] {
            if [refused] == version(domain) {
                continue;
            }
            let mut bytes = domain.to_vec();
            bytes.push(refused);
            assert_eq!(decode(&bytes), Err(GuestError::UnsupportedFormat));
        }
        let mut current = domain.to_vec();
        current.extend_from_slice(version(domain));
        assert!(matches!(decode(&current), Err(GuestError::Decode(_))));
    }
}

#[test]
fn delivery_round_trips_and_refuses_unknown_fields() -> Result<(), Box<dyn std::error::Error>> {
    let delivery = Delivery {
        request: crate::guest::RequestId::new(1),
        actor: ActorReference::new(b"account", b"namespace", b"actor")?,
        epoch: 2,
        trigger: Trigger::Alarm {
            generation: 4,
            now_unix_ms: 9,
        },
        now_unix_ms: 9,
        record: None,
        subscriptions: SubscriptionCuts::new(
            StreamPath::new("actors/original-state")?,
            CommitId::from_bytes([17; 32]),
            [
                (
                    "empty".into(),
                    Subscription {
                        path: StreamPath::new("inputs/empty")?,
                        paused: true,
                        cursor: 0,
                    },
                ),
                (
                    "inbox".into(),
                    Subscription {
                        path: StreamPath::new("inputs/inbox")?,
                        paused: false,
                        cursor: 42,
                    },
                ),
            ]
            .into(),
        ),
    };
    let committed = transition()?.checkpoint;
    let mut bytes = encode_delivery(&delivery, Some(&committed));
    assert_eq!(decode_delivery(&bytes)?, (delivery, Some(committed)));
    bytes.extend_from_slice(&[0x78, 0x01]);
    assert!(decode_delivery(&bytes).is_err());
    Ok(())
}

/// A delivery and a transition carrying a checkpoint at the admitted
/// maximum, with every other field at its Streams limit, fit the bound.
///
/// Negative control: a checkpoint larger than admitted by the envelope
/// does not, so the bound follows the admitted checkpoint size.
#[test]
fn envelope_bound_holds_at_the_admitted_checkpoint_maximum()
-> Result<(), Box<dyn std::error::Error>> {
    const ADMITTED: u64 = 1 << 20;
    let bound = envelope_bytes(Some(ADMITTED));
    let checkpoint = |bytes: u64| -> Result<Checkpoint, Box<dyn std::error::Error>> {
        Ok(Checkpoint {
            codec: CodecIdentity {
                name: "codec".into(),
                version: "1".into(),
            },
            bytes: vec![7; usize::try_from(bytes)?].into(),
        })
    };
    let record = Bytes::from(vec![1; acyclic_stream::MAX_RECORD_BYTES]);
    let delivery = Delivery {
        request: crate::guest::RequestId::new(u64::MAX),
        actor: ActorReference::new(b"account", b"namespace", b"actor")?,
        epoch: u64::MAX,
        trigger: Trigger::Record {
            subscription: "inbox".into(),
            sequence: u64::MAX,
        },
        now_unix_ms: u64::MAX,
        record: Some(acyclic_stream::Record {
            sequence: u64::MAX,
            value: record.clone().into(),
            commit_id: CommitId::from_bytes([9; 32]),
            committed_at_micros: u64::MAX,
            headers: Default::default(),
            origin: StreamPath::new("agents/a/inbox")?,
        }),
        subscriptions: SubscriptionCuts::new(
            StreamPath::new("actors/state")?,
            CommitId::from_bytes([17; 32]),
            [(
                "inbox".into(),
                Subscription {
                    path: StreamPath::new("agents/a/inbox")?,
                    paused: false,
                    cursor: u64::MAX,
                },
            )]
            .into(),
        ),
    };
    let admitted = checkpoint(ADMITTED)?;
    assert!(encode_delivery(&delivery, Some(&admitted)).len() as u64 <= bound);
    let path = StreamPath::new("agents/a/outbox")?;
    let records = acyclic_stream::MAX_COMMAND_BYTES / acyclic_stream::MAX_RECORD_BYTES - 1;
    let mut transition = transition()?;
    transition.response = Some(record.clone());
    transition.checkpoint = admitted;
    transition.effects = vec![acyclic_stream::CommitEffect {
        conditions: Vec::new(),
        mutations: vec![CommitMutation::Append {
            path,
            records: vec![record.clone().into(); records],
        }],
        soft: false,
    }];
    transition.participant = Some(Participant {
        kind: "spawn".into(),
        plan: record,
    });
    assert!(encode_transition(&transition)?.len() as u64 <= bound);
    transition.checkpoint = checkpoint(ADMITTED + ENVELOPE_BYTES)?;
    assert!(encode_transition(&transition)?.len() as u64 > bound);
    Ok(())
}

#[test]
fn delivery_refuses_ambiguous_subscription_cuts() -> Result<(), Box<dyn std::error::Error>> {
    let delivery = Delivery {
        request: crate::guest::RequestId::new(1),
        actor: ActorReference::new(b"account", b"namespace", b"actor")?,
        epoch: 2,
        trigger: Trigger::Alarm {
            generation: 4,
            now_unix_ms: 9,
        },
        now_unix_ms: 9,
        record: None,
        subscriptions: SubscriptionCuts::new(
            StreamPath::new("actors/original-state")?,
            CommitId::from_bytes([17; 32]),
            [
                (
                    "a".into(),
                    Subscription {
                        path: StreamPath::new("inputs/a")?,
                        paused: false,
                        cursor: 0,
                    },
                ),
                (
                    "b".into(),
                    Subscription {
                        path: StreamPath::new("inputs/b")?,
                        paused: false,
                        cursor: 42,
                    },
                ),
            ]
            .into(),
        ),
    };
    let bytes = encode_delivery(&delivery, None);
    let mut wire: DeliveryWire = canonical(&bytes, "delivery", DELIVERY_DOMAIN)?;
    let original_path = std::mem::take(&mut wire.state_path);
    assert!(decode_delivery(&encode(&wire, DELIVERY_DOMAIN)).is_err());
    wire.state_path = original_path;
    wire.state_commit.clear();
    assert!(decode_delivery(&encode(&wire, DELIVERY_DOMAIN)).is_err());
    wire.state_commit = Bytes::from_static(&[17; 32]);
    wire.subscriptions.swap(0, 1);
    assert!(decode_delivery(&encode(&wire, DELIVERY_DOMAIN)).is_err());
    wire.subscriptions[1] = wire.subscriptions[0].clone();
    assert!(decode_delivery(&encode(&wire, DELIVERY_DOMAIN)).is_err());
    wire.subscriptions.truncate(1);
    wire.subscriptions[0].path = String::new();
    assert!(decode_delivery(&encode(&wire, DELIVERY_DOMAIN)).is_err());
    Ok(())
}
