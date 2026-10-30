//! The canonical byte form of a [`Manifest`], a [`Delivery`] and its
//! [`Transition`], so one [`Handler`] runs in a guest while its host owns
//! scheduling and publication. [`Remote`] is the host's side: a handler that
//! calls the guest's `onRecord` with these bytes.
//!
//! Records, conditions and mutations use the Stream protobuf. Decoding is
//! canonical: input that does not re-encode to identical bytes (unknown
//! fields, reordered or non-minimal encodings) is refused, so a guest cannot
//! smuggle data the host would ignore.

use super::{
    AlarmChange, CancellationToken, Checkpoint, CodecIdentity, Delivery, Effect, GuestError,
    GuestFuture, Handler, Interrupt, Manifest, Participant, Subscription, SubscriptionCuts,
    Transition, Trigger,
};
use crate::{addressing::ActorReference, domain};
use acyclic_stream::{CommitId, CommitRequest, IdempotencyKey, StreamPath, wire, wire_codec};
use bytes::Bytes;
use prost::Message as _;

/// Fixed key of the encoded effect commit; publication supplies its own key.
const EFFECTS_KEY: &[u8] = b"acyclic.actors.transition.v1";

#[derive(Clone, PartialEq, prost::Message)]
struct DeliveryWire {
    #[prost(bytes = "bytes", tag = "1")]
    actor: Bytes,
    #[prost(uint64, tag = "2")]
    epoch: u64,
    #[prost(uint64, tag = "3")]
    now_unix_ms: u64,
    #[prost(oneof = "TriggerWire", tags = "4, 5")]
    trigger: Option<TriggerWire>,
    #[prost(message, optional, tag = "6")]
    record: Option<wire::Record>,
    /// The actor's last committed checkpoint, absent before its first transition.
    #[prost(message, optional, tag = "7")]
    checkpoint: Option<CheckpointWire>,
    #[prost(message, repeated, tag = "8")]
    subscriptions: Vec<SubscriptionWire>,
    #[prost(bytes = "bytes", tag = "9")]
    state_commit: Bytes,
    #[prost(string, tag = "10")]
    state_path: String,
}

#[derive(Clone, PartialEq, prost::Message)]
struct SubscriptionWire {
    #[prost(string, tag = "1")]
    id: String,
    #[prost(string, tag = "2")]
    path: String,
    #[prost(uint64, tag = "3")]
    cursor: u64,
}

#[derive(Clone, PartialEq, prost::Message)]
struct CheckpointWire {
    #[prost(string, tag = "1")]
    codec_name: String,
    #[prost(string, tag = "2")]
    codec_version: String,
    #[prost(bytes = "bytes", tag = "3")]
    bytes: Bytes,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ManifestWire {
    #[prost(bytes = "bytes", tag = "1")]
    code: Bytes,
    #[prost(string, tag = "2")]
    codec_name: String,
    #[prost(string, tag = "3")]
    codec_version: String,
    #[prost(string, repeated, tag = "4")]
    subscriptions: Vec<String>,
    #[prost(message, repeated, tag = "5")]
    bindings: Vec<crate::wire::Binding>,
    #[prost(message, optional, tag = "6")]
    limits: Option<crate::wire::ActorLimits>,
    #[prost(message, repeated, tag = "7")]
    interrupts: Vec<InterruptWire>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct InterruptWire {
    #[prost(string, tag = "1")]
    subscription: String,
    #[prost(bytes = "bytes", tag = "2")]
    value_prefix: Bytes,
}

#[derive(Clone, PartialEq, prost::Oneof)]
enum TriggerWire {
    #[prost(message, tag = "4")]
    Record(RecordTrigger),
    #[prost(message, tag = "5")]
    Alarm(AlarmTrigger),
}

#[derive(Clone, PartialEq, prost::Message)]
struct RecordTrigger {
    #[prost(string, tag = "1")]
    subscription: String,
    #[prost(uint64, tag = "2")]
    sequence: u64,
}

#[derive(Clone, PartialEq, prost::Message)]
struct AlarmTrigger {
    #[prost(uint64, tag = "1")]
    generation: u64,
    #[prost(uint64, tag = "2")]
    now_unix_ms: u64,
}

#[derive(Clone, PartialEq, prost::Message)]
struct TransitionWire {
    #[prost(message, optional, tag = "1")]
    checkpoint: Option<CheckpointWire>,
    #[prost(oneof = "AlarmWire", tags = "4, 5, 6")]
    alarm: Option<AlarmWire>,
    /// Every effect: conditions, then one mutation per effect.
    #[prost(message, optional, tag = "7")]
    effects: Option<wire::CommitRequest>,
    #[prost(message, optional, tag = "8")]
    participant: Option<ParticipantWire>,
    #[prost(bytes = "bytes", optional, tag = "9")]
    response: Option<Bytes>,
}

#[derive(Clone, PartialEq, prost::Oneof)]
enum AlarmWire {
    #[prost(bool, tag = "4")]
    Keep(bool),
    #[prost(uint64, tag = "5")]
    Set(u64),
    #[prost(bool, tag = "6")]
    Clear(bool),
}

#[derive(Clone, PartialEq, prost::Message)]
struct ParticipantWire {
    #[prost(string, tag = "1")]
    kind: String,
    #[prost(bytes = "bytes", tag = "2")]
    plan: Bytes,
}

fn invalid(field: &str) -> GuestError {
    GuestError::Decode(format!("non-canonical guest {field}"))
}

/// Decode `bytes` as `M`, refusing anything but its canonical encoding.
fn canonical<M: prost::Message + Default>(bytes: &[u8], field: &str) -> Result<M, GuestError> {
    let message = M::decode(bytes).map_err(|_| invalid(field))?;
    if message.encode_to_vec() != bytes {
        return Err(invalid(field));
    }
    Ok(message)
}

fn checkpoint_wire(checkpoint: &Checkpoint) -> CheckpointWire {
    CheckpointWire {
        codec_name: checkpoint.codec.name.clone(),
        codec_version: checkpoint.codec.version.clone(),
        bytes: checkpoint.bytes.clone(),
    }
}

fn checkpoint(wire: CheckpointWire) -> Checkpoint {
    Checkpoint {
        codec: CodecIdentity {
            name: wire.codec_name,
            version: wire.codec_version,
        },
        bytes: wire.bytes,
    }
}

/// Encode an artifact's manifest: code, codec, subscriptions, bindings and limits.
#[must_use]
pub fn encode_manifest(manifest: &Manifest) -> Vec<u8> {
    ManifestWire {
        code: Bytes::copy_from_slice(manifest.code.as_bytes()),
        codec_name: manifest.codec.name.clone(),
        codec_version: manifest.codec.version.clone(),
        subscriptions: manifest.subscriptions.clone(),
        interrupts: manifest
            .interrupts
            .iter()
            .map(|filter| InterruptWire {
                subscription: filter.subscription.clone(),
                value_prefix: filter.value_prefix.clone(),
            })
            .collect(),
        bindings: manifest.bindings.iter().cloned().map(Into::into).collect(),
        limits: Some(manifest.limits.into()),
    }
    .encode_to_vec()
}

/// Decode and admit an artifact's canonical manifest on its host.
pub fn decode_manifest(bytes: &[u8]) -> Result<Manifest, GuestError> {
    let wire: ManifestWire = canonical(bytes, "manifest")?;
    let limits = wire.limits.ok_or_else(|| invalid("limits"))?;
    let manifest = Manifest {
        code: domain::CodeSha256::try_from(wire.code.to_vec()).map_err(|_| invalid("code"))?,
        codec: CodecIdentity {
            name: wire.codec_name,
            version: wire.codec_version,
        },
        subscriptions: wire.subscriptions,
        interrupts: wire
            .interrupts
            .into_iter()
            .map(|filter| Interrupt {
                subscription: filter.subscription,
                value_prefix: filter.value_prefix,
            })
            .collect(),
        bindings: wire
            .bindings
            .into_iter()
            .map(domain::Binding::try_from)
            .collect::<Result<_, _>>()
            .map_err(|_| invalid("binding"))?,
        limits: domain::ActorLimits::try_from(limits).map_err(|_| invalid("limits"))?,
    };
    manifest.validate_interrupts()?;
    Ok(manifest)
}

/// Encode one delivery and the actor's committed checkpoint for a guest.
#[must_use]
pub fn encode_delivery(delivery: &Delivery, committed: Option<&Checkpoint>) -> Vec<u8> {
    DeliveryWire {
        actor: delivery.actor.encode().into(),
        epoch: delivery.epoch,
        now_unix_ms: delivery.now_unix_ms,
        trigger: Some(match &delivery.trigger {
            Trigger::Record {
                subscription,
                sequence,
            } => TriggerWire::Record(RecordTrigger {
                subscription: subscription.clone(),
                sequence: *sequence,
            }),
            Trigger::Alarm {
                generation,
                now_unix_ms,
            } => TriggerWire::Alarm(AlarmTrigger {
                generation: *generation,
                now_unix_ms: *now_unix_ms,
            }),
        }),
        // A delivery names its subscription, not a path, so it spells the origin.
        record: delivery
            .record
            .clone()
            .map(|record| wire_codec::record_wire(record, None)),
        checkpoint: committed.map(checkpoint_wire),
        state_commit: Bytes::copy_from_slice(delivery.subscriptions.anchor().as_bytes()),
        state_path: delivery.subscriptions.state_path().as_str().into(),
        subscriptions: delivery
            .subscriptions
            .iter()
            .map(|(id, cut)| SubscriptionWire {
                id: id.clone(),
                path: cut.path.as_str().into(),
                cursor: cut.cursor,
            })
            .collect(),
    }
    .encode_to_vec()
}

/// Decode one canonical delivery and committed checkpoint inside a guest.
pub fn decode_delivery(bytes: &[u8]) -> Result<(Delivery, Option<Checkpoint>), GuestError> {
    let wire: DeliveryWire = canonical(bytes, "delivery")?;
    if wire
        .subscriptions
        .windows(2)
        .any(|pair| pair[0].id >= pair[1].id)
    {
        return Err(invalid("subscription order"));
    }
    let subscriptions = wire
        .subscriptions
        .into_iter()
        .map(|cut| {
            Ok((
                cut.id,
                Subscription {
                    path: StreamPath::new(cut.path).map_err(|_| invalid("subscription path"))?,
                    cursor: cut.cursor,
                },
            ))
        })
        .collect::<Result<_, GuestError>>()?;
    let trigger = match wire.trigger.ok_or_else(|| invalid("trigger"))? {
        TriggerWire::Record(RecordTrigger {
            subscription,
            sequence,
        }) => Trigger::Record {
            subscription,
            sequence,
        },
        TriggerWire::Alarm(AlarmTrigger {
            generation,
            now_unix_ms,
        }) => Trigger::Alarm {
            generation,
            now_unix_ms,
        },
    };
    let delivery = Delivery {
        actor: ActorReference::decode(&wire.actor).map_err(|_| invalid("actor"))?,
        epoch: wire.epoch,
        trigger,
        now_unix_ms: wire.now_unix_ms,
        subscriptions: SubscriptionCuts::new(
            StreamPath::new(wire.state_path).map_err(|_| invalid("state path"))?,
            CommitId::from_bytes(
                wire.state_commit
                    .as_ref()
                    .try_into()
                    .map_err(|_| invalid("state commit"))?,
            ),
            subscriptions,
        ),
        record: wire
            .record
            .map(|record| wire_codec::record(record, None))
            .transpose()
            .map_err(|_| invalid("record"))?,
    };
    Ok((delivery, wire.checkpoint.map(checkpoint)))
}

/// Encode a guest's transition for its host.
pub fn encode_transition(transition: &Transition) -> Result<Vec<u8>, GuestError> {
    let mut effects = CommitRequest {
        deadline_unix_millis: None,
        conditions: Vec::new(),
        mutations: Vec::with_capacity(transition.effects.len()),
        idempotency_key: IdempotencyKey::new(EFFECTS_KEY.to_vec())
            .map_err(|error| GuestError::Encode(error.to_string()))?,
    };
    for effect in &transition.effects {
        effects.conditions.extend(effect.conditions.iter().cloned());
        effects.mutations.push(effect.mutation.clone());
    }
    Ok(TransitionWire {
        response: transition.response.clone(),
        checkpoint: Some(checkpoint_wire(&transition.checkpoint)),
        alarm: Some(match transition.alarm {
            AlarmChange::Keep => AlarmWire::Keep(true),
            AlarmChange::Set { deadline_unix_ms } => AlarmWire::Set(deadline_unix_ms),
            AlarmChange::Clear => AlarmWire::Clear(true),
        }),
        effects: Some(wire_codec::commit_to_wire(&effects)),
        participant: transition
            .participant
            .as_ref()
            .map(|participant| ParticipantWire {
                kind: participant.kind.clone(),
                plan: participant.plan.clone(),
            }),
    }
    .encode_to_vec())
}

/// Decode a guest's canonical transition on its host. Effects arrive as one
/// effect carrying every condition, which joins identically to the original.
pub fn decode_transition(bytes: &[u8]) -> Result<Transition, GuestError> {
    let wire: TransitionWire = canonical(bytes, "transition")?;
    let alarm = match wire.alarm.ok_or_else(|| invalid("alarm"))? {
        AlarmWire::Keep(true) => AlarmChange::Keep,
        AlarmWire::Set(deadline_unix_ms) => AlarmChange::Set { deadline_unix_ms },
        AlarmWire::Clear(true) => AlarmChange::Clear,
        AlarmWire::Keep(false) | AlarmWire::Clear(false) => return Err(invalid("alarm")),
    };
    let effects = wire_codec::commit_from_wire(wire.effects.ok_or_else(|| invalid("effects"))?)
        .map_err(|_| invalid("effects"))?;
    if effects.idempotency_key.as_bytes() != EFFECTS_KEY {
        return Err(invalid("effects"));
    }
    let mut conditions = Some(effects.conditions);
    Ok(Transition {
        response: wire.response,
        checkpoint: checkpoint(wire.checkpoint.ok_or_else(|| invalid("checkpoint"))?),
        alarm,
        effects: effects
            .mutations
            .into_iter()
            .map(|mutation| Effect {
                conditions: conditions.take().unwrap_or_default(),
                mutation,
            })
            .collect(),
        participant: wire.participant.map(|participant| Participant {
            kind: participant.kind,
            plan: participant.plan,
        }),
    })
}

/// The host's [`Handler`] for an artifact that runs in a guest: `call` passes
/// one encoded delivery to the guest's `onRecord` and returns its encoded
/// transition. Scheduling, timeouts and publication stay with the host's
/// runtime and [`crate::local::Store`].
pub struct Remote<F> {
    manifest: Manifest,
    call: F,
}

impl<F> Remote<F>
where
    F: Fn(Vec<u8>, CancellationToken) -> GuestFuture<'static, Vec<u8>>
        + acyclic_stream::ProviderPlatform,
{
    /// The guest of `manifest`, as [`decode_manifest`] admitted it.
    pub const fn new(manifest: Manifest, call: F) -> Self {
        Self { manifest, call }
    }
}

impl<F> Handler for Remote<F>
where
    F: Fn(Vec<u8>, CancellationToken) -> GuestFuture<'static, Vec<u8>>
        + acyclic_stream::ProviderPlatform,
{
    fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    fn on_record<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        cancellation: CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        let call = (self.call)(encode_delivery(&delivery, checkpoint), cancellation);
        Box::pin(async move { decode_transition(&call.await?) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_stream::{CommitCondition, CommitMutation, StreamPath};

    fn transition() -> Result<Transition, Box<dyn std::error::Error>> {
        let path = StreamPath::new("agents/a/inbox")?;
        Ok(Transition {
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
            effects: vec![Effect {
                conditions: vec![CommitCondition::Tail {
                    path: path.clone(),
                    expected: 3,
                }],
                mutation: CommitMutation::Append {
                    path,
                    records: vec![Bytes::from_static(b"record").into()],
                },
            }],
            participant: Some(Participant {
                kind: "spawn".into(),
                plan: Bytes::from_static(b"plan"),
            }),
        })
    }

    #[test]
    fn transition_round_trips_and_refuses_non_canonical_bytes()
    -> Result<(), Box<dyn std::error::Error>> {
        let transition = transition()?;
        let bytes = encode_transition(&transition)?;
        assert_eq!(decode_transition(&bytes)?, transition);
        // An unknown field the host would otherwise ignore.
        let mut unknown = bytes.clone();
        unknown.extend_from_slice(&[0x50, 0x01]);
        assert!(decode_transition(&unknown).is_err());
        // A repeated message field decodes (merged), but is not canonical.
        let mut duplicated = bytes;
        duplicated.extend_from_slice(&[0x0a, 0x03, 0x0a, 0x01, b'x']);
        assert!(decode_transition(&duplicated).is_err());
        Ok(())
    }

    #[test]
    fn delivery_round_trips_and_refuses_unknown_fields() -> Result<(), Box<dyn std::error::Error>> {
        let delivery = Delivery {
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
                            cursor: 0,
                        },
                    ),
                    (
                        "inbox".into(),
                        Subscription {
                            path: StreamPath::new("inputs/inbox")?,
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

    #[test]
    fn delivery_refuses_ambiguous_subscription_cuts() -> Result<(), Box<dyn std::error::Error>> {
        let delivery = Delivery {
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
                            cursor: 0,
                        },
                    ),
                    (
                        "b".into(),
                        Subscription {
                            path: StreamPath::new("inputs/b")?,
                            cursor: 42,
                        },
                    ),
                ]
                .into(),
            ),
        };
        let bytes = encode_delivery(&delivery, None);
        let mut wire = DeliveryWire::decode(bytes.as_slice())?;
        let original_path = std::mem::take(&mut wire.state_path);
        assert!(decode_delivery(&wire.encode_to_vec()).is_err());
        wire.state_path = original_path;
        wire.state_commit.clear();
        assert!(decode_delivery(&wire.encode_to_vec()).is_err());
        wire.state_commit = Bytes::from_static(&[17; 32]);
        wire.subscriptions.swap(0, 1);
        assert!(decode_delivery(&wire.encode_to_vec()).is_err());
        wire.subscriptions[1] = wire.subscriptions[0].clone();
        assert!(decode_delivery(&wire.encode_to_vec()).is_err());
        wire.subscriptions.truncate(1);
        wire.subscriptions[0].path = String::new();
        assert!(decode_delivery(&wire.encode_to_vec()).is_err());
        Ok(())
    }

    #[test]
    fn interrupt_filters_round_trip_and_refuse_empty_or_undeclared_prefixes()
    -> Result<(), Box<dyn std::error::Error>> {
        let package = crate::conformance::Package::new()?;
        let mut manifest = decode_manifest(&encode_manifest(package.manifest()))?;
        let encoded = encode_manifest(&manifest);
        assert_eq!(encode_manifest(&decode_manifest(&encoded)?), encoded);
        manifest.interrupts[0].value_prefix = Bytes::new();
        assert!(decode_manifest(&encode_manifest(&manifest)).is_err());
        manifest.interrupts[0].value_prefix = Bytes::from_static(b"stop");
        manifest.interrupts[0].subscription = "another-subscription".into();
        assert!(decode_manifest(&encode_manifest(&manifest)).is_err());
        Ok(())
    }
}
