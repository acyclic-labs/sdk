use super::*;
use prost::Message as _;

use super::*;
use crate::conversation::{
    ConversationMessage, FileDescriptor, FileRef, MessageKind, VolumeClass, VolumeOwner, VolumeRef,
};
use crate::core::{Action, ApplyResult, AuthorityIssuer, Command, Reducer, SchemaRegistry};
use crate::resources::ProviderRef;
use crate::{AgentId, Capabilities, IdempotencyKey, OperationId};
use std::collections::BTreeMap;
use uuid::Uuid;

#[test]
fn protocol_digest_and_error_semantics_are_preserved() -> Result<()> {
    let event = Event {
        revision: 1,
        operation_id: OperationId::from_bytes([1; 16]),
        intent_digest: [2; 32],
        scope: RecordedScope::from_wire(
            "scope".into(),
            Capabilities::new(["event:append"]),
            "issuer".into(),
            None,
        ),
        attestation: [3; 32],
        causal_parent: None,
        payload: EventPayload::Custom {
            record: crate::core::ExtensionRecord {
                name: "example.event".into(),
                version: 1,
                schema_digest: [4; 32],
                implementation_digest: [5; 32],
                fork_policy: crate::core::ExtensionForkPolicy::Inherit,
                content: FileRef::new(
                    VolumeRef::new(
                        ProviderRef::new(
                            "test".into(),
                            "filesystem".into(),
                            "2".into(),
                            &mut None,
                        )?,
                        "extension".into(),
                        VolumeClass::AgentPrivate,
                        VolumeOwner::Agent(AgentId::from_bytes([9; 16])),
                        &mut None,
                    )?,
                    "extension/event.json".into(),
                    "generation-1".into(),
                    FileDescriptor::from_bytes(b"null", "application/json", &mut None)?,
                    "event.json".into(),
                    &mut None,
                )?,
            },
        },
    };
    let authority = Authority {
        kind: AggregateKind::Task,
        id: "task-1".into(),
    };
    let bytes = encode_event(&authority, &event, &mut None)?;
    assert_eq!(
        bytes,
        encode_event_envelope(&authority, &event)?.encode_to_vec()
    );
    let mut envelope = wire::EventEnvelope::decode(bytes.as_slice())
        .map_err(|error| Error::Storage(error.to_string()))?;
    envelope
        .protocol
        .as_mut()
        .ok_or_else(|| Error::Storage("protocol".into()))?
        .version = "1".into();
    assert!(matches!(
        decode_event(&envelope.encode_to_vec(), &mut None),
        Err(Error::Unsupported(_))
    ));
    envelope
        .protocol
        .as_mut()
        .ok_or_else(|| Error::Storage("protocol".into()))?
        .version = "2".into();
    envelope
        .protocol
        .as_mut()
        .ok_or_else(|| Error::Storage("protocol".into()))?
        .descriptor_digest = "wrong".into();
    assert!(matches!(
        decode_event(&envelope.encode_to_vec(), &mut None),
        Err(Error::Unsupported(_))
    ));

    let operation = OperationId::from_bytes([8; 16]);
    let encoded = encode_error(&Error::Indeterminate(operation));
    assert_eq!(encoded.code, wire::ErrorCode::Indeterminate as i32);
    assert_eq!(encoded.operation_id, operation.to_string());
    for (reason, code) in [
        (
            crate::InteractionRejection::Declined,
            wire::ErrorCode::InteractionDeclined,
        ),
        (
            crate::InteractionRejection::Cancelled,
            wire::ErrorCode::InteractionCancelled,
        ),
        (
            crate::InteractionRejection::Expired,
            wire::ErrorCode::InteractionExpired,
        ),
        (
            crate::InteractionRejection::Denied,
            wire::ErrorCode::InteractionDenied,
        ),
    ] {
        assert_eq!(
            encode_error(&Error::InteractionRejected(reason)).code,
            code as i32
        );
    }
    Ok(())
}

#[test]
fn native_event_bytes_match_cross_language_fixture() -> Result<()> {
    let authority = Authority {
        kind: AggregateKind::Conversation,
        id: "conversation-1".into(),
    };
    let issuer = AuthorityIssuer::new("test", [7; 32], authority.clone());
    let mut reducer = Reducer::new(
        authority.clone(),
        issuer.verifier(&mut None).expect("verifier copy"),
        SchemaRegistry::new(),
    );
    let agent = AgentId::from_bytes([4; 16]);
    let scope = issuer
        .issue(
            "root",
            &Capabilities::new(["conversation:bind", "conversation:append"]),
            None,
            None,
            &mut None,
        )
        .expect("scope proof");
    reducer.apply(Command {
        operation_id: OperationId::from_bytes([1; 16]),
        idempotency_key: IdempotencyKey("bind-1".into()),
        expected_revision: 0,
        scope: scope.clone(),
        causal_parent: None,
        action: Action::BindConversation { agent },
    })?;
    let file = FileRef::new(
        VolumeRef::new(
            ProviderRef::new("fixture".into(), "filesystem".into(), "2".into(), &mut None)?,
            "scratch".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
            &mut None,
        )?,
        "messages/input.txt".into(),
        "pinned-generation".into(),
        FileDescriptor::from_bytes(b"fixture text", "text/plain", &mut None)?,
        "input.txt".into(),
        &mut None,
    )?;
    let command = Command {
        operation_id: OperationId::from_bytes([2; 16]),
        idempotency_key: IdempotencyKey("append-2".into()),
        expected_revision: 1,
        scope,
        causal_parent: None,
        action: Action::AppendConversationMessage {
            message: Box::new(ConversationMessage {
                id: Uuid::from_bytes([3; 16]),
                sequence: 1,
                kind: MessageKind::User,
                content: file,
                attachments: crate::conversation::ReferencedAttachments::Inline {
                    items: Vec::new(),
                },
                reply_to: None,
                tool_call_id: None,
                extensions: BTreeMap::new(),
            }),
        },
    };
    let ApplyResult::Applied { event } = reducer.apply(command.clone())? else {
        return Err(Error::Conflict("first vector application replayed".into()));
    };
    let ApplyResult::Replayed { event: replayed } = reducer.apply(command)? else {
        return Err(Error::Conflict("vector retry was not replayed".into()));
    };
    let native = encode_event(&authority, &event, &mut None)?;
    assert_eq!(native, encode_event(&authority, &replayed, &mut None)?);
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../conformance/native-wasm-event-v2.json"))
            .map_err(|error| Error::Invalid(error.to_string()))?;
    let expected = fixture
        .get("event_wire_hex")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| Error::Invalid("native/WASM fixture is missing event bytes".into()))?;
    let actual = native
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    assert_eq!(
        actual, expected,
        "replace the native/WASM fixture with: {actual}"
    );
    Ok(())
}

#[test]
fn native_command_envelopes_require_exact_payload_protocol_and_retry_identity() -> Result<()> {
    let action = Action::TransitionLifecycle {
        to: crate::core::LifecycleState::Active,
        reason: None,
    };
    let envelope = wire::CommandEnvelope {
        protocol: Some(protocol_identity()),
        authority: Some(wire::Authority {
            kind: wire::AggregateKind::Task as i32,
            id: "native-source".into(),
        }),
        operation: Some(wire::OperationIdentity {
            operation_id: OperationId::from_bytes([31; 16]).to_string(),
            idempotency_key: "native-once".into(),
        }),
        scope: Some(wire::Scope {
            id: "explicit-producer-scope".into(),
            issuer: "producer".into(),
            capabilities: vec!["lifecycle:transition".into()],
            proof: vec![7; 32],
            ..wire::Scope::default()
        }),
        action_type: "transition_lifecycle".into(),
        canonical_action_json: canonical_json_bytes(&action)?,
        ..wire::CommandEnvelope::default()
    };
    let (authority, command) = decode_command_envelope(envelope.clone())?;
    assert_eq!(authority.kind, AggregateKind::Task);
    assert_eq!(command.operation_id, OperationId::from_bytes([31; 16]));
    assert_eq!(command.idempotency_key, IdempotencyKey::new("native-once")?);
    let mut wrong = envelope.clone();
    wrong.canonical_action_json =
        serde_json::to_vec_pretty(&action).map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(matches!(
        decode_command_envelope(wrong),
        Err(Error::Invalid(_))
    ));
    let mut wrong = envelope.clone();
    wrong.action_type = "append_custom".into();
    assert!(matches!(
        decode_command_envelope(wrong),
        Err(Error::Invalid(_))
    ));
    let mut wrong = envelope.clone();
    wrong
        .operation
        .as_mut()
        .ok_or_else(|| Error::Invalid("operation missing".into()))?
        .idempotency_key
        .clear();
    assert!(matches!(
        decode_command_envelope(wrong),
        Err(Error::Invalid(_))
    ));
    let mut wrong = envelope;
    wrong.intent_digest = vec![0; 32];
    assert!(matches!(
        decode_command_envelope(wrong),
        Err(Error::Conflict(_))
    ));
    Ok(())
}
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod native_custody_controls {
    use super::*;
    use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};

    fn budget() -> WorkBudget {
        WorkBudget {
            source_bytes_read: 16_777_216,
            bytes_copied: 16_777_216,
            bytes_encoded: 16_777_216,
            bytes_hashed: 16_777_216,
            items_examined: 1_000_000,
            allocation_operations: 10_000,
            peak_allocation_bytes: 16_777_216,
            ..WorkBudget::default()
        }
    }

    fn fixture() -> (Authority, Event) {
        (
            Authority {
                kind: AggregateKind::Conversation,
                id: "native-original".into(),
            },
            Event {
                revision: 1,
                operation_id: OperationId::from_bytes([42; 16]),
                intent_digest: [43; 32],
                attestation: [44; 32],
                causal_parent: None,
                scope: RecordedScope::from_wire(
                    "scope".into(),
                    Capabilities::new(["conversation:bind"]),
                    "issuer".into(),
                    None,
                ),
                payload: EventPayload::ConversationBound {
                    agent: AgentId::from_bytes([45; 16]),
                },
            },
        )
    }

    #[test]
    fn native_event_round_trip_releases_custody() -> Result<()> {
        let (authority, event) = fixture();
        let bytes = encode_event(&authority, &event, &mut None)?;
        let cancellation = CancellationToken::new();
        let work =
            NativeMaterializationWork::new(budget(), &cancellation).expect("original ledger");
        let baseline = work.live_allocation_bytes();
        let mut io = crate::store::history::native_work_io(&work).expect("original observer");
        let ((decoded_authority, decoded_event), _) = decode_event(&bytes, &mut Some(&mut io))?;
        assert_eq!((&decoded_authority, &decoded_event), (&authority, &event));
        let custody = io.into_custody();
        drop(decoded_event);
        drop(decoded_authority);
        drop(custody);
        assert_eq!(work.live_allocation_bytes(), baseline);
        Ok(())
    }

    #[test]
    fn native_quota_precedes_allocating_original_deserialize_failure() -> Result<()> {
        let (authority, event) = fixture();
        let mut envelope = encode_event_envelope(&authority, &event)?;
        envelope.canonical_payload_json = format!(
            r#"{{"type":"not_an_event","untrusted":["{}"]}}"#,
            "x".repeat(4096)
        )
        .into_bytes();
        assert!(serde_json::from_slice::<EventPayload>(&envelope.canonical_payload_json).is_err());
        let bytes = envelope.encode_to_vec();
        let carrier = admission::decode_quota(&bytes, &mut None)?;
        let mut allowance = budget();
        allowance.peak_allocation_bytes =
            NativeMaterializationWork::accounting_state_bytes() + carrier + 8192;
        let cancellation = CancellationToken::new();
        let work =
            NativeMaterializationWork::new(allowance, &cancellation).expect("original ledger");
        let mut io = crate::store::history::native_work_io(&work).expect("original observer");
        let error = decode_event(&bytes, &mut Some(&mut io))
            .expect_err("closed payload admission must deny before original Deserialize");
        assert!(!matches!(
            error,
            Error::NativeDecode { .. } | Error::NativeProstDecode { .. }
        ));
        drop(error);
        assert_eq!(io.into_custody().bytes(), 0);
        Ok(())
    }

    #[test]
    fn native_partial_payload_cause_keeps_original_guard_until_cause_drop() -> Result<()> {
        let (authority, event) = fixture();
        let mut envelope = encode_event_envelope(&authority, &event)?;
        envelope.canonical_payload_json =
            br#"{"type":"conversation_bound","agent":"not-a-UUID"}"#.to_vec();
        let original = serde_json::from_slice::<EventPayload>(&envelope.canonical_payload_json)
            .expect_err("original concrete UUID parser rejects data");
        let bytes = envelope.encode_to_vec();
        let cancellation = CancellationToken::new();
        let work =
            NativeMaterializationWork::new(budget(), &cancellation).expect("original ledger");
        let baseline = work.live_allocation_bytes();
        let mut io = crate::store::history::native_work_io(&work).expect("original observer");
        let error = decode_event(&bytes, &mut Some(&mut io)).expect_err("original typed cause");
        let Error::NativeDecode {
            code,
            source,
            custody: Some(custody),
        } = error
        else {
            panic!("typed original source and original guard required");
        };
        assert_eq!(code, wire::ErrorCode::Storage);
        assert_eq!(source.classify(), original.classify());
        assert!(work.shares_ledger(custody.work()));
        assert!(custody.bytes() > 0);
        assert_eq!(io.into_custody().bytes(), 0);
        assert_eq!(work.live_allocation_bytes(), baseline + custody.bytes());
        drop(source);
        assert_eq!(work.live_allocation_bytes(), baseline + custody.bytes());
        drop(custody);
        assert_eq!(work.live_allocation_bytes(), baseline);
        Ok(())
    }

    #[test]
    fn native_prost_partial_carrier_retains_actual_decode_cause() -> Result<()> {
        let (authority, event) = fixture();
        let mut bytes = encode_event(&authority, &event, &mut None)?;
        bytes.extend_from_slice(&[0x22, 0x01, 0xff]);
        let cancellation = CancellationToken::new();
        let work =
            NativeMaterializationWork::new(budget(), &cancellation).expect("original ledger");
        let baseline = work.live_allocation_bytes();
        let mut io = crate::store::history::native_work_io(&work).expect("original observer");
        let error = decode_event(&bytes, &mut Some(&mut io)).expect_err("original prost failure");
        let Error::NativeProstDecode {
            code,
            source,
            custody: Some(custody),
        } = error
        else {
            panic!("original prost source and guard required");
        };
        assert_eq!(code, wire::ErrorCode::Storage);
        assert_eq!(io.into_custody().bytes(), 0);
        assert!(custody.bytes() > 0);
        assert_eq!(work.live_allocation_bytes(), baseline + custody.bytes());
        drop(source);
        drop(custody);
        assert_eq!(work.live_allocation_bytes(), baseline);
        Ok(())
    }

    #[test]
    fn native_canonical_tamper_retires_all_original_carriers() -> Result<()> {
        let (authority, event) = fixture();
        let mut bytes = encode_event(&authority, &event, &mut None)?;
        bytes.extend_from_slice(&[0x78, 0x01]);
        let cancellation = CancellationToken::new();
        let work =
            NativeMaterializationWork::new(budget(), &cancellation).expect("original ledger");
        let baseline = work.live_allocation_bytes();
        let mut io = crate::store::history::native_work_io(&work).expect("original observer");
        let error = decode_event(&bytes, &mut Some(&mut io)).expect_err("canonical tamper");
        assert!(matches!(
            error,
            Error::NativeBoundary {
                code: wire::ErrorCode::Storage,
                ..
            }
        ));
        drop(error);
        assert_eq!(io.into_custody().bytes(), 0);
        assert_eq!(work.live_allocation_bytes(), baseline);
        Ok(())
    }
}
