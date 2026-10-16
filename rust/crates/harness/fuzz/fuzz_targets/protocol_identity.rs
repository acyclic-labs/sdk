//! Fuzz protocol validation and protobuf round-trip stability.
#![no_main]

use acyclic_harness::{core::Action, wire, wire_api, wire_codec};
use libfuzzer_sys::fuzz_target;
use prost::Message as _;

fn canonical_action_matches_reference(bytes: &[u8]) {
    let Ok(action) = serde_json::from_slice::<Action>(bytes) else {
        return;
    };
    let Ok(mut value) = serde_json::to_value(&action) else {
        return;
    };
    value.sort_all_objects();
    let Ok(encoded) = serde_json::to_vec(&value) else {
        panic!("invalid canonical Value");
    };
    let Ok(mut probe) = wire::CommandEnvelope::decode(
        include_bytes!("../corpus/protocol_identity/command-valid").as_slice(),
    ) else {
        panic!("invalid command seed");
    };
    probe.protocol = Some(wire_api::current_protocol());
    probe.action_type = value
        .get("kind")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_else(|| unreachable!("serialized Action has a kind tag"))
        .into();
    probe.canonical_action_json = bytes.to_vec();
    let result = wire_codec::decode_command_envelope(probe);
    assert_eq!(result.is_ok(), encoded == bytes);
}

fn event_protocol_round_trip(data: &[u8]) {
    if data.len() > acyclic_stream::MAX_RECORD_BYTES {
        return;
    }
    let Ok(event) = wire::EventEnvelope::decode(data) else {
        return;
    };
    let expected = match event.protocol.as_ref() {
        None => Err(wire::ErrorCode::Storage),
        Some(protocol) if protocol != &wire_api::current_protocol() => {
            Err(wire::ErrorCode::Unsupported)
        }
        Some(_) => Ok(()),
    };
    let canonical = event.encode_to_vec();
    let raw = read_event(data).map_err(|error| (error.code(), error.to_string()));
    if data == include_bytes!("../corpus/protocol_identity/event-valid") {
        assert!(
            raw.is_ok(),
            "valid event seed must reach attested history decoding"
        );
    }
    if let Err(code) = expected {
        assert!(matches!(&raw, Err((actual, _)) if *actual == code));
    }
    if data != canonical {
        assert!(raw.is_err(), "noncanonical event bytes were admitted");
    }
    let restored = wire::EventEnvelope::decode(canonical.as_slice());
    assert!(matches!(&restored, Ok(restored) if restored == &event));
    if let Ok(restored) = restored {
        assert_eq!(
            read_event(&canonical).map_err(|error| (error.code(), error.to_string())),
            read_event(&restored.encode_to_vec())
                .map_err(|error| (error.code(), error.to_string())),
        );
    }
}

fn read_event(data: &[u8]) -> acyclic_harness::Result<acyclic_harness::store::HistoryPage> {
    use acyclic_harness::{
        core::{AggregateKind, Authority, AuthorityIssuer},
        store::{HistoryReadLimits, HistoryReader},
    };
    use acyclic_stream::{MemoryStream, Stream};
    use std::sync::Arc;

    futures::executor::block_on(async {
        let authority = Authority {
            kind: AggregateKind::Conversation,
            id: "conversation-1".into(),
        };
        let issuer = AuthorityIssuer::new("test", [7; 32], authority.clone());
        let client = Arc::new(MemoryStream::default());
        let stream = Stream::new(client.clone(), authority.stream_path(&mut None)?)?;
        // The canonical fixture is revision two. Its predecessor is outside the pinned page.
        stream.append(Vec::new()).await?;
        stream.append(data.to_vec()).await?;
        let reader =
            HistoryReader::new(&client, &authority, &issuer.verifier(&mut None)?, &mut None)?;
        let cursor = reader.pin(1, None).await?;
        reader
            .read_page(
                &cursor,
                HistoryReadLimits {
                    maximum_events: 1,
                    maximum_bytes: acyclic_stream::MAX_RECORD_BYTES as u64,
                },
                None,
            )
            .await
    })
}

fuzz_target!(|data: &[u8]| {
    event_protocol_round_trip(data);
    let Ok(command) = wire::CommandEnvelope::decode(data) else {
        return;
    };
    canonical_action_matches_reference(&command.canonical_action_json);
    let expected = match command.protocol.as_ref() {
        None => Err(wire::ErrorCode::Invalid),
        Some(protocol) if protocol != &wire_api::current_protocol() => {
            Err(wire::ErrorCode::Unsupported)
        }
        Some(_) => Ok(()),
    };
    assert_eq!(
        wire_api::validate_command_protocol(&command).map_err(|error| error.code()),
        expected
    );
    let decoded = wire_codec::decode_command_envelope(command.clone())
        .map_err(|error| (error.code(), error.to_string()));
    if data == include_bytes!("../corpus/protocol_identity/command-valid")
        || data == include_bytes!("../corpus/protocol_identity/command-nested")
    {
        assert!(decoded.is_ok(), "valid seed must reach action decoding");
    }
    if let Err(code) = expected {
        assert!(matches!(&decoded, Err((actual, _)) if *actual == code));
    }
    let restored = wire::CommandEnvelope::decode(command.encode_to_vec().as_slice());
    assert!(matches!(&restored, Ok(restored) if restored == &command));
    if let Ok(restored) = restored {
        assert_eq!(
            wire_api::validate_command_protocol(&restored).map_err(|error| error.code()),
            expected
        );
        assert_eq!(
            wire_codec::decode_command_envelope(restored)
                .map_err(|error| (error.code(), error.to_string())),
            decoded
        );
    }
    let resume = wire::ResumeRequest {
        protocol: command.protocol.clone(),
        ..Default::default()
    };
    assert_eq!(
        wire_api::validate_resume_protocol(&resume).map_err(|error| error.code()),
        expected
    );
    let owner = wire::Authority {
        kind: wire::AggregateKind::Task as i32,
        id: "owner".into(),
    };
    let status = wire::OperationStatus {
        protocol: command.protocol,
        owner: Some(owner.clone()),
        operation: Some(wire::OperationIdentity {
            operation_id: "operation".into(),
            idempotency_key: String::new(),
        }),
        state: wire::CompletionState::Running as i32,
        ..Default::default()
    };
    let expected = expected.map_err(|code| {
        if code == wire::ErrorCode::Invalid {
            wire::ErrorCode::Conflict
        } else {
            code
        }
    });
    assert_eq!(
        wire_api::validate_operation_status("operation", Some(&owner), &status)
            .map_err(|error| error.code()),
        expected
    );
});
