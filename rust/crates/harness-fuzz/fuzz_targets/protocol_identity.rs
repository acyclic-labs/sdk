//! Fuzz protocol validation and protobuf round-trip stability.
#![no_main]

pub use acyclic_fuzz_support as fuzz_support;

use acyclic_harness::{core::Action, wire, wire_api, wire_codec};
use prost::Message as _;

fn canonical_action_matches_reference(fuzz_input: fuzz_support::FuzzInput<'_>) {
    let bytes = fuzz_input.bytes();
    let Some(action) = fuzz_support::FuzzInput::parse(fuzz_input, serde_json::from_slice::<Action>)
    else {
        return;
    };
    let mut value = fuzz_support::must_decode(serde_json::to_value(&action));
    value.sort_all_objects();
    let encoded = fuzz_support::must_decode(serde_json::to_vec(&value));
    let mut probe = fuzz_support::must_decode(wire::CommandEnvelope::decode(
        include_bytes!("../corpus/protocol_identity/command-valid").as_slice(),
    ));
    probe.protocol = Some(wire_api::current_protocol());
    probe.action_type = value
        .get("kind")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_else(|| unreachable!("serialized Action has a kind tag"))
        .into();
    probe.canonical_action_json = bytes.to_vec();
    let _ = fuzz_support::accepts(encoded == bytes, wire_codec::decode_command_envelope(probe));
}

fn event_protocol_round_trip(fuzz_input: fuzz_support::FuzzInput<'_>) {
    let data = fuzz_input.bytes();
    if data.len() > acyclic_stream::MAX_RECORD_BYTES {
        return;
    }
    let Some(event) = fuzz_support::FuzzInput::parse(fuzz_input, wire::EventEnvelope::decode)
    else {
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
    if let Some(code) = expected.as_ref().err() {
        assert_eq!(raw.as_ref().err().map(|(actual, _)| actual), Some(code));
    }
    if data != canonical {
        assert!(raw.is_err(), "noncanonical event bytes were admitted");
    }
    let restored =
        fuzz_support::round_trip(&event, wire::EventEnvelope::decode(canonical.as_slice()));
    {
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

fuzz_support::fuzz_target!(|data: &[u8], fuzz_input| {
    event_protocol_round_trip(fuzz_input);
    let Some(command) = fuzz_support::protobuf_input!(fuzz_input, wire::CommandEnvelope) else {
        return;
    };
    canonical_action_matches_reference(command.field(|command| &command.canonical_action_json));
    let decoded = fuzz_support::input_result(&command, |command| {
        wire_codec::decode_command_envelope(command.clone())
    })
    .map_err(|error| (error.code(), error.to_string()));
    let command = command.into_value();
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
    if data == include_bytes!("../corpus/protocol_identity/command-valid")
        || data == include_bytes!("../corpus/protocol_identity/command-nested")
    {
        assert!(decoded.is_ok(), "valid seed must reach action decoding");
    }
    if let Some(code) = expected.as_ref().err() {
        assert_eq!(decoded.as_ref().err().map(|(actual, _)| actual), Some(code));
    }
    let restored = fuzz_support::round_trip(
        &command,
        wire::CommandEnvelope::decode(command.encode_to_vec().as_slice()),
    );
    {
        assert_eq!(
            wire_api::validate_command_protocol(&restored).map_err(|error| error.code()),
            expected
        );
        let _ = fuzz_support::same_result(
            &decoded,
            wire_codec::decode_command_envelope(restored)
                .map_err(|error| (error.code(), error.to_string())),
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
