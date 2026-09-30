//! Service consumers use the canonical encoder without transport/WASM features.
use acyclic_stream::{http_response, wire};
use bytes::Bytes;
use prost::Message;

fn envelope() -> wire::CommittedEnvelope {
    let id = Bytes::from(vec![1; 32]);
    wire::CommittedEnvelope {
        commit_id: id.clone(),
        mutations: vec![wire::CommittedMutation {
            mutation: Some(wire::committed_mutation::Mutation::Append(
                wire::CommittedAppend {
                    path: "events".into(),
                    start: 0,
                    end: 1,
                    tail: 1,
                    records: vec![wire::Record {
                        sequence: 0,
                        value: Bytes::from_static(b"actual"),
                        commit_id: id,
                        committed_at_micros: 9007199254740993,
                    }],
                },
            )),
        }],
    }
}

#[test]
fn commit_encoder_preserves_atomic_envelope_and_compact_compatibility()
-> Result<(), Box<dyn std::error::Error>> {
    let response = wire::CommitResponse {
        outcome: Some(wire::commit_response::Outcome::Committed(envelope())),
    };
    let protobuf = response.encode_to_vec();
    let encoded = http_response::encode("commit", &protobuf, 4096)?;
    let json: serde_json::Value = serde_json::from_slice(&encoded)?;
    assert_eq!(json.pointer("/tails/events"), Some(&serde_json::json!("1")));
    assert_eq!(json.get("commitId"), json.pointer("/envelope/commitId"));
    assert_eq!(
        json.pointer("/envelope/mutations/0/records/0/committedAtMicros"),
        Some(&serde_json::json!("9007199254740993"))
    );
    assert_eq!(
        http_response::encode("commit", &protobuf, encoded.len() - 1),
        Err("limit_exceeded")
    );
    assert_eq!(
        http_response::encode("commit", &protobuf, 0),
        Err("limit_exceeded")
    );
    Ok(())
}

#[test]
fn encoder_rejects_invalid_nested_record_and_preserves_absent_observation()
-> Result<(), Box<dyn std::error::Error>> {
    let mut envelope = envelope();
    let Some(wire::committed_mutation::Mutation::Append(append)) = &mut envelope
        .mutations
        .first_mut()
        .ok_or("missing fixture mutation")?
        .mutation
    else {
        return Err("missing fixture append".into());
    };
    append
        .records
        .first_mut()
        .ok_or("missing fixture record")?
        .commit_id = Bytes::from_static(b"bad");
    assert_eq!(
        http_response::encode("commits/read", &envelope.encode_to_vec(), 4096),
        Err("invalid_response")
    );
    assert_eq!(
        http_response::encode(
            "idempotency/inspect",
            &wire::InspectIdempotencyResponse::default().encode_to_vec(),
            4096
        )?,
        b"null"
    );
    Ok(())
}
