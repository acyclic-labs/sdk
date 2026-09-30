//! Native protobuf JSON boundary conformance.
#![cfg(feature = "http-codec")]

use acyclic_inference::{
    MAXIMUM_HTTP_JSON_BYTES, MAXIMUM_MESSAGE_BYTES,
    http_codec::{decode_http_request, encode_http_response, routes},
    wire,
};
use prost::Message;
use prost_reflect::DynamicMessage;

#[test]
fn every_declared_rpc_has_a_native_json_boundary() -> Result<(), Box<dyn std::error::Error>> {
    let routes = routes()?;
    assert_eq!(routes.len(), 14);
    let streaming = routes
        .iter()
        .filter(|route| route.method.is_server_streaming())
        .map(|route| route.path.as_str())
        .collect::<Vec<_>>();
    assert_eq!(streaming, ["runs/watch"]);
    for route in routes {
        let request = decode_http_request(&route.path, b"{}")?;
        DynamicMessage::decode(route.method.input(), request.as_slice())?;
        let response = DynamicMessage::new(route.method.output());
        assert_eq!(
            encode_http_response(&route.path, &response.encode_to_vec())?,
            b"{}"
        );
    }
    Ok(())
}

#[test]
fn watch_cursor_and_events_preserve_uint64_bytes_and_enums()
-> Result<(), Box<dyn std::error::Error>> {
    let request = decode_http_request(
        "runs/watch",
        br#"{"runId":"AQID","fromSequence":"18446744073709551615"}"#,
    )?;
    let request = wire::WatchRunRequest::decode(request.as_slice())?;
    assert_eq!(request.run_id, [1, 2, 3]);
    assert_eq!(request.from_sequence, u64::MAX);
    // Serialization does not claim identity-width or semantic admission checks.
    let event = wire::RunEvent {
        sequence: u64::MAX,
        event: Some(wire::run_event::Event::Terminal(
            wire::RunTerminal::Completed.into(),
        )),
    };
    let json = encode_http_response("runs/watch", &event.encode_to_vec())?;
    let value: serde_json::Value = serde_json::from_slice(&json)?;
    assert_eq!(
        value.get("sequence").and_then(serde_json::Value::as_str),
        Some(u64::MAX.to_string().as_str())
    );
    assert_eq!(
        value.get("terminal").and_then(serde_json::Value::as_str),
        Some("RUN_TERMINAL_COMPLETED")
    );
    assert!(!json.contains(&b'\n'));
    let event = wire::RunEvent {
        sequence: 1,
        event: Some(wire::run_event::Event::Output(vec![1, 2, 3])),
    };
    let json = encode_http_response("runs/watch", &event.encode_to_vec())?;
    let value: serde_json::Value = serde_json::from_slice(&json)?;
    assert_eq!(
        value.get("output").and_then(serde_json::Value::as_str),
        Some("AQID")
    );
    Ok(())
}

#[test]
fn malformed_unknown_and_oversized_inputs_fail_closed() {
    for json in [
        b"{".as_slice(),
        b"{} {}",
        b"{\"unknown\":true}",
        b"{\"fromSequence\":\"18446744073709551616\"}",
    ] {
        assert!(decode_http_request("runs/watch", json).is_err());
    }
    assert!(decode_http_request("/v1/inference/runs/watch", b"{}").is_err());
    assert!(decode_http_request("models/list", &vec![b' '; MAXIMUM_HTTP_JSON_BYTES + 1]).is_err());
    assert!(encode_http_response("runs/watch", &[255]).is_err());
    assert!(encode_http_response("runs/watch", &vec![0; MAXIMUM_MESSAGE_BYTES + 1]).is_err());
    // Escaped strings can fit the protobuf ceiling while exceeding the JSON ceiling.
    let event = wire::RunEvent {
        sequence: 1,
        event: Some(wire::run_event::Event::Progress(wire::RunProgress {
            kind: "\0".repeat(MAXIMUM_MESSAGE_BYTES / 2),
        })),
    };
    let wire = event.encode_to_vec();
    assert!(wire.len() < MAXIMUM_MESSAGE_BYTES);
    assert!(encode_http_response("runs/watch", &wire).is_err());
}
