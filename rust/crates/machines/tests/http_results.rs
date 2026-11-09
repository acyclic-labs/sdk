//! Canonical Machines result routes and streaming JSON boundary conformance.
#![cfg(feature = "http-codec")]

use acyclic_machines::{
    MachinesMethod,
    http_codec::{HTTP_ROUTES, decode_http_request, encode_http_response},
    wire,
};
use prost::Message as _;

#[test]
fn result_routes_preserve_cuts_and_stream_canonical_json_events()
-> Result<(), Box<dyn std::error::Error>> {
    let read = HTTP_ROUTES
        .iter()
        .find(|route| route.method == MachinesMethod::ReadResults)
        .unwrap();
    let follow = HTTP_ROUTES
        .iter()
        .find(|route| route.method == MachinesMethod::FollowEvents)
        .unwrap();
    assert!(!read.descriptor()?.is_server_streaming());
    assert!(follow.descriptor()?.is_server_streaming());
    let request = wire::ReadResultsRequest::decode(decode_http_request(
        "machines/read-results",
        br#"{"cursor":{"cut":{"count":"18446744073709551615","nativeRevision":"17"},"from":"18446744073709551614"},"credit":1}"#,
        1024,
    ).map_err(|error| format!("request codec: {error:?}"))?.as_slice())?;
    let cursor = request.cursor.unwrap();
    assert_eq!(cursor.from, u64::MAX - 1);
    assert_eq!(cursor.cut.as_ref().unwrap().count, u64::MAX);
    let page = wire::ResultPage {
        cursor: Some(cursor.clone()),
        machines: Vec::new(),
        next: Some(cursor),
    };
    let mut json = Vec::new();
    encode_http_response(read.path, &page.encode_to_vec(), &mut json)
        .map_err(|error| format!("result codec: {error:?}"))?;
    let value: serde_json::Value = serde_json::from_slice(&json)?;
    assert_eq!(value["cursor"]["cut"]["count"], u64::MAX.to_string());
    let mut body = Vec::new();
    for sequence in [u64::MAX - 1, u64::MAX] {
        let event = wire::MachineEvent {
            sequence,
            observed_at_unix_ms: sequence,
            kind: wire::EventKind::State as i32,
            state: wire::MachineStatus::Destroyed as i32,
            ..Default::default()
        };
        encode_http_response("machines/follow-events", &event.encode_to_vec(), &mut body)
            .map_err(|error| format!("event codec: {error:?}"))?;
        body.push(b'\n');
    }
    let lines = body
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(serde_json::from_slice::<serde_json::Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[1]["sequence"], u64::MAX.to_string());
    assert_eq!(lines[1]["state"], "MACHINE_STATUS_DESTROYED");
    assert!(decode_http_request(follow.path, b"{\"unknown\":true}", 1024).is_err());
    assert!(decode_http_request(follow.path, b"{}{}", 1024).is_err());
    assert!(decode_http_request("machines/unknown", b"{}", 1024).is_err());
    assert!(decode_http_request(follow.path, b"{}", 1).is_err());
    Ok(())
}
