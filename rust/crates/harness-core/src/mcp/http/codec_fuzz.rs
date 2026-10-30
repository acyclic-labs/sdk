//! Finite-input oracles over the production HTTP/SSE and JSON-RPC decoders.
use super::{McpHttpResponse, response};
use crate::{OperationId, mcp::rpc, wire::ErrorCode};
use bytes::Bytes;
use serde_json::Value;

fn parse(input: &[u8], width: usize, media: &str, maximum: u32) -> Result<Value, ErrorCode> {
    let body = futures::stream::unfold(
        Bytes::copy_from_slice(input),
        move |mut remaining| async move {
            if remaining.is_empty() {
                None
            } else {
                let chunk = remaining.split_to(width.min(remaining.len()));
                Some((Ok(chunk), remaining))
            }
        },
    );
    let response = McpHttpResponse {
        status: 200,
        headers: [("content-type".into(), media.into())].into(),
        body: Box::pin(body),
    };
    futures::executor::block_on(response::decode(
        response,
        OperationId::from_bytes([7; 16]),
        maximum,
        None,
    ))
    .map_err(|error| error.code())
}

/// Chunking cannot change a decoded result or its typed refusal. The finite
/// body supplies every event synchronously and closes at its original EOF.
pub fn check(input: &[u8]) {
    for media in ["application/json", "text/event-stream"] {
        for maximum in [0, 1, u32::MAX] {
            let whole = parse(input, input.len().max(1), media, maximum);
            for width in [1, 17, 127] {
                assert_eq!(whole, parse(input, width, media, maximum));
            }
        }
    }
    if let Ok(value) = crate::contract::json_from_slice::<Value>(input) {
        let encoded = crate::contract::canonical_json_bytes(&value).expect("JSON value encodes");
        let replay =
            crate::contract::json_from_slice::<Value>(&encoded).expect("JSON output decodes");
        assert_eq!(
            rpc::initialization(&value, None).map_err(|error| error.code()),
            rpc::initialization(&replay, None).map_err(|error| error.code())
        );
        assert_eq!(
            rpc::notification(&value, None).map_err(|error| error.code()),
            rpc::notification(&replay, None).map_err(|error| error.code())
        );
    }
}

/// Capture the actual client request writer. Server response seeds require
/// retained output from a running server.
pub fn seeds() -> Result<Vec<(&'static str, Vec<u8>)>, Box<dyn std::error::Error>> {
    let operation = OperationId::from_bytes([7; 16]);
    Ok(vec![(
        "mcp-request",
        rpc::request(operation, "ping", serde_json::json!({}))?,
    )])
}
