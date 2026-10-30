//! The actual finite stdio exchange, with no process or readiness substitute.
use super::{McpStdioRequest, codec, services};
use crate::{OperationId, wire::ErrorCode};
use serde_json::Value;

fn request() -> McpStdioRequest {
    McpStdioRequest {
        initialization: OperationId::from_bytes([6; 16]),
        operation: OperationId::from_bytes([7; 16]),
        method: "tools/list".into(),
        params: serde_json::json!({}),
        maximum_bytes: u32::MAX,
    }
}

fn parse(input: &[u8], width: usize) -> Result<(Vec<u8>, Value), ErrorCode> {
    let (mut exchange, mut writes, _) = request().exchange(None).map_err(|error| error.code())?;
    for chunk in input.chunks(width) {
        match exchange.push(chunk, None) {
            Ok(next) => writes.extend_from_slice(&next.0),
            Err(error) => {
                let code = error.code();
                assert!(
                    exchange.push(&[], None).is_err(),
                    "a failed exchange reopened"
                );
                assert!(
                    exchange.finish().is_err(),
                    "a failed exchange published a result"
                );
                return Err(code);
            }
        }
    }
    let result = exchange.finish().map_err(|error| error.code())?;
    let value =
        services::validate_result("tools/list", result.0, None).map_err(|error| error.code())?;
    Ok((writes, value))
}

/// Accepted transcripts preserve both their emitted handshake and result over
/// every chunk partition. Incomplete input ends at the typed EOF refusal.
pub fn check(input: &[u8]) {
    let whole = parse(input, input.len().max(1));
    for width in [1, 17, 127] {
        assert_eq!(whole, parse(input, width));
    }
    if let Ok(decoded) = codec::decode_value(input, None) {
        let encoded = codec::encode_line(&decoded.0, usize::MAX, None).expect("JSON line encodes");
        let body = encoded.0.strip_suffix(b"\n").expect("line terminator");
        let replay = codec::decode_value(body, None).expect("JSON line decodes");
        assert_eq!(decoded.0, replay.0);
    }
}

/// Capture the client's actual first write. Successful response transcripts
/// must be captured from a running server, rather than assembled here.
pub fn seed() -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    Ok(request().exchange(None)?.1)
}
