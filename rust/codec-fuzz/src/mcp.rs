//! Semantic service and callback admission after the shared JSON boundary.
use acyclic_harness_core::{
    OperationId, contract,
    mcp::{
        callbacks::{McpCallbackPolicy, McpCallbackRequest},
        services,
    },
    test_support::error_observation,
    wire::ErrorCode,
};
use serde_json::Value;

pub fn check(bytes: &[u8]) {
    frames(bytes);
    let Ok(value) = contract::json_from_slice::<Value>(bytes) else {
        return;
    };
    for value in std::iter::once(&value)
        .chain(value.get("params"))
        .chain(value.get("result"))
    {
        let encoded = contract::canonical_json_bytes(value).expect("JSON value encodes");
        let restored = contract::json_from_slice::<Value>(&encoded).expect("JSON value decodes");
        for method in [
            "resources/list",
            "resources/templates/list",
            "resources/read",
            "prompts/list",
            "prompts/get",
            "tools/list",
            "tools/call",
        ] {
            assert_eq!(
                services::validate_params(method, value, None)
                    .map_err(|error| error_observation(&error)),
                services::validate_params(method, &restored, None)
                    .map_err(|error| error_observation(&error))
            );
            assert_eq!(
                services::validate_result(method, value.clone(), None)
                    .map_err(|error| error_observation(&error)),
                services::validate_result(method, restored.clone(), None)
                    .map_err(|error| error_observation(&error))
            );
        }
    }
    let request = McpCallbackRequest {
        parent: OperationId::from_bytes([7; 16]),
        policy: McpCallbackPolicy {
            server: "capture".into(),
            revision: "1".into(),
            sampling: true,
            maximum_requests: u32::MAX,
        },
        message: value,
    };
    callback(request);
    if let Ok(request) = contract::json_from_slice::<McpCallbackRequest>(bytes) {
        callback(request);
    }
}

fn callback(mut request: McpCallbackRequest) {
    let admitted = request.validate().map(|(operation, _)| operation);
    let encoded = contract::canonical_json_bytes(&request).expect("callback request encodes");
    let restored = contract::json_from_slice::<McpCallbackRequest>(&encoded)
        .expect("callback request decodes");
    assert_eq!(request, restored);
    assert_eq!(
        admitted.as_ref().map_err(error_observation),
        restored
            .validate()
            .map(|(operation, _)| operation)
            .as_ref()
            .map_err(error_observation)
    );
    if admitted.is_ok() {
        request.policy.sampling = false;
        assert_eq!(request.validate().unwrap_err().code(), ErrorCode::Invalid);
        request.policy.sampling = true;
        request.message["jsonrpc"] = "unsupported".into();
        assert_eq!(request.validate().unwrap_err().code(), ErrorCode::Invalid);
    }
}

/// Compare the actual buffered codec with blocking admission under pipe fragmentation.
fn frames(bytes: &[u8]) {
    use acyclic_mcp::{BoundedCodec, JsonLineReader};
    use bytes::BytesMut;
    use std::{
        io::{self, Read},
        sync::{Arc, atomic::AtomicU32},
    };
    use tokio_util::codec::Decoder as _;

    struct Fragmented<'a> {
        bytes: &'a [u8],
        width: usize,
    }
    impl Read for Fragmented<'_> {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            let length = buffer.len().min(self.width);
            self.bytes.read(&mut buffer[..length])
        }
    }
    for maximum in [64, 65_536] {
        let mut codec = BoundedCodec::<Value>::new(Arc::new(AtomicU32::new(maximum)));
        let mut buffer = BytesMut::from(bytes);
        let mut expected = Vec::new();
        loop {
            match codec.decode_eof(&mut buffer) {
                Ok(Some(value)) => expected.push(Ok(value)),
                Ok(None) => break,
                Err(error) => {
                    expected.push(Err((error.kind(), error.to_string())));
                    break;
                }
            }
        }
        for width in [1, 13, 8192] {
            let actual = JsonLineReader::new(Fragmented { bytes, width }, maximum)
                .map(|value| value.map_err(|error| (error.kind(), error.to_string())))
                .collect::<Vec<_>>();
            assert_eq!(
                actual, expected,
                "native MCP pipe fragmentation changed admission"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_harness_core::mcp::{http, stdio};

    #[test]
    fn captured_mcp_refusals_keep_diagnostics_and_uncertainty() {
        // Original stdio and HTTP captures; provenance and byte digests are
        // retained in corpus/EXTERNAL.tsv. Browser replay uses the same SSE bytes.
        for (codec, method, bytes) in [
            (
                "stdio",
                "tools/list",
                include_bytes!("../corpus/harness_codecs/mcp-filesystem-2.1.0-stdio").as_slice(),
            ),
            (
                "json",
                "tools/list",
                include_bytes!("../corpus/harness_codecs/mcp-filesystem-2.1.0-result").as_slice(),
            ),
            (
                "sse",
                "initialize",
                include_bytes!("../corpus/harness_codecs/mcp-everything-2.1.0-http-initialize")
                    .as_slice(),
            ),
            (
                "sse",
                "tools/list",
                include_bytes!("../corpus/harness_codecs/mcp-everything-2.1.0-http-tools-list")
                    .as_slice(),
            ),
        ] {
            crate::admit_external(codec, method, bytes)
                .expect("original captured response must be admitted");
            let operation = OperationId::from_bytes([7; 16]);
            let missing = &bytes[..0];
            let interrupted = if codec == "stdio" {
                stdio::codec_fuzz::admit(missing)
            } else {
                let media = if codec == "sse" {
                    "text/event-stream"
                } else {
                    "application/json"
                };
                http::codec_fuzz::admit(missing, media, operation, false)
            }
            .expect_err("missing response cannot publish a result");
            assert!(
                error_observation(&interrupted).1,
                "missing MCP reply lost mutation uncertainty"
            );
            assert_eq!(
                acyclic_harness_core::encode_error(&interrupted).operation_id,
                operation.to_string(),
                "missing MCP reply lost its original operation"
            );
            let exercise = |input: &[u8]| {
                http::codec_fuzz::check(input);
                stdio::codec_fuzz::check(input);
                check(input);
            };
            exercise(bytes);
            // Generated truncations and malformed bytes derive from the real
            // capture; they are not claimed to be captured server responses.
            for cut in [0, 1, bytes.len() / 2, bytes.len() - 1] {
                exercise(&bytes[..cut]);
            }
            let mut malformed = bytes.to_vec();
            malformed[0] = 0xff;
            exercise(&malformed);
        }
    }
}
