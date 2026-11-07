//! Semantic service and callback admission after the shared JSON boundary.
use acyclic_harness_core::{
    OperationId, contract,
    mcp::{
        callbacks::{McpCallbackPolicy, McpCallbackRequest},
        services,
    },
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
                services::validate_params(method, value, None).map_err(|error| error.code()),
                services::validate_params(method, &restored, None).map_err(|error| error.code())
            );
            assert_eq!(
                services::validate_result(method, value.clone(), None)
                    .map_err(|error| error.code()),
                services::validate_result(method, restored.clone(), None)
                    .map_err(|error| error.code())
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
        admitted.as_ref().map_err(|error| error.code()),
        restored
            .validate()
            .map(|(operation, _)| operation)
            .as_ref()
            .map_err(|error| error.code())
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
