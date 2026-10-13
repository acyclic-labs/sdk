/// Project a hosted HTTP error code onto the public Stream error vocabulary.
#[wasm_bindgen(js_name = publicHttpErrorCode)]
pub fn public_http_error_code(raw: &str, route: &str) -> Option<String> {
    let code = match raw {
        "stream_not_found" => "stream_not_found",
        "destination_exists" => "destination_exists",
        "capacity_exhausted" => "capacity_exhausted",
        "commit_not_found" => "commit_not_found",
        _ => match StreamError::from_code(raw)? {
            StreamError::NotFound if route == "commits/read" => "commit_not_found",
            StreamError::NotFound => "stream_not_found",
            StreamError::AlreadyExists => "destination_exists",
            StreamError::Capacity => "capacity_exhausted",
            other => other.code(),
        },
    };
    if code == "commit_not_found" && route != "commits/read" {
        return None;
    }
    Some(code.to_owned())
}

/// Encode one protobuf request into the hosted Stream HTTP JSON shape.
#[wasm_bindgen(js_name = encodeHttpRequest)]
pub fn encode_http_request(route: &str, input: &[u8]) -> Result<String, JsValue> {
    check_command_size(input)?;
    http::encode(route, input).map_err(JsValue::from_str)
}

/// Validate one hosted HTTP JSON success response using the same path, width,
#[wasm_bindgen(js_name = validateHttpResponse)]
pub fn validate_http_response(route: &str, response_json: &str) -> Result<(), JsValue> {
    let value: Value = serde_json::from_str(response_json)
        .map_err(|error| JsValue::from_str(&format!("invalid JSON: {error}")))?;
    http::validate(route, &value).map_err(JsValue::from_str)
}

/// Validate and project one hosted HTTP JSON success response into the public
#[wasm_bindgen(js_name = decodeHttpResponse, unchecked_return_type = "unknown")]
pub fn decode_http_response(route: &str, response_json: &str) -> Result<JsValue, JsValue> {
    let value: Value = serde_json::from_str(response_json)
        .map_err(|error| JsValue::from_str(&format!("invalid JSON: {error}")))?;
    http::decode(route, &value).map_err(JsValue::from_str)
}

/// Decode one unary memory-provider response from canonical protobuf bytes
#[wasm_bindgen(js_name = projectMemoryResponse, unchecked_return_type = "unknown")]
pub fn project_memory_response(operation: &str, input: &[u8]) -> Result<JsValue, JsValue> {
    http::decode_wire_response(operation, input).map_err(JsValue::from_str)
}

/// Decode one `ReadResponse` frame's fields to its encoded `RecordBatch`.
#[wasm_bindgen(js_name = decodeReadResponse)]
pub fn decode_read_response(
    codec: i32,
    data: Vec<u8>,
    decoded_length: u64,
) -> Result<Vec<u8>, JsValue> {
    let response = wire::ReadResponse {
        codec,
        data: data.into(),
        decoded_length,
    };
    wire_codec::read_response_batch(response)
        .map(|batch| batch.to_vec())
        .map_err(js_error)
}
