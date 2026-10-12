use super::*;

/// Validate canonical protobuf bytes for one append request.
#[wasm_bindgen(js_name = validateAppendRequest)]
pub fn validate_append_request(input: &[u8]) -> String {
    let result = check_command_size(input)
        .map_err(|_| StreamError::LimitExceeded)
        .and_then(|_| wire::AppendRequest::decode(input).map_err(|_| StreamError::InvalidArgument))
        .and_then(wire_codec::append_from_wire)
        .and_then(|request| {
            memory::provider_result(memory::validate_records(&request.records, None))?;
            memory::validate_append_size(&request)
        });
    result.map_or_else(|error| error.code().to_owned(), |_| String::new())
}

/// Validate one canonical Stream path using the same parser used by every provider and wire decoder.
#[wasm_bindgen(js_name = validatePath)]
pub fn validate_path(path: &str) -> String {
    crate::StreamPath::new(path).map_or_else(|error| error.code().to_owned(), |_| String::new())
}

/// Validate one JavaScript representation of a canonical Stream sequence.
#[wasm_bindgen(js_name = validateSequence)]
pub fn validate_sequence(value: &str) -> String {
    if value.parse::<u64>().is_ok() {
        String::new()
    } else {
        StreamError::InvalidArgument.code().to_owned()
    }
}

/// Normalize and encode canonical protobuf bytes for one commit request.
#[wasm_bindgen(js_name = normalizeCommitRequest)]
pub fn normalize_commit_request(input: &[u8]) -> Result<Vec<u8>, JsValue> {
    normalize_commit_bytes(input).map_err(JsValue::from_str)
}

fn normalize_commit_bytes(input: &[u8]) -> Result<Vec<u8>, &'static str> {
    if input.len() > MAX_COMMAND_BYTES {
        return Err("limit_exceeded");
    }
    let request = wire::CommitRequest::decode(input).map_err(|_| "invalid_argument")?;
    let deadline_unix_millis = request.deadline_unix_millis;
    let mut request = wire_codec::commit_from_wire(request).map_err(|error| error.code())?;
    memory::provider_result(memory::normalize_commit(&mut request, None))
        .map_err(|error| error.code())?;
    memory::provider_result(memory::validate_commit_shape(&request, None))
        .map_err(|error| error.code())?;
    let mut normalized = wire_codec::commit_to_wire(&request);
    normalized.deadline_unix_millis = deadline_unix_millis;
    Ok(normalized.encode_to_vec())
}

/// Validate one canonical protobuf request at the browser boundary.
#[wasm_bindgen(js_name = validateRequest)]
pub fn validate_request(kind: &str, input: &[u8]) -> String {
    let result = check_command_size(input)
        .map_err(|_| StreamError::LimitExceeded)
        .and_then(|_| match kind {
            "tail" => wire::TailRequest::decode(input)
                .map_err(|_| StreamError::InvalidArgument)
                .and_then(|request| wire_codec::path(request.path).map(|_| ())),
            "fork" => wire::ForkRequest::decode(input)
                .map_err(|_| StreamError::InvalidArgument)
                .and_then(wire_codec::fork_from_wire)
                .and_then(|request| {
                    if request.source == request.destination {
                        return Err(StreamError::InvalidArgument);
                    }
                    memory::validate_fork_size(&request)
                }),
            "read" => wire::ReadRequest::decode(input)
                .map_err(|_| StreamError::InvalidArgument)
                .and_then(wire_codec::read_from_wire)
                .and_then(|request| memory::validate_limit(request.limit)),
            "follow" => wire::FollowRequest::decode(input)
                .map_err(|_| StreamError::InvalidArgument)
                .and_then(wire_codec::follow_from_wire)
                .map(|_| ()),
            "children" => wire::ChildrenRequest::decode(input)
                .map_err(|_| StreamError::InvalidArgument)
                .and_then(wire_codec::children_from_wire)
                .and_then(|request| memory::validate_limit(request.limit)),
            "children_page" => wire::ChildrenPageRequest::decode(input)
                .map_err(|_| StreamError::InvalidArgument)
                .and_then(wire_codec::children_page_from_wire)
                .and_then(|request| {
                    if request.after.is_some() && request.hierarchy_version.is_none() {
                        return Err(StreamError::InvalidArgument);
                    }
                    Ok(request)
                })
                .and_then(|request| memory::validate_limit(request.limit)),
            _ => Err(StreamError::InvalidArgument),
        });
    result.map_or_else(|error| error.code().to_owned(), |_| String::new())
}

/// Decodes one bounded ReadResponse frame.
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
        delivery_token: None,
    };
    wire_codec::read_response_batch(response)
        .map(|batch| batch.to_vec())
        .map_err(js_error)
}

#[cfg(test)]
#[path = "validation_tests.rs"]
mod tests;
