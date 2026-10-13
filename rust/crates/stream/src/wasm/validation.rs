fn error_code_str(error: &StreamError) -> &'static str {
    error.code()
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "passed to map_err, which hands over the error by value"
)]
fn js_error(error: StreamError) -> JsValue {
    let value = js_sys::Error::new(&error.to_string());
    let _property_result = Reflect::set(
        value.as_ref(),
        &JsValue::from_str("code"),
        &JsValue::from_str(error.code()),
    );
    value.into()
}

/// Return whether a code can be emitted by this WASM adapter.
#[wasm_bindgen]
pub fn is_stream_error_code(value: &str) -> bool {
    StreamError::from_code(value).is_some()
}

/// Type-only bridge for the complete Rust-owned Stream error-code contract.
#[wasm_bindgen(js_name = __streamErrorCodeContract)]
pub fn stream_error_code_contract(value: Ts<StreamErrorCode>) -> Ts<StreamErrorCode> {
    value
}

fn decode<T: Message + Default>(input: &[u8]) -> Result<T, JsValue> {
    T::decode(input).map_err(|_| js_error(StreamError::InvalidArgument))
}

fn check_command_size(input: &[u8]) -> Result<(), JsValue> {
    if input.len() > MAX_COMMAND_BYTES {
        Err(js_error(StreamError::LimitExceeded))
    } else {
        Ok(())
    }
}

async fn dispatch_commit(provider: &dyn StreamProvider, input: &[u8]) -> Result<Vec<u8>, JsValue> {
    let request = decode::<wire::CommitRequest>(input)?;
    let deadline_unix_millis = request.deadline_unix_millis;
    let request = wire_codec::commit_from_wire(request).map_err(js_error)?;
    let response = match deadline_unix_millis {
        Some(deadline) => provider
            .commit_before(request, deadline)
            .await
            .map_err(js_error)?,
        None => provider.commit(request).await.map_err(js_error)?,
    };
    Ok(wire_codec::commit_outcome_to_wire(response).encode_to_vec())
}

fn bytes_array(values: Vec<Vec<u8>>) -> JsValue {
    let array = Array::new();
    for value in values {
        array.push(&Uint8Array::from(value.as_slice()));
    }
    array.into()
}

/// Validate canonical protobuf bytes for one append request.
#[wasm_bindgen(js_name = validateAppendRequest)]
pub fn validate_append_request(input: &[u8]) -> String {
    let result = check_command_size(input)
        .map_err(|_| StreamError::LimitExceeded)
        .and_then(|_| wire::AppendRequest::decode(input).map_err(|_| StreamError::InvalidArgument))
        .and_then(wire_codec::append_from_wire)
        .and_then(|request| {
            crate::request::validate_records(&request.records)?;
            crate::request::validate_append_size(&request)
        });
    result.map_or_else(|error| error_code_str(&error).to_owned(), |_| String::new())
}

/// Validate one canonical Stream path using the same parser used by every
#[wasm_bindgen(js_name = validatePath)]
pub fn validate_path(path: &str) -> String {
    crate::StreamPath::new(path)
        .map_or_else(|error| error_code_str(&error).to_owned(), |_| String::new())
}

/// Validate one JavaScript representation of a canonical Stream sequence.
#[wasm_bindgen(js_name = validateSequence)]
pub fn validate_sequence(value: &str) -> String {
    if value.parse::<u64>().is_ok() {
        String::new()
    } else {
        error_code_str(&StreamError::InvalidArgument).to_owned()
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
    let mut request =
        wire_codec::commit_from_wire(request).map_err(|error| error_code_str(&error))?;
    crate::request::normalize_commit(&mut request).map_err(|error| error_code_str(&error))?;
    crate::request::validate_commit_shape(&request).map_err(|error| error_code_str(&error))?;
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
                    crate::request::validate_fork_size(&request)
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
    result.map_or_else(|error| error_code_str(&error).to_owned(), |_| String::new())
}

mod http {
    use super::*;

    type Result<T = ()> = std::result::Result<T, &'static str>;

    pub(super) fn encode(route: &str, input: &[u8]) -> Result<String> {
        crate::http_codec::encode(route, input)
    }
    pub(super) fn decode_wire_response(operation: &str, input: &[u8]) -> Result<JsValue> {
        let route = match operation {
            "inspect_idempotency" => "idempotency/inspect",
            "read_commit" => "commits/read",
            "children_page" => "children/page",
            "append" | "fork" | "commit" => operation,
            _ => return Err("invalid_argument"),
        };
        match crate::http_response::value(route, input)? {
            Some(value) => decode(route, &value),
            None => Ok(JsValue::UNDEFINED),
        }
    }

    fn object(value: &Value) -> Result<&serde_json::Map<String, Value>> {
        value.as_object().ok_or("expected object")
    }
    fn field<'a>(item: &'a serde_json::Map<String, Value>, name: &str) -> Result<&'a Value> {
        item.get(name).ok_or("missing field")
    }
    fn string(value: &Value) -> Result<&str> {
        value
            .as_str()
            .filter(|value| !value.is_empty())
            .ok_or("expected non-empty string")
    }
    fn encoded_bytes(value: &Value) -> Result<&str> {
        value.as_str().ok_or("expected base64 string")
    }
    fn u64_string(value: &Value) -> Result<u64> {
        let value = string(value)?;
        if !value.as_bytes().iter().all(|byte| byte.is_ascii_digit())
            || (value.len() > 1 && value.starts_with('0'))
        {
            return Err("expected decimal uint64 string");
        }
        value.parse().map_err(|_| "expected decimal uint64 string")
    }
    fn boolean(value: &Value) -> Result<bool> {
        value.as_bool().ok_or("expected boolean")
    }
    fn array(value: &Value) -> Result<&Vec<Value>> {
        value.as_array().ok_or("expected array")
    }
    fn path(value: &Value) -> Result<()> {
        let value = string(value)?;
        crate::StreamPath::new(value)
            .map(|_| ())
            .map_err(|_| "invalid path")
    }
    fn bytes(value: &Value) -> Result<Vec<u8>> {
        let value = encoded_bytes(value)?;
        decode_base64(value).ok_or("expected base64")
    }
    fn id(value: &Value) -> Result<()> {
        (bytes(value)?.len() == 32)
            .then_some(())
            .ok_or("expected 32-byte id")
    }
    fn key(value: &Value) -> Result<()> {
        let len = bytes(value)?.len();
        (len > 0 && len <= crate::MAX_IDEMPOTENCY_KEY_BYTES)
            .then_some(())
            .ok_or("invalid idempotency key")
    }
    fn set(object: &Object, name: &str, value: &JsValue) -> Result {
        Reflect::set(object.as_ref(), &JsValue::from_str(name), value)
            .map(|_| ())
            .map_err(|_| "could not construct response value")
    }

    fn string_js(value: &Value) -> Result<JsValue> {
        Ok(JsValue::from_str(string(value)?))
    }

    fn bigint_js(value: &Value) -> Result<JsValue> {
        Ok(JsValue::from(BigInt::from(u64_string(value)?)))
    }

    fn bytes_js(value: &Value) -> Result<JsValue> {
        Ok(Uint8Array::from(bytes(value)?.as_slice()).into())
    }

    fn array_js<F>(value: &Value, mut convert: F) -> Result<JsValue>
    where
        F: FnMut(&Value) -> Result<JsValue>,
    {
        let result = Array::new();
        for item in array(value)? {
            result.push(&convert(item)?);
        }
        Ok(result.into())
    }

    fn record_js(value: &Value) -> Result<JsValue> {
        let item = object(value)?;
        id(field(item, "commitId")?)?;
        let result = Object::new();
        set(&result, "sequence", &bigint_js(field(item, "sequence")?)?)?;
        set(&result, "value", &bytes_js(field(item, "value")?)?)?;
        set(&result, "commitId", &bytes_js(field(item, "commitId")?)?)?;
        set(
            &result,
            "committedAtMicros",
            &bigint_js(field(item, "committedAtMicros")?)?,
        )?;
        Ok(result.into())
    }

    fn children_page_js(value: &Value) -> Result<JsValue> {
        let item = object(value)?;
        id(field(item, "hierarchyVersion")?)?;
        let result = Object::new();
        set(
            &result,
            "hierarchyVersion",
            &bytes_js(field(item, "hierarchyVersion")?)?,
        )?;
        set(
            &result,
            "children",
            &array_js(field(item, "children")?, |value| {
                let child = object(value)?;
                path(field(child, "path")?)?;
                let result = Object::new();
                set(&result, "path", &string_js(field(child, "path")?)?)?;
                Ok(result.into())
            })?,
        )?;
        if let Some(next_after) = item.get("nextAfter").filter(|value| !value.is_null()) {
            set(&result, "nextAfter", &string_js(next_after)?)?;
        }
        Ok(result.into())
    }

    fn append_result_js(value: &Value) -> Result<JsValue> {
        let item = object(value)?;
        let result = Object::new();
        let ok = boolean(field(item, "ok")?)?;
        set(&result, "ok", &JsValue::from_bool(ok))?;
        if ok {
            let start = u64_string(field(item, "start")?)?;
            let end = u64_string(field(item, "end")?)?;
            let tail = u64_string(field(item, "tail")?)?;
            if start > end || end > tail {
                return Err("invalid append positions");
            }
            id(field(item, "commitId")?)?;
            set(&result, "start", &bigint_js(field(item, "start")?)?)?;
            set(&result, "end", &bigint_js(field(item, "end")?)?)?;
            set(&result, "tail", &bigint_js(field(item, "tail")?)?)?;
            set(&result, "commitId", &bytes_js(field(item, "commitId")?)?)?;
            return Ok(result.into());
        }
        if string(field(item, "code")?)? != "tail_conflict" {
            return Err("invalid append conflict code");
        }
        set(&result, "code", &string_js(field(item, "code")?)?)?;
        set(
            &result,
            "actualTail",
            &bigint_js(field(item, "actualTail")?)?,
        )?;
        Ok(result.into())
    }

    fn fork_receipt_js(value: &Value) -> Result<JsValue> {
        let item = object(value)?;
        path(field(item, "source")?)?;
        path(field(item, "destination")?)?;
        let forked_at = u64_string(field(item, "forkedAt")?)?;
        let tail = u64_string(field(item, "tail")?)?;
        if forked_at > tail {
            return Err("invalid fork positions");
        }
        id(field(item, "commitId")?)?;
        let result = Object::new();
        set(&result, "source", &string_js(field(item, "source")?)?)?;
        set(
            &result,
            "destination",
            &string_js(field(item, "destination")?)?,
        )?;
        set(&result, "forkedAt", &bigint_js(field(item, "forkedAt")?)?)?;
        set(&result, "tail", &bigint_js(field(item, "tail")?)?)?;
        set(&result, "commitId", &bytes_js(field(item, "commitId")?)?)?;
        Ok(result.into())
    }

    fn conflict_js(value: &Value) -> Result<JsValue> {
        let item = object(value)?;
        path(field(item, "path")?)?;
        let result = Object::new();
        set(&result, "path", &string_js(field(item, "path")?)?)?;
        if item.get("expectedAbsent") == Some(&Value::Bool(true)) {
            match string(field(item, "actual")?)? {
                "exists" => {}
                _ => return Err("invalid conflict state"),
            }
            set(&result, "expectedAbsent", &JsValue::TRUE)?;
            set(&result, "actual", &string_js(field(item, "actual")?)?)?;
        } else {
            u64_string(field(item, "expectedTail")?)?;
            if let Some(actual) = item.get("actualTail") {
                u64_string(actual)?;
            }
            set(
                &result,
                "expectedTail",
                &bigint_js(field(item, "expectedTail")?)?,
            )?;
            if let Some(actual) = item.get("actualTail") {
                set(&result, "actualTail", &bigint_js(actual)?)?;
            }
        }
        Ok(result.into())
    }

    fn tails_js(value: &Value) -> Result<JsValue> {
        let null_prototype: Object = JsValue::NULL.unchecked_into();
        let result = Object::create(&null_prototype);
        for (path_name, tail) in object(value)? {
            // A null prototype keeps `__proto__` as an own data property.
            crate::StreamPath::new(path_name).map_err(|_| "invalid path")?;
            set(&result, path_name, &bigint_js(tail)?)?;
        }
        Ok(result.into())
    }

    fn commit_result_js(value: &Value) -> Result<JsValue> {
        let item = object(value)?;
        let result = Object::new();
        let ok = boolean(field(item, "ok")?)?;
        set(&result, "ok", &JsValue::from_bool(ok))?;
        if ok {
            id(field(item, "commitId")?)?;
            set(&result, "commitId", &bytes_js(field(item, "commitId")?)?)?;
            set(&result, "tails", &tails_js(field(item, "tails")?)?)?;
            set(
                &result,
                "forks",
                &array_js(field(item, "forks")?, |value| {
                    let fork = object(value)?;
                    path(field(fork, "path")?)?;
                    u64_string(field(fork, "tail")?)?;
                    let result = Object::new();
                    set(&result, "path", &string_js(field(fork, "path")?)?)?;
                    set(&result, "tail", &bigint_js(field(fork, "tail")?)?)?;
                    Ok(result.into())
                })?,
            )?;
        } else {
            if string(field(item, "code")?)? != "conflict" {
                return Err("invalid commit conflict code");
            }
            set(&result, "code", &string_js(field(item, "code")?)?)?;
            set(
                &result,
                "conflicts",
                &array_js(field(item, "conflicts")?, conflict_js)?,
            )?;
        }
        Ok(result.into())
    }

    fn mutation_js(value: &Value) -> Result<JsValue> {
        let item = object(value)?;
        let result = Object::new();
        let kind = string(field(item, "type")?)?;
        set(&result, "type", &JsValue::from_str(kind))?;
        match kind {
            "append" => {
                path(field(item, "path")?)?;
                let start = u64_string(field(item, "start")?)?;
                let end = u64_string(field(item, "end")?)?;
                let tail = u64_string(field(item, "tail")?)?;
                if start > end || end > tail {
                    return Err("invalid append positions");
                }
                set(&result, "path", &string_js(field(item, "path")?)?)?;
                set(&result, "start", &bigint_js(field(item, "start")?)?)?;
                set(&result, "end", &bigint_js(field(item, "end")?)?)?;
                set(&result, "tail", &bigint_js(field(item, "tail")?)?)?;
                set(
                    &result,
                    "records",
                    &array_js(field(item, "records")?, record_js)?,
                )?;
            }
            "fork" => {
                path(field(item, "source")?)?;
                path(field(item, "destination")?)?;
                let forked_at = u64_string(field(item, "forkedAt")?)?;
                let tail = u64_string(field(item, "tail")?)?;
                if forked_at > tail {
                    return Err("invalid fork positions");
                }
                set(&result, "source", &string_js(field(item, "source")?)?)?;
                set(
                    &result,
                    "destination",
                    &string_js(field(item, "destination")?)?,
                )?;
                set(&result, "forkedAt", &bigint_js(field(item, "forkedAt")?)?)?;
                set(&result, "tail", &bigint_js(field(item, "tail")?)?)?;
                if item.get("records").is_some() {
                    set(
                        &result,
                        "records",
                        &array_js(field(item, "records")?, record_js)?,
                    )?;
                }
            }
            _ => return Err("invalid mutation type"),
        }
        Ok(result.into())
    }

    fn envelope_js(value: &Value) -> Result<JsValue> {
        let item = object(value)?;
        id(field(item, "commitId")?)?;
        let result = Object::new();
        set(&result, "commitId", &bytes_js(field(item, "commitId")?)?)?;
        set(
            &result,
            "mutations",
            &array_js(field(item, "mutations")?, mutation_js)?,
        )?;
        Ok(result.into())
    }

    fn idempotency_js(value: &Value) -> Result<JsValue> {
        let item = object(value)?;
        key(field(item, "idempotencyKey")?)?;
        if bytes(field(item, "requestDigest")?)?.len() != 32 {
            return Err("requestDigest must contain 32 bytes");
        }
        let result = Object::new();
        set(
            &result,
            "idempotencyKey",
            &bytes_js(field(item, "idempotencyKey")?)?,
        )?;
        set(
            &result,
            "requestDigest",
            &bytes_js(field(item, "requestDigest")?)?,
        )?;
        let outcome = object(field(item, "outcome")?)?;
        let outcome_result = Object::new();
        let kind = string(field(outcome, "type")?)?;
        set(&outcome_result, "type", &JsValue::from_str(kind))?;
        match kind {
            "append" => set(
                &outcome_result,
                "outcome",
                &append_result_js(field(outcome, "outcome")?)?,
            )?,
            "fork" => set(
                &outcome_result,
                "receipt",
                &fork_receipt_js(field(outcome, "receipt")?)?,
            )?,
            "commit" => set(
                &outcome_result,
                "outcome",
                &commit_result_js(field(outcome, "outcome")?)?,
            )?,
            _ => return Err("invalid idempotency outcome type"),
        }
        set(&result, "outcome", &outcome_result.into())?;
        Ok(result.into())
    }

    fn token_js(value: &Value) -> Result<JsValue> {
        let item = object(value)?;
        let result = Object::new();
        set(&result, "token", &string_js(field(item, "token")?)?)?;
        let timestamp = string(field(item, "expiresAt")?)?;
        let date = Date::new(&JsValue::from_str(timestamp));
        if date.get_time().is_nan() {
            return Err("expiresAt must be an ISO timestamp");
        }
        set(&result, "expiresAt", &date.into())?;
        Ok(result.into())
    }

    pub(super) fn decode(route: &str, value: &Value) -> Result<JsValue> {
        crate::http_validation::validate(route, value)?;
        match crate::HTTP_RESPONSE_CONTRACT
            .iter()
            .find_map(|(candidate, kind)| (*candidate == route).then_some(*kind))
        {
            Some("sequence") => bigint_js(value),
            Some("append") => append_result_js(value),
            Some("fork") => fork_receipt_js(value),
            Some("records") => array_js(value, record_js),
            Some("children") => array_js(value, |value| {
                let item = object(value)?;
                path(field(item, "path")?)?;
                let result = Object::new();
                set(&result, "path", &string_js(field(item, "path")?)?)?;
                Ok(result.into())
            }),
            Some("children_page") => children_page_js(value),
            Some("commit") => commit_result_js(value),
            Some("envelope") => envelope_js(value),
            Some("observation") => {
                if value.is_null() {
                    Ok(JsValue::UNDEFINED)
                } else {
                    idempotency_js(value)
                }
            }
            Some("token") => token_js(value),
            _ => Err("unknown HTTP route"),
        }
    }

    pub(super) fn validate(route: &str, value: &Value) -> Result {
        crate::http_validation::validate(route, value)
    }
    fn decode_base64(value: &str) -> Option<Vec<u8>> {
        if !value.len().is_multiple_of(4) {
            return None;
        }
        let mut output = Vec::with_capacity(value.len() / 4 * 3);
        let bytes = value.as_bytes();
        for (index, chunk) in bytes.as_chunks::<4>().0.iter().enumerate() {
            let &[a_byte, b_byte, c_byte, d_byte] = chunk;
            let a = sextet(a_byte)?;
            let b = sextet(b_byte)?;
            let c = if c_byte == b'=' { 0 } else { sextet(c_byte)? };
            let d = if d_byte == b'=' { 0 } else { sextet(d_byte)? };
            if c_byte == b'=' {
                if d_byte != b'=' || b & 0x0f != 0 {
                    return None;
                }
            } else if d_byte == b'=' && (c & 0x03 != 0 || index + 1 != bytes.len() / 4) {
                return None;
            }
            output.push(a << 2 | b >> 4);
            if c_byte != b'=' {
                output.push(b << 4 | c >> 2);
            }
            if d_byte != b'=' {
                output.push(c << 6 | d);
            }
        }
        Some(output)
    }
    fn sextet(value: u8) -> Option<u8> {
        match value {
            b'A'..=b'Z' => Some(value - b'A'),
            b'a'..=b'z' => Some(value - b'a' + 26),
            b'0'..=b'9' => Some(value - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
}
