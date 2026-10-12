use super::*;

/// Encode one protobuf request into the hosted Stream HTTP JSON shape.
#[wasm_bindgen(js_name = encodeHttpRequest)]
pub fn encode_http_request(route: &str, input: &[u8]) -> Result<String, JsValue> {
    check_command_size(input)?;
    crate::http_codec::encode(route, input).map_err(JsValue::from_str)
}

/// Validate one hosted HTTP JSON success response using the same path, width, identity, and tagged-union rules as the canonical Stream domain.
#[wasm_bindgen(js_name = validateHttpResponse)]
pub fn validate_http_response(route: &str, response_json: &str) -> Result<(), JsValue> {
    let value: Value = serde_json::from_str(response_json)
        .map_err(|error| JsValue::from_str(&format!("invalid JSON: {error}")))?;
    crate::http_validation::validate(route, &value).map_err(JsValue::from_str)
}

/// Validate and project one hosted HTTP JSON success response into the public JavaScript shape.
#[wasm_bindgen(js_name = decodeHttpResponse, unchecked_return_type = "unknown")]
pub fn decode_http_response(route: &str, response_json: &str) -> Result<JsValue, JsValue> {
    let value: Value = serde_json::from_str(response_json)
        .map_err(|error| JsValue::from_str(&format!("invalid JSON: {error}")))?;
    http::decode(route, &value).map_err(JsValue::from_str)
}

/// Decode one unary memory-provider response from canonical protobuf bytes into the public JavaScript result shape.
#[wasm_bindgen(js_name = projectMemoryResponse, unchecked_return_type = "unknown")]
pub fn project_memory_response(operation: &str, input: &[u8]) -> Result<JsValue, JsValue> {
    http::decode_wire_response(operation, input).map_err(JsValue::from_str)
}

mod http {
    use super::*;

    type Result<T = ()> = std::result::Result<T, &'static str>;

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

    use crate::http_validation::{array, boolean, bytes, field, object, string, u64_string};

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

    fn array_js(value: &Value, convert: fn(&Value) -> Result<JsValue>) -> Result<JsValue> {
        let result = Array::new();
        for item in array(value)? {
            result.push(&convert(item)?);
        }
        Ok(result.into())
    }

    fn record_js(value: &Value) -> Result<JsValue> {
        let item = object(value)?;
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

    fn child_js(value: &Value) -> Result<JsValue> {
        let item = object(value)?;
        let result = Object::new();
        set(&result, "path", &string_js(field(item, "path")?)?)?;
        Ok(result.into())
    }

    fn children_page_js(value: &Value) -> Result<JsValue> {
        let item = object(value)?;
        let result = Object::new();
        set(
            &result,
            "hierarchyVersion",
            &bytes_js(field(item, "hierarchyVersion")?)?,
        )?;
        set(
            &result,
            "children",
            &array_js(field(item, "children")?, child_js)?,
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
            set(&result, "start", &bigint_js(field(item, "start")?)?)?;
            set(&result, "end", &bigint_js(field(item, "end")?)?)?;
            set(&result, "tail", &bigint_js(field(item, "tail")?)?)?;
            set(&result, "commitId", &bytes_js(field(item, "commitId")?)?)?;
            return Ok(result.into());
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
        let result = Object::new();
        set(&result, "path", &string_js(field(item, "path")?)?)?;
        if item.get("expectedAbsent") == Some(&Value::Bool(true)) {
            set(&result, "expectedAbsent", &JsValue::TRUE)?;
            set(&result, "actual", &string_js(field(item, "actual")?)?)?;
        } else {
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
            set(&result, "commitId", &bytes_js(field(item, "commitId")?)?)?;
            set(&result, "tails", &tails_js(field(item, "tails")?)?)?;
            set(
                &result,
                "forks",
                &array_js(field(item, "forks")?, |value| {
                    let fork = object(value)?;
                    let result = Object::new();
                    set(&result, "path", &string_js(field(fork, "path")?)?)?;
                    set(&result, "tail", &bigint_js(field(fork, "tail")?)?)?;
                    Ok(result.into())
                })?,
            )?;
        } else {
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
            Some("empty") => Ok(JsValue::UNDEFINED),
            Some("sequence") => bigint_js(value),
            Some("append") => append_result_js(value),
            Some("fork") => fork_receipt_js(value),
            Some("records") => array_js(value, record_js),
            Some("children") => array_js(value, child_js),
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
}
