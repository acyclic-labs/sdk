use super::*;


/// Parses canonical JSON without passing full-width integer literals through
/// JavaScript Number. Large serde integers are returned as `BigInt`.
#[wasm_bindgen(js_name = decodeCanonicalJson)]
pub fn decode_canonical_json(bytes: &[u8]) -> Result<JsValue, JsValue> {
    let value = parse_json_bytes(bytes)?;
    let canonical = crate::contract::canonical_json_bytes(&value).map_err(js_error)?;
    if canonical != bytes {
        return Err(JsValue::from_str("JSON is not canonical"));
    }
    to_js(&value)
}

/// Parses external JSON with exact integers but without demanding canonical
/// key order or whitespace. Callers must still apply their schema and numeric
/// range policy before presenting model-authored values to an executor.
#[wasm_bindgen(js_name = decodeJson)]
pub fn decode_json(bytes: &[u8]) -> Result<JsValue, JsValue> {
    to_js(&parse_json_bytes(bytes)?)
}

/// Admits one canonical ref-only execution journal observation through Rust.
#[wasm_bindgen(js_name = decodeExecutionEventJson, unchecked_return_type = "ExecutionEvent")]
pub fn decode_execution_event_json(bytes: &[u8]) -> Result<JsValue, JsValue> {
    let event: crate::executor::ExecutionEvent =
        crate::executor::decode_json(bytes, None).map_err(js_error)?.0;
    to_js_admitted(&event)
}

pub(super) fn parse_json_bytes(bytes: &[u8]) -> Result<serde_json::Value, JsValue> {
    reject_out_of_range_integer_tokens(bytes)?;
    crate::contract::json_from_slice(bytes).map_err(|error| JsValue::from_str(&error.to_string()))
}

// serde_json without arbitrary_precision promotes an integer literal outside
// i64/u64 to f64. Reject it before parsing so the JS boundary never presents
// rounded integer data as a successful read.
pub(super) fn reject_out_of_range_integer_tokens(bytes: &[u8]) -> Result<(), JsValue> {
    let mut offset = 0;
    let mut quoted = false;
    let mut escaped = false;
    while let Some(&byte) = bytes.get(offset) {
        if quoted {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                quoted = false;
            }
            offset += 1;
            continue;
        }
        if byte == b'"' {
            quoted = true;
            offset += 1;
            continue;
        }
        if byte != b'-' && !byte.is_ascii_digit() {
            offset += 1;
            continue;
        }
        let start = offset;
        while bytes
            .get(offset)
            .is_some_and(|byte| matches!(byte, b'0'..=b'9' | b'+' | b'-' | b'.' | b'e' | b'E'))
        {
            offset += 1;
        }
        let token = bytes
            .get(start..offset)
            .ok_or_else(|| JsValue::from_str("JSON token range is invalid"))?;
        if !token.iter().any(|part| matches!(*part, b'.' | b'e' | b'E')) {
            let text = std::str::from_utf8(token)
                .map_err(|_| JsValue::from_str("JSON integer token is not UTF-8"))?;
            if text.parse::<i64>().is_err() && text.parse::<u64>().is_err() {
                return Err(JsValue::from_str(
                    "JSON integer exceeds the exact 64-bit range",
                ));
            }
        }
    }
    Ok(())
}

/// Serializes a plain JavaScript data value through Rust's canonical JSON
/// representation. Unsafe integer Numbers are rejected before conversion;
/// callers must supply `BigInt` for exact full-width identities and counters.
#[wasm_bindgen(js_name = encodeCanonicalJson)]
pub fn encode_canonical_json(value: JsValue) -> Result<Vec<u8>, JsValue> {
    let value = js_json_value(&value)?;
    let bytes = crate::contract::canonical_json_bytes(&value).map_err(js_error)?;
    Ok(bytes)
}

pub(super) fn js_json_value(value: &JsValue) -> Result<serde_json::Value, JsValue> {
    let mut ancestors = Vec::new();
    snapshot_js_json(value, &mut ancestors)
}

/// Hashes the same bounded canonical JSON bytes used by native admissions.
#[wasm_bindgen(js_name = digestCanonicalJson)]
pub fn digest_canonical_json(value: JsValue) -> Result<Vec<u8>, JsValue> {
    Ok(blake3::hash(&encode_canonical_json(value)?)
        .as_bytes()
        .to_vec())
}

pub(super) fn checked_js_string(value: JsValue, field: &str) -> Result<String, JsValue> {
    let js_value: js_sys::JsString = value
        .dyn_into()
        .map_err(|_| JsValue::from_str(&format!("{field} must be a string")))?;
    if !js_value.is_valid_utf16() {
        return Err(JsValue::from_str(&format!("{field} is invalid")));
    }
    js_value
        .as_string()
        .ok_or_else(|| JsValue::from_str(&format!("{field} must be a string")))
}

#[expect(
    clippy::too_many_lines,
    reason = "one exhaustive traversal owns JS JSON admission"
)]
pub(super) fn snapshot_js_json(
    value: &JsValue,
    ancestors: &mut Vec<JsValue>,
) -> Result<serde_json::Value, JsValue> {
    if value.is_undefined() {
        return Err(JsValue::from_str("undefined is not canonical JSON"));
    }
    if let Some(number) = value.as_f64() {
        if !crate::conversation::is_exact_js_number(number) {
            return Err(JsValue::from_str(
                "JSON contains an unsafe JavaScript Number",
            ));
        }
        let admitted = if number.fract() == 0.0 {
            let exact = number
                .to_string()
                .parse::<i64>()
                .map_err(|_| JsValue::from_str("JSON integer is outside the exact range"))?;
            serde_json::Value::from(exact)
        } else {
            serde_json::Value::Number(
                serde_json::Number::from_f64(number)
                    .ok_or_else(|| JsValue::from_str("JSON number is invalid"))?,
            )
        };
        return Ok(admitted);
    }
    if value.is_null() {
        return Ok(serde_json::Value::Null);
    }
    if value.is_bigint() {
        let spelling = JsValue::from(
            value
                .unchecked_ref::<js_sys::BigInt>()
                .to_string(10)
                .map_err(JsValue::from)?,
        )
        .as_string()
        .ok_or_else(|| JsValue::from_str("BigInt spelling is invalid"))?;
        if let Ok(signed) = spelling.parse::<i64>() {
            return Ok(serde_json::Value::from(signed));
        }
        if let Ok(unsigned) = spelling.parse::<u64>() {
            return Ok(serde_json::Value::from(unsigned));
        }
        return Err(JsValue::from_str("BigInt exceeds the exact 64-bit range"));
    }
    if let Some(boolean) = value.as_bool() {
        return Ok(serde_json::Value::Bool(boolean));
    }
    if value.is_string() {
        return Ok(serde_json::Value::String(checked_js_string(
            value.clone(),
            "JSON string",
        )?));
    }
    if !value.is_object() {
        return Err(JsValue::from_str("value is not canonical JSON"));
    }
    if value.is_instance_of::<js_sys::Uint8Array>() {
        return Ok(serde_json::Value::Array(
            js_sys::Uint8Array::new(value)
                .to_vec()
                .into_iter()
                .map(serde_json::Value::from)
                .collect(),
        ));
    }
    if ancestors
        .iter()
        .any(|ancestor| js_sys::Object::is(ancestor, value))
    {
        return Err(JsValue::from_str("cyclic values are not canonical JSON"));
    }
    let is_map = value.is_instance_of::<js_sys::Map>();
    if !js_sys::Array::is_array(value) && !is_map {
        let prototype: JsValue = js_sys::Object::get_prototype_of(value).into();
        let ordinary: JsValue =
            js_sys::Object::get_prototype_of(&js_sys::Object::new().into()).into();
        if !prototype.is_null() && !js_sys::Object::is(&prototype, &ordinary) {
            return Err(JsValue::from_str("only plain objects are canonical JSON"));
        }
    }
    ancestors.push(value.clone());
    let snapshot = if js_sys::Array::is_array(value) {
        let items = js_sys::Array::from(value);
        let mut snapshot = Vec::with_capacity(items.length() as usize);
        for item in items.iter() {
            snapshot.push(snapshot_js_json(&item, ancestors)?);
        }
        serde_json::Value::Array(snapshot)
    } else if is_map {
        let mut failure = None;
        let mut snapshot = serde_json::Map::new();
        value
            .unchecked_ref::<js_sys::Map>()
            .for_each(&mut |child, key| {
                if failure.is_some() {
                    return;
                }
                let key = match checked_js_string(key, "JSON object key") {
                    Ok(key) => key,
                    Err(error) => {
                        failure = Some(error);
                        return;
                    }
                };
                match snapshot_js_json(&child, ancestors) {
                    Ok(admitted) => {
                        snapshot.insert(key, admitted);
                    }
                    Err(error) => failure = Some(error),
                }
            });
        if let Some(error) = failure {
            return Err(error);
        }
        serde_json::Value::Object(snapshot)
    } else {
        let object: &js_sys::Object = value.unchecked_ref();
        let mut snapshot = serde_json::Map::new();
        for key in js_sys::Object::keys(object).iter() {
            let key_text = checked_js_string(key.clone(), "JSON object key")?;
            let child = js_sys::Reflect::get(value, &key)?;
            let admitted = snapshot_js_json(&child, ancestors)?;
            if snapshot.insert(key_text, admitted).is_some() {
                return Err(JsValue::from_str("duplicate JSON object key"));
            }
        }
        serde_json::Value::Object(snapshot)
    };
    ancestors.pop();
    Ok(snapshot)
}

pub(super) fn from_js<T: serde::de::DeserializeOwned>(value: JsValue) -> Result<T, JsValue> {
    // A single traversal detaches the admitted value from getters and Proxies.
    // Never validate the original object and then deserialize it again: a
    // mutable accessor could return a finite number first and Infinity later.
    let value = js_json_value(&value)?;
    serde_json::from_value(value).map_err(|error| JsValue::from_str(&error.to_string()))
}

pub(super) fn to_js<T: serde::Serialize>(value: &T) -> Result<JsValue, JsValue> {
    let serializer = serde_wasm_bindgen::Serializer::new()
        .serialize_large_number_types_as_bigints(true)
        .serialize_missing_as_null(true);
    value
        .serialize(&serializer)
        .map_err(|error| JsValue::from_str(&error.to_string()))
}

// Durable JSON keeps integral literals. Only admitted, ref-only contract
// outputs pass through this projection: a validated FileDescriptor length is
// represented as Number and every other 64-bit integer remains BigInt.
// Generic events, snapshots, model values and extension payloads use to_js.
pub(super) fn to_js_admitted<T: serde::Serialize>(value: &T) -> Result<JsValue, JsValue> {
    let js = to_js(value)?;
    let json =
        serde_json::to_value(value).map_err(|error| JsValue::from_str(&error.to_string()))?;
    normalize_descriptor_lengths(&js, &json)?;
    Ok(js)
}

pub(super) fn exact_js_number(value: u64) -> Result<JsValue, JsValue> {
    if value > crate::conversation::MAX_EXACT_JS_INTEGER {
        return Err(JsValue::from_str(
            "integer exceeds JavaScript Number precision",
        ));
    }
    let number = value
        .to_string()
        .parse::<f64>()
        .map_err(|_| JsValue::from_str("integer cannot be projected to JavaScript"))?;
    Ok(JsValue::from_f64(number))
}

pub(super) fn set_js_field(js: &JsValue, key: &str, value: &JsValue) -> Result<(), JsValue> {
    let key = JsValue::from_str(key);
    if js.is_instance_of::<js_sys::Map>() {
        js.unchecked_ref::<js_sys::Map>().set(&key, value);
    } else if !js_sys::Reflect::set(js, &key, value)? {
        return Err(JsValue::from_str("typed JS projection is not writable"));
    }
    Ok(())
}

pub(super) fn normalize_descriptor_lengths(js: &JsValue, value: &serde_json::Value) -> Result<(), JsValue> {
    match value {
        serde_json::Value::Object(fields) => {
            if fields.len() == 3
                && fields.contains_key("sha256")
                && fields.contains_key("byte_length")
                && fields.contains_key("media_type")
            {
                let descriptor: FileDescriptor =
                    serde_json::from_value(serde_json::Value::Object(fields.clone()))
                        .map_err(|error| JsValue::from_str(&error.to_string()))?;
                set_js_field(
                    js,
                    "byte_length",
                    &exact_js_number(descriptor.byte_length())?,
                )?;
            }
            for (key, child) in fields {
                let key = JsValue::from_str(key);
                let js_child = if js.is_instance_of::<js_sys::Map>() {
                    js.unchecked_ref::<js_sys::Map>().get(&key)
                } else {
                    js_sys::Reflect::get(js, &key)?
                };
                normalize_descriptor_lengths(&js_child, child)?;
            }
        }
        serde_json::Value::Array(items) => {
            for (index, child) in items.iter().enumerate() {
                let index = u32::try_from(index)
                    .map_err(|_| JsValue::from_str("JS array index exceeds u32"))?;
                let js_child = js_sys::Reflect::get(js, &JsValue::from_f64(f64::from(index)))?;
                normalize_descriptor_lengths(&js_child, child)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Throws a JS `Error` whose `code` is the wire [`crate::wire::ErrorCode`].
pub(super) fn js_error(error: crate::Error) -> JsValue {
    let js = js_sys::Error::new(&error.to_string());
    match js_sys::Reflect::set(&js, &"code".into(), &(error.code() as i32).into()) {
        Ok(_) => js.into(),
        Err(_) => JsValue::from_str(&error.to_string()),
    }
}
