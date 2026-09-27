//! Validation for the hosted Objects JSON envelope.
//!
//! The hosted adapter uses JSON wrappers for values that JSON cannot represent directly.
//! Keeping the shape checks here makes the HTTP transport consume the same small, explicit
//! contract as the Rust implementation without maintaining a second TypeScript schema.

use base64::Engine as _;
use js_sys::{Array, BigInt, Date, Map, Object, Reflect, Set, Uint8Array};
use serde_json::Value;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use wasm_bindgen::{JsCast, prelude::*};

#[path = "response_contract.rs"]
mod response_contract;

pub use response_contract::HTTP_RESPONSE_CONTRACT;

type Result<T> = std::result::Result<T, String>;

fn object(value: &Value) -> Result<&serde_json::Map<String, Value>> {
    value.as_object().ok_or_else(|| "expected object".into())
}

fn field<'a>(value: &'a Value, name: &str) -> Result<&'a Value> {
    object(value)?
        .get(name)
        .ok_or_else(|| format!("missing {name}"))
}

fn text(value: &Value, name: &str) -> Result<()> {
    if value.as_str().is_some_and(|value| !value.is_empty()) {
        Ok(())
    } else {
        Err(format!("{name} must be a non-empty string"))
    }
}

fn boolean(value: &Value, name: &str) -> Result<()> {
    value
        .as_bool()
        .map(|_| ())
        .ok_or_else(|| format!("{name} must be boolean"))
}

fn integer(value: &Value, name: &str) -> Result<i64> {
    let number = value
        .as_i64()
        .ok_or_else(|| format!("{name} must be an integer"))?;
    if number.unsigned_abs() > 9_007_199_254_740_991 {
        return Err(format!("{name} must be a safe integer"));
    }
    Ok(number)
}

fn bigint(value: &Value, name: &str, unsigned: bool) -> Result<()> {
    let wrapper = value
        .as_object()
        .ok_or_else(|| format!("{name} must be bigint"))?;
    let raw = wrapper
        .get("$bigint")
        .ok_or_else(|| format!("{name} must be bigint"))?;
    let raw = raw
        .as_str()
        .ok_or_else(|| format!("{name} must be bigint"))?;
    if unsigned {
        raw.parse::<u64>()
            .map(|_| ())
            .map_err(|_| format!("{name} must be uint64"))
    } else {
        raw.parse::<i64>()
            .map(|_| ())
            .map_err(|_| format!("{name} must be int64"))
    }
}

fn bytes(value: &Value, name: &str) -> Result<()> {
    let raw = object(value)?
        .get("$bytes")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{name} must be bytes"))?;
    if raw.len() % 4 == 1 {
        return Err(format!("{name} must be bytes"));
    }
    let padding = raw.bytes().rev().take_while(|byte| *byte == b'=').count();
    if padding > 2 || (padding > 0 && raw.len() % 4 != 0) {
        return Err(format!("{name} must be bytes"));
    }
    let content_len = raw.len() - padding;
    if raw
        .as_bytes()
        .get(..content_len)
        .ok_or_else(|| format!("{name} must be bytes"))?
        .iter()
        .copied()
        .any(|byte| !(byte.is_ascii_alphanumeric() || byte == b'+' || byte == b'/'))
    {
        return Err(format!("{name} must be bytes"));
    }
    if padding > 0 && content_len % 4 != 4 - padding {
        return Err(format!("{name} must be bytes"));
    }
    Ok(())
}

fn map(value: &Value, name: &str) -> Result<()> {
    let entries = object(value)?
        .get("$map")
        .ok_or_else(|| format!("{name} must be a map"))?;
    let entries = entries
        .as_array()
        .ok_or_else(|| format!("{name} must be a map"))?;
    for entry in entries {
        let pair = entry
            .as_array()
            .ok_or_else(|| format!("{name} must be a map"))?;
        if pair.len() != 2 {
            return Err(format!("{name} must be a map"));
        }
        text(
            pair.first()
                .ok_or_else(|| format!("{name} must be a map"))?,
            "map key",
        )?;
        text(
            pair.get(1).ok_or_else(|| format!("{name} must be a map"))?,
            "map value",
        )?;
    }
    Ok(())
}

fn bucket(value: &Value) -> Result<()> {
    text(field(value, "bucketId")?, "bucketId")?;
    text(field(value, "name")?, "name")
}

fn metadata(value: &Value) -> Result<()> {
    let object = object(value)?;
    for name in [
        "contentType",
        "contentEncoding",
        "cacheControl",
        "contentDisposition",
        "contentLanguage",
    ] {
        if let Some(value) = object.get(name)
            && !value.is_null()
            && value.as_str().is_none()
        {
            return Err(format!("{name} must be a string"));
        }
    }
    if let Some(value) = object.get("expiresUnixSeconds") {
        bigint(value, "expiresUnixSeconds", false)?;
    }
    map(object.get("user").ok_or("missing user")?, "metadata.user")
}

fn version(value: &Value) -> Result<()> {
    text(field(value, "versionId")?, "versionId")?;
    text(field(value, "etag")?, "etag")?;
    bigint(field(value, "size")?, "size", true)?;
    boolean(field(value, "deleteMarker")?, "deleteMarker")?;
    if let Some(created_at) = object(value)?.get("createdAt") {
        text(created_at, "createdAt")?;
    }
    metadata(field(value, "metadata")?)
}

fn snapshot(value: &Value) -> Result<()> {
    text(field(value, "snapshotId")?, "snapshotId")?;
    text(field(value, "sourceBucketId")?, "sourceBucketId")
}

fn range(value: &Value) -> Result<()> {
    let start = integer(field(value, "start")?, "range.start")?;
    let end = integer(field(value, "endExclusive")?, "range.endExclusive")?;
    let total = integer(field(value, "total")?, "range.total")?;
    if start < 0 || end <= start || end > total {
        return Err("content range is invalid".into());
    }
    Ok(())
}

fn stored(value: &Value) -> Result<()> {
    version(field(value, "version")?)?;
    bytes(field(value, "body")?, "body")?;
    if let Some(value) = object(value)?.get("contentRange") {
        range(value)?;
    }
    Ok(())
}

fn multipart(value: &Value) -> Result<()> {
    text(field(value, "uploadId")?, "uploadId")?;
    bucket(field(value, "bucket")?)?;
    text(field(value, "objectKey")?, "objectKey")?;
    metadata(field(value, "metadata")?)
}

fn part(value: &Value) -> Result<()> {
    let number = integer(field(value, "partNumber")?, "partNumber")?;
    if !(1..=10_000).contains(&number) {
        return Err("partNumber must be between 1 and 10000".into());
    }
    text(field(value, "etag")?, "etag")?;
    bigint(field(value, "size")?, "size", true)
}

fn response_kind(route: &str) -> Option<&'static str> {
    HTTP_RESPONSE_CONTRACT
        .iter()
        .find_map(|(candidate, kind)| (*candidate == route).then_some(*kind))
}

fn validate(route: &str, value: &Value) -> Result<()> {
    match response_kind(route) {
        Some("bucket") => bucket(value),
        Some("existed") => boolean(value, "existed"),
        Some("version") => version(value),
        Some("stored") => stored(value),
        Some("delete") => {
            boolean(field(value, "existed")?, "existed")?;
            if let Some(marker) = object(value)?.get("marker") {
                version(marker)?;
            }
            Ok(())
        }
        Some("list") => {
            let entries = field(value, "entries")?
                .as_array()
                .ok_or_else(|| "entries must be an array".to_owned())?;
            for entry in entries {
                text(field(entry, "objectKey")?, "objectKey")?;
                version(field(entry, "version")?)?;
            }
            for prefix in field(value, "commonPrefixes")?
                .as_array()
                .ok_or_else(|| "commonPrefixes must be an array".to_owned())?
            {
                text(prefix, "commonPrefix")?;
            }
            if let Some(token) = object(value)?.get("continuation") {
                text(token, "continuation")?;
            }
            Ok(())
        }
        Some("snapshot") => snapshot(value),
        Some("multipart") => multipart(value),
        Some("part") => part(value),
        Some("parts") => {
            for value in field(value, "parts")?
                .as_array()
                .ok_or_else(|| "parts must be an array".to_owned())?
            {
                part(value)?;
            }
            Ok(())
        }
        Some(_) | None => Err(format!("unsupported Objects HTTP route: {route}")),
    }
}

/// Validate one successful hosted Objects response before the TypeScript decoder projects it.
#[wasm_bindgen]
#[allow(clippy::needless_pass_by_value)]
pub fn validate_http_response(
    route: String,
    response_json: String,
) -> std::result::Result<(), JsValue> {
    let value: Value = serde_json::from_str(&response_json)
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    validate(&route, &value).map_err(|error| JsValue::from_str(&error))
}

/// Validate and project one successful hosted Objects response into its natural JavaScript
/// values.  The wire wrappers stay an HTTP concern; callers receive `bigint`, `Uint8Array`,
/// and `Map` values without a second TypeScript reviver implementing this contract.
#[wasm_bindgen]
#[allow(
    clippy::needless_pass_by_value,
    reason = "wasm-bindgen exports owned JavaScript strings"
)]
pub fn decode_http_response(
    route: String,
    response_json: String,
) -> std::result::Result<JsValue, JsValue> {
    let value: Value = serde_json::from_str(&response_json)
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    validate(&route, &value).map_err(|error| JsValue::from_str(&error))?;
    project(&value).map_err(|error| JsValue::from_str(&error))
}

/// Encode one natural JavaScript request into the hosted Objects JSON wrapper format.
///
/// This is intentionally kept beside response validation and projection so the hosted adapter
/// has one Rust implementation of bigint, bytes, and map wrappers in both directions.
#[wasm_bindgen]
#[allow(
    clippy::needless_pass_by_value,
    reason = "wasm-bindgen accepts an owned JavaScript request value"
)]
pub fn encode_http_request(request: JsValue) -> std::result::Result<String, JsValue> {
    let active = Set::<JsValue>::new_empty();
    let value = encode_value(&request, &active)
        .map_err(|error| JsValue::from_str(&error))?
        .ok_or_else(|| JsValue::from_str("request must be a JSON value"))?;
    serde_json::to_string(&value).map_err(|error| JsValue::from_str(&error.to_string()))
}

fn encode_value(value: &JsValue, active: &Set<JsValue>) -> Result<Option<Value>> {
    if value.is_null() {
        return Ok(Some(Value::Null));
    }
    if value.is_undefined() || value.is_function() || value.is_symbol() {
        return Ok(None);
    }
    if let Some(value) = value.as_bool() {
        return Ok(Some(Value::Bool(value)));
    }
    if let Some(value) = value.as_string() {
        return Ok(Some(Value::String(value)));
    }
    if let Some(value) = value.as_f64() {
        return Ok(Some(
            serde_json::Number::from_f64(value)
                .map(Value::Number)
                .unwrap_or(Value::Null),
        ));
    }
    if value.is_bigint() {
        let bigint: BigInt = value.clone().unchecked_into();
        let value = String::from(
            bigint
                .to_string(10)
                .map_err(|_| "failed to stringify bigint".to_owned())?,
        );
        return Ok(Some(Value::Object(serde_json::Map::from_iter([(
            "$bigint".to_owned(),
            Value::String(value),
        )]))));
    }
    if !value.is_object() {
        return Ok(None);
    }
    if active.has(value) {
        return Err("request contains cycles".to_owned());
    }
    active.add(value);
    let result = (|| {
        if value.is_instance_of::<Uint8Array>() {
            let bytes: Uint8Array = value.clone().unchecked_into();
            return Ok(Some(Value::Object(serde_json::Map::from_iter([(
                "$bytes".to_owned(),
                Value::String(base64::engine::general_purpose::STANDARD.encode(bytes.to_vec())),
            )]))));
        }
        if Array::is_array(value) {
            let array: Array = value.clone().unchecked_into();
            let object: Object = array.clone().into();
            let mut result = Vec::with_capacity(array.length() as usize);
            for index in 0..array.length() {
                let key = JsValue::from_str(&index.to_string());
                let item = descriptor_value(&object, &key)?.unwrap_or(JsValue::NULL);
                result.push(encode_value(&item, active)?.unwrap_or(Value::Null));
            }
            return Ok(Some(Value::Array(result)));
        }
        if value.is_instance_of::<Map>() {
            let map: Map = value.clone().unchecked_into();
            let entries = Map::<JsValue, JsValue>::entries(&map);
            let mut result = Vec::with_capacity(map.size() as usize);
            for entry in entries {
                let entry = entry.map_err(|_| "failed to read map entry".to_owned())?;
                let pair = Array::from(&entry);
                let key = encode_value(&pair.get(0), active)?.unwrap_or(Value::Null);
                let value = encode_value(&pair.get(1), active)?.unwrap_or(Value::Null);
                result.push(Value::Array(vec![key, value]));
            }
            let mut wrapped = serde_json::Map::new();
            wrapped.insert("$map".to_owned(), Value::Array(result));
            return Ok(Some(Value::Object(wrapped)));
        }

        let object: Object = value.clone().unchecked_into();
        let mut result = serde_json::Map::new();
        for key in Object::keys(&object).iter() {
            let key = key
                .as_string()
                .ok_or_else(|| "request contains a non-string object key".to_owned())?;
            let property = descriptor_value(&object, &JsValue::from_str(&key))?
                .ok_or_else(|| "request property disappeared during encoding".to_owned())?;
            if let Some(property) = encode_value(&property, active)? {
                result.insert(key, property);
            }
        }
        Ok(Some(Value::Object(result)))
    })();
    active.delete(value);
    result
}

fn descriptor_value(object: &Object, key: &JsValue) -> Result<Option<JsValue>> {
    let descriptor = Object::get_own_property_descriptor(object, key);
    if descriptor.is_undefined() {
        return Ok(None);
    }
    let descriptor: Object = descriptor.unchecked_into();
    let getter = Reflect::get(descriptor.as_ref(), &JsValue::from_str("get"))
        .map_err(|_| "could not inspect request property".to_owned())?;
    if !getter.is_undefined() && !getter.is_null() {
        return Err("request contains an accessor property".to_owned());
    }
    Reflect::get(descriptor.as_ref(), &JsValue::from_str("value"))
        .map(Some)
        .map_err(|_| "could not inspect request property".to_owned())
}

fn project(value: &Value) -> Result<JsValue> {
    match value {
        Value::Null => Ok(JsValue::NULL),
        Value::Bool(value) => Ok(JsValue::from_bool(*value)),
        Value::Number(value) => value
            .as_i64()
            .map(|value| JsValue::from_f64(value as f64))
            .or_else(|| value.as_u64().map(|value| JsValue::from_f64(value as f64)))
            .or_else(|| value.as_f64().map(JsValue::from_f64))
            .ok_or_else(|| "invalid JSON number".to_owned()),
        Value::String(value) => Ok(JsValue::from_str(value)),
        Value::Array(values) => {
            let result = Array::new();
            for value in values {
                result.push(&project(value)?);
            }
            Ok(result.into())
        }
        Value::Object(values) => {
            if let Some(value) = values.get("$bigint").and_then(Value::as_str) {
                return BigInt::new(&JsValue::from_str(value))
                    .map(Into::into)
                    .map_err(|_| "invalid bigint".to_owned());
            }
            if let Some(value) = values.get("$bytes").and_then(Value::as_str) {
                let mut encoded = value.to_owned();
                match encoded.len() % 4 {
                    2 => encoded.push_str("=="),
                    3 => encoded.push('='),
                    _ => {}
                }
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(encoded)
                    .map_err(|_| "invalid bytes".to_owned())?;
                return Ok(Uint8Array::new_from_slice(&bytes).into());
            }
            if let Some(entries) = values.get("$map").and_then(Value::as_array) {
                let result = Map::new();
                for entry in entries {
                    let pair = entry.as_array().ok_or_else(|| "invalid map".to_owned())?;
                    let key = project(pair.first().ok_or_else(|| "invalid map".to_owned())?)?;
                    let value = project(pair.get(1).ok_or_else(|| "invalid map".to_owned())?)?;
                    result.set(&key, &value);
                }
                return Ok(result.into());
            }

            let has_user_map = values
                .get("user")
                .and_then(Value::as_object)
                .and_then(|value| value.get("$map"))
                .and_then(Value::as_array)
                .is_some();
            let is_object_version = values.contains_key("versionId")
                && values.contains_key("etag")
                && values.contains_key("size")
                && values.contains_key("deleteMarker")
                && values.contains_key("metadata");
            // Use a null-prototype object so a wire key such as `__proto__` is retained as
            // data instead of invoking Object.prototype's legacy setter.
            let result: Object = Object::create(&JsValue::NULL.unchecked_into());
            for (key, value) in values {
                // This is an SDK projection derived from the server's ISO timestamp.  Do not
                // let an optional wire property override the exact value computed below.
                if is_object_version && key == "createdAtUnixNanos" {
                    continue;
                }
                if has_user_map
                    && matches!(
                        key.as_str(),
                        "contentType"
                            | "contentEncoding"
                            | "cacheControl"
                            | "contentDisposition"
                            | "contentLanguage"
                    )
                    && value.is_null()
                {
                    Reflect::set(&result, &JsValue::from_str(key), &JsValue::from_str(""))
                        .map_err(|_| "failed to project metadata".to_owned())?;
                    continue;
                }
                let projected = if key == "createdAt" {
                    let (date, unix_nanos) = timestamp_js(value)?;
                    if is_object_version {
                        Reflect::set(
                            &result,
                            &JsValue::from_str("createdAtUnixNanos"),
                            &unix_nanos,
                        )
                        .map_err(|_| "failed to project object".to_owned())?;
                    }
                    date
                } else {
                    project(value)?
                };
                Reflect::set(&result, &JsValue::from_str(key), &projected)
                    .map_err(|_| "failed to project object".to_owned())?;
            }
            if is_object_version && !values.contains_key("createdAt") {
                Reflect::set(
                    &result,
                    &JsValue::from_str("createdAt"),
                    &JsValue::UNDEFINED,
                )
                .map_err(|_| "failed to project object".to_owned())?;
                Reflect::set(
                    &result,
                    &JsValue::from_str("createdAtUnixNanos"),
                    &JsValue::UNDEFINED,
                )
                .map_err(|_| "failed to project object".to_owned())?;
            }
            if has_user_map {
                for key in [
                    "contentType",
                    "contentEncoding",
                    "cacheControl",
                    "contentDisposition",
                    "contentLanguage",
                ] {
                    if !values.contains_key(key) {
                        Reflect::set(&result, &JsValue::from_str(key), &JsValue::from_str(""))
                            .map_err(|_| "failed to project metadata".to_owned())?;
                    }
                }
            }
            Ok(result.into())
        }
    }
}

fn timestamp_js(value: &Value) -> Result<(JsValue, JsValue)> {
    let timestamp = value
        .as_str()
        .ok_or_else(|| "createdAt must be an ISO timestamp".to_owned())?;
    let date = Date::new(&JsValue::from_str(timestamp));
    if date.get_time().is_nan() {
        return Err("createdAt must be an ISO timestamp".to_owned());
    }
    let unix_nanos = OffsetDateTime::parse(timestamp, &Rfc3339)
        .map_err(|_| "createdAt must be an ISO timestamp".to_owned())?
        .unix_timestamp_nanos()
        .to_string();
    let unix_nanos = BigInt::new(&JsValue::from_str(&unix_nanos))
        .map_err(|_| "createdAt must be an ISO timestamp".to_owned())?;
    Ok((date.into(), unix_nanos.into()))
}

#[cfg(test)]
mod tests {
    use super::validate;

    #[test]
    fn validates_all_response_families() {
        let version = r#"{"versionId":"v","etag":"e","size":{"$bigint":"1"},"deleteMarker":false,"metadata":{"user":{"$map":[["k","v"]]}}}"#;
        let version_value: serde_json::Value =
            serde_json::from_str(version).unwrap_or(serde_json::Value::Null);
        assert!(validate("objects/put", &version_value).is_ok());
        assert!(
            validate(
                "objects/put",
                &serde_json::json!({
                    "versionId": "v",
                    "etag": "e",
                    "size": {"$bigint": "1"},
                    "deleteMarker": false,
                    "createdAt": "2030-01-02T03:04:05.000Z",
                    "metadata": {"user": {"$map": []}}
                })
            )
            .is_ok()
        );
        assert!(
            validate(
                "objects/put",
                &serde_json::json!({
                    "versionId": "v",
                    "etag": "e",
                    "size": {"$bigint": "1"},
                    "deleteMarker": false,
                    "createdAt": 3,
                    "metadata": {"user": {"$map": []}}
                })
            )
            .is_err()
        );
        assert!(validate("objects/put", &serde_json::json!({"versionId":"v"})).is_err());
        assert!(
            validate(
                "multipart/upload-part",
                &serde_json::json!({"partNumber": 1,"etag":"e","size":{"$bigint":"2"}})
            )
            .is_ok()
        );
        assert!(
            validate(
                "objects/list",
                &serde_json::json!({"entries":[],"commonPrefixes":[]})
            )
            .is_ok()
        );
        assert!(
            validate(
                "objects/get",
                &serde_json::json!({
                    "version": version_value,
                    "body": {"$bytes": "YQ"}
                })
            )
            .is_ok()
        );
        assert!(
            validate(
                "objects/get",
                &serde_json::json!({
                    "version": version_value,
                    "body": {"$bytes": "YQ="}
                })
            )
            .is_err()
        );
        assert!(
            validate(
                "objects/get",
                &serde_json::json!({
                    "version": version_value,
                    "body": {"$bytes": "YQ==="}
                })
            )
            .is_err()
        );
        assert!(
            validate(
                "objects/list",
                &serde_json::json!({
                    "entries": [],
                    "commonPrefixes": [],
                    "continuation": 9_007_199_254_740_992_i64
                })
            )
            .is_err()
        );
    }
}
