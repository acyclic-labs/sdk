use super::*;
use serde::de::DeserializeSeed;
use std::borrow::Cow;

/// Decodes a complete document after source and ownership admission.
pub fn json_from_slice_bounded<
    'de,
    S: DeserializeSeed<'de>,
    F: FnMut(JsonCodecCharge) -> Result<()>,
>(
    seed: S,
    bytes: &'de [u8],
    bounds: JsonBounds,
    maximum_owned_bytes: u64,
    code: Option<crate::wire::ErrorCode>,
    value_schema: Option<JsonValueSchema>,
    charge: &mut F,
) -> Result<S::Value> {
    let bounds = JsonBounds {
        maximum_depth: bounds.maximum_depth.min(256),
        ..bounds
    };
    let shape = admission::source(
        bytes,
        bounds,
        value_schema,
        code.map(|_| crate::wire::ErrorCode::Invalid),
        charge,
    )?;
    let owned = (bytes.len() as u64)
        .max(8)
        .checked_mul(2)
        .and_then(|scratch| maximum_owned_bytes.checked_add(scratch))
        .ok_or_else(|| {
            boundary(
                "JSON owned bound overflow",
                code.map(|_| crate::wire::ErrorCode::Invalid),
            )
        })?;
    charge(JsonCodecCharge::Admission {
        maximum_owned_bytes: owned,
        value_shape: shape,
    })?;
    charge(JsonCodecCharge::Work { bytes: 0, items: 0 })?;
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    decoder.disable_recursion_limit();
    let value = seed
        .deserialize(&mut decoder)
        .map_err(|error| decode_error(error, code))?;
    decoder.end().map_err(|error| decode_error(error, code))?;
    Ok(value)
}

/// Converts a borrowed or consuming Value after its original tree admission.
pub fn json_from_value_bounded<T: DeserializeOwned, F: FnMut(JsonCodecCharge) -> Result<()>>(
    source: Cow<'_, Value>,
    bounds: JsonBounds,
    maximum_owned_bytes: u64,
    code: Option<crate::wire::ErrorCode>,
    value_schema: JsonValueSchema,
    charge: &mut F,
) -> Result<T> {
    let mut shape = json_owned_value_shape_bounded(source.as_ref(), bounds, value_schema, charge)?;
    if matches!(source, Cow::Borrowed(_)) {
        shape.origin = Origin::Borrowed;
    }
    charge(JsonCodecCharge::Admission {
        maximum_owned_bytes,
        value_shape: shape,
    })?;
    match source {
        Cow::Borrowed(value) => T::deserialize(value),
        Cow::Owned(value) => T::deserialize(value),
    }
    .map_err(|error| decode_error(error, code))
}

pub(super) fn decode_error(
    source: serde_json::Error,
    code: Option<crate::wire::ErrorCode>,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(code) = code {
        return Error::NativeJson {
            code,
            source,
            custody: None,
        };
    }
    let _ = code;
    Error::Invalid(source.to_string())
}

/// Known owned String and container slot capacities of a resident Value.
pub(crate) fn json_value_logical_owned_bytes(value: &Value) -> Result<u64> {
    let overflow = || {
        boundary(
            "JSON value capacity inventory overflows",
            Some(crate::wire::ErrorCode::Invalid),
        )
    };
    match value {
        Value::String(value) => Ok(value.capacity() as u64),
        Value::Array(values) => {
            let slots = values
                .capacity()
                .checked_mul(std::mem::size_of::<Value>())
                .ok_or_else(overflow)? as u64;
            values.iter().try_fold(slots, |bytes, value| {
                bytes
                    .checked_add(json_value_logical_owned_bytes(value)?)
                    .ok_or_else(overflow)
            })
        }
        Value::Object(values) => {
            let slots = values
                .len()
                .checked_mul(std::mem::size_of::<(String, Value)>())
                .ok_or_else(overflow)? as u64;
            values.iter().try_fold(slots, |bytes, (key, value)| {
                let value_bytes = json_value_logical_owned_bytes(value)?;
                bytes
                    .checked_add(key.capacity() as u64)
                    .and_then(|bytes| bytes.checked_add(value_bytes))
                    .ok_or_else(overflow)
            })
        }
        _ => Ok(0),
    }
}
