use super::*;
use serde::de::DeserializeSeed;
use std::borrow::Cow;

/// Decodes a complete document after source and ownership admission.
///
/// The admission walks the document against the decoded type's
/// [`HeapLayout`](super::layout::HeapLayout) and charges the walk's peak
/// before the decode allocates.
pub fn json_from_slice_bounded<'de, S, F>(
    seed: S,
    bytes: &'de [u8],
    bounds: JsonBounds,
    code: Option<crate::wire::ErrorCode>,
    value_schema: Option<&'static JsonValueSchema>,
    charge: &mut F,
) -> Result<S::Value>
where
    S: DeserializeSeed<'de>,
    S::Value: super::layout::HeapLayout,
    F: FnMut(JsonCodecCharge) -> Result<()>,
{
    let bounds = JsonBounds {
        maximum_depth: bounds.maximum_depth.min(256),
        ..bounds
    };
    let admitted = admission::admit_typed(
        bytes,
        bounds,
        value_schema,
        <S::Value as super::layout::HeapLayout>::LAYOUT,
        code,
        charge,
    )?;
    let (value_shape, layout) = match admitted {
        Ok(admitted) => admitted,
        Err((value_shape, syntax)) => {
            // Nothing is decoded; the caller still holds the diagnostic the
            // syntax error renders.
            charge(JsonCodecCharge::Admission {
                maximum_owned_bytes: 0,
                value_shape,
            })?;
            return Err(decode_error(syntax, code));
        }
    };
    charge(JsonCodecCharge::Admission {
        maximum_owned_bytes: decoded_bytes::<S::Value>(layout)?,
        value_shape,
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

/// The heap a typed decode reaches at its peak, plus the decoded value
/// itself, which its holder owns once the decode returns.
pub(super) fn decoded_bytes<T>(layout: super::layout::LayoutCost) -> Result<u64> {
    layout
        .bytes()
        .checked_add(size_of::<T>() as u64)
        .ok_or_else(|| boundary("typed decode reservation overflows", None))
}

/// Converts a borrowed or consuming Value after its original tree admission
/// and the typed layout admission of the conversion.
pub fn json_from_value_bounded<T, F>(
    source: Cow<'_, Value>,
    bounds: JsonBounds,
    code: Option<crate::wire::ErrorCode>,
    value_schema: &'static JsonValueSchema,
    charge: &mut F,
) -> Result<T>
where
    T: DeserializeOwned + super::layout::HeapLayout,
    F: FnMut(JsonCodecCharge) -> Result<()>,
{
    let (mut value_shape, layout) = admission::admit_value_layout(
        source.as_ref(),
        bounds,
        value_schema,
        T::LAYOUT,
        code,
        charge,
    )?;
    if matches!(source, Cow::Borrowed(_)) {
        value_shape.origin = Origin::Borrowed;
    }
    charge(JsonCodecCharge::Admission {
        maximum_owned_bytes: decoded_bytes::<T>(layout)?,
        value_shape,
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
    let message = source.to_string();
    drop(source);
    Error::Invalid(message)
}

/// Known owned String and container slot capacities of a resident Value.
pub fn json_value_logical_owned_bytes(value: &Value) -> Result<u64> {
    value_owned_bytes(value, false)
}

pub(crate) fn json_value_retained_owned_bytes(value: &Value) -> Result<u64> {
    value_owned_bytes(value, true)
}

/// The bytes an object of `entries` entries holds in its own slots,
/// excluding what its keys and values own.
fn object_slot_bytes(entries: usize, retained: bool) -> Option<u64> {
    if retained && entries != 0 {
        let node = 11 * std::mem::size_of::<(String, Value)>() + 14 * std::mem::size_of::<usize>();
        (1 + (entries - 1) / 5).checked_mul(node)
    } else {
        entries.checked_mul(std::mem::size_of::<(String, Value)>())
    }
    .map(|bytes| bytes as u64)
}

/// Sets `key` to `value` in `object`, returning the retained bytes the
/// object grew by ([`json_value_retained_owned_bytes`]), zero when it shrank.
pub(crate) fn json_object_insert(
    object: &mut serde_json::Map<String, Value>,
    key: &str,
    value: Value,
) -> Result<u64> {
    let overflow = || {
        boundary(
            "JSON value capacity inventory overflows",
            Some(crate::wire::ErrorCode::Invalid),
        )
    };
    let before = object_slot_bytes(object.len(), true).ok_or_else(overflow)?;
    let added = value_owned_bytes(&value, true)?;
    let (added, removed) = match object.insert(key.to_owned(), value) {
        Some(old) => (added, value_owned_bytes(&old, true)?),
        None => (added.checked_add(key.len() as u64).ok_or_else(overflow)?, 0),
    };
    let after = object_slot_bytes(object.len(), true).ok_or_else(overflow)?;
    Ok((after - before)
        .checked_add(added)
        .ok_or_else(overflow)?
        .saturating_sub(removed))
}

fn value_owned_bytes(value: &Value, retained: bool) -> Result<u64> {
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
                    .checked_add(value_owned_bytes(value, retained)?)
                    .ok_or_else(overflow)
            })
        }
        Value::Object(values) => {
            // std BTreeMap nodes contain 11 key/value slots, a two-word
            // header, and (for internal nodes) 12 child pointers. The root
            // owns at least one entry; every other node owns at least five.
            // Newly decoded empty maps have no node. Retain the bound until
            // destruction, including unused slots in sparsely filled nodes.
            let slots = object_slot_bytes(values.len(), retained).ok_or_else(overflow)?;
            values.iter().try_fold(slots, |bytes, (key, value)| {
                let value_bytes = value_owned_bytes(value, retained)?;
                bytes
                    .checked_add(key.capacity() as u64)
                    .and_then(|bytes| bytes.checked_add(value_bytes))
                    .ok_or_else(overflow)
            })
        }
        _ => Ok(0),
    }
}
