use super::*;

/// Logical owned capacities of an already admitted, finite-depth Value.
pub(crate) fn owned_logical_bytes(value: &Value) -> u64 {
    match value {
        Value::String(value) => value.capacity() as u64,
        Value::Array(values) => (values.capacity() * std::mem::size_of::<Value>()) as u64
            + values.iter().map(owned_logical_bytes).sum::<u64>(),
        Value::Object(values) => (values.len() * std::mem::size_of::<(String, Value)>()) as u64
            + values.iter().map(|(key, value)| key.capacity() as u64 + owned_logical_bytes(value)).sum::<u64>(),
        _ => 0,
    }
}

/// Closed SDK-generated encoder/Raw-Value diagnostic allowance only.
pub const fn sdk_json_diagnostic_bytes() -> u64 {
    const CAPACITY_ERROR: &str = concat!("memory allocation failed",
        " because the computed capacity exceeded the collection's maximum");
    const SYNTAX_ERROR: &str =
        "control character (\\u0000-\\u001F) found while parsing a string";
    let largest = if CAPACITY_ERROR.len() > SYNTAX_ERROR.len() {
        CAPACITY_ERROR.len() as u64
    } else { SYNTAX_ERROR.len() as u64 };
    let rendered = largest + "invalid request: ".len() as u64
        + " at line ".len() as u64 + " column ".len() as u64
        + 2 * (u64::MAX.ilog10() as u64 + 1);
    let wrappers = std::mem::size_of::<CodecError>() as u64
        + std::mem::size_of::<serde_json::Error>() as u64
        + std::mem::size_of::<std::io::Error>() as u64
        + std::mem::size_of::<String>() as u64;
    4 * rendered + 2 * wrappers
}

/// Conservative logical quota for the CLOSED serde_json::Value parser, derived from the SDK-issued shape in the ORIGINAL source pass.
pub fn json_value_decode_owned_bytes(shape: JsonValueShape) -> Result<u64> {
    value_full_bytes(shape).ok_or_else(|| shape_boundary("JSON owned bound overflow"))
}
pub(super) fn value_full_bytes(shape: JsonValueShape) -> Option<u64> {
    value_storage_bytes(shape)
        .and_then(|bytes| shape.input_bytes.max(8).checked_mul(4).and_then(|strings| bytes.checked_add(strings)))
}

/// Inspection only before an SDK's existing consuming from_value conversion.
pub(crate) fn json_owned_value_shape_bounded<F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &Value, bounds: JsonBounds, schema: JsonValueSchema, charge: &mut F,
) -> Result<JsonValueShape> {
    value_shape::inspect_owned(value, JsonBounds { maximum_depth: bounds.maximum_depth.min(256), ..bounds }, schema, charge)
        .map_err(|failure| match failure {
            AdmissionFailure::Root(error) => error,
            AdmissionFailure::Bound(reason) => shape_boundary(reason),
        })
}

/// NEW logical Value-container/Raw replay/scratch/static diagnostic quota for a genuine consuming Value converter, using the original SDK-issued owned shape.
pub fn json_owned_value_conversion_additional_bytes(shape: JsonValueShape) -> Result<u64> {
    if shape.origin != ShapeOrigin::OwnedConversion { return Err(shape_boundary("JSON conversion shape is not owned")); }
    value_storage_bytes(shape).ok_or_else(|| shape_boundary("JSON owned bound overflow"))
}

/// Pure arithmetic for an audited SDK BORROWED closed DTO.
pub(crate) fn json_typed_value_decode_owned_bytes(shape: JsonValueShape, maximum_slot_bytes: u64) -> Result<u64> {
    if shape.origin != ShapeOrigin::BorrowedConversion {
        return Err(shape_boundary("JSON typed decode shape is not borrowed"));
    }
    shape_nodes(shape)
        .and_then(|nodes| nodes.checked_mul(8))
        .and_then(|slots| slots.checked_mul(maximum_slot_bytes))
        .and_then(|typed| value_full_bytes(shape).and_then(|value| typed.checked_add(value)))
        .ok_or_else(|| shape_boundary("JSON typed owned bound overflow"))
}

pub(super) fn shape_nodes(shape: JsonValueShape) -> Option<u64> {
    if shape.unresolved_raw { Some(shape.input_bytes.max(shape.lexical_items)) }
    else { shape.lexical_items.checked_add(shape.virtual_items) }
}

pub(crate) fn json_value_virtual_owned_bytes(shape: JsonValueShape) -> Result<u64> {
    let mut normal = shape;
    normal.virtual_items = 0;
    normal.raw_bytes = 0;
    normal.raw_frames = 0;
    normal.unresolved_raw = false;
    value_storage_bytes(shape)
        .and_then(|actual| value_storage_bytes(normal).and_then(|baseline| actual.checked_sub(baseline)))
        .ok_or_else(|| shape_boundary("JSON virtual owned bound overflow"))
}

pub(super) fn shape_boundary(reason: &'static str) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    { Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason } }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    { Error::Invalid(reason.into()) }
}

pub(super) fn value_storage_bytes(shape: JsonValueShape) -> Option<u64> {
    let nodes = shape_nodes(shape)?;
    let slots = nodes.checked_mul((10 * std::mem::size_of::<Value>()
        + std::mem::size_of::<(String, Value)>()) as u64)?;
    let raw = if shape.unresolved_raw {
        shape.input_bytes.max(8).checked_mul(4 * (u64::from(usize::BITS) + 2))?
    } else {
        shape.raw_bytes.checked_mul(4)
            .and_then(|bytes| shape.raw_frames.checked_mul(16).and_then(|minimum| bytes.checked_add(minimum)))
            ?
    };
    slots.checked_add(raw)
        .and_then(|bytes| bytes.checked_add(sdk_json_diagnostic_bytes()))
}
