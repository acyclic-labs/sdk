use super::*;

/// Validates a protobuf handshake; returns encoded `Error` bytes, or empty on success.
#[wasm_bindgen(js_name = validateWireHandshake)]
pub fn validate_wire_handshake(request: Vec<u8>, response: Vec<u8>) -> Vec<u8> {
    validate_wire(|| crate::wire_validation::validate_wire_handshake(&request, &response))
}

/// Validates a protobuf replay request against the compiled protocol identity.
#[wasm_bindgen(js_name = validateWireResume)]
pub fn validate_wire_resume(request: Vec<u8>) -> Vec<u8> {
    validate_wire(|| {
        let request = crate::wire::ResumeRequest::decode(request.as_slice())
            .map_err(|error| crate::Error::Invalid(format!("invalid resume request: {error}")))?;
        crate::wire_api::validate_resume_protocol(&request)
    })
}

/// Validates a protobuf observe request using the canonical Rust scope rules.
#[wasm_bindgen(js_name = validateWireObserve)]
pub fn validate_wire_observe(request: Vec<u8>) -> Vec<u8> {
    validate_wire(|| {
        let request = crate::wire::ObserveRequest::decode(request.as_slice())
            .map_err(|error| crate::Error::Invalid(format!("invalid observe request: {error}")))?;
        crate::wire_api::validate_observe_request(&request).map(|_| ())
    })
}

/// Validates a protobuf cancel request using the canonical Rust scope rules.
#[wasm_bindgen(js_name = validateWireCancel)]
pub fn validate_wire_cancel(request: Vec<u8>) -> Vec<u8> {
    validate_wire(|| {
        let request = crate::wire::CancelRequest::decode(request.as_slice())
            .map_err(|error| crate::Error::Invalid(format!("invalid cancel request: {error}")))?;
        crate::wire_api::validate_cancel_request(&request).map(|_| ())
    })
}

/// Validates a protobuf operation status identity; returns encoded `Error` bytes, or empty on success.
#[wasm_bindgen(js_name = validateWireStatus)]
pub fn validate_wire_status(request: Vec<u8>, status: Vec<u8>) -> Vec<u8> {
    validate_wire(|| crate::wire_validation::validate_wire_status(&request, &status))
}

/// Validates a protobuf cancellation identity; returns encoded `Error` bytes, or empty on success.
#[wasm_bindgen(js_name = validateWireCancellation)]
pub fn validate_wire_cancellation(request: Vec<u8>, response: Vec<u8>) -> Vec<u8> {
    validate_wire(|| crate::wire_validation::validate_wire_cancellation(&request, &response))
}

pub(super) fn validate_wire(validate: impl FnOnce() -> crate::Result<()>) -> Vec<u8> {
    validate().map_or_else(
        |error| crate::encode_error(&error).encode_to_vec(),
        |_| Vec::new(),
    )
}

#[wasm_bindgen(js_name = decodeAggregateKind)]
pub fn decode_aggregate_kind_wasm(value: i32) -> Result<JsValue, JsValue> {
    let kind =
        crate::wire_codec::decode_aggregate_kind(value, crate::wire::ErrorCode::Invalid, &None)
            .map_err(js_error)?;
    to_js(&kind)
}
