use super::*;

/// Decodes one canonical event payload using the native event union.
#[wasm_bindgen(js_name = decodeEventPayload)]
pub fn decode_event_payload(
    event_type: String,
    canonical_payload_json: Vec<u8>,
) -> Result<JsValue, JsValue> {
    let payload = decode_payload_wire(&event_type, &canonical_payload_json).map_err(js_error)?;
    to_js(&payload)
}

/// Decodes and validates a canonical Protobuf apply response.
#[wasm_bindgen(js_name = decodeApplyResponse)]
pub fn decode_apply_response(bytes: Vec<u8>) -> Result<JsValue, JsValue> {
    let response = crate::wire::ApplyResponse::decode(bytes.as_slice())
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let envelope = response
        .event
        .ok_or_else(|| JsValue::from_str("apply response event is missing"))?;
    let (_, event) = crate::wire_codec::decode_event(&envelope.encode_to_vec(), &mut None)
        .map_err(js_error)?
        .0;
    let result = match crate::wire::ApplyState::try_from(response.state)
        .map_err(|_| JsValue::from_str("apply response state is invalid"))?
    {
        crate::wire::ApplyState::Applied => ApplyResult::Applied { event },
        crate::wire::ApplyState::Replayed => ApplyResult::Replayed { event },
        crate::wire::ApplyState::Unspecified => {
            return Err(JsValue::from_str("apply response state is unspecified"));
        }
    };
    to_js(&result)
}

/// Decodes a generated aggregate kind using the native enum mapping.
#[wasm_bindgen(js_name = decodeAggregateKind)]
pub fn decode_aggregate_kind_wasm(value: i32) -> Result<JsValue, JsValue> {
    let kind =
        crate::wire_codec::decode_aggregate_kind(value, crate::Error::Invalid).map_err(js_error)?;
    to_js(&kind)
}
