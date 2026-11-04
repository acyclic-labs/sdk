//! Shared JavaScript projection of a member's original refusal envelope.
use js_sys::{Reflect, Uint8Array};
use wasm_bindgen::JsCast as _;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(inline_js = "
export function refusalStatus(value) {
    const status = value.status;
    if (typeof status !== 'number' || (status | 0) !== status)
        throw new TypeError('invalid refusal status');
    return status;
}
export function refusalArrayLength(value) {
    if (!Array.isArray(value)) throw new TypeError('invalid refusal array');
    const length = value.length;
    if (!Number.isInteger(length) || length < 0 || length > 0xffffffff)
        throw new TypeError('invalid refusal array length');
    return length;
}
")]
extern "C" {
    #[wasm_bindgen(catch, js_name = refusalStatus)]
    fn status(value: &JsValue) -> Result<i32, JsValue>;
    #[wasm_bindgen(catch, js_name = refusalArrayLength)]
    fn array_length(value: &JsValue) -> Result<u32, JsValue>;
}

/// Owns the original envelope emitted by [`value`]. Every getter is caught;
/// each byte view goes through the caller's shared admitted snapshot once,
/// with no await between reading its extent and copying it. Metadata stays
/// raw HTTP header bytes, including binary values' original encoding.
pub fn parts(
    value: &JsValue,
    mut snapshot: impl FnMut(&Uint8Array) -> Result<Vec<u8>, JsValue>,
) -> Result<super::Parts, JsValue> {
    let invalid = || JsValue::from_str("invalid refusal envelope");
    let code = status(value)?;
    let message = Reflect::get(value, &"message".into())?
        .as_string()
        .ok_or_else(invalid)?;
    let details = Reflect::get(value, &"details".into())?
        .dyn_into::<Uint8Array>()
        .map_err(|_| invalid())?;
    let details = snapshot(&details)?.into();
    let entries = Reflect::get(value, &"metadata".into())?;
    let count = array_length(&entries)?;
    let mut metadata = http::HeaderMap::new();
    for index in 0..count {
        let pair = Reflect::get(&entries, &index.into())?;
        if array_length(&pair)? != 2 {
            return Err(invalid());
        }
        let name = Reflect::get(&pair, &0.into())?
            .as_string()
            .ok_or_else(invalid)?;
        let name = http::HeaderName::from_bytes(name.as_bytes()).map_err(|_| invalid())?;
        let bytes = Reflect::get(&pair, &1.into())?
            .dyn_into::<Uint8Array>()
            .map_err(|_| invalid())?;
        let bytes = snapshot(&bytes)?;
        let header = http::HeaderValue::from_maybe_shared(bytes).map_err(|_| invalid())?;
        metadata.try_append(name, header).map_err(|_| invalid())?;
    }
    Ok(super::Parts {
        code,
        message,
        details,
        metadata,
    })
}

/// A member's original status, verbatim: metadata values stay bytes.
#[must_use]
pub fn value(parts: &super::Parts) -> JsValue {
    let refusal = js_sys::Object::new();
    let metadata = js_sys::Array::new();
    for (name, value) in &parts.metadata {
        metadata.push(&js_sys::Array::of2(
            &JsValue::from_str(name.as_str()),
            &js_sys::Uint8Array::from(value.as_bytes()),
        ));
    }
    for (key, field) in [
        ("status", JsValue::from_f64(f64::from(parts.code))),
        ("message", JsValue::from_str(&parts.message)),
        (
            "details",
            js_sys::Uint8Array::from(&parts.details[..]).into(),
        ),
        ("metadata", metadata.into()),
    ] {
        let _property_result = Reflect::set(&refusal, &JsValue::from_str(key), &field);
    }
    refusal.into()
}
