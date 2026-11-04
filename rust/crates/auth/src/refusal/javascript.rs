//! Shared JavaScript projection of a member's original refusal envelope.
use js_sys::Reflect;
use wasm_bindgen::prelude::*;

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
