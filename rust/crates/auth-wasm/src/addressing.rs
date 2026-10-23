//! Every language-visible operation delegates to the shared pair codec.
use acyclic_auth::addressing::{CanonicalHost, ResourceIdentity};
use wasm_bindgen::prelude::*;

fn error(name: &str, code: &str, value: impl std::fmt::Display) -> JsValue {
    let error = js_sys::Error::new(&value.to_string());
    error.set_name(name);
    // Error.code is the stable discriminant; message remains descriptive.
    let _ = js_sys::Reflect::set(
        error.as_ref(),
        &JsValue::from_str("code"),
        &JsValue::from_str(code),
    );
    error.into()
}
fn identity_error(value: acyclic_auth::addressing::IdentityError) -> JsValue {
    error("IdentityError", value.code(), value)
}
fn dns_error(value: acyclic_auth::addressing::DnsError) -> JsValue {
    error("DnsError", value.code(), value)
}

/// Canonical wire/storage bytes for the literal (account, id) pair.
#[wasm_bindgen(js_name = encodeResourceIdentity)]
pub fn encode_resource_identity(account: &[u8], id: &[u8]) -> Result<Vec<u8>, JsValue> {
    Ok(ResourceIdentity::new(account, id)
        .map_err(identity_error)?
        .encode())
}
/// Validate a canonical encoding; returns the exact account component.
#[wasm_bindgen(js_name = resourceIdentityAccount)]
pub fn resource_identity_account(encoded: &[u8]) -> Result<Vec<u8>, JsValue> {
    Ok(ResourceIdentity::decode(encoded)
        .map_err(identity_error)?
        .account()
        .as_bytes()
        .to_vec())
}
/// Validate a canonical encoding; returns the exact id component.
#[wasm_bindgen(js_name = resourceIdentityId)]
pub fn resource_identity_id(encoded: &[u8]) -> Result<Vec<u8>, JsValue> {
    Ok(ResourceIdentity::decode(encoded)
        .map_err(identity_error)?
        .id()
        .as_bytes()
        .to_vec())
}
/// Direct account-id hostname, unchanged for HTTP Host, SNI and signing.
#[wasm_bindgen(js_name = resourceIdentityHostname)]
pub fn resource_identity_hostname(encoded: &[u8], domain: &str) -> Result<String, JsValue> {
    Ok(ResourceIdentity::decode(encoded)
        .map_err(identity_error)?
        .hostname(domain)
        .map_err(dns_error)?
        .as_str()
        .to_owned())
}
/// Validate an already composed canonical hostname.
#[wasm_bindgen(js_name = canonicalResourceHost)]
pub fn canonical_resource_host(host: &str) -> Result<String, JsValue> {
    Ok(CanonicalHost::parse(host)
        .map_err(dns_error)?
        .as_str()
        .to_owned())
}
