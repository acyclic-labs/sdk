//! The address codec is shared with native Rust; JavaScript never rehashes it.
use acyclic_auth::addressing::{AddressError, CanonicalHost, ResourceKind, ResourceName};
use wasm_bindgen::prelude::*;

fn error(value: impl std::fmt::Display) -> JsValue {
    let error = js_sys::Error::new(&value.to_string());
    error.set_name("AddressError");
    error.into()
}

/// Derive the native codec's 32-byte address from exact caller bytes.
#[wasm_bindgen(js_name = resourceAddress)]
pub fn resource_address(account: &[u8], kind: f64, name: &[u8]) -> Result<Vec<u8>, JsValue> {
    Ok(resource(account, kind, name)
        .map_err(error)?
        .address()
        .as_bytes()
        .to_vec())
}

/// Encode the same transcript used for the native address digest.
#[wasm_bindgen(js_name = resourceAddressPreimage)]
pub fn resource_address_preimage(
    account: &[u8],
    kind: f64,
    name: &[u8],
) -> Result<Vec<u8>, JsValue> {
    Ok(resource(account, kind, name).map_err(error)?.encode())
}

fn resource<'a>(
    account: &'a [u8],
    kind: f64,
    name: &'a [u8],
) -> Result<ResourceName<'a>, AddressError> {
    let kind = match kind {
        1.0 => ResourceKind::Bucket,
        2.0 => ResourceKind::Stream,
        3.0 => ResourceKind::Actor,
        4.0 => ResourceKind::Worker,
        5.0 => ResourceKind::FsWorkspace,
        6.0 => ResourceKind::Machine,
        _ => return Err(AddressError::Malformed),
    };
    ResourceName::new(account, kind, name)
}

/// Validate and return the unique host spelling used for HTTP, SNI and signing.
#[wasm_bindgen(js_name = canonicalResourceHost)]
pub fn canonical_resource_host(host: &str) -> Result<String, JsValue> {
    Ok(CanonicalHost::parse(host)
        .map_err(error)?
        .as_str()
        .to_owned())
}
