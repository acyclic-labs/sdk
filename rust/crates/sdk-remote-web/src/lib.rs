//! Rust-owned browser admission and response-boundary helpers.
//!
//! Fetch, streaming, and cancellation remain JavaScript platform adapters.
//! Endpoint, credential, request admission, and cumulative response bounds are
//! evaluated here so browser and native projections share one policy.

use acyclic_sdk_contract_wire::{BEARER_NO_CRLF, credential};
use url::Url;
use wasm_bindgen::prelude::*;

fn invalid(message: &str) -> JsValue {
    JsValue::from_str(message)
}

fn loopback(host: Option<&str>) -> bool {
    matches!(host, Some("localhost" | "127.0.0.1" | "::1" | "[::1]"))
}

/// Validates the HTTPS or loopback-HTTP endpoint shared by Actors and Workers.
#[wasm_bindgen]
pub fn validate_remote_web_endpoint(endpoint: &str) -> Result<(), JsValue> {
    let parsed = Url::parse(endpoint).map_err(|_| invalid("invalid remote endpoint"))?;
    let secure = parsed.scheme() == "https";
    let local_http = parsed.scheme() == "http" && loopback(parsed.host_str());
    if !(secure || local_http)
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(invalid(
            "endpoint must be HTTPS or loopback HTTP without credentials, query, or fragment",
        ));
    }
    Ok(())
}

/// Validates one bearer credential according to the shared Rust policy.
#[wasm_bindgen]
pub fn validate_remote_web_credential(token: &str) -> Result<(), JsValue> {
    credential::validate(BEARER_NO_CRLF, token)
        .then_some(())
        .ok_or_else(|| invalid("invalid bearer credential"))
}

/// Advances a cumulative response byte count under the caller's configured bound.
#[wasm_bindgen]
pub fn validate_remote_web_response_chunk(
    observed: u64,
    chunk: u64,
    maximum: u64,
) -> Result<u64, JsValue> {
    if maximum == 0 {
        return Err(invalid("response bound must be positive"));
    }
    let total = observed
        .checked_add(chunk)
        .ok_or_else(|| invalid("response exceeds configured bound"))?;
    if total > maximum {
        return Err(invalid("response exceeds configured bound"));
    }
    Ok(total)
}

/// Validates the configured cumulative response bound.
#[wasm_bindgen]
pub fn validate_remote_web_response_limit(maximum: u64) -> Result<(), JsValue> {
    if maximum == 0 {
        return Err(invalid("response bound must be positive"));
    }
    Ok(())
}

/// Checks an HTTP content-length without first narrowing it through a JS number.
#[wasm_bindgen]
pub fn validate_remote_web_content_length(
    content_length: &str,
    maximum: u64,
) -> Result<(), JsValue> {
    let chunk = content_length
        .parse::<u64>()
        .map_err(|_| invalid("invalid response content length"))?;
    validate_remote_web_response_chunk(0, chunk, maximum).map(|_| ())
}

fn validate_method(method: &str) -> Result<(), JsValue> {
    // Protobuf string fields use the empty string as their wire default. The
    // service keeps accepting that default for backwards-compatible request
    // construction; a caller-provided method is still checked for header
    // delimiter bytes before it reaches fetch.
    if method.contains(['\r', '\n']) {
        return Err(invalid("method contains forbidden header delimiter"));
    }
    Ok(())
}

fn validate_alias(alias: &str) -> Result<(), JsValue> {
    if alias.is_empty()
        || alias == "."
        || alias == ".."
        || alias.len() > 256
        || !alias
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(invalid("invalid deployment alias"));
    }
    Ok(())
}

/// Validates the Rust Workers invoke-version path and request admission rules.
#[wasm_bindgen]
pub fn validate_workers_invoke_version(version_sha256: &[u8], method: &str) -> Result<(), JsValue> {
    if version_sha256.len() != 32 {
        return Err(invalid("version digest must contain exactly 32 bytes"));
    }
    validate_method(method)
}

/// Validates the Rust Workers invoke-deployment path and request admission rules.
#[wasm_bindgen]
pub fn validate_workers_invoke_deployment(alias: &str, method: &str) -> Result<(), JsValue> {
    validate_alias(alias)?;
    validate_method(method)
}

/// Validates the Rust Actors invoke request admission rules.
#[wasm_bindgen]
pub fn validate_actors_invoke(actor_id: &str, method: &str) -> Result<(), JsValue> {
    if actor_id.is_empty() {
        return Err(invalid("actor id must be non-empty"));
    }
    validate_method(method)
}
