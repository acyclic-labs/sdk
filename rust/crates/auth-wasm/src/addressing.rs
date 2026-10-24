//! Every language-visible operation delegates to the shared pair codec.
use acyclic_auth::addressing::{ActorReference, CanonicalHost, ResourceIdentity};
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

/// Admit exactly one lowercase LDH label through the shared Rust validator.
#[wasm_bindgen(js_name = validateResourceLabel)]
pub fn validate_resource_label(label: &[u8]) -> Result<(), JsValue> {
    acyclic_auth::addressing::validate_dns_label(label).map_err(dns_error)
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

/// Parse a Host through the exact product suffix without normalizing it.
#[wasm_bindgen(js_name = resourceIdentityFromHostname)]
pub fn resource_identity_from_hostname(host: &str, domain: &str) -> Result<Vec<u8>, JsValue> {
    Ok(ResourceIdentity::from_hostname(host, domain)
        .map_err(dns_error)?
        .encode())
}

/// Encode a namespace-scoped address. No credential is present in this form.
#[wasm_bindgen(js_name = encodeActorReference)]
pub fn encode_actor_reference(
    account: &[u8],
    namespace: &[u8],
    id: &[u8],
) -> Result<Vec<u8>, JsValue> {
    Ok(ActorReference::new(account, namespace, id)
        .map_err(identity_error)?
        .encode())
}

/// Resolve only the absent legacy namespace field, preserving the old ID.
#[wasm_bindgen(js_name = actorReferenceFromLegacyFields)]
pub fn actor_reference_from_legacy_fields(
    account: &[u8],
    namespace: Option<String>,
    id: &str,
) -> Result<Vec<u8>, JsValue> {
    Ok(
        ActorReference::from_legacy_fields(account, namespace.as_deref(), id)
            .map_err(identity_error)?
            .encode(),
    )
}

/// Recover an exact actor-reference field after validating the entire encoding.
#[wasm_bindgen(js_name = actorReferenceField)]
pub fn actor_reference_field(encoded: &[u8], field: u32) -> Result<Vec<u8>, JsValue> {
    let value = ActorReference::decode(encoded).map_err(identity_error)?;
    match field {
        0 => Ok(value.account().as_bytes().to_vec()),
        1 => Ok(value.namespace().as_str().as_bytes().to_vec()),
        2 => Ok(value.id().as_bytes().to_vec()),
        _ => Err(identity_error(
            acyclic_auth::addressing::IdentityError::Malformed,
        )),
    }
}
/// Canonicalize a Worker route declaration using the shared Rust grammar.
#[wasm_bindgen(js_name = canonicalWorkerRoute)]
pub fn canonical_worker_route(pattern: &str) -> Result<String, JsValue> {
    acyclic_auth::addressing::RoutePattern::parse(pattern)
        .map(|p| p.as_str().to_owned())
        .map_err(|e| error("RouteError", "invalid_route", e))
}
/// Match a URL including its query, through the same route implementation as Rust.
#[wasm_bindgen(js_name = workerRouteMatches)]
pub fn worker_route_matches(pattern: &str, url: &str) -> Result<bool, JsValue> {
    acyclic_auth::addressing::RoutePattern::parse(pattern)
        .and_then(|p| p.matches(url))
        .map_err(|e| error("RouteError", "invalid_route", e))
}
/// Select a target; empty target means an exclusion, undefined means no match.
#[wasm_bindgen(js_name = selectWorkerRoute)]
pub fn select_worker_route(
    patterns: Vec<String>,
    targets: Vec<String>,
    url: &str,
) -> Result<Option<String>, JsValue> {
    use acyclic_auth::addressing::{RoutePattern, WorkerRoute, select_route};
    if patterns.len() != targets.len() {
        return Err(error(
            "RouteError",
            "invalid_route",
            "route arrays differ in length",
        ));
    }
    let routes = patterns
        .into_iter()
        .zip(targets)
        .map(|(pattern, target)| {
            Ok(WorkerRoute {
                pattern: RoutePattern::parse(&pattern)?,
                target: if target.is_empty() {
                    None
                } else {
                    Some(target)
                },
            })
        })
        .collect::<Result<Vec<_>, acyclic_auth::addressing::RouteError>>()
        .map_err(|e| error("RouteError", "invalid_route", e))?;
    select_route(&routes, url)
        .map(|result| result.map(|target| target.unwrap_or_default().to_owned()))
        .map_err(|e| error("RouteError", "invalid_route", e))
}

/// Validate the same first-access placement hint in every runtime binding.
#[wasm_bindgen(js_name = actorLocationHint)]
pub fn actor_location_hint(value: &str) -> Result<String, JsValue> {
    acyclic_auth::addressing::validate_location_hint(value).map_err(identity_error)?;
    Ok(value.to_owned())
}
