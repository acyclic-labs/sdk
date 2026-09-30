//! Descriptor-derived protobuf JSON boundary for native HTTP adapters.
//!
//! Enable `http-codec` independently of `host`. This module handles bounded
//! serialization, not service authorization, semantic admission or caller-bound
//! response validation. Adapters must apply those checks to the generated wire
//! messages. For `runs/watch`, encode each event separately and frame it as NDJSON.

use std::{io::Write, sync::OnceLock};

use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, MethodDescriptor, Value};

use crate::{MAXIMUM_HTTP_JSON_BYTES, MAXIMUM_MESSAGE_BYTES};

/// One canonical POST route, relative to `/v1/inference/`.
#[derive(Clone, Debug)]
pub struct HttpRoute {
    /// Path declared by the protobuf method option.
    pub path: String,
    /// Generated service and method, including input/output descriptors.
    pub method: MethodDescriptor,
}

fn pool() -> Result<&'static DescriptorPool, &'static str> {
    static POOL: OnceLock<Option<DescriptorPool>> = OnceLock::new();
    POOL.get_or_init(|| {
        DescriptorPool::decode(include_bytes!("../inference_descriptor.bin").as_slice()).ok()
    })
    .as_ref()
    .ok_or("inference descriptor is unavailable")
}

/// Derive the complete public route inventory from the canonical descriptor.
///
/// # Errors
/// Rejects missing, duplicate or malformed route options and unsupported streams.
pub fn routes() -> Result<Vec<HttpRoute>, &'static str> {
    let pool = pool()?;
    let option = pool
        .get_extension_by_name("acyclic.validation.v1.http_path")
        .ok_or("inference HTTP option is unavailable")?;
    let mut paths = std::collections::BTreeSet::new();
    let mut routes = Vec::new();
    for service in pool.services() {
        if service.package_name() != "inference.customer.v1" {
            continue;
        }
        for method in service.methods() {
            let options = method.options();
            if !options.has_extension(&option) || method.is_client_streaming() {
                return Err("inference HTTP method option is invalid");
            }
            let Value::String(path) = options.get_extension(&option).into_owned() else {
                return Err("inference HTTP path is invalid");
            };
            if path.is_empty()
                || path.split('/').any(|part| {
                    part.is_empty()
                        || !part
                            .bytes()
                            .all(|byte| byte.is_ascii_lowercase() || byte == b'-')
                })
                || !paths.insert(path.clone())
            {
                return Err("inference HTTP path is invalid or duplicated");
            }
            routes.push(HttpRoute { path, method });
        }
    }
    if routes.is_empty() {
        return Err("inference HTTP routes are absent");
    }
    Ok(routes)
}

fn method(path: &str) -> Result<MethodDescriptor, &'static str> {
    routes()?
        .into_iter()
        .find(|route| route.path == path)
        .map(|route| route.method)
        .ok_or("unknown inference HTTP route")
}

/// Decode a bounded protobuf JSON request into the route's generated wire type.
///
/// # Errors
/// Rejects unknown routes/fields, malformed or trailing JSON and transport limits.
pub fn decode_http_request(path: &str, json: &[u8]) -> Result<Vec<u8>, &'static str> {
    if json.len() > MAXIMUM_HTTP_JSON_BYTES {
        return Err("inference HTTP request exceeds JSON ceiling");
    }
    let mut deserializer = serde_json::Deserializer::from_slice(json);
    let message = DynamicMessage::deserialize(method(path)?.input(), &mut deserializer)
        .map_err(|_| "malformed inference protobuf JSON request")?;
    deserializer
        .end()
        .map_err(|_| "trailing inference HTTP request data")?;
    if message.encoded_len() > MAXIMUM_MESSAGE_BYTES {
        return Err("inference HTTP request exceeds wire ceiling");
    }
    Ok(message.encode_to_vec())
}

struct BoundedJson(Vec<u8>);

impl Write for BoundedJson {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > MAXIMUM_HTTP_JSON_BYTES.saturating_sub(self.0.len()) {
            return Err(std::io::Error::other(
                "inference HTTP response exceeds JSON ceiling",
            ));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Encode a bounded generated response or one server-streaming event as JSON.
///
/// Protobuf's JSON mapping preserves uint64 values as decimal strings, bytes as
/// base64 and enum names. The returned bytes contain no NDJSON line terminator.
///
/// # Errors
/// Rejects unknown routes, malformed protobuf and wire/JSON transport limits.
pub fn encode_http_response(path: &str, bytes: &[u8]) -> Result<Vec<u8>, &'static str> {
    if bytes.len() > MAXIMUM_MESSAGE_BYTES {
        return Err("inference HTTP response exceeds wire ceiling");
    }
    let message = DynamicMessage::decode(method(path)?.output(), bytes)
        .map_err(|_| "malformed inference protobuf response")?;
    let mut output = BoundedJson(Vec::new());
    serde_json::to_writer(&mut output, &message)
        .map_err(|_| "inference HTTP response serialization failed or exceeds JSON ceiling")?;
    Ok(output.0)
}
