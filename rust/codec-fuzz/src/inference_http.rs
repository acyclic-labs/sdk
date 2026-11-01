//! Exercise the actual route-specific HTTP decoder and response writer.
use acyclic_inference::{MAXIMUM_MESSAGE_BYTES, http_codec};
use prost::Message;
use prost_reflect::DynamicMessage;
use std::sync::OnceLock;

fn routes() -> &'static [http_codec::HttpRoute] {
    static ROUTES: OnceLock<Vec<http_codec::HttpRoute>> = OnceLock::new();
    ROUTES.get_or_init(|| {
        let routes = http_codec::routes().expect("canonical HTTP route descriptor");
        // The descriptor grammar cannot declare a NUL-containing route.
        assert!(http_codec::decode_http_request("\0", b"{}").is_err());
        assert!(http_codec::encode_http_response("\0", &[]).is_err());
        routes
    })
}

fn json(bytes: &[u8]) -> serde_json::Value {
    serde_json::from_slice(bytes).expect("production JSON writer output")
}

/// The first eight bytes select the actual route and request/response direction.
pub fn check(bytes: &[u8]) {
    let Some((selector, payload)) = bytes.split_first_chunk::<8>() else {
        return;
    };
    let selector = u64::from_le_bytes(*selector);
    let routes = routes();
    let route = &routes[((selector / 2) % routes.len() as u64) as usize];
    if selector & 1 == 0 {
        if let Ok(wire) = http_codec::decode_http_request(&route.path, payload) {
            assert!(wire.len() <= MAXIMUM_MESSAGE_BYTES);
            let canonical = acyclic_protobuf_json::encode(route.method.input(), &wire)
                .expect("actual HTTP request wire decodes");
            let repeated = http_codec::decode_http_request(&route.path, &canonical)
                .expect("canonical request retains its admitted wire frame");
            assert_eq!(
                json(&acyclic_protobuf_json::encode(route.method.input(), &repeated).unwrap()),
                json(&canonical)
            );
            super::semantic::check(route.method.input().full_name(), &wire);
            let mut trailing = payload.to_vec();
            trailing.extend_from_slice(b" false");
            assert!(http_codec::decode_http_request(&route.path, &trailing).is_err());
        }
    } else if let Ok(encoded) = http_codec::encode_http_response(&route.path, payload) {
        assert!(payload.len() <= MAXIMUM_MESSAGE_BYTES);
        let wire = acyclic_protobuf_json::decode(route.method.output(), &encoded, encoded.len())
            .expect("actual HTTP response JSON decodes")
            .encode_to_vec();
        if wire.len() <= MAXIMUM_MESSAGE_BYTES {
            let repeated = http_codec::encode_http_response(&route.path, &wire)
                .expect("canonical response fits both transport domains");
            assert_eq!(json(&repeated), json(&encoded));
        }
        super::semantic::check(route.method.output().full_name(), payload);
    }
}

/// Capture real descriptor-driven SDK writer output for every route/direction.
pub fn seeds() -> Result<Vec<(String, Vec<u8>)>, Box<dyn std::error::Error>> {
    let mut outputs = Vec::new();
    for (index, route) in routes().iter().enumerate() {
        let input = DynamicMessage::new(route.method.input()).encode_to_vec();
        let request = acyclic_protobuf_json::encode(route.method.input(), &input)
            .map_err(|error| format!("HTTP request producer failed for {}: {error:?}; recovery: cargo run --example capture", route.path))?;
        let output = DynamicMessage::new(route.method.output()).encode_to_vec();
        let response = http_codec::encode_http_response(&route.path, &output)?;
        // Both directions are positively readmitted before publication.
        http_codec::decode_http_request(&route.path, &request)?;
        acyclic_protobuf_json::decode(route.method.output(), &response, response.len())
            .map_err(|error| format!("HTTP response producer failed for {}: {error:?}; recovery: cargo run --example capture", route.path))?;
        let prefix = u64::try_from(index)?
            .checked_mul(2)
            .ok_or("HTTP route selector overflow")?;
        for (direction, selector, body) in [
            ("request", prefix, request),
            ("response", prefix + 1, output),
        ] {
            let mut bytes = selector.to_le_bytes().to_vec();
            bytes.extend_from_slice(&body);
            outputs.push((
                format!("{}-{direction}", route.path.replace('/', "-")),
                bytes,
            ));
        }
    }
    Ok(outputs)
}

/// Reuse populated, positively admitted service writer outputs at their routes.
pub fn semantic_seeds(
    name: &str,
    variant: &str,
    wire: &[u8],
) -> Result<Vec<(String, Vec<u8>)>, Box<dyn std::error::Error>> {
    let mut outputs = Vec::new();
    for (index, route) in routes().iter().enumerate() {
        for (response, descriptor) in [(false, route.method.input()), (true, route.method.output())]
        {
            if descriptor.full_name() != name {
                continue;
            }
            let payload = if response {
                http_codec::encode_http_response(&route.path, wire)?;
                wire.to_vec()
            } else {
                let json = acyclic_protobuf_json::encode(descriptor, wire)
                    .map_err(|error| format!("HTTP semantic producer failed for {}: {error:?}; recovery: cargo run --example capture", route.path))?;
                http_codec::decode_http_request(&route.path, &json)?;
                json
            };
            let selector = u64::try_from(index)?
                .checked_mul(2)
                .ok_or("HTTP route selector overflow")?
                + u64::from(response);
            let mut bytes = selector.to_le_bytes().to_vec();
            bytes.extend_from_slice(&payload);
            let direction = if response { "response" } else { "request" };
            outputs.push((
                format!("{}-{direction}-{variant}", route.path.replace('/', "-")),
                bytes,
            ));
        }
    }
    Ok(outputs)
}
