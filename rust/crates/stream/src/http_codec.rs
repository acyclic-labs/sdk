//! Hosted Stream requests shared by native and browser clients.
use crate::{MAX_COMMAND_BYTES, TOKEN_OPERATIONS, memory, wire, wire_codec};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use prost::Message;
use serde_json::{Value, json};
type Result<T = ()> = std::result::Result<T, &'static str>;

fn options(
    name: &str,
    sequence: Option<u64>,
    key: Option<&crate::IdempotencyKey>,
) -> Option<Value> {
    if sequence.is_none() && key.is_none() {
        return None;
    }
    let mut value = serde_json::Map::from_iter([]);
    if let Some(sequence) = sequence {
        value.insert(name.into(), json!(sequence.to_string()));
    }
    if let Some(key) = key {
        value.insert(
            "idempotencyKey".into(),
            json!(STANDARD.encode(key.as_bytes())),
        );
    }
    Some(Value::Object(value))
}

fn valid_token_expiry(value: &str) -> bool {
    let bytes = value.as_bytes();
    let Some((unit, digits)) = bytes.split_last() else {
        return false;
    };
    !digits.is_empty()
        && digits.iter().all(|byte| byte.is_ascii_digit())
        && matches!(*unit, b's' | b'm' | b'h' | b'd')
}

fn validate_token_request(request: &wire::CreateTokenRequest) -> Result {
    if !valid_token_expiry(&request.expires_in) || request.allow.is_empty() {
        return Err("invalid_argument");
    }
    for grant in &request.allow {
        wire_codec::path(grant.path.clone()).map_err(|error| error.code())?;
        if grant.operations.is_empty()
            || grant
                .operations
                .iter()
                .any(|operation| !TOKEN_OPERATIONS.contains(&operation.as_str()))
        {
            return Err("invalid_argument");
        }
    }
    Ok(())
}

fn request_json_append(input: &[u8]) -> Result<Value> {
    let request = wire::AppendRequest::decode(input).map_err(|_| "invalid_argument")?;
    let request = wire_codec::append_from_wire(request).map_err(|error| error.code())?;
    append_json(&request)
}

fn append_json(request: &crate::AppendRequest) -> Result<Value> {
    crate::request::validate_records(&request.records).map_err(|error| error.code())?;
    crate::request::validate_append_size(request).map_err(|error| error.code())?;
    let values: Vec<_> = request.records.iter().map(append_record_json).collect();
    let mut value = serde_json::Map::from_iter([
        ("path".into(), json!(request.path.to_string())),
        ("records".into(), json!(values)),
    ]);
    if let Some(options) = options("ifTail", request.if_tail, request.idempotency_key.as_ref()) {
        value.insert("options".into(), options);
    }
    Ok(Value::Object(value))
}

fn append_record_json(record: &crate::AppendRecord) -> Value {
    json!({
        "value": STANDARD.encode(&record.value),
        "keys": record.headers.keys().iter().map(|key| STANDARD.encode(key)).collect::<Vec<_>>(),
        "attributes": record.headers.attributes().iter().map(|attribute| json!({
            "name": attribute.name,
            "value": STANDARD.encode(&attribute.value),
        })).collect::<Vec<_>>(),
    })
}

fn request_json_fork(input: &[u8]) -> Result<Value> {
    let request = wire::ForkRequest::decode(input).map_err(|_| "invalid_argument")?;
    let request = wire_codec::fork_from_wire(request).map_err(|error| error.code())?;
    if request.source == request.destination {
        return Err("invalid_argument");
    }
    crate::request::validate_fork_size(&request).map_err(|error| error.code())?;
    let mut value = serde_json::Map::from_iter([
        ("source".into(), json!(request.source.to_string())),
        ("destination".into(), json!(request.destination.to_string())),
    ]);
    if let Some(options) = options("atTail", request.at_tail, request.idempotency_key.as_ref()) {
        value.insert("options".into(), options);
    }
    Ok(Value::Object(value))
}

fn request_json_commit(input: &[u8]) -> Result<Value> {
    let request = wire::CommitRequest::decode(input).map_err(|_| "invalid_argument")?;
    let deadline = request.deadline_unix_millis;
    let mut request = wire_codec::commit_from_wire(request).map_err(|error| error.code())?;
    crate::request::normalize_commit(&mut request).map_err(|error| error.code())?;
    crate::request::validate_commit_shape(&request).map_err(|error| error.code())?;
    if request
        .conditions
        .iter()
        .any(|condition| matches!(condition, crate::CommitCondition::Head { .. }))
    {
        return Err("unsupported");
    }
    let conditions: Vec<_> = request
        .conditions
        .iter()
        .map(|condition| match condition {
            crate::CommitCondition::Head { .. } => {
                unreachable!("head conditions require a negotiated hosted capability")
            }
            crate::CommitCondition::Tail { path, expected } => json!({
                "path": path.to_string(),
                "ifTail": expected.to_string(),
            }),
            crate::CommitCondition::Absent { path } => json!({
                "path": path.to_string(),
                "ifAbsent": true,
            }),
        })
        .collect();
    let mutations: Vec<_> = request
        .mutations
        .iter()
        .map(|mutation| match mutation {
            crate::CommitMutation::Append { path, records } => json!({
                "append": {
                    "path": path.to_string(),
                    "records": records.iter().map(append_record_json).collect::<Vec<_>>(),
                },
            }),
            crate::CommitMutation::Fork {
                source,
                destination,
                at_tail,
                records,
            } => json!({
                "fork": {
                    "source": source.to_string(),
                    "destination": destination.to_string(),
                    "atTail": at_tail.to_string(),
                    "records": records.iter().map(append_record_json).collect::<Vec<_>>(),
                },
            }),
        })
        .collect();
    let mut commit_options = serde_json::Map::from_iter([(
        "idempotencyKey".into(),
        json!(STANDARD.encode(request.idempotency_key.as_bytes())),
    )]);
    if let Some(deadline) = deadline {
        commit_options.insert("deadlineUnixMillis".into(), json!(deadline.to_string()));
    }
    let value = json!({
        "request": {
            "conditions": conditions,
            "mutations": mutations,
        },
        "options": commit_options,
    });
    Ok(value)
}

fn request_json_tokens_create(input: &[u8]) -> Result<Value> {
    let request = wire::CreateTokenRequest::decode(input).map_err(|_| "invalid_argument")?;
    validate_token_request(&request)?;
    let allow = request
        .allow
        .iter()
        .map(|grant| {
            let mut value = serde_json::Map::from_iter([
                (
                    "path".into(),
                    json!(
                        wire_codec::path(grant.path.clone())
                            .map_err(|error| error.code())?
                            .to_string()
                    ),
                ),
                ("operations".into(), json!(grant.operations)),
            ]);
            if let Some(subtree) = grant.subtree {
                value.insert("subtree".into(), json!(subtree));
            }
            Ok(Value::Object(value))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(json!({
        "expiresIn": request.expires_in,
        "allow": allow,
    }))
}

fn request_json(route: &str, input: &[u8]) -> Result<Value> {
    if input.len() > MAX_COMMAND_BYTES {
        return Err("limit_exceeded");
    }
    match route {
        "delivery/acknowledge" => {
            let request =
                wire::AcknowledgeDeliveryRequest::decode(input).map_err(|_| "invalid_argument")?;
            if request.delivery_token.len() != 32 {
                return Err("invalid_argument");
            }
            Ok(json!({ "deliveryToken": STANDARD.encode(request.delivery_token) }))
        }
        "idempotency/inspect" => {
            let request =
                wire::InspectIdempotencyRequest::decode(input).map_err(|_| "invalid_argument")?;
            let key = crate::IdempotencyKey::new(request.idempotency_key)
                .map_err(|_| "invalid_argument")?;
            Ok(json!({ "idempotencyKey": STANDARD.encode(key.as_bytes()) }))
        }
        "tail" => {
            let request = wire::TailRequest::decode(input).map_err(|_| "invalid_argument")?;
            let path = wire_codec::path(request.path).map_err(|_| "invalid_path")?;
            Ok(json!({ "path": path.to_string() }))
        }
        "append" => request_json_append(input),
        "fork" => request_json_fork(input),
        "read" => {
            let request = wire::ReadRequest::decode(input).map_err(|_| "invalid_argument")?;
            let request = wire_codec::read_from_wire(request).map_err(|error| error.code())?;
            memory::validate_limit(request.limit).map_err(|error| error.code())?;
            Ok(json!({
                "path": request.path.to_string(),
                "from": request.from.to_string(),
                "limit": request.limit,
            }))
        }
        "children" => {
            let request = wire::ChildrenRequest::decode(input).map_err(|_| "invalid_argument")?;
            let request = wire_codec::children_from_wire(request).map_err(|error| error.code())?;
            memory::validate_limit(request.limit).map_err(|error| error.code())?;
            let mut value = serde_json::Map::from_iter([("limit".into(), json!(request.limit))]);
            if let Some(parent) = request.parent {
                value.insert("parent".into(), json!(parent.to_string()));
            }
            Ok(Value::Object(value))
        }
        "children/page" => {
            let request =
                wire::ChildrenPageRequest::decode(input).map_err(|_| "invalid_argument")?;
            let request =
                wire_codec::children_page_from_wire(request).map_err(|error| error.code())?;
            if request.after.is_some() && request.hierarchy_version.is_none() {
                return Err("invalid_argument");
            }
            memory::validate_limit(request.limit).map_err(|error| error.code())?;
            let mut value = serde_json::Map::from_iter([("limit".into(), json!(request.limit))]);
            if let Some(parent) = request.parent {
                value.insert("parent".into(), json!(parent.to_string()));
            }
            if let Some(after) = request.after {
                value.insert("after".into(), json!(after.to_string()));
            }
            if let Some(version) = request.hierarchy_version {
                value.insert(
                    "hierarchyVersion".into(),
                    json!(STANDARD.encode(version.as_bytes())),
                );
            }
            Ok(Value::Object(value))
        }
        "commit" => request_json_commit(input),
        "commits/read" => {
            let request = wire::ReadCommitRequest::decode(input).map_err(|_| "invalid_argument")?;
            let id =
                <[u8; 32]>::try_from(request.commit_id.as_ref()).map_err(|_| "invalid_argument")?;
            Ok(json!({ "commitId": STANDARD.encode(id) }))
        }
        "tokens/create" => request_json_tokens_create(input),
        _ => Err("invalid_argument"),
    }
}

pub(crate) fn encode(route: &str, input: &[u8]) -> Result<String> {
    serde_json::to_string(&request_json(route, input)?).map_err(|_| "could not encode request")
}

#[cfg(all(feature = "http", not(target_arch = "wasm32")))]
pub(crate) fn encode_append(request: &crate::AppendRequest) -> Result<String> {
    serde_json::to_string(&append_json(request)?).map_err(|_| "could not encode request")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commit_projection_preserves_deadline_without_losing_uint64_precision()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let request = crate::CommitRequest {
            conditions: vec![crate::CommitCondition::Absent {
                path: crate::StreamPath::new("deadline/events")?,
            }],
            mutations: vec![crate::CommitMutation::Append {
                path: crate::StreamPath::new("deadline/events")?,
                records: vec![crate::OwnedBytes::from_static(b"record")],
            }],
            idempotency_key: crate::IdempotencyKey::new(bytes::Bytes::from_static(b"identity"))?,
        };
        let mut wire = wire_codec::commit_to_wire(&request);
        wire.deadline_unix_millis = Some(9_007_199_254_740_993);
        let projected: Value = serde_json::from_str(&encode("commit", &wire.encode_to_vec())?)?;
        assert_eq!(
            projected
                .pointer("/options/deadlineUnixMillis")
                .and_then(Value::as_str),
            Some("9007199254740993")
        );
        wire.deadline_unix_millis = None;
        let projected: Value = serde_json::from_str(&encode("commit", &wire.encode_to_vec())?)?;
        assert!(projected.pointer("/options/deadlineUnixMillis").is_none());
        Ok(())
    }
}
