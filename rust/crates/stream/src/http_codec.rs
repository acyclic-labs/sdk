//! SDK-owned hosted Stream request decoding and client request projection.
type Result<T = ()> = std::result::Result<T, &'static str>;
mod server;
pub use server::{HTTP_ROUTES, decode};

#[cfg(any(all(feature = "http", not(target_arch = "wasm32")), all(feature = "wasm", target_arch = "wasm32")))]
pub(crate) use client::encode;

#[cfg(any(test, all(feature = "http", not(target_arch = "wasm32")), all(feature = "wasm", target_arch = "wasm32")))]
mod client {
use super::Result;
use crate::http_response::{json_bytes, json_object, json_string, json_u64};
use crate::{MAX_COMMAND_BYTES, TOKEN_OPERATIONS, memory, wire, wire_codec};
use prost::Message;
use serde_json::Value;
fn optional_key_json(value: Option<&crate::IdempotencyKey>) -> Option<Value> {
    value.map(|value| json_bytes(value.as_bytes()))
}

fn options_json(
    sequence_name: &str,
    sequence: Option<u64>,
    key: Option<&crate::IdempotencyKey>,
) -> Option<Value> {
    let mut entries = Vec::new();
    if let Some(sequence) = sequence {
        entries.push((sequence_name, json_u64(sequence)));
    }
    if let Some(key) = optional_key_json(key) {
        entries.push(("idempotencyKey", key));
    }
    (!entries.is_empty()).then(|| json_object(entries))
}

fn valid_token_operation(value: &str) -> bool {
    TOKEN_OPERATIONS.contains(&value)
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
                .any(|operation| !valid_token_operation(operation))
        {
            return Err("invalid_argument");
        }
    }
    Ok(())
}

fn request_json_append(input: &[u8]) -> Result<Value> {
    let request = wire::AppendRequest::decode(input).map_err(|_| "invalid_argument")?;
    let request = wire_codec::append_from_wire(request).map_err(|error| error.code())?;
    memory::validate_records(&request.records).map_err(|error| error.code())?;
    memory::validate_append_size(&request).map_err(|error| error.code())?;
    let values = request
        .records
        .iter()
        .map(|value| json_bytes(value))
        .collect();
    let mut entries = vec![
        ("path", json_string(request.path.to_string())),
        ("values", Value::Array(values)),
    ];
    if let Some(options) = options_json("ifTail", request.if_tail, request.idempotency_key.as_ref())
    {
        entries.push(("options", options));
    }
    Ok(json_object(entries))
}

fn request_json_fork(input: &[u8]) -> Result<Value> {
    let request = wire::ForkRequest::decode(input).map_err(|_| "invalid_argument")?;
    let request = wire_codec::fork_from_wire(request).map_err(|error| error.code())?;
    if request.source == request.destination {
        return Err("invalid_argument");
    }
    memory::validate_fork_size(&request).map_err(|error| error.code())?;
    let mut entries = vec![
        ("source", json_string(request.source.to_string())),
        ("destination", json_string(request.destination.to_string())),
    ];
    if let Some(options) = options_json("atTail", request.at_tail, request.idempotency_key.as_ref())
    {
        entries.push(("options", options));
    }
    Ok(json_object(entries))
}

fn request_json_commit(input: &[u8]) -> Result<Value> {
    let request = wire::CommitRequest::decode(input).map_err(|_| "invalid_argument")?;
    let deadline = request.deadline_unix_millis;
    let mut request = wire_codec::commit_from_wire(request).map_err(|error| error.code())?;
    memory::normalize_commit(&mut request).map_err(|error| error.code())?;
    memory::validate_commit_shape(&request).map_err(|error| error.code())?;
    let conditions = request
        .conditions
        .iter()
        .map(|condition| match condition {
            crate::CommitCondition::Tail { path, expected } => json_object(vec![
                ("path", json_string(path.to_string())),
                ("ifTail", json_u64(*expected)),
            ]),
            crate::CommitCondition::Absent { path } => json_object(vec![
                ("path", json_string(path.to_string())),
                ("ifAbsent", Value::Bool(true)),
            ]),
        })
        .collect();
    let mutations = request
        .mutations
        .iter()
        .map(|mutation| match mutation {
            crate::CommitMutation::Append { path, records } => json_object(vec![(
                "append",
                json_object(vec![
                    ("path", json_string(path.to_string())),
                    (
                        "values",
                        Value::Array(records.iter().map(|value| json_bytes(value)).collect()),
                    ),
                ]),
            )]),
            crate::CommitMutation::Fork {
                source,
                destination,
                at_tail,
                records,
            } => json_object(vec![(
                "fork",
                json_object(vec![
                    ("source", json_string(source.to_string())),
                    ("destination", json_string(destination.to_string())),
                    ("atTail", json_u64(*at_tail)),
                    (
                        "values",
                        Value::Array(records.iter().map(|record| json_bytes(record)).collect()),
                    ),
                ]),
            )]),
        })
        .collect();
    let mut options = vec![(
        "idempotencyKey",
        json_bytes(request.idempotency_key.as_bytes()),
    )];
    if let Some(deadline) = deadline {
        options.push(("deadlineUnixMillis", json_u64(deadline)));
    }
    Ok(json_object(vec![
        (
            "request",
            json_object(vec![
                ("conditions", Value::Array(conditions)),
                ("mutations", Value::Array(mutations)),
            ]),
        ),
        ("options", json_object(options)),
    ]))
}

fn request_json_tokens_create(input: &[u8]) -> Result<Value> {
    let request = wire::CreateTokenRequest::decode(input).map_err(|_| "invalid_argument")?;
    validate_token_request(&request)?;
    let allow = request
        .allow
        .iter()
        .map(|grant| {
            let mut entries = vec![(
                "path",
                json_string(
                    wire_codec::path(grant.path.clone())
                        .map_err(|error| error.code())?
                        .to_string(),
                ),
            )];
            if let Some(subtree) = grant.subtree {
                entries.push(("subtree", Value::Bool(subtree)));
            }
            entries.push((
                "operations",
                Value::Array(
                    grant
                        .operations
                        .iter()
                        .map(|operation| json_string(operation.clone()))
                        .collect(),
                ),
            ));
            Ok(json_object(entries))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(json_object(vec![
        ("expiresIn", json_string(request.expires_in)),
        ("allow", Value::Array(allow)),
    ]))
}

fn request_json(route: &str, input: &[u8]) -> Result<Value> {
    if input.len() > MAX_COMMAND_BYTES {
        return Err("limit_exceeded");
    }
    match route {
        "idempotency/inspect" => {
            let request =
                wire::InspectIdempotencyRequest::decode(input).map_err(|_| "invalid_argument")?;
            let key = crate::IdempotencyKey::new(request.idempotency_key)
                .map_err(|_| "invalid_argument")?;
            Ok(json_object(vec![(
                "idempotencyKey",
                json_bytes(key.as_bytes()),
            )]))
        }
        "tail" => {
            let request = wire::TailRequest::decode(input).map_err(|_| "invalid_argument")?;
            let path = wire_codec::path(request.path).map_err(|_| "invalid_path")?;
            Ok(json_object(vec![("path", json_string(path.to_string()))]))
        }
        "append" => request_json_append(input),
        "fork" => request_json_fork(input),
        "read" => {
            let request = wire::ReadRequest::decode(input).map_err(|_| "invalid_argument")?;
            let request = wire_codec::read_from_wire(request).map_err(|error| error.code())?;
            memory::validate_limit(request.limit).map_err(|error| error.code())?;
            Ok(json_object(vec![
                ("path", json_string(request.path.to_string())),
                ("from", json_u64(request.from)),
                ("limit", Value::from(request.limit)),
            ]))
        }
        "follow" => {
            let request = wire::FollowRequest::decode(input).map_err(|_| "invalid_argument")?;
            let path = wire_codec::path(request.path).map_err(|_| "invalid_path")?;
            Ok(json_object(vec![
                ("path", json_string(path.to_string())),
                ("from", json_u64(request.from)),
            ]))
        }
        "children" => {
            let request = wire::ChildrenRequest::decode(input).map_err(|_| "invalid_argument")?;
            let request = wire_codec::children_from_wire(request).map_err(|error| error.code())?;
            memory::validate_limit(request.limit).map_err(|error| error.code())?;
            let mut entries = vec![("limit", Value::from(request.limit))];
            if let Some(parent) = request.parent {
                entries.insert(0, ("parent", json_string(parent.to_string())));
            }
            Ok(json_object(entries))
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
            let mut entries = vec![("limit", Value::from(request.limit))];
            if let Some(parent) = request.parent {
                entries.insert(0, ("parent", json_string(parent.to_string())));
            }
            if let Some(after) = request.after {
                entries.insert(0, ("after", json_string(after.to_string())));
            }
            if let Some(version) = request.hierarchy_version {
                entries.insert(0, ("hierarchyVersion", json_bytes(version.as_bytes())));
            }
            Ok(json_object(entries))
        }
        "commit" => request_json_commit(input),
        "commits/read" => {
            let request = wire::ReadCommitRequest::decode(input).map_err(|_| "invalid_argument")?;
            let commit_id =
                <[u8; 32]>::try_from(request.commit_id.as_ref()).map_err(|_| "invalid_argument")?;
            Ok(json_object(vec![("commitId", json_bytes(&commit_id))]))
        }
        "tokens/create" => request_json_tokens_create(input),
        _ => Err("invalid_argument"),
    }
}

pub(crate) fn encode(route: &str, input: &[u8]) -> Result<String> {
    serde_json::to_string(&request_json(route, input)?).map_err(|_| "could not encode request")
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
                records: vec![bytes::Bytes::from_static(b"record")],
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
}
