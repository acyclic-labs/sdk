//! Shared Rust-owned hosted Stream request projection for native and browser clients.
use crate::{MAX_COMMAND_BYTES, StreamError, TOKEN_OPERATIONS, memory, wire, wire_codec};
use prost::Message;
use serde_json::Value;
type Result<T = ()> = std::result::Result<T, &'static str>;

pub(crate) fn error_code_str(error: &StreamError) -> &'static str {
    match error {
        StreamError::InvalidPath => "invalid_path",
        StreamError::InvalidArgument => "invalid_argument",
        StreamError::LimitExceeded => "limit_exceeded",
        StreamError::NotFound => "not_found",
        StreamError::AlreadyExists => "already_exists",
        StreamError::PrefixNotRetained => "prefix_not_retained",
        StreamError::OutOfRange => "out_of_range",
        StreamError::IdempotencyMismatch => "idempotency_mismatch",
        StreamError::Capacity => "capacity",
        StreamError::AccessDenied => "access_denied",
        StreamError::Unavailable => "unavailable",
        StreamError::HierarchyChanged => "hierarchy_changed",
        StreamError::DeadlineElapsed => "deadline_elapsed",
        StreamError::Unsupported => "unsupported",
    }
}
pub(crate) fn json_object(entries: Vec<(&str, Value)>) -> Value {
    Value::Object(
        entries
            .into_iter()
            .map(|(name, value)| (name.to_owned(), value))
            .collect(),
    )
}

pub(crate) fn json_string(value: impl Into<String>) -> Value {
    Value::String(value.into())
}

pub(crate) fn json_u64(value: u64) -> Value {
    json_string(value.to_string())
}

pub(crate) fn json_bytes(value: &[u8]) -> Value {
    json_string(encode_base64(value))
}

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
        wire_codec::path(grant.path.clone()).map_err(|error| error_code_str(&error))?;
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
    let request = wire_codec::append_from_wire(request).map_err(|error| error_code_str(&error))?;
    memory::validate_records(&request.records).map_err(|error| error_code_str(&error))?;
    memory::validate_append_size(&request).map_err(|error| error_code_str(&error))?;
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
    let request = wire_codec::fork_from_wire(request).map_err(|error| error_code_str(&error))?;
    if request.source == request.destination {
        return Err("invalid_argument");
    }
    memory::validate_fork_size(&request).map_err(|error| error_code_str(&error))?;
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
    let mut request =
        wire_codec::commit_from_wire(request).map_err(|error| error_code_str(&error))?;
    memory::normalize_commit(&mut request).map_err(|error| error_code_str(&error))?;
    memory::validate_commit_shape(&request).map_err(|error| error_code_str(&error))?;
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
                        .map_err(|error| error_code_str(&error))?
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
            let request =
                wire_codec::read_from_wire(request).map_err(|error| error_code_str(&error))?;
            memory::validate_limit(request.limit).map_err(|error| error_code_str(&error))?;
            Ok(json_object(vec![
                ("path", json_string(request.path.to_string())),
                ("from", json_u64(request.from)),
                ("limit", Value::from(request.limit)),
            ]))
        }
        "children" => {
            let request = wire::ChildrenRequest::decode(input).map_err(|_| "invalid_argument")?;
            let request =
                wire_codec::children_from_wire(request).map_err(|error| error_code_str(&error))?;
            memory::validate_limit(request.limit).map_err(|error| error_code_str(&error))?;
            let mut entries = vec![("limit", Value::from(request.limit))];
            if let Some(parent) = request.parent {
                entries.insert(0, ("parent", json_string(parent.to_string())));
            }
            Ok(json_object(entries))
        }
        "children/page" => {
            let request =
                wire::ChildrenPageRequest::decode(input).map_err(|_| "invalid_argument")?;
            let request = wire_codec::children_page_from_wire(request)
                .map_err(|error| error_code_str(&error))?;
            if request.after.is_some() && request.hierarchy_version.is_none() {
                return Err("invalid_argument");
            }
            memory::validate_limit(request.limit).map_err(|error| error_code_str(&error))?;
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

fn encode_base64(value: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let table_char = |index: u8| TABLE.get(index as usize).copied().unwrap_or_default() as char;
    let mut output = String::with_capacity(value.len().div_ceil(3) * 4);
    for chunk in value.chunks(3) {
        let Some(&first) = chunk.first() else {
            continue;
        };
        output.push(table_char(first >> 2));
        if chunk.len() == 1 {
            output.push(table_char((first & 0x03) << 4));
            output.push_str("==");
            continue;
        }
        let Some(&second) = chunk.get(1) else {
            continue;
        };
        output.push(table_char(((first & 0x03) << 4) | (second >> 4)));
        if chunk.len() == 2 {
            output.push(table_char((second & 0x0f) << 2));
            output.push('=');
            continue;
        }
        let Some(&third) = chunk.get(2) else {
            continue;
        };
        output.push(table_char(((second & 0x0f) << 2) | (third >> 6)));
        output.push(table_char(third & 0x3f));
    }
    output
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
