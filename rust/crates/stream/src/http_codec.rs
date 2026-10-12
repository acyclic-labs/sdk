//! Shared Rust-owned hosted Stream request projection for native and browser clients.
use crate::http_response::{json_bytes, json_object, json_string, json_u64};
use crate::{MAX_COMMAND_BYTES, TOKEN_OPERATIONS, memory, wire, wire_codec};
use prost::Message;
use serde_json::Value;
type Result<T = ()> = std::result::Result<T, &'static str>;

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

pub(crate) fn validate_append_size(request: &crate::AppendRequest) -> std::result::Result<(), crate::StreamError> {
    (wire_codec::CanonicalWire::Append(request)
        .encoded_len(None, None)
        .expect("ordinary sizing has no fallible meter")
        <= MAX_COMMAND_BYTES)
        .then_some(())
        .ok_or(crate::StreamError::LimitExceeded)
}

fn request_json_append(input: &[u8]) -> Result<Value> {
    let request = wire::AppendRequest::decode(input).map_err(|_| "invalid_argument")?;
    let request = wire_codec::append_from_wire(request).map_err(|error| error.code())?;
    memory::provider_result(memory::validate_records(&request.records, None))
        .map_err(|error| error.code())?;
    validate_append_size(&request).map_err(|error| error.code())?;
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
    memory::provider_result(memory::normalize_commit(&mut request, None))
        .map_err(|error| error.code())?;
    memory::provider_result(memory::validate_commit_shape(&request, None))
        .map_err(|error| error.code())?;
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
        "delivery/acknowledge" => {
            let request =
                wire::AcknowledgeDeliveryRequest::decode(input).map_err(|_| "invalid_argument")?;
            if request.delivery_token.len() != 32 {
                return Err("invalid_argument");
            }
            Ok(json_object(vec![(
                "deliveryToken",
                json_bytes(&request.delivery_token),
            )]))
        }
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
    fn command_size_uses_exact_protobuf_encoding() -> std::result::Result<(), crate::StreamError> {
        let wire_records = vec![bytes::Bytes::from(vec![0_u8; crate::MAX_RECORD_BYTES]); 16];
        let records: Vec<_> = wire_records.iter().cloned().map(crate::OwnedBytes::from).collect();
        let mut low = 1_usize;
        let mut high = crate::MAX_PATH_BYTES;
        while low < high {
            let middle = low + (high - low).div_ceil(2);
            let path = crate::StreamPath::new("x".repeat(middle))?;
            let wire = crate::wire::AppendRequest {
                path: path.to_string(),
                records: wire_records.clone(),
                if_tail: None,
                idempotency_key: None,
            };
            if wire.encoded_len() <= MAX_COMMAND_BYTES {
                low = middle;
            } else {
                high = middle - 1;
            }
        }
        let request = crate::AppendRequest {
            path: crate::StreamPath::new("x".repeat(low))?,
            records,
            if_tail: None,
            idempotency_key: None,
        };
        assert!(validate_append_size(&request).is_ok());
        let next = crate::StreamPath::new("x".repeat(low + 1))?;
        let next_wire = crate::wire::AppendRequest {
            path: next.to_string(),
            records: wire_records,
            if_tail: None,
            idempotency_key: None,
        };
        assert!(next_wire.encoded_len() > MAX_COMMAND_BYTES);
        Ok(())
    }

    #[test]
    fn delivery_ack_projection_requires_exact_token_width()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let route = "delivery/acknowledge";
        let request = wire::AcknowledgeDeliveryRequest {
            delivery_token: bytes::Bytes::from_static(&[0; 32]),
        };
        let projected: Value = serde_json::from_str(&encode(route, &request.encode_to_vec())?)?;
        assert_eq!(
            projected,
            serde_json::json!({"deliveryToken": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="})
        );
        for length in [0, 1, 31, 33, 64] {
            let invalid = wire::AcknowledgeDeliveryRequest {
                delivery_token: bytes::Bytes::from(vec![0; length]),
            };
            assert_eq!(
                encode(route, &invalid.encode_to_vec()),
                Err("invalid_argument")
            );
        }
        assert_eq!(encode(route, &[0xff]), Err("invalid_argument"));
        Ok(())
    }

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
