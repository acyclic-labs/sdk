//! Hosted Stream requests shared by native and browser clients.
use crate::MAX_COMMAND_BYTES;
#[cfg(any(test, feature = "test-support", target_arch = "wasm32"))]
use crate::wire_codec;
#[cfg(any(test, feature = "test-support", target_arch = "wasm32"))]
use acyclic_stream_wire::token::TokenOperation;
#[cfg(any(test, feature = "test-support", target_arch = "wasm32"))]
use acyclic_stream_wire::wire;
use base64::{Engine as _, engine::general_purpose::STANDARD};
#[cfg(any(test, feature = "test-support", target_arch = "wasm32"))]
use prost::Message;
use serde_json::{Value, json};
type Result<T = ()> = std::result::Result<T, &'static str>;

fn options(name: &str, sequence: Option<u64>, key: &crate::IdempotencyKey) -> Value {
    let mut value = serde_json::Map::from_iter([(
        "idempotencyKey".into(),
        json!(STANDARD.encode(key.as_bytes())),
    )]);
    if let Some(sequence) = sequence {
        value.insert(name.into(), json!(sequence.to_string()));
    }
    Value::Object(value)
}

#[cfg(any(test, feature = "test-support", target_arch = "wasm32"))]
fn valid_token_expiry(value: &str) -> bool {
    let bytes = value.as_bytes();
    let Some((unit, digits)) = bytes.split_last() else {
        return false;
    };
    !digits.is_empty()
        && digits.iter().all(|byte| byte.is_ascii_digit())
        && matches!(*unit, b's' | b'm' | b'h' | b'd')
}

#[cfg(any(test, feature = "test-support", target_arch = "wasm32"))]
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
                .any(|operation| operation.parse::<TokenOperation>().is_err())
        {
            return Err("invalid_argument");
        }
    }
    Ok(())
}

#[cfg(any(test, feature = "test-support", target_arch = "wasm32"))]
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
    value.insert(
        "options".into(),
        options("ifTail", request.if_tail, &request.idempotency_key),
    );
    Ok(Value::Object(value))
}

fn append_record_json(record: &crate::AppendRecord) -> Value {
    json!({
        "value": STANDARD.encode(&record.value),
        "keys": crate::changes::wire_keys(&record.headers).iter().map(|key| STANDARD.encode(key)).collect::<Vec<_>>(),
        "attributes": record.headers.attributes().iter().map(|attribute| json!({
            "name": attribute.name,
            "value": STANDARD.encode(attribute.value),
        })).collect::<Vec<_>>(),
    })
}

#[cfg(any(test, feature = "test-support", target_arch = "wasm32"))]
fn request_json_fork(input: &[u8]) -> Result<Value> {
    let request = wire::ForkRequest::decode(input).map_err(|_| "invalid_argument")?;
    let request = wire_codec::fork_from_wire(request).map_err(|error| error.code())?;
    fork_json(&request)
}

fn fork_json(request: &crate::ForkRequest) -> Result<Value> {
    crate::request::validate_fork(request).map_err(|error| error.code())?;
    Ok(json!({
        "source": request.source.as_str(),
        "destination": request.destination.as_str(),
        "exclude": request.exclude.iter().map(crate::StreamPath::as_str).collect::<Vec<_>>(),
        "options": options("atTail", request.at_tail, &request.idempotency_key),
    }))
}

/// One effect, `{conditions, mutations, soft}`; Head conditions were refused.
fn effect_json(effect: &crate::CommitEffect) -> Value {
    let conditions: Vec<_> = effect
        .conditions
        .iter()
        .map(|condition| match condition {
            crate::CommitCondition::Present { path } => json!({
                "path": path.to_string(),
                "ifPresent": true,
            }),
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
    let mutations: Vec<_> = effect
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
                exclude,
            } => json!({
                "fork": {
                    "source": source.to_string(),
                    "destination": destination.to_string(),
                    "atTail": at_tail.to_string(),
                    "exclude": exclude.iter().map(|path| path.to_string()).collect::<Vec<_>>(),
                    "records": records.iter().map(append_record_json).collect::<Vec<_>>(),
                },
            }),
        })
        .collect();
    json!({ "conditions": conditions, "mutations": mutations, "soft": effect.soft })
}

#[cfg(any(test, feature = "test-support", target_arch = "wasm32"))]
fn request_json_commit(input: &[u8]) -> Result<Value> {
    let request = wire::CommitRequest::decode(input).map_err(|_| "invalid_argument")?;
    if request.include_envelope {
        return Err("unsupported");
    }
    let request = wire_codec::commit_from_wire(request).map_err(|error| error.code())?;
    commit_json(request)
}

fn commit_json(mut request: crate::CommitRequest) -> Result<Value> {
    crate::request::normalize_commit(&mut request).map_err(|error| error.code())?;
    if request
        .conditions()
        .any(|condition| matches!(condition, crate::CommitCondition::Head { .. }))
    {
        return Err("unsupported");
    }
    let effects: Vec<_> = request.effects.iter().map(effect_json).collect();
    let mut commit_options = serde_json::Map::from_iter([(
        "idempotencyKey".into(),
        json!(STANDARD.encode(request.idempotency_key.as_bytes())),
    )]);
    if let Some(deadline) = request.deadline_unix_millis {
        commit_options.insert("deadlineUnixMillis".into(), json!(deadline.to_string()));
    }
    let value = json!({
        "request": { "effects": effects },
        "options": commit_options,
    });
    Ok(value)
}

#[cfg(any(test, feature = "test-support", target_arch = "wasm32"))]
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

#[cfg(any(test, feature = "test-support", target_arch = "wasm32"))]
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
            Ok(identity_json("idempotencyKey", key.as_bytes()))
        }
        "heads" => {
            let wire = wire::HeadsRequest::decode(input).map_err(|_| "invalid_argument")?;
            let request =
                wire_codec::heads_from_wire(wire.clone()).map_err(|error| error.code())?;
            let children = wire
                .children
                .map(|children| request_json("children/page", &children.encode_to_vec()))
                .transpose()?;
            let retained: Vec<_> = request.retained.iter().map(|selected| json!({
                "commitId": STANDARD.encode(selected.commit_id.as_bytes()),
                "path": selected.path.to_string(), "sequence": selected.sequence.to_string(),
            })).collect();
            let mut value = json!({ "retained": retained, "durableThrough": request.durable_through.map(|through| through.to_string()), "paths": request.paths.iter().map(ToString::to_string).collect::<Vec<_>>(), "children": children, "limit": request.limit, "projection": match request.projection { crate::HeadProjection::History => "history", crate::HeadProjection::Watermarks => "watermarks", crate::HeadProjection::Tail => "tail" } });
            if let Some(at) = &request.at {
                let Some(object) = value.as_object_mut() else {
                    return Err("invalid_response");
                };
                object.insert(
                    "at".into(),
                    crate::http_response::revision_json(Some(&wire_codec::revision_to_wire(at))),
                );
            }
            Ok(with_at_least(value, &wire.at_least))
        }
        "head" => {
            let request = wire::HeadRequest::decode(input).map_err(|_| "invalid_argument")?;
            let path = wire_codec::path(request.path).map_err(|_| "invalid_path")?;
            Ok(json!({ "path": path.to_string() }))
        }
        "tail" => {
            let request = wire::TailRequest::decode(input).map_err(|_| "invalid_argument")?;
            wire_codec::cut_from_wire(request.at_least.clone()).map_err(|error| error.code())?;
            let path = wire_codec::path(request.path).map_err(|_| "invalid_path")?;
            Ok(with_at_least(
                json!({ "path": path.to_string() }),
                &request.at_least,
            ))
        }
        "append" => request_json_append(input),
        "fork" => request_json_fork(input),
        "read" => {
            let request = wire::ReadRequest::decode(input).map_err(|_| "invalid_argument")?;
            let at_least = request.at_least.clone();
            let request = wire_codec::read_from_wire(request).map_err(|error| error.code())?;
            crate::validate_limit(request.limit).map_err(|error| error.code())?;
            let mut value = json!({
                "path": request.path.to_string(),
                "from": request.from.to_string(),
                "limit": request.limit,
            });
            if let Some(key) = request.key {
                let mut selector = json!({
                    "key": STANDARD.encode(&key.key),
                    "before": key.before.to_string(),
                    "order": match key.order {
                        crate::ReadOrder::Ascending => "ascending",
                        crate::ReadOrder::LatestFirst => "latest_first",
                    },
                });
                if let Some(end) = key.end {
                    let Some(object) = selector.as_object_mut() else {
                        return Err("invalid_response");
                    };
                    object.insert("end".into(), json!(STANDARD.encode(end)));
                }
                let Some(object) = value.as_object_mut() else {
                    return Err("invalid_response");
                };
                object.insert("key".into(), selector);
            }
            Ok(with_at_least(value, &at_least))
        }
        "children/page" => {
            let request =
                wire::ChildrenPageRequest::decode(input).map_err(|_| "invalid_argument")?;
            let request =
                wire_codec::children_page_from_wire(request).map_err(|error| error.code())?;
            crate::validate_limit(request.limit).map_err(|error| error.code())?;
            let mut value = serde_json::Map::from_iter([("limit".into(), json!(request.limit))]);
            if request.offset != 0 {
                value.insert("offset".into(), json!(request.offset.to_string()));
            }
            if let Some(tail_stream) = request.tail_stream {
                value.insert("tailStream".into(), json!(tail_stream.to_string()));
            }
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
            let id = wire_codec::commit_id(&request.commit_id).map_err(|_| "invalid_argument")?;
            Ok(identity_json("commitId", id.as_bytes()))
        }
        "tokens/create" => request_json_tokens_create(input),
        _ => Err("invalid_argument"),
    }
}

fn identity_json(name: &str, bytes: &[u8]) -> Value {
    json!({ (name): STANDARD.encode(bytes) })
}

#[cfg(not(target_arch = "wasm32"))]
pub fn encode_identity(name: &str, bytes: &[u8]) -> Result<String> {
    serde_json::to_string(&identity_json(name, bytes)).map_err(|_| "could not encode request")
}

#[cfg(any(test, feature = "test-support", target_arch = "wasm32"))]
pub fn encode(route: &str, input: &[u8]) -> Result<String> {
    serde_json::to_string(&request_json(route, input)?).map_err(|_| "could not encode request")
}

#[cfg(not(target_arch = "wasm32"))]
pub fn encode_append(request: &crate::AppendRequest) -> Result<String> {
    serde_json::to_string(&append_json(request)?).map_err(|_| "could not encode request")
}

#[cfg(not(target_arch = "wasm32"))]
pub fn encode_fork(request: &crate::ForkRequest) -> Result<String> {
    crate::request::validate_fork_size(request).map_err(|error| error.code())?;
    serde_json::to_string(&fork_json(request)?).map_err(|_| "could not encode request")
}

#[cfg(not(target_arch = "wasm32"))]
pub fn encode_commit(request: crate::CommitRequest) -> Result<String> {
    if crate::wire_codec::requests::commit_len(&request) > MAX_COMMAND_BYTES {
        return Err("limit_exceeded");
    }
    serde_json::to_string(&commit_json(request)?).map_err(|_| "could not encode request")
}

/// Adds the minimum cut, one entry per authority; the host serves at or above it.
#[cfg(any(test, feature = "test-support", target_arch = "wasm32"))]
fn with_at_least(value: Value, at_least: &[wire::Revision]) -> Value {
    match value {
        Value::Object(mut object) => {
            if !at_least.is_empty() {
                object.insert(
                    "atLeast".into(),
                    at_least
                        .iter()
                        .map(|revision| crate::http_response::revision_json(Some(revision)))
                        .collect(),
                );
            }
            Value::Object(object)
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request::{encode::append, len::append_len};
    use crate::{AppendRequest, OwnedBytes};

    #[test]
    fn retained_cut_and_child_offset_preserve_full_uint64_positions()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let request = crate::HeadsRequest {
            at: Some(crate::Revision {
                scope: crate::RevisionScope::new(crate::OwnedBytes::from_static(b"http-cut")),
                incarnation: 1,
                index: u64::MAX,
            }),
            retained: Default::default(),
            durable_through: None,
            projection: crate::HeadProjection::Tail,
            paths: [crate::StreamPath::new("events")?].into_iter().collect(),
            children: None,
            limit: 1,
            at_least: Default::default(),
        };
        let wire = wire_codec::heads_request_to_wire(request);
        let projected: Value = serde_json::from_str(&encode("heads", &wire.encode_to_vec())?)?;
        assert_eq!(
            projected.pointer("/at/index").and_then(Value::as_str),
            Some("18446744073709551615")
        );
        for offset in [0, u64::MAX] {
            let request = crate::ChildrenPageRequest {
                parent: Some(crate::StreamPath::new("events")?),
                offset,
                after: None,
                hierarchy_version: None,
                limit: 1,
                tail_stream: None,
            };
            let wire = wire_codec::children_page_request_to_wire(request);
            let projected: Value =
                serde_json::from_str(&encode("children/page", &wire.encode_to_vec())?)?;
            assert_eq!(
                projected.get("offset").and_then(Value::as_str),
                (offset != 0).then_some("18446744073709551615")
            );
        }
        Ok(())
    }

    #[test]
    fn commit_projection_preserves_deadline_without_losing_uint64_precision()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let request = crate::CommitRequest {
            deadline_unix_millis: Some(9_007_199_254_740_993),
            effects: vec![crate::CommitEffect::hard(
                vec![crate::CommitCondition::Absent {
                    path: crate::StreamPath::new("deadline/events")?,
                }],
                vec![crate::CommitMutation::Append {
                    path: crate::StreamPath::new("deadline/events")?,
                    records: vec![(crate::OwnedBytes::from_static(b"record")).into()],
                }],
            )],
            idempotency_key: crate::IdempotencyKey::new(bytes::Bytes::from_static(b"identity"))?,
        };
        let mut wire = wire_codec::commit_to_wire(&request);
        assert_eq!(
            wire_codec::commit_from_wire(wire.clone())?.deadline_unix_millis,
            request.deadline_unix_millis
        );
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
    #[test]
    fn request_path_append_encoding_matches_generated_protobuf()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        for tail in [None, Some(0), Some(u64::MAX)] {
            for key in [
                crate::IdempotencyKey::new(bytes::Bytes::from_static(b"k"))?,
                crate::IdempotencyKey::new(bytes::Bytes::from_static(b"request-key"))?,
            ] {
                let request = AppendRequest {
                    path: crate::StreamPath::new("request/events")?,
                    records: vec![
                        OwnedBytes::from_static(b"").into(),
                        OwnedBytes::from(vec![7; 65_536]).into(),
                    ],
                    if_tail: tail,
                    idempotency_key: key,
                };
                let expected = acyclic_stream_wire::wire::AppendRequest {
                    path: request.path.to_string(),
                    records: request
                        .records
                        .iter()
                        .cloned()
                        .map(crate::wire_codec::append_record_wire)
                        .collect(),
                    if_tail: tail,
                    idempotency_key: bytes::Bytes::copy_from_slice(
                        request.idempotency_key.as_bytes(),
                    ),
                }
                .encode_to_vec();
                let mut actual = Vec::with_capacity(append_len(&request));
                let capacity = actual.capacity();
                append(&mut actual, &request);
                assert_eq!(actual, expected);
                assert_eq!(actual.len(), append_len(&request));
                assert_eq!(actual.capacity(), capacity);
                assert_eq!(
                    crate::http_codec::encode_append(&request)?,
                    crate::http_codec::encode("append", &expected)?
                );
            }
        }
        Ok(())
    }
    #[test]
    fn request_path_append_json_rejects_invalid_records_and_command_size()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let mut request = AppendRequest {
            path: crate::StreamPath::new("request/events")?,
            records: Vec::new(),
            if_tail: None,
            idempotency_key: crate::IdempotencyKey::unique(),
        };
        for records in [
            Vec::new(),
            vec![OwnedBytes::from(vec![0; crate::MAX_RECORD_BYTES + 1])],
            vec![OwnedBytes::from_static(b""); crate::MAX_ITEMS + 1],
            vec![
                OwnedBytes::from(vec![0; crate::MAX_RECORD_BYTES]);
                crate::MAX_COMMAND_BYTES / crate::MAX_RECORD_BYTES + 1
            ],
        ] {
            request.records = records.into_iter().map(Into::into).collect();
            let mut bytes = Vec::with_capacity(append_len(&request));
            append(&mut bytes, &request);
            assert_eq!(
                crate::http_codec::encode_append(&request),
                Err("limit_exceeded")
            );
            assert_eq!(
                crate::http_codec::encode_append(&request),
                crate::http_codec::encode("append", &bytes)
            );
        }
        Ok(())
    }
}
