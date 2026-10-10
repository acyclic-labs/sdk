//! Canonical hosted Stream response projection shared by native servers and WASM.
use crate::{http_validation::validate, wire};
use prost::Message;
use serde_json::Value;
type Result<T> = std::result::Result<T, &'static str>;
mod streaming;
pub use streaming::{Collection, StreamProjection};

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

/// Encode a generated unary response for one canonical HTTP route.
/// Successful Commit includes its complete immutable envelope atomically.
/// This projects already admitted outcomes; it does not authorize or publish them.
/// # Errors
/// Rejects unknown routes, malformed responses, zero bounds and oversized output.
pub fn encode(route: &str, input: &[u8], maximum_bytes: usize) -> Result<Vec<u8>> {
    if maximum_bytes == 0 || input.len() > maximum_bytes {
        return Err("limit_exceeded");
    }
    let value = value(route, input)?.unwrap_or(Value::Null);
    let encoded = serde_json::to_vec(&value).map_err(|_| "invalid_response")?;
    if encoded.len() > maximum_bytes {
        return Err("limit_exceeded");
    }
    Ok(encoded)
}
pub(crate) fn value(route: &str, input: &[u8]) -> Result<Option<Value>> {
    let value = match route {
        "tail" => json_u64(
            wire::TailResponse::decode(input)
                .map_err(|_| "invalid_response")?
                .tail,
        ),
        "append" => {
            append_value(wire::AppendResponse::decode(input).map_err(|_| "invalid_response")?)?
        }
        "fork" => fork_value(wire::ForkReceipt::decode(input).map_err(|_| "invalid_response")?),
        "commit" => {
            commit_value(wire::CommitResponse::decode(input).map_err(|_| "invalid_response")?)?
        }
        "commits/read" => envelope_value(
            &wire::CommittedEnvelope::decode(input).map_err(|_| "invalid_response")?,
        )?,
        "children/page" => children_page_value(
            wire::ChildrenPageResponse::decode(input).map_err(|_| "invalid_response")?,
        )?,
        "idempotency/inspect" => match observation_value(
            wire::InspectIdempotencyResponse::decode(input).map_err(|_| "invalid_response")?,
        )? {
            Some(value) => value,
            None => return Ok(None),
        },
        _ => return Err("invalid_argument"),
    };
    validate(route, &value)?;
    Ok(Some(value))
}
fn record_json(value: &wire::Record) -> Value {
    json_object(vec![
        ("sequence", json_u64(value.sequence)),
        ("value", json_bytes(value.value.as_ref())),
        ("commitId", json_bytes(value.commit_id.as_ref())),
        ("committedAtMicros", json_u64(value.committed_at_micros)),
    ])
}

fn append_value(value: wire::AppendResponse) -> Result<Value> {
    let outcome = value.outcome.ok_or("invalid_response")?;
    Ok(match outcome {
        wire::append_response::Outcome::Committed(receipt) => json_object(vec![
            ("ok", Value::Bool(true)),
            ("start", json_u64(receipt.start)),
            ("end", json_u64(receipt.end)),
            ("tail", json_u64(receipt.tail)),
            ("commitId", json_bytes(receipt.commit_id.as_ref())),
        ]),
        wire::append_response::Outcome::Conflict(conflict) => json_object(vec![
            ("ok", Value::Bool(false)),
            ("code", json_string("tail_conflict")),
            ("actualTail", json_u64(conflict.actual_tail)),
        ]),
    })
}

fn fork_value(value: wire::ForkReceipt) -> Value {
    json_object(vec![
        ("source", json_string(value.source)),
        ("destination", json_string(value.destination)),
        ("forkedAt", json_u64(value.forked_at)),
        ("tail", json_u64(value.tail)),
        ("commitId", json_bytes(value.commit_id.as_ref())),
    ])
}

fn mutation_value(value: &wire::CommittedMutation) -> Result<Value> {
    let mutation = value.mutation.as_ref().ok_or("invalid_response")?;
    Ok(match mutation {
        wire::committed_mutation::Mutation::Append(value) => json_object(vec![
            ("type", json_string("append")),
            ("path", json_string(value.path.clone())),
            ("start", json_u64(value.start)),
            ("end", json_u64(value.end)),
            ("tail", json_u64(value.tail)),
            (
                "records",
                Value::Array(value.records.iter().map(record_json).collect()),
            ),
        ]),
        wire::committed_mutation::Mutation::Fork(value) => json_object(vec![
            ("type", json_string("fork")),
            ("source", json_string(value.source.clone())),
            ("destination", json_string(value.destination.clone())),
            ("forkedAt", json_u64(value.forked_at)),
            ("tail", json_u64(value.tail)),
            (
                "records",
                Value::Array(value.records.iter().map(record_json).collect()),
            ),
        ]),
    })
}

fn envelope_value(value: &wire::CommittedEnvelope) -> Result<Value> {
    crate::wire_codec::envelope_from_wire(value.clone()).map_err(|_| "invalid_response")?;
    let mutations = value
        .mutations
        .iter()
        .map(mutation_value)
        .collect::<Result<Vec<_>>>()?;
    Ok(json_object(vec![
        ("commitId", json_bytes(value.commit_id.as_ref())),
        ("mutations", Value::Array(mutations)),
    ]))
}

fn children_page_value(value: wire::ChildrenPageResponse) -> Result<Value> {
    let mut entries = vec![
        (
            "hierarchyVersion",
            json_bytes(value.hierarchy_version.as_ref()),
        ),
        (
            "children",
            Value::Array(
                value
                    .children
                    .into_iter()
                    .map(|child| json_object(vec![("path", json_string(child.path))]))
                    .collect(),
            ),
        ),
    ];
    if let Some(next_after) = value.next_after {
        entries.push(("nextAfter", json_string(next_after)));
    }
    Ok(json_object(entries))
}

fn conflict_value(value: &wire::CommitConflict) -> Result<Value> {
    let conflict = value.conflict.as_ref().ok_or("invalid_response")?;
    Ok(match conflict {
        wire::commit_conflict::Conflict::Tail(value) => {
            let mut entries = vec![
                ("path", json_string(value.path.clone())),
                ("expectedTail", json_u64(value.expected)),
            ];
            if let Some(actual) = value.actual {
                entries.push(("actualTail", json_u64(actual)));
            }
            json_object(entries)
        }
        wire::commit_conflict::Conflict::Exists(value) => json_object(vec![
            ("path", json_string(value.path.clone())),
            ("expectedAbsent", Value::Bool(true)),
            ("actual", json_string("exists")),
        ]),
    })
}

fn commit_value(value: wire::CommitResponse) -> Result<Value> {
    let outcome = value.outcome.ok_or("invalid_response")?;
    Ok(match outcome {
        wire::commit_response::Outcome::Committed(envelope) => {
            // Commit responses can be larger than the bounded command
            // request that produced them. Validate the complete envelope
            // before deriving its compact public result so nested records
            // cannot bypass identity and width checks.
            let envelope_json = envelope_value(&envelope)?;
            validate("commits/read", &envelope_json)?;
            let mut tails = serde_json::Map::new();
            let mut forks = Vec::new();
            for mutation in &envelope.mutations {
                let mutation = mutation.mutation.as_ref().ok_or("invalid_response")?;
                match mutation {
                    wire::committed_mutation::Mutation::Append(value) => {
                        tails.insert(value.path.clone(), json_u64(value.tail));
                    }
                    wire::committed_mutation::Mutation::Fork(value) => {
                        forks.push(json_object(vec![
                            ("path", json_string(value.destination.clone())),
                            ("tail", json_u64(value.tail)),
                        ]));
                    }
                }
            }
            json_object(vec![
                ("ok", Value::Bool(true)),
                ("commitId", json_bytes(envelope.commit_id.as_ref())),
                ("tails", Value::Object(tails)),
                ("forks", Value::Array(forks)),
                ("envelope", envelope_json),
            ])
        }
        wire::commit_response::Outcome::Conflict(conflicts) => json_object(vec![
            ("ok", Value::Bool(false)),
            ("code", json_string("conflict")),
            (
                "conflicts",
                Value::Array(
                    conflicts
                        .conflicts
                        .iter()
                        .map(conflict_value)
                        .collect::<Result<Vec<_>>>()?,
                ),
            ),
        ]),
    })
}

fn observation_value(value: wire::InspectIdempotencyResponse) -> Result<Option<Value>> {
    let Some(observation) = value.observation else {
        return Ok(None);
    };
    let outcome = observation.outcome.ok_or("invalid_response")?;
    let outcome = match outcome {
        wire::idempotency_observation::Outcome::Append(value) => json_object(vec![
            ("type", json_string("append")),
            ("outcome", append_value(value)?),
        ]),
        wire::idempotency_observation::Outcome::Fork(value) => json_object(vec![
            ("type", json_string("fork")),
            ("receipt", fork_value(value)),
        ]),
        wire::idempotency_observation::Outcome::Commit(value) => json_object(vec![
            ("type", json_string("commit")),
            ("outcome", commit_value(value)?),
        ]),
    };
    Ok(Some(json_object(vec![
        (
            "idempotencyKey",
            json_bytes(observation.idempotency_key.as_ref()),
        ),
        (
            "requestDigest",
            json_bytes(observation.request_digest.as_ref()),
        ),
        ("outcome", outcome),
    ])))
}

/// Shared canonical gRPC status mapping used by gRPC and hosted HTTP clients.
/// The message argument is the decoded gRPC message, not its HTTP percent form.
pub fn error_from_grpc(code: u8, message: &str) -> crate::StreamError {
    use crate::StreamError as E;
    match (code, message) {
        (3, "invalid_path") => E::InvalidPath,
        (3, "limit_exceeded") => E::LimitExceeded,
        (3, _) => E::InvalidArgument,
        (5, _) => E::NotFound,
        (6, _) => E::AlreadyExists,
        (11, _) => E::OutOfRange,
        (9, "hierarchy_changed") => E::HierarchyChanged,
        (7 | 16, _) => E::AccessDenied,
        (8, _) => E::Capacity,
        (9, "idempotency_mismatch") => E::IdempotencyMismatch,
        (9, "prefix_not_retained") => E::PrefixNotRetained,
        (9, "deadline_elapsed") => E::DeadlineElapsed,
        (12, "unsupported_capability") => E::Unsupported,
        _ => E::Unavailable,
    }
}
