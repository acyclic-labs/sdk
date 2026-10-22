//! Hosted Stream responses shared by native servers and WASM.
use crate::{http_validation::validate, wire};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use prost::Message;
use serde_json::{Value, json};
type Result<T> = std::result::Result<T, &'static str>;

/// Encodes one bounded canonical HTTP response.
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
        "delivery/acknowledge" => {
            wire::AcknowledgeDeliveryResponse::decode(input).map_err(|_| "invalid_response")?;
            json!({})
        }
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
    json!({
        "sequence": value.sequence.to_string(),
        "value": STANDARD.encode(&value.value),
        "commitId": STANDARD.encode(&value.commit_id),
        "committedAtMicros": value.committed_at_micros.to_string(),
        "keys": value.keys.iter().map(|key| STANDARD.encode(key)).collect::<Vec<_>>(),
        "attributes": value.attributes.iter().map(|attribute| json!({
            "name": attribute.name,
            "value": STANDARD.encode(&attribute.value),
        })).collect::<Vec<_>>(),
    })
}

fn append_value(value: wire::AppendResponse) -> Result<Value> {
    Ok(match value.outcome.ok_or("invalid_response")? {
        wire::append_response::Outcome::Committed(value) => json!({
            "ok": true,
            "start": value.start.to_string(),
            "end": value.end.to_string(),
            "tail": value.tail.to_string(),
            "commitId": STANDARD.encode(value.commit_id),
        }),
        wire::append_response::Outcome::Conflict(value) => json!({
            "ok": false,
            "code": "tail_conflict",
            "actualTail": value.actual_tail.to_string(),
        }),
    })
}

fn fork_value(value: wire::ForkReceipt) -> Value {
    json!({
        "source": value.source,
        "destination": value.destination,
        "forkedAt": value.forked_at.to_string(),
        "tail": value.tail.to_string(),
        "commitId": STANDARD.encode(value.commit_id),
    })
}

fn mutation_value(value: &wire::CommittedMutation) -> Result<Value> {
    Ok(match value.mutation.as_ref().ok_or("invalid_response")? {
        wire::committed_mutation::Mutation::Append(value) => json!({
            "type": "append",
            "path": value.path,
            "start": value.start.to_string(),
            "end": value.end.to_string(),
            "tail": value.tail.to_string(),
            "records": value.records.iter().map(record_json).collect::<Vec<_>>(),
        }),
        wire::committed_mutation::Mutation::Fork(value) => json!({
            "type": "fork",
            "source": value.source,
            "destination": value.destination,
            "forkedAt": value.forked_at.to_string(),
            "tail": value.tail.to_string(),
            "records": value.records.iter().map(record_json).collect::<Vec<_>>(),
        }),
    })
}

fn envelope_value(value: &wire::CommittedEnvelope) -> Result<Value> {
    crate::wire_codec::envelope_from_wire(value.clone()).map_err(|_| "invalid_response")?;
    let mutations = value
        .mutations
        .iter()
        .map(mutation_value)
        .collect::<Result<Vec<_>>>()?;
    Ok(json!({
        "commitId": STANDARD.encode(&value.commit_id),
        "mutations": mutations,
    }))
}

fn children_page_value(value: wire::ChildrenPageResponse) -> Result<Value> {
    let children: Vec<_> = value
        .children
        .into_iter()
        .map(|child| json!({ "path": child.path }))
        .collect();
    let mut result = serde_json::Map::from_iter([
        (
            "hierarchyVersion".into(),
            json!(STANDARD.encode(value.hierarchy_version)),
        ),
        ("children".into(), json!(children)),
    ]);
    if let Some(next) = value.next_after {
        result.insert("nextAfter".into(), json!(next));
    }
    Ok(Value::Object(result))
}

fn conflict_value(value: &wire::CommitConflict) -> Result<Value> {
    Ok(match value.conflict.as_ref().ok_or("invalid_response")? {
        wire::commit_conflict::Conflict::Head(value) => json!({
            "path": value.path,
            "expectedCommitId": STANDARD.encode(&value.expected),
            "actualCommitId": STANDARD.encode(&value.actual),
        }),
        wire::commit_conflict::Conflict::Tail(value) => {
            let mut result = serde_json::Map::from_iter([
                ("path".into(), json!(value.path)),
                ("expectedTail".into(), json!(value.expected.to_string())),
            ]);
            if let Some(actual) = value.actual {
                result.insert("actualTail".into(), json!(actual.to_string()));
            }
            Value::Object(result)
        }
        wire::commit_conflict::Conflict::Exists(value) => json!({
            "path": value.path,
            "expectedAbsent": true,
            "actual": "exists",
        }),
    })
}

fn commit_value(value: wire::CommitResponse) -> Result<Value> {
    Ok(match value.outcome.ok_or("invalid_response")? {
        wire::commit_response::Outcome::Committed(envelope) => {
            let envelope_json = envelope_value(&envelope)?;
            validate("commits/read", &envelope_json)?;
            let mut tails = serde_json::Map::new();
            let mut forks = Vec::new();
            for mutation in &envelope.mutations {
                match mutation.mutation.as_ref().ok_or("invalid_response")? {
                    wire::committed_mutation::Mutation::Append(value) => {
                        tails.insert(value.path.clone(), json!(value.tail.to_string()));
                    }
                    wire::committed_mutation::Mutation::Fork(value) => forks.push(json!({
                        "path": value.destination,
                        "tail": value.tail.to_string(),
                    })),
                }
            }
            json!({
                "ok": true,
                "commitId": STANDARD.encode(envelope.commit_id),
                "tails": tails,
                "forks": forks,
                "envelope": envelope_json,
            })
        }
        wire::commit_response::Outcome::Conflict(conflicts) => json!({
            "ok": false,
            "code": "conflict",
            "conflicts": conflicts.conflicts.iter().map(conflict_value).collect::<Result<Vec<_>>>()?,
        }),
    })
}

fn observation_value(value: wire::InspectIdempotencyResponse) -> Result<Option<Value>> {
    let Some(value) = value.observation else {
        return Ok(None);
    };
    let outcome = match value.outcome.ok_or("invalid_response")? {
        wire::idempotency_observation::Outcome::Append(value) => json!({
            "type": "append",
            "outcome": append_value(value)?,
        }),
        wire::idempotency_observation::Outcome::Fork(value) => json!({
            "type": "fork",
            "receipt": fork_value(value),
        }),
        wire::idempotency_observation::Outcome::Commit(value) => json!({
            "type": "commit",
            "outcome": commit_value(value)?,
        }),
    };
    Ok(Some(json!({
        "idempotencyKey": STANDARD.encode(value.idempotency_key),
        "requestDigest": STANDARD.encode(value.request_digest),
        "outcome": outcome,
    })))
}
