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
        "heads" => {
            let view = crate::wire_codec::heads_from_response(
                wire::HeadsResponse::decode(input).map_err(|_| "invalid_response")?,
            )
            .map_err(|_| "invalid_response")?;
            let view = crate::wire_codec::heads_to_wire(view);
            let heads: Vec<_> = view
                .heads
                .into_iter()
                .map(|observation| observation.head.as_ref().map(head_json))
                .collect();
            json!({ "snapshot": revision_json(view.snapshot.as_ref()), "heads": heads, "children": view.children.map(children_page_value).transpose()?, "projection": match wire::HeadProjection::try_from(view.projection).map_err(|_| "invalid_response")? { wire::HeadProjection::History => "history", wire::HeadProjection::Watermarks => "watermarks" } })
        }
        "head" => {
            let head = crate::wire_codec::head_to_wire(
                crate::wire_codec::head_from_wire(
                    wire::PathHead::decode(input).map_err(|_| "invalid_response")?,
                )
                .map_err(|_| "invalid_response")?,
            );
            head_json(&head)
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

/// The hosted spelling of the shared revision token; absent is null.
pub(crate) fn revision_json(value: Option<&wire::Revision>) -> Value {
    value.map_or(Value::Null, |value| {
        json!({
            "scope": STANDARD.encode(&value.scope),
            "incarnation": STANDARD.encode(&value.incarnation),
            "index": value.index.to_string(),
        })
    })
}

fn origin_json(value: &wire::ForkOrigin) -> Value {
    json!({
        "source": value.source,
        "forkedAt": value.forked_at.to_string(),
        "commitId": STANDARD.encode(&value.commit_id),
    })
}

fn append_value(value: wire::AppendResponse) -> Result<Value> {
    Ok(match value.outcome.ok_or("invalid_response")? {
        wire::append_response::Outcome::Committed(value) => json!({
            "ok": true,
            "start": value.start.to_string(),
            "end": value.end.to_string(),
            "tail": value.tail.to_string(),
            "revision": revision_json(value.revision.as_ref()),
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
        "revision": revision_json(value.revision.as_ref()),
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
            "exclude": value.exclude,
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
        "committedAtMicros": value.committed_at_micros.to_string(),
    }))
}

fn children_page_value(value: wire::ChildrenPageResponse) -> Result<Value> {
    let children: Vec<_> = value
        .children
        .into_iter()
        .map(|child| {
            let mut value = serde_json::Map::from_iter([("path".into(), json!(child.path))]);
            if let Some(record) = child.tail_record {
                value.insert("tailRecord".into(), record_json(&record));
            }
            if let Some(origin) = child.origin {
                value.insert("origin".into(), origin_json(&origin));
            }
            Value::Object(value)
        })
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
        wire::commit_conflict::Conflict::Missing(value) => json!({
            "path": value.path,
            "expectedPresent": true,
            "actual": "missing",
        }),
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
        wire::commit_response::Outcome::Committed(receipt) => {
            crate::wire_codec::receipt_from_wire(receipt.clone())
                .map_err(|_| "invalid_response")?;
            json!({
                "ok": true,
                "commitId": STANDARD.encode(receipt.commit_id),
                "committedAtMicros": receipt.committed_at_micros.to_string(),
                "revision": revision_json(receipt.revision.as_ref()),
                "appends": receipt.appends.into_iter().map(|range| json!({
                    "path": range.path, "start": range.start.to_string(), "tail": range.tail.to_string(),
                })).collect::<Vec<_>>(),
                "forks": receipt.forks.into_iter().map(fork_value).collect::<Vec<_>>(),
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
            "outcome": envelope_value(&value)?,
        }),
    };
    Ok(Some(json!({
        "idempotencyKey": STANDARD.encode(value.idempotency_key),
        "requestDigest": STANDARD.encode(value.request_digest),
        "outcome": outcome,
        "revision": revision_json(value.revision.as_ref()),
    })))
}

fn head_json(head: &wire::PathHead) -> Value {
    json!({
        "tail": head.tail.to_string(),
        "last": head.last.as_ref().map(record_json),
        "durable": head.durable.to_string(),
        "origin": head.origin.as_ref().map(origin_json),
    })
}
