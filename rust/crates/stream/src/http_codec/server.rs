//! Reverse projection of the SDK-owned hosted request contract.
use super::Result;
use crate::{MAX_COMMAND_BYTES, wire};
use prost::Message;
use serde_json::{Map, Value};

/// Hosted routes that have a canonical `StreamService` RPC. Token issuance is an
/// account-authority operation and is deliberately not a fabricated Stream RPC.
pub const HTTP_ROUTES: &[(&str, &str)] = &[
    ("idempotency/inspect", "InspectIdempotency"), ("tail", "Tail"),
    ("append", "Append"), ("fork", "Fork"), ("read", "Read"),
    ("follow", "Follow"), ("children", "Children"),
    ("children/page", "ChildrenPage"), ("commit", "Commit"),
    ("commits/read", "ReadCommit"),
];

fn object<'a>(value: &'a Value, fields: &[&str]) -> Result<&'a Map<String, Value>> {
    let object = value.as_object().ok_or("invalid_argument")?;
    if object.keys().any(|key| !fields.contains(&key.as_str())) { return Err("invalid_argument"); }
    Ok(object)
}
fn field<'a>(object: &'a Map<String, Value>, name: &str) -> Result<&'a Value> {
    object.get(name).ok_or("invalid_argument")
}
fn string(value: &Value) -> Result<String> {
    value.as_str().map(str::to_owned).ok_or("invalid_argument")
}
fn sequence(value: &Value) -> Result<u64> {
    let value = value.as_str().ok_or("invalid_argument")?;
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit())
        || (value.len() > 1 && value.starts_with('0')) { return Err("invalid_argument"); }
    value.parse().map_err(|_| "invalid_argument")
}
fn limit(value: &Value) -> Result<u32> {
    u32::try_from(value.as_u64().ok_or("invalid_argument")?).map_err(|_| "invalid_argument")
}
fn bytes(value: &Value) -> Result<bytes::Bytes> {
    crate::http_validation::decode_base64(value.as_str().ok_or("invalid_argument")?)
        .map(bytes::Bytes::from).ok_or("invalid_argument")
}
fn values(value: &Value) -> Result<Vec<bytes::Bytes>> {
    let values = value.as_array().ok_or("invalid_argument")?;
    if values.len() > crate::MAX_ITEMS { return Err("limit_exceeded"); }
    values.iter().map(bytes).collect()
}
fn options<'a>(value: &'a Map<String, Value>, fields: &[&str]) -> Result<Option<&'a Map<String, Value>>> {
    value.get("options").map(|value| object(value, fields)).transpose()
}
fn optional<T>(object: Option<&Map<String, Value>>, name: &str, decode: impl FnOnce(&Value) -> Result<T>) -> Result<Option<T>> {
    object.and_then(|object| object.get(name)).map(decode).transpose()
}

/// Decode one hosted JSON request to its generated protobuf message. This is
/// shape projection; authorization and operation validation stay at the provider.
pub fn decode(route: &str, input: &[u8], maximum_bytes: usize) -> Result<Vec<u8>> {
    if maximum_bytes == 0 || input.len() > maximum_bytes { return Err("limit_exceeded"); }
    let value: Value = serde_json::from_slice(input).map_err(|_| "invalid_argument")?;
    let message = match route {
        "idempotency/inspect" => {
            let value = object(&value, &["idempotencyKey"])?;
            wire::InspectIdempotencyRequest { idempotency_key: bytes(field(value, "idempotencyKey")?)? }.encode_to_vec()
        }
        "tail" => {
            let value = object(&value, &["path"])?;
            wire::TailRequest { path: string(field(value, "path")?)? }.encode_to_vec()
        }
        "append" => {
            let value = object(&value, &["path", "values", "options"])?;
            let options = options(value, &["ifTail", "idempotencyKey"])?;
            wire::AppendRequest {
                path: string(field(value, "path")?)?, records: values(field(value, "values")?)?,
                if_tail: optional(options, "ifTail", sequence)?,
                idempotency_key: optional(options, "idempotencyKey", bytes)?,
            }.encode_to_vec()
        }
        "fork" => {
            let value = object(&value, &["source", "destination", "options"])?;
            let options = options(value, &["atTail", "idempotencyKey"])?;
            wire::ForkRequest {
                source: string(field(value, "source")?)?, destination: string(field(value, "destination")?)?,
                at_tail: optional(options, "atTail", sequence)?,
                idempotency_key: optional(options, "idempotencyKey", bytes)?,
            }.encode_to_vec()
        }
        "read" => {
            let value = object(&value, &["path", "from", "limit"])?;
            wire::ReadRequest { path: string(field(value, "path")?)?, from: sequence(field(value, "from")?)?, limit: limit(field(value, "limit")?)? }.encode_to_vec()
        }
        "follow" => {
            let value = object(&value, &["path", "from"])?;
            wire::FollowRequest { path: string(field(value, "path")?)?, from: sequence(field(value, "from")?)? }.encode_to_vec()
        }
        "children" => {
            let value = object(&value, &["parent", "limit"])?;
            wire::ChildrenRequest { parent: optional(Some(value), "parent", string)?, limit: limit(field(value, "limit")?)? }.encode_to_vec()
        }
        "children/page" => {
            let value = object(&value, &["parent", "after", "hierarchyVersion", "limit"])?;
            wire::ChildrenPageRequest {
                parent: optional(Some(value), "parent", string)?, after: optional(Some(value), "after", string)?,
                hierarchy_version: optional(Some(value), "hierarchyVersion", bytes)?, limit: limit(field(value, "limit")?)?,
            }.encode_to_vec()
        }
        "commit" => {
            let value = object(&value, &["request", "options"])?;
            let request = object(field(value, "request")?, &["conditions", "mutations"])?;
            let options = object(field(value, "options")?, &["idempotencyKey", "deadlineUnixMillis"])?;
            let conditions = field(request, "conditions")?.as_array().ok_or("invalid_argument")?;
            let mutations = field(request, "mutations")?.as_array().ok_or("invalid_argument")?;
            if conditions.len() > crate::MAX_ITEMS || mutations.len() > crate::MAX_ITEMS { return Err("limit_exceeded"); }
            wire::CommitRequest {
                conditions: conditions.iter().map(condition).collect::<Result<_>>()?,
                mutations: mutations.iter().map(mutation).collect::<Result<_>>()?,
                idempotency_key: bytes(field(options, "idempotencyKey")?)?,
                deadline_unix_millis: optional(Some(options), "deadlineUnixMillis", sequence)?,
            }.encode_to_vec()
        }
        "commits/read" => {
            let value = object(&value, &["commitId"])?;
            wire::ReadCommitRequest { commit_id: bytes(field(value, "commitId")?)? }.encode_to_vec()
        }
        _ => return Err("invalid_argument"),
    };
    if message.len() > MAX_COMMAND_BYTES { return Err("limit_exceeded"); }
    Ok(message)
}
fn condition(value: &Value) -> Result<wire::CommitCondition> {
    let value = object(value, &["path", "ifTail", "ifAbsent"])?;
    let path = string(field(value, "path")?)?;
    let condition = match (value.get("ifTail"), value.get("ifAbsent")) {
        (Some(tail), None) => wire::commit_condition::Condition::Tail(wire::TailCondition { path, expected: sequence(tail)? }),
        (None, Some(Value::Bool(true))) => wire::commit_condition::Condition::Absent(wire::AbsentCondition { path }),
        _ => return Err("invalid_argument"),
    };
    Ok(wire::CommitCondition { condition: Some(condition) })
}
fn mutation(value: &Value) -> Result<wire::CommitMutation> {
    let value = object(value, &["append", "fork"])?;
    let mutation = match (value.get("append"), value.get("fork")) {
        (Some(append), None) => {
            let append = object(append, &["path", "values"])?;
            wire::commit_mutation::Mutation::Append(wire::AppendMutation { path: string(field(append, "path")?)?, records: values(field(append, "values")?)? })
        }
        (None, Some(fork)) => {
            let fork = object(fork, &["source", "destination", "atTail", "values"])?;
            wire::commit_mutation::Mutation::Fork(wire::ForkMutation {
                source: string(field(fork, "source")?)?, destination: string(field(fork, "destination")?)?,
                at_tail: sequence(field(fork, "atTail")?)?, records: values(field(fork, "values")?)?,
            })
        }
        _ => return Err("invalid_argument"),
    };
    Ok(wire::CommitMutation { mutation: Some(mutation) })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn hosted_decode_preserves_cas_and_uint64() {
        let request = decode("append", br#"{"path":"events","values":["eA=="],"options":{"ifTail":"9007199254740993","idempotencyKey":"aWQ="}}"#, 4096).unwrap();
        let request = wire::AppendRequest::decode(request.as_slice()).unwrap();
        assert_eq!(request.if_tail, Some(9_007_199_254_740_993));
        assert_eq!(request.records[0].as_ref(), b"x");
        for bad in [br#"{"path":"events","from":0,"limit":1}"#.as_slice(), br#"{"path":"events","from":"00","limit":1}"#.as_slice(), br#"{"path":"events","from":"0","limit":1,"account":"other"}"#.as_slice()] {
            assert!(decode("read", bad, 4096).is_err());
        }
    }
}
