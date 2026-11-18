//! JSON field and receipt validation for the hosted Stream protocol.

use crate::validation::{is_idempotency_key, is_path};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Map, Value};

type Result<T = ()> = std::result::Result<T, &'static str>;

/// Reads a JSON object.
///
/// # Errors
/// Returns the wire validation reason for an invalid value.
pub fn object(value: &Value) -> Result<&Map<String, Value>> {
    value.as_object().ok_or("expected object")
}

/// Reads a required member.
///
/// # Errors
/// Returns the wire validation reason for an invalid value.
pub fn field<'a>(item: &'a Map<String, Value>, name: &str) -> Result<&'a Value> {
    item.get(name).ok_or("missing field")
}

/// Reads a nonempty string.
///
/// # Errors
/// Returns the wire validation reason for an invalid value.
pub fn string(value: &Value) -> Result<&str> {
    value
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or("expected non-empty string")
}

fn encoded_bytes(value: &Value) -> Result<&str> {
    value.as_str().ok_or("expected base64 string")
}

/// Reads a canonical decimal uint64 string.
///
/// # Errors
/// Returns the wire validation reason for an invalid value.
pub fn u64_string(value: &Value) -> Result<u64> {
    let value = string(value)?;
    if !acyclic_decimal_u64::is_canonical(value) {
        return Err("expected decimal uint64 string");
    }
    value.parse().map_err(|_| "expected decimal uint64 string")
}

/// Reads a JSON boolean.
///
/// # Errors
/// Returns the wire validation reason for an invalid value.
pub fn boolean(value: &Value) -> Result<bool> {
    value.as_bool().ok_or("expected boolean")
}

/// Reads a JSON array.
///
/// # Errors
/// Returns the wire validation reason for an invalid value.
pub fn array(value: &Value) -> Result<&Vec<Value>> {
    value.as_array().ok_or("expected array")
}

/// Checks a revision identity and index.
///
/// # Errors
/// Returns the wire validation reason for an invalid value.
pub fn validate_revision(value: &Value) -> Result {
    let item = object(value)?;
    bytes(field(item, "scope")?)?;
    if bytes(field(item, "incarnation")?)?.len() != 16 {
        return Err("invalid revision incarnation");
    }
    u64_string(field(item, "index")?)?;
    Ok(())
}

/// Checks a canonical path string.
///
/// # Errors
/// Returns the wire validation reason for an invalid value.
pub fn path(value: &Value) -> Result {
    let value = string(value)?;
    is_path(value).then_some(()).ok_or("invalid path")
}

/// Decodes a base64 value.
///
/// # Errors
/// Returns the wire validation reason for an invalid value.
pub fn bytes(value: &Value) -> Result<Vec<u8>> {
    STANDARD
        .decode(encoded_bytes(value)?)
        .map_err(|_| "expected base64")
}

/// Checks the width of a commit or hierarchy identity.
///
/// # Errors
/// Returns the wire validation reason for an invalid value.
pub fn id(value: &Value) -> Result {
    (bytes(value)?.len() == 32)
        .then_some(())
        .ok_or("expected 32-byte id")
}

/// Checks a caller retry identity.
///
/// # Errors
/// Returns the wire validation reason for an invalid value.
pub fn key(value: &Value) -> Result {
    is_idempotency_key(&bytes(value)?)
        .then_some(())
        .ok_or("invalid idempotency key")
}

/// Checks a fork origin and returns its source position.
///
/// # Errors
/// Returns the wire validation reason for an invalid value.
pub fn validate_origin(value: &Value) -> Result<u64> {
    let origin = object(value)?;
    path(field(origin, "source")?)?;
    id(field(origin, "commitId")?)?;
    u64_string(field(origin, "forkedAt")?)
}

/// Checks append receipt positions or a typed tail conflict.
///
/// # Errors
/// Returns the wire validation reason for an invalid value.
pub fn validate_append_result(value: &Value) -> Result {
    let item = object(value)?;
    if boolean(field(item, "ok")?)? {
        let start = u64_string(field(item, "start")?)?;
        let end = u64_string(field(item, "end")?)?;
        let tail = u64_string(field(item, "tail")?)?;
        if start > end || end > tail {
            return Err("invalid append positions");
        }
        id(field(item, "commitId")?)?;
        return Ok(());
    }
    if string(field(item, "code")?)? != "tail_conflict" {
        return Err("invalid append conflict code");
    }
    u64_string(field(item, "actualTail")?)?;
    Ok(())
}

/// Checks fork identity and positions.
///
/// # Errors
/// Returns the wire validation reason for an invalid value.
pub fn validate_fork_receipt(value: &Value) -> Result {
    let item = object(value)?;
    path(field(item, "source")?)?;
    path(field(item, "destination")?)?;
    let forked_at = u64_string(field(item, "forkedAt")?)?;
    let tail = u64_string(field(item, "tail")?)?;
    if forked_at > tail {
        return Err("invalid fork positions");
    }
    id(field(item, "commitId")?)
}

fn validate_conflict(value: &Value) -> Result {
    let item = object(value)?;
    path(field(item, "path")?)?;
    if item.get("expectedPresent") == Some(&Value::Bool(true)) {
        if string(field(item, "actual")?)? != "missing" {
            return Err("invalid presence conflict state");
        }
        Ok(())
    } else if item.get("expectedAbsent") == Some(&Value::Bool(true)) {
        match string(field(item, "actual")?)? {
            "exists" => Ok(()),
            _ => Err("invalid conflict state"),
        }
    } else if let Some(expected) = item.get("expectedCommitId") {
        id(expected)?;
        id(field(item, "actualCommitId")?)?;
        if expected == field(item, "actualCommitId")? {
            return Err("head conflict identities match");
        }
        Ok(())
    } else {
        u64_string(field(item, "expectedTail")?)?;
        if let Some(actual) = item.get("actualTail") {
            u64_string(actual)?;
        }
        Ok(())
    }
}

/// Checks committed ranges, fork receipts or typed conflicts.
///
/// # Errors
/// Returns the wire validation reason for an invalid value.
pub fn validate_commit_result(value: &Value) -> Result {
    let item = object(value)?;
    if boolean(field(item, "ok")?)? {
        let commit = field(item, "commitId")?;
        id(commit)?;
        u64_string(field(item, "committedAtMicros")?)?;
        let appends = array(field(item, "appends")?)?;
        let forks = array(field(item, "forks")?)?;
        let skipped = match item.get("skipped") {
            Some(skipped) => array(skipped)?.as_slice(),
            None => &[],
        };
        // A commit that skipped every effect publishes nothing.
        if appends.len().saturating_add(forks.len()) == 0 && skipped.is_empty() {
            return Err("invalid receipt size");
        }
        let mut previous = None;
        for index in skipped {
            let index = index
                .as_u64()
                .filter(|index| *index <= u64::from(u32::MAX))
                .ok_or("skipped must list effect indices")?;
            if previous.is_some_and(|previous| previous >= index) {
                return Err("skipped must ascend");
            }
            previous = Some(index);
        }
        for range in appends {
            let range = object(range)?;
            path(field(range, "path")?)?;
            let start = u64_string(field(range, "start")?)?;
            let tail = u64_string(field(range, "tail")?)?;
            if start >= tail {
                return Err("invalid append range");
            }
        }
        for fork in forks {
            validate_fork_receipt(fork)?;
            if fork.get("commitId") != Some(commit) {
                return Err("fork receipt names another commit");
            }
        }
        return Ok(());
    }
    if string(field(item, "code")?)? != "conflict" {
        return Err("invalid commit conflict code");
    }
    for conflict in array(field(item, "conflicts")?)? {
        validate_conflict(conflict)?;
    }
    Ok(())
}
