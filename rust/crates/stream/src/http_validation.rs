//! Pure validation for hosted Stream HTTP response payloads.

use serde_json::{Map, Value};

type Result<T = ()> = std::result::Result<T, &'static str>;

fn object(value: &Value) -> Result<&Map<String, Value>> {
    value.as_object().ok_or("expected object")
}

fn field<'a>(item: &'a Map<String, Value>, name: &str) -> Result<&'a Value> {
    item.get(name).ok_or("missing field")
}

fn string(value: &Value) -> Result<&str> {
    value
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or("expected non-empty string")
}

fn u64_string(value: &Value) -> Result<u64> {
    let value = string(value)?;
    if !value.as_bytes().iter().all(|byte| byte.is_ascii_digit())
        || (value.len() > 1 && value.starts_with('0'))
    {
        return Err("expected decimal uint64 string");
    }
    value.parse().map_err(|_| "expected decimal uint64 string")
}

fn boolean(value: &Value) -> Result<bool> {
    value.as_bool().ok_or("expected boolean")
}

fn array(value: &Value) -> Result<&Vec<Value>> {
    value.as_array().ok_or("expected array")
}

fn path(value: &Value) -> Result {
    let value = string(value)?;
    crate::StreamPath::new(value)
        .map(|_| ())
        .map_err(|_| "invalid path")
}

fn decode_base64(value: &str) -> Option<Vec<u8>> {
    if value.is_empty() || !value.len().is_multiple_of(4) {
        return None;
    }
    let mut output = Vec::with_capacity(value.len() / 4 * 3);
    let bytes = value.as_bytes();
    for (index, chunk) in bytes.chunks_exact(4).enumerate() {
        let &[a_byte, b_byte, c_byte, d_byte] = chunk else {
            return None;
        };
        let a = sextet(a_byte)?;
        let b = sextet(b_byte)?;
        let c = if c_byte == b'=' { 0 } else { sextet(c_byte)? };
        let d = if d_byte == b'=' { 0 } else { sextet(d_byte)? };
        if c_byte == b'=' {
            if d_byte != b'=' || b & 0x0f != 0 {
                return None;
            }
        } else if d_byte == b'=' && (c & 0x03 != 0 || index + 1 != bytes.len() / 4) {
            return None;
        }
        output.push(a << 2 | b >> 4);
        if c_byte != b'=' {
            output.push(b << 4 | c >> 2);
        }
        if d_byte != b'=' {
            output.push(c << 6 | d);
        }
    }
    Some(output)
}

fn sextet(value: u8) -> Option<u8> {
    match value {
        b'A'..=b'Z' => Some(value - b'A'),
        b'a'..=b'z' => Some(value - b'a' + 26),
        b'0'..=b'9' => Some(value - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

fn bytes(value: &Value) -> Result<Vec<u8>> {
    decode_base64(string(value)?).ok_or("expected base64")
}

fn id(value: &Value) -> Result {
    (bytes(value)?.len() == 32)
        .then_some(())
        .ok_or("expected 32-byte id")
}

fn key(value: &Value) -> Result {
    let len = bytes(value)?.len();
    (len > 0 && len <= crate::MAX_IDEMPOTENCY_KEY_BYTES)
        .then_some(())
        .ok_or("invalid idempotency key")
}

fn validate_record(value: &Value) -> Result {
    let item = object(value)?;
    u64_string(field(item, "sequence")?)?;
    bytes(field(item, "value")?)?;
    id(field(item, "commitId")?)
}

fn validate_append_result(value: &Value) -> Result {
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

fn validate_fork_receipt(value: &Value) -> Result {
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

fn validate_trim_receipt(value: &Value) -> Result {
    let item = object(value)?;
    path(field(item, "path")?)?;
    u64_string(field(item, "trimPoint")?)?;
    id(field(item, "commitId")?)
}

fn validate_delete_receipt(value: &Value) -> Result {
    let item = object(value)?;
    path(field(item, "path")?)?;
    id(field(item, "commitId")?)
}

fn validate_conflict(value: &Value) -> Result {
    let item = object(value)?;
    path(field(item, "path")?)?;
    if item.get("expectedAbsent") == Some(&Value::Bool(true)) {
        match string(field(item, "actual")?)? {
            "exists" | "retired" => Ok(()),
            _ => Err("invalid conflict state"),
        }
    } else {
        u64_string(field(item, "expectedTail")?)?;
        if let Some(actual) = item.get("actualTail") {
            u64_string(actual)?;
        }
        Ok(())
    }
}

fn validate_commit_result(value: &Value) -> Result {
    let item = object(value)?;
    if boolean(field(item, "ok")?)? {
        id(field(item, "commitId")?)?;
        for (path_name, tail) in object(field(item, "tails")?)? {
            crate::StreamPath::new(path_name).map_err(|_| "invalid path")?;
            u64_string(tail)?;
        }
        for fork in array(field(item, "forks")?)? {
            let fork = object(fork)?;
            path(field(fork, "path")?)?;
            u64_string(field(fork, "tail")?)?;
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

fn validate_mutation(value: &Value) -> Result {
    let item = object(value)?;
    match string(field(item, "type")?)? {
        "append" => {
            path(field(item, "path")?)?;
            let start = u64_string(field(item, "start")?)?;
            let end = u64_string(field(item, "end")?)?;
            let tail = u64_string(field(item, "tail")?)?;
            if start > end || end > tail {
                return Err("invalid append positions");
            }
            for record in array(field(item, "records")?)? {
                validate_record(record)?;
            }
        }
        "fork" => {
            path(field(item, "source")?)?;
            path(field(item, "destination")?)?;
            let forked_at = u64_string(field(item, "forkedAt")?)?;
            let tail = u64_string(field(item, "tail")?)?;
            if forked_at > tail {
                return Err("invalid fork positions");
            }
        }
        "trim" => {
            path(field(item, "path")?)?;
            u64_string(field(item, "trimPoint")?)?;
        }
        "delete" => path(field(item, "path")?)?,
        _ => return Err("invalid mutation type"),
    }
    Ok(())
}

fn validate_envelope(value: &Value) -> Result {
    let item = object(value)?;
    id(field(item, "commitId")?)?;
    for mutation in array(field(item, "mutations")?)? {
        validate_mutation(mutation)?;
    }
    Ok(())
}

fn validate_idempotency(value: &Value) -> Result {
    let item = object(value)?;
    key(field(item, "idempotencyKey")?)?;
    if bytes(field(item, "requestDigest")?)?.len() != 32 {
        return Err("requestDigest must contain 32 bytes");
    }
    let outcome = object(field(item, "outcome")?)?;
    match string(field(outcome, "type")?)? {
        "append" => validate_append_result(field(outcome, "outcome")?),
        "fork" => validate_fork_receipt(field(outcome, "receipt")?),
        "trim" => validate_trim_receipt(field(outcome, "receipt")?),
        "delete" => validate_delete_receipt(field(outcome, "receipt")?),
        "commit" => validate_commit_result(field(outcome, "outcome")?),
        _ => Err("invalid idempotency outcome type"),
    }
}

pub fn validate(route: &str, value: &Value) -> Result {
    match crate::HTTP_RESPONSE_CONTRACT
        .iter()
        .find_map(|(candidate, kind)| (*candidate == route).then_some(*kind))
    {
        Some("sequence") => {
            u64_string(value)?;
            Ok(())
        }
        Some("append") => validate_append_result(value),
        Some("fork") => validate_fork_receipt(value),
        Some("trim") => validate_trim_receipt(value),
        Some("delete") => validate_delete_receipt(value),
        Some("records") => {
            for item in array(value)? {
                validate_record(item)?;
            }
            Ok(())
        }
        Some("children") => {
            for item in array(value)? {
                path(field(object(item)?, "path")?)?;
            }
            Ok(())
        }
        Some("commit") => validate_commit_result(value),
        Some("envelope") => validate_envelope(value),
        Some("observation") => {
            if value.is_null() {
                Ok(())
            } else {
                validate_idempotency(value)
            }
        }
        Some("token") => {
            let item = object(value)?;
            string(field(item, "token")?)?;
            string(field(item, "expiresAt")?)?;
            Ok(())
        }
        _ => Err("unknown HTTP route"),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    fn json_fixture(value: &str) -> serde_json::Result<Value> {
        serde_json::from_str(value)
    }

    #[test]
    fn hosted_http_validation_covers_receipts_and_tagged_conflicts() -> serde_json::Result<()> {
        let commit_id = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
        let valid =
            format!(r#"{{"ok":true,"start":"0","end":"1","tail":"1","commitId":"{commit_id}"}}"#);
        assert!(super::validate("append", &json_fixture(&valid)?).is_ok());

        let impossible =
            format!(r#"{{"ok":true,"start":"2","end":"1","tail":"1","commitId":"{commit_id}"}}"#);
        assert!(super::validate("append", &json_fixture(&impossible)?).is_err());

        let conflict = r#"{"ok":false,"code":"conflict","conflicts":[{"path":"events","expectedTail":"2","actualTail":"3"}]}"#;
        assert!(super::validate("commit", &json_fixture(conflict)?).is_ok());
        let invalid_conflict = r#"{"ok":false,"code":"conflict","conflicts":[{"path":"events","expectedAbsent":true,"actual":"missing"}]}"#;
        assert!(super::validate("commit", &json_fixture(invalid_conflict)?).is_err());
        Ok(())
    }

    #[test]
    fn hosted_http_validation_rejects_bad_ids_paths_and_decimal_widths() -> serde_json::Result<()> {
        let bad_id = r#"{"path":"events","trimPoint":"01","commitId":"AAAA"}"#;
        assert!(super::validate("trim", &json_fixture(bad_id)?).is_err());
        let bad_path = r#"[{"path":"events//child"}]"#;
        assert!(super::validate("children", &json_fixture(bad_path)?).is_err());
        assert!(super::validate("tail", &json_fixture("1")?).is_err());
        Ok(())
    }
}
