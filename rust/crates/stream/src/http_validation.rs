//! Pure validation for hosted Stream HTTP response payloads.

use serde_json::{Map, Value};

type Result<T = ()> = std::result::Result<T, &'static str>;

/// Validates the endpoint policy shared by native and browser HTTP clients.
/// HTTPS is allowed for hosted endpoints; HTTP is limited to loopback fixture
/// servers. Credentials, queries, and fragments are never accepted.
pub fn validate_endpoint(endpoint: &str) -> Result {
    if endpoint
        .chars()
        .any(|character| character.is_ascii_control() || character.is_ascii_whitespace())
    {
        return Err("invalid_endpoint");
    }
    let Some((scheme, remainder)) = endpoint.split_once("://") else {
        return Err("invalid_endpoint");
    };
    if scheme != "https" && scheme != "http" {
        return Err("invalid_endpoint");
    }
    let authority_end = remainder.find(['/', '?', '#']).unwrap_or(remainder.len());
    let authority = remainder
        .get(..authority_end)
        .ok_or("invalid_endpoint")?;
    if authority.is_empty()
        || authority.contains('@')
        || remainder
            .get(authority_end..)
            .ok_or("invalid_endpoint")?
            .bytes()
            .any(|character| character == b'?' || character == b'#')
    {
        return Err("invalid_endpoint");
    }
    let (host, port) = if let Some(host) = authority.strip_prefix('[') {
        let Some(end) = host.find(']') else {
            return Err("invalid_endpoint");
        };
        let port = host.get(end + 1..).ok_or("invalid_endpoint")?;
        if !port.is_empty() && !port.starts_with(':') {
            return Err("invalid_endpoint");
        }
        (host.get(..end).ok_or("invalid_endpoint")?, port.strip_prefix(':'))
    } else {
        let (host, port) = match authority.rsplit_once(':') {
            Some((host, port)) => (host, Some(port)),
            None => (authority, None),
        };
        if host.contains(':') {
            return Err("invalid_endpoint");
        }
        (host, port)
    };
    if host.is_empty() || port.is_some_and(|port| port.is_empty() || port.parse::<u16>().is_err()) {
        return Err("invalid_endpoint");
    }
    let loopback = host == "localhost"
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback());
    if scheme == "http" && !loopback {
        return Err("invalid_endpoint");
    }
    Ok(())
}

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

fn encoded_bytes(value: &Value) -> Result<&str> {
    value.as_str().ok_or("expected base64 string")
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

pub(crate) fn decode_base64(value: &str) -> Option<Vec<u8>> {
    if !value.len().is_multiple_of(4) {
        return None;
    }
    let mut output = Vec::with_capacity(value.len() / 4 * 3);
    let bytes = value.as_bytes();
    let (chunks, _) = bytes.as_chunks::<4>();
    for (index, &[a_byte, b_byte, c_byte, d_byte]) in chunks.iter().enumerate() {
        let a = sextet(a_byte)?;
        let b = sextet(b_byte)?;
        let c = if c_byte == b'=' { 0 } else { sextet(c_byte)? };
        let d = if d_byte == b'=' { 0 } else { sextet(d_byte)? };
        if c_byte == b'=' {
            if d_byte != b'=' || b & 0x0f != 0 || index + 1 != bytes.len() / 4 {
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
    decode_base64(encoded_bytes(value)?).ok_or("expected base64")
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
    id(field(item, "commitId")?)?;
    u64_string(field(item, "committedAtMicros")?)?;
    Ok(())
}

fn validate_children_page(value: &Value) -> Result {
    let item = object(value)?;
    id(field(item, "hierarchyVersion")?)?;
    let children = array(field(item, "children")?)?;
    let mut previous: Option<&str> = None;
    for child in children {
        let child = object(child)?;
        let path_value = field(child, "path")?;
        path(path_value)?;
        let current = string(path_value)?;
        if previous.is_some_and(|previous| previous >= current) {
            return Err("children are not ordered");
        }
        previous = Some(current);
    }
    if let Some(next_after) = item.get("nextAfter").filter(|value| !value.is_null()) {
        let next_after = string(next_after)?;
        if previous != Some(next_after) {
            return Err("child continuation does not match final child");
        }
    }
    Ok(())
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

fn validate_conflict(value: &Value) -> Result {
    let item = object(value)?;
    path(field(item, "path")?)?;
    if item.get("expectedAbsent") == Some(&Value::Bool(true)) {
        match string(field(item, "actual")?)? {
            "exists" => Ok(()),
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
        if let Some(envelope) = item.get("envelope") {
            validate_envelope(envelope)?;
            if envelope.get("commitId") != item.get("commitId") {
                return Err("commit envelope identity differs");
            }
            let mut tails = Map::new();
            let mut forks = Vec::new();
            for mutation in array(field(object(envelope)?, "mutations")?)? {
                let mutation = object(mutation)?;
                match string(field(mutation, "type")?)? {
                    "append" => {
                        tails.insert(
                            string(field(mutation, "path")?)?.to_owned(),
                            field(mutation, "tail")?.clone(),
                        );
                    }
                    "fork" => {
                        let mut fork = Map::new();
                        fork.insert("path".to_owned(), field(mutation, "destination")?.clone());
                        fork.insert("tail".to_owned(), field(mutation, "tail")?.clone());
                        forks.push(Value::Object(fork));
                    }
                    _ => return Err("invalid mutation type"),
                }
            }
            if field(item, "tails")? != &Value::Object(tails)
                || field(item, "forks")? != &Value::Array(forks)
            {
                return Err("commit envelope summary differs");
            }
        }
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
            if let Some(records) = item.get("records") {
                for record in array(records)? {
                    validate_record(record)?;
                }
            }
        }
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
        Some("children_page") => validate_children_page(value),
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

/// Returns the validated tail from a hosted tail response. Keeping the
/// decimal uint64 conversion here lets follow cursors compare positions in
/// Rust without reopening the JSON scalar contract in a language adapter.
pub fn tail_value(value: &Value) -> Result<u64> {
    validate("tail", value)?;
    u64_string(value)
}

/// Validates a hosted read page against the cursor captured by its caller.
/// The response schema alone cannot prove contiguity because the starting
/// sequence is request state, so this is kept as an explicit Rust boundary.
pub fn validate_read_from(value: &Value, from: u64) -> Result {
    validate("read", value)?;
    let mut expected = from;
    for item in array(value)? {
        let sequence = u64_string(field(object(item)?, "sequence")?)?;
        if sequence != expected {
            return Err("non-contiguous cursor");
        }
        expected = expected.checked_add(1).ok_or("non-contiguous cursor")?;
    }
    Ok(())
}

/// Validates a hosted read page and returns the next cursor for follow.
pub fn next_follow_cursor(value: &Value, from: u64) -> Result<u64> {
    validate_read_from(value, from)?;
    let Some(item) = array(value)?.last() else {
        return Ok(from);
    };
    let sequence = u64_string(field(object(item)?, "sequence")?)?;
    sequence.checked_add(1).ok_or("non-contiguous cursor")
}

/// Advances the cumulative response byte count while enforcing one canonical
/// bound for streamed hosted responses.
pub(crate) fn consume_response_bytes(total: u64, chunk: u64, maximum: u64) -> Result<u64> {
    let next = total.checked_add(chunk).ok_or("response_too_large")?;
    if next > maximum {
        return Err("response_too_large");
    }
    Ok(next)
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    #[test]
    fn endpoint_policy_is_shared_with_native_and_wasm_clients() {
        for endpoint in [
            "https://stream.example",
            "http://localhost:8080",
            "http://127.0.0.1:8080",
            "http://[::1]:8080",
        ] {
            assert!(super::validate_endpoint(endpoint).is_ok(), "{endpoint}");
        }
        for endpoint in [
            "http://stream.example",
            "https://user@stream.example",
            "https://stream.example/?query=1",
            "https://stream.example/#fragment",
            "http://127.0.0.1:",
        ] {
            assert!(super::validate_endpoint(endpoint).is_err(), "{endpoint}");
        }
    }

    #[test]
    fn base64_padding_only_terminates_the_last_quartet() {
        assert!(super::decode_base64("AA==AAAA").is_none());
        assert_eq!(super::decode_base64("AA=="), Some(vec![0]));
        assert_eq!(super::decode_base64("AAAA"), Some(vec![0, 0, 0]));
    }

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
        let bad_id = r#"{"source":"events","destination":"copy","forkedAt":"0","tail":"0","commitId":"AAAA"}"#;
        assert!(super::validate("fork", &json_fixture(bad_id)?).is_err());
        let bad_path = r#"[{"path":"events//child"}]"#;
        assert!(super::validate("children", &json_fixture(bad_path)?).is_err());
        assert!(super::validate("tail", &json_fixture("1")?).is_err());
        Ok(())
    }

    #[test]
    fn hosted_http_validation_accepts_empty_record_values() -> serde_json::Result<()> {
        let commit_id = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
        let response = format!(
            r#"[{{"sequence":"0","value":"","commitId":"{commit_id}","committedAtMicros":"1000"}}]"#
        );

        assert!(super::validate("read", &json_fixture(&response)?).is_ok());
        let missing_time = format!(r#"[{{"sequence":"0","value":"","commitId":"{commit_id}"}}]"#);
        assert!(super::validate("read", &json_fixture(&missing_time)?).is_err());
        Ok(())
    }

    #[test]
    fn hosted_http_read_validation_checks_request_cursor_contiguity() -> serde_json::Result<()> {
        let commit_id = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
        let response = format!(
            r#"[{{"sequence":"0","value":"","commitId":"{commit_id}","committedAtMicros":"1000"}},{{"sequence":"1","value":"","commitId":"{commit_id}","committedAtMicros":"1001"}}]"#
        );
        let value = json_fixture(&response)?;
        assert!(super::validate_read_from(&value, 0).is_ok());
        assert!(super::validate_read_from(&value, 1).is_err());
        let gap = format!(
            r#"[{{"sequence":"0","value":"","commitId":"{commit_id}","committedAtMicros":"1000"}},{{"sequence":"2","value":"","commitId":"{commit_id}","committedAtMicros":"1002"}}]"#
        );
        assert!(super::validate_read_from(&json_fixture(&gap)?, 0).is_err());
        Ok(())
    }

    #[test]
    fn follow_cursor_projection_is_request_relative() -> serde_json::Result<()> {
        let commit_id = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
        let response = format!(
            r#"[{{"sequence":"9","value":"","commitId":"{commit_id}","committedAtMicros":"1000"}},{{"sequence":"10","value":"","commitId":"{commit_id}","committedAtMicros":"1001"}}]"#
        );
        assert_eq!(
            super::next_follow_cursor(&json_fixture(&response)?, 9),
            Ok(11)
        );
        assert_eq!(
            super::next_follow_cursor(&serde_json::json!([]), 27),
            Ok(27)
        );
        assert!(super::next_follow_cursor(&json_fixture(&response)?, 8).is_err());
        Ok(())
    }

    #[test]
    fn cumulative_response_bytes_are_checked_in_rust() {
        assert_eq!(super::consume_response_bytes(4, 3, 8), Ok(7));
        assert_eq!(
            super::consume_response_bytes(7, 2, 8),
            Err("response_too_large")
        );
        assert_eq!(
            super::consume_response_bytes(u64::MAX, 1, u64::MAX),
            Err("response_too_large")
        );
    }

    #[test]
    fn hosted_http_validation_checks_children_page_continuations() -> serde_json::Result<()> {
        let commit_id = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
        let valid = format!(
            r#"{{"hierarchyVersion":"{commit_id}","children":[{{"path":"runs/a"}},{{"path":"runs/b"}}],"nextAfter":"runs/b"}}"#
        );
        assert!(super::validate("children/page", &json_fixture(&valid)?).is_ok());

        let null_continuation =
            format!(r#"{{"hierarchyVersion":"{commit_id}","children":[],"nextAfter":null}}"#);
        assert!(super::validate("children/page", &json_fixture(&null_continuation)?).is_ok());

        let wrong_continuation = format!(
            r#"{{"hierarchyVersion":"{commit_id}","children":[{{"path":"runs/a"}}],"nextAfter":"runs/b"}}"#
        );
        assert!(super::validate("children/page", &json_fixture(&wrong_continuation)?).is_err());

        let unordered = format!(
            r#"{{"hierarchyVersion":"{commit_id}","children":[{{"path":"runs/b"}},{{"path":"runs/a"}}]}}"#
        );
        assert!(super::validate("children/page", &json_fixture(&unordered)?).is_err());
        Ok(())
    }
}
