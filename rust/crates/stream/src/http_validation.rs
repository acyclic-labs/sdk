//! Pure validation for hosted Stream HTTP response payloads.

use base64::{Engine as _, engine::general_purpose::STANDARD};
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

fn validate_revision(value: &Value) -> Result {
    let item = object(value)?;
    bytes(field(item, "scope")?)?;
    if bytes(field(item, "incarnation")?)?.len() != 16 {
        return Err("invalid revision incarnation");
    }
    u64_string(field(item, "index")?)?;
    Ok(())
}

fn path(value: &Value) -> Result {
    let value = string(value)?;
    crate::StreamPath::new(value)
        .map(|_| ())
        .map_err(|_| "invalid path")
}

fn bytes(value: &Value) -> Result<Vec<u8>> {
    STANDARD
        .decode(encoded_bytes(value)?)
        .map_err(|_| "expected base64")
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
    let value = bytes(field(item, "value")?)?;
    let keys = array(field(item, "keys")?)?;
    let attributes = array(field(item, "attributes")?)?;
    if keys.len() > crate::MAX_RECORD_KEYS || attributes.len() > crate::MAX_RECORD_ATTRIBUTES {
        return Err("record header count exceeded");
    }
    let headers = crate::changes::record_headers(
        &value,
        keys.iter()
            .map(|key| bytes(key).map(Into::into))
            .collect::<Result<Vec<_>>>()?,
        attributes
            .iter()
            .map(|attribute| {
                let attribute = object(attribute)?;
                Ok(crate::Attribute {
                    name: string(field(attribute, "name")?)?.to_owned(),
                    value: bytes(field(attribute, "value")?)?.into(),
                })
            })
            .collect::<Result<Vec<_>>>()?,
    )
    .map_err(|_| "invalid record headers")?;
    if value.len().saturating_add(headers.payload_bytes()) > crate::MAX_RECORD_BYTES {
        return Err("record payload exceeded");
    }
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
        if let Some(record) = child.get("tailRecord") {
            validate_record(record)?;
        }
        if let Some(origin) = child.get("origin") {
            validate_origin(origin)?;
        }
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

fn validate_origin(value: &Value) -> Result<u64> {
    let origin = object(value)?;
    path(field(origin, "source")?)?;
    id(field(origin, "commitId")?)?;
    u64_string(field(origin, "forkedAt")?)
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

fn validate_commit_result(value: &Value) -> Result {
    let item = object(value)?;
    if boolean(field(item, "ok")?)? {
        let commit = field(item, "commitId")?;
        id(commit)?;
        u64_string(field(item, "committedAtMicros")?)?;
        let appends = array(field(item, "appends")?)?;
        let forks = array(field(item, "forks")?)?;
        if appends.len().saturating_add(forks.len()) == 0
            || appends.len().saturating_add(forks.len()) > crate::MAX_ITEMS
        {
            return Err("invalid receipt size");
        }
        for range in appends {
            let range = object(range)?;
            path(field(range, "path")?)?;
            let start = u64_string(field(range, "start")?)?;
            let tail = u64_string(field(range, "tail")?)?;
            if start >= tail || tail - start > crate::MAX_ITEMS as u64 {
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
            let excluded = array(field(item, "exclude")?)?;
            if excluded.len() > crate::MAX_ITEMS {
                return Err("too many exclusions");
            }
            for excluded in excluded {
                path(excluded)?;
            }
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
    u64_string(field(item, "committedAtMicros")?)?;
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
        "commit" => validate_envelope(field(outcome, "outcome")?),
        _ => Err("invalid idempotency outcome type"),
    }
}

pub fn validate(route: &str, value: &Value) -> Result {
    match crate::HTTP_RESPONSE_CONTRACT
        .iter()
        .find_map(|(candidate, kind)| (*candidate == route).then_some(*kind))
    {
        Some("empty") => {
            if object(value)?.is_empty() {
                Ok(())
            } else {
                Err("expected empty acknowledgement response")
            }
        }
        Some("heads") => {
            let item = object(value)?;
            for record in array(field(item, "retained")?)? {
                if !record.is_null() {
                    validate_record(record)?;
                }
            }
            validate_revision(field(item, "snapshot")?)?;
            let projection = string(field(item, "projection")?)?;
            if !matches!(projection, "history" | "watermarks" | "tail") {
                return Err("invalid head projection");
            }
            for head in array(field(item, "heads")?)? {
                if !head.is_null() {
                    let item = object(head)?;
                    if let Some(through) =
                        item.get("retainedThrough").filter(|value| !value.is_null())
                    {
                        if projection == "tail"
                            || u64_string(through)? > u64_string(field(item, "tail")?)?
                        {
                            return Err("invalid retained prefix proof");
                        }
                    }
                    if projection == "history" {
                        validate("head", head)?;
                    } else {
                        let head = object(head)?;
                        if (projection == "tail" && u64_string(field(head, "durable")?)? != 0)
                            || u64_string(field(head, "durable")?)?
                                > u64_string(field(head, "tail")?)?
                            || !field(head, "last")?.is_null()
                            || !field(head, "origin")?.is_null()
                        {
                            return Err("invalid watermark projection");
                        }
                    }
                }
            }
            if let Some(page) = item.get("children").filter(|value| !value.is_null()) {
                validate_children_page(page)?;
            }
            Ok(())
        }
        Some("head") => {
            let item = object(value)?;
            let tail = u64_string(field(item, "tail")?)?;
            if let Some(through) = item.get("retainedThrough").filter(|value| !value.is_null()) {
                if u64_string(through)? > tail {
                    return Err("retained prefix exceeds head");
                }
            }
            if u64_string(field(item, "durable")?)? > tail {
                return Err("durable prefix exceeds accepted head");
            }
            let last = field(item, "last")?;
            if last.is_null() {
                if tail != 0 {
                    return Err("nonempty head is missing its last record");
                }
            } else {
                validate_record(last)?;
                if u64_string(field(object(last)?, "sequence")?)?.checked_add(1) != Some(tail) {
                    return Err("last record does not match head");
                }
            }
            let origin = field(item, "origin")?;
            if !origin.is_null() {
                if validate_origin(origin)? > tail {
                    return Err("inherited cut exceeds head");
                }
            }
            Ok(())
        }
        Some("tail") => {
            let item = object(value)?;
            u64_string(field(item, "tail")?)?;
            // The applied revision the host served, when it tracks revisions.
            match item.get("applied") {
                None | Some(Value::Null) => Ok(()),
                Some(applied) => validate_revision(applied),
            }
        }
        Some("append") => validate_append_result(value),
        Some("fork") => validate_fork_receipt(value),
        Some("read") => {
            let item = object(value)?;
            for record in array(field(item, "records")?)? {
                validate_record(record)?;
            }
            // The applied revision the host served the page at, when it
            // tracks revisions: a later read on a lagging replica floors here.
            match item.get("applied") {
                None | Some(Value::Null) => Ok(()),
                Some(applied) => validate_revision(applied),
            }
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
        let record = format!(
            r#"{{"sequence":"0","value":"","commitId":"{commit_id}","committedAtMicros":"1000","keys":[],"attributes":[]}}"#
        );
        let response = format!(r#"{{"records":[{record}]}}"#);

        assert!(super::validate("read", &json_fixture(&response)?).is_ok());
        // Negative control: the pre-revision bare record array is refused.
        assert!(super::validate("read", &json_fixture(&format!("[{record}]"))?).is_err());
        let missing_time =
            format!(r#"{{"records":[{{"sequence":"0","value":"","commitId":"{commit_id}"}}]}}"#);
        assert!(super::validate("read", &json_fixture(&missing_time)?).is_err());
        Ok(())
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
