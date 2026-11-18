//! Pure validation for hosted Stream HTTP response payloads.

use crate::http_fields::{
    array, bytes, field, id, key, object, path, string, u64_string, validate_append_result,
    validate_commit_result, validate_fork_receipt, validate_origin, validate_revision,
};
use crate::{
    changes::{RECORD_FORMAT_ATTRIBUTE, V1, WRITER_FORMAT, visit_v1, visit_v2},
    headers::HeaderValidation,
    wire::StreamLimit,
};
use serde_json::Value;

type Result<T = ()> = std::result::Result<T, &'static str>;

fn validate_record_body(value: &Value) -> Result {
    let item = object(value)?;
    u64_string(field(item, "sequence")?)?;
    let value = bytes(field(item, "value")?)?;
    let keys = array(field(item, "keys")?)?;
    let attributes = array(field(item, "attributes")?)?;
    let keys = keys.iter().map(bytes).collect::<Result<Vec<_>>>()?;
    let attributes = attributes
        .iter()
        .map(|attribute| {
            let attribute = object(attribute)?;
            Ok((
                string(field(attribute, "name")?)?,
                bytes(field(attribute, "value")?)?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    validate_record_payload(&value, &keys, &attributes)?;
    id(field(item, "commitId")?)?;
    u64_string(field(item, "committedAtMicros")?)?;
    Ok(())
}

fn validate_record_payload(
    body: &[u8],
    keys: &[Vec<u8>],
    attributes: &[(&str, Vec<u8>)],
) -> Result {
    const MAXIMUM: usize = StreamLimit::MaxRecordBytes as usize;
    const INVALID_HEADERS: &str = "invalid record headers";
    let mut validation = HeaderValidation::default();
    for key in keys {
        validation.key(key).map_err(|_| INVALID_HEADERS)?;
    }
    for (name, value) in attributes {
        validation.attribute(name, value);
    }
    let payload = body.len().saturating_add(validation.payload_bytes());
    validation.finish(MAXIMUM).map_err(|_| INVALID_HEADERS)?;
    let marker = attributes
        .iter()
        .find(|(name, _)| *name == RECORD_FORMAT_ATTRIBUTE)
        .map(|(_, value)| value.as_slice());
    match marker {
        Some(WRITER_FORMAT) if keys.is_empty() => {
            let remaining = MAXIMUM.checked_sub(payload).ok_or(INVALID_HEADERS)?;
            visit_v2::<Vec<u8>>(body, None, Some(remaining), |_, _, _| Ok(()))
                .map_err(|_| INVALID_HEADERS)?;
        }
        Some(V1) if payload <= MAXIMUM => {
            visit_v1(body, Some(keys), |_, _, _| Ok(())).map_err(|_| INVALID_HEADERS)?;
        }
        Some(_) => return Err(INVALID_HEADERS),
        None if payload > MAXIMUM => return Err("record payload exceeded"),
        None => {}
    }
    Ok(())
}

fn validate_record(value: &Value) -> Result {
    validate_record_body(value)?;
    path(field(object(value)?, "origin")?)
}

fn validate_owned_record(value: &Value) -> Result {
    validate_record_body(value)?;
    if object(value)?.contains_key("origin") {
        return Err("owned record must elide its origin");
    }
    Ok(())
}

fn validate_children_page(value: &Value) -> Result {
    let item = object(value)?;
    id(field(item, "hierarchyVersion")?)?;
    let children = array(field(item, "children")?)?;
    let scanned = field(item, "scanned")?
        .as_u64()
        .ok_or("invalid scanned count")?;
    if scanned > crate::wire::StreamLimit::MaxItems as u64 || children.len() as u64 > scanned {
        return Err("invalid scanned count");
    }
    let mut previous: Option<&str> = None;
    for child in children {
        let child = object(child)?;
        let tail = child.get("tail").map(u64_string).transpose()?;
        let sequence = child
            .get("tailRecord")
            .map(|record| u64_string(field(object(record)?, "sequence")?))
            .transpose()?;
        if sequence != tail.and_then(|tail| tail.checked_sub(1)) {
            return Err("child tail does not match its record");
        }
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
        path(&Value::String(next_after.into()))?;
        if scanned == 0 || previous.is_some_and(|last| last > next_after) {
            return Err("child continuation precedes consumed rows");
        }
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
                validate_owned_record(record)?;
            }
        }
        "fork" => {
            path(field(item, "source")?)?;
            path(field(item, "destination")?)?;
            let excluded = array(field(item, "exclude")?)?;
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
                    validate_owned_record(record)?;
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

/// Validates one hosted response against its declared route.
///
/// # Errors
/// Returns the wire rejection when the response shape or contents are invalid.
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
            for observation in array(field(item, "retained")?)? {
                let observation = object(observation)?;
                path(field(observation, "path")?)?;
                let record = field(observation, "record")?;
                if !record.is_null() {
                    validate_record_body(record)?;
                    if let Some(origin) = object(record)?.get("origin") {
                        path(origin)?;
                        if string(origin)? == string(field(observation, "path")?)? {
                            return Err("selected record must elide its origin");
                        }
                    }
                }
            }
            validate_revision(field(item, "snapshot")?)?;
            validate_revision(field(item, "applied")?)?;
            let projection = string(field(item, "projection")?)?;
            if !matches!(projection, "history" | "watermarks" | "tail") {
                return Err("invalid head projection");
            }
            for head in array(field(item, "heads")?)? {
                if !head.is_null() {
                    let item = object(head)?;
                    u64_string(field(item, "changedAt")?)?;
                    if let Some(through) =
                        item.get("retainedThrough").filter(|value| !value.is_null())
                        && (projection == "tail"
                            || u64_string(through)? > u64_string(field(item, "tail")?)?)
                    {
                        return Err("invalid retained prefix proof");
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
            u64_string(field(item, "changedAt")?)?;
            let tail = u64_string(field(item, "tail")?)?;
            if let Some(through) = item.get("retainedThrough").filter(|value| !value.is_null())
                && u64_string(through)? > tail
            {
                return Err("retained prefix exceeds head");
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
            if !origin.is_null() && validate_origin(origin)? > tail {
                return Err("inherited cut exceeds head");
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
                validate_record_body(record)?;
                // The request's followed path supplies an omitted origin.
                // The typed projection checks that contextual spelling.
                if let Some(origin) = object(record)?.get("origin") {
                    path(origin)?;
                }
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
    use crate::{
        changes::{
            ChangeDecodeError, RECORD_FORMAT_ATTRIBUTE, V1, WRITER_FORMAT, visit_v1, visit_v2,
        },
        headers::HeaderValidation,
        wire::StreamLimit,
    };
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
            r#"{{"hierarchyVersion":"{commit_id}","children":[{{"path":"runs/a"}},{{"path":"runs/b"}}],"scanned":2,"nextAfter":"runs/b"}}"#
        );
        assert!(super::validate("children/page", &json_fixture(&valid)?).is_ok());

        let hidden = format!(
            r#"{{"hierarchyVersion":"{commit_id}","children":[],"scanned":2,"nextAfter":"runs/b"}}"#
        );
        assert!(super::validate("children/page", &json_fixture(&hidden)?).is_ok());

        let null_continuation = format!(
            r#"{{"hierarchyVersion":"{commit_id}","children":[],"scanned":0,"nextAfter":null}}"#
        );
        assert!(super::validate("children/page", &json_fixture(&null_continuation)?).is_ok());

        let wrong_continuation = format!(
            r#"{{"hierarchyVersion":"{commit_id}","children":[{{"path":"runs/a"}}],"scanned":1,"nextAfter":"runs/0"}}"#
        );
        assert!(super::validate("children/page", &json_fixture(&wrong_continuation)?).is_err());

        let unordered = format!(
            r#"{{"hierarchyVersion":"{commit_id}","children":[{{"path":"runs/b"}},{{"path":"runs/a"}}],"scanned":2}}"#
        );
        assert!(super::validate("children/page", &json_fixture(&unordered)?).is_err());
        Ok(())
    }
}
