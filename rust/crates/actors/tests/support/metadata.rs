//! Independent metadata admission oracle; no production validator is reused.
use acyclic_actors::guest::{AdmittedCaller, CallerCapability, RuntimeHandle};
use serde_json::Value;

// Serde struct decoding accepts both named maps and positional sequences.
fn field<'a>(value: &'a Value, name: &str, index: usize) -> Option<&'a Value> {
    match value {
        Value::Array(items) => items.get(index),
        _ => value.get(name),
    }
}

pub fn text(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 1024
        && value.trim() == value
        && !value.chars().any(char::is_control)
}
fn handle(value: &Value) -> bool {
    field(value, "version", 0)
        .and_then(Value::as_u64)
        .is_some_and(|v| v > 0 && v <= u64::from(u32::MAX))
        && field(value, "handle", 1)
            .and_then(Value::as_str)
            .is_some_and(text)
}
fn caller(value: &Value) -> bool {
    ["accountId", "orgId", "principal", "credentialId", "actorId"]
        .iter()
        .enumerate()
        .all(|(index, name)| {
            field(value, name, index)
                .and_then(Value::as_str)
                .is_some_and(text)
        })
        && field(value, "expiresAtMs", 5)
            .and_then(Value::as_str)
            .is_some_and(|raw| {
                raw.len() <= 20
                    && raw
                        .parse::<u64>()
                        .is_ok_and(|n| n > 0 && n.to_string() == raw)
            })
        && field(value, "capability", 6).is_none_or(|v| v.is_null() || handle(v))
}

pub fn check(input: &[u8]) {
    let Ok(value) = serde_json::from_slice::<Value>(input) else {
        return;
    };
    if let (Some(version), Some(raw)) = (
        field(&value, "version", 0)
            .and_then(Value::as_u64)
            .and_then(|v| u32::try_from(v).ok()),
        field(&value, "handle", 1).and_then(Value::as_str),
    ) {
        let expected = version != 0 && text(raw);
        assert_eq!(
            CallerCapability::new(version, raw.into()).is_ok(),
            expected,
            "metadata constructor admission"
        );
        assert_eq!(
            RuntimeHandle::new(version, raw.into()).is_ok(),
            expected,
            "metadata constructor admission"
        );
    }
    let fields = [
        ("accountId", 0),
        ("orgId", 1),
        ("principal", 2),
        ("credentialId", 3),
        ("actorId", 4),
    ]
    .map(|(name, index)| field(&value, name, index).and_then(Value::as_str));
    if let [
        Some(account),
        Some(org),
        Some(principal),
        Some(credential),
        Some(actor),
    ] = fields
    {
        if let Some(expiry) = field(&value, "expiresAtMs", 5)
            .and_then(Value::as_str)
            .filter(|raw| raw.len() <= 20)
            .and_then(|raw| raw.parse::<u64>().ok())
        {
            let expected = expiry > 0
                && [account, org, principal, credential, actor]
                    .into_iter()
                    .all(text);
            assert_eq!(
                AdmittedCaller::new(
                    account.into(),
                    org.into(),
                    principal.into(),
                    credential.into(),
                    actor.into(),
                    expiry
                )
                .is_ok(),
                expected,
                "metadata constructor admission"
            );
        }
    }
    // One-way implication preserves legitimate duplicate-field rejection that Value loses.
    if serde_json::from_slice::<CallerCapability>(input).is_ok() {
        assert!(handle(&value), "metadata deserialization admission");
    }
    if serde_json::from_slice::<RuntimeHandle>(input).is_ok() {
        assert!(handle(&value), "metadata deserialization admission");
    }
    if serde_json::from_slice::<AdmittedCaller>(input).is_ok() {
        assert!(caller(&value), "metadata deserialization admission");
    }
}
