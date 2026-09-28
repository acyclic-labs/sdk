//! Validation for the hosted Machines HTTP DTO boundary.
//!
//! The HTTP transport still owns fetching, byte bounds, and the JSON wrapper
//! codec.  This module owns the response contract so the TypeScript adapter
//! does not maintain a second, subtly different schema implementation.

use js_sys::{Array, BigInt, Map as JsMap, Object, Reflect, Uint8Array};
use serde_json::Value;
use wasm_bindgen::{JsCast, prelude::*};

use crate::http_route;

const CAPABILITIES: &[&str] = &[
    "elastic-cpu",
    "elastic-memory",
    "live-checkpoint",
    "live-fork",
    "disk-fork",
    "suspend-resume",
    "live-movement",
];
const STATES: &[&str] = &[
    "starting",
    "running",
    "suspending",
    "suspended",
    "waking",
    "destroying",
    "destroyed",
    "failed",
    "indeterminate",
];
const EVENTS: &[&str] = &["state", "pressure", "capacity-changed"];
const PRESSURES: &[&str] = &["customer-budget", "machine-limit", "service-saturation"];
const OPERATION_PHASES: &[&str] = &[
    "pending",
    "succeeded",
    "cancelled",
    "indeterminate",
    "failed",
];

fn error(message: impl Into<String>) -> JsValue {
    JsValue::from_str(&message.into())
}
fn object<'a>(value: &'a Value, name: &str) -> Result<&'a serde_json::Map<String, Value>, JsValue> {
    value
        .as_object()
        .ok_or_else(|| error(format!("{name} must be an object")))
}
fn field<'a>(value: &'a Value, name: &str) -> Result<&'a Value, JsValue> {
    object(value, "response")?
        .get(name)
        .ok_or_else(|| error(format!("{name} is required")))
}
fn text(value: &Value, name: &str) -> Result<String, JsValue> {
    let value = value
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| error(format!("{name} must be a non-empty string")))?;
    Ok(value.to_owned())
}
fn bool_value(value: &Value, name: &str) -> Result<(), JsValue> {
    value
        .as_bool()
        .ok_or_else(|| error(format!("{name} must be boolean")))
        .map(|_| ())
}
fn integer(value: &Value, name: &str) -> Result<u64, JsValue> {
    let value = value
        .as_u64()
        .ok_or_else(|| error(format!("{name} must be a safe integer")))?;
    if !safe_u64(value) {
        return Err(error(format!("{name} must be a safe integer")));
    }
    Ok(value)
}
fn safe_u64(value: u64) -> bool {
    value <= 9_007_199_254_740_991
}
fn big(value: &Value, name: &str) -> Result<u64, JsValue> {
    if value
        .as_object()
        .and_then(|value| value.get("$bigint"))
        .and_then(Value::as_str)
        .is_some_and(|value| value.starts_with('-'))
    {
        return Err(error(format!("{name} cannot be negative")));
    }
    big_payload(value).ok_or_else(|| error(format!("{name} must be bigint")))
}
fn big_payload(value: &Value) -> Option<u64> {
    value.as_object()?.get("$bigint")?.as_str()?.parse().ok()
}
fn positive(value: &Value, name: &str) -> Result<u64, JsValue> {
    let value = integer(value, name)?;
    if value == 0 {
        return Err(error(format!("{name} must be positive")));
    }
    Ok(value)
}
fn one_of(value: &Value, values: &[&str], name: &str) -> Result<String, JsValue> {
    let value = text(value, name)?;
    if !values.contains(&value.as_str()) {
        return Err(error(format!("{name} is invalid")));
    }
    Ok(value)
}
fn digest(value: &Value, name: &str) -> Result<(), JsValue> {
    let value = text(value, name)?;
    if !valid_digest(&value) {
        return Err(error(format!(
            "{name} must be a non-zero lowercase SHA-256 digest"
        )));
    }
    Ok(())
}
fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        && value.bytes().any(|byte| byte != b'0')
}
fn unique_strings(values: &[Value], name: &str) -> Result<(), JsValue> {
    let mut seen = std::collections::BTreeSet::new();
    for value in values {
        if !seen.insert(text(value, name)?) {
            return Err(error(format!("{name} contains duplicates")));
        }
    }
    Ok(())
}
fn array<'a>(value: &'a Value, name: &str) -> Result<&'a Vec<Value>, JsValue> {
    value
        .as_array()
        .ok_or_else(|| error(format!("{name} must be an array")))
}
fn bytes(value: &Value, name: &str) -> Result<(), JsValue> {
    if bytes_payload(value).is_none() {
        return Err(error(format!("{name} must be bytes")));
    }
    Ok(())
}
fn bytes_payload(value: &Value) -> Option<Vec<u8>> {
    decode_base64(value.as_object()?.get("$bytes")?.as_str()?)
}
fn decode_base64(value: &str) -> Option<Vec<u8>> {
    if !value.len().is_multiple_of(4)
        || value
            .bytes()
            .any(|byte| !byte.is_ascii_alphanumeric() && !b"+/=".contains(&byte))
    {
        return None;
    }
    let mut output = Vec::new();
    let bytes = value.as_bytes();
    for (index, chunk) in bytes.chunks_exact(4).enumerate() {
        let padding = chunk.iter().rev().take_while(|byte| **byte == b'=').count();
        if padding > 2
            || (padding > 0 && index + 1 != bytes.len() / 4)
            || chunk.iter().take(4 - padding).any(|byte| *byte == b'=')
        {
            return None;
        }
        let mut n = 0_u32;
        for byte in chunk {
            n <<= 6;
            n |= match *byte {
                b'A'..=b'Z' => u32::from(byte - b'A'),
                b'a'..=b'z' => u32::from(byte - b'a' + 26),
                b'0'..=b'9' => u32::from(byte - b'0' + 52),
                b'+' => 62,
                b'/' => 63,
                b'=' => 0,
                _ => return None,
            };
        }
        if (padding == 1 && n & 0xc0 != 0) || (padding == 2 && n & 0xf000 != 0) {
            return None;
        }
        output.push(u8::try_from(n >> 16).ok()?);
        if padding < 2 {
            output.push(u8::try_from((n >> 8) & 0xff).ok()?);
        }
        if padding == 0 {
            output.push(u8::try_from(n & 0xff).ok()?);
        }
    }
    Some(output)
}

fn image(value: &Value) -> Result<(), JsValue> {
    let kind = one_of(
        field(value, "kind")?,
        &["managed-oci", "custom", "checkpoint"],
        "image.kind",
    )?;
    if kind == "checkpoint" {
        text(field(value, "checkpointId")?, "checkpointId")?;
    } else {
        digest(field(value, "digestHex")?, "digestHex")?;
    }
    Ok(())
}
fn suspension(value: &Value) -> Result<(), JsValue> {
    let kind = one_of(
        field(value, "kind")?,
        &["manual", "after-idle"],
        "suspension.kind",
    )?;
    if kind == "after-idle" {
        positive(field(value, "milliseconds")?, "milliseconds")?;
    }
    Ok(())
}
fn contract(value: &Value) -> Result<(), JsValue> {
    image(field(value, "image")?)?;
    let capabilities = array(field(value, "capabilities")?, "capabilities")?;
    for value in capabilities {
        one_of(value, CAPABILITIES, "capability")?;
    }
    unique_strings(capabilities, "capabilities")?;
    let compatibility = field(value, "compatibility")?;
    let kind = one_of(
        field(compatibility, "kind")?,
        &["best-effort", "require"],
        "compatibility.kind",
    )?;
    if kind == "require" {
        let required = array(
            field(compatibility, "capabilities")?,
            "required capabilities",
        )?;
        if required.is_empty() {
            return Err(error("compatibility policy is contradictory"));
        }
        unique_strings(required, "required capabilities")?;
        for value in required {
            let capability = text(value, "capability")?;
            if !capabilities
                .iter()
                .any(|item| item.as_str() == Some(capability.as_str()))
            {
                return Err(error("compatibility policy is contradictory"));
            }
        }
    } else if let Some(value) = object(compatibility, "compatibility")?.get("capabilities")
        && !array(value, "compatibility.capabilities")?.is_empty()
    {
        return Err(error("compatibility policy is contradictory"));
    }
    one_of(
        field(value, "performance")?,
        &["elastic", "dedicated"],
        "performance",
    )?;
    suspension(field(value, "suspension")?)?;
    let expiration = field(value, "expiration")?;
    let expiration_kind = one_of(
        field(expiration, "kind")?,
        &["never", "max-age", "at", "idle"],
        "expiration.kind",
    )?;
    if expiration_kind != "never" {
        positive(
            field(expiration, "milliseconds")?,
            "expiration.milliseconds",
        )?;
    }
    digest(
        field(value, "networkPolicyDigestHex")?,
        "networkPolicyDigestHex",
    )?;
    let budgets = field(value, "budgets")?;
    big(field(budgets, "spendMicros")?, "spendMicros")?;
    integer(field(budgets, "concurrency")?, "concurrency")?;
    digest(
        field(value, "compatibilityRevisionHex")?,
        "compatibilityRevisionHex",
    )
}
fn machine(value: &Value) -> Result<(), JsValue> {
    text(field(value, "id")?, "machine.id")?;
    one_of(field(value, "state")?, STATES, "machine.state")?;
    contract(field(value, "contract")?)?;
    let endpoints = array(field(value, "endpoints")?, "endpoints")?;
    let mut names = std::collections::BTreeSet::new();
    for endpoint in endpoints {
        let name = text(field(endpoint, "name")?, "endpoint.name")?;
        text(field(endpoint, "uri")?, "endpoint.uri")?;
        if !names.insert(name) {
            return Err(error("machine endpoints contain duplicate names"));
        }
    }
    let created = positive(field(value, "createdAtUnixMs")?, "createdAtUnixMs")?;
    let changed = positive(field(value, "changedAtUnixMs")?, "changedAtUnixMs")?;
    if changed < created {
        return Err(error("machine timestamps are reversed"));
    }
    let last_checkpoint = field(value, "lastCheckpoint")?;
    if !last_checkpoint.is_null() {
        text(last_checkpoint, "lastCheckpoint")?;
    }
    Ok(())
}
fn checkpoint(value: &Value) -> Result<(), JsValue> {
    text(field(value, "id")?, "checkpoint.id")?;
    text(field(value, "source")?, "checkpoint.source")?;
    contract(field(value, "contract")?)?;
    bool_value(field(value, "forkable")?, "forkable")?;
    positive(field(value, "createdAtUnixMs")?, "createdAtUnixMs")?;
    Ok(())
}
fn mutation(value: &Value) -> Result<(), JsValue> {
    match text(field(value, "kind")?, "mutation.kind")?.as_str() {
        "created" => machine(field(value, "machine")?),
        "checkpointed" => checkpoint(field(value, "checkpoint")?),
        "forked" => {
            for value in array(field(value, "machines")?, "machines")? {
                machine(value)?;
            }
            Ok(())
        }
        "machine-forked" => {
            text(field(value, "source")?, "source")?;
            one_of(
                field(value, "fidelity")?,
                &["memory-and-disk", "disk-only"],
                "fidelity",
            )?;
            for value in array(field(value, "children")?, "children")? {
                machine(value)?;
            }
            Ok(())
        }
        "suspension-policy-set" => {
            text(field(value, "machineId")?, "machineId")?;
            suspension(field(value, "policy")?)
        }
        "suspended" | "woken" | "machine-destroyed" => {
            text(field(value, "machineId")?, "machineId")?;
            Ok(())
        }
        "checkpoint-destroyed" => {
            text(field(value, "checkpointId")?, "checkpointId")?;
            Ok(())
        }
        _ => Err(error("mutation kind is invalid")),
    }
}

fn mutation_kind(value: &Value) -> Result<String, JsValue> {
    text(field(value, "kind")?, "mutation.kind")
}

fn require_mutation_kind(value: &Value, expected: &str) -> Result<(), JsValue> {
    if mutation_kind(value)? != expected {
        return Err(error(format!(
            "mutation kind is invalid for this route; expected {expected}"
        )));
    }
    Ok(())
}
fn operation(value: &Value) -> Result<(), JsValue> {
    text(field(value, "id")?, "operation.id")?;
    one_of(field(value, "phase")?, OPERATION_PHASES, "operation.phase")?;
    Ok(())
}
fn event(value: &Value) -> Result<(String, u64), JsValue> {
    let machine = text(field(value, "machine")?, "event.machine")?;
    let sequence = positive(field(value, "sequence")?, "event.sequence")?;
    positive(field(value, "observedAtUnixMs")?, "observedAtUnixMs")?;
    let fact = field(value, "fact")?;
    match one_of(field(fact, "kind")?, EVENTS, "event.fact.kind")?.as_str() {
        "state" => {
            one_of(field(fact, "state")?, STATES, "event.state")?;
        }
        "pressure" => {
            one_of(field(fact, "pressure")?, PRESSURES, "event.pressure")?;
        }
        "capacity-changed" => {}
        _ => unreachable!(),
    }
    Ok((machine, sequence))
}
fn usage(value: &Value) -> Result<(), JsValue> {
    text(field(value, "machine")?, "usage.machine")?;
    integer(field(value, "startUnixMs")?, "startUnixMs")?;
    integer(field(value, "endUnixMs")?, "endUnixMs")?;
    for name in [
        "elasticCpuNs",
        "dedicatedCpuNs",
        "privateResidentByteSeconds",
        "durablePrivateBytes",
        "egressBytes",
    ] {
        big(field(value, name)?, name)?;
    }
    bytes(
        field(value, "lineageReceiptSha256")?,
        "lineageReceiptSha256",
    )?;
    bytes(field(value, "receipt")?, "receipt")
}

fn expected_text(expected: &Value, name: &str) -> Result<String, JsValue> {
    text(field(expected, name)?, name)
}
fn same(value: &Value, expected: &Value, field_name: &str, label: &str) -> Result<(), JsValue> {
    same_text(value, field_name, expected, label, label)
}
fn same_text(
    value: &Value,
    value_field: &str,
    expected: &Value,
    expected_field: &str,
    label: &str,
) -> Result<(), JsValue> {
    if text(field(value, value_field)?, value_field)?
        != text(field(expected, expected_field)?, expected_field)?
    {
        return Err(error(format!("{label} identity was substituted")));
    }
    Ok(())
}

fn same_value(
    value: &Value,
    value_field: &str,
    expected: &Value,
    expected_field: &str,
    label: &str,
) -> Result<(), JsValue> {
    if field(value, value_field)? != field(expected, expected_field)? {
        return Err(error(format!("{label} was substituted")));
    }
    Ok(())
}

fn same_count(values: &Value, values_field: &str, expected: &Value) -> Result<(), JsValue> {
    let values = array(field(values, values_field)?, values_field)?;
    let expected = positive(field(expected, "count")?, "count")?;
    if u64::try_from(values.len()).ok() != Some(expected) {
        return Err(error("mutation child count was substituted"));
    }
    Ok(())
}

fn bind_created_machine(value: &Value, expected: &Value) -> Result<(), JsValue> {
    let machine = field(value, "machine")?;
    let contract = field(machine, "contract")?;
    for (name, label) in [
        ("image", "image"),
        ("compatibility", "compatibility policy"),
        ("performance", "performance policy"),
        ("suspension", "suspension policy"),
        ("expiration", "expiration policy"),
        ("networkPolicyDigestHex", "network policy"),
        ("budgets", "budgets"),
    ] {
        same_value(contract, name, expected, name, label)?;
    }
    Ok(())
}

fn same_performance(machine: &Value, expected: &Value, label: &str) -> Result<(), JsValue> {
    if one_of(
        field(field(machine, "contract")?, "performance")?,
        &["elastic", "dedicated"],
        "machine performance",
    )? != one_of(
        field(expected, "performance")?,
        &["elastic", "dedicated"],
        "performance",
    )? {
        return Err(error(format!("{label} performance was substituted")));
    }
    Ok(())
}

fn bind_checkpoint_child(machine: &Value, expected: &Value) -> Result<(), JsValue> {
    let checkpoint_id = expected_text(expected, "checkpointId")?;
    same_text(
        machine,
        "lastCheckpoint",
        expected,
        "checkpointId",
        "checkpoint child checkpoint",
    )?;
    let image = field(field(machine, "contract")?, "image")?;
    if one_of(
        field(image, "kind")?,
        &["checkpoint"],
        "checkpoint child image.kind",
    )? != "checkpoint"
    {
        return Err(error("checkpoint child image was substituted"));
    }
    if text(field(image, "checkpointId")?, "checkpointId")? != checkpoint_id {
        return Err(error("checkpoint child image was substituted"));
    }
    same_performance(machine, expected, "checkpoint child")
}

fn bind_machine_fork_children(
    value: &Value,
    expected: &Value,
    fidelity: &str,
) -> Result<(), JsValue> {
    let source = expected_text(expected, "machineId")?;
    let mut ids = std::collections::BTreeSet::new();
    for child in array(field(value, "children")?, "children")? {
        machine(child)?;
        let id = text(field(child, "id")?, "machine.id")?;
        if id == source || !ids.insert(id) {
            return Err(error("machine fork returned duplicate or source children"));
        }
        if one_of(field(child, "state")?, STATES, "machine.state")? != "running" {
            return Err(error("machine fork child is not running"));
        }
        if !field(child, "lastCheckpoint")?.is_null() {
            return Err(error("machine fork child retained a checkpoint"));
        }
        let capabilities = array(
            field(field(child, "contract")?, "capabilities")?,
            "capabilities",
        )?;
        let has_live_fork = capabilities
            .iter()
            .any(|value| value.as_str() == Some("live-fork"));
        let has_disk_fork = capabilities
            .iter()
            .any(|value| value.as_str() == Some("disk-fork"));
        let consistent = match fidelity {
            "memory-and-disk" => has_live_fork,
            "disk-only" => has_disk_fork && !has_live_fork,
            _ => false,
        };
        if !consistent {
            return Err(error("machine fork child capabilities contradict fidelity"));
        }
    }
    Ok(())
}

fn bind_mutation(route: &str, value: &Value, expected: &Value) -> Result<(), JsValue> {
    match route {
        http_route::MACHINES_CREATE => {
            require_mutation_kind(value, "created")?;
            bind_created_machine(value, expected)
        }
        http_route::MACHINES_CHECKPOINT => {
            require_mutation_kind(value, "checkpointed")?;
            same_text(
                field(value, "checkpoint")?,
                "source",
                expected,
                "machineId",
                "checkpoint source",
            )
        }
        http_route::MACHINES_FORK => {
            require_mutation_kind(value, "machine-forked")?;
            same_text(
                value,
                "source",
                expected,
                "machineId",
                "machine fork source",
            )?;
            same_count(value, "children", expected)?;
            let fidelity = one_of(
                field(value, "fidelity")?,
                &["memory-and-disk", "disk-only"],
                "fidelity",
            )?;
            bind_machine_fork_children(value, expected, &fidelity)
        }
        http_route::CHECKPOINTS_FORK => {
            require_mutation_kind(value, "forked")?;
            same_count(value, "machines", expected)?;
            for child in array(field(value, "machines")?, "machines")? {
                bind_checkpoint_child(child, expected)?;
            }
            Ok(())
        }
        http_route::MACHINES_SUSPEND => {
            require_mutation_kind(value, "suspended")?;
            same_text(
                value,
                "machineId",
                expected,
                "machineId",
                "suspended machine",
            )
        }
        http_route::MACHINES_WAKE => {
            require_mutation_kind(value, "woken")?;
            same_text(value, "machineId", expected, "machineId", "woken machine")
        }
        http_route::MACHINES_SUSPENSION_POLICY => {
            require_mutation_kind(value, "suspension-policy-set")?;
            same_text(
                value,
                "machineId",
                expected,
                "machineId",
                "suspension-policy machine",
            )?;
            same_value(value, "policy", expected, "policy", "suspension policy")
        }
        http_route::MACHINES_DESTROY => {
            require_mutation_kind(value, "machine-destroyed")?;
            same_text(
                value,
                "machineId",
                expected,
                "machineId",
                "destroyed machine",
            )
        }
        http_route::CHECKPOINTS_DESTROY => {
            require_mutation_kind(value, "checkpoint-destroyed")?;
            same_text(
                value,
                "checkpointId",
                expected,
                "checkpointId",
                "destroyed checkpoint",
            )
        }
        http_route::OPERATIONS_RECOVER => Ok(()),
        _ => unreachable!("mutation binding called for a non-mutation route"),
    }
}

/// Validate a decoded hosted HTTP response. `expected` is the request object
/// captured by the TypeScript adapter before the fetch; IDs remain arbitrary
/// HTTP strings and are never passed through simulator UUID normalization.
#[allow(
    clippy::too_many_lines,
    reason = "the hosted HTTP route table intentionally centralizes the wire contract"
)]
fn validate_values(
    route: &str,
    response_value: &Value,
    expected_value: &Value,
) -> Result<(), JsValue> {
    let route = http_route::canonical(route)?;
    match route {
        http_route::IMAGES_QUALIFY => {
            let response_image = field(&response_value, "image")?;
            let expected_image = field(&expected_value, "image")?;
            image(response_image)?;
            image(expected_image)?;
            if response_image != expected_image {
                return Err(error("qualified image was substituted"));
            }
            let capabilities = array(field(&response_value, "capabilities")?, "capabilities")?;
            for capability in capabilities {
                one_of(capability, CAPABILITIES, "capability")?;
            }
            unique_strings(capabilities, "capabilities")?;
            digest(
                field(&response_value, "compatibilityRevisionHex")?,
                "compatibilityRevisionHex",
            )?;
        }
        http_route::MACHINES_INSPECT => {
            machine(&response_value)?;
            same(&response_value, &expected_value, "id", "machineId")?;
        }
        http_route::MACHINES_LIST => {
            for value in array(field(&response_value, "machines")?, "machines")? {
                machine(value)?;
            }
            let next = field(&response_value, "next")?;
            if !next.is_null() {
                text(next, "next")?;
            }
        }
        http_route::CHECKPOINTS_INSPECT => {
            checkpoint(&response_value)?;
            same(&response_value, &expected_value, "id", "checkpointId")?;
        }
        http_route::MACHINES_EVENTS => {
            let expected_machine = expected_text(&expected_value, "machineId")?;
            let mut previous = object(&expected_value, "expected")?
                .get("afterSequence")
                .and_then(Value::as_u64);
            let events = array(field(&response_value, "events")?, "events")?;
            for value in events {
                let (machine_id, sequence) = event(value)?;
                if machine_id != expected_machine
                    || previous.is_some_and(|previous| sequence <= previous)
                {
                    return Err(error("event identity or ordering is invalid"));
                }
                previous = Some(sequence);
            }
            let next = field(&response_value, "nextSequence")?;
            if !next.is_null() {
                let next = positive(next, "nextSequence")?;
                if previous != Some(next) {
                    return Err(error("event continuation does not match the page"));
                }
            }
        }
        http_route::MACHINES_USAGE => {
            usage(&response_value)?;
            if text(field(&response_value, "machine")?, "usage.machine")?
                != expected_text(&expected_value, "machineId")?
                || integer(field(&response_value, "startUnixMs")?, "startUnixMs")?
                    != integer(field(&expected_value, "startUnixMs")?, "startUnixMs")?
                || integer(field(&response_value, "endUnixMs")?, "endUnixMs")?
                    != integer(field(&expected_value, "endUnixMs")?, "endUnixMs")?
            {
                return Err(error("usage identity or interval was substituted"));
            }
            if integer(field(&expected_value, "startUnixMs")?, "startUnixMs")?
                >= integer(field(&expected_value, "endUnixMs")?, "endUnixMs")?
            {
                return Err(error("usage identity or interval was substituted"));
            }
            if let Some(value) = object(&response_value, "usage")?.get("lineageReceiptSha256") {
                let encoded = object(value, "lineageReceiptSha256")?
                    .get("$bytes")
                    .and_then(Value::as_str);
                if encoded
                    .and_then(decode_base64)
                    .is_none_or(|value| value.len() != 32)
                {
                    return Err(error("usage receipt is malformed"));
                }
            }
        }
        http_route::OPERATIONS_INSPECT | http_route::OPERATIONS_CANCEL => {
            operation(&response_value)?;
            same(&response_value, &expected_value, "id", "operationId")?;
        }
        http_route::OPERATIONS_WATCH => {
            for value in array(&response_value, "operations")? {
                operation(value)?;
                same(value, &expected_value, "id", "operationId")?;
            }
        }
        http_route::OPERATIONS_RECOVER_ID => {
            text(&response_value, "operationId")?;
        }
        http_route::MACHINES_CREATE
        | http_route::MACHINES_CHECKPOINT
        | http_route::MACHINES_FORK
        | http_route::CHECKPOINTS_FORK
        | http_route::MACHINES_SUSPEND
        | http_route::MACHINES_WAKE
        | http_route::MACHINES_SUSPENSION_POLICY
        | http_route::MACHINES_DESTROY
        | http_route::CHECKPOINTS_DESTROY
        | http_route::OPERATIONS_RECOVER => {
            mutation(&response_value)?;
            bind_mutation(route, &response_value, &expected_value)?;
        }
        _ => unreachable!("canonical route table and response validator are out of sync"),
    }
    Ok(())
}

/// Validate and project a hosted response into the same JavaScript DTO shape
/// produced by the simulator.  The wire wrapper grammar is decoded here so
/// TypeScript does not maintain a second bigint/bytes implementation.
pub fn decode(route: &str, response_json: &str, expected_json: &str) -> Result<JsValue, JsValue> {
    let response_value: Value =
        serde_json::from_str(response_json).map_err(|e| error(e.to_string()))?;
    let expected_value: Value =
        serde_json::from_str(expected_json).map_err(|e| error(e.to_string()))?;
    validate_values(route, &response_value, &expected_value)?;
    project(&response_value)
}

/// Validate one hosted HTTP JSON success response against its request context.
/// This remains available for callers that only need validation; the transport
/// uses `decode` so validation and scalar projection share this Rust boundary.
pub fn validate(route: &str, response_json: &str, expected_json: &str) -> Result<(), JsValue> {
    let response_value: Value =
        serde_json::from_str(response_json).map_err(|e| error(e.to_string()))?;
    let expected_value: Value =
        serde_json::from_str(expected_json).map_err(|e| error(e.to_string()))?;
    validate_values(route, &response_value, &expected_value)
}

fn project(value: &Value) -> Result<JsValue, JsValue> {
    match value {
        Value::Null => Ok(JsValue::NULL),
        Value::Bool(value) => Ok(JsValue::from_bool(*value)),
        Value::Number(value) => value
            .as_f64()
            .map(JsValue::from_f64)
            .ok_or_else(|| error("number cannot cross the JavaScript boundary")),
        Value::String(value) => Ok(JsValue::from_str(value)),
        Value::Array(values) => {
            let result = Array::new();
            for value in values {
                result.push(&project(value)?);
            }
            Ok(result.into())
        }
        Value::Object(values) => {
            if let Some(value) = values.get("$bigint") {
                let value = value
                    .as_str()
                    .and_then(|value| value.parse::<u64>().ok())
                    .ok_or_else(|| error("$bigint must contain an unsigned decimal integer"))?;
                return Ok(BigInt::from(value).into());
            }
            if let Some(value) = values.get("$bytes") {
                let value = value
                    .as_str()
                    .and_then(decode_base64)
                    .ok_or_else(|| error("$bytes must contain valid base64"))?;
                return Ok(Uint8Array::from(value.as_slice()).into());
            }
            let null_prototype: Object = JsValue::NULL.unchecked_into();
            let result = Object::create(&null_prototype);
            for (key, value) in values {
                Reflect::set(result.as_ref(), &JsValue::from_str(key), &project(value)?)
                    .map_err(|_| error("could not construct response value"))?;
            }
            Ok(result.into())
        }
    }
}

/// Encode a natural JavaScript request using the hosted transport's wrapper
/// grammar.  The traversal is Rust-owned so request and response scalar
/// boundaries cannot drift apart in TypeScript.
pub fn encode_request(value: &JsValue) -> Result<String, JsValue> {
    let seen = JsMap::new();
    let value = encode_js(value, &seen).map_err(error)?;
    serde_json::to_string(&value).map_err(|cause| error(cause.to_string()))
}

fn encode_js(value: &JsValue, seen: &JsMap) -> Result<Value, String> {
    if value.is_null() {
        return Ok(Value::Null);
    }
    if let Some(value) = value.as_bool() {
        return Ok(Value::Bool(value));
    }
    if let Some(value) = value.as_f64() {
        let number = if value.is_finite() && value.fract() == 0.0 {
            if value >= 0.0 && value <= u64::MAX as f64 {
                serde_json::Number::from_u128(value as u128)
            } else if value >= i64::MIN as f64 {
                serde_json::Number::from_i128(value as i128)
            } else {
                serde_json::Number::from_f64(value)
            }
        } else {
            serde_json::Number::from_f64(value)
        };
        return Ok(number.map(Value::Number).unwrap_or(Value::Null));
    }
    if let Some(value) = value.as_string() {
        return Ok(Value::String(value));
    }
    if value.is_undefined() || value.is_function() || value.is_symbol() {
        return Err("request contains an unsupported JavaScript value".to_owned());
    }
    if BigInt::is_type_of(value) {
        let value: BigInt = value.clone().unchecked_into();
        let value = value
            .to_string(10)
            .map_err(|_| "could not stringify bigint".to_owned())?
            .as_string()
            .ok_or_else(|| "could not stringify bigint".to_owned())?;
        let mut wrapper = serde_json::Map::new();
        wrapper.insert("$bigint".to_owned(), Value::String(value));
        return Ok(Value::Object(wrapper));
    }
    if Uint8Array::is_type_of(value) {
        let value = Uint8Array::new(value).to_vec();
        let mut wrapper = serde_json::Map::new();
        wrapper.insert("$bytes".to_owned(), Value::String(encode_base64(&value)));
        return Ok(Value::Object(wrapper));
    }
    if !value.is_object() {
        return Err("request contains an unsupported JavaScript value".to_owned());
    }
    if seen.has(value) {
        return Err("request contains a cyclic object".to_owned());
    }
    seen.set(value, &JsValue::TRUE);
    let result = if Array::is_array(value) {
        let array: Array = value.clone().unchecked_into();
        let object: Object = array.clone().into();
        let mut result = Vec::with_capacity(array.length() as usize);
        for index in 0..array.length() {
            let key = JsValue::from_str(&index.to_string());
            let item = descriptor_value(&object, &key)?.unwrap_or(JsValue::NULL);
            result.push(encode_js(&item, seen)?);
        }
        Ok(Value::Array(result))
    } else {
        let object: Object = value.clone().unchecked_into();
        let mut result = serde_json::Map::new();
        for key in Object::keys(&object).iter() {
            let key = key
                .as_string()
                .ok_or_else(|| "request contains a non-string property key".to_owned())?;
            let item = descriptor_value(&object, &JsValue::from_str(&key))?
                .ok_or_else(|| "request property disappeared during encoding".to_owned())?;
            result.insert(key, encode_js(&item, seen)?);
        }
        Ok(Value::Object(result))
    };
    seen.delete(value);
    result
}

fn descriptor_value(object: &Object, key: &JsValue) -> Result<Option<JsValue>, String> {
    let descriptor = Object::get_own_property_descriptor(object, key);
    if descriptor.is_undefined() {
        return Ok(None);
    }
    let descriptor: Object = descriptor.unchecked_into();
    let getter = Reflect::get(descriptor.as_ref(), &JsValue::from_str("get"))
        .map_err(|_| "could not inspect request property".to_owned())?;
    if !getter.is_undefined() && !getter.is_null() {
        return Err("request contains an accessor property".to_owned());
    }
    Reflect::get(descriptor.as_ref(), &JsValue::from_str("value"))
        .map(Some)
        .map_err(|_| "could not inspect request property".to_owned())
}

fn encode_base64(value: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::with_capacity(value.len().div_ceil(3) * 4);
    for chunk in value.chunks(3) {
        let first = chunk[0];
        result.push(ALPHABET[(first >> 2) as usize] as char);
        if chunk.len() == 1 {
            result.push(ALPHABET[((first & 0x03) << 4) as usize] as char);
            result.push('=');
            result.push('=');
            continue;
        }
        let second = chunk[1];
        result.push(ALPHABET[((first & 0x03) << 4 | second >> 4) as usize] as char);
        if chunk.len() == 2 {
            result.push(ALPHABET[((second & 0x0f) << 2) as usize] as char);
            result.push('=');
            continue;
        }
        let third = chunk[2];
        result.push(ALPHABET[((second & 0x0f) << 2 | third >> 6) as usize] as char);
        result.push(ALPHABET[(third & 0x3f) as usize] as char);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn machine(
        id: &str,
        image: &Value,
        capabilities: &[&str],
        performance: &str,
        last_checkpoint: &Value,
    ) -> Value {
        let digest = "a".repeat(64);
        serde_json::json!({
            "id": id,
            "state": "running",
            "contract": {
                "image": image,
                "capabilities": capabilities,
                "compatibility": {"kind": "best-effort"},
                "performance": performance,
                "suspension": {"kind": "manual"},
                "expiration": {"kind": "never"},
                "networkPolicyDigestHex": digest,
                "compatibilityRevisionHex": digest,
                "budgets": {"spendMicros": {"$bigint": "0"}, "concurrency": 0}
            },
            "endpoints": [{"name": "default", "uri": "memory://machine"}],
            "lastCheckpoint": last_checkpoint,
            "createdAtUnixMs": 1,
            "changedAtUnixMs": 1
        })
    }

    #[test]
    fn checkpoint_fork_children_are_bound_to_checkpoint_and_performance() {
        let expected = serde_json::json!({
            "checkpointId": "checkpoint-1",
            "count": 1,
            "performance": "elastic"
        });
        let response = |child| serde_json::json!({"kind": "forked", "machines": [child]});
        let valid = machine(
            "child",
            &serde_json::json!({"kind": "checkpoint", "checkpointId": "checkpoint-1"}),
            &["disk-fork"],
            "elastic",
            &serde_json::json!("checkpoint-1"),
        );
        assert!(validate_values(http_route::CHECKPOINTS_FORK, &response(valid), &expected).is_ok());

        let wrong_checkpoint = machine(
            "child",
            &serde_json::json!({"kind": "checkpoint", "checkpointId": "other"}),
            &["disk-fork"],
            "elastic",
            &serde_json::json!("other"),
        );
        assert!(
            validate_values(
                http_route::CHECKPOINTS_FORK,
                &response(wrong_checkpoint),
                &expected
            )
            .is_err()
        );

        let wrong_performance = machine(
            "child",
            &serde_json::json!({"kind": "checkpoint", "checkpointId": "checkpoint-1"}),
            &["disk-fork"],
            "dedicated",
            &serde_json::json!("checkpoint-1"),
        );
        assert!(
            validate_values(
                http_route::CHECKPOINTS_FORK,
                &response(wrong_performance),
                &expected
            )
            .is_err()
        );
    }

    #[test]
    fn machine_fork_children_are_fresh_and_match_fidelity() {
        let expected = serde_json::json!({"machineId": "source", "count": 1});
        let response = |child, fidelity| {
            serde_json::json!({
                "kind": "machine-forked",
                "source": "source",
                "fidelity": fidelity,
                "children": [child]
            })
        };
        let valid = machine(
            "child",
            &serde_json::json!({"kind": "managed-oci", "digestHex": "b".repeat(64)}),
            &["disk-fork"],
            "elastic",
            &Value::Null,
        );
        assert!(
            validate_values(
                http_route::MACHINES_FORK,
                &response(valid, "disk-only"),
                &expected
            )
            .is_ok()
        );

        let source_child = machine(
            "source",
            &serde_json::json!({"kind": "managed-oci", "digestHex": "b".repeat(64)}),
            &["disk-fork"],
            "elastic",
            &Value::Null,
        );
        assert!(
            validate_values(
                http_route::MACHINES_FORK,
                &response(source_child, "disk-only"),
                &expected
            )
            .is_err()
        );

        let wrong_capability = machine(
            "child",
            &serde_json::json!({"kind": "managed-oci", "digestHex": "b".repeat(64)}),
            &["live-fork"],
            "elastic",
            &Value::Null,
        );
        assert!(
            validate_values(
                http_route::MACHINES_FORK,
                &response(wrong_capability, "disk-only"),
                &expected
            )
            .is_err()
        );
    }

    #[test]
    fn digest_rejects_uppercase_and_zero() {
        assert!(!valid_digest(&"AB".repeat(32)));
        assert!(!valid_digest(&"00".repeat(32)));
        assert!(valid_digest(&"ab".repeat(32)));
    }

    #[test]
    fn hosted_wrappers_are_strict() {
        assert_eq!(big_payload(&serde_json::json!({"$bigint": "7"})), Some(7));
        assert!(big_payload(&serde_json::json!(7)).is_none());
        assert!(decode_base64("AQID").is_some());
        assert_eq!(decode_base64("AQI="), Some(vec![1, 2]));
        assert_eq!(decode_base64("AQ=="), Some(vec![1]));
        assert!(decode_base64("A=ID").is_none());
        assert!(decode_base64("AQ==AQID").is_none());
        assert!(decode_base64("AR==").is_none());
        assert!(decode_base64("not base64!").is_none());
        assert!(safe_u64(9_007_199_254_740_991));
        assert!(!safe_u64(9_007_199_254_740_992));
        assert_eq!(
            bytes_payload(&serde_json::json!({"$bytes": "AQID"})),
            Some(vec![1, 2, 3])
        );
        assert!(bytes_payload(&serde_json::json!([1, 2, 3])).is_none());
        assert!(bytes_payload(&serde_json::json!("AQID")).is_none());
    }
}
