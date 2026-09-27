//! Reflection based validation for the protobuf-declared Inference invariants.

use std::sync::OnceLock;

use js_sys::{Array, BigInt, Reflect, Uint8Array};
use prost::Message;
use prost::bytes::Bytes;
use prost_reflect::{DescriptorPool, DynamicMessage, Kind, ReflectMessage, Value};
use wasm_bindgen::{JsCast, prelude::*};

const DESCRIPTOR_SET: &[u8] = include_bytes!("../inference_reflection_descriptor.bin");
const VALIDATION_PACKAGE: &str = "acyclic.validation.v1.";

fn pool() -> Option<&'static DescriptorPool> {
    static POOL: OnceLock<Option<DescriptorPool>> = OnceLock::new();
    POOL.get_or_init(|| DescriptorPool::decode(DESCRIPTOR_SET).ok())
        .as_ref()
}

fn option(message: &DynamicMessage, name: &str) -> Option<Value> {
    let ext = pool()?.get_extension_by_name(&format!("{VALIDATION_PACKAGE}{name}"))?;
    message
        .has_extension(&ext)
        .then(|| message.get_extension(&ext).into_owned())
}

fn bool_option(message: &DynamicMessage, name: &str) -> bool {
    matches!(option(message, name), Some(Value::Bool(true)))
}

/// Return the terminal metadata declared by the protobuf enum options.
///
/// The browser-facing package consumes this small boundary instead of
/// reimplementing enum naming or deciding which terminal outcomes may carry
/// incomplete output. The descriptor is the same generated descriptor used
/// by schema validation, so adding or renaming a terminal must update the
/// protobuf contract before it can reach TypeScript.
#[wasm_bindgen]
pub fn run_terminal_metadata() -> Result<String, JsValue> {
    terminal_metadata().map_err(JsValue::from_str)
}

/// Return terminal metadata for native generators and the WASM boundary.
pub fn terminal_metadata() -> Result<String, &'static str> {
    let Some(pool) = pool() else {
        return Err("inference reflection descriptor is unavailable");
    };
    let Some(enumeration) = pool.get_enum_by_name("inference.customer.v1.RunTerminal") else {
        return Err("inference RunTerminal descriptor is unavailable");
    };
    let values = enumeration
        .values()
        .filter(|value| value.number() != 0)
        .map(|value| {
            let kind = value
                .name()
                .strip_prefix("RUN_TERMINAL_")
                .unwrap_or(value.name())
                .to_ascii_lowercase()
                .replace('_', "-");
            let partial = bool_option(&value.options(), "partial_terminal");
            format!(
                r#"{{"number":{},"kind":"{kind}","partial":{partial}}}"#,
                value.number()
            )
        })
        .collect::<Vec<_>>();
    Ok(format!("[{}]", values.join(",")))
}

/// Return every fixed byte width declared by the canonical customer descriptor.
///
/// This is consumed by the TypeScript generator. Keeping the list derived from
/// the same reflection descriptor used for validation makes a change to a
/// `nonzero_fixed_bytes` option fail the generated-contract check instead of
/// silently leaving a stale handwritten width in a client helper.
pub fn fixed_width_metadata() -> Result<String, &'static str> {
    let Some(pool) = pool() else {
        return Err("inference reflection descriptor is unavailable");
    };
    let mut values = pool
        .all_messages()
        .flat_map(|message| {
            let name = message.full_name().to_owned();
            message
                .fields()
                .filter_map(move |field| {
                    let width = u32_option(&field.options(), "nonzero_fixed_bytes");
                    (width > 0).then(|| (name.clone(), field.name().to_owned(), width))
                })
                .collect::<Vec<_>>()
                .into_iter()
        })
        .collect::<Vec<_>>();
    values.sort_unstable();
    let entries = values
        .into_iter()
        .map(|(message, field, width)| {
            format!(r#"{{"message":"{message}","field":"{field}","width":{width}}}"#)
        })
        .collect::<Vec<_>>();
    Ok(format!("[{}]", entries.join(",")))
}

fn u32_option(message: &DynamicMessage, name: &str) -> u32 {
    match option(message, name) {
        Some(Value::U32(value)) => value,
        _ => 0,
    }
}

fn u64_option(message: &DynamicMessage, name: &str) -> u64 {
    match option(message, name) {
        Some(Value::U64(value)) => value,
        _ => 0,
    }
}

fn field_bytes(value: &Value) -> Option<usize> {
    match value {
        Value::Bytes(bytes) => Some(bytes.len()),
        Value::String(value) => Some(value.len()),
        _ => None,
    }
}

#[allow(clippy::cognitive_complexity, clippy::too_many_lines)]
fn validate_message(message: &DynamicMessage, path: &str) -> Option<String> {
    let descriptor = message.descriptor();
    for field in descriptor.fields() {
        let field_path = format!("{path}.{}", field.json_name());
        if field.containing_oneof().is_some() && !message.has_field(&field) {
            continue;
        }
        let present = message.has_field(&field);
        let value = message.get_field(&field);
        let fixed = u32_option(&field.options(), "nonzero_fixed_bytes") as usize;
        let required = bool_option(&field.options(), "required_message");
        let positive = bool_option(&field.options(), "positive_uint64");
        let minimum = u32_option(&field.options(), "min_items") as usize;
        let maximum = u32_option(&field.options(), "max_items") as usize;
        let max_bytes = u32_option(&field.options(), "nonempty_max_bytes") as usize;
        let max_item_bytes = u32_option(&field.options(), "nonempty_max_item_bytes") as usize;
        let max_uint = u64_option(&field.options(), "max_uint64");
        let known_enum = bool_option(&field.options(), "known_nonzero_enum");

        if !present && field.supports_presence() {
            if required {
                return Some(format!("{field_path} is required"));
            }
            if positive {
                return Some(format!("{field_path} must be positive"));
            }
            if minimum > 0 {
                return Some(format!("{field_path} must have at least {minimum} items"));
            }
            if max_bytes > 0 {
                return Some(format!("{field_path} must contain 1 to {max_bytes} bytes"));
            }
            if known_enum {
                return Some(format!("{field_path} must be a known nonzero enum value"));
            }
            if fixed > 0
                && field.containing_oneof().is_none()
                && field.field_descriptor_proto().proto3_optional != Some(true)
            {
                return Some(format!("{field_path} must be exactly {fixed} bytes"));
            }
            continue;
        }
        if !present && required {
            return Some(format!("{field_path} is required"));
        }

        if fixed > 0 {
            if field_bytes(&value) != Some(fixed) {
                return Some(format!("{field_path} must be exactly {fixed} bytes"));
            }
            if let Value::Bytes(bytes) = value.as_ref()
                && bytes.iter().all(|byte| *byte == 0)
            {
                return Some(format!("{field_path} must be nonzero"));
            }
        }
        if positive && !matches!(value.as_ref(), Value::U64(value) if *value > 0) {
            return Some(format!("{field_path} must be positive"));
        }
        if max_uint > 0 && !matches!(value.as_ref(), Value::U64(value) if *value <= max_uint) {
            return Some(format!("{field_path} must be at most {max_uint}"));
        }
        if max_bytes > 0 {
            let size = field_bytes(&value).unwrap_or(usize::MAX);
            if !(1..=max_bytes).contains(&size) {
                return Some(format!("{field_path} must contain 1 to {max_bytes} bytes"));
            }
        }
        if known_enum {
            let valid = match (&field.kind(), value.as_ref()) {
                (Kind::Enum(enumeration), Value::EnumNumber(number)) => {
                    *number != 0 && enumeration.get_value(*number).is_some()
                }
                _ => false,
            };
            if !valid {
                return Some(format!("{field_path} must be a known nonzero enum value"));
            }
        }
        if minimum > 0 || maximum > 0 || max_item_bytes > 0 {
            let Value::List(items) = value.as_ref() else {
                return Some(format!("{field_path} item count is invalid"));
            };
            if items.len() < minimum || (maximum > 0 && items.len() > maximum) {
                return Some(format!("{field_path} item count is invalid"));
            }
            if max_item_bytes > 0 && items.iter().any(|item| {
                !matches!(field_bytes(item), Some(size) if (1..=max_item_bytes).contains(&size))
            }) {
                return Some(format!("{field_path} item byte length is invalid"));
            }
        }
        match value.as_ref() {
            Value::Message(child) => {
                if let Some(error) = validate_message(child, &field_path) {
                    return Some(error);
                }
            }
            Value::List(items) if matches!(field.kind(), Kind::Message(_)) => {
                for (index, item) in items.iter().enumerate() {
                    if let Value::Message(child) = item
                        && let Some(error) =
                            validate_message(child, &format!("{field_path}[{index}]"))
                    {
                        return Some(error);
                    }
                }
            }
            _ => {}
        }
    }
    for oneof in descriptor.oneofs() {
        if !bool_option(&oneof.options(), "required_oneof") {
            continue;
        }
        if !oneof.fields().any(|field| message.has_field(&field)) {
            return Some(format!("{path}.{} is required", oneof.name()));
        }
    }
    None
}

/// Validate a protobuf message by its fully-qualified descriptor name.
/// Returns a TypeScript-compatible diagnostic, or `None` when valid.
#[wasm_bindgen]
pub fn schema_error(message_name: &str, bytes: &[u8]) -> Option<String> {
    let Some(pool) = pool() else {
        return Some("inference reflection descriptor is unavailable".to_owned());
    };
    let Some(descriptor) = pool.get_message_by_name(message_name) else {
        return Some(format!("unknown protobuf message {message_name}"));
    };
    let Ok(message) = DynamicMessage::decode(descriptor, bytes) else {
        return Some(format!("{message_name} protobuf is invalid"));
    };
    validate_message(&message, message.descriptor().name())
}

fn property(value: &JsValue, name: &str) -> JsValue {
    Reflect::get(value, &JsValue::from_str(name)).unwrap_or(JsValue::UNDEFINED)
}

fn camel_case(name: &str) -> String {
    let mut upper = false;
    name.chars()
        .filter_map(|character| {
            if character == '_' {
                upper = true;
                None
            } else if upper {
                upper = false;
                Some(character.to_ascii_uppercase())
            } else {
                Some(character)
            }
        })
        .collect()
}

fn valid_scalar(kind: &Kind, value: &JsValue) -> bool {
    match kind {
        Kind::Bytes => Uint8Array::is_type_of(value),
        Kind::String => value.is_string(),
        Kind::Bool => value.as_bool().is_some(),
        Kind::Int64 | Kind::Uint64 | Kind::Sint64 | Kind::Fixed64 | Kind::Sfixed64 => {
            value.is_bigint()
        }
        Kind::Double | Kind::Float => value.as_f64().is_some(),
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => {
            valid_integer(value, f64::from(i32::MIN), f64::from(i32::MAX))
        }
        Kind::Uint32 | Kind::Fixed32 => valid_integer(value, 0.0, f64::from(u32::MAX)),
        Kind::Message(_) | Kind::Enum(_) => false,
    }
}

fn valid_integer(value: &JsValue, minimum: f64, maximum: f64) -> bool {
    value.as_f64().is_some_and(|number| {
        number.is_finite() && number.fract() == 0.0 && (minimum..=maximum).contains(&number)
    })
}

#[allow(clippy::too_many_lines)]
fn validate_runtime_message(
    descriptor: &prost_reflect::MessageDescriptor,
    value: &JsValue,
    path: &str,
) -> Option<String> {
    if !value.is_object() || value.is_null() {
        return Some(format!("{path} must be a message"));
    }

    for oneof in descriptor.oneofs() {
        if oneof.is_synthetic() {
            continue;
        }
        let oneof_path = format!("{path}.{}", camel_case(oneof.name()));
        let selected = property(value, &camel_case(oneof.name()));
        if selected.is_undefined() {
            continue;
        }
        if !selected.is_object() || selected.is_null() {
            return Some(format!("{oneof_path} has an invalid protobuf selection"));
        }
        let case = property(&selected, "case");
        let selected_value = property(&selected, "value");
        if case.is_undefined() && selected_value.is_undefined() {
            continue;
        }
        let Some(case) = case.as_string() else {
            return Some(format!("{oneof_path} has an invalid protobuf selection"));
        };
        let Some(field) = oneof.fields().find(|field| field.json_name() == case) else {
            return Some(format!("{oneof_path} has an invalid protobuf selection"));
        };
        if selected_value.is_undefined() {
            return Some(format!("{path}.{} is missing its value", field.json_name()));
        }
    }

    for field in descriptor.fields() {
        let field_path = format!("{path}.{}", field.json_name());
        let item = if let Some(oneof) = field
            .containing_oneof()
            .filter(|oneof| !oneof.is_synthetic())
        {
            let selected = property(value, &camel_case(oneof.name()));
            if selected.is_undefined()
                || property(&selected, "case").as_string().as_deref() != Some(field.json_name())
            {
                continue;
            }
            property(&selected, "value")
        } else {
            property(value, field.json_name())
        };
        if item.is_undefined() {
            continue;
        }
        if field.is_list() {
            if !Array::is_array(&item) {
                return Some(format!("{field_path} must be a list"));
            }
            let items = Array::from(&item);
            for index in 0..items.length() {
                let child = items.get(index);
                match field.kind() {
                    Kind::Message(message) => {
                        if let Some(error) = validate_runtime_message(
                            &message,
                            &child,
                            &format!("{field_path}[{index}]"),
                        ) {
                            return Some(error);
                        }
                    }
                    Kind::Enum(_) => {
                        if !valid_integer(&child, f64::from(i32::MIN), f64::from(i32::MAX)) {
                            return Some(format!(
                                "{field_path}[{index}] has an invalid protobuf type"
                            ));
                        }
                    }
                    kind if !valid_scalar(&kind, &child) => {
                        return Some(format!(
                            "{field_path}[{index}] has an invalid protobuf type"
                        ));
                    }
                    _ => {}
                }
            }
        } else {
            match field.kind() {
                Kind::Message(message) => {
                    if let Some(error) = validate_runtime_message(&message, &item, &field_path) {
                        return Some(error);
                    }
                }
                Kind::Enum(_) => {
                    if !valid_integer(&item, f64::from(i32::MIN), f64::from(i32::MAX)) {
                        return Some(format!("{field_path} has an invalid protobuf type"));
                    }
                }
                kind if !valid_scalar(&kind, &item) => {
                    return Some(format!("{field_path} has an invalid protobuf type"));
                }
                _ => {}
            }
        }
    }
    None
}

/// Validate the JavaScript object shape before protobuf encoding can coerce it.
#[wasm_bindgen]
#[allow(clippy::needless_pass_by_value)]
pub fn runtime_shape_error(message_name: &str, value: JsValue) -> Option<String> {
    let Some(pool) = pool() else {
        return Some("inference reflection descriptor is unavailable".to_owned());
    };
    let Some(descriptor) = pool.get_message_by_name(message_name) else {
        return Some(format!("{message_name} is not a known protobuf message"));
    };
    validate_runtime_message(&descriptor, &value, descriptor.name())
}

fn bigint_text(value: &JsValue) -> Option<String> {
    if !value.is_bigint() {
        return None;
    }
    let value: BigInt = value.clone().unchecked_into();
    BigInt::to_string(&value, 10).ok().map(|text| text.into())
}

fn runtime_integer(value: &JsValue, minimum: i128, maximum: i128) -> Option<i128> {
    if let Some(number) = value.as_f64()
        && number.is_finite()
        && number.fract() == 0.0
    {
        // Parsing the canonical decimal representation avoids the saturating
        // semantics of a float-to-integer cast while retaining the existing
        // exact-integer check for values that f64 cannot represent exactly.
        if let Some(integer) = number
            .to_string()
            .parse::<i128>()
            .ok()
            .filter(|integer| (minimum..=maximum).contains(integer))
            && integer.to_string().parse::<f64>().ok() == Some(number)
        {
            return Some(integer);
        }
    }
    bigint_text(value)?
        .parse()
        .ok()
        .filter(|integer| (minimum..=maximum).contains(integer))
}

fn runtime_value(kind: &Kind, value: &JsValue, path: &str) -> Result<Value, String> {
    let invalid = || format!("{path} has an invalid protobuf type");
    match kind {
        Kind::Bytes if Uint8Array::is_type_of(value) => {
            Ok(Value::Bytes(Bytes::from(Uint8Array::new(value).to_vec())))
        }
        Kind::Bytes => Err(invalid()),
        Kind::String => value.as_string().map(Value::String).ok_or_else(invalid),
        Kind::Bool => value.as_bool().map(Value::Bool).ok_or_else(invalid),
        Kind::Double => value.as_f64().map(Value::F64).ok_or_else(invalid),
        Kind::Float => value
            .as_f64()
            .and_then(|number| number.to_string().parse::<f32>().ok())
            .map(Value::F32)
            .ok_or_else(invalid),
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => {
            runtime_integer(value, i32::MIN.into(), i32::MAX.into())
                .and_then(|number| i32::try_from(number).ok())
                .map(Value::I32)
                .ok_or_else(invalid)
        }
        Kind::Uint32 | Kind::Fixed32 => runtime_integer(value, 0, u32::MAX.into())
            .and_then(|number| u32::try_from(number).ok())
            .map(Value::U32)
            .ok_or_else(invalid),
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => {
            runtime_integer(value, i64::MIN.into(), i64::MAX.into())
                .and_then(|number| i64::try_from(number).ok())
                .map(Value::I64)
                .ok_or_else(invalid)
        }
        Kind::Uint64 | Kind::Fixed64 => runtime_integer(value, 0, u64::MAX.into())
            .and_then(|number| u64::try_from(number).ok())
            .map(Value::U64)
            .ok_or_else(invalid),
        Kind::Enum(_) => runtime_integer(value, i32::MIN.into(), i32::MAX.into())
            .and_then(|number| i32::try_from(number).ok())
            .map(Value::EnumNumber)
            .ok_or_else(invalid),
        Kind::Message(descriptor) => runtime_message(descriptor, value, path).map(Value::Message),
    }
}

fn runtime_message(
    descriptor: &prost_reflect::MessageDescriptor,
    value: &JsValue,
    path: &str,
) -> Result<DynamicMessage, String> {
    let mut message = DynamicMessage::new(descriptor.clone());
    for field in descriptor.fields() {
        let item = if let Some(oneof) = field
            .containing_oneof()
            .filter(|oneof| !oneof.is_synthetic())
        {
            let selected = property(value, &camel_case(oneof.name()));
            if selected.is_undefined()
                || property(&selected, "case").as_string().as_deref() != Some(field.json_name())
            {
                continue;
            }
            property(&selected, "value")
        } else {
            property(value, field.json_name())
        };
        if item.is_undefined() {
            continue;
        }
        let field_path = format!("{path}.{}", field.json_name());
        let converted = if field.is_map() {
            return Err(format!("{field_path} map fields are unsupported"));
        } else if field.is_list() {
            if !Array::is_array(&item) {
                return Err(format!("{field_path} must be a list"));
            }
            let items = Array::from(&item);
            let values = (0..items.length())
                .map(|index| {
                    runtime_value(
                        &field.kind(),
                        &items.get(index),
                        &format!("{field_path}[{index}]"),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            Value::List(values)
        } else {
            runtime_value(&field.kind(), &item, &field_path)?
        };
        message
            .try_set_field(&field, converted)
            .map_err(|_| format!("{field_path} has an invalid protobuf type"))?;
    }
    Ok(message)
}

/// Encode a JavaScript protobuf-shaped value using the Rust descriptor and
/// canonical dynamic message encoder. Shape validation and protobuf encoding
/// therefore share one reflection boundary instead of relying on a TypeScript
/// cast followed by generated runtime coercion.
#[wasm_bindgen]
pub fn runtime_encode(message_name: &str, value: &JsValue) -> Result<Uint8Array, JsValue> {
    let Some(pool) = pool() else {
        return Err(JsValue::from_str(
            "inference reflection descriptor is unavailable",
        ));
    };
    let Some(descriptor) = pool.get_message_by_name(message_name) else {
        return Err(JsValue::from_str(&format!(
            "{message_name} is not a known protobuf message"
        )));
    };
    if let Some(error) = validate_runtime_message(&descriptor, value, descriptor.name()) {
        return Err(JsValue::from_str(&error));
    }
    let message = runtime_message(&descriptor, value, descriptor.name())
        .map_err(|error| JsValue::from_str(&error))?;
    Ok(Uint8Array::from(message.encode_to_vec().as_slice()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_inference_contract::wire;
    use prost::Message;

    #[test]
    fn rejects_invalid_fixed_bytes() {
        let value = wire::ModelCapability {
            model: "model".into(),
            execution_profile: vec![1],
            ..Default::default()
        };
        assert_eq!(
            schema_error(
                "inference.customer.v1.ModelCapability",
                &value.encode_to_vec()
            ),
            Some("ModelCapability.executionProfile must be exactly 32 bytes".into())
        );
    }

    #[test]
    fn rejects_unknown_message_and_malformed_bytes() {
        assert!(schema_error("unknown.Message", &[]).is_some());
        assert!(schema_error("inference.customer.v1.ModelCapability", &[0x80]).is_some());
    }

    #[test]
    fn terminal_metadata_comes_from_enum_options() {
        let metadata = run_terminal_metadata();
        assert!(
            metadata.is_ok(),
            "descriptor metadata should load: {metadata:?}"
        );
        if let Ok(metadata) = metadata {
            assert!(metadata.contains(r#""number":1,"kind":"completed","partial":false"#));
            assert!(metadata.contains(r#""number":5,"kind":"cancelled","partial":true"#));
            assert!(metadata.contains(r#""number":7,"kind":"indeterminate","partial":true"#));
        }
    }

    #[test]
    fn fixed_width_metadata_comes_from_field_options() {
        let metadata = fixed_width_metadata();
        assert!(
            metadata.is_ok(),
            "descriptor metadata should load: {metadata:?}"
        );
        if let Ok(metadata) = metadata {
            assert!(metadata.contains(
                r#"{"message":"inference.customer.v1.RequestIdentity","field":"request_id","width":16}"#
            ));
            assert!(metadata.contains(
                r#"{"message":"inference.customer.v1.ContextView","field":"revision","width":32}"#
            ));
            assert!(!metadata.contains(r#""width":0"#));
        }
    }
}
