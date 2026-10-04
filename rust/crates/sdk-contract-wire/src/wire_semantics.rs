//! Descriptor driven protobuf semantic comparison.
//!
//! Raw protobuf bytes are useful for provenance, but they are not a stable
//! semantic identity: field order and map entry order may vary.  This module
//! decodes both messages against the same Rust-owned descriptor and compares
//! presence, oneof selection, scalar/enum values, repeated values, and map
//! contents recursively.

use std::collections::HashMap;
use std::collections::BTreeMap;
use std::fmt;

use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, FieldDescriptor, MapKey, MessageDescriptor, MethodDescriptor, ReflectMessage, Value};

use crate::family_registry::family_view;

/// Whether unknown wire fields are part of the semantic contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnknownFieldPolicy {
    /// Ignore unknown fields, for forward-compatible readers.
    Ignore,
    /// Compare unknown fields by number, wire kind, and canonical value.
    CompareCanonical,
}

/// How floating point NaN values are compared.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FloatPolicy {
    /// Require identical IEEE-754 payload bits.
    Bitwise,
    /// Treat all NaN payloads as the same semantic value.
    AllNaNsEqual,
}

/// Options for descriptor-bound semantic comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompareOptions {
    pub unknown_fields: UnknownFieldPolicy,
    pub floats: FloatPolicy,
}

/// Selects the Rust-owned protobuf type bound to one RPC direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcDirection { Request, Response }

/// Streaming shape declared by the Rust-owned RPC descriptor.
///
/// Receipt validators use this metadata to distinguish frame cardinality from
/// payload semantics. A unary success requires one response frame, a unary
/// error may have zero response frames, a server stream may have zero or more
/// response frames, and a client stream must carry an explicitly ordered,
/// non-empty request sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RpcStreaming {
    pub client_streaming: bool,
    pub server_streaming: bool,
}

/// Failure resolving or comparing a Rust-owned RPC message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RpcSemanticError {
    InvalidIdentity(String), UnknownFamily(String), UnknownService(String), UnknownMethod(String), Semantic(SemanticMismatch),
}

impl fmt::Display for RpcSemanticError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidIdentity(value) => write!(f, "invalid RPC identity {value:?}; expected /fully.qualified.Service/Method"),
            Self::UnknownFamily(value) => write!(f, "unknown Rust-owned contract family {value}"),
            Self::UnknownService(value) => write!(f, "unknown Rust-owned RPC service {value}"),
            Self::UnknownMethod(value) => write!(f, "unknown Rust-owned RPC method {value}"),
            Self::Semantic(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for RpcSemanticError {}
impl From<SemanticMismatch> for RpcSemanticError {
    fn from(value: SemanticMismatch) -> Self { Self::Semantic(value) }
}

impl Default for CompareOptions {
    fn default() -> Self {
        Self { unknown_fields: UnknownFieldPolicy::CompareCanonical, floats: FloatPolicy::Bitwise }
    }
}

/// Resolve a full RPC identity against the Rust descriptor pool and compare
/// request or response bytes against that method's bound message descriptor.
pub fn compare_rpc_message(
    pool: &DescriptorPool,
    full_rpc: &str,
    direction: RpcDirection,
    expected: &[u8],
    observed: &[u8],
) -> Result<(), RpcSemanticError> {
    compare_rpc_message_with_options(pool, full_rpc, direction, expected, observed, CompareOptions::default())
}

/// Resolve the Rust-owned streaming shape for one fully qualified RPC.
pub fn rpc_streaming(
    pool: &DescriptorPool,
    full_rpc: &str,
) -> Result<RpcStreaming, RpcSemanticError> {
    let method = resolve_method(pool, full_rpc)?;
    Ok(RpcStreaming {
        client_streaming: method.is_client_streaming(),
        server_streaming: method.is_server_streaming(),
    })
}

/// Resolve the Rust-owned streaming shape through a registered contract family.
pub fn family_rpc_streaming(
    family: &str,
    full_rpc: &str,
) -> Result<RpcStreaming, RpcSemanticError> {
    let pool = family_descriptor_pool(family)?;
    rpc_streaming(&pool, full_rpc)
}

/// Compare one receipt frame through the Rust-owned family registry.
///
/// The family registry supplies the descriptor, so receipt consumers cannot
/// accidentally validate a frame against a separately authored schema. Raw
/// frame hashes remain an independent provenance check at the receipt layer.
pub fn compare_family_rpc_message(
    family: &str,
    full_rpc: &str,
    direction: RpcDirection,
    expected: &[u8],
    observed: &[u8],
) -> Result<(), RpcSemanticError> {
    let pool = family_descriptor_pool(family)?;
    compare_rpc_message(&pool, full_rpc, direction, expected, observed)
}

fn family_descriptor_pool(family: &str) -> Result<DescriptorPool, RpcSemanticError> {
    let view = family_view(family)
        .ok_or_else(|| RpcSemanticError::UnknownFamily(family.to_owned()))?;
    // Inference's public model descriptor is intentionally a compact target
    // file. Its Rust-owned option closure retains the validation extension
    // definitions and their dependency descriptors; use that closure here so
    // semantic verification never resolves a custom option through a fake
    // placeholder file.
    let model_descriptor = if family == "inference" {
        crate::inference_descriptor_with_options()
    } else {
        view.model.descriptor()
    };
    let mut descriptor_set = prost_types::FileDescriptorSet::decode(model_descriptor.as_slice())
        .map_err(|error| RpcSemanticError::UnknownFamily(format!("{family}: descriptor decode failed: {error}")))?;
    if descriptor_set.file.iter().any(|file| {
        file.dependency
            .iter()
            .any(|dependency| dependency == "google/protobuf/timestamp.proto")
    }) && !descriptor_set.file.iter().any(|file| {
        file.name.as_deref() == Some("google/protobuf/timestamp.proto")
    }) {
        descriptor_set.file.push(timestamp_descriptor());
    }
    if descriptor_set.file.iter().any(|file| {
        file.dependency
            .iter()
            .any(|dependency| dependency == "validation/v1/options.proto")
    }) && !descriptor_set.file.iter().any(|file| {
        file.name.as_deref() == Some("validation/v1/options.proto")
    }) {
        return Err(RpcSemanticError::UnknownFamily(
            format!("{family}: Rust-owned validation option descriptor closure is incomplete"),
        ));
    }
    let descriptor_bytes = descriptor_set.encode_to_vec();
    DescriptorPool::decode(descriptor_bytes.as_slice())
        .map_err(|error| RpcSemanticError::UnknownFamily(format!("{family}: descriptor decode failed: {error}")))
}

fn timestamp_descriptor() -> prost_types::FileDescriptorProto {
    prost_types::FileDescriptorProto {
        name: Some("google/protobuf/timestamp.proto".to_owned()),
        package: Some("google.protobuf".to_owned()),
        syntax: Some("proto3".to_owned()),
        message_type: vec![prost_types::DescriptorProto {
            name: Some("Timestamp".to_owned()),
            field: vec![
                prost_types::FieldDescriptorProto {
                    name: Some("seconds".to_owned()),
                    number: Some(1),
                    label: Some(prost_types::field_descriptor_proto::Label::Optional as i32),
                    r#type: Some(prost_types::field_descriptor_proto::Type::Int64 as i32),
                    ..Default::default()
                },
                prost_types::FieldDescriptorProto {
                    name: Some("nanos".to_owned()),
                    number: Some(2),
                    label: Some(prost_types::field_descriptor_proto::Label::Optional as i32),
                    r#type: Some(prost_types::field_descriptor_proto::Type::Int32 as i32),
                    ..Default::default()
                },
            ],
            ..Default::default()
        }],
        ..Default::default()
    }
}

pub fn compare_rpc_message_with_options(
    pool: &DescriptorPool,
    full_rpc: &str,
    direction: RpcDirection,
    expected: &[u8],
    observed: &[u8],
    options: CompareOptions,
) -> Result<(), RpcSemanticError> {
    let method = resolve_method(pool, full_rpc)?;
    let descriptor = match direction { RpcDirection::Request => method.input(), RpcDirection::Response => method.output() };
    compare_message_with_options(descriptor, expected, observed, options).map_err(Into::into)
}

fn resolve_method(pool: &DescriptorPool, full_rpc: &str) -> Result<MethodDescriptor, RpcSemanticError> {
    let identity = full_rpc.trim_start_matches('/');
    let (service_name, method_name) = identity
        .split_once('/')
        .ok_or_else(|| RpcSemanticError::InvalidIdentity(full_rpc.to_owned()))?;
    let service = pool
        .get_service_by_name(service_name)
        .ok_or_else(|| RpcSemanticError::UnknownService(service_name.to_owned()))?;
    service
        .methods()
        .find(|candidate| candidate.name() == method_name)
        .ok_or_else(|| RpcSemanticError::UnknownMethod(format!("{service_name}/{method_name}")))
}

/// A semantic difference between two protobuf messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SemanticMismatch {
    /// A protobuf field path, such as `contract.labels["region"]`.
    pub path: String,
    /// The expected value at `path`.
    pub expected: String,
    /// The observed value at `path`.
    pub observed: String,
}

impl fmt::Display for SemanticMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "protobuf semantic mismatch at {}: expected {}, observed {}", self.path, self.expected, self.observed)
    }
}

impl std::error::Error for SemanticMismatch {}

/// Decode and compare two messages using one descriptor.
///
/// The descriptor must be resolved from the Rust-owned RPC method's request or
/// response descriptor. There is deliberately no separately supplied observed
/// descriptor: both byte sequences are decoded against that one bound type.
/// RPC identity, frame index, and request-versus-response selection remain the
/// responsibility of the receipt caller that performs this lookup.
pub fn compare_message(
    descriptor: MessageDescriptor,
    expected: &[u8],
    observed: &[u8],
) -> Result<(), SemanticMismatch> {
    compare_message_with_options(descriptor, expected, observed, CompareOptions::default())
}

/// Decode and compare two messages using one descriptor and explicit wire policy.
pub fn compare_message_with_options(
    descriptor: MessageDescriptor,
    expected: &[u8],
    observed: &[u8],
    options: CompareOptions,
) -> Result<(), SemanticMismatch> {
    let expected = DynamicMessage::decode(descriptor.clone(), expected).map_err(|error| SemanticMismatch {
        path: "<decode expected>".to_owned(),
        expected: "valid protobuf bytes".to_owned(),
        observed: error.to_string(),
    })?;
    let observed = DynamicMessage::decode(descriptor, observed).map_err(|error| SemanticMismatch {
        path: "<decode observed>".to_owned(),
        expected: "valid protobuf bytes".to_owned(),
        observed: error.to_string(),
    })?;
    compare_dynamic("$", &expected, &observed, options)
}

fn compare_dynamic(
    path: &str,
    expected: &DynamicMessage,
    observed: &DynamicMessage,
    options: CompareOptions,
) -> Result<(), SemanticMismatch> {
    for field in expected.descriptor().fields() {
        let expected_present = expected.has_field(&field);
        let observed_present = observed.has_field(&field);
        let field_path = format!("{path}.{}", field.name());
        if expected_present != observed_present {
            return Err(mismatch(
                field_path,
                if expected_present { "present" } else { "absent" },
                if observed_present { "present" } else { "absent" },
            ));
        }
        if !expected_present {
            continue;
        }
        let expected_value = expected.get_field(&field);
        let observed_value = observed.get_field(&field);
        compare_value(&field_path, &field, expected_value.as_ref(), observed_value.as_ref(), options)?;
    }
    if options.unknown_fields == UnknownFieldPolicy::CompareCanonical {
        let expected_unknown = unknown_signature(expected);
        let observed_unknown = unknown_signature(observed);
        if expected_unknown != observed_unknown {
            return Err(mismatch(format!("{path}.<unknown>"), format!("{expected_unknown:?}"), format!("{observed_unknown:?}")));
        }
    }
    Ok(())
}

fn compare_value(
    path: &str,
    field: &FieldDescriptor,
    expected: &Value,
    observed: &Value,
    options: CompareOptions,
) -> Result<(), SemanticMismatch> {
    match (expected, observed) {
        (Value::Message(expected), Value::Message(observed)) => compare_dynamic(path, expected, observed, options),
        (Value::List(expected), Value::List(observed)) => {
            if expected.len() != observed.len() {
                return Err(mismatch(path, format!("list[{} items]", expected.len()), format!("list[{} items]", observed.len())));
            }
            for (index, (expected, observed)) in expected.iter().zip(observed).enumerate() {
                compare_value(path, field, expected, observed, options).map_err(|mut error| {
                    error.path = format!("{path}[{index}]{}", error.path.strip_prefix(path).unwrap_or(""));
                    error
                })?;
            }
            Ok(())
        }
        (Value::Map(expected), Value::Map(observed)) => compare_map(path, field, expected, observed, options),
        _ if values_equal(expected, observed, options.floats) => Ok(()),
        _ => Err(mismatch(path, value_summary(expected), value_summary(observed))),
    }
}

fn compare_map(
    path: &str,
    field: &FieldDescriptor,
    expected: &HashMap<MapKey, Value>,
    observed: &HashMap<MapKey, Value>,
    options: CompareOptions,
) -> Result<(), SemanticMismatch> {
    if expected.len() != observed.len() {
        return Err(mismatch(path, format!("map[{} entries]", expected.len()), format!("map[{} entries]", observed.len())));
    }
    for (key, expected_value) in expected {
        let observed_value = observed.get(key).ok_or_else(|| {
            mismatch(format!("{path}[{key:?}]"), "present", "absent")
        })?;
        compare_value(&format!("{path}[{key:?}]"), field, expected_value, observed_value, options)?;
    }
    Ok(())
}

fn values_equal(expected: &Value, observed: &Value, floats: FloatPolicy) -> bool {
    match (expected, observed) {
        (Value::F32(left), Value::F32(right)) => {
            (left.to_bits() == right.to_bits()) || (floats == FloatPolicy::AllNaNsEqual && left.is_nan() && right.is_nan())
        }
        (Value::F64(left), Value::F64(right)) => {
            (left.to_bits() == right.to_bits()) || (floats == FloatPolicy::AllNaNsEqual && left.is_nan() && right.is_nan())
        }
        _ => expected == observed,
    }
}

fn unknown_signature(message: &DynamicMessage) -> BTreeMap<u32, Vec<(String, Vec<u8>)>> {
    let mut grouped = BTreeMap::new();
    for unknown in message.unknown_fields() {
        let mut bytes = Vec::new();
        unknown.encode(&mut bytes);
        // Sort distinct field numbers, but preserve the complete wire order
        // within one field. A future descriptor may reinterpret a mixture of
        // packed and unpacked values as one repeated field.
        grouped.entry(unknown.number()).or_insert_with(Vec::new).push((format!("{:?}", unknown.wire_type()), bytes));
    }
    grouped
}

fn mismatch(path: impl Into<String>, expected: impl Into<String>, observed: impl Into<String>) -> SemanticMismatch {
    SemanticMismatch { path: path.into(), expected: expected.into(), observed: observed.into() }
}

fn value_summary(value: &Value) -> String {
    match value {
        Value::Bytes(value) => format!("bytes:{}", hex::encode(value)),
        Value::String(value) => format!("{value:?}"),
        Value::Message(_) => "message".to_owned(),
        Value::List(value) => format!("list[{} items]", value.len()),
        Value::Map(value) => format!("map[{} entries]", value.len()),
        value => format!("{value:?}"),
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;
    use prost_reflect::DescriptorPool;
    use prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet, MessageOptions, MethodDescriptorProto, ServiceDescriptorProto, field_descriptor_proto};

    use super::*;

    fn map_descriptor() -> MessageDescriptor {
        let entry = DescriptorProto {
            name: Some("LabelsEntry".to_owned()),
            field: vec![
                FieldDescriptorProto { name: Some("key".to_owned()), number: Some(1), label: Some(field_descriptor_proto::Label::Optional as i32), r#type: Some(field_descriptor_proto::Type::String as i32), ..Default::default() },
                FieldDescriptorProto { name: Some("value".to_owned()), number: Some(2), label: Some(field_descriptor_proto::Label::Optional as i32), r#type: Some(field_descriptor_proto::Type::Int32 as i32), ..Default::default() },
            ],
            options: Some(MessageOptions { map_entry: Some(true), ..Default::default() }),
            ..Default::default()
        };
        let message = DescriptorProto {
            name: Some("Envelope".to_owned()),
            field: vec![FieldDescriptorProto {
                name: Some("labels".to_owned()), number: Some(1),
                label: Some(field_descriptor_proto::Label::Repeated as i32),
                r#type: Some(field_descriptor_proto::Type::Message as i32),
                type_name: Some(".example.Envelope.LabelsEntry".to_owned()), ..Default::default()
            }],
            nested_type: vec![entry], ..Default::default()
        };
        let files = FileDescriptorSet { file: vec![FileDescriptorProto {
            name: Some("example.proto".to_owned()), package: Some("example".to_owned()),
            syntax: Some("proto3".to_owned()), message_type: vec![message], ..Default::default()
        }] };
        let pool = DescriptorPool::decode(files.encode_to_vec().as_slice()).expect("descriptor");
        pool.get_message_by_name("example.Envelope").expect("message")
    }

    fn encoded(entries: &[(&str, i32)]) -> Vec<u8> {
        // Each map entry is field 1, containing key field 1 and value field 2.
        let mut output = Vec::new();
        for (key, value) in entries {
            let mut entry = Vec::new();
            entry.push(0x0a); entry.push(key.len() as u8); entry.extend_from_slice(key.as_bytes());
            entry.push(0x10); entry.push(*value as u8);
            output.push(0x0a); output.push(entry.len() as u8); output.extend_from_slice(&entry);
        }
        output
    }

    #[test]
    fn map_order_is_semantic() {
        let descriptor = map_descriptor();
        compare_message(descriptor.clone(), &encoded(&[("a", 1), ("b", 2)]), &encoded(&[("b", 2), ("a", 1)])).expect("map order is irrelevant");
        let error = compare_message(descriptor, &encoded(&[("a", 1)]), &encoded(&[("a", 2)])).expect_err("typed value changed");
        assert!(error.path.contains("labels"));
    }

    fn presence_descriptor() -> MessageDescriptor {
        let message = DescriptorProto {
            name: Some("Presence".to_owned()),
            oneof_decl: vec![
                prost_types::OneofDescriptorProto { name: Some("_explicit".to_owned()), ..Default::default() },
                prost_types::OneofDescriptorProto { name: Some("choice".to_owned()), ..Default::default() },
            ],
            field: vec![
                FieldDescriptorProto { name: Some("explicit".to_owned()), number: Some(1), label: Some(field_descriptor_proto::Label::Optional as i32), r#type: Some(field_descriptor_proto::Type::Int32 as i32), proto3_optional: Some(true), oneof_index: Some(0), ..Default::default() },
                FieldDescriptorProto { name: Some("left".to_owned()), number: Some(2), label: Some(field_descriptor_proto::Label::Optional as i32), r#type: Some(field_descriptor_proto::Type::String as i32), oneof_index: Some(1), ..Default::default() },
                FieldDescriptorProto { name: Some("right".to_owned()), number: Some(3), label: Some(field_descriptor_proto::Label::Optional as i32), r#type: Some(field_descriptor_proto::Type::String as i32), oneof_index: Some(1), ..Default::default() },
                FieldDescriptorProto { name: Some("float_value".to_owned()), number: Some(4), label: Some(field_descriptor_proto::Label::Optional as i32), r#type: Some(field_descriptor_proto::Type::Float as i32), ..Default::default() },
            ],
            ..Default::default()
        };
        let files = FileDescriptorSet { file: vec![FileDescriptorProto {
            name: Some("presence.proto".to_owned()), package: Some("example".to_owned()),
            syntax: Some("proto3".to_owned()), message_type: vec![message], ..Default::default()
        }] };
        let pool = DescriptorPool::decode(files.encode_to_vec().as_slice()).expect("descriptor");
        pool.get_message_by_name("example.Presence").expect("message")
    }

    fn rpc_pool() -> DescriptorPool {
        let request = DescriptorProto { name: Some("Request".to_owned()), field: vec![FieldDescriptorProto { name: Some("value".to_owned()), number: Some(1), label: Some(field_descriptor_proto::Label::Optional as i32), r#type: Some(field_descriptor_proto::Type::Int32 as i32), ..Default::default() }], ..Default::default() };
        let response = DescriptorProto { name: Some("Response".to_owned()), field: vec![FieldDescriptorProto { name: Some("value".to_owned()), number: Some(1), label: Some(field_descriptor_proto::Label::Optional as i32), r#type: Some(field_descriptor_proto::Type::String as i32), ..Default::default() }], ..Default::default() };
        let file = FileDescriptorProto {
            name: Some("rpc.proto".to_owned()), package: Some("example".to_owned()), syntax: Some("proto3".to_owned()),
            message_type: vec![request, response],
            service: vec![ServiceDescriptorProto { name: Some("Service".to_owned()), method: vec![MethodDescriptorProto { name: Some("Call".to_owned()), input_type: Some(".example.Request".to_owned()), output_type: Some(".example.Response".to_owned()), ..Default::default() }], ..Default::default() }],
            ..Default::default()
        };
        DescriptorPool::decode(FileDescriptorSet { file: vec![file] }.encode_to_vec().as_slice()).expect("RPC descriptor pool")
    }

    fn enum_and_repeated_descriptor() -> MessageDescriptor {
        let message = DescriptorProto {
            name: Some("StateEnvelope".to_owned()),
            field: vec![
                FieldDescriptorProto {
                    name: Some("state".to_owned()),
                    number: Some(1),
                    label: Some(field_descriptor_proto::Label::Optional as i32),
                    r#type: Some(field_descriptor_proto::Type::Enum as i32),
                    type_name: Some(".example.State".to_owned()),
                    ..Default::default()
                },
                FieldDescriptorProto {
                    name: Some("items".to_owned()),
                    number: Some(2),
                    label: Some(field_descriptor_proto::Label::Repeated as i32),
                    r#type: Some(field_descriptor_proto::Type::String as i32),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let state = prost_types::EnumDescriptorProto {
            name: Some("State".to_owned()),
            value: vec![
                prost_types::EnumValueDescriptorProto { name: Some("STATE_UNSPECIFIED".to_owned()), number: Some(0), options: None },
                prost_types::EnumValueDescriptorProto { name: Some("READY".to_owned()), number: Some(1), options: None },
                prost_types::EnumValueDescriptorProto { name: Some("DONE".to_owned()), number: Some(2), options: None },
            ],
            ..Default::default()
        };
        let file = FileDescriptorProto {
            name: Some("enum.proto".to_owned()),
            package: Some("example".to_owned()),
            syntax: Some("proto3".to_owned()),
            message_type: vec![message],
            enum_type: vec![state],
            ..Default::default()
        };
        DescriptorPool::decode(FileDescriptorSet { file: vec![file] }.encode_to_vec().as_slice())
            .expect("enum descriptor pool")
            .get_message_by_name("example.StateEnvelope")
            .expect("state envelope")
    }

    #[test]
    fn presence_oneof_unknown_and_nan_policies_are_explicit() {
        let descriptor = presence_descriptor();
        let presence = compare_message(descriptor.clone(), &[], &[0x08, 0x00]).expect_err("explicit default presence changed");
        assert!(presence.path.contains("explicit"));

        let oneof = compare_message(descriptor.clone(), &[0x12, 0x01, b'a'], &[0x1a, 0x01, b'a']).expect_err("oneof case changed");
        assert!(oneof.path.contains("left") || oneof.path.contains("right"));

        let nan_a = [0x25, 0x01, 0x00, 0x80, 0x7f];
        let nan_b = [0x25, 0x02, 0x00, 0x80, 0x7f];
        compare_message(descriptor.clone(), &nan_a, &nan_b).expect_err("bitwise NaN policy rejects payload change");
        compare_message_with_options(descriptor, &nan_a, &nan_b, CompareOptions { floats: FloatPolicy::AllNaNsEqual, ..CompareOptions::default() }).expect("semantic NaN policy accepts payload change");

        let unknown_descriptor = map_descriptor();
        compare_message(unknown_descriptor.clone(), &[], &[0x48, 0x01]).expect_err("default policy checks unknown fields");
        compare_message_with_options(unknown_descriptor, &[], &[0x48, 0x01], CompareOptions { unknown_fields: UnknownFieldPolicy::Ignore, ..CompareOptions::default() }).expect("forward-compatible policy ignores unknown fields");
    }

    #[test]
    fn unknown_fields_sort_by_identity_but_preserve_repeated_order() {
        let descriptor = map_descriptor();
        // Unknown fields with distinct numbers are canonicalized by identity.
        compare_message(descriptor.clone(), &[0x48, 0x01, 0x50, 0x02], &[0x50, 0x02, 0x48, 0x01]).expect("distinct unknown field order is irrelevant");
        // Repeated values for one unknown field retain their wire order.
        compare_message(descriptor.clone(), &[0x48, 0x01, 0x48, 0x02], &[0x48, 0x02, 0x48, 0x01]).expect_err("repeated unknown order is meaningful");
        // Unknown varints are re-encoded canonically before comparison.
        compare_message(descriptor, &[0x48, 0x81, 0x00], &[0x48, 0x01]).expect("equivalent varint encodings are canonicalized");
    }

    #[test]
    fn mixed_packed_unknown_values_preserve_field_order() {
        let descriptor = map_descriptor();
        // Field 10 alternates unpacked varints and a length-delimited packed
        // segment. Reordering those entries must remain observable.
        let original = [0x50, 0x01, 0x52, 0x02, 0x02, 0x03, 0x50, 0x04];
        let reordered = [0x50, 0x01, 0x50, 0x04, 0x52, 0x02, 0x02, 0x03];
        compare_message(descriptor, &original, &reordered).expect_err("mixed packed and unpacked order is meaningful");
    }

    #[test]
    fn rpc_binding_rejects_unknown_and_wrong_direction() {
        let pool = rpc_pool();
        compare_rpc_message(&pool, "/example.Service/Call", RpcDirection::Request, &[0x08, 0x01], &[0x08, 0x01]).expect("request binds to input type");
        let wrong_direction = compare_rpc_message(&pool, "/example.Service/Call", RpcDirection::Response, &[0x08, 0x01], &[0x08, 0x01]).expect_err("request bytes do not satisfy response descriptor");
        assert!(matches!(wrong_direction, RpcSemanticError::Semantic(_)));
        let unknown = compare_rpc_message(&pool, "/example.Service/Missing", RpcDirection::Request, &[], &[]).expect_err("unknown method rejected");
        assert!(matches!(unknown, RpcSemanticError::UnknownMethod(_)));
    }

    #[test]
    fn family_registry_binds_receipt_frames_to_rust_descriptors() {
        compare_family_rpc_message(
            "actors",
            "acyclic.actors.v1.ActorsService/CreateActor",
            RpcDirection::Request,
            &[],
            &[],
        )
        .expect("known family and method resolve through the Rust registry");
        let unknown = compare_family_rpc_message(
            "not-a-family",
            "acyclic.actors.v1.ActorsService/CreateActor",
            RpcDirection::Request,
            &[],
            &[],
        )
        .expect_err("receipt cannot select a separately authored family");
        assert!(matches!(unknown, RpcSemanticError::UnknownFamily(_)));
    }

    #[test]
    fn descriptor_streaming_shape_is_rust_owned() {
        let read = family_rpc_streaming("stream", "acyclic.stream.v2.StreamService/Read")
            .expect("Stream Read is registered");
        assert!(!read.client_streaming);
        assert!(read.server_streaming);

        let put = family_rpc_streaming("objects", "acyclic.objects.v2.ObjectsService/PutObject")
            .expect("Objects PutObject is registered");
        assert!(put.client_streaming);
        assert!(!put.server_streaming);

        let unknown = family_rpc_streaming("stream", "acyclic.stream.v2.StreamService/Missing")
            .expect_err("unknown RPC shape must fail closed");
        assert!(matches!(unknown, RpcSemanticError::UnknownMethod(_)));
    }

    #[test]
    fn inference_semantic_pool_retains_rust_owned_option_closure() {
        let bytes = crate::inference_descriptor_with_options();
        let descriptor_set = FileDescriptorSet::decode(bytes.as_slice())
            .expect("Rust-owned Inference descriptor closure decodes");
        let options = descriptor_set
            .file
            .iter()
            .find(|file| file.name.as_deref() == Some("validation/v1/options.proto"))
            .expect("validation option descriptor is included in the closure");
        assert!(!options.extension.is_empty(), "validation extension definitions are retained");
        let pool = DescriptorPool::decode(bytes.as_slice())
            .expect("Rust-owned Inference descriptor closure resolves");
        assert!(pool
            .get_service_by_name("inference.customer.v1.RunsService")
            .is_some());
    }

    #[test]
    fn every_rust_registry_rpc_binds_input_and_output_descriptors() {
        let family_views = crate::family_registry::FAMILY_VIEWS;
        assert_eq!(family_views.len(), 8, "all Rust-owned families are registered");
        let mut total_methods = 0;
        for family in family_views {
            let descriptor = family.model.descriptor();
            let descriptor_set = FileDescriptorSet::decode(descriptor.as_slice())
                .unwrap_or_else(|error| panic!("{} descriptor set decodes: {error}", family.name));
            let mut family_methods = 0;
            for file in descriptor_set.file {
                let package = file.package.as_deref().unwrap_or_default();
                for service in file.service {
                    let service_name = if package.is_empty() {
                        service.name.clone().unwrap_or_default()
                    } else {
                        format!("{package}.{}", service.name.as_deref().unwrap_or_default())
                    };
                    for method in service.method {
                        let rpc = format!(
                            "{service_name}/{}",
                            method.name.as_deref().unwrap_or_default()
                        );
                        compare_family_rpc_message(
                            family.name,
                            &rpc,
                            RpcDirection::Request,
                            &[],
                            &[],
                        )
                        .unwrap_or_else(|error| {
                            panic!("{} request {rpc} binds: {error}", family.name)
                        });
                        compare_family_rpc_message(
                            family.name,
                            &rpc,
                            RpcDirection::Response,
                            &[],
                            &[],
                        )
                        .unwrap_or_else(|error| {
                            panic!("{} response {rpc} binds: {error}", family.name)
                        });
                        family_methods += 1;
                    }
                }
            }
            assert!(family_methods > 0, "{0} has no descriptor-bound RPCs", family.name);
            total_methods += family_methods;
        }
        assert_eq!(total_methods, 106, "registry descriptor RPC inventory remains complete");
    }

    #[test]
    fn enum_values_and_repeated_order_are_semantic() {
        let descriptor = enum_and_repeated_descriptor();
        // state=READY, items=["a","b"].
        let expected = [0x08, 0x01, 0x12, 0x01, b'a', 0x12, 0x01, b'b'];
        // Map-like reordering is allowed only for map fields; repeated order is
        // part of the Rust-owned contract.
        let reordered = [0x08, 0x01, 0x12, 0x01, b'b', 0x12, 0x01, b'a'];
        compare_message(descriptor.clone(), &expected, &expected).expect("equal enum and repeated values");
        let repeated_error = compare_message(descriptor.clone(), &expected, &reordered)
            .expect_err("repeated order changed");
        assert!(repeated_error.path.contains("items"));
        let enum_error = compare_message(descriptor, &expected, &[0x08, 0x02, 0x12, 0x01, b'a', 0x12, 0x01, b'b'])
            .expect_err("enum value changed");
        assert!(enum_error.path.contains("state"));
    }
}
