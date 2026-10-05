//! Rust-owned strongest practical Swift and C++ projections.
//!
//! The generated files are the public semantic boundary above protobuf.  The
//! field bindings, nested routes, and open unions all originate in
//! `type_policy.rs`; target code does not carry a second contract.

use std::collections::{BTreeMap, BTreeSet};

use crate::type_policy::{
    resolved_request_fields, resolved_response_fields, resolved_rpc_methods, semantic_type, PublicFieldDirection,
    PUBLIC_FIELD_BINDINGS, PUBLIC_NESTED_ROUTES, SEMANTIC_TYPES, SemanticRule, WireValueKind,
    WIRE_UNION_VARIANTS,
};
use prost_types::field_descriptor_proto::{Label as FieldLabel, Type as FieldType};

pub const SWIFT_TYPED_PATH: &str = "swift/RustTypedClients.swift";
pub const CPP_TYPED_PATH: &str = "cpp/include/acyclic/rust_typed_clients.hpp";

pub fn generate_swift_cpp_typed_facades() -> Vec<(&'static str, String)> {
    vec![(SWIFT_TYPED_PATH, render_swift()), (CPP_TYPED_PATH, render_cpp())]
}

fn camel(value: &str) -> String {
    value.split('_').filter(|part| !part.is_empty()).map(|part| {
        let mut chars = part.chars();
        chars.next().map(|first| first.to_uppercase().collect::<String>() + chars.as_str()).unwrap_or_default()
    }).collect()
}

fn cpp_method_name(value: &str) -> String {
    let mut chars = value.chars();
    let candidate = chars.next().map(|first| first.to_lowercase().collect::<String>() + chars.as_str()).unwrap_or_else(|| value.to_owned());
    if matches!(candidate.as_str(), "alignas" | "alignof" | "and" | "and_eq" | "asm" | "atomic_cancel" | "atomic_commit" | "atomic_noexcept" | "auto" | "bitand" | "bitor" | "bool" | "break" | "case" | "catch" | "char" | "char8_t" | "char16_t" | "char32_t" | "class" | "compl" | "concept" | "const" | "consteval" | "constexpr" | "constinit" | "const_cast" | "co_await" | "co_return" | "co_yield" | "decltype" | "default" | "delete" | "do" | "double" | "dynamic_cast" | "else" | "enum" | "explicit" | "export" | "extern" | "false" | "float" | "for" | "friend" | "goto" | "if" | "inline" | "int" | "long" | "mutable" | "namespace" | "new" | "noexcept" | "not" | "not_eq" | "nullptr" | "operator" | "or" | "or_eq" | "private" | "protected" | "public" | "reflexpr" | "register" | "reinterpret_cast" | "requires" | "return" | "short" | "signed" | "sizeof" | "static" | "static_assert" | "static_cast" | "struct" | "switch" | "synchronized" | "template" | "this" | "thread_local" | "throw" | "true" | "try" | "typedef" | "typeid" | "typename" | "union" | "unsigned" | "using" | "virtual" | "void" | "volatile" | "wchar_t" | "while" | "xor" | "xor_eq") {
        format!("rpc_{candidate}")
    } else {
        candidate
    }
}

fn swift_field_name(value: &str) -> String {
    if matches!(value, "associatedtype" | "as" | "break" | "case" | "catch" | "class" | "continue" | "convenience" | "default" | "defer" | "deinit" | "didSet" | "do" | "dynamic" | "else" | "enum" | "extension" | "fallthrough" | "false" | "fileprivate" | "final" | "for" | "func" | "get" | "guard" | "if" | "import" | "in" | "indirect" | "infix" | "init" | "inout" | "internal" | "is" | "let" | "mutating" | "nil" | "none" | "nonmutating" | "open" | "operator" | "optional" | "override" | "postfix" | "prefix" | "private" | "protocol" | "public" | "repeat" | "required" | "rethrows" | "return" | "self" | "set" | "some" | "static" | "struct" | "subscript" | "super" | "switch" | "throws" | "throw" | "true" | "try" | "typealias" | "unowned" | "var" | "weak" | "where" | "while" | "willSet") {
        format!("`{value}`")
    } else {
        value.to_owned()
    }
}

fn swift_method_name(value: &str) -> String {
    if matches!(value, "associatedtype" | "as" | "break" | "case" | "catch" | "class" | "continue" | "default" | "defer" | "deinit" | "do" | "else" | "enum" | "extension" | "fallthrough" | "false" | "for" | "func" | "guard" | "if" | "import" | "in" | "init" | "inout" | "internal" | "is" | "let" | "nil" | "none" | "open" | "operator" | "private" | "protocol" | "public" | "repeat" | "return" | "self" | "set" | "static" | "struct" | "subscript" | "super" | "switch" | "throw" | "throws" | "true" | "try" | "typealias" | "var" | "where" | "while") {
        format!("{}{}{}", "`", value, "`")
    } else {
        value.to_owned()
    }
}

fn swift_kind(kind: WireValueKind, name: &str) -> String {
    match kind { WireValueKind::String | WireValueKind::Bytes | WireValueKind::Message | WireValueKind::UnsignedInteger => name.to_owned(), WireValueKind::SignedInteger => "Int64".into(), WireValueKind::Boolean => "Bool".into(), WireValueKind::Timestamp => "Date".into(), WireValueKind::Enum | WireValueKind::Oneof => "WireChoice".into() }
}

fn cpp_kind(kind: WireValueKind, name: &str) -> String {
    match kind { WireValueKind::String | WireValueKind::Bytes | WireValueKind::Message | WireValueKind::UnsignedInteger => name.to_owned(), WireValueKind::SignedInteger => "std::int64_t".into(), WireValueKind::Boolean => "bool".into(), WireValueKind::Timestamp => "std::chrono::system_clock::time_point".into(), WireValueKind::Enum | WireValueKind::Oneof => "WireChoice".into() }
}

fn swift_checks(kind: WireValueKind, rules: &[SemanticRule]) -> String {
    let mut out = String::new();
    for rule in rules { match rule {
        SemanticRule::NonEmpty => match kind { WireValueKind::String | WireValueKind::Bytes => out.push_str(" guard !value.isEmpty else { return nil };"), WireValueKind::Message => out.push_str(" guard !value.wire.isEmpty else { return nil };"), _ => panic!("NonEmpty is not supported for this Swift semantic kind") },
        SemanticRule::Utf8 => if !matches!(kind, WireValueKind::String) { panic!("Utf8 requires a Swift String") },
        SemanticRule::NonNegative => match kind { WireValueKind::SignedInteger => out.push_str(" guard value >= 0 else { return nil };"), WireValueKind::UnsignedInteger => {}, _ => panic!("NonNegative requires an integer") },
        SemanticRule::StrictlyPositive => match kind { WireValueKind::SignedInteger | WireValueKind::UnsignedInteger => out.push_str(" guard value > 0 else { return nil };"), _ => panic!("StrictlyPositive requires an integer") },
        SemanticRule::FixedLength(n) => match kind { WireValueKind::String | WireValueKind::Bytes => out.push_str(&format!(" guard value.count == {n} else {{ return nil }};")), WireValueKind::Message => out.push_str(&format!(" guard value.wire.count == {n} else {{ return nil }};")), _ => panic!("FixedLength requires text, bytes, or a wire-preserving message") },
        SemanticRule::MaxBytes(n) => match kind { WireValueKind::String | WireValueKind::Bytes => out.push_str(&format!(" guard value.count <= {n} else {{ return nil }};")), _ => panic!("MaxBytes requires text or bytes") },
        SemanticRule::MaxItems(n) => match kind { WireValueKind::UnsignedInteger | WireValueKind::SignedInteger => out.push_str(&format!(" guard value <= {n} else {{ return nil }};")), _ => panic!("MaxItems requires an integer") },
        SemanticRule::BoundedInteger { min, max } => match kind { WireValueKind::SignedInteger | WireValueKind::UnsignedInteger => out.push_str(&format!(" guard value >= {min} && value <= {max} else {{ return nil }};")), _ => panic!("BoundedInteger requires an integer") },
        SemanticRule::Sha256Digest => match kind { WireValueKind::Bytes => out.push_str(" guard value.count == 32 else { return nil };"), _ => panic!("Sha256Digest requires bytes") },
        SemanticRule::CanonicalResourceName => match kind { WireValueKind::String => out.push_str(" guard !value.isEmpty && value == value.trimmingCharacters(in: .whitespacesAndNewlines) else { return nil };"), _ => panic!("CanonicalResourceName requires text") },
        SemanticRule::Immutable | SemanticRule::ExactOneof | SemanticRule::ExplicitPresence | SemanticRule::PreserveUnknownEnum | SemanticRule::PreserveUnknownOneof => {},
        SemanticRule::Monotonic => panic!("Monotonic requires cross-value state and cannot be projected to a scalar constructor"),
    } }
    out
}

fn cpp_checks(kind: WireValueKind, rules: &[SemanticRule]) -> String {
    let mut out = String::new();
    for rule in rules { match rule {
        SemanticRule::NonEmpty => match kind { WireValueKind::String | WireValueKind::Bytes => out.push_str(" if (value.empty()) throw std::invalid_argument(\"value must be non-empty\");"), WireValueKind::Message => out.push_str(" if (value.wire.empty()) throw std::invalid_argument(\"value must be non-empty\");"), _ => panic!("NonEmpty is not supported for this C++ semantic kind") },
        SemanticRule::Utf8 => if !matches!(kind, WireValueKind::String) { panic!("Utf8 requires a C++ string") },
        SemanticRule::NonNegative => match kind { WireValueKind::SignedInteger => out.push_str(" if (value < 0) throw std::invalid_argument(\"value must be non-negative\");"), WireValueKind::UnsignedInteger => {}, _ => panic!("NonNegative requires an integer") },
        SemanticRule::StrictlyPositive => match kind { WireValueKind::SignedInteger | WireValueKind::UnsignedInteger => out.push_str(" if (value <= 0) throw std::invalid_argument(\"value must be positive\");"), _ => panic!("StrictlyPositive requires an integer") },
        SemanticRule::FixedLength(n) => match kind { WireValueKind::String | WireValueKind::Bytes => out.push_str(&format!(" if (value.size() != {n}) throw std::invalid_argument(\"invalid fixed length\");")), WireValueKind::Message => out.push_str(&format!(" if (value.wire.size() != {n}) throw std::invalid_argument(\"invalid fixed wire length\");")), _ => panic!("FixedLength requires text, bytes, or a wire-preserving message") },
        SemanticRule::MaxBytes(n) => match kind { WireValueKind::String | WireValueKind::Bytes => out.push_str(&format!(" if (value.size() > {n}) throw std::invalid_argument(\"value exceeds maximum bytes\");")), _ => panic!("MaxBytes requires text or bytes") },
        SemanticRule::MaxItems(n) => match kind { WireValueKind::UnsignedInteger | WireValueKind::SignedInteger => out.push_str(&format!(" if (value > {n}) throw std::invalid_argument(\"value exceeds maximum\");")), _ => panic!("MaxItems requires an integer") },
        SemanticRule::BoundedInteger { min, max } => match kind { WireValueKind::SignedInteger | WireValueKind::UnsignedInteger => out.push_str(&format!(" if (value < {min} || value > {max}) throw std::invalid_argument(\"value outside bounds\");")), _ => panic!("BoundedInteger requires an integer") },
        SemanticRule::Sha256Digest => match kind { WireValueKind::Bytes => out.push_str(" if (value.size() != 32) throw std::invalid_argument(\"invalid digest length\");"), _ => panic!("Sha256Digest requires bytes") },
        SemanticRule::CanonicalResourceName => match kind { WireValueKind::String => out.push_str(" if (value.empty() || value.front() == ' ' || value.back() == ' ') throw std::invalid_argument(\"value must be canonical\");"), _ => panic!("CanonicalResourceName requires text") },
        SemanticRule::Immutable | SemanticRule::ExactOneof | SemanticRule::ExplicitPresence | SemanticRule::PreserveUnknownEnum | SemanticRule::PreserveUnknownOneof => {},
        SemanticRule::Monotonic => panic!("Monotonic requires cross-value state and cannot be projected to a scalar constructor"),
    } }
    out
}

fn swift_message_checks(rules: &[SemanticRule]) -> String {
    let mut out = String::new();
    for rule in rules {
        match rule {
            SemanticRule::NonEmpty => out.push_str(" guard !wire.wire.isEmpty else { return nil };"),
            SemanticRule::Immutable | SemanticRule::ExactOneof | SemanticRule::ExplicitPresence | SemanticRule::PreserveUnknownEnum | SemanticRule::PreserveUnknownOneof => {},
            SemanticRule::FixedLength(_) => {},
            other => panic!("unsupported message constructor rule: {other:?}"),
        }
    }
    out
}

fn swift_nested_value_checks(field: &crate::type_policy::ResolvedRequestField, rules: &[SemanticRule]) -> String {
    if field.field != "value" || field.wire_type != Some(FieldType::Bytes as i32) {
        return String::new();
    }
    let mut conditions = Vec::new();
    for rule in rules {
        match rule {
            SemanticRule::NonEmpty => conditions.push("!value.isEmpty".to_owned()),
            SemanticRule::FixedLength(n) => conditions.push(format!("value.count == {n}")),
            SemanticRule::Sha256Digest => conditions.push("value.count == 32".to_owned()),
            _ => {},
        }
    }
    if conditions.is_empty() { String::new() } else { format!(" guard let value, {} else {{ return nil }};", conditions.join(", ")) }
}

fn cpp_message_checks(rules: &[SemanticRule]) -> String {
    let mut out = String::new();
    for rule in rules {
        match rule {
            SemanticRule::NonEmpty => out.push_str(" if (value.wire.empty()) throw std::invalid_argument(\"value must be non-empty\");"),
            SemanticRule::FixedLength(n) => out.push_str(&format!(" if (value.wire.size() != {n}) throw std::invalid_argument(\"invalid fixed wire length\");")),
            SemanticRule::Immutable | SemanticRule::ExactOneof | SemanticRule::ExplicitPresence | SemanticRule::PreserveUnknownEnum | SemanticRule::PreserveUnknownOneof => {},
            other => panic!("unsupported message constructor rule: {other:?}"),
        }
    }
    out
}

fn semantic_request_groups() -> BTreeMap<(&'static str, &'static str), Vec<&'static crate::type_policy::PublicFieldBinding>> {
    let mut groups = BTreeMap::new();
    for binding in PUBLIC_FIELD_BINDINGS.iter().filter(|binding| binding.direction == PublicFieldDirection::Request) {
        groups.entry((binding.module, binding.message)).or_insert_with(Vec::new).push(binding);
    }
    groups
}

fn nested_message_groups() -> BTreeMap<&'static str, Vec<&'static crate::type_policy::PublicFieldBinding>> {
    let mut groups = BTreeMap::new();
    for binding in PUBLIC_FIELD_BINDINGS.iter().filter(|binding| binding.direction == PublicFieldDirection::NestedMessage) {
        groups.entry(binding.message).or_insert_with(Vec::new).push(binding);
    }
    groups
}

fn message_leaf(path: &str) -> &str {
    path.rsplit('.').next().unwrap_or(path)
}

fn descriptor_groups(fields: Vec<crate::type_policy::ResolvedRequestField>) -> BTreeMap<(String, String), Vec<crate::type_policy::ResolvedRequestField>> {
    let mut groups = BTreeMap::new();
    for field in fields {
        if message_leaf(&field.message_path) != message_leaf(&field.root_message) {
            continue;
        }
        let key = (field.family.clone(), message_leaf(&field.root_message).to_owned());
        let group = groups.entry(key).or_insert_with(Vec::new);
        if !group.iter().any(|existing: &crate::type_policy::ResolvedRequestField| existing.field == field.field) {
            group.push(field);
        }
    }
    groups
}

fn request_groups() -> BTreeMap<(String, String), Vec<crate::type_policy::ResolvedRequestField>> {
    let mut groups = descriptor_groups(resolved_request_fields().expect("Rust request descriptors must resolve"));
    for method in resolved_rpc_methods().expect("Rust RPC identities must resolve") {
        groups.entry((method.family, message_leaf(&method.input_message).to_owned())).or_default();
    }
    groups
}

fn response_groups() -> BTreeMap<(String, String), Vec<crate::type_policy::ResolvedRequestField>> {
    let mut groups = descriptor_groups(resolved_response_fields().expect("Rust response descriptors must resolve"));
    for method in resolved_rpc_methods().expect("Rust RPC identities must resolve") {
        groups.entry((method.family, message_leaf(&method.output_message).to_owned())).or_default();
    }
    groups
}

fn semantic_nested_fields(message: &str) -> Vec<crate::type_policy::ResolvedRequestField> {
    let mut fields = Vec::new();
    for field in resolved_request_fields().expect("Rust request descriptors must resolve") {
        if message_leaf(&field.message_path) == message
            && message_leaf(&field.message_path) != message_leaf(&field.root_message)
            && !fields.iter().any(|existing: &crate::type_policy::ResolvedRequestField| existing.field == field.field)
        {
            fields.push(field);
        }
    }
    fields
}

fn semantic_binding(family: &str, message: &str, field: &str, direction: PublicFieldDirection) -> Option<&'static crate::type_policy::PublicFieldBinding> {
    PUBLIC_FIELD_BINDINGS.iter().find(|binding| binding.family == family && binding.message == message && binding.field == field && binding.direction == direction)
}

fn swift_wire_type(field: &crate::type_policy::ResolvedRequestField) -> String {
    match field.wire_type {
        Some(kind) if kind == FieldType::String as i32 => "String".into(),
        Some(kind) if kind == FieldType::Bytes as i32 => "Data".into(),
        Some(kind) if kind == FieldType::Int64 as i32 || kind == FieldType::Sfixed64 as i32 || kind == FieldType::Sint64 as i32 => "Int64".into(),
        Some(kind) if kind == FieldType::Uint64 as i32 || kind == FieldType::Fixed64 as i32 => "UInt64".into(),
        Some(kind) if kind == FieldType::Int32 as i32 || kind == FieldType::Sfixed32 as i32 || kind == FieldType::Sint32 as i32 => "Int32".into(),
        Some(kind) if kind == FieldType::Uint32 as i32 || kind == FieldType::Fixed32 as i32 => "UInt32".into(),
        Some(kind) if kind == FieldType::Bool as i32 => "Bool".into(),
        Some(kind) if kind == FieldType::Double as i32 => "Double".into(),
        Some(kind) if kind == FieldType::Float as i32 => "Float".into(),
        Some(kind) if kind == FieldType::Enum as i32 => "RustWireEnum".into(),
        Some(kind) if kind == FieldType::Message as i32 || kind == FieldType::Group as i32 => "RustWireMessage".into(),
        _ => "Data".into(),
    }
}

fn cpp_wire_type(field: &crate::type_policy::ResolvedRequestField) -> String {
    match field.wire_type {
        Some(kind) if kind == FieldType::String as i32 => "std::string".into(),
        Some(kind) if kind == FieldType::Bytes as i32 => "std::vector<std::uint8_t>".into(),
        Some(kind) if kind == FieldType::Int64 as i32 || kind == FieldType::Sfixed64 as i32 || kind == FieldType::Sint64 as i32 => "std::int64_t".into(),
        Some(kind) if kind == FieldType::Uint64 as i32 || kind == FieldType::Fixed64 as i32 => "std::uint64_t".into(),
        Some(kind) if kind == FieldType::Int32 as i32 || kind == FieldType::Sfixed32 as i32 || kind == FieldType::Sint32 as i32 => "std::int32_t".into(),
        Some(kind) if kind == FieldType::Uint32 as i32 || kind == FieldType::Fixed32 as i32 => "std::uint32_t".into(),
        Some(kind) if kind == FieldType::Bool as i32 => "bool".into(),
        Some(kind) if kind == FieldType::Double as i32 => "double".into(),
        Some(kind) if kind == FieldType::Float as i32 => "float".into(),
        Some(kind) if kind == FieldType::Enum as i32 => "RustWireEnum".into(),
        Some(kind) if kind == FieldType::Message as i32 || kind == FieldType::Group as i32 => "RustWireMessage".into(),
        _ => "std::vector<std::uint8_t>".into(),
    }
}

fn swift_field_type(field: &crate::type_policy::ResolvedRequestField, direction: PublicFieldDirection) -> String {
    let message = if direction == PublicFieldDirection::NestedMessage { message_leaf(&field.message_path) } else { message_leaf(&field.root_message) };
    let base = semantic_binding(&field.family, message, &field.field, direction)
        .and_then(|binding| semantic_type(binding.semantic_type))
        .map(|semantic| swift_kind(semantic.wire_kind, semantic.rust_name))
        .unwrap_or_else(|| swift_wire_type(field));
    let repeated = field.label == Some(FieldLabel::Repeated as i32);
    let optional = !repeated && (field.proto3_optional || field.oneof_index.is_some());
    let value = if repeated { format!("[{base}]") } else { base };
    if optional { format!("{value}?") } else { value }
}

fn cpp_field_type(field: &crate::type_policy::ResolvedRequestField, direction: PublicFieldDirection) -> String {
    let message = if direction == PublicFieldDirection::NestedMessage { message_leaf(&field.message_path) } else { message_leaf(&field.root_message) };
    let base = semantic_binding(&field.family, message, &field.field, direction)
        .and_then(|binding| semantic_type(binding.semantic_type))
        .map(|semantic| cpp_kind(semantic.wire_kind, semantic.rust_name))
        .unwrap_or_else(|| cpp_wire_type(field));
    let repeated = field.label == Some(FieldLabel::Repeated as i32);
    let optional = !repeated && (field.proto3_optional || field.oneof_index.is_some());
    let value = if repeated { format!("std::vector<{base}>") } else { base };
    if optional { format!("std::optional<{value}>") } else { value }
}

fn swift_field_cast(field: &crate::type_policy::ResolvedRequestField) -> String {
    let base = swift_wire_type(field);
    if field.label == Some(FieldLabel::Repeated as i32) { format!("[{base}]") } else { base }
}

fn swift_wire_cast(kind: WireValueKind) -> &'static str {
    match kind {
        WireValueKind::String => "String",
        WireValueKind::Message => "RustWireMessage",
        WireValueKind::Bytes => "Data",
        WireValueKind::UnsignedInteger => "UInt64",
        WireValueKind::SignedInteger => "Int64",
        WireValueKind::Boolean => "Bool",
        WireValueKind::Timestamp => "Date",
        WireValueKind::Enum | WireValueKind::Oneof => "Data",
    }
}

fn render_swift() -> String {
    let mut out = String::from("// Generated by acyclic-sdk-contract-wire; do not edit.\nimport Foundation\n\npublic enum RustWireDecodeError: Error { case missingField(String); case invalidField(String) }\npublic struct RustWireMessage: Sendable { public let wire: Data; public init(wire: Data) { self.wire = wire } }\npublic struct RustWireEnum: Sendable { public let raw: Int32; public init(raw: Int32) { self.raw = raw } }\npublic protocol RustWireRequest { associatedtype Wire; func toWire() -> Wire }\npublic protocol RustWireResponse { associatedtype Wire; static func fromWire(_ wire: Wire) throws -> Self }\n\n");
    let mut seen = BTreeSet::new();
    for item in SEMANTIC_TYPES {
        if !seen.insert(item.rust_name) { continue; }
        match item.wire_kind {
            WireValueKind::String => out.push_str(&format!("public struct {}: Sendable {{ public let value: String; public init?(_ value: String) {{{} self.value = value }} }}\n", item.rust_name, swift_checks(item.wire_kind, item.rules))),
            WireValueKind::Bytes => out.push_str(&format!("public struct {}: Sendable {{ public let value: Data; public init?(_ value: Data) {{{} self.value = value }} }}\n", item.rust_name, swift_checks(item.wire_kind, item.rules))),
            WireValueKind::UnsignedInteger => out.push_str(&format!("public struct {}: Sendable {{ public let value: UInt64; public init?(_ value: UInt64) {{{} self.value = value }} }}\n", item.rust_name, swift_checks(item.wire_kind, item.rules))),
            WireValueKind::SignedInteger => out.push_str(&format!("public struct {}: Sendable {{ public let value: Int64; public init?(_ value: Int64) {{{} self.value = value }} }}\n", item.rust_name, swift_checks(item.wire_kind, item.rules))),
            WireValueKind::Message => {
                let nested = semantic_nested_fields(item.rust_name);
                let nested_type = |field: &crate::type_policy::ResolvedRequestField| {
                    let ty = swift_field_type(field, PublicFieldDirection::NestedMessage);
                    if ty.ends_with('?') { ty } else { format!("{ty}?") }
                };
                let declarations = nested.iter().map(|field| format!("public let {}: {};\n", swift_field_name(&field.field), nested_type(field))).collect::<String>();
                let params = nested.iter().map(|field| format!("{}: {} = nil", swift_field_name(&field.field), nested_type(field))).collect::<Vec<_>>().join(", ");
                let assignments = nested.iter().map(|field| format!("self.{0} = {0}\n", swift_field_name(&field.field))).collect::<String>();
                let raw_assignments = nested.iter().map(|field| format!("self.{} = nil\n", swift_field_name(&field.field))).collect::<String>();
                let typed_checks = nested.iter().map(|field| swift_nested_value_checks(field, item.rules)).collect::<String>();
                out.push_str(&format!("public struct {}: Sendable {{\n public let wire: RustWireMessage\n{} public init?(_ wire: RustWireMessage) {{{} self.wire = wire\n{} }}\n public init?(wire: RustWireMessage, {}) {{{} self.wire = wire\n{} }}\n}}\n", item.rust_name, declarations, swift_message_checks(item.rules), raw_assignments, params, typed_checks, assignments));
            }
            _ => {}
        }
    }
    out.push_str("\npublic enum WireChoice: Sendable { case known(tag: String, payload: Data); case unknown(rawTag: Int32, payload: Data) }\n\n");
    for ((module, message), fields) in request_groups() {
        let name = format!("{}{}Request", camel(&module), camel(message.trim_end_matches("Request")));
        let field_declarations = fields.iter().map(|field| format!("public let {}: {};\n", swift_field_name(&field.field), swift_field_type(field, PublicFieldDirection::Request))).collect::<String>();
        let params = fields.iter().map(|field| format!("{}: {}", swift_field_name(&field.field), swift_field_type(field, PublicFieldDirection::Request))).collect::<Vec<_>>().join(", ");
        let assignments = fields.iter().map(|field| format!("self.{0} = {0}\n", swift_field_name(&field.field))).collect::<String>();
        let wire_fields = fields.iter().map(|field| format!("\"{}\": {}", field.json_name, swift_field_name(&field.field))).collect::<Vec<_>>().join(", ");
        let wire_literal = if wire_fields.is_empty() { "[:]".to_owned() } else { format!("[{wire_fields}]") };
        out.push_str(&format!("public struct {name}: RustWireRequest, Sendable {{\n{field_declarations} public init({params}) {{\n{assignments} }}\n public typealias Wire = [String: Any]\n public func toWire() -> [String: Any] {{ {wire_literal} }}\n}}\n"));
    }
    for ((module, message), fields) in response_groups() {
        let name = format!("{}{}Response", camel(&module), camel(&message));
        let field_declarations = fields.iter().map(|field| format!("public let {}: {};\n", swift_field_name(&field.field), swift_field_type(field, PublicFieldDirection::Response))).collect::<String>();
        let params = fields.iter().map(|field| format!("{}: {}", swift_field_name(&field.field), swift_field_type(field, PublicFieldDirection::Response))).collect::<Vec<_>>().join(", ");
        let assignments = fields.iter().map(|field| format!("self.{0} = {0}\n", swift_field_name(&field.field))).collect::<String>();
        let decodes = fields.iter().map(|field| {
            let cast = swift_field_cast(field);
            let key = &field.json_name;
            if field.proto3_optional || field.oneof_index.is_some() {
                format!("let {field} = wire[\"{key}\"] as? {cast};", field=swift_field_name(&field.field), key=key, cast=cast)
            } else if let Some(binding) = semantic_binding(&field.family, message_leaf(&field.root_message), &field.field, PublicFieldDirection::Response) {
                let semantic = semantic_type(binding.semantic_type).expect("Rust policy response binding");
                format!("guard let raw_{field} = wire[\"{key}\"] as? {cast}, let {field} = {ty}(raw_{field}) else {{ throw RustWireDecodeError.invalidField(\"{key}\") }};", field=swift_field_name(&field.field), key=key, cast=swift_wire_cast(semantic.wire_kind), ty=semantic.rust_name)
            } else {
                format!("guard let {field} = wire[\"{key}\"] as? {cast} else {{ throw RustWireDecodeError.invalidField(\"{key}\") }};", field=swift_field_name(&field.field), key=key, cast=cast)
            }
        }).collect::<String>();
        let args = fields.iter().map(|field| {
            let name = swift_field_name(&field.field);
            format!("{name}: {name}")
        }).collect::<Vec<_>>().join(", ");
        out.push_str(&format!("public struct {name}: RustWireResponse, Sendable {{\n{field_declarations} public init({params}) {{\n{assignments} }}\n public typealias Wire = [String: Any]\n public static func fromWire(_ wire: [String: Any]) throws -> Self {{ {decodes} return Self({args}) }}\n}}\n"));
    }
    for (message, bindings) in nested_message_groups() {
        let fields = bindings.iter().map(|binding| {
            let semantic = semantic_type(binding.semantic_type).expect("Rust policy nested binding");
            format!("public let {}: {};", swift_field_name(binding.field), swift_kind(semantic.wire_kind, semantic.rust_name))
        }).collect::<String>();
        let params = bindings.iter().map(|binding| {
            let semantic = semantic_type(binding.semantic_type).expect("Rust policy nested binding");
            format!("{}: {}", swift_field_name(binding.field), swift_kind(semantic.wire_kind, semantic.rust_name))
        }).collect::<Vec<_>>().join(", ");
        let assignments = bindings.iter().map(|binding| format!("self.{0} = {0};", swift_field_name(binding.field))).collect::<String>();
        out.push_str(&format!("public struct {}: Sendable {{ {} public init({}) {{ {} }} }}\n", camel(message), fields, params, assignments));
    }
    let nested_messages = PUBLIC_NESTED_ROUTES.iter().flat_map(|route| route.fields.iter()).filter_map(|(_, kind)| match kind {
        crate::type_policy::PublicNestedFieldKind::Message(message) => Some(*message),
        crate::type_policy::PublicNestedFieldKind::Text => None,
    }).collect::<BTreeSet<_>>();
    for message in nested_messages {
        if !nested_message_groups().contains_key(message) {
            out.push_str(&format!("public struct {}: Sendable {{ public let wire: Data; public init(wire: Data) {{ self.wire = wire }} }}\n", camel(message)));
        }
    }
    for route in PUBLIC_NESTED_ROUTES {
        let fields = route.fields.iter().map(|(field, kind)| {
            let ty = match kind {
                crate::type_policy::PublicNestedFieldKind::Text => "String".to_owned(),
                crate::type_policy::PublicNestedFieldKind::Message(message) => camel(message),
            };
            format!("public let {}: {ty};", swift_field_name(field))
        }).collect::<String>();
        let params = route.fields.iter().map(|(field, kind)| {
            let ty = match kind {
                crate::type_policy::PublicNestedFieldKind::Text => "String".to_owned(),
                crate::type_policy::PublicNestedFieldKind::Message(message) => camel(message),
            };
            format!("{}: {ty}", swift_field_name(field))
        }).collect::<Vec<_>>().join(", ");
        let assignments = route.fields.iter().map(|(field, _)| format!("self.{0} = {0};", swift_field_name(field))).collect::<String>();
        out.push_str(&format!("public struct {}{}NestedRequest: Sendable {{ {} public init({}) {{ {} }} }}\n", camel(route.module), camel(route.operation), fields, params, assignments));
    }
    out.push_str("\npublic protocol RustTypedRemoteTransport { func call<Request: RustWireRequest, Response: RustWireResponse>(_ rpc: String, _ request: Request) async throws -> Response }\npublic struct RustTypedClient<Transport: RustTypedRemoteTransport> { public let transport: Transport; public init(transport: Transport) { self.transport = transport }\n");
    let mut methods = BTreeSet::new();
    for method in resolved_rpc_methods().expect("Rust RPC identities must resolve") {
        let message = message_leaf(&method.input_message);
        let module = semantic_request_groups().iter().find_map(|((module, candidate), bindings)| {
            (*candidate == message && bindings.first().map(|binding| binding.family == method.family).unwrap_or(false)).then_some(*module)
        }).unwrap_or(method.family.as_str());
        if methods.insert((method.family.clone(), method.rpc.clone(), message.to_owned())) {
            let request = format!("{}{}Request", camel(module), camel(message.trim_end_matches("Request")));
            let mut method_chars = method.method.chars();
            let method_name = method_chars.next().map(|first| first.to_lowercase().collect::<String>() + method_chars.as_str()).unwrap_or_else(|| method.method.clone());
            out.push_str(&format!(" public func {}<Response: RustWireResponse>(_ request: {}) async throws -> Response {{ try await transport.call(\"{}\", request) }}\n", swift_method_name(&method_name), request, method.rpc));
        }
    }
    out.push_str("}\n");
    out
}

fn render_cpp() -> String {
    let mut out = String::from("// Generated by acyclic-sdk-contract-wire; do not edit.\n#pragma once\n#include <chrono>\n#include <cstdint>\n#include <optional>\n#include <stdexcept>\n#include <string>\n#include <utility>\n#include <variant>\n#include <vector>\nnamespace acyclic::rust_typed {\n\nstruct RustWireMessage { std::vector<std::uint8_t> wire; };\nstruct RustWireEnum { std::int32_t raw; };\n\n");
    let mut seen = BTreeSet::new();
    for item in SEMANTIC_TYPES {
        if !seen.insert(item.rust_name) { continue; }
        match item.wire_kind {
            WireValueKind::String => out.push_str(&format!("struct {} {{ std::string value; explicit {}(std::string value) : value(std::move(value)) {{{}}} }};\n", item.rust_name, item.rust_name, cpp_checks(item.wire_kind, item.rules))),
            WireValueKind::Bytes => out.push_str(&format!("struct {} {{ std::vector<std::uint8_t> value; explicit {}(std::vector<std::uint8_t> value) : value(std::move(value)) {{{}}} }};\n", item.rust_name, item.rust_name, cpp_checks(item.wire_kind, item.rules))),
            WireValueKind::UnsignedInteger => out.push_str(&format!("struct {} {{ std::uint64_t value; explicit {}(std::uint64_t value) : value(value) {{{}}} }};\n", item.rust_name, item.rust_name, cpp_checks(item.wire_kind, item.rules))),
            WireValueKind::SignedInteger => out.push_str(&format!("struct {} {{ std::int64_t value; explicit {}(std::int64_t value) : value(value) {{{}}} }};\n", item.rust_name, item.rust_name, cpp_checks(item.wire_kind, item.rules))),
            WireValueKind::Message => {
                let nested = semantic_nested_fields(item.rust_name);
                let declarations = nested.iter().map(|field| {
                    let ty = cpp_field_type(field, PublicFieldDirection::NestedMessage);
                    let ty = if ty.starts_with("std::optional<") { ty } else { format!("std::optional<{ty}>") };
                    format!("{ty} {};\n", field.field)
                }).collect::<String>();
                out.push_str(&format!("struct {} {{ RustWireMessage wire; {} explicit {}(RustWireMessage value) {{{} wire = std::move(value); }} }};\n", item.rust_name, declarations, item.rust_name, cpp_message_checks(item.rules)));
            }
            _ => {}
        }
    }
    let _ = WIRE_UNION_VARIANTS;
    out.push_str("struct KnownOneof { std::string tag; std::vector<std::uint8_t> payload; };\nstruct UnknownOneof { std::int32_t raw_tag; std::vector<std::uint8_t> payload; };\nusing WireChoice = std::variant<KnownOneof, UnknownOneof>;\n\n");
    for ((module, message), fields) in request_groups() {
        let name = format!("{}{}Request", camel(&module), camel(message.trim_end_matches("Request")));
        let fields = fields.iter().map(|field| format!("{} {};", cpp_field_type(field, PublicFieldDirection::Request), field.field)).collect::<String>();
        out.push_str(&format!("struct {name} {{ {fields} }};\n"));
    }
    for ((module, message), fields) in response_groups() {
        let name = format!("{}{}Response", camel(&module), camel(&message));
        let fields = fields.iter().map(|field| format!("{} {};", cpp_field_type(field, PublicFieldDirection::Response), field.field)).collect::<String>();
        out.push_str(&format!("struct {name} {{ {fields} }};\n"));
    }
    for (message, bindings) in nested_message_groups() {
        let fields = bindings.iter().map(|binding| {
            let semantic = semantic_type(binding.semantic_type).expect("Rust policy nested binding");
            format!("{} {};", cpp_kind(semantic.wire_kind, semantic.rust_name), binding.field)
        }).collect::<String>();
        out.push_str(&format!("struct {} {{ {} }};\n", camel(message), fields));
    }
    let nested_messages = PUBLIC_NESTED_ROUTES.iter().flat_map(|route| route.fields.iter()).filter_map(|(_, kind)| match kind {
        crate::type_policy::PublicNestedFieldKind::Message(message) => Some(*message),
        crate::type_policy::PublicNestedFieldKind::Text => None,
    }).collect::<BTreeSet<_>>();
    for message in nested_messages {
        if !nested_message_groups().contains_key(message) {
            out.push_str(&format!("struct {} {{ std::vector<std::uint8_t> wire; }};\n", camel(message)));
        }
    }
    for route in PUBLIC_NESTED_ROUTES {
        let fields = route.fields.iter().map(|(field, kind)| {
            let ty = match kind {
                crate::type_policy::PublicNestedFieldKind::Text => "std::string".to_owned(),
                crate::type_policy::PublicNestedFieldKind::Message(message) => camel(message),
            };
            format!("{} {};", ty, field)
        }).collect::<String>();
        out.push_str(&format!("struct {}{}NestedRequest {{ {} }};\n", camel(route.module), camel(route.operation), fields));
    }
    out.push_str("\ntemplate<class Transport> class RustTypedClient { Transport& transport_; public: explicit RustTypedClient(Transport& transport) : transport_(transport) {} template<class Request, class Response> Response call(const char* rpc, const Request& request) { return transport_.template call<Request, Response>(rpc, request); }\n");
    let mut methods = BTreeSet::new();
    for method_info in resolved_rpc_methods().expect("Rust RPC identities must resolve") {
        let message = message_leaf(&method_info.input_message);
        let module = semantic_request_groups().iter().find_map(|((module, candidate), bindings)| {
            (*candidate == message && bindings.first().map(|binding| binding.family == method_info.family).unwrap_or(false)).then_some(*module)
        }).unwrap_or(method_info.family.as_str());
        if methods.insert((method_info.family.clone(), method_info.rpc.clone(), message.to_owned())) {
            let request = format!("{}{}Request", camel(module), camel(message.trim_end_matches("Request")));
            let method_name = cpp_method_name(&method_info.method);
            out.push_str(&format!(" template<class Response> Response {}(const {}& request) {{ return call<{}, Response>(\"{}\", request); }}\n", method_name, request, request, method_info.rpc));
        }
    }
    out.push_str("};\n}\n");
    out
}

#[cfg(test)]
mod tests {
    use super::generate_swift_cpp_typed_facades;
    #[test]
    fn emits_nominal_clients_and_open_unions() {
        let files = generate_swift_cpp_typed_facades();
        assert!(files.iter().any(|(_, source)| source.contains("RustTypedRemoteTransport")));
        assert!(files.iter().any(|(_, source)| source.contains("RustTypedClient")));
        assert!(files.iter().any(|(_, source)| source.contains("WireChoice")));
    }
}
