//! Rust-owned strongest practical Swift and C++ projections.
//!
//! The generated files are the public semantic boundary above protobuf.  The
//! field bindings, nested routes, and open unions all originate in
//! `type_policy.rs`; target code does not carry a second contract.

use std::collections::{BTreeMap, BTreeSet};

use crate::type_policy::{
    PUBLIC_FIELD_BINDINGS, PUBLIC_NESTED_ROUTES, PublicFieldDirection, ResolvedRequestField,
    SEMANTIC_TYPES, SemanticRule, WIRE_UNION_VARIANTS, WireValueKind, field_semantic_type,
    resolved_enum_fields, resolved_request_fields, resolved_response_fields, resolved_rpc_methods,
    semantic_type,
};
use prost_types::field_descriptor_proto::{Label as FieldLabel, Type as FieldType};

pub const SWIFT_TYPED_PATH: &str = "swift/RustTypedClients.swift";
pub const CPP_TYPED_PATH: &str = "cpp/include/acyclic/rust_typed_clients.hpp";

pub fn generate_swift_cpp_typed_facades() -> Vec<(&'static str, String)> {
    vec![
        (SWIFT_TYPED_PATH, render_swift()),
        (CPP_TYPED_PATH, render_cpp()),
    ]
}

fn camel(value: &str) -> String {
    value
        .split('_')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            chars
                .next()
                .map(|first| first.to_uppercase().collect::<String>() + chars.as_str())
                .unwrap_or_default()
        })
        .collect()
}

fn cpp_method_name(value: &str) -> String {
    let mut chars = value.chars();
    let candidate = chars
        .next()
        .map(|first| first.to_lowercase().collect::<String>() + chars.as_str())
        .unwrap_or_else(|| value.to_owned());
    if matches!(
        candidate.as_str(),
        "alignas"
            | "alignof"
            | "and"
            | "and_eq"
            | "asm"
            | "atomic_cancel"
            | "atomic_commit"
            | "atomic_noexcept"
            | "auto"
            | "bitand"
            | "bitor"
            | "bool"
            | "break"
            | "case"
            | "catch"
            | "char"
            | "char8_t"
            | "char16_t"
            | "char32_t"
            | "class"
            | "compl"
            | "concept"
            | "const"
            | "consteval"
            | "constexpr"
            | "constinit"
            | "const_cast"
            | "co_await"
            | "co_return"
            | "co_yield"
            | "decltype"
            | "default"
            | "delete"
            | "do"
            | "double"
            | "dynamic_cast"
            | "else"
            | "enum"
            | "explicit"
            | "export"
            | "extern"
            | "false"
            | "float"
            | "for"
            | "friend"
            | "goto"
            | "if"
            | "inline"
            | "int"
            | "long"
            | "mutable"
            | "namespace"
            | "new"
            | "noexcept"
            | "not"
            | "not_eq"
            | "nullptr"
            | "operator"
            | "or"
            | "or_eq"
            | "private"
            | "protected"
            | "public"
            | "reflexpr"
            | "register"
            | "reinterpret_cast"
            | "requires"
            | "return"
            | "short"
            | "signed"
            | "sizeof"
            | "static"
            | "static_assert"
            | "static_cast"
            | "struct"
            | "switch"
            | "synchronized"
            | "template"
            | "this"
            | "thread_local"
            | "throw"
            | "true"
            | "try"
            | "typedef"
            | "typeid"
            | "typename"
            | "union"
            | "unsigned"
            | "using"
            | "virtual"
            | "void"
            | "volatile"
            | "wchar_t"
            | "while"
            | "xor"
            | "xor_eq"
    ) {
        format!("rpc_{candidate}")
    } else {
        candidate
    }
}

fn swift_field_name(value: &str) -> String {
    if matches!(
        value,
        "associatedtype"
            | "as"
            | "break"
            | "case"
            | "catch"
            | "class"
            | "continue"
            | "convenience"
            | "default"
            | "defer"
            | "deinit"
            | "didSet"
            | "do"
            | "dynamic"
            | "else"
            | "enum"
            | "extension"
            | "fallthrough"
            | "false"
            | "fileprivate"
            | "final"
            | "for"
            | "func"
            | "get"
            | "guard"
            | "if"
            | "import"
            | "in"
            | "indirect"
            | "infix"
            | "init"
            | "inout"
            | "internal"
            | "is"
            | "let"
            | "mutating"
            | "nil"
            | "none"
            | "nonmutating"
            | "open"
            | "operator"
            | "optional"
            | "override"
            | "postfix"
            | "prefix"
            | "private"
            | "protocol"
            | "public"
            | "repeat"
            | "required"
            | "rethrows"
            | "return"
            | "self"
            | "set"
            | "some"
            | "static"
            | "struct"
            | "subscript"
            | "super"
            | "switch"
            | "throws"
            | "throw"
            | "true"
            | "try"
            | "typealias"
            | "unowned"
            | "var"
            | "weak"
            | "where"
            | "while"
            | "willSet"
    ) {
        format!("`{value}`")
    } else {
        value.to_owned()
    }
}

fn swift_local_name(value: &str) -> String {
    match value {
        "protocol" => "protocolValue".into(),
        "class" => "classValue".into(),
        "struct" => "structValue".into(),
        "self" => "selfValue".into(),
        _ => value.to_owned(),
    }
}

fn swift_method_name(value: &str) -> String {
    if matches!(
        value,
        "associatedtype"
            | "as"
            | "break"
            | "case"
            | "catch"
            | "class"
            | "continue"
            | "default"
            | "defer"
            | "deinit"
            | "do"
            | "else"
            | "enum"
            | "extension"
            | "fallthrough"
            | "false"
            | "for"
            | "func"
            | "guard"
            | "if"
            | "import"
            | "in"
            | "init"
            | "inout"
            | "internal"
            | "is"
            | "let"
            | "nil"
            | "none"
            | "open"
            | "operator"
            | "private"
            | "protocol"
            | "public"
            | "repeat"
            | "return"
            | "self"
            | "set"
            | "static"
            | "struct"
            | "subscript"
            | "super"
            | "switch"
            | "throw"
            | "throws"
            | "true"
            | "try"
            | "typealias"
            | "var"
            | "where"
            | "while"
    ) {
        format!("{}{}{}", "`", value, "`")
    } else {
        value.to_owned()
    }
}

fn cpp_field_name(value: &str) -> String {
    if matches!(
        value,
        "alignas"
            | "alignof"
            | "and"
            | "and_eq"
            | "asm"
            | "auto"
            | "bitand"
            | "bitor"
            | "bool"
            | "break"
            | "case"
            | "catch"
            | "char"
            | "char8_t"
            | "char16_t"
            | "char32_t"
            | "class"
            | "compl"
            | "concept"
            | "const"
            | "consteval"
            | "constexpr"
            | "constinit"
            | "const_cast"
            | "continue"
            | "co_await"
            | "co_return"
            | "co_yield"
            | "decltype"
            | "default"
            | "delete"
            | "do"
            | "double"
            | "dynamic_cast"
            | "else"
            | "enum"
            | "explicit"
            | "export"
            | "extern"
            | "false"
            | "float"
            | "for"
            | "friend"
            | "goto"
            | "if"
            | "inline"
            | "int"
            | "long"
            | "mutable"
            | "namespace"
            | "new"
            | "noexcept"
            | "not"
            | "not_eq"
            | "nullptr"
            | "operator"
            | "or"
            | "or_eq"
            | "private"
            | "protected"
            | "public"
            | "register"
            | "reinterpret_cast"
            | "requires"
            | "return"
            | "short"
            | "signed"
            | "sizeof"
            | "static"
            | "static_assert"
            | "static_cast"
            | "struct"
            | "switch"
            | "template"
            | "this"
            | "thread_local"
            | "throw"
            | "true"
            | "try"
            | "typedef"
            | "typeid"
            | "typename"
            | "union"
            | "unsigned"
            | "using"
            | "virtual"
            | "void"
            | "volatile"
            | "wchar_t"
            | "while"
            | "xor"
            | "xor_eq"
    ) {
        format!("field_{value}")
    } else {
        value.to_owned()
    }
}

fn cpp_field_annotation(value: &str) -> String {
    let projected = cpp_field_name(value);
    if projected == value {
        String::new()
    } else {
        format!(" /* Rust field {value} */")
    }
}

fn swift_kind(kind: WireValueKind, name: &str) -> String {
    match kind {
        WireValueKind::String
        | WireValueKind::Bytes
        | WireValueKind::Message
        | WireValueKind::UnsignedInteger => name.to_owned(),
        WireValueKind::SignedInteger => "Int64".into(),
        WireValueKind::Boolean => "Bool".into(),
        WireValueKind::Timestamp => "Date".into(),
        // Closed Rust-owned descriptor enums and oneofs are emitted as
        // concrete per-identity types below.  Never erase a semantic value
        // into a public catch-all WireChoice.
        WireValueKind::Enum | WireValueKind::Oneof => name.to_owned(),
    }
}

fn cpp_kind(kind: WireValueKind, name: &str) -> String {
    match kind {
        WireValueKind::String | WireValueKind::Bytes | WireValueKind::UnsignedInteger => {
            name.to_owned()
        }
        WireValueKind::Message => format!("std::shared_ptr<{name}>"),
        WireValueKind::SignedInteger => "std::int64_t".into(),
        WireValueKind::Boolean => "bool".into(),
        WireValueKind::Timestamp => "std::chrono::system_clock::time_point".into(),
        // Closed Rust-owned descriptor enums and oneofs are emitted as
        // concrete per-identity types below.  Never erase a semantic value
        // into a public catch-all WireChoice.
        WireValueKind::Enum | WireValueKind::Oneof => name.to_owned(),
    }
}

fn swift_checks(kind: WireValueKind, rules: &[SemanticRule]) -> String {
    let mut out = String::new();
    for rule in rules {
        match rule {
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
    }
    }
    out
}

fn cpp_checks(kind: WireValueKind, rules: &[SemanticRule]) -> String {
    let mut out = String::new();
    for rule in rules {
        match rule {
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
    }
    }
    out
}

fn swift_message_checks(rules: &[SemanticRule]) -> String {
    let mut out = String::new();
    for rule in rules {
        match rule {
            SemanticRule::NonEmpty => {
                out.push_str(" guard !wire.wire.isEmpty else { return nil };")
            }
            SemanticRule::Immutable
            | SemanticRule::ExactOneof
            | SemanticRule::ExplicitPresence
            | SemanticRule::PreserveUnknownEnum
            | SemanticRule::PreserveUnknownOneof => {}
            SemanticRule::FixedLength(_) => {}
            other => panic!("unsupported message constructor rule: {other:?}"),
        }
    }
    out
}

fn swift_nested_value_checks(
    field: &crate::type_policy::ResolvedRequestField,
    rules: &[SemanticRule],
) -> String {
    if field.field != "value" || field.wire_type != Some(FieldType::Bytes as i32) {
        return String::new();
    }
    let mut conditions = Vec::new();
    for rule in rules {
        match rule {
            SemanticRule::NonEmpty => conditions.push("!value.isEmpty".to_owned()),
            SemanticRule::FixedLength(n) => conditions.push(format!("value.count == {n}")),
            SemanticRule::Sha256Digest => conditions.push("value.count == 32".to_owned()),
            _ => {}
        }
    }
    if conditions.is_empty() {
        String::new()
    } else {
        format!(
            " guard let value, {} else {{ return nil }};",
            conditions.join(", ")
        )
    }
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

fn semantic_request_groups()
-> BTreeMap<(&'static str, &'static str), Vec<&'static crate::type_policy::PublicFieldBinding>> {
    let mut groups = BTreeMap::new();
    for binding in PUBLIC_FIELD_BINDINGS
        .iter()
        .filter(|binding| binding.direction == PublicFieldDirection::Request)
    {
        groups
            .entry((binding.module, binding.message))
            .or_insert_with(Vec::new)
            .push(binding);
    }
    groups
}

fn nested_message_groups()
-> BTreeMap<&'static str, Vec<&'static crate::type_policy::PublicFieldBinding>> {
    let mut groups = BTreeMap::new();
    for binding in PUBLIC_FIELD_BINDINGS
        .iter()
        .filter(|binding| binding.direction == PublicFieldDirection::NestedMessage)
    {
        groups
            .entry(binding.message)
            .or_insert_with(Vec::new)
            .push(binding);
    }
    groups
}

fn message_leaf(path: &str) -> &str {
    path.rsplit('.').next().unwrap_or(path)
}

fn descriptor_projection_prefix(path: &str) -> String {
    path.split('.')
        .filter(|part| !part.is_empty() && *part != "acyclic")
        .filter(|part| {
            let bytes = part.as_bytes();
            !(bytes.len() >= 2 && bytes[0] == b'v' && bytes[1..].iter().all(u8::is_ascii_digit))
        })
        .map(camel)
        .collect()
}

fn descriptor_package_prefix(path: &str) -> String {
    path.rsplit_once('.')
        .map(|(package, _)| descriptor_projection_prefix(package))
        .unwrap_or_default()
}

/// The descriptor's fully-qualified type name is part of the Rust-owned wire
/// contract.  Keep that identity in generated public fields instead of
/// collapsing every ordinary message or enum to the catch-all wire holder.
fn descriptor_type_name(field: &crate::type_policy::ResolvedRequestField) -> Option<String> {
    field
        .type_name
        .as_deref()
        .map(|name| format!("{}{}Wire", camel(&field.family), message_leaf(name)))
}

/// Return the generated nominal name for the message that owns a descriptor
/// field.  `type_name` identifies a field's payload; `message_path` identifies
/// the declaration that owns the field.  Keeping these separate is essential
/// for recursive nested messages and for projecting presence and oneof arms
/// on the declaration rather than on an unrelated payload type.
fn descriptor_owner_name(field: &crate::type_policy::ResolvedRequestField) -> String {
    format!(
        "{}{}Wire",
        camel(&field.family),
        message_leaf(&field.message_path)
    )
}

fn descriptor_groups(
    fields: Vec<crate::type_policy::ResolvedRequestField>,
) -> BTreeMap<(String, String), Vec<crate::type_policy::ResolvedRequestField>> {
    let mut groups = BTreeMap::new();
    for field in fields {
        if message_leaf(&field.message_path) != message_leaf(&field.root_message) {
            continue;
        }
        let key = (
            field.family.clone(),
            message_leaf(&field.root_message).to_owned(),
        );
        let group = groups.entry(key).or_insert_with(Vec::new);
        if !group
            .iter()
            .any(|existing: &crate::type_policy::ResolvedRequestField| {
                existing.field == field.field
            })
        {
            group.push(field);
        }
    }
    groups
}

fn request_groups() -> BTreeMap<(String, String), Vec<crate::type_policy::ResolvedRequestField>> {
    let mut groups = descriptor_groups(
        resolved_request_fields().expect("Rust request descriptors must resolve"),
    );
    for method in resolved_rpc_methods().expect("Rust RPC identities must resolve") {
        groups
            .entry((
                method.family,
                message_leaf(&method.input_message).to_owned(),
            ))
            .or_default();
    }
    groups
}

fn response_groups() -> BTreeMap<(String, String), Vec<crate::type_policy::ResolvedRequestField>> {
    let mut groups = descriptor_groups(
        resolved_response_fields().expect("Rust response descriptors must resolve"),
    );
    for method in resolved_rpc_methods().expect("Rust RPC identities must resolve") {
        groups
            .entry((
                method.family,
                message_leaf(&method.output_message).to_owned(),
            ))
            .or_default();
    }
    groups
}

fn ordinary_descriptor_names(kind: FieldType) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for field in resolved_request_fields()
        .expect("Rust request descriptors must resolve")
        .into_iter()
        .chain(resolved_response_fields().expect("Rust response descriptors must resolve"))
    {
        if kind == FieldType::Message {
            if message_leaf(&field.message_path) != message_leaf(&field.root_message) {
                names.insert(descriptor_owner_name(&field));
            }
            if field.wire_type == Some(kind as i32)
                && let Some(name) = descriptor_type_name(&field)
            {
                names.insert(name);
            }
        } else if kind != FieldType::Message
            && field.wire_type == Some(kind as i32)
            && let Some(name) = descriptor_type_name(&field)
        {
            names.insert(name);
        }
    }
    for item in SEMANTIC_TYPES
        .iter()
        .filter(|item| item.wire_kind == WireValueKind::Message)
    {
        for field in resolved_request_fields().expect("Rust request descriptors must resolve") {
            if message_leaf(&field.message_path) == item.rust_name
                && field.wire_type == Some(kind as i32)
                && let Some(name) = descriptor_type_name(&field)
            {
                names.insert(name);
            }
        }
    }
    names
}

/// Return the complete Rust-descriptor field inventory for an ordinary
/// message.  The previous emitter declared ordinary messages as a single
/// opaque wire holder, which meant a nested protobuf field lost its nominal
/// target-language type.  Keep the wire envelope for forward compatibility,
/// but expose every descriptor field with its recursively resolved public
/// type as well.
fn ordinary_descriptor_fields(name: &str) -> Vec<crate::type_policy::ResolvedRequestField> {
    let mut fields = Vec::new();
    for field in resolved_request_fields()
        .expect("Rust request descriptors must resolve")
        .into_iter()
        .chain(resolved_response_fields().expect("Rust response descriptors must resolve"))
    {
        if descriptor_owner_name(&field) == name
            && !fields
                .iter()
                .any(|existing: &crate::type_policy::ResolvedRequestField| {
                    existing.field == field.field
                })
        {
            fields.push(field);
        }
    }
    fields.sort_by_key(|field| field.number);
    fields
}

fn semantic_nested_fields(message: &str) -> Vec<crate::type_policy::ResolvedRequestField> {
    let mut fields = Vec::new();
    for field in resolved_request_fields().expect("Rust request descriptors must resolve") {
        if message_leaf(&field.message_path) == message
            && message_leaf(&field.message_path) != message_leaf(&field.root_message)
            && !fields
                .iter()
                .any(|existing: &crate::type_policy::ResolvedRequestField| {
                    existing.field == field.field
                })
        {
            fields.push(field);
        }
    }
    fields
}

fn semantic_nested_descriptor_names(kind: FieldType) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for item in SEMANTIC_TYPES
        .iter()
        .filter(|item| item.wire_kind == WireValueKind::Message)
    {
        for field in semantic_nested_fields(item.rust_name) {
            if field.wire_type == Some(kind as i32)
                && let Some(name) = descriptor_type_name(&field)
            {
                names.insert(name);
            }
        }
    }
    names
}

/// All enum identities reachable from the Rust descriptor closure.  The
/// ordinary field scan is intentionally supplemented by the Rust inventory so
/// an enum that is carried through a nested or oneof declaration still gets a
/// nominal target type even when no root request field directly mentions it.
fn resolved_descriptor_enum_names() -> BTreeSet<String> {
    resolved_enum_fields()
        .expect("Rust enum descriptors must resolve")
        .into_iter()
        .map(|entry| {
            format!(
                "{}{}Wire",
                descriptor_package_prefix(&entry.enum_type),
                message_leaf(&entry.enum_type)
            )
        })
        .collect()
}

fn semantic_binding(
    family: &str,
    message: &str,
    field: &str,
    direction: PublicFieldDirection,
) -> Option<&'static crate::type_policy::PublicFieldBinding> {
    PUBLIC_FIELD_BINDINGS.iter().find(|binding| {
        binding.family == family
            && binding.message == message
            && (binding.field == field || binding.wire_field == field)
            && binding.direction == direction
    })
}

fn semantic_field(
    field: &crate::type_policy::ResolvedRequestField,
    direction: PublicFieldDirection,
) -> Option<&'static crate::type_policy::SemanticType> {
    let message = if direction == PublicFieldDirection::NestedMessage {
        message_leaf(&field.message_path)
    } else {
        message_leaf(&field.root_message)
    };
    field
        .semantic_type
        .as_deref()
        .and_then(semantic_type)
        .or_else(|| {
            semantic_binding(&field.family, message, &field.field, direction)
                .and_then(|binding| semantic_type(binding.semantic_type))
        })
        .or_else(|| field_semantic_type(&field.family, &field.field))
}

fn public_nested_type_name(message: &str) -> String {
    let name = camel(message);
    if SEMANTIC_TYPES.iter().any(|item| item.rust_name == name)
        || ordinary_descriptor_names(FieldType::Message).contains(&name)
        || ordinary_descriptor_names(FieldType::Enum).contains(&name)
    {
        format!("{name}Fields")
    } else {
        name
    }
}

fn swift_wire_type(field: &crate::type_policy::ResolvedRequestField) -> String {
    match field.wire_type {
        Some(kind) if kind == FieldType::String as i32 => "String".into(),
        Some(kind) if kind == FieldType::Bytes as i32 => "Data".into(),
        Some(kind)
            if kind == FieldType::Int64 as i32
                || kind == FieldType::Sfixed64 as i32
                || kind == FieldType::Sint64 as i32 =>
        {
            "Int64".into()
        }
        Some(kind) if kind == FieldType::Uint64 as i32 || kind == FieldType::Fixed64 as i32 => {
            "UInt64".into()
        }
        Some(kind)
            if kind == FieldType::Int32 as i32
                || kind == FieldType::Sfixed32 as i32
                || kind == FieldType::Sint32 as i32 =>
        {
            "Int32".into()
        }
        Some(kind) if kind == FieldType::Uint32 as i32 || kind == FieldType::Fixed32 as i32 => {
            "UInt32".into()
        }
        Some(kind) if kind == FieldType::Bool as i32 => "Bool".into(),
        Some(kind) if kind == FieldType::Double as i32 => "Double".into(),
        Some(kind) if kind == FieldType::Float as i32 => "Float".into(),
        Some(kind) if kind == FieldType::Enum as i32 => {
            descriptor_type_name(field).unwrap_or_else(|| "RustWireEnum".into())
        }
        Some(kind) if kind == FieldType::Message as i32 || kind == FieldType::Group as i32 => {
            descriptor_type_name(field).unwrap_or_else(|| "RustWireMessage".into())
        }
        _ => "Data".into(),
    }
}

fn cpp_wire_type(field: &crate::type_policy::ResolvedRequestField) -> String {
    match field.wire_type {
        Some(kind) if kind == FieldType::String as i32 => "std::string".into(),
        Some(kind) if kind == FieldType::Bytes as i32 => "std::vector<std::uint8_t>".into(),
        Some(kind)
            if kind == FieldType::Int64 as i32
                || kind == FieldType::Sfixed64 as i32
                || kind == FieldType::Sint64 as i32 =>
        {
            "std::int64_t".into()
        }
        Some(kind) if kind == FieldType::Uint64 as i32 || kind == FieldType::Fixed64 as i32 => {
            "std::uint64_t".into()
        }
        Some(kind)
            if kind == FieldType::Int32 as i32
                || kind == FieldType::Sfixed32 as i32
                || kind == FieldType::Sint32 as i32 =>
        {
            "std::int32_t".into()
        }
        Some(kind) if kind == FieldType::Uint32 as i32 || kind == FieldType::Fixed32 as i32 => {
            "std::uint32_t".into()
        }
        Some(kind) if kind == FieldType::Bool as i32 => "bool".into(),
        Some(kind) if kind == FieldType::Double as i32 => "double".into(),
        Some(kind) if kind == FieldType::Float as i32 => "float".into(),
        Some(kind) if kind == FieldType::Enum as i32 => {
            descriptor_type_name(field).unwrap_or_else(|| "RustWireEnum".into())
        }
        Some(kind) if kind == FieldType::Message as i32 || kind == FieldType::Group as i32 => {
            descriptor_type_name(field)
                .map(|name| format!("std::shared_ptr<{name}>"))
                .unwrap_or_else(|| "std::shared_ptr<RustWireMessage>".into())
        }
        _ => "std::vector<std::uint8_t>".into(),
    }
}

fn field_has_presence(field: &crate::type_policy::ResolvedRequestField) -> bool {
    field.label != Some(FieldLabel::Repeated as i32)
        && (field.proto3_optional
            || field.oneof_index.is_some()
            || matches!(
                field.wire_type,
                Some(kind)
                    if kind == FieldType::Message as i32 || kind == FieldType::Group as i32
            ))
}

fn swift_field_type(
    field: &crate::type_policy::ResolvedRequestField,
    direction: PublicFieldDirection,
) -> String {
    let base = semantic_field(field, direction)
        .map(|semantic| swift_kind(semantic.wire_kind, semantic.rust_name))
        .unwrap_or_else(|| swift_wire_type(field));
    let repeated = field.label == Some(FieldLabel::Repeated as i32);
    let optional = field_has_presence(field);
    let value = if repeated { format!("[{base}]") } else { base };
    if optional { format!("{value}?") } else { value }
}

fn cpp_field_type(
    field: &crate::type_policy::ResolvedRequestField,
    direction: PublicFieldDirection,
) -> String {
    let base = semantic_field(field, direction)
        .map(|semantic| cpp_kind(semantic.wire_kind, semantic.rust_name))
        .unwrap_or_else(|| cpp_wire_type(field));
    let repeated = field.label == Some(FieldLabel::Repeated as i32);
    let optional = field_has_presence(field);
    let value = if repeated {
        format!("std::vector<{base}>")
    } else {
        base
    };
    if optional {
        format!("std::optional<{value}>")
    } else {
        value
    }
}

fn response_oneof_groups() -> BTreeMap<(String, String), Vec<ResolvedRequestField>> {
    let mut groups = BTreeMap::<(String, String), Vec<ResolvedRequestField>>::new();
    for field in resolved_request_fields()
        .expect("Rust response descriptors must resolve")
        .into_iter()
        .filter(|field| field.oneof_name.is_some())
        .chain(
            resolved_response_fields()
                .expect("Rust response descriptors must resolve")
                .into_iter()
                .filter(|field| field.oneof_name.is_some()),
        )
    {
        groups
            .entry((
                field.message_path.clone(),
                field.oneof_name.clone().unwrap(),
            ))
            .or_default()
            .push(field);
    }
    for members in groups.values_mut() {
        members.sort_by_key(|field| field.number);
        members.dedup_by(|left, right| left.field == right.field && left.number == right.number);
    }
    groups
}

/// Oneof arms that belong to a public request/response model.  The global
/// oneof inventory also contains nested declarations; those must not leak into
/// the top-level RPC model that owns the wire message.
fn model_oneof_groups(
    message: &str,
    direction: PublicFieldDirection,
) -> BTreeMap<String, Vec<ResolvedRequestField>> {
    let fields = match direction {
        PublicFieldDirection::Request => {
            resolved_request_fields().expect("Rust request descriptors must resolve")
        }
        PublicFieldDirection::Response => {
            resolved_response_fields().expect("Rust response descriptors must resolve")
        }
        PublicFieldDirection::NestedMessage => resolved_request_fields()
            .expect("Rust nested descriptors must resolve")
            .into_iter()
            .chain(
                resolved_response_fields()
                    .expect("Rust nested response descriptors must resolve")
                    .into_iter(),
            )
            .collect(),
        PublicFieldDirection::EmbeddedOnly => Vec::new(),
    };
    let nested_message = direction == PublicFieldDirection::NestedMessage;
    let mut groups = BTreeMap::<String, Vec<ResolvedRequestField>>::new();
    for field in fields.into_iter().filter(|field| {
        field.oneof_name.is_some()
            && message_leaf(&field.message_path) == message
            && (message_leaf(&field.root_message) == message || nested_message)
    }) {
        groups
            .entry(field.oneof_name.clone().expect("oneof name"))
            .or_default()
            .push(field);
    }
    for members in groups.values_mut() {
        members.sort_by_key(|field| field.number);
    }
    groups
}

fn swift_oneof_choice_type(members: &[ResolvedRequestField]) -> String {
    response_oneof_name(
        members.first().expect("oneof must contain an arm"),
        members
            .first()
            .and_then(|field| field.oneof_name.as_deref())
            .expect("oneof name"),
    )
}

fn swift_oneof_choice_property(oneof: &str) -> String {
    swift_field_name(&format!("{}Choice", camel(oneof)))
}

fn response_oneof_name(field: &ResolvedRequestField, oneof: &str) -> String {
    let message = field.message_path.rsplit('.').next().unwrap_or("Message");
    format!(
        "{}{}{}Choice",
        descriptor_package_prefix(&field.message_path),
        camel(message),
        camel(oneof)
    )
}

fn cpp_oneof_arm_type(field: &ResolvedRequestField) -> String {
    let ty = cpp_field_type(field, PublicFieldDirection::Response);
    ty.strip_prefix("std::optional<")
        .and_then(|value| value.strip_suffix('>'))
        .unwrap_or(&ty)
        .to_owned()
}

fn cpp_oneof_choice_value_type(oneof: &str, members: &[ResolvedRequestField]) -> String {
    format!(
        "{}Value",
        response_oneof_name(members.first().expect("oneof must contain an arm"), oneof)
    )
}

fn cpp_oneof_choice_field_name(oneof: &str) -> String {
    cpp_field_name(&format!("{}Choice", camel(oneof)))
}

fn cpp_oneof_none_type(oneof: &str, members: &[ResolvedRequestField]) -> String {
    format!(
        "{}None",
        response_oneof_name(members.first().expect("oneof must contain an arm"), oneof)
    )
}

fn render_swift_oneof_models(out: &mut String) {
    for ((_message, oneof), members) in response_oneof_groups() {
        let Some(first) = members.first() else {
            continue;
        };
        let name = response_oneof_name(first, &oneof);
        out.push_str(&format!("public enum {name}: Sendable {{\n"));
        for field in members {
            let value_type = swift_field_type(&field, PublicFieldDirection::Response)
                .trim_end_matches('?')
                .to_owned();
            out.push_str(&format!(" case {}({value_type})\n", camel(&field.field)));
        }
        out.push_str(" case none\n case unknown(rawTag: Int32, payload: Data)\n}\n\n");
    }
}

fn render_cpp_oneof_models(out: &mut String) {
    for ((_message, oneof), members) in response_oneof_groups() {
        let Some(first) = members.first() else {
            continue;
        };
        let name = response_oneof_name(first, &oneof);
        let mut alternatives = Vec::new();
        for field in members {
            let arm = format!("{name}{}", camel(&field.field));
            let value_type = cpp_oneof_arm_type(&field);
            out.push_str(&format!("struct {arm} {{ {value_type} value; }};\n"));
            alternatives.push(arm);
        }
        let unknown = format!("{name}Unknown");
        out.push_str(&format!(
            "struct {unknown} {{ std::int32_t raw_tag; std::vector<std::uint8_t> payload; }};\n"
        ));
        alternatives.push(unknown);
        let none = format!("{name}None");
        out.push_str(&format!("struct {none} {{}};\n"));
        alternatives.push(none);
        out.push_str(&format!(
            "using {name}Value = std::variant<{}>;\n\n",
            alternatives.join(", ")
        ));
    }
}

fn swift_field_cast(field: &crate::type_policy::ResolvedRequestField) -> String {
    // Dictionaries carry the protobuf wire holder.  Public fields may use a
    // named descriptor wrapper, so decoding must cast the raw holder before
    // constructing that wrapper.
    match field.wire_type {
        Some(kind) if kind == FieldType::Message as i32 || kind == FieldType::Group as i32 => {
            "RustWireMessage".to_owned()
        }
        Some(kind) if kind == FieldType::Enum as i32 => "RustWireEnum".to_owned(),
        _ => swift_wire_type(field),
    }
}

fn swift_oneof_choice_wire_switch(
    property: &str,
    members: &[ResolvedRequestField],
    oneof: &str,
) -> String {
    let mut out = format!(" switch {property} {{");
    for field in members {
        out.push_str(&format!(
            " case .{}(let value): wire[\"{}\"] = value",
            camel(&field.field),
            field.json_name
        ));
    }
    out.push_str(" case .none: break");
    out.push_str(&format!(
        " case .unknown(let rawTag, let payload): wire[\"__unknown_{oneof}\"] = RustWireUnknownOneof(rawTag: rawTag, payload: payload) }}"
    ));
    out
}

fn swift_oneof_decode_value(
    field: &ResolvedRequestField,
    direction: PublicFieldDirection,
    raw: &str,
    local: &str,
) -> String {
    let target = swift_field_type(field, direction)
        .trim_end_matches('?')
        .to_owned();
    let cast = swift_field_cast(field);
    let key = &field.json_name;
    let value = format!("value_{local}");
    let constructor = semantic_field(field, direction).is_some()
        || descriptor_type_name(field).is_some();
    let converted = if constructor {
        format!(
            "guard let {value} = {target}({raw}) else {{ throw RustWireDecodeError.invalidField(\"{key}\") }};",
            target = target
        )
    } else {
        format!("let {value}: {target} = {raw};", target = target)
    };
    format!(
        "if let {raw_name} = wire[\"{key}\"] as? {cast} {{ {converted} guard selected_{local} == nil else {{ throw RustWireDecodeError.invalidField(\"oneof arm {key} conflicts\") }}; selected_{local} = .{case_name}({value}) }}",
        raw_name = raw,
        case_name = camel(&field.field),
    )
}

fn swift_oneof_decode_block(
    property: &str,
    choice_type: &str,
    oneof: &str,
    members: &[ResolvedRequestField],
    direction: PublicFieldDirection,
) -> String {
    let local = swift_local_name(oneof);
    let mut out = format!(
        "var selected_{local}: {choice_type}? = nil;",
        local = local,
        choice_type = choice_type
    );
    for field in members {
        out.push_str(&swift_oneof_decode_value(
            field,
            direction,
            &format!("raw_{}", swift_local_name(&field.field)),
            &local,
        ));
    }
    out.push_str(&format!(
        "if let unknown_{local} = wire[\"__unknown_{oneof}\"] as? RustWireUnknownOneof {{ guard selected_{local} == nil else {{ throw RustWireDecodeError.invalidField(\"oneof {oneof} contains known and unknown arms\") }}; selected_{local} = .unknown(rawTag: unknown_{local}.rawTag, payload: unknown_{local}.payload) }} let {property}: {choice_type} = selected_{local} ?? .none;",
        local = local,
        property = property,
        choice_type = choice_type,
    ));
    out
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
    let mut out = String::from(
        r#"// Generated by acyclic-sdk-contract-wire; do not edit.
import Foundation

public enum RustWireDecodeError: Error {
 case missingField(String)
 case invalidField(String)
}
public struct RustWireMessage: Sendable { public let wire: Data; public init(wire: Data) { self.wire = wire } }
public struct RustWireEnum: Sendable { public let raw: Int32; public init(raw: Int32) { self.raw = raw } }
/// A transport may place this value under `__unknown_<oneof>` when a oneof
/// arm is newer than this generated facade.
public struct RustWireUnknownOneof: Sendable {
 public let rawTag: Int32
 public let payload: Data
 public init(rawTag: Int32, payload: Data) { self.rawTag = rawTag; self.payload = payload }
}
public protocol RustWireRequest: Sendable { associatedtype Wire; func toWire() -> Wire }
public protocol RustWireResponse: Sendable { associatedtype Wire; static func fromWire(_ wire: Wire) throws -> Self }

private final class RustTypedSerialExecutor: @unchecked Sendable {
 private let lock = NSLock()
 private var nextTicket = 0
 private var servingTicket = 0
 private var waiters: [Int: CheckedContinuation<Void, Never>] = [:]
 func submit<T: Sendable>(_ operation: @escaping @Sendable () async throws -> T) -> Task<T, Error> {
   lock.lock()
   let ticket = nextTicket
   nextTicket += 1
   lock.unlock()
   return Task<T, Error> {
    await self.waitForTurn(ticket)
    defer { self.complete(ticket) }
    return try await operation()
   }
 }
 private func waitForTurn(_ ticket: Int) async {
  lock.lock()
  if ticket == servingTicket {
   lock.unlock()
   return
  }
  await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
   waiters[ticket] = continuation
   lock.unlock()
  }
 }
 private func complete(_ ticket: Int) {
  lock.lock()
  guard ticket == servingTicket else { lock.unlock(); return }
  servingTicket += 1
  let continuation = waiters.removeValue(forKey: servingTicket)
  lock.unlock()
  continuation?.resume()
 }
}

private final class RustTypedCancellationState: @unchecked Sendable {
 private let lock = NSLock()
 private var cancelled = false
 private let operation: @Sendable () -> Void
 init(_ operation: @escaping @Sendable () -> Void) { self.operation = operation }
 func cancelIfNeeded() {
  lock.lock()
  guard !cancelled else { lock.unlock(); return }
  cancelled = true
  lock.unlock()
  operation()
 }
 func completeWithoutCancel() {
  lock.lock()
  cancelled = true
  lock.unlock()
 }
}

public enum RustTypedStreamState: Sendable, Equatable { case open; case finished; case cancelled }
public final class RustTypedStream<Element: RustWireResponse>: @unchecked Sendable {
 private let nextOperation: @Sendable () async throws -> Element?
 private let executor = RustTypedSerialExecutor()
 private let cancellation: RustTypedCancellationState
 private let lock = NSLock()
 private var lifecycle: RustTypedStreamState = .open
 public init(next: @escaping @Sendable () async throws -> Element?, cancel: @escaping @Sendable () -> Void) { self.nextOperation = next; self.cancellation = RustTypedCancellationState(cancel) }
 deinit { cancellation.cancelIfNeeded() }
 public var state: RustTypedStreamState { lock.lock(); defer { lock.unlock() }; return lifecycle }
 public func next() async throws -> Element? {
  let operation = nextOperation
  lock.lock()
  guard lifecycle == .open else { lock.unlock(); throw RustWireDecodeError.invalidField("stream is not open") }
  let task = executor.submit { try await operation() }
  lock.unlock()
  let result = try await task.value
  if result == nil { lock.lock(); if lifecycle == .open { lifecycle = .finished }; lock.unlock(); cancellation.completeWithoutCancel() }
  return result
 }
 public func cancel() { lock.lock(); guard lifecycle == .open else { lock.unlock(); return }; lifecycle = .cancelled; lock.unlock(); cancellation.cancelIfNeeded() }
}
public final class RustTypedRequestSequence<Request: RustWireRequest>: @unchecked Sendable {
 private let nextOperation: @Sendable () async throws -> Request?
 private let executor = RustTypedSerialExecutor()
 private let cancellation: RustTypedCancellationState
 private let lock = NSLock()
 private var lifecycle: RustTypedStreamState = .open
 public init(next: @escaping @Sendable () async throws -> Request?, cancel: @escaping @Sendable () -> Void) { self.nextOperation = next; self.cancellation = RustTypedCancellationState(cancel) }
 deinit { cancellation.cancelIfNeeded() }
 public var state: RustTypedStreamState { lock.lock(); defer { lock.unlock() }; return lifecycle }
 public func next() async throws -> Request? {
  let operation = nextOperation
  lock.lock()
  guard lifecycle == .open else { lock.unlock(); throw RustWireDecodeError.invalidField("request sequence is not open") }
  let task = executor.submit { try await operation() }
  lock.unlock()
  let result = try await task.value
  if result == nil { lock.lock(); if lifecycle == .open { lifecycle = .finished }; lock.unlock(); cancellation.completeWithoutCancel() }
  return result
 }
 public func cancel() { lock.lock(); guard lifecycle == .open else { lock.unlock(); return }; lifecycle = .cancelled; lock.unlock(); cancellation.cancelIfNeeded() }
}
public final class RustTypedClientStream<Request: RustWireRequest, Response: RustWireResponse>: @unchecked Sendable {
 private let sendOperation: @Sendable (Request) async throws -> Void
 private let finishOperation: @Sendable () async throws -> Response
 private let nextOperation: (@Sendable () async throws -> Response?)?
 private let executor = RustTypedSerialExecutor()
 private let cancellation: RustTypedCancellationState
 private let lock = NSLock()
 private var lifecycle: RustTypedStreamState = .open
 public init(send: @escaping @Sendable (Request) async throws -> Void, finish: @escaping @Sendable () async throws -> Response, next: (@Sendable () async throws -> Response?)? = nil, cancel: @escaping @Sendable () -> Void) { self.sendOperation = send; self.finishOperation = finish; self.nextOperation = next; self.cancellation = RustTypedCancellationState(cancel) }
 deinit { cancellation.cancelIfNeeded() }
 public var state: RustTypedStreamState { lock.lock(); defer { lock.unlock() }; return lifecycle }
 public func send(_ request: Request) async throws {
  let operation = sendOperation
  lock.lock(); guard lifecycle == .open else { lock.unlock(); throw RustWireDecodeError.invalidField("client stream is not open") }; let task = executor.submit { try await operation(request) }; lock.unlock()
  try await task.value
 }
 public func next() async throws -> Response? {
  guard let nextOperation else { throw RustWireDecodeError.invalidField("client stream has no response sequence") }
  lock.lock(); guard lifecycle == .open || lifecycle == .finished else { lock.unlock(); throw RustWireDecodeError.invalidField("client stream is cancelled") }; let task = executor.submit { try await nextOperation() }; lock.unlock()
  let result = try await task.value
  if result == nil { lock.lock(); if lifecycle == .open { lifecycle = .finished }; lock.unlock(); cancellation.completeWithoutCancel() }
  return result
 }
 public func finish() async throws -> Response {
  let operation = finishOperation
  lock.lock(); guard lifecycle == .open else { lock.unlock(); throw RustWireDecodeError.invalidField("client stream is not open") }; lifecycle = .finished; let task = executor.submit { try await operation() }; lock.unlock()
  let result = try await task.value
  cancellation.completeWithoutCancel()
  return result
 }
 public func cancel() { lock.lock(); guard lifecycle == .open else { lock.unlock(); return }; lifecycle = .cancelled; lock.unlock(); cancellation.cancelIfNeeded() }
}

"#,
    );
    let mut seen = BTreeSet::new();
    for item in SEMANTIC_TYPES {
        if !seen.insert(item.rust_name) {
            continue;
        }
        match item.wire_kind {
            WireValueKind::String => out.push_str(&format!("public struct {}: Sendable {{ public let value: String; public init?(_ value: String) {{{} self.value = value }} }}\n", item.rust_name, swift_checks(item.wire_kind, item.rules))),
            WireValueKind::Bytes => out.push_str(&format!("public struct {}: Sendable {{ public let value: Data; public init?(_ value: Data) {{{} self.value = value }} }}\n", item.rust_name, swift_checks(item.wire_kind, item.rules))),
            WireValueKind::UnsignedInteger => out.push_str(&format!("public struct {}: Sendable {{ public let value: UInt64; public init?(_ value: UInt64) {{{} self.value = value }} }}\n", item.rust_name, swift_checks(item.wire_kind, item.rules))),
            WireValueKind::SignedInteger => out.push_str(&format!("public struct {}: Sendable {{ public let value: Int64; public init?(_ value: Int64) {{{} self.value = value }} }}\n", item.rust_name, swift_checks(item.wire_kind, item.rules))),
            WireValueKind::Message => {
                let nested = semantic_nested_fields(item.rust_name);
                let oneofs = model_oneof_groups(item.rust_name, PublicFieldDirection::NestedMessage);
                let nested_type = |field: &crate::type_policy::ResolvedRequestField| {
                    let ty = swift_field_type(field, PublicFieldDirection::NestedMessage);
                    if ty.ends_with('?') { ty } else { format!("{ty}?") }
                };
                let declarations = nested
                    .iter()
                    .filter(|field| field.oneof_index.is_none())
                    .map(|field| format!("public let {}: {};\n", swift_field_name(&field.field), nested_type(field)))
                    .chain(oneofs.iter().map(|(oneof, members)| format!(
                        "public let {}: {};\n",
                        swift_oneof_choice_property(oneof),
                        swift_oneof_choice_type(members)
                    )))
                    .collect::<String>();
                let params = nested
                    .iter()
                    .filter(|field| field.oneof_index.is_none())
                    .map(|field| format!("{}: {} = nil", swift_field_name(&field.field), nested_type(field)))
                    .chain(oneofs.iter().map(|(oneof, members)| format!(
                        "{}: {}? = nil",
                        swift_oneof_choice_property(oneof),
                        swift_oneof_choice_type(members)
                    )))
                    .collect::<Vec<_>>()
                    .join(", ");
                let assignments = nested
                    .iter()
                    .filter(|field| field.oneof_index.is_none())
                    .map(|field| format!("self.{0} = {0}\n", swift_field_name(&field.field)))
                    .chain(oneofs.iter().map(|(oneof, _)| {
                        let property = swift_oneof_choice_property(oneof);
                        format!("self.{property} = {property} ?? .none\n")
                    }))
                    .collect::<String>();
                let raw_assignments = nested
                    .iter()
                    .filter(|field| field.oneof_index.is_none())
                    .map(|field| format!("self.{} = nil\n", swift_field_name(&field.field)))
                    .chain(oneofs.iter().map(|(oneof, _)| {
                        format!("self.{} = .none\n", swift_oneof_choice_property(oneof))
                    }))
                    .collect::<String>();
                let typed_checks = nested
                    .iter()
                    .filter(|field| field.oneof_index.is_none())
                    .map(|field| swift_nested_value_checks(field, item.rules))
                    .collect::<String>();
                let image_oneof = if item.rust_name == "Image" {
                    oneofs
                        .keys()
                        .map(|oneof| {
                            let property = swift_oneof_choice_property(oneof);
                            format!(
                                " guard let choice = {property} else {{ return nil }}; if case .none = choice {{ return nil }};"
                            )
                        })
                        .collect::<String>()
                } else {
                    String::new()
                };
                out.push_str(&format!("public final class {}: Sendable {{\n private let wire: RustWireMessage\n{} internal init?(_ wire: RustWireMessage) {{{} self.wire = wire\n{} }}\n public init?({}) {{{}{} self.wire = RustWireMessage(wire: Data())\n{} }}\n}}\n", item.rust_name, declarations, swift_message_checks(item.rules), raw_assignments, params, typed_checks, image_oneof, assignments));
            }
            _ => {}
        }
    }
    // Preserve ordinary descriptor identities as public nominal holders.  The
    // raw bytes remain available for forward compatibility, while a field's
    // generated type still tells consumers which message or enum it carries.
    let mut ordinary_messages = ordinary_descriptor_names(FieldType::Message);
    ordinary_messages.extend(semantic_nested_descriptor_names(FieldType::Message));
    for name in ordinary_messages {
        let fields = ordinary_descriptor_fields(&name);
        let declarations = fields
            .iter()
            .map(|field| {
                let ty = swift_field_type(field, PublicFieldDirection::NestedMessage);
                let ty = if field.proto3_optional || field.oneof_index.is_some() {
                    ty
                } else {
                    format!("{ty}?")
                };
                format!(" public let {}: {ty};\n", swift_field_name(&field.field))
            })
            .collect::<String>();
        let raw_assignments = fields
            .iter()
            .map(|field| format!(" self.{} = nil;\n", swift_field_name(&field.field)))
            .collect::<String>();
        let params = fields
            .iter()
            .map(|field| {
                let ty = swift_field_type(field, PublicFieldDirection::NestedMessage);
                let ty = if field.proto3_optional || field.oneof_index.is_some() {
                    ty
                } else {
                    format!("{ty}?")
                };
                format!("{}: {ty} = nil", swift_field_name(&field.field))
            })
            .collect::<Vec<_>>()
            .join(", ");
        let assignments = fields
            .iter()
            .map(|field| format!(" self.{0} = {0};\n", swift_field_name(&field.field)))
            .collect::<String>();
        out.push_str(&format!(
            "public final class {name}: Sendable {{\n private let wire: RustWireMessage\n{declarations} internal init(_ wire: RustWireMessage) {{ self.wire = wire\n{raw_assignments} }}\n public init(wire: RustWireMessage = RustWireMessage(wire: Data()), {params}) {{ self.wire = wire\n{assignments} }}\n}}\n"
        ));
    }
    let mut ordinary_enums = ordinary_descriptor_names(FieldType::Enum);
    ordinary_enums.extend(resolved_descriptor_enum_names());
    ordinary_enums.extend(semantic_nested_descriptor_names(FieldType::Enum));
    for name in ordinary_enums {
        out.push_str(&format!("public struct {name}: Sendable {{ public let raw: Int32; public init(raw: Int32) {{ self.raw = raw }}; public init(_ value: RustWireEnum) {{ self.raw = value.raw }} }}\n"));
    }
    render_swift_oneof_models(&mut out);
    for ((module, message), fields) in request_groups() {
        let name = format!(
            "{}{}Request",
            camel(&module),
            camel(message.trim_end_matches("Request"))
        );
        let oneofs = model_oneof_groups(&message, PublicFieldDirection::Request);
        let field_declarations = fields
            .iter()
            .filter(|field| field.oneof_index.is_none())
            .map(|field| {
                format!(
                    "public let {}: {};\n",
                    swift_field_name(&field.field),
                    swift_field_type(field, PublicFieldDirection::Request)
                )
            })
            .collect::<String>();
        let choice_declarations = oneofs
            .iter()
            .map(|(oneof, members)| {
                format!(
                    "public let {}: {};\n",
                    swift_oneof_choice_property(oneof),
                    swift_oneof_choice_type(members)
                )
            })
            .collect::<String>();
        let params = fields
            .iter()
            .filter(|field| field.oneof_index.is_none())
            .map(|field| {
                format!(
                    "{}: {}",
                    swift_field_name(&field.field),
                    swift_field_type(field, PublicFieldDirection::Request)
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let choice_params = oneofs
            .iter()
            .map(|(oneof, members)| {
                format!(
                    "{}: {}? = nil",
                    swift_oneof_choice_property(oneof),
                    swift_oneof_choice_type(members)
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let assignments = fields
            .iter()
            .filter(|field| field.oneof_index.is_none())
            .map(|field| format!("self.{0} = {0}\n", swift_field_name(&field.field)))
            .chain(oneofs.iter().map(|(oneof, _)| {
                let property = swift_oneof_choice_property(oneof);
                format!("self.{property} = {property} ?? .none\n")
            }))
            .collect::<String>();
        let wire_fields = fields
            .iter()
            .filter(|field| field.oneof_index.is_none())
            .map(|field| {
                format!(
                    "\"{}\": {}",
                    field.json_name,
                    swift_field_name(&field.field)
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let wire_literal = if wire_fields.is_empty() {
            "var wire: [String: Any] = [:]".to_owned()
        } else {
            format!("var wire: [String: Any] = [{wire_fields}]")
        };
        let choice_wire = oneofs
            .iter()
            .map(|(oneof, _)| {
                swift_oneof_choice_wire_switch(
                    &swift_oneof_choice_property(oneof),
                    oneofs.get(oneof).expect("oneof members"),
                    oneof,
                )
            })
            .collect::<String>();
        let all_params = if params.is_empty() {
            choice_params.clone()
        } else if choice_params.is_empty() {
            params.clone()
        } else {
            format!("{params}, {choice_params}")
        };
        out.push_str(&format!("public struct {name}: RustWireRequest, Sendable {{\n{field_declarations}{choice_declarations} public init({all_params}) {{\n{assignments} }}\n public typealias Wire = [String: Any]\n public func toWire() -> [String: Any] {{ {wire_literal};{choice_wire} return wire }}\n}}\n"));
    }
    for ((module, message), fields) in response_groups() {
        let name = format!("{}{}Response", camel(&module), camel(&message));
        let oneofs = model_oneof_groups(&message, PublicFieldDirection::Response);
        let field_declarations = fields
            .iter()
            .filter(|field| field.oneof_index.is_none())
            .map(|field| {
                format!(
                    "public let {}: {};\n",
                    swift_field_name(&field.field),
                    swift_field_type(field, PublicFieldDirection::Response)
                )
            })
            .collect::<String>();
        let params = fields
            .iter()
            .filter(|field| field.oneof_index.is_none())
            .map(|field| {
                format!(
                    "{}: {}",
                    swift_field_name(&field.field),
                    swift_field_type(field, PublicFieldDirection::Response)
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let assignments = fields
            .iter()
            .filter(|field| field.oneof_index.is_none())
            .map(|field| format!("self.{0} = {0}\n", swift_field_name(&field.field)))
            .chain(oneofs.iter().map(|(oneof, _)| {
                let property = swift_oneof_choice_property(oneof);
                format!("self.{property} = {property} ?? .none\n")
            }))
            .collect::<String>();
        let choice_declarations = oneofs
            .iter()
            .map(|(oneof, members)| {
                format!(
                    "public let {}: {};\n",
                    swift_oneof_choice_property(oneof),
                    swift_oneof_choice_type(members)
                )
            })
            .collect::<String>();
        let choice_params = oneofs
            .iter()
            .map(|(oneof, members)| {
                format!(
                    "{}: {}? = nil",
                    swift_oneof_choice_property(oneof),
                    swift_oneof_choice_type(members)
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let decodes = fields.iter().map(|field| {
            if field.oneof_index.is_some() {
                return String::new();
            }
            let cast = swift_field_cast(field);
            let key = &field.json_name;
            let local = swift_local_name(&field.field);
            let field_name = swift_field_name(&field.field);
            let named_descriptor = semantic_field(field, PublicFieldDirection::Response).is_none() && matches!(field.wire_type, Some(kind) if kind == FieldType::Message as i32 || kind == FieldType::Group as i32 || kind == FieldType::Enum as i32)
                && descriptor_type_name(field).is_some();
            if named_descriptor {
                let target = swift_wire_type(field);
                if field.label == Some(FieldLabel::Repeated as i32) {
                    format!("guard let raw_{local} = wire[\"{key}\"] as? [{cast}] else {{ throw RustWireDecodeError.invalidField(\"{key}\") }}; let {field_name} = raw_{local}.map {{ {target}($0) }};", local=local, field_name=field_name, key=key, cast=cast, target=target)
                } else if field_has_presence(field) {
                    format!("let {field_name} = (wire[\"{key}\"] as? {cast}).map {{ {target}($0) }};", field_name=field_name, key=key, cast=cast, target=target)
                } else {
                    format!("guard let raw_{local} = wire[\"{key}\"] as? {cast} else {{ throw RustWireDecodeError.invalidField(\"{key}\") }}; let {field_name} = {target}(raw_{local});", local=local, field_name=field_name, key=key, cast=cast, target=target)
                }
            } else if field_has_presence(field) && semantic_field(field, PublicFieldDirection::Response).is_none() {
                format!("let {field} = wire[\"{key}\"] as? {cast};", field=swift_field_name(&field.field), key=key, cast=cast)
            } else if let Some(semantic) = semantic_field(field, PublicFieldDirection::Response) {
                let wire_cast = swift_wire_cast(semantic.wire_kind);
                if field.label == Some(FieldLabel::Repeated as i32) {
                    format!("guard let raw_{local} = wire[\"{key}\"] as? [{wire_cast}] else {{ throw RustWireDecodeError.invalidField(\"{key}\") }}; let {field_name} = raw_{local}.compactMap {{ {ty}($0) }};", local=local, field_name=field_name, key=key, wire_cast=wire_cast, ty=semantic.rust_name)
                } else if field_has_presence(field) {
                    format!("let {field_name} = (wire[\"{key}\"] as? {wire_cast}).flatMap {{ {ty}($0) }};", field_name=field_name, key=key, wire_cast=wire_cast, ty=semantic.rust_name)
                } else {
                    format!("guard let raw_{local} = wire[\"{key}\"] as? {wire_cast}, let {field_name} = {ty}(raw_{local}) else {{ throw RustWireDecodeError.invalidField(\"{key}\") }};", local=local, field_name=field_name, key=key, wire_cast=wire_cast, ty=semantic.rust_name)
                }
            } else {
                if field.label == Some(FieldLabel::Repeated as i32) {
                    format!("guard let {field_name} = wire[\"{key}\"] as? [{cast}] else {{ throw RustWireDecodeError.invalidField(\"{key}\") }};", field_name=field_name, key=key, cast=cast)
                } else {
                    format!("guard let {field_name} = wire[\"{key}\"] as? {cast} else {{ throw RustWireDecodeError.invalidField(\"{key}\") }};", field_name=field_name, key=key, cast=cast)
                }
            }
        }).collect::<String>();
        let choice_decodes = oneofs
            .iter()
            .map(|(oneof, members)| {
                let property = swift_oneof_choice_property(oneof);
                let choice_type = swift_oneof_choice_type(members);
                swift_oneof_decode_block(
                    &property,
                    &choice_type,
                    oneof,
                    members,
                    PublicFieldDirection::Response,
                )
            })
            .collect::<String>();
        let choice_args = oneofs
            .iter()
            .map(|(oneof, _)| {
                let property = swift_oneof_choice_property(oneof);
                format!("{property}: {property}")
            })
            .collect::<Vec<_>>()
            .join(", ");
        let args = fields
            .iter()
            .filter(|field| field.oneof_index.is_none())
            .map(|field| {
                let name = swift_field_name(&field.field);
                format!("{name}: {name}")
            })
            .collect::<Vec<_>>()
            .join(", ");
        let all_params = if params.is_empty() {
            choice_params.clone()
        } else if choice_params.is_empty() {
            params.clone()
        } else {
            format!("{params}, {choice_params}")
        };
        let all_args = if args.is_empty() {
            choice_args.clone()
        } else if choice_args.is_empty() {
            args.clone()
        } else {
            format!("{args}, {choice_args}")
        };
        out.push_str(&format!("public struct {name}: RustWireResponse, Sendable {{\n{field_declarations}{choice_declarations} public init({all_params}) throws {{\n{assignments} }}\n public typealias Wire = [String: Any]\n public static func fromWire(_ wire: [String: Any]) throws -> Self {{ {decodes}{choice_decodes} return try Self({all_args}) }}\n}}\n"));
    }
    for (message, bindings) in nested_message_groups() {
        let fields = bindings
            .iter()
            .map(|binding| {
                let semantic =
                    semantic_type(binding.semantic_type).expect("Rust policy nested binding");
                format!(
                    "public let {}: {};",
                    swift_field_name(binding.field),
                    swift_kind(semantic.wire_kind, semantic.rust_name)
                )
            })
            .collect::<String>();
        let params = bindings
            .iter()
            .map(|binding| {
                let semantic =
                    semantic_type(binding.semantic_type).expect("Rust policy nested binding");
                format!(
                    "{}: {}",
                    swift_field_name(binding.field),
                    swift_kind(semantic.wire_kind, semantic.rust_name)
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let assignments = bindings
            .iter()
            .map(|binding| format!("self.{0} = {0};", swift_field_name(binding.field)))
            .collect::<String>();
        out.push_str(&format!(
            "public struct {}: Sendable {{ {} public init({}) {{ {} }} }}\n",
            public_nested_type_name(message),
            fields,
            params,
            assignments
        ));
    }
    let nested_messages = PUBLIC_NESTED_ROUTES
        .iter()
        .flat_map(|route| route.fields.iter())
        .filter_map(|(_, kind)| match kind {
            crate::type_policy::PublicNestedFieldKind::Message(message) => Some(*message),
            crate::type_policy::PublicNestedFieldKind::Text => None,
        })
        .collect::<BTreeSet<_>>();
    for message in nested_messages {
        if !nested_message_groups().contains_key(message) {
            out.push_str(&format!("public struct {}: Sendable {{ private let wire: Data; internal init(wire: Data) {{ self.wire = wire }} }}\n", public_nested_type_name(message)));
        }
    }
    for route in PUBLIC_NESTED_ROUTES {
        let fields = route
            .fields
            .iter()
            .map(|(field, kind)| {
                let ty = match kind {
                    crate::type_policy::PublicNestedFieldKind::Text => "String".to_owned(),
                    crate::type_policy::PublicNestedFieldKind::Message(message) => {
                        public_nested_type_name(message)
                    }
                };
                format!("public let {}: {ty};", swift_field_name(field))
            })
            .collect::<String>();
        let params = route
            .fields
            .iter()
            .map(|(field, kind)| {
                let ty = match kind {
                    crate::type_policy::PublicNestedFieldKind::Text => "String".to_owned(),
                    crate::type_policy::PublicNestedFieldKind::Message(message) => {
                        public_nested_type_name(message)
                    }
                };
                format!("{}: {ty}", swift_field_name(field))
            })
            .collect::<Vec<_>>()
            .join(", ");
        let assignments = route
            .fields
            .iter()
            .map(|(field, _)| format!("self.{0} = {0};", swift_field_name(field)))
            .collect::<String>();
        out.push_str(&format!(
            "public struct {}{}NestedRequest: Sendable {{ {} public init({}) {{ {} }} }}\n",
            camel(route.module),
            camel(route.operation),
            fields,
            params,
            assignments
        ));
    }
    out.push_str("\npublic protocol RustTypedRemoteTransport { func call<Request: RustWireRequest, Response: RustWireResponse>(_ rpc: String, _ request: Request) async throws -> Response; func stream<Request: RustWireRequest, Response: RustWireResponse>(_ rpc: String, _ request: Request) async throws -> RustTypedStream<Response>; func openClientStream<Request: RustWireRequest, Response: RustWireResponse>(_ rpc: String) async throws -> RustTypedClientStream<Request, Response>; func sendClientStream<Request: RustWireRequest, Response: RustWireResponse>(_ rpc: String, _ requests: RustTypedRequestSequence<Request>) async throws -> Response }\npublic struct RustTypedClient<Transport: RustTypedRemoteTransport> { public let transport: Transport; public init(transport: Transport) { self.transport = transport }\n");
    let mut methods = BTreeSet::new();
    for method in resolved_rpc_methods().expect("Rust RPC identities must resolve") {
        let message = message_leaf(&method.input_message);
        let module = semantic_request_groups()
            .iter()
            .find_map(|((module, candidate), bindings)| {
                (*candidate == message
                    && bindings
                        .first()
                        .map(|binding| binding.family == method.family)
                        .unwrap_or(false))
                .then_some(*module)
            })
            .unwrap_or(method.family.as_str());
        if methods.insert((
            method.family.clone(),
            method.rpc.clone(),
            message.to_owned(),
        )) {
            let request = format!(
                "{}{}Request",
                camel(module),
                camel(message.trim_end_matches("Request"))
            );
            let mut method_chars = method.method.chars();
            let method_name = method_chars
                .next()
                .map(|first| first.to_lowercase().collect::<String>() + method_chars.as_str())
                .unwrap_or_else(|| method.method.clone());
            let response = format!(
                "{}{}Response",
                camel(&method.family),
                camel(message_leaf(&method.output_message))
            );
            let method_name = swift_method_name(&method_name);
            if method.client_streaming {
                out.push_str(&format!(" public func {method_name}() async throws -> RustTypedClientStream<{request}, {response}> {{ try await transport.openClientStream(\"{rpc}\") }} public func {method_name}(_ requests: RustTypedRequestSequence<{request}>) async throws -> {response} {{ try await transport.sendClientStream(\"{rpc}\", requests) }}\n", rpc = method.rpc));
            } else if method.server_streaming {
                out.push_str(&format!(" public func {method_name}(_ request: {request}) async throws -> RustTypedStream<{response}> {{ try await transport.stream(\"{rpc}\", request) }}\n", rpc = method.rpc));
            } else {
                out.push_str(&format!(" public func {method_name}(_ request: {request}) async throws -> {response} {{ try await transport.call(\"{rpc}\", request) }}\n", rpc = method.rpc));
            }
        }
    }
    out.push_str("}\n");
    out
}

fn render_cpp() -> String {
    let mut out = String::from(
        r#"// Generated by acyclic-sdk-contract-wire; do not edit.
#pragma once
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>
#include <variant>
#include <vector>
namespace acyclic::rust_typed {

struct RustWireMessage { std::vector<std::uint8_t> wire; };
struct RustWireEnum { std::int32_t raw; };

class RustCancellationToken {
  std::shared_ptr<const std::atomic_bool> flag_;
  explicit RustCancellationToken(std::shared_ptr<const std::atomic_bool> flag) : flag_(std::move(flag)) {}
  friend class RustCancellationSource;
public:
  RustCancellationToken() : flag_(std::make_shared<const std::atomic_bool>(false)) {}
  bool stop_requested() const noexcept { return flag_->load(std::memory_order_acquire); }
};

class RustCancellationSource {
  std::shared_ptr<std::atomic_bool> flag_ = std::make_shared<std::atomic_bool>(false);
public:
  RustCancellationSource() = default;
  RustCancellationSource(const RustCancellationSource&) = delete;
  RustCancellationSource& operator=(const RustCancellationSource&) = delete;
  RustCancellationSource(RustCancellationSource&& other) noexcept : flag_(std::move(other.flag_)) {
    if (!flag_) flag_ = std::make_shared<std::atomic_bool>(false);
    other.flag_ = std::make_shared<std::atomic_bool>(false);
  }
  RustCancellationSource& operator=(RustCancellationSource&& other) noexcept {
    if (this != &other) {
      flag_ = std::move(other.flag_);
      if (!flag_) flag_ = std::make_shared<std::atomic_bool>(false);
      other.flag_ = std::make_shared<std::atomic_bool>(false);
    }
    return *this;
  }
  RustCancellationToken get_token() const noexcept { return RustCancellationToken(flag_); }
  bool request_stop() noexcept {
    bool expected = false;
    return flag_->compare_exchange_strong(expected, true, std::memory_order_acq_rel);
  }
};

// Stream handles own cancellation.  A dropped handle requests stop through the
// shared token, while an explicit cancel calls the transport hook once.
template<class Element>
class RustTypedStream {
  RustCancellationSource cancellation_;
protected:
  virtual void on_cancel() noexcept {}
public:
  RustTypedStream() = default;
  RustTypedStream(const RustTypedStream&) = delete;
  RustTypedStream& operator=(const RustTypedStream&) = delete;
  RustTypedStream(RustTypedStream&&) noexcept = default;
  RustTypedStream& operator=(RustTypedStream&&) noexcept = default;
  virtual ~RustTypedStream() { cancellation_.request_stop(); }
  RustCancellationToken cancellation_token() const noexcept { return cancellation_.get_token(); }
  void cancel() noexcept {
    if (cancellation_.request_stop()) on_cancel();
  }
  virtual std::optional<Element> next() = 0;
};

template<class Request>
class RustTypedRequestSequence {
  RustCancellationSource cancellation_;
protected:
  virtual void on_cancel() noexcept {}
public:
  RustTypedRequestSequence() = default;
  RustTypedRequestSequence(const RustTypedRequestSequence&) = delete;
  RustTypedRequestSequence& operator=(const RustTypedRequestSequence&) = delete;
  RustTypedRequestSequence(RustTypedRequestSequence&&) noexcept = default;
  RustTypedRequestSequence& operator=(RustTypedRequestSequence&&) noexcept = default;
  virtual ~RustTypedRequestSequence() { cancellation_.request_stop(); }
  RustCancellationToken cancellation_token() const noexcept { return cancellation_.get_token(); }
  void cancel() noexcept {
    if (cancellation_.request_stop()) on_cancel();
  }
  virtual std::optional<Request> next() = 0;
};

template<class Request, class Response>
class RustTypedClientStream {
  RustCancellationSource cancellation_;
protected:
  virtual void on_cancel() noexcept {}
public:
  RustTypedClientStream() = default;
  RustTypedClientStream(const RustTypedClientStream&) = delete;
  RustTypedClientStream& operator=(const RustTypedClientStream&) = delete;
  RustTypedClientStream(RustTypedClientStream&&) noexcept = default;
  RustTypedClientStream& operator=(RustTypedClientStream&&) noexcept = default;
  virtual ~RustTypedClientStream() { cancellation_.request_stop(); }
  RustCancellationToken cancellation_token() const noexcept { return cancellation_.get_token(); }
  void cancel() noexcept {
    if (cancellation_.request_stop()) on_cancel();
  }
  virtual void send(const Request& request) = 0;
  virtual Response finish() = 0;
  virtual std::optional<Response> next() = 0;
};

"#,
    );
    // C++ requires nominal field types to be declared before any semantic
    // message that embeds them.  Emit descriptor-derived ordinary messages
    // and enums first so nested public fields never fall back to an opaque
    // RustWireMessage/RustWireEnum solely because of declaration order.
    let mut semantic_message_names = BTreeSet::new();
    for item in SEMANTIC_TYPES {
        if item.wire_kind == WireValueKind::Message && semantic_message_names.insert(item.rust_name)
        {
            out.push_str(&format!("struct {};\n", item.rust_name));
        }
    }
    let mut scalar_seen = BTreeSet::new();
    for item in SEMANTIC_TYPES {
        if !scalar_seen.insert(item.rust_name) || item.wire_kind == WireValueKind::Message {
            continue;
        }
        match item.wire_kind {
            WireValueKind::String => out.push_str(&format!("struct {} {{ std::string value; explicit {}(std::string value) : value(value) {{{}}} }};\n", item.rust_name, item.rust_name, cpp_checks(item.wire_kind, item.rules))),
            WireValueKind::Bytes => out.push_str(&format!("struct {} {{ std::vector<std::uint8_t> value; explicit {}(std::vector<std::uint8_t> value) : value(value) {{{}}} }};\n", item.rust_name, item.rust_name, cpp_checks(item.wire_kind, item.rules))),
            WireValueKind::UnsignedInteger => out.push_str(&format!("struct {} {{ std::uint64_t value; explicit {}(std::uint64_t value) : value(value) {{{}}} }};\n", item.rust_name, item.rust_name, cpp_checks(item.wire_kind, item.rules))),
            WireValueKind::SignedInteger => out.push_str(&format!("struct {} {{ std::int64_t value; explicit {}(std::int64_t value) : value(value) {{{}}} }};\n", item.rust_name, item.rust_name, cpp_checks(item.wire_kind, item.rules))),
            _ => {}
        }
    }
    // Enum wrappers must precede message definitions because C++ requires a
    // complete value type inside std::optional<T> and std::vector<T>.
    let mut ordinary_enums = ordinary_descriptor_names(FieldType::Enum);
    ordinary_enums.extend(resolved_descriptor_enum_names());
    ordinary_enums.extend(semantic_nested_descriptor_names(FieldType::Enum));
    for name in ordinary_enums {
        out.push_str(&format!("struct {name} {{ std::int32_t raw; explicit {name}(std::int32_t value) : raw(value) {{}} explicit {name}(RustWireEnum value) : raw(value.raw) {{}} }};\n"));
    }
    let mut ordinary_messages = ordinary_descriptor_names(FieldType::Message);
    ordinary_messages.extend(semantic_nested_descriptor_names(FieldType::Message));
    for name in &ordinary_messages {
        out.push_str(&format!("struct {name};\n"));
    }
    for name in ordinary_messages {
        let fields = ordinary_descriptor_fields(&name);
        let declarations = fields
            .iter()
            .map(|field| {
                let mut ty = cpp_field_type(field, PublicFieldDirection::NestedMessage);
                if !ty.starts_with("std::vector<") && !ty.starts_with("std::optional<") {
                    ty = format!("std::optional<{ty}>");
                }
                format!(
                    " {ty} {};{}\n",
                    cpp_field_name(&field.field),
                    cpp_field_annotation(&field.field)
                )
            })
            .collect::<String>();
        out.push_str(&format!("struct {name} {{ private: RustWireMessage wire; public: {declarations} explicit {name}(RustWireMessage value) : wire(std::move(value)) {{}} }};\n"));
    }
    render_cpp_oneof_models(&mut out);
    let mut seen = BTreeSet::new();
    for item in SEMANTIC_TYPES {
        if !seen.insert(item.rust_name) || item.wire_kind != WireValueKind::Message {
            continue;
        }
        match item.wire_kind {
            WireValueKind::String => out.push_str(&format!("struct {} {{ std::string value; explicit {}(std::string value) : value(value) {{{}}} }};\n", item.rust_name, item.rust_name, cpp_checks(item.wire_kind, item.rules))),
            WireValueKind::Bytes => out.push_str(&format!("struct {} {{ std::vector<std::uint8_t> value; explicit {}(std::vector<std::uint8_t> value) : value(value) {{{}}} }};\n", item.rust_name, item.rust_name, cpp_checks(item.wire_kind, item.rules))),
            WireValueKind::UnsignedInteger => out.push_str(&format!("struct {} {{ std::uint64_t value; explicit {}(std::uint64_t value) : value(value) {{{}}} }};\n", item.rust_name, item.rust_name, cpp_checks(item.wire_kind, item.rules))),
            WireValueKind::SignedInteger => out.push_str(&format!("struct {} {{ std::int64_t value; explicit {}(std::int64_t value) : value(value) {{{}}} }};\n", item.rust_name, item.rust_name, cpp_checks(item.wire_kind, item.rules))),
            WireValueKind::Message => {
                let nested = semantic_nested_fields(item.rust_name);
                let oneofs = model_oneof_groups(item.rust_name, PublicFieldDirection::NestedMessage);
                let declarations = nested
                    .iter()
                    .filter(|field| field.oneof_index.is_none())
                    .map(|field| {
                    let ty = cpp_field_type(field, PublicFieldDirection::NestedMessage);
                    let ty = if ty.starts_with("std::optional<") { ty } else { format!("std::optional<{ty}>") };
                    format!(
                        "{ty} {};{}\n",
                        cpp_field_name(&field.field),
                        cpp_field_annotation(&field.field)
                    )
                })
                    .chain(oneofs.iter().map(|(oneof, members)| {
                        format!(
                            "{} {}_choice = {}{{}};\n",
                            cpp_oneof_choice_value_type(oneof, members),
                            cpp_oneof_choice_field_name(oneof),
                            cpp_oneof_none_type(oneof, members)
                        )
                    }))
                    .collect::<String>();
                let typed_params = nested
                    .iter()
                    .filter(|field| field.oneof_index.is_none())
                    .map(|field| {
                        let ty = cpp_field_type(field, PublicFieldDirection::NestedMessage);
                        let ty = if ty.starts_with("std::optional<") {
                            ty
                        } else {
                            format!("std::optional<{ty}>")
                        };
                        format!("{ty} {} = std::nullopt", cpp_field_name(&field.field))
                    })
                    .chain(oneofs.iter().map(|(oneof, members)| {
                        format!(
                            "{} {}_choice = {}{{}}",
                            cpp_oneof_choice_value_type(oneof, members),
                            cpp_oneof_choice_field_name(oneof),
                            cpp_oneof_none_type(oneof, members)
                        )
                    }))
                    .collect::<Vec<_>>()
                    .join(", ");
                let typed_assignments = nested
                    .iter()
                    .filter(|field| field.oneof_index.is_none())
                    .map(|field| {
                        let name = cpp_field_name(&field.field);
                        format!(" this->{name} = std::move({name});")
                    })
                    .chain(oneofs.iter().map(|(oneof, _)| {
                        let name = cpp_oneof_choice_field_name(oneof);
                        format!(" this->{name}_choice = std::move({name}_choice);")
                    }))
                    .collect::<String>();
                let image_check = if item.rust_name == "Image" {
                    let checks = oneofs
                        .iter()
                        .map(|(oneof, members)| {
                            let name = cpp_oneof_choice_field_name(oneof);
                            let none = cpp_oneof_none_type(oneof, members);
                            format!("std::holds_alternative<{none}>({name}_choice)")
                        })
                        .collect::<Vec<_>>();
                    if checks.is_empty() {
                        String::new()
                    } else {
                        format!(" if ({}) throw std::invalid_argument(\"Image requires exactly one immutable reference\");", checks.join(" || "))
                    }
                } else {
                    String::new()
                };
                let raw_choice_assignments = oneofs
                    .iter()
                    .map(|(oneof, members)| {
                        format!(
                            ", {}_choice({}{{}})",
                            cpp_oneof_choice_field_name(oneof),
                            cpp_oneof_none_type(oneof, members)
                        )
                    })
                    .collect::<String>();
                out.push_str(&format!("struct {} {{ private: RustWireMessage wire; explicit {}(RustWireMessage value) : wire(std::move(value)){} {{{} }} public: {} {}({}) : {} {{{}}} }};\n", item.rust_name, item.rust_name, raw_choice_assignments, cpp_message_checks(item.rules), declarations, item.rust_name, typed_params, typed_assignments, image_check));
            }
            _ => {}
        }
    }
    let _ = WIRE_UNION_VARIANTS;
    // Keep the raw unknown representation available to generated internals,
    // while closed descriptor-bound choices remain the only public union API.
    out.push_str("namespace detail { struct UnknownOneof { std::int32_t raw_tag; std::vector<std::uint8_t> payload; }; }\n\n");
    for ((module, message), fields) in request_groups() {
        let name = format!(
            "{}{}Request",
            camel(&module),
            camel(message.trim_end_matches("Request"))
        );
        let oneofs = model_oneof_groups(&message, PublicFieldDirection::Request);
        let fields = fields
            .iter()
            .filter(|field| field.oneof_index.is_none())
            .map(|field| {
                format!(
                    "{} {};{}",
                    cpp_field_type(field, PublicFieldDirection::Request),
                    cpp_field_name(&field.field),
                    cpp_field_annotation(&field.field)
                )
            })
            .collect::<String>();
        let choices = oneofs
            .iter()
            .map(|(oneof, members)| {
                format!(
                    " {} {}_choice = {}{{}};",
                    cpp_oneof_choice_value_type(oneof, members),
                    cpp_oneof_choice_field_name(oneof),
                    cpp_oneof_none_type(oneof, members)
                )
            })
            .collect::<String>();
        out.push_str(&format!("struct {name} {{ {fields}{choices} }};\n"));
    }
    for ((module, message), fields) in response_groups() {
        let name = format!("{}{}Response", camel(&module), camel(&message));
        let oneofs = model_oneof_groups(&message, PublicFieldDirection::Response);
        let fields = fields
            .iter()
            .filter(|field| field.oneof_index.is_none())
            .map(|field| {
                format!(
                    "{} {};{}",
                    cpp_field_type(field, PublicFieldDirection::Response),
                    cpp_field_name(&field.field),
                    cpp_field_annotation(&field.field)
                )
            })
            .collect::<String>();
        let choices = oneofs
            .iter()
            .map(|(oneof, members)| {
                format!(
                    "{} {}_choice = {}{{}};",
                    cpp_oneof_choice_value_type(oneof, members),
                    cpp_oneof_choice_field_name(oneof),
                    cpp_oneof_none_type(oneof, members)
                )
            })
            .collect::<String>();
        out.push_str(&format!("struct {name} {{ {fields}{choices} }};\n"));
    }
    for (message, bindings) in nested_message_groups() {
        let fields = bindings
            .iter()
            .map(|binding| {
                let semantic =
                    semantic_type(binding.semantic_type).expect("Rust policy nested binding");
                format!(
                    "{} {};{}",
                    cpp_kind(semantic.wire_kind, semantic.rust_name),
                    cpp_field_name(binding.field),
                    cpp_field_annotation(binding.field)
                )
            })
            .collect::<String>();
        out.push_str(&format!(
            "struct {} {{ {} }};\n",
            public_nested_type_name(message),
            fields
        ));
    }
    let nested_messages = PUBLIC_NESTED_ROUTES
        .iter()
        .flat_map(|route| route.fields.iter())
        .filter_map(|(_, kind)| match kind {
            crate::type_policy::PublicNestedFieldKind::Message(message) => Some(*message),
            crate::type_policy::PublicNestedFieldKind::Text => None,
        })
        .collect::<BTreeSet<_>>();
    for message in nested_messages {
        if !nested_message_groups().contains_key(message) {
            out.push_str(&format!(
                "struct {} {{ std::vector<std::uint8_t> wire; }};\n",
                public_nested_type_name(message)
            ));
        }
    }
    for route in PUBLIC_NESTED_ROUTES {
        let fields = route
            .fields
            .iter()
            .map(|(field, kind)| {
                let ty = match kind {
                    crate::type_policy::PublicNestedFieldKind::Text => "std::string".to_owned(),
                    crate::type_policy::PublicNestedFieldKind::Message(message) => {
                        public_nested_type_name(message)
                    }
                };
                format!(
                    "{} {};{}",
                    ty,
                    cpp_field_name(field),
                    cpp_field_annotation(field)
                )
            })
            .collect::<String>();
        out.push_str(&format!(
            "struct {}{}NestedRequest {{ {} }};\n",
            camel(route.module),
            camel(route.operation),
            fields
        ));
    }
    out.push_str("\ntemplate<class Transport> class RustTypedClient { Transport& transport_; public: explicit RustTypedClient(Transport& transport) : transport_(transport) {} template<class Request, class Response> Response call(const char* rpc, const Request& request) { return transport_.template call<Request, Response>(rpc, request); } template<class Request, class Response> Response call_cancellable(const char* rpc, const Request& request, RustCancellationToken cancellation_token) { if (cancellation_token.stop_requested()) throw std::system_error(std::make_error_code(std::errc::operation_canceled)); return transport_.template call<Request, Response>(rpc, request, cancellation_token); } template<class Request, class Response> std::unique_ptr<RustTypedStream<Response>> stream(const char* rpc, const Request& request) { return transport_.template stream<Request, Response>(rpc, request); } template<class Request, class Response> std::unique_ptr<RustTypedStream<Response>> stream_cancellable(const char* rpc, const Request& request, RustCancellationToken cancellation_token) { if (cancellation_token.stop_requested()) throw std::system_error(std::make_error_code(std::errc::operation_canceled)); return transport_.template stream<Request, Response>(rpc, request, cancellation_token); } template<class Request, class Response> std::unique_ptr<RustTypedClientStream<Request, Response>> open_client_stream(const char* rpc) { return transport_.template open_client_stream<Request, Response>(rpc); } template<class Request, class Response> Response send_client_stream(const char* rpc, RustTypedRequestSequence<Request>& requests) { return transport_.template send_client_stream<Request, Response>(rpc, requests); }\n");
    let mut methods = BTreeSet::new();
    for method_info in resolved_rpc_methods().expect("Rust RPC identities must resolve") {
        let message = message_leaf(&method_info.input_message);
        let module = semantic_request_groups()
            .iter()
            .find_map(|((module, candidate), bindings)| {
                (*candidate == message
                    && bindings
                        .first()
                        .map(|binding| binding.family == method_info.family)
                        .unwrap_or(false))
                .then_some(*module)
            })
            .unwrap_or(method_info.family.as_str());
        if methods.insert((
            method_info.family.clone(),
            method_info.rpc.clone(),
            message.to_owned(),
        )) {
            let request = format!(
                "{}{}Request",
                camel(module),
                camel(message.trim_end_matches("Request"))
            );
            let method_name = cpp_method_name(&method_info.method);
            let response = format!(
                "{}{}Response",
                camel(&method_info.family),
                camel(message_leaf(&method_info.output_message))
            );
            if method_info.client_streaming {
                out.push_str(&format!(" std::unique_ptr<RustTypedClientStream<{request}, {response}>> {method_name}() {{ return open_client_stream<{request}, {response}>(\"{rpc}\"); }} {response} {method_name}(RustTypedRequestSequence<{request}>& requests) {{ return send_client_stream<{request}, {response}>(\"{rpc}\", requests); }}\n", rpc = method_info.rpc));
            } else if method_info.server_streaming {
                out.push_str(&format!(" std::unique_ptr<RustTypedStream<{response}>> {method_name}(const {request}& request) {{ return stream<{request}, {response}>(\"{rpc}\", request); }} std::unique_ptr<RustTypedStream<{response}>> {method_name}(const {request}& request, RustCancellationToken cancellation_token) {{ return stream_cancellable<{request}, {response}>(\"{rpc}\", request, cancellation_token); }}\n", rpc = method_info.rpc));
            } else {
                out.push_str(&format!(" {response} {method_name}(const {request}& request) {{ return call<{request}, {response}>(\"{rpc}\", request); }} {response} {method_name}(const {request}& request, RustCancellationToken cancellation_token) {{ return call_cancellable<{request}, {response}>(\"{rpc}\", request, cancellation_token); }}\n", rpc = method_info.rpc));
            }
        }
    }
    out.push_str("};\n}\n");
    out
}

#[cfg(test)]
mod tests {
    use super::{CPP_TYPED_PATH, SWIFT_TYPED_PATH, generate_swift_cpp_typed_facades};
    #[test]
    fn emits_nominal_clients_and_closed_unions_with_unknown_fallback() {
        let files = generate_swift_cpp_typed_facades();
        assert!(
            files
                .iter()
                .any(|(_, source)| source.contains("RustTypedRemoteTransport"))
        );
        assert!(
            files
                .iter()
                .any(|(_, source)| source.contains("RustTypedClient"))
        );
        assert!(
            files
                .iter()
                .any(|(_, source)| source.contains("InferenceCustomerRunResultContextChoice"))
        );
        let swift = files
            .iter()
            .find(|(path, _)| *path == SWIFT_TYPED_PATH)
            .map(|(_, source)| source)
            .expect("Swift facade must be generated");
        assert!(swift.contains("public enum InferenceCustomerRunResultContextChoice: Sendable"));
        assert!(swift.contains("case Context(InferenceContextViewWire)"));
        assert!(swift.contains("case unknown(rawTag: Int32, payload: Data)"));
        assert!(swift.contains("public struct RustWireUnknownOneof"));
        assert!(swift.contains("public final class RustTypedStream"));
        assert!(swift.contains("public final class RustTypedRequestSequence"));
        assert!(swift.contains("public final class RustTypedClientStream"));
        assert!(swift.contains("func openClientStream<Request: RustWireRequest, Response: RustWireResponse>"));
        assert!(swift.contains("func sendClientStream<Request: RustWireRequest, Response: RustWireResponse>"));
        assert!(swift.contains("func stream<Request: RustWireRequest, Response: RustWireResponse>"));
        assert!(!swift.contains("func run<Response: RustWireResponse>"));
        assert!(swift.contains("private var nextTicket = 0"));
        assert!(swift.contains("await self.waitForTurn(ticket)"));
        assert!(swift.contains("defer { self.complete(ticket) }"));
        assert!(swift.contains("deinit { cancellation.cancelIfNeeded() }"));
        assert!(swift.contains("cancellation.completeWithoutCancel()"));
        assert!(swift.contains("guard lifecycle == .open"));
        assert!(!swift.contains("executor.submit { try await self."));
        assert!(swift.contains("Choice: "));
        assert!(swift.contains("__unknown_"));
        assert!(!swift.contains("case known(tag: String"));
        assert!(!swift.contains("public enum WireChoice"));
        assert!(!swift.contains("std::shared_ptr"));
        let cpp = files
            .iter()
            .find(|(path, _)| *path == CPP_TYPED_PATH)
            .map(|(_, source)| source)
            .expect("C++ facade must be generated");
        assert!(cpp.contains("using InferenceCustomerRunResultContextChoiceValue = std::variant"));
        assert!(cpp.contains("struct InferenceCustomerRunResultContextChoiceContext { std::shared_ptr<InferenceContextViewWire> value; }"));
        assert!(cpp.contains("struct InferenceCustomerRunResultContextChoiceUnknown { std::int32_t raw_tag; std::vector<std::uint8_t> payload; }"));
        assert!(cpp.contains("class RustCancellationToken"));
        assert!(cpp.contains("class RustCancellationSource"));
        assert!(cpp.contains("class RustTypedStream"));
        assert!(cpp.contains("class RustTypedRequestSequence"));
        assert!(cpp.contains("class RustTypedClientStream"));
        assert!(cpp.contains("open_client_stream"));
        assert!(cpp.contains("send_client_stream"));
        assert!(cpp.contains("stream_cancellable"));
        assert!(cpp.contains("RustCancellationToken cancellation_token"));
        assert!(cpp.contains("call_cancellable"));
        assert!(cpp.contains("std::errc::operation_canceled"));
        assert!(cpp.contains("RustTypedStream(const RustTypedStream&) = delete"));
        assert!(cpp.contains("RustTypedStream(RustTypedStream&&) noexcept = default"));
        assert!(cpp.contains("RustCancellationSource(RustCancellationSource&& other) noexcept"));
        assert!(cpp.contains("virtual ~RustTypedStream() { cancellation_.request_stop(); }"));
        assert!(cpp.contains("void on_cancel() noexcept"));
        assert!(cpp.contains("if (cancellation_.request_stop()) on_cancel();"));
        assert!(cpp.contains("virtual ~RustTypedClientStream() { cancellation_.request_stop(); }"));
        assert!(!cpp.contains("virtual void cancel() noexcept = 0"));
        assert!(!cpp.contains("struct KnownOneof"));
        assert!(!cpp.contains("std::variant<std::string, std::vector<std::uint8_t>>"));
        assert!(!cpp.contains("using WireChoice"));
    }
}
