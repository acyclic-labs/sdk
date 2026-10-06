//! Rust-owned nominal projections for the Ruby, PHP, and Dart SDKs.
//!
//! The generated files are deliberately separate from the transport policy
//! files.  They are the public type boundary: request and response classes,
//! semantic refinements, open wire values, and per-RPC client methods all
//! come from the resolved Rust descriptor model and `type_policy.rs`.

use std::collections::{BTreeMap, BTreeSet};

use prost_types::field_descriptor_proto::Type as FieldType;

use crate::type_policy::{
    resolved_oneof_members, resolved_request_fields, resolved_response_fields,
    resolved_rpc_methods, ResolvedOneofMember, ResolvedRequestField, SemanticType,
    WireValueKind, SEMANTIC_TYPES,
};

pub const RUBY_TYPED_PATH: &str = "ruby/lib/acyclic_sdk/generated_typed.rb";
pub const RUBY_RBS_PATH: &str = "ruby/sig/acyclic_sdk/generated_typed.rbs";
pub const RUBY_SORBET_PATH: &str = "ruby/sorbet/rust_generated.rbi";
pub const PHP_TYPED_PATH: &str = "php/src/Acyclic/Generated/RustTyped.php";
pub const DART_TYPED_PATH: &str = "dart/lib/src/generated_typed.dart";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PortableTypedOutput {
    pub path: &'static str,
    pub source: String,
}

pub fn generate_portable_typed_facades() -> Vec<PortableTypedOutput> {
    vec![
        PortableTypedOutput { path: RUBY_TYPED_PATH, source: render_ruby() },
        PortableTypedOutput { path: RUBY_RBS_PATH, source: render_rbs() },
        PortableTypedOutput { path: RUBY_SORBET_PATH, source: render_sorbet() },
        PortableTypedOutput { path: PHP_TYPED_PATH, source: render_php() },
        PortableTypedOutput { path: DART_TYPED_PATH, source: render_dart() },
    ]
}

fn models() -> (Vec<crate::type_policy::ResolvedRpcMethod>, Vec<ResolvedRequestField>, Vec<ResolvedRequestField>) {
    (
        resolved_rpc_methods().expect("Rust RPC identities must resolve before typed facade generation"),
        resolved_request_fields().expect("Rust request shapes must resolve before typed facade generation"),
        resolved_response_fields().expect("Rust response shapes must resolve before typed facade generation"),
    )
}

fn camel(value: &str) -> String {
    value.split(['.', '_', '-']).filter(|part| !part.is_empty()).map(|part| {
        let mut chars = part.chars();
        chars.next().map(|first| first.to_uppercase().collect::<String>() + chars.as_str()).unwrap_or_default()
    }).collect()
}

fn method_name(value: &str) -> String {
    let mut out = String::new();
    for (index, ch) in value.chars().enumerate() {
        if ch.is_ascii_uppercase() && index != 0 { out.push('_'); }
        out.push(ch.to_ascii_lowercase());
    }
    out
}

fn rpc_method_name(method: &crate::type_policy::ResolvedRpcMethod) -> String {
    method_name(&format!("{}_{}_{}", method.family, method.service.trim_end_matches("Service"), method.method))
}

fn semantic_descriptor(id: &str) -> Option<&'static SemanticType> {
    SEMANTIC_TYPES.iter().find(|semantic| semantic.id == id)
}

/// Return exactly the semantic identities reached by the Rust descriptor
/// closure.  The previous implementation carried a second handwritten list
/// here, which could silently expose stale wrappers or omit newly bound Rust
/// fields.  The descriptor-resolved fields are the authority; the policy table
/// only supplies the native name and wire kind for each reached identity.
fn semantic_inventory() -> Vec<&'static SemanticType> {
    let mut ids = BTreeSet::new();
    for field in resolved_request_fields()
        .expect("Rust request fields must resolve")
        .into_iter()
        .chain(resolved_response_fields().expect("Rust response fields must resolve"))
    {
        if let Some(id) = field.semantic_type {
            ids.insert(id);
        }
        if field
            .wire_type
            .and_then(|value| FieldType::try_from(value).ok())
            == Some(FieldType::Enum)
        {
            // Open enum values are a Rust-owned wire projection even when a
            // field has no narrower semantic binding in the policy table.
            ids.insert("enum_value".to_owned());
        }
    }
    let mut emitted_names = BTreeSet::new();
    let mut output = SEMANTIC_TYPES
        .iter()
        .filter(|semantic| {
            ids.contains(semantic.id) && emitted_names.insert(semantic.rust_name)
        })
        .collect::<Vec<_>>();
    output.sort_by_key(|semantic| semantic.id);
    output
}

fn semantic_class(value: &str) -> String {
    semantic_descriptor(value)
        .map(|semantic| format!("Rust{}", camel(semantic.rust_name)))
        .unwrap_or_else(|| format!("Rust{}", camel(value)))
}

fn semantic_value_type(id: &str) -> &'static str {
    match semantic_descriptor(id).map(|semantic| semantic.wire_kind) {
        Some(WireValueKind::Boolean) => "Boolean",
        Some(WireValueKind::SignedInteger | WireValueKind::UnsignedInteger | WireValueKind::Enum | WireValueKind::Oneof) => "Integer",
        _ => "String",
    }
}

fn message_class(value: &str) -> String { format!("Rust{}", camel(value.trim_start_matches('.'))) }

fn semantic_or_wire(field: &ResolvedRequestField) -> String {
    if let Some(semantic) = field.semantic_type.as_deref() {
        return semantic_class(semantic);
    }
    match field.wire_type.and_then(|value| FieldType::try_from(value).ok()) {
        Some(FieldType::Bool) => "bool".into(),
        Some(FieldType::Double | FieldType::Float) => "float".into(),
        Some(FieldType::Int32 | FieldType::Sint32 | FieldType::Sfixed32 | FieldType::Int64 | FieldType::Sint64 | FieldType::Sfixed64 | FieldType::Uint32 | FieldType::Fixed32 | FieldType::Uint64 | FieldType::Fixed64) => "int".into(),
        Some(FieldType::String) => "String".into(),
        Some(FieldType::Bytes) => "Bytes".into(),
        Some(FieldType::Message | FieldType::Group) => field.type_name.as_deref().map(message_class).unwrap_or_else(|| "RustWireMessage".into()),
        Some(FieldType::Enum) => "RustOpenEnumValue".into(),
        None => "RustWireValue".into(),
    }
}

fn repeated(field: &ResolvedRequestField) -> bool { field.label == Some(3) }
fn required(field: &ResolvedRequestField) -> bool { field.label == Some(2) }
fn message_field(field: &ResolvedRequestField) -> bool {
    matches!(field.wire_type.and_then(|value| FieldType::try_from(value).ok()), Some(FieldType::Message | FieldType::Group))
}
fn optional(field: &ResolvedRequestField) -> bool {
    if repeated(field) || required(field) { return false; }
    field.proto3_optional || field.oneof_index.is_some() || message_field(field)
}

fn rust_messages(fields: impl IntoIterator<Item = ResolvedRequestField>) -> BTreeMap<String, Vec<ResolvedRequestField>> {
    let mut out = BTreeMap::new();
    for field in fields {
        let message = field.message_path.trim_start_matches('.').to_owned();
        let fields = out.entry(message).or_insert_with(Vec::new);
        if !fields.iter().any(|candidate: &ResolvedRequestField| candidate.number == field.number) {
            fields.push(field);
        }
    }
    out
}

fn all_messages() -> BTreeMap<String, Vec<ResolvedRequestField>> {
    let (methods, requests, responses) = models();
    let mut out = rust_messages(requests.into_iter().chain(responses));
    for method in methods {
        out.entry(method.input_message.trim_start_matches('.').to_owned()).or_insert_with(Vec::new);
        out.entry(method.output_message.trim_start_matches('.').to_owned()).or_insert_with(Vec::new);
    }
    let referenced = out.values().flat_map(|fields| fields.iter()).filter_map(|field| {
        match field.wire_type.and_then(|value| FieldType::try_from(value).ok()) {
            Some(FieldType::Message | FieldType::Group) => field.type_name.clone(),
            _ => None,
        }
    }).collect::<Vec<_>>();
    for message in referenced { out.entry(message.trim_start_matches('.').to_owned()).or_insert_with(Vec::new); }
    out
}

/// Group concrete protobuf oneof members by their descriptor message and
/// oneof identity.  Synthetic oneofs backing proto3 optional fields are
/// intentionally excluded: those fields are represented by nullable members
/// and must not become a misleading user-facing union.  The returned members
/// still carry their exact field numbers and payload type identities, and each
/// generated target receives an explicit unknown arm for forward compatibility.
fn oneof_groups() -> Vec<(String, String, Vec<ResolvedOneofMember>)> {
    let mut groups: BTreeMap<(String, String), Vec<ResolvedOneofMember>> = BTreeMap::new();
    for member in resolved_oneof_members()
        .expect("Rust oneof members must resolve")
        .into_iter()
        .filter(|member| !member.field.proto3_optional)
    {
        let Some(oneof) = member.field.oneof_name.clone() else { continue };
        let key = (
            member.field.message_path.trim_start_matches('.').to_owned(),
            oneof,
        );
        let entries = groups.entry(key).or_default();
        if !entries.iter().any(|existing| existing.field.number == member.field.number) {
            entries.push(member);
        }
    }
    groups
        .into_iter()
        .filter(|(_, members)| !members.is_empty())
        .map(|((message, oneof), members)| (message, oneof, members))
        .collect()
}

fn oneof_class(message: &str, oneof: &str) -> String {
    format!("{}{}Choice", message_class(message), camel(oneof.trim_start_matches('_')))
}

fn oneof_variant(member: &ResolvedOneofMember) -> String {
    camel(&member.field.field)
}

fn wire_scalar_type(field: &ResolvedRequestField) -> &'static str {
    match field.wire_type.and_then(|value| FieldType::try_from(value).ok()) {
        Some(FieldType::Bool) => "bool",
        Some(FieldType::Double | FieldType::Float) => "float",
        Some(FieldType::Int32 | FieldType::Sint32 | FieldType::Sfixed32 | FieldType::Int64 | FieldType::Sint64 | FieldType::Sfixed64 | FieldType::Uint32 | FieldType::Fixed32 | FieldType::Uint64 | FieldType::Fixed64) => "int",
        Some(FieldType::String) => "String",
        Some(FieldType::Bytes) => "Bytes",
        Some(FieldType::Enum) => "RustOpenEnumValue",
        Some(FieldType::Message | FieldType::Group) => "RustWireMessage",
        None => "RustWireValue",
    }
}

fn oneof_ruby_payload(member: &ResolvedOneofMember) -> String {
    if member.payload_type.is_some() && matches!(member.payload_kind, FieldType::Message | FieldType::Group) {
        message_class(member.payload_type.as_deref().unwrap_or("RustWireMessage"))
    } else if member.payload_kind == FieldType::Enum {
        "RustOpenEnumValue".to_owned()
    } else {
        match wire_scalar_type(&member.field) {
            "bool" => "bool".to_owned(),
            "float" => "Float".to_owned(),
            "int" => "Integer".to_owned(),
            "Bytes" => "String".to_owned(),
            "String" => "String".to_owned(),
            other => other.to_owned(),
        }
    }
}

fn oneof_dart_payload(member: &ResolvedOneofMember) -> String {
    if member.payload_type.is_some() && matches!(member.payload_kind, FieldType::Message | FieldType::Group) {
        message_class(member.payload_type.as_deref().unwrap_or("RustWireMessage"))
    } else {
        match wire_scalar_type(&member.field) {
            "bool" => "bool".to_owned(),
            "float" => "double".to_owned(),
            "int" => "int".to_owned(),
            "Bytes" => "List<int>".to_owned(),
            "String" => "String".to_owned(),
            "RustOpenEnumValue" => "RustOpenEnumValue".to_owned(),
            other => other.to_owned(),
        }
    }
}

fn oneof_php_payload(member: &ResolvedOneofMember) -> String {
    match oneof_dart_payload(member).as_str() {
        "bool" => "bool".to_owned(),
        "double" => "float".to_owned(),
        "int" => "int".to_owned(),
        "List<int>" | "String" => "string".to_owned(),
        value => value.to_owned(),
    }
}

fn ruby_type(field: &ResolvedRequestField) -> String {
    let semantic = semantic_or_wire(field);
    let base = match semantic.as_str() {
        "bool" => "bool".to_owned(),
        "float" => "Float".to_owned(),
        "int" => "Integer".to_owned(),
        "String" => "String".to_owned(),
        "Bytes" => "String".to_owned(),
        value => format!("Acyclic::GeneratedTyped::{value}"),
    };
    let base = if repeated(field) { format!("Array[{}]", base) } else { base };
    if optional(field) { format!("{}?", base) } else { base }
}

fn ruby_default(field: &ResolvedRequestField) -> &'static str {
    if optional(field) { return "nil"; }
    if repeated(field) { return "[]"; }
    match semantic_or_wire(field).as_str() {
        "bool" => "false", "int" => "0", "float" => "0.0", "String" | "Bytes" => "\"\"", _ => "nil",
    }
}

fn php_type(field: &ResolvedRequestField) -> String {
    let semantic = semantic_or_wire(field);
    let base = match semantic.as_str() {
        "bool" => "bool".to_owned(), "float" => "float".to_owned(), "int" => "int".to_owned(), "String" | "Bytes" => "string".to_owned(),
        value => value.to_owned(),
    };
    let base = if repeated(field) { "array".to_owned() } else { base };
    if optional(field) { format!("?{}", base) } else { base }
}

fn php_default(field: &ResolvedRequestField) -> String {
    if repeated(field) { return " = []".to_owned(); }
    if optional(field) { return " = null".to_owned(); }
    match semantic_or_wire(field).as_str() {
        "bool" => " = false".to_owned(),
        "float" => " = 0.0".to_owned(),
        "int" => " = 0".to_owned(),
        "String" | "Bytes" => " = ''".to_owned(),
        "RustOpenEnumValue" => " = new RustOpenEnumValue(0)".to_owned(),
        _ => String::new(),
    }
}

fn php_scalar_default(field: &ResolvedRequestField) -> &'static str {
    match semantic_or_wire(field).as_str() {
        "bool" => "false",
        "float" => "0.0",
        "int" => "0",
        "String" | "Bytes" => "''",
        _ => "null",
    }
}

fn php_decode_single(field: &ResolvedRequestField, value: &str) -> String {
    if let Some(semantic) = field.semantic_type.as_deref() {
        let class = semantic_class(semantic);
        let scalar = if semantic_value_type(semantic) == "Integer" || class.ends_with("PageLimit") || class.ends_with("OpenEnumValue") || class.ends_with("OneofArm") { "int" } else { "string" };
        return format!("new {class}(({scalar})({value}))");
    }
    if message_field(field) {
        let class = field.type_name.as_deref().map(message_class).unwrap_or_else(|| "RustWireMessage".into());
        return format!("{class}::fromWire(self::wireMap({value}))");
    }
    match semantic_or_wire(field).as_str() {
        "bool" => format!("(bool)({value})"),
        "float" => format!("(float)({value})"),
        "int" => format!("(int)({value})"),
        "String" | "Bytes" => format!("(string)({value})"),
        "RustOpenEnumValue" => format!("new RustOpenEnumValue((int)({value}))"),
        semantic => format!("new {semantic}((string)({value}))"),
    }
}

fn php_decode_value(field: &ResolvedRequestField) -> String {
    let raw = format!("($value['{}'] ?? {})", field.json_name, if optional(field) { "null" } else if repeated(field) { "[]" } else { php_scalar_default(field) });
    if repeated(field) {
        let item = php_decode_single(field, "$item");
        return format!("array_map(static fn(mixed $item) => {item}, self::wireArray({raw}))");
    }
    let decoded = php_decode_single(field, &raw);
    if optional(field) { format!("({raw} === null ? null : ({decoded}))") } else { decoded }
}

fn php_doc_type(field: &ResolvedRequestField) -> String {
    let base = match semantic_or_wire(field).as_str() {
        "bool" => "bool".to_owned(),
        "float" => "float".to_owned(),
        "int" => "int".to_owned(),
        "String" | "Bytes" => "string".to_owned(),
        value => value.to_owned(),
    };
    if repeated(field) { format!("array<int, {base}>") } else if optional(field) { format!("?{base}") } else { base }
}

fn dart_type(field: &ResolvedRequestField) -> String {
    let semantic = semantic_or_wire(field);
    let base = match semantic.as_str() {
        "bool" => "bool".to_owned(), "float" => "double".to_owned(), "int" => "int".to_owned(), "String" => "String".to_owned(), "Bytes" => "List<int>".to_owned(),
        value => value.to_owned(),
    };
    let base = if repeated(field) { format!("List<{}>", base) } else { base };
    if optional(field) { format!("{}?", base) } else { base }
}

fn ruby_field(field: &ResolvedRequestField) -> String {
    match field.field.as_str() {
        "alias" | "and" | "begin" | "break" | "case" | "class" | "def" | "defined" | "do" | "else" | "elsif" | "end" | "ensure" | "false" | "for" | "if" | "in" | "module" | "next" | "nil" | "not" | "or" | "redo" | "rescue" | "retry" | "return" | "self" | "super" | "then" | "true" | "undef" | "unless" | "until" | "when" | "while" | "yield" => format!("{}_", field.field),
        value => value.to_owned(),
    }
}

fn ruby_wire_field(field: &ResolvedRequestField) -> String { format!(":{}", field.field) }
fn php_field(field: &ResolvedRequestField) -> String { field.field.clone() }
fn dart_field(field: &ResolvedRequestField) -> String { field.field.clone() }

fn ruby_wrapper(name: &str, _value_type: &str) -> String {
    format!("        class Rust{name}\n          attr_reader :value\n          def initialize(value)\n            raise ArgumentError, \"invalid {name}\" if value.nil?\n            @value = value\n            freeze\n          end\n          def to_wire = @value\n        end\n")
}

fn ruby_message(name: &str, fields: &[ResolvedRequestField]) -> String {
    let mut out = format!("        class {name}\n          attr_reader {}\n          def initialize(\n", fields.iter().map(|f| format!(":{}", ruby_field(f))).collect::<Vec<_>>().join(", "));
    for field in fields { out.push_str(&format!("            {}: {},\n", ruby_field(field), ruby_default(field))); }
    out.push_str("            **unknown\n          )\n");
    for field in fields { out.push_str(&format!("            @{} = {}\n", ruby_field(field), ruby_field(field))); }
    out.push_str("            @unknown = unknown.freeze\n            freeze\n          end\n          def to_wire\n            values = {\n");
    for field in fields { out.push_str(&format!("              {0} => {1}.respond_to?(:to_wire) ? {1}.to_wire : {1},\n", ruby_wire_field(field), ruby_field(field))); }
    out.push_str("            }\n            values.merge(@unknown)\n          end\n        end\n");
    out
}

fn ruby_oneof_group(message: &str, oneof: &str, members: &[ResolvedOneofMember]) -> String {
    let base = oneof_class(message, oneof);
    let mut out = format!("        class {base}\n          attr_reader :tag, :value\n          def initialize(tag:, value:)\n            @tag = String(tag).freeze\n            @value = value\n            freeze\n          end\n          def self.unknown(tag:, payload:) = Unknown.new(tag: tag, payload: payload)\n          class Unknown < {base}\n            attr_reader :unknown_tag, :payload\n            def initialize(tag:, payload:)\n              @unknown_tag = Integer(tag)\n              @payload = payload.freeze\n              super(tag: \"unknown:#{{tag}}\", value: @payload)\n            end\n          end\n");
    for member in members {
        let variant = oneof_variant(member);
        out.push_str(&format!("          class {variant} < {base}\n            def initialize(value) = super(tag: \"{}\", value: value)\n          end\n", member.field.field));
    }
    out.push_str("        end\n");
    out
}

fn render_ruby() -> String {
    let (methods, _, _) = models();
    let mut out = String::from("# Generated by acyclic-sdk-contract-wire; do not edit.\n# Every public type and RPC signature originates in Rust type_policy.rs.\n\nmodule Acyclic\n  module GeneratedTyped\n");
    for semantic in semantic_inventory() { out.push_str(&ruby_wrapper(&camel(semantic.rust_name), semantic_value_type(semantic.id))); }
    for (message, oneof, members) in oneof_groups() { out.push_str(&ruby_oneof_group(&message, &oneof, &members)); }
    for (message, fields) in all_messages() { out.push_str(&ruby_message(&message_class(&message), &fields)); }
    out.push_str("        class Client\n          def initialize(callable) = (@call = callable)\n");
    for method in methods {
        let request = message_class(&method.input_message);
        let response = message_class(&method.output_message);
        let request_type = if method.client_streaming { format!("Array[{}]", request) } else { request.clone() };
        let request_check = if method.client_streaming { format!("request.is_a?(Array) && request.all? {{ |item| item.is_a?({request}) }}") } else { format!("request.is_a?({request})") };
        let result = if method.server_streaming { format!("value.map {{ |item| {}.new(**(item.respond_to?(:to_h) ? item.to_h : item)) }}", response) } else { format!("{}.new(**(value.respond_to?(:to_h) ? value.to_h : value))", response) };
        out.push_str(&format!("          def {}(request)\n            raise TypeError, \"expected {}\" unless {}\n            value = @call.call({:?}, request.respond_to?(:to_wire) ? request.to_wire : request)\n            {}\n          end\n", rpc_method_name(&method), request_type, request_check, method.rpc, result));
    }
    out.push_str("        end\n  end\nend\n");
    out
}

fn rbs_type(field: &ResolvedRequestField) -> String { ruby_type(field) }

fn render_rbs() -> String {
    let (methods, _, _) = models();
    let mut out = String::from("# Generated by acyclic-sdk-contract-wire.\nmodule Acyclic\n  module GeneratedTyped\n");
    for semantic in semantic_inventory() { let name = camel(semantic.rust_name); out.push_str(&format!("    class Rust{name} < Object\n      attr_reader value: {}\n      def initialize: ({}) -> void\n      def to_wire: () -> {}\n    end\n", semantic_value_type(semantic.id), semantic_value_type(semantic.id), semantic_value_type(semantic.id))); }
    for (message, oneof, members) in oneof_groups() {
        let base = oneof_class(&message, &oneof);
        out.push_str(&format!("    class {base} < Object\n      attr_reader tag: String\n      attr_reader value: untyped\n      def initialize: (tag: String, value: untyped) -> void\n      def self.unknown: (tag: Integer, payload: String) -> {base}::Unknown\n"));
        for member in members { out.push_str(&format!("      class {} < {base}\n        def initialize: (untyped) -> void\n      end\n", oneof_variant(&member))); }
        out.push_str(&format!("      class Unknown < {base}\n        attr_reader unknown_tag: Integer\n        attr_reader payload: String\n        def initialize: (tag: Integer, payload: String) -> void\n      end\n"));
        out.push_str("    end\n");
    }
    for (message, fields) in all_messages() {
        let name = message_class(&message);
        out.push_str(&format!("    class {name} < Object\n"));
        for field in &fields { out.push_str(&format!("      attr_reader {}: {}\n", ruby_field(field), rbs_type(field))); }
        out.push_str("      def initialize: (**untyped) -> void\n      def to_wire: () -> Hash[Symbol, untyped]\n    end\n");
    }
    out.push_str("    class Client\n");
    for method in methods {
        let request = message_class(&method.input_message);
        let response = message_class(&method.output_message);
        let req = if method.client_streaming { format!("Array[{request}]") } else { request };
        let ret = if method.server_streaming { format!("Array[{response}]") } else { response };
        out.push_str(&format!("      def {}: ({req}) -> {ret}\n", rpc_method_name(&method)));
    }
    out.push_str("    end\n  end\nend\n");
    out
}

fn render_sorbet() -> String {
    let (methods, _, _) = models();
    let mut out = String::from("# typed: true\n# Generated by acyclic-sdk-contract-wire; do not edit.\nmodule Acyclic\n  module GeneratedTyped\n");
    for semantic in semantic_inventory() { let name = camel(semantic.rust_name); out.push_str(&format!("    class Rust{name} < T::Struct\n      const :value, {}\n      sig {{ returns({}) }}\n      def to_wire; value; end\n    end\n", if semantic_value_type(semantic.id) == "Integer" { "Integer" } else { "String" }, if semantic_value_type(semantic.id) == "Integer" { "Integer" } else { "String" })); }
    for (message, oneof, members) in oneof_groups() {
        let base = oneof_class(&message, &oneof);
        out.push_str(&format!("    class {base} < T::Struct\n      const :tag, String\n      const :value, T.untyped\n      sig {{ params(tag: String, value: T.untyped).void }}\n      def initialize(tag:, value:); super; end\n    end\n"));
        for member in members { out.push_str(&format!("    class {base}{} < {base}\n      const :value, {}\n    end\n", oneof_variant(&member), match oneof_ruby_payload(&member).as_str() { "bool" => "T::Boolean", "Float" => "Float", "Integer" => "Integer", "String" => "String", _ => "T.untyped" })); }
        out.push_str(&format!("    class {base}Unknown < {base}\n      const :unknown_tag, Integer\n      const :payload, String\n    end\n"));
    }
    for (message, fields) in all_messages() {
        let name = message_class(&message);
        out.push_str(&format!("    class {name} < T::Struct\n      const :unknown, T::Hash[Symbol, T.untyped], default: {{}}\n"));
        for field in &fields { out.push_str(&format!("      const :{}, {}, default: nil\n", ruby_field(field), sorbet_type(field))); }
        out.push_str("      sig { returns(T::Hash[Symbol, T.untyped]) }\n      def to_wire; {} end\n    end\n");
    }
    out.push_str("    class Client\n");
    for method in methods {
        let req = message_class(&method.input_message);
        let ret = message_class(&method.output_message);
        out.push_str(&format!("      sig {{ params(request: {req}).returns({ret}) }}\n      def {}; end\n", rpc_method_name(&method)));
    }
    out.push_str("    end\n  end\nend\n");
    out
}

fn sorbet_type(field: &ResolvedRequestField) -> String {
    let semantic = semantic_or_wire(field);
    let base = match semantic.as_str() { "bool" => "T::Boolean".to_owned(), "float" => "Float".to_owned(), "int" => "Integer".to_owned(), "String" | "Bytes" => "String".to_owned(), value => format!("Acyclic::GeneratedTyped::{value}") };
    let base = if repeated(field) { format!("T::Array[{base}]") } else { base };
    if optional(field) { format!("T.nilable({base})") } else { base }
}

fn php_message(name: &str, fields: &[ResolvedRequestField]) -> String {
    let mut out = format!("final readonly class {name}\n{{\n    public function __construct(\n");
    for field in fields.iter().filter(|field| php_default(field).is_empty()) {
        out.push_str(&format!("        /** @var {} */\n        public {} ${}{},\n", php_doc_type(field), php_type(field), php_field(field), php_default(field)));
    }
    for field in fields.iter().filter(|field| !php_default(field).is_empty()) {
        out.push_str(&format!("        /** @var {} */\n        public {} ${}{},\n", php_doc_type(field), php_type(field), php_field(field), php_default(field)));
    }
    out.push_str("    ) {}\n    /** @return array<array-key, mixed> */\n    private static function wireMap(mixed $value): array\n    {\n        if (!is_array($value)) { throw new \\InvalidArgumentException('expected nested message'); }\n        return $value;\n    }\n\n    /** @return array<int, mixed> */\n    private static function wireArray(mixed $value): array\n    {\n        return is_array($value) ? array_values($value) : [];\n    }\n    private static function wireValue(mixed $value): mixed\n    {\n        if (is_object($value) && method_exists($value, 'toWire')) { return $value->toWire(); }\n        if (is_array($value)) { return array_map(static fn(mixed $item): mixed => self::wireValue($item), $value); }\n        return $value;\n    }\n    /** @return array<string, mixed> */\n    public function toWire(): array\n    {\n        return [\n");
    for field in fields { out.push_str(&format!("            '{0}' => self::wireValue($this->{0}),\n", php_field(field))); }
    out.push_str("        ];\n    }\n    /** @param array<array-key, mixed> $value */\n    public static function fromWire(array $value): self\n    {\n        return new self(\n");
    for field in fields { out.push_str(&format!("            {0}: {1},\n", php_field(field), php_decode_value(field))); }
    out.push_str("        );\n    }\n}\n");
    out
}

fn php_oneof_group(message: &str, oneof: &str, members: &[ResolvedOneofMember]) -> String {
    let base = oneof_class(message, oneof);
    let mut out = format!("abstract readonly class {base}\n{{\n    public function __construct(public readonly string $tag) {{}}\n}}\n");
    for member in members {
        let variant = format!("{base}{}", oneof_variant(member));
        let payload = oneof_php_payload(member);
        out.push_str(&format!("final readonly class {variant} extends {base}\n{{\n    public function __construct(public readonly {payload} $value) {{ parent::__construct({:?}); }}\n}}\n", member.field.field));
    }
    out.push_str(&format!("final readonly class {base}Unknown extends {base}\n{{\n    /** @param string $payload */\n    public function __construct(public readonly int $unknownTag, public readonly string $payload) {{ parent::__construct('unknown:' . (string) $unknownTag); }}\n}}\n"));
    out
}

fn render_php() -> String {
    let (methods, _, _) = models();
    let mut out = String::from("<?php\n\n// Generated by acyclic-sdk-contract-wire; do not edit.\n// Public types and RPC signatures originate in Rust type_policy.rs.\n\nnamespace Acyclic\\Generated;\n\n");
    for semantic in semantic_inventory() { let name = camel(semantic.rust_name); let scalar = match semantic_value_type(semantic.id) { "Integer" => "int", "Boolean" => "bool", _ => "string" }; out.push_str(&format!("final readonly class Rust{name} {{ public function __construct(public readonly {scalar} $value) {{ }} public function toWire(): {scalar} {{ return $this->value; }} }}\n")); }
    for (message, oneof, members) in oneof_groups() { out.push_str(&php_oneof_group(&message, &oneof, &members)); }
    for (message, fields) in all_messages() { out.push_str(&php_message(&message_class(&message), &fields)); }
    out.push_str("final class RustTypedClient\n{\n    public function __construct(private readonly \\Closure $call) {}\n");
    for method in methods {
        let req = message_class(&method.input_message);
        let resp = message_class(&method.output_message);
        let req = if method.client_streaming { "array".to_owned() } else { req };
        let ret = if method.server_streaming { "array".to_owned() } else { resp.clone() };
        let call_input = if method.client_streaming { "$request" } else { "$request->toWire()" };
        let response = if method.server_streaming {
            format!("if (!is_array($value)) {{ throw new \\UnexpectedValueException('expected stream array'); }} return array_values(array_map(static fn($item) => $item instanceof {resp} ? $item : (is_array($item) ? {resp}::fromWire($item) : throw new \\UnexpectedValueException('expected response object')), $value));")
        } else {
            format!("if ($value instanceof {resp}) {{ return $value; }} if (!is_array($value)) {{ throw new \\UnexpectedValueException('expected response object'); }} return {resp}::fromWire($value);")
        };
        if method.client_streaming { out.push_str(&format!("    /** @param array<int, {}> $request */\n", message_class(&method.input_message))); }
        if method.server_streaming { out.push_str(&format!("    /** @return array<int, {resp}> */\n")); }
        out.push_str(&format!("    public function {}({} $request): {}\n    {{\n        $value = ($this->call)({:?}, {});\n        {}\n    }}\n", rpc_method_name(&method), req, ret, method.rpc, call_input, response));
    }
    out.push_str("}\n");
    out
}

fn dart_message(name: &str, fields: &[ResolvedRequestField]) -> String {
    let mut out = format!("final class {name} {{\n");
    for field in fields { out.push_str(&format!("  final {} {};\n", dart_type(field), dart_field(field))); }
    if fields.is_empty() { out.push_str(&format!("  const {name}();\n}}\n")); return out; }
    out.push_str(&format!("  const {name}({{\n"));
    for field in fields { if repeated(field) { out.push_str(&format!("    this.{} = const [],\n", dart_field(field))); } else if optional(field) { out.push_str(&format!("    this.{},\n", dart_field(field))); } else { out.push_str(&format!("    required this.{},\n", dart_field(field))); } }
    out.push_str("  });\n  Map<String, Object?> toWire() => <String, Object?>{\n");
    for field in fields { out.push_str(&format!("    '{}': {},\n", field.json_name, dart_field(field))); }
    out.push_str("  };\n}\n");
    out
}

fn dart_oneof_group(message: &str, oneof: &str, members: &[ResolvedOneofMember]) -> String {
    let base = oneof_class(message, oneof);
    let mut out = format!("sealed class {base} {{ const {base}(); }}\n");
    for member in members {
        let variant = format!("{base}{}", oneof_variant(member));
        let payload = oneof_dart_payload(member);
        out.push_str(&format!("final class {variant} extends {base} {{ final {payload} value; const {variant}(this.value); }}\n"));
    }
    out.push_str(&format!("final class {base}Unknown extends {base} {{ final int tag; final List<int> payload; const {base}Unknown(this.tag, this.payload); }}\n"));
    out
}

fn render_dart() -> String {
    let (methods, _, _) = models();
    let mut out = String::from("// Generated by acyclic-sdk-contract-wire; do not edit.\n// Public types and RPC signatures originate in Rust type_policy.rs.\n\n");
    for semantic in semantic_inventory() { let name = camel(semantic.rust_name); let scalar = match semantic_value_type(semantic.id) { "Integer" => "int", "Boolean" => "bool", _ => "String" }; out.push_str(&format!("final class Rust{name} {{ final {scalar} value; const Rust{name}(this.value); }}\n")); }
    out.push_str("sealed class RustWireChoice { const RustWireChoice(); }\nfinal class RustKnownWireChoice extends RustWireChoice { final String tag; final List<int> payload; const RustKnownWireChoice(this.tag, this.payload); }\nfinal class RustUnknownWireChoice extends RustWireChoice { final int tag; final List<int> payload; const RustUnknownWireChoice(this.tag, this.payload); }\n");
    for (message, oneof, members) in oneof_groups() { out.push_str(&dart_oneof_group(&message, &oneof, &members)); }
    for (message, fields) in all_messages() { out.push_str(&dart_message(&message_class(&message), &fields)); }
    out.push_str("final class RustTypedClient {\n  final Future<Object?> Function(String, Object?) _call;\n  const RustTypedClient(this._call);\n");
    for method in methods {
        let req = message_class(&method.input_message);
        let resp = message_class(&method.output_message);
        let req = if method.client_streaming { format!("List<{req}>") } else { req };
        let ret = if method.server_streaming { format!("List<{resp}>") } else { resp };
        let wire = if method.client_streaming { "request" } else { "(request as dynamic).toWire()" };
        out.push_str(&format!("  Future<{ret}> {}({req} request) async {{ final value = await _call({:?}, {wire}); return value as {ret}; }}\n", rpc_method_name(&method), method.rpc));
    }
    out.push_str("}\n");
    out
}
