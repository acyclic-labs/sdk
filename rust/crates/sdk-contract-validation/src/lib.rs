//! Semantic compatibility checks for generated protobuf descriptor sets.
//!
//! The checker deliberately has two separate results:
//!
//! * exact bytes, which is the value used by protocol descriptor digests; and
//! * semantic compatibility, which ignores source locations and protobuf
//!   encoding details while retaining every wire-significant descriptor field.
//!
//! `prost-types` drops unknown extension fields when it decodes `*Options`.
//! The raw descriptor is therefore walked in parallel and the original option
//! messages are canonicalized as protobuf wire fields. This preserves custom
//! validation and HTTP route options without requiring this crate to know every
//! extension number at compile time.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::path::Path;

use prost::Message;
use prost_types::{
    DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto, FieldDescriptorProto,
    FileDescriptorProto, FileDescriptorSet, MethodDescriptorProto, OneofDescriptorProto,
    ServiceDescriptorProto,
};

/// A descriptor comparison result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ComparisonReport {
    /// Whether the input files were byte-for-byte identical.
    pub exact_bytes_equal: bool,
    /// Length of the baseline input in bytes.
    pub baseline_len: usize,
    /// Length of the candidate input in bytes.
    pub candidate_len: usize,
    /// BLAKE3 digest of the baseline input.
    pub baseline_digest: String,
    /// BLAKE3 digest of the candidate input.
    pub candidate_digest: String,
    /// Whether all wire-significant descriptor semantics are equal.
    pub semantic_compatible: bool,
    /// Differences found by the semantic comparison.
    pub differences: Vec<DescriptorDifference>,
}

/// A semantic descriptor difference.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescriptorDifference {
    /// Stable descriptor path, such as `field:example.Widget.id`.
    pub path: String,
    /// Broad class of the change.
    pub kind: DifferenceKind,
    /// Human-readable detail. This intentionally does not include raw option
    /// payloads, which may contain secrets in application-defined extensions.
    pub detail: String,
}

/// Classification used by compatibility reports.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DifferenceKind {
    /// A descriptor exists only in the candidate.
    Added,
    /// A descriptor exists only in the baseline.
    Removed,
    /// A field number changed.
    FieldTag,
    /// A field protobuf type or type reference changed.
    FieldType,
    /// A field label or explicit-presence setting changed.
    Presence,
    /// A field's containing oneof changed.
    Oneof,
    /// An enum value number changed.
    EnumValue,
    /// A service method's streaming mode changed.
    ServiceStreaming,
    /// A reserved range or reserved name changed.
    Reserved,
    /// A descriptor option, including an unknown custom extension, changed.
    CustomOption,
    /// Another wire-significant descriptor property changed.
    Other,
}

impl fmt::Display for DifferenceKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::Added => "added",
            Self::Removed => "removed",
            Self::FieldTag => "field-tag",
            Self::FieldType => "field-type",
            Self::Presence => "presence",
            Self::Oneof => "oneof",
            Self::EnumValue => "enum-value",
            Self::ServiceStreaming => "service-streaming",
            Self::Reserved => "reserved",
            Self::CustomOption => "custom-option",
            Self::Other => "other",
        };
        formatter.write_str(label)
    }
}

/// Compare two encoded [`FileDescriptorSet`] values.
pub fn compare_bytes(
    baseline: &[u8],
    candidate: &[u8],
) -> Result<ComparisonReport, ValidationError> {
    let baseline_model = ContractModel::decode(baseline)?;
    let candidate_model = ContractModel::decode(candidate)?;
    let differences = compare_models(&baseline_model, &candidate_model);

    Ok(ComparisonReport {
        exact_bytes_equal: baseline == candidate,
        baseline_len: baseline.len(),
        candidate_len: candidate.len(),
        baseline_digest: blake3::hash(baseline).to_hex().to_string(),
        candidate_digest: blake3::hash(candidate).to_hex().to_string(),
        semantic_compatible: differences.is_empty(),
        differences,
    })
}

/// Compare descriptor files on disk.
pub fn compare_files(
    baseline: impl AsRef<Path>,
    candidate: impl AsRef<Path>,
) -> Result<ComparisonReport, ValidationError> {
    let baseline_path = baseline.as_ref();
    let candidate_path = candidate.as_ref();
    let baseline_bytes = fs::read(baseline_path).map_err(|error| ValidationError::Read {
        path: baseline_path.display().to_string(),
        error: error.to_string(),
    })?;
    let candidate_bytes = fs::read(candidate_path).map_err(|error| ValidationError::Read {
        path: candidate_path.display().to_string(),
        error: error.to_string(),
    })?;
    compare_bytes(&baseline_bytes, &candidate_bytes)
}

/// Errors produced while decoding or reading descriptor sets.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ValidationError {
    /// An input file could not be read.
    Read { path: String, error: String },
    /// An input was not a valid protobuf descriptor set.
    Decode { error: String },
    /// A descriptor's raw wire message could not be parsed.
    RawWire { error: String },
    /// Two descriptors claim the same stable identity.
    Duplicate { path: String },
}

impl fmt::Display for ValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read { path, error } => write!(formatter, "failed to read {path}: {error}"),
            Self::Decode { error } => write!(formatter, "invalid descriptor set: {error}"),
            Self::RawWire { error } => write!(formatter, "invalid descriptor wire data: {error}"),
            Self::Duplicate { path } => write!(formatter, "duplicate descriptor identity: {path}"),
        }
    }
}

impl std::error::Error for ValidationError {}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SemanticItem {
    signature: String,
    options: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ContractModel {
    items: BTreeMap<String, SemanticItem>,
}

impl ContractModel {
    fn decode(bytes: &[u8]) -> Result<Self, ValidationError> {
        let set = FileDescriptorSet::decode(bytes).map_err(|error| ValidationError::Decode {
            error: error.to_string(),
        })?;
        if set.file.is_empty() {
            return Err(ValidationError::Decode {
                error: "descriptor set contains no files".to_owned(),
            });
        }
        if set
            .file
            .iter()
            .any(|file| file.name.as_deref().is_none_or(str::is_empty))
        {
            return Err(ValidationError::Decode {
                error: "file descriptor is missing a non-empty name".to_owned(),
            });
        }
        validate_unique_identities(&set)?;
        let raw_options = collect_raw_options(bytes)?;
        let mut items = BTreeMap::new();
        add_item(&mut items, "descriptor-set", String::new(), &raw_options);

        for file in &set.file {
            let file_name = file.name.as_deref().unwrap_or("<unnamed>");
            let file_path = format!("file:{file_name}");
            add_item(&mut items, &file_path, file_signature(file), &raw_options);

            let package = file.package.as_deref().unwrap_or_default();
            for message in &file.message_type {
                collect_message(&mut items, message, package, &raw_options);
            }
            for enumeration in &file.enum_type {
                collect_enum(&mut items, enumeration, package, &raw_options);
            }
            for service in &file.service {
                collect_service(&mut items, service, package, &raw_options);
            }
            for extension in &file.extension {
                collect_field(&mut items, extension, package, "extension", &raw_options);
            }
        }

        Ok(Self { items })
    }
}

fn validate_unique_identities(set: &FileDescriptorSet) -> Result<(), ValidationError> {
    let mut identities = BTreeSet::new();
    for file in &set.file {
        let file_name = file.name.as_deref().unwrap_or("<unnamed>");
        insert_identity(&mut identities, &format!("file:{file_name}"))?;
        let package = file.package.as_deref().unwrap_or_default();
        for message in &file.message_type {
            validate_message_identity(message, package, &mut identities)?;
        }
        for enumeration in &file.enum_type {
            validate_enum_identity(enumeration, package, &mut identities)?;
        }
        for service in &file.service {
            let name = service.name.as_deref().unwrap_or("<unnamed>");
            let full_name = qualify(package, name);
            insert_identity(&mut identities, &format!("service:{full_name}"))?;
            for method in &service.method {
                let method_name = method.name.as_deref().unwrap_or("<unnamed>");
                insert_identity(
                    &mut identities,
                    &format!("method:{full_name}.{method_name}"),
                )?;
            }
        }
        validate_field_numbers(&file.extension, package, "extension")?;
        for extension in &file.extension {
            validate_field_identity(extension, package, "extension", &mut identities)?;
        }
    }
    Ok(())
}

fn validate_message_identity(
    message: &DescriptorProto,
    parent: &str,
    identities: &mut BTreeSet<String>,
) -> Result<(), ValidationError> {
    let name = message.name.as_deref().unwrap_or("<unnamed>");
    let full_name = qualify(parent, name);
    insert_identity(identities, &format!("message:{full_name}"))?;
    validate_field_numbers(&message.field, &full_name, "field")?;
    for field in &message.field {
        validate_field_identity(field, &full_name, "field", identities)?;
    }
    validate_field_numbers(&message.extension, &full_name, "extension")?;
    for extension in &message.extension {
        validate_field_identity(extension, &full_name, "extension", identities)?;
    }
    for oneof in &message.oneof_decl {
        let oneof_name = oneof.name.as_deref().unwrap_or("<unnamed>");
        insert_identity(identities, &format!("oneof:{full_name}.{oneof_name}"))?;
    }
    for nested in &message.nested_type {
        validate_message_identity(nested, &full_name, identities)?;
    }
    for enumeration in &message.enum_type {
        validate_enum_identity(enumeration, &full_name, identities)?;
    }
    Ok(())
}

fn validate_field_numbers(
    fields: &[FieldDescriptorProto],
    parent: &str,
    kind: &str,
) -> Result<(), ValidationError> {
    let mut numbers = BTreeSet::new();
    for field in fields {
        if let Some(number) = field.number {
            if !numbers.insert(number) {
                return Err(ValidationError::Duplicate {
                    path: format!("{kind}:{parent}#number:{number}"),
                });
            }
        }
    }
    Ok(())
}

fn validate_field_identity(
    field: &FieldDescriptorProto,
    parent: &str,
    kind: &str,
    identities: &mut BTreeSet<String>,
) -> Result<(), ValidationError> {
    let name = field.name.as_deref().unwrap_or("<unnamed>");
    insert_identity(identities, &format!("{kind}:{parent}.{name}"))
}

fn validate_enum_identity(
    enumeration: &EnumDescriptorProto,
    parent: &str,
    identities: &mut BTreeSet<String>,
) -> Result<(), ValidationError> {
    let name = enumeration.name.as_deref().unwrap_or("<unnamed>");
    let full_name = qualify(parent, name);
    insert_identity(identities, &format!("enum:{full_name}"))?;
    for value in &enumeration.value {
        let value_name = value.name.as_deref().unwrap_or("<unnamed>");
        insert_identity(identities, &format!("enum:{full_name}.{value_name}"))?;
    }
    Ok(())
}

fn insert_identity(identities: &mut BTreeSet<String>, path: &str) -> Result<(), ValidationError> {
    if identities.insert(path.to_owned()) {
        Ok(())
    } else {
        Err(ValidationError::Duplicate {
            path: path.to_owned(),
        })
    }
}

fn add_item(
    items: &mut BTreeMap<String, SemanticItem>,
    path: &str,
    signature: String,
    raw_options: &BTreeMap<String, Vec<u8>>,
) {
    let options = raw_options.get(path).cloned().unwrap_or_default();
    items.insert(path.to_owned(), SemanticItem { signature, options });
}

fn collect_message(
    items: &mut BTreeMap<String, SemanticItem>,
    message: &DescriptorProto,
    parent: &str,
    raw_options: &BTreeMap<String, Vec<u8>>,
) {
    let name = message.name.as_deref().unwrap_or("<unnamed>");
    let full_name = qualify(parent, name);
    let path = format!("message:{full_name}");
    add_item(items, &path, message_signature(message), raw_options);

    let reserved_path = format!("{path}:reserved");
    add_item(
        items,
        &reserved_path,
        reserved_message_signature(message),
        raw_options,
    );

    for field in &message.field {
        collect_field(items, field, &full_name, "field", raw_options);
    }
    for extension in &message.extension {
        collect_field(items, extension, &full_name, "extension", raw_options);
    }
    for oneof in &message.oneof_decl {
        collect_oneof(items, oneof, &full_name, raw_options);
    }
    for nested in &message.nested_type {
        collect_message(items, nested, &full_name, raw_options);
    }
    for enumeration in &message.enum_type {
        collect_enum(items, enumeration, &full_name, raw_options);
    }
}

fn collect_field(
    items: &mut BTreeMap<String, SemanticItem>,
    field: &FieldDescriptorProto,
    parent: &str,
    kind: &str,
    raw_options: &BTreeMap<String, Vec<u8>>,
) {
    let name = field.name.as_deref().unwrap_or("<unnamed>");
    let path = format!("{kind}:{parent}.{name}");
    add_item(items, &path, field_signature(field), raw_options);
}

fn collect_oneof(
    items: &mut BTreeMap<String, SemanticItem>,
    oneof: &OneofDescriptorProto,
    parent: &str,
    raw_options: &BTreeMap<String, Vec<u8>>,
) {
    let name = oneof.name.as_deref().unwrap_or("<unnamed>");
    let path = format!("oneof:{parent}.{name}");
    add_item(items, &path, oneof_signature(oneof), raw_options);
}

fn collect_enum(
    items: &mut BTreeMap<String, SemanticItem>,
    enumeration: &EnumDescriptorProto,
    parent: &str,
    raw_options: &BTreeMap<String, Vec<u8>>,
) {
    let name = enumeration.name.as_deref().unwrap_or("<unnamed>");
    let full_name = qualify(parent, name);
    let path = format!("enum:{full_name}");
    add_item(items, &path, enum_signature(enumeration), raw_options);

    let reserved_path = format!("{path}:reserved");
    add_item(
        items,
        &reserved_path,
        reserved_enum_signature(enumeration),
        raw_options,
    );

    for value in &enumeration.value {
        let value_name = value.name.as_deref().unwrap_or("<unnamed>");
        let value_path = format!("enum:{full_name}.{value_name}");
        add_item(items, &value_path, enum_value_signature(value), raw_options);
    }
}

fn collect_service(
    items: &mut BTreeMap<String, SemanticItem>,
    service: &ServiceDescriptorProto,
    parent: &str,
    raw_options: &BTreeMap<String, Vec<u8>>,
) {
    let name = service.name.as_deref().unwrap_or("<unnamed>");
    let full_name = qualify(parent, name);
    let path = format!("service:{full_name}");
    add_item(items, &path, service_signature(service), raw_options);

    for method in &service.method {
        let method_name = method.name.as_deref().unwrap_or("<unnamed>");
        let method_path = format!("method:{full_name}.{method_name}");
        add_item(items, &method_path, method_signature(method), raw_options);
    }
}

fn qualify(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_owned()
    } else if name.is_empty() {
        parent.to_owned()
    } else {
        format!("{parent}.{name}")
    }
}

fn file_signature(file: &FileDescriptorProto) -> String {
    format!(
        "package={:?};syntax={:?};dependency={:?};public={:?};weak={:?};",
        file.package, file.syntax, file.dependency, file.public_dependency, file.weak_dependency,
    )
}

fn message_signature(message: &DescriptorProto) -> String {
    format!(
        "extension_range={:?};",
        message
            .extension_range
            .iter()
            .map(|range| (range.start, range.end))
            .collect::<Vec<_>>()
    )
}

fn reserved_message_signature(message: &DescriptorProto) -> String {
    format!(
        "range={:?};name={:?};",
        message
            .reserved_range
            .iter()
            .map(|range| (range.start, range.end))
            .collect::<Vec<_>>(),
        message.reserved_name
    )
}

fn field_signature(field: &FieldDescriptorProto) -> String {
    format!(
        "number={:?};label={:?};type={:?};type_name={:?};extendee={:?};default={:?};json_name={:?};proto3_optional={:?};oneof_index={:?};",
        field.number,
        field.label,
        field.r#type,
        field.type_name,
        field.extendee,
        field.default_value,
        field.json_name,
        field.proto3_optional,
        field.oneof_index,
    )
}

fn oneof_signature(oneof: &OneofDescriptorProto) -> String {
    format!("name={:?};", oneof.name)
}

fn enum_signature(enumeration: &EnumDescriptorProto) -> String {
    format!(
        "allow_alias={:?};",
        enumeration.options.as_ref().and_then(|o| o.allow_alias)
    )
}

fn reserved_enum_signature(enumeration: &EnumDescriptorProto) -> String {
    format!(
        "range={:?};name={:?};",
        enumeration
            .reserved_range
            .iter()
            .map(|range| (range.start, range.end))
            .collect::<Vec<_>>(),
        enumeration.reserved_name
    )
}

fn enum_value_signature(value: &EnumValueDescriptorProto) -> String {
    format!("number={:?};", value.number)
}

fn service_signature(service: &ServiceDescriptorProto) -> String {
    format!("name={:?};", service.name)
}

fn method_signature(method: &MethodDescriptorProto) -> String {
    format!(
        "input={:?};output={:?};client_streaming={:?};server_streaming={:?};",
        method.input_type, method.output_type, method.client_streaming, method.server_streaming
    )
}

fn compare_models(
    baseline: &ContractModel,
    candidate: &ContractModel,
) -> Vec<DescriptorDifference> {
    let mut differences = Vec::new();
    let keys = baseline
        .items
        .keys()
        .chain(candidate.items.keys())
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();

    for path in keys {
        match (baseline.items.get(&path), candidate.items.get(&path)) {
            (None, Some(_)) => differences.push(DescriptorDifference {
                kind: added_removed_kind(&path, DifferenceKind::Added),
                path,
                detail: "descriptor was added".to_owned(),
            }),
            (Some(_), None) => differences.push(DescriptorDifference {
                kind: added_removed_kind(&path, DifferenceKind::Removed),
                path,
                detail: "descriptor was removed".to_owned(),
            }),
            (Some(old), Some(new)) => {
                if old.signature != new.signature {
                    differences.push(DescriptorDifference {
                        kind: classify_signature_difference(&path, &old.signature, &new.signature),
                        detail: format!("structural descriptor changed ({})", path_kind(&path)),
                        path: path.clone(),
                    });
                }
                if old.options != new.options {
                    differences.push(DescriptorDifference {
                        path,
                        kind: DifferenceKind::CustomOption,
                        detail: "descriptor options or unknown wire fields changed, including extensions unknown to prost-types"
                            .to_owned(),
                    });
                }
            }
            (None, None) => {}
        }
    }
    differences
}

fn path_kind(path: &str) -> &str {
    path.split_once(':').map_or(path, |(kind, _)| kind)
}

fn added_removed_kind(path: &str, fallback: DifferenceKind) -> DifferenceKind {
    match path_kind(path) {
        "oneof" => DifferenceKind::Oneof,
        "enum" if !path.ends_with(":reserved") && path.contains('.') => DifferenceKind::EnumValue,
        "message" | "enum" if path.ends_with(":reserved") => DifferenceKind::Reserved,
        _ => fallback,
    }
}

fn classify_signature_difference(path: &str, old: &str, new: &str) -> DifferenceKind {
    match path_kind(path) {
        "field" | "extension" => {
            if property_changed(old, new, "number=") {
                DifferenceKind::FieldTag
            } else if property_changed(old, new, "proto3_optional=")
                || property_changed(old, new, "label=")
            {
                DifferenceKind::Presence
            } else if property_changed(old, new, "oneof_index=") {
                DifferenceKind::Oneof
            } else if property_changed(old, new, "type=")
                || property_changed(old, new, "type_name=")
            {
                DifferenceKind::FieldType
            } else {
                DifferenceKind::Other
            }
        }
        "enum" if !path.ends_with(":reserved") && path.contains('.') => DifferenceKind::EnumValue,
        "method"
            if property_changed(old, new, "client_streaming=")
                || property_changed(old, new, "server_streaming=") =>
        {
            DifferenceKind::ServiceStreaming
        }
        "message" | "enum" if path.ends_with(":reserved") => DifferenceKind::Reserved,
        _ => DifferenceKind::Other,
    }
}

fn property_changed(old: &str, new: &str, property: &str) -> bool {
    let old_value = old
        .split_once(property)
        .and_then(|(_, rest)| rest.split_once(';').map(|(v, _)| v));
    let new_value = new
        .split_once(property)
        .and_then(|(_, rest)| rest.split_once(';').map(|(v, _)| v));
    old_value != new_value
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct WireField {
    number: u32,
    wire_type: u8,
    value: Vec<u8>,
    encoded: Vec<u8>,
}

fn collect_raw_options(bytes: &[u8]) -> Result<BTreeMap<String, Vec<u8>>, ValidationError> {
    let set_fields = parse_wire_fields(bytes)?;
    let mut result = BTreeMap::new();
    insert_unknown_fields(&mut result, "descriptor-set", &set_fields, &[1]);
    let set = FileDescriptorSet::decode(bytes).map_err(|error| ValidationError::Decode {
        error: error.to_string(),
    })?;
    let raw_files = set_fields
        .iter()
        .filter(|field| field.number == 1)
        .collect::<Vec<_>>();
    for (file, raw_file) in set.file.iter().zip(raw_files) {
        collect_raw_file(raw_file.value.as_slice(), file, &mut result)?;
    }
    Ok(result)
}

fn collect_raw_file(
    raw: &[u8],
    file: &FileDescriptorProto,
    result: &mut BTreeMap<String, Vec<u8>>,
) -> Result<(), ValidationError> {
    let fields = parse_wire_fields(raw)?;
    let file_name = file.name.as_deref().unwrap_or("<unnamed>");
    let file_path = format!("file:{file_name}");
    insert_raw_option(result, &file_path, &fields, 8)?;
    insert_unknown_fields(
        result,
        &file_path,
        &fields,
        // Buf's ImageFileExtension metadata is not part of protobuf wire
        // semantics and may be present on one descriptor image only.
        &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 8042],
    );
    let package = file.package.as_deref().unwrap_or_default();

    collect_raw_messages(&fields, &file.message_type, package, 4, result)?;
    collect_raw_enums(&fields, &file.enum_type, package, 5, result)?;
    collect_raw_services(&fields, &file.service, package, result)?;
    collect_raw_fields(&fields, &file.extension, package, "extension", result)?;
    Ok(())
}

fn collect_raw_messages(
    fields: &[WireField],
    messages: &[DescriptorProto],
    parent: &str,
    field_number: u32,
    result: &mut BTreeMap<String, Vec<u8>>,
) -> Result<(), ValidationError> {
    let raw_messages = fields
        .iter()
        .filter(|field| field.number == field_number)
        .collect::<Vec<_>>();
    for (message, raw_message) in messages.iter().zip(raw_messages) {
        collect_raw_message(raw_message.value.as_slice(), message, parent, result)?;
    }
    Ok(())
}

fn collect_raw_message(
    raw: &[u8],
    message: &DescriptorProto,
    parent: &str,
    result: &mut BTreeMap<String, Vec<u8>>,
) -> Result<(), ValidationError> {
    let fields = parse_wire_fields(raw)?;
    let name = message.name.as_deref().unwrap_or("<unnamed>");
    let full_name = qualify(parent, name);
    let path = format!("message:{full_name}");
    insert_raw_option(result, &path, &fields, 7)?;
    insert_unknown_fields(
        &mut *result,
        &path,
        &fields,
        &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    );
    let raw_fields = fields
        .iter()
        .filter(|field| field.number == 2)
        .collect::<Vec<_>>();
    for (field, raw_field) in message.field.iter().zip(raw_fields) {
        let field_path = format!(
            "field:{full_name}.{}",
            field.name.as_deref().unwrap_or("<unnamed>")
        );
        let field_fields = parse_wire_fields(raw_field.value.as_slice())?;
        insert_raw_option(result, &field_path, &field_fields, 8)?;
        insert_unknown_fields(
            result,
            &field_path,
            &field_fields,
            &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 17],
        );
    }
    let raw_extensions = fields
        .iter()
        .filter(|field| field.number == 6)
        .collect::<Vec<_>>();
    for (extension, raw_extension) in message.extension.iter().zip(raw_extensions) {
        let extension_path = format!(
            "extension:{full_name}.{}",
            extension.name.as_deref().unwrap_or("<unnamed>")
        );
        let extension_fields = parse_wire_fields(raw_extension.value.as_slice())?;
        insert_raw_option(result, &extension_path, &extension_fields, 8)?;
        insert_unknown_fields(
            result,
            &extension_path,
            &extension_fields,
            &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 17],
        );
    }

    let raw_oneofs = fields
        .iter()
        .filter(|field| field.number == 8)
        .collect::<Vec<_>>();
    for (oneof, raw_oneof) in message.oneof_decl.iter().zip(raw_oneofs) {
        let oneof_path = format!(
            "oneof:{full_name}.{}",
            oneof.name.as_deref().unwrap_or("<unnamed>")
        );
        let oneof_fields = parse_wire_fields(raw_oneof.value.as_slice())?;
        insert_raw_option(result, &oneof_path, &oneof_fields, 2)?;
        insert_unknown_fields(result, &oneof_path, &oneof_fields, &[1, 2]);
    }

    collect_raw_messages(&fields, &message.nested_type, &full_name, 3, result)?;
    collect_raw_enums(&fields, &message.enum_type, &full_name, 4, result)?;
    Ok(())
}

fn collect_raw_enums(
    fields: &[WireField],
    enums: &[EnumDescriptorProto],
    parent: &str,
    field_number: u32,
    result: &mut BTreeMap<String, Vec<u8>>,
) -> Result<(), ValidationError> {
    let raw_enums = fields
        .iter()
        .filter(|field| field.number == field_number)
        .collect::<Vec<_>>();
    for (enumeration, raw_enum) in enums.iter().zip(raw_enums) {
        collect_raw_enum(raw_enum.value.as_slice(), enumeration, parent, result)?;
    }
    Ok(())
}

fn collect_raw_enum(
    raw: &[u8],
    enumeration: &EnumDescriptorProto,
    parent: &str,
    result: &mut BTreeMap<String, Vec<u8>>,
) -> Result<(), ValidationError> {
    let fields = parse_wire_fields(raw)?;
    let name = enumeration.name.as_deref().unwrap_or("<unnamed>");
    let full_name = qualify(parent, name);
    let path = format!("enum:{full_name}");
    insert_raw_option(result, &path, &fields, 3)?;
    insert_unknown_fields(result, &path, &fields, &[1, 2, 3, 4, 5]);
    let raw_values = fields
        .iter()
        .filter(|field| field.number == 2)
        .collect::<Vec<_>>();
    for (value, raw_value) in enumeration.value.iter().zip(raw_values) {
        let value_path = format!(
            "enum:{full_name}.{}",
            value.name.as_deref().unwrap_or("<unnamed>")
        );
        let value_fields = parse_wire_fields(raw_value.value.as_slice())?;
        insert_raw_option(result, &value_path, &value_fields, 3)?;
        insert_unknown_fields(result, &value_path, &value_fields, &[1, 2, 3]);
    }
    Ok(())
}

fn collect_raw_services(
    fields: &[WireField],
    services: &[ServiceDescriptorProto],
    parent: &str,
    result: &mut BTreeMap<String, Vec<u8>>,
) -> Result<(), ValidationError> {
    let raw_services = fields
        .iter()
        .filter(|field| field.number == 6)
        .collect::<Vec<_>>();
    for (service, raw_service) in services.iter().zip(raw_services) {
        let service_name = service.name.as_deref().unwrap_or("<unnamed>");
        let full_name = qualify(parent, service_name);
        let service_path = format!("service:{full_name}");
        let service_fields = parse_wire_fields(raw_service.value.as_slice())?;
        insert_raw_option(result, &service_path, &service_fields, 3)?;
        insert_unknown_fields(result, &service_path, &service_fields, &[1, 2, 3]);
        let raw_methods = service_fields
            .iter()
            .filter(|field| field.number == 2)
            .collect::<Vec<_>>();
        for (method, raw_method) in service.method.iter().zip(raw_methods) {
            let method_path = format!(
                "method:{full_name}.{}",
                method.name.as_deref().unwrap_or("<unnamed>")
            );
            let method_fields = parse_wire_fields(raw_method.value.as_slice())?;
            insert_raw_option(result, &method_path, &method_fields, 4)?;
            insert_unknown_fields(result, &method_path, &method_fields, &[1, 2, 3, 4, 5, 6]);
        }
    }
    Ok(())
}

fn collect_raw_fields(
    fields: &[WireField],
    descriptors: &[FieldDescriptorProto],
    parent: &str,
    kind: &str,
    result: &mut BTreeMap<String, Vec<u8>>,
) -> Result<(), ValidationError> {
    let field_number = if kind == "extension" { 7 } else { 2 };
    let raw_fields = fields
        .iter()
        .filter(|field| field.number == field_number)
        .collect::<Vec<_>>();
    for (descriptor, raw_field) in descriptors.iter().zip(raw_fields) {
        let path = format!(
            "{kind}:{parent}.{}",
            descriptor.name.as_deref().unwrap_or("<unnamed>")
        );
        let field_fields = parse_wire_fields(raw_field.value.as_slice())?;
        insert_raw_option(result, &path, &field_fields, 8)?;
        insert_unknown_fields(
            result,
            &path,
            &field_fields,
            &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 17],
        );
    }
    Ok(())
}

fn insert_raw_option(
    result: &mut BTreeMap<String, Vec<u8>>,
    path: &str,
    fields: &[WireField],
    option_field_number: u32,
) -> Result<(), ValidationError> {
    let options = fields
        .iter()
        .filter(|field| field.number == option_field_number)
        .collect::<Vec<_>>();
    if !options.is_empty() {
        let mut option_bytes = Vec::new();
        for option in options {
            if option.wire_type != 2 {
                return Err(ValidationError::RawWire {
                    error: format!(
                        "options field {option_field_number} at {path} is not length-delimited"
                    ),
                });
            }
            option_bytes.extend_from_slice(&option.value);
        }
        result.insert(path.to_owned(), canonical_wire_message(&option_bytes)?);
    }
    Ok(())
}

fn insert_unknown_fields(
    result: &mut BTreeMap<String, Vec<u8>>,
    path: &str,
    fields: &[WireField],
    known_fields: &[u32],
) {
    let unknown = fields
        .iter()
        .filter(|field| !known_fields.contains(&field.number))
        .map(|field| field.encoded.clone())
        .collect::<Vec<_>>();
    if unknown.is_empty() {
        return;
    }
    let options = result.entry(path.to_owned()).or_default();
    // Keep unknown descriptor fields in the same opaque comparison payload as
    // options, in their source order. Unknown fields and extension bytes are
    // part of the owner image; sorting them would hide a reordered payload.
    options.extend([0, 0xff]);
    options.extend(unknown.into_iter().flatten());
}

fn canonical_wire_message(bytes: &[u8]) -> Result<Vec<u8>, ValidationError> {
    // Descriptor option fields are authored in a deterministic order by the
    // Rust owner and by protoc. Preserve that order here: reordering even two
    // distinct extension fields must be visible to compatibility checks. A
    // field-number sort would silently turn a changed option sequence into a
    // compatible descriptor.
    Ok(parse_wire_fields(bytes)?
        .into_iter()
        .flat_map(|field| field.encoded)
        .collect::<Vec<_>>())
}

fn parse_wire_fields(bytes: &[u8]) -> Result<Vec<WireField>, ValidationError> {
    let mut cursor = 0;
    let mut fields = Vec::new();
    while cursor < bytes.len() {
        let start = cursor;
        let key = read_varint(bytes, &mut cursor)?;
        let number = u32::try_from(key >> 3).map_err(|_| ValidationError::RawWire {
            error: "protobuf field number overflow".to_owned(),
        })?;
        let wire_type = u8::try_from(key & 0x07).map_err(|_| ValidationError::RawWire {
            error: "protobuf wire type overflow".to_owned(),
        })?;
        let value = match wire_type {
            0 => {
                let value_start = cursor;
                let _ = read_varint(bytes, &mut cursor)?;
                bytes[value_start..cursor].to_vec()
            }
            1 => take(bytes, &mut cursor, 8)?,
            2 => {
                let length = usize::try_from(read_varint(bytes, &mut cursor)?).map_err(|_| {
                    ValidationError::RawWire {
                        error: "protobuf length overflow".to_owned(),
                    }
                })?;
                take(bytes, &mut cursor, length)?
            }
            5 => take(bytes, &mut cursor, 4)?,
            3 | 4 => {
                return Err(ValidationError::RawWire {
                    error: "group wire types are unsupported in descriptors".to_owned(),
                });
            }
            _ => {
                return Err(ValidationError::RawWire {
                    error: format!("unsupported protobuf wire type {wire_type}"),
                });
            }
        };
        fields.push(WireField {
            number,
            wire_type,
            value,
            encoded: bytes[start..cursor].to_vec(),
        });
    }
    Ok(fields)
}

fn read_varint(bytes: &[u8], cursor: &mut usize) -> Result<u64, ValidationError> {
    let mut result = 0u64;
    for shift in (0..70).step_by(7) {
        let byte = *bytes.get(*cursor).ok_or_else(|| ValidationError::RawWire {
            error: "truncated protobuf varint".to_owned(),
        })?;
        *cursor += 1;
        result |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
    }
    Err(ValidationError::RawWire {
        error: "protobuf varint overflow".to_owned(),
    })
}

fn take(bytes: &[u8], cursor: &mut usize, length: usize) -> Result<Vec<u8>, ValidationError> {
    let end = cursor
        .checked_add(length)
        .ok_or_else(|| ValidationError::RawWire {
            error: "protobuf length overflow".to_owned(),
        })?;
    let value = bytes
        .get(*cursor..end)
        .ok_or_else(|| ValidationError::RawWire {
            error: "truncated protobuf field".to_owned(),
        })?;
    *cursor = end;
    Ok(value.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost_types::{
        DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto, FieldDescriptorProto,
        FieldOptions, FileDescriptorProto, FileDescriptorSet, MethodDescriptorProto,
        OneofDescriptorProto, ServiceDescriptorProto, SourceCodeInfo, descriptor_proto,
        field_descriptor_proto,
    };

    fn descriptor(field: FieldDescriptorProto) -> Vec<u8> {
        FileDescriptorSet {
            file: vec![FileDescriptorProto {
                name: Some("example.proto".into()),
                package: Some("example".into()),
                message_type: vec![DescriptorProto {
                    name: Some("Widget".into()),
                    field: vec![field],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
        .encode_to_vec()
    }

    fn field(number: i32, field_type: field_descriptor_proto::Type) -> FieldDescriptorProto {
        FieldDescriptorProto {
            name: Some("id".into()),
            number: Some(number),
            label: Some(field_descriptor_proto::Label::Optional as i32),
            r#type: Some(field_type as i32),
            json_name: Some("id".into()),
            ..Default::default()
        }
    }

    fn assert_kind(report: &ComparisonReport, kind: DifferenceKind) {
        assert!(!report.semantic_compatible, "{report:?}");
        assert!(
            report
                .differences
                .iter()
                .any(|difference| difference.kind == kind),
            "{report:?}"
        );
    }

    #[test]
    fn source_locations_change_exact_bytes_but_not_semantics() {
        let baseline = descriptor(field(1, field_descriptor_proto::Type::String));
        let mut changed_set = FileDescriptorSet::decode(baseline.as_slice()).expect("fixture");
        changed_set.file[0].source_code_info = Some(SourceCodeInfo {
            location: Vec::new(),
        });
        let candidate = changed_set.encode_to_vec();
        let report = compare_bytes(&baseline, &candidate).expect("compare");
        assert!(!report.exact_bytes_equal);
        assert!(report.semantic_compatible, "{report:?}");
    }

    #[test]
    fn changed_field_tag_is_incompatible() {
        let report = compare_bytes(
            &descriptor(field(1, field_descriptor_proto::Type::String)),
            &descriptor(field(2, field_descriptor_proto::Type::String)),
        )
        .expect("compare");
        assert_kind(&report, DifferenceKind::FieldTag);
    }

    #[test]
    fn changed_field_type_is_incompatible() {
        let report = compare_bytes(
            &descriptor(field(1, field_descriptor_proto::Type::String)),
            &descriptor(field(1, field_descriptor_proto::Type::Int32)),
        )
        .expect("compare");
        assert_kind(&report, DifferenceKind::FieldType);
    }

    #[test]
    fn empty_descriptor_sets_are_rejected() {
        let error = compare_bytes(&[], &[]).expect_err("empty descriptor set");
        assert!(matches!(error, ValidationError::Decode { .. }));
    }

    #[test]
    fn duplicate_descriptor_identities_are_rejected() {
        let bytes = FileDescriptorSet {
            file: vec![
                FileDescriptorProto {
                    name: Some("example.proto".into()),
                    ..Default::default()
                },
                FileDescriptorProto {
                    name: Some("example.proto".into()),
                    ..Default::default()
                },
            ],
        }
        .encode_to_vec();
        let error = compare_bytes(&bytes, &bytes).expect_err("duplicate descriptor identity");
        assert!(matches!(error, ValidationError::Duplicate { .. }));
    }

    #[test]
    fn unnamed_files_are_rejected() {
        let bytes = FileDescriptorSet {
            file: vec![FileDescriptorProto::default()],
        }
        .encode_to_vec();
        let error = compare_bytes(&bytes, &bytes).expect_err("unnamed file");
        assert!(matches!(error, ValidationError::Decode { .. }));
    }

    #[test]
    fn duplicate_field_numbers_are_rejected() {
        let bytes = FileDescriptorSet {
            file: vec![FileDescriptorProto {
                name: Some("example.proto".into()),
                message_type: vec![DescriptorProto {
                    name: Some("Widget".into()),
                    field: vec![
                        field(1, field_descriptor_proto::Type::String),
                        FieldDescriptorProto {
                            name: Some("other".into()),
                            number: Some(1),
                            label: Some(field_descriptor_proto::Label::Optional as i32),
                            r#type: Some(field_descriptor_proto::Type::String as i32),
                            json_name: Some("other".into()),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
        .encode_to_vec();
        let error = compare_bytes(&bytes, &bytes).expect_err("duplicate field number");
        assert!(matches!(error, ValidationError::Duplicate { .. }));
    }

    #[test]
    fn changed_presence_and_oneof_are_incompatible() {
        let baseline = descriptor(field(1, field_descriptor_proto::Type::String));
        let mut candidate_set = FileDescriptorSet::decode(baseline.as_slice()).expect("fixture");
        let message = &mut candidate_set.file[0].message_type[0];
        message.oneof_decl.push(OneofDescriptorProto {
            name: Some("_id".into()),
            ..Default::default()
        });
        message.field[0].proto3_optional = Some(true);
        message.field[0].oneof_index = Some(0);
        let report = compare_bytes(&baseline, &candidate_set.encode_to_vec()).expect("compare");
        assert_kind(&report, DifferenceKind::Presence);
        assert_kind(&report, DifferenceKind::Oneof);
    }

    #[test]
    fn changed_enum_value_is_incompatible() {
        let make = |number| {
            FileDescriptorSet {
                file: vec![FileDescriptorProto {
                    name: Some("example.proto".into()),
                    enum_type: vec![EnumDescriptorProto {
                        name: Some("State".into()),
                        value: vec![EnumValueDescriptorProto {
                            name: Some("Ready".into()),
                            number: Some(number),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
            }
            .encode_to_vec()
        };
        let report = compare_bytes(&make(1), &make(2)).expect("compare");
        assert_kind(&report, DifferenceKind::EnumValue);
    }

    #[test]
    fn changed_service_streaming_is_incompatible() {
        let make = |server_streaming| {
            FileDescriptorSet {
                file: vec![FileDescriptorProto {
                    name: Some("example.proto".into()),
                    service: vec![ServiceDescriptorProto {
                        name: Some("Widgets".into()),
                        method: vec![MethodDescriptorProto {
                            name: Some("Watch".into()),
                            input_type: Some(".example.Request".into()),
                            output_type: Some(".example.Event".into()),
                            server_streaming: Some(server_streaming),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
            }
            .encode_to_vec()
        };
        let report = compare_bytes(&make(false), &make(true)).expect("compare");
        assert_kind(&report, DifferenceKind::ServiceStreaming);
    }

    #[test]
    fn changed_reserved_ranges_are_incompatible() {
        let make = |end| {
            FileDescriptorSet {
                file: vec![FileDescriptorProto {
                    name: Some("example.proto".into()),
                    message_type: vec![DescriptorProto {
                        name: Some("Widget".into()),
                        reserved_range: vec![descriptor_proto::ReservedRange {
                            start: Some(10),
                            end: Some(end),
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
            }
            .encode_to_vec()
        };
        let report = compare_bytes(&make(20), &make(21)).expect("compare");
        assert_kind(&report, DifferenceKind::Reserved);
    }

    #[test]
    fn changed_map_entry_option_is_incompatible() {
        let make = |map_entry| {
            FileDescriptorSet {
                file: vec![FileDescriptorProto {
                    name: Some("example.proto".into()),
                    package: Some("example".into()),
                    message_type: vec![DescriptorProto {
                        name: Some("LabelsEntry".into()),
                        options: Some(prost_types::MessageOptions {
                            map_entry: Some(map_entry),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
            }
            .encode_to_vec()
        };
        let report = compare_bytes(&make(false), &make(true)).expect("compare");
        assert_kind(&report, DifferenceKind::CustomOption);
    }

    #[test]
    fn changed_packed_option_is_incompatible() {
        let make = |packed| {
            descriptor_with_field(FieldDescriptorProto {
                name: Some("values".into()),
                number: Some(1),
                label: Some(field_descriptor_proto::Label::Repeated as i32),
                r#type: Some(field_descriptor_proto::Type::Int32 as i32),
                options: Some(FieldOptions {
                    packed: Some(packed),
                    ..Default::default()
                }),
                json_name: Some("values".into()),
                ..Default::default()
            })
        };
        let report = compare_bytes(&make(false), &make(true)).expect("compare");
        assert_kind(&report, DifferenceKind::CustomOption);
    }

    #[test]
    fn changed_default_and_json_names_are_incompatible() {
        let make = |default_value: &str, json_name: &str| {
            descriptor_with_field(FieldDescriptorProto {
                name: Some("mode".into()),
                number: Some(1),
                label: Some(field_descriptor_proto::Label::Optional as i32),
                r#type: Some(field_descriptor_proto::Type::String as i32),
                default_value: Some(default_value.into()),
                json_name: Some(json_name.into()),
                ..Default::default()
            })
        };
        let default_report =
            compare_bytes(&make("ready", "mode"), &make("stopped", "mode")).expect("compare");
        assert_kind(&default_report, DifferenceKind::Other);
        let json_report =
            compare_bytes(&make("ready", "mode"), &make("ready", "wireMode")).expect("compare");
        assert_kind(&json_report, DifferenceKind::Other);
    }

    #[test]
    fn dependency_order_is_incompatible() {
        let make = |dependency: [&str; 2]| {
            FileDescriptorSet {
                file: vec![FileDescriptorProto {
                    name: Some("example.proto".into()),
                    dependency: dependency.into_iter().map(str::to_owned).collect(),
                    ..Default::default()
                }],
            }
            .encode_to_vec()
        };
        let report = compare_bytes(&make(["a.proto", "b.proto"]), &make(["b.proto", "a.proto"]))
            .expect("compare");
        assert_kind(&report, DifferenceKind::Other);
    }

    #[test]
    fn malformed_options_wire_is_rejected_before_semantic_comparison() {
        let baseline = descriptor(field(1, field_descriptor_proto::Type::String));
        let set_fields = parse_wire_fields(&baseline).expect("set fields");
        let file_field = set_fields
            .iter()
            .find(|field| field.number == 1)
            .expect("file");

        // FileDescriptorProto.options is field 8 and must be a length-delimited
        // message. A wrong wire type must never be treated as an absent option.
        let mut malformed_file = file_field.value.clone();
        malformed_file.extend(encode_varint(u64::from(8u32 << 3)));
        malformed_file.extend(encode_varint(1));
        let malformed = length_delimited_field(1, &malformed_file);
        let error = compare_bytes(&malformed, &malformed)
            .expect_err("malformed options wire should be rejected");
        assert!(
            matches!(
                error,
                ValidationError::Decode { .. } | ValidationError::RawWire { .. }
            ),
            "unexpected malformed options error: {error:?}"
        );
    }

    #[test]
    fn unknown_descriptor_field_is_incompatible() {
        let baseline = descriptor(field(1, field_descriptor_proto::Type::String));
        let set_fields = parse_wire_fields(&baseline).expect("set fields");
        let file_field = set_fields
            .iter()
            .find(|field| field.number == 1)
            .expect("file");
        let file_fields = parse_wire_fields(&file_field.value).expect("file fields");
        let message_field = file_fields
            .iter()
            .find(|field| field.number == 4)
            .expect("message");
        let mut message_bytes = message_field.value.clone();
        message_bytes.extend(length_delimited_field(1000, b"future-descriptor-field"));
        let mut candidate_file = Vec::new();
        for field in file_fields {
            if field.number == 4 {
                candidate_file.extend(length_delimited_field(4, &message_bytes));
            } else {
                candidate_file.extend(field.encoded);
            }
        }
        let candidate = length_delimited_field(1, &candidate_file);
        let report = compare_bytes(&baseline, &candidate).expect("compare");
        assert_kind(&report, DifferenceKind::CustomOption);
    }

    #[test]
    fn unknown_descriptor_set_field_is_incompatible() {
        let baseline = descriptor(field(1, field_descriptor_proto::Type::String));
        let mut candidate = baseline.clone();
        candidate.extend(length_delimited_field(1000, b"future-set-field"));
        let report = compare_bytes(&baseline, &candidate).expect("compare");
        assert_kind(&report, DifferenceKind::CustomOption);
    }

    #[test]
    fn unknown_descriptor_field_reordering_is_incompatible() {
        let baseline = descriptor(field(1, field_descriptor_proto::Type::String));
        let set_fields = parse_wire_fields(&baseline).expect("set fields");
        let file_field = set_fields
            .iter()
            .find(|field| field.number == 1)
            .expect("file");
        let mut file_bytes = file_field.value.clone();
        file_bytes.extend(length_delimited_field(1000, b"first"));
        file_bytes.extend(length_delimited_field(1001, b"second"));
        let first = length_delimited_field(1, &file_bytes);

        let mut reordered_file_bytes = file_field.value.clone();
        reordered_file_bytes.extend(length_delimited_field(1001, b"second"));
        reordered_file_bytes.extend(length_delimited_field(1000, b"first"));
        let second = length_delimited_field(1, &reordered_file_bytes);

        let report = compare_bytes(&first, &second).expect("compare");
        assert_kind(&report, DifferenceKind::CustomOption);
    }

    #[test]
    fn repeated_custom_option_reordering_is_semantically_incompatible() {
        let baseline = descriptor_with_repeated_custom_file_options(&[b"first", b"second"]);
        let candidate = descriptor_with_repeated_custom_file_options(&[b"second", b"first"]);
        let report = compare_bytes(&baseline, &candidate).expect("compare");
        assert_kind(&report, DifferenceKind::CustomOption);
    }

    #[test]
    fn distinct_custom_option_reordering_is_semantically_incompatible() {
        let baseline = descriptor_with_ordered_custom_file_options(&[
            (51_012, b"/v1/widgets".as_slice()),
            (51_011, b"partial".as_slice()),
        ]);
        let candidate = descriptor_with_ordered_custom_file_options(&[
            (51_011, b"partial".as_slice()),
            (51_012, b"/v1/widgets".as_slice()),
        ]);
        let report = compare_bytes(&baseline, &candidate).expect("compare");
        assert_kind(&report, DifferenceKind::CustomOption);
    }

    #[test]
    fn unknown_custom_option_is_preserved_from_raw_wire() {
        let baseline = descriptor_with_custom_file_option(b"/v1/widgets");
        let candidate = descriptor_with_custom_file_option(b"/v1/things");
        let report = compare_bytes(&baseline, &candidate).expect("compare");
        assert_kind(&report, DifferenceKind::CustomOption);
    }

    #[test]
    fn repeated_options_envelopes_are_not_ignored() {
        let mut baseline_file = FileDescriptorProto {
            name: Some("example.proto".into()),
            ..Default::default()
        }
        .encode_to_vec();
        baseline_file.extend(length_delimited_field(
            8,
            &length_delimited_field(51_012, b"first"),
        ));

        let mut candidate_file = baseline_file.clone();
        candidate_file.extend(length_delimited_field(
            8,
            &length_delimited_field(51_012, b"second"),
        ));
        let baseline = length_delimited_field(1, &baseline_file);
        let candidate = length_delimited_field(1, &candidate_file);
        let report = compare_bytes(&baseline, &candidate).expect("compare");
        assert_kind(&report, DifferenceKind::CustomOption);
    }

    fn descriptor_with_field(field: FieldDescriptorProto) -> Vec<u8> {
        descriptor(field)
    }

    fn descriptor_with_custom_file_option(value: &[u8]) -> Vec<u8> {
        let option = length_delimited_field(51_012, value);
        let file = FileDescriptorProto {
            name: Some("example.proto".into()),
            ..Default::default()
        };
        let mut file_bytes = file.encode_to_vec();
        file_bytes.extend(length_delimited_field(8, &option));
        let mut set_bytes = Vec::new();
        set_bytes.extend(length_delimited_field(1, &file_bytes));
        set_bytes
    }

    fn descriptor_with_repeated_custom_file_options(values: &[&[u8]]) -> Vec<u8> {
        let option = values
            .iter()
            .flat_map(|value| length_delimited_field(51_012, value))
            .collect::<Vec<_>>();
        let file = FileDescriptorProto {
            name: Some("example.proto".into()),
            ..Default::default()
        };
        let mut file_bytes = file.encode_to_vec();
        file_bytes.extend(length_delimited_field(8, &option));
        length_delimited_field(1, &file_bytes)
    }

    fn descriptor_with_ordered_custom_file_options(values: &[(u32, &[u8])]) -> Vec<u8> {
        let option = values
            .iter()
            .flat_map(|(number, value)| length_delimited_field(*number, value))
            .collect::<Vec<_>>();
        let file = FileDescriptorProto {
            name: Some("example.proto".into()),
            ..Default::default()
        };
        let mut file_bytes = file.encode_to_vec();
        file_bytes.extend(length_delimited_field(8, &option));
        length_delimited_field(1, &file_bytes)
    }

    fn length_delimited_field(number: u32, value: &[u8]) -> Vec<u8> {
        let mut encoded = encode_varint(u64::from(number) << 3 | 2);
        encoded.extend(encode_varint(value.len() as u64));
        encoded.extend(value);
        encoded
    }

    fn encode_varint(mut value: u64) -> Vec<u8> {
        let mut bytes = Vec::new();
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            bytes.push(byte);
            if value == 0 {
                return bytes;
            }
        }
    }
}
