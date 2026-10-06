//! Rust-owned custom protobuf options.
//!
//! `prost-types` does not know application-defined descriptor extensions and
//! drops them on an options decode/encode round trip. This crate keeps the
//! extension identities and typed values in Rust, emits their exact raw wire
//! fields, and emits a compilable `.proto` projection for a protoc check.

use prost::Message;
use prost_types::{
    DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto, FieldDescriptorProto,
    FileDescriptorProto, FileDescriptorSet, MethodDescriptorProto, OneofDescriptorProto,
    ServiceDescriptorProto, field_descriptor_proto,
};
use std::fmt;

/// The descriptor options message receiving an extension.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OptionTarget {
    Field,
    EnumValue,
    Oneof,
    Method,
}

impl OptionTarget {
    /// Return targets in the immutable options.proto declaration order.
    pub const fn all() -> [Self; 4] {
        [Self::Field, Self::EnumValue, Self::Oneof, Self::Method]
    }
}

/// Stable custom option identity owned by Rust.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OptionSpec {
    pub target: OptionTarget,
    pub name: &'static str,
    pub number: u32,
    pub scalar_type: &'static str,
}

/// Look up an option identity by its stable field number.
pub fn option_spec(number: u32) -> Option<OptionSpec> {
    OPTION_SPECS
        .iter()
        .find(|item| item.number == number)
        .copied()
}

/// Look up an option identity by its descriptor target and source name.
pub fn option_spec_by_name(target: OptionTarget, name: &str) -> Option<OptionSpec> {
    OPTION_SPECS
        .iter()
        .find(|item| item.target == target && item.name == name)
        .copied()
}

/// All active custom options, in stable proto source order.
pub const OPTION_SPECS: &[OptionSpec] = &[
    OptionSpec {
        target: OptionTarget::Field,
        name: "nonzero_fixed_bytes",
        number: 51001,
        scalar_type: "uint32",
    },
    OptionSpec {
        target: OptionTarget::Field,
        name: "required_message",
        number: 51002,
        scalar_type: "bool",
    },
    OptionSpec {
        target: OptionTarget::Field,
        name: "positive_uint64",
        number: 51003,
        scalar_type: "bool",
    },
    OptionSpec {
        target: OptionTarget::Oneof,
        name: "required_oneof",
        number: 51004,
        scalar_type: "bool",
    },
    OptionSpec {
        target: OptionTarget::Field,
        name: "min_items",
        number: 51005,
        scalar_type: "uint32",
    },
    OptionSpec {
        target: OptionTarget::Field,
        name: "max_items",
        number: 51006,
        scalar_type: "uint32",
    },
    OptionSpec {
        target: OptionTarget::Field,
        name: "nonempty_max_bytes",
        number: 51007,
        scalar_type: "uint32",
    },
    OptionSpec {
        target: OptionTarget::Field,
        name: "max_uint64",
        number: 51008,
        scalar_type: "uint64",
    },
    OptionSpec {
        target: OptionTarget::Field,
        name: "known_nonzero_enum",
        number: 51009,
        scalar_type: "bool",
    },
    OptionSpec {
        target: OptionTarget::Field,
        name: "nonempty_max_item_bytes",
        number: 51010,
        scalar_type: "uint32",
    },
    OptionSpec {
        target: OptionTarget::EnumValue,
        name: "partial_terminal",
        number: 51011,
        scalar_type: "bool",
    },
    OptionSpec {
        target: OptionTarget::Method,
        name: "http_path",
        number: 51012,
        scalar_type: "string",
    },
];

/// Typed `FieldOptions` values.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FieldOptionValues {
    pub nonzero_fixed_bytes: Option<u32>,
    pub required_message: Option<bool>,
    pub positive_uint64: Option<bool>,
    pub min_items: Option<u32>,
    pub max_items: Option<u32>,
    pub nonempty_max_bytes: Option<u32>,
    pub max_uint64: Option<u64>,
    pub known_nonzero_enum: Option<bool>,
    pub nonempty_max_item_bytes: Option<u32>,
}

/// A typed value carried by a raw custom option field.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RawOptionValue {
    Bool(bool),
    U32(u32),
    U64(u64),
    String(String),
}

/// One extension field with its explicit Rust-owned identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawOption {
    pub spec: OptionSpec,
    pub value: RawOptionValue,
}

/// Generic options container for descriptor exporters.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RawOptions {
    fields: Vec<RawOption>,
}

/// A malformed or ambiguous typed option assignment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RawOptionError {
    /// The supplied identity is not one of the pinned extension definitions.
    UnknownSpec { number: u32 },
    /// The same singular extension was assigned more than once.
    Duplicate { number: u32 },
    /// The value variant does not match the extension's scalar type.
    TypeMismatch {
        name: &'static str,
        scalar_type: &'static str,
    },
}

impl fmt::Display for RawOptionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownSpec { number } => write!(formatter, "unknown custom option {number}"),
            Self::Duplicate { number } => write!(formatter, "duplicate custom option {number}"),
            Self::TypeMismatch { name, scalar_type } => {
                write!(formatter, "option {name} requires {scalar_type}")
            }
        }
    }
}

impl std::error::Error for RawOptionError {}

impl RawOptions {
    /// Create an empty options message.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add one explicitly identified extension value.
    pub fn push(&mut self, spec: OptionSpec, value: RawOptionValue) {
        self.fields.push(RawOption { spec, value });
    }

    /// Add an option after checking its pinned identity, type, and uniqueness.
    pub fn try_push(
        &mut self,
        spec: OptionSpec,
        value: RawOptionValue,
    ) -> Result<(), RawOptionError> {
        let Some(known) = option_spec(spec.number) else {
            return Err(RawOptionError::UnknownSpec {
                number: spec.number,
            });
        };
        if known != spec {
            return Err(RawOptionError::UnknownSpec {
                number: spec.number,
            });
        }
        if self
            .fields
            .iter()
            .any(|field| field.spec.number == spec.number)
        {
            return Err(RawOptionError::Duplicate {
                number: spec.number,
            });
        }
        if !value_matches(spec.scalar_type, &value) {
            return Err(RawOptionError::TypeMismatch {
                name: spec.name,
                scalar_type: spec.scalar_type,
            });
        }
        self.push(spec, value);
        Ok(())
    }

    /// Validate all assignments, including values added through [`Self::push`].
    pub fn validate(&self) -> Result<(), RawOptionError> {
        let mut checked = RawOptions::new();
        for field in &self.fields {
            checked.try_push(field.spec, field.value.clone())?;
        }
        Ok(())
    }

    /// Expose typed fields to descriptor/proto exporters.
    pub fn fields(&self) -> &[RawOption] {
        &self.fields
    }

    /// Encode the fields as a protobuf options message, retaining source order.
    pub fn encode(&self) -> Vec<u8> {
        // Typed owner models use `try_push`; this check also protects callers
        // that construct a RawOptions value through the low-level push API.
        self.try_encode()
            .expect("invalid Rust-owned custom option assignment")
    }

    /// Encode the fields while reporting malformed owner assignments.
    pub fn try_encode(&self) -> Result<Vec<u8>, RawOptionError> {
        self.validate()?;
        let mut out = Vec::new();
        for option in &self.fields {
            match &option.value {
                RawOptionValue::Bool(value) => {
                    bool_option(&mut out, option.spec.number, Some(*value))
                }
                RawOptionValue::U32(value) => {
                    varint_option(&mut out, option.spec.number, Some(*value))
                }
                RawOptionValue::U64(value) => {
                    varint_option(&mut out, option.spec.number, Some(*value))
                }
                RawOptionValue::String(value) => {
                    out.extend(length_delimited(option.spec.number, value.as_bytes()))
                }
            }
        }
        Ok(out)
    }
}

fn spec(number: u32) -> OptionSpec {
    option_spec(number).expect("known custom option")
}

fn value_matches(scalar_type: &str, value: &RawOptionValue) -> bool {
    matches!(
        (scalar_type, value),
        ("bool", RawOptionValue::Bool(_))
            | ("uint32", RawOptionValue::U32(_))
            | ("uint64", RawOptionValue::U64(_))
            | ("string", RawOptionValue::String(_))
    )
}

impl FieldOptionValues {
    /// Convert typed values to the generic raw option model consumed by exporters.
    pub fn raw(&self) -> RawOptions {
        let mut options = RawOptions::new();
        if let Some(value) = self.nonzero_fixed_bytes {
            options.push(spec(51001), RawOptionValue::U32(value));
        }
        if let Some(value) = self.required_message {
            options.push(spec(51002), RawOptionValue::Bool(value));
        }
        if let Some(value) = self.positive_uint64 {
            options.push(spec(51003), RawOptionValue::Bool(value));
        }
        if let Some(value) = self.min_items {
            options.push(spec(51005), RawOptionValue::U32(value));
        }
        if let Some(value) = self.max_items {
            options.push(spec(51006), RawOptionValue::U32(value));
        }
        if let Some(value) = self.nonempty_max_bytes {
            options.push(spec(51007), RawOptionValue::U32(value));
        }
        if let Some(value) = self.max_uint64 {
            options.push(spec(51008), RawOptionValue::U64(value));
        }
        if let Some(value) = self.known_nonzero_enum {
            options.push(spec(51009), RawOptionValue::Bool(value));
        }
        if let Some(value) = self.nonempty_max_item_bytes {
            options.push(spec(51010), RawOptionValue::U32(value));
        }
        options
    }

    /// Encode extension fields exactly as a proto2 options message.
    pub fn encode(&self) -> Vec<u8> {
        self.raw().encode()
    }
}

/// Typed `EnumValueOptions` values.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EnumValueOptionValues {
    pub partial_terminal: Option<bool>,
}
impl EnumValueOptionValues {
    pub fn raw(&self) -> RawOptions {
        let mut options = RawOptions::new();
        if let Some(value) = self.partial_terminal {
            options.push(spec(51011), RawOptionValue::Bool(value));
        }
        options
    }

    pub fn encode(&self) -> Vec<u8> {
        self.raw().encode()
    }
}

/// Typed `OneofOptions` values.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OneofOptionValues {
    pub required_oneof: Option<bool>,
}
impl OneofOptionValues {
    pub fn raw(&self) -> RawOptions {
        let mut options = RawOptions::new();
        if let Some(value) = self.required_oneof {
            options.push(spec(51004), RawOptionValue::Bool(value));
        }
        options
    }

    pub fn encode(&self) -> Vec<u8> {
        self.raw().encode()
    }
}

/// Typed `MethodOptions` values.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MethodOptionValues {
    pub http_path: Option<String>,
}
impl MethodOptionValues {
    pub fn raw(&self) -> RawOptions {
        let mut options = RawOptions::new();
        if let Some(value) = &self.http_path {
            options.push(spec(51012), RawOptionValue::String(value.clone()));
        }
        options
    }

    pub fn encode(&self) -> Vec<u8> {
        self.raw().encode()
    }
}

/// Emit the canonical proto2 extension source.
pub fn options_proto() -> String {
    r#"syntax = "proto2";
package acyclic.validation.v1;
import "google/protobuf/descriptor.proto";

extend google.protobuf.FieldOptions {
  optional uint32 nonzero_fixed_bytes = 51001;
  optional bool required_message = 51002;
  optional bool positive_uint64 = 51003;
  optional uint32 min_items = 51005;
  optional uint32 max_items = 51006;
  optional uint32 nonempty_max_bytes = 51007;
  optional uint64 max_uint64 = 51008;
  optional bool known_nonzero_enum = 51009;
  optional uint32 nonempty_max_item_bytes = 51010;
}
extend google.protobuf.EnumValueOptions {
  optional bool partial_terminal = 51011;
}
extend google.protobuf.OneofOptions {
  optional bool required_oneof = 51004;
}
extend google.protobuf.MethodOptions {
  optional string http_path = 51012;
}
"#
    .to_owned()
}

/// Emit a descriptor fixture carrying all custom option targets.
pub fn descriptor_with_options() -> Vec<u8> {
    let base = FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("filesystem/harness/options-fixture.proto".into()),
            package: Some("acyclic.filesystem.v1".into()),
            message_type: vec![DescriptorProto {
                name: Some("FilesystemRequest".into()),
                field: vec![FieldDescriptorProto {
                    name: Some("id".into()),
                    number: Some(1),
                    label: Some(field_descriptor_proto::Label::Optional as i32),
                    r#type: Some(field_descriptor_proto::Type::Bytes as i32),
                    json_name: Some("id".into()),
                    ..Default::default()
                }],
                oneof_decl: vec![OneofDescriptorProto {
                    name: Some("source".into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            enum_type: vec![EnumDescriptorProto {
                name: Some("HarnessOutcome".into()),
                value: vec![EnumValueDescriptorProto {
                    name: Some("PARTIAL".into()),
                    number: Some(0),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            service: vec![ServiceDescriptorProto {
                name: Some("FilesystemHarnessService".into()),
                method: vec![MethodDescriptorProto {
                    name: Some("Run".into()),
                    input_type: Some(".acyclic.filesystem.v1.FilesystemRequest".into()),
                    output_type: Some(".acyclic.filesystem.v1.FilesystemRequest".into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
    .encode_to_vec();
    let field_options = FieldOptionValues {
        nonzero_fixed_bytes: Some(16),
        required_message: Some(true),
        positive_uint64: Some(true),
        min_items: Some(1),
        max_items: Some(8),
        nonempty_max_bytes: Some(64),
        max_uint64: Some(u64::MAX),
        known_nonzero_enum: Some(true),
        nonempty_max_item_bytes: Some(32),
    }
    .encode();
    let enum_options = EnumValueOptionValues {
        partial_terminal: Some(true),
    }
    .encode();
    let oneof_options = OneofOptionValues {
        required_oneof: Some(true),
    }
    .encode();
    let method_options = MethodOptionValues {
        http_path: Some("/v1/options/run".into()),
    }
    .encode();

    let file = first(&base, 1);
    let message = first(&file, 4);
    let field = append_options(&first(&message, 2), 8, &field_options);
    let message = replace(&message, 2, &length_delimited(2, &field));
    let oneof = append_options(&first(&message, 8), 2, &oneof_options);
    let message = replace(&message, 8, &length_delimited(8, &oneof));
    let file = replace(&file, 4, &length_delimited(4, &message));
    let enumeration = first(&file, 5);
    let value = append_options(&first(&enumeration, 2), 3, &enum_options);
    let enumeration = replace(&enumeration, 2, &length_delimited(2, &value));
    let file = replace(&file, 5, &length_delimited(5, &enumeration));
    let service = first(&file, 6);
    let method = append_options(&first(&service, 2), 4, &method_options);
    let service = replace(&service, 2, &length_delimited(2, &method));
    let file = replace(&file, 6, &length_delimited(6, &service));
    length_delimited(1, &file)
}

fn varint_option<T: Into<u64> + Copy>(out: &mut Vec<u8>, number: u32, value: Option<T>) {
    if let Some(value) = value {
        out.extend(varint(u64::from(number) << 3));
        out.extend(varint(value.into()));
    }
}
fn bool_option(out: &mut Vec<u8>, number: u32, value: Option<bool>) {
    varint_option(out, number, value.map(u64::from));
}
fn append_options(message: &[u8], number: u32, options: &[u8]) -> Vec<u8> {
    let mut out = message.to_vec();
    out.extend(length_delimited(number, options));
    out
}
fn first(bytes: &[u8], number: u32) -> Vec<u8> {
    parse(bytes)
        .into_iter()
        .find(|f| f.number == number)
        .expect("fixture field")
        .value
}
fn replace(bytes: &[u8], number: u32, replacement: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut done = false;
    for field in parse(bytes) {
        if !done && field.number == number {
            out.extend(replacement);
            done = true;
        } else {
            out.extend(field.encoded);
        }
    }
    assert!(done);
    out
}
#[derive(Clone)]
struct Wire {
    number: u32,
    value: Vec<u8>,
    encoded: Vec<u8>,
}
fn parse(bytes: &[u8]) -> Vec<Wire> {
    let mut cursor = 0;
    let mut out = Vec::new();
    while cursor < bytes.len() {
        let start = cursor;
        let key = read_varint(bytes, &mut cursor);
        let number = (key >> 3) as u32;
        match key & 7 {
            0 => {
                read_varint(bytes, &mut cursor);
            }
            1 => cursor += 8,
            2 => {
                let len = read_varint(bytes, &mut cursor) as usize;
                let end = cursor + len;
                out.push(Wire {
                    number,
                    value: bytes[cursor..end].to_vec(),
                    encoded: bytes[start..end].to_vec(),
                });
                cursor = end;
                continue;
            }
            5 => cursor += 4,
            wire_type => panic!("unsupported fixture wire type {wire_type} for field {number}"),
        }
        out.push(Wire {
            number,
            value: Vec::new(),
            encoded: bytes[start..cursor].to_vec(),
        });
    }
    out
}
fn read_varint(bytes: &[u8], cursor: &mut usize) -> u64 {
    let mut value = 0;
    for shift in (0..70).step_by(7) {
        let byte = bytes[*cursor];
        *cursor += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return value;
        }
    }
    panic!("fixture varint overflow")
}
fn length_delimited(number: u32, value: &[u8]) -> Vec<u8> {
    let mut out = varint((u64::from(number) << 3) | 2);
    out.extend(varint(value.len() as u64));
    out.extend(value);
    out
}
fn varint(mut value: u64) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            return out;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_sdk_contract_validation::{DifferenceKind, compare_bytes};
    use prost::Message;
    use std::fs;
    use std::path::Path;
    use std::process::Command;

    #[test]
    fn identities_and_targets_are_stable() {
        assert_eq!(OPTION_SPECS.len(), 12);
        assert_eq!(OPTION_SPECS[0].number, 51001);
        assert_eq!(OPTION_SPECS[3].target, OptionTarget::Oneof);
        assert_eq!(OPTION_SPECS[10].target, OptionTarget::EnumValue);
        assert_eq!(OPTION_SPECS[11].target, OptionTarget::Method);
        assert_eq!(
            option_spec(51012),
            option_spec_by_name(OptionTarget::Method, "http_path")
        );
    }

    #[test]
    fn raw_options_reject_ambiguous_assignments() {
        let mut options = RawOptions::new();
        let field = option_spec(51001).expect("pinned option");
        options
            .try_push(field, RawOptionValue::U32(32))
            .expect("valid option");
        assert_eq!(
            options.try_push(field, RawOptionValue::U32(64)),
            Err(RawOptionError::Duplicate { number: 51001 })
        );
        assert!(matches!(
            RawOptions::new().try_push(field, RawOptionValue::Bool(true)),
            Err(RawOptionError::TypeMismatch { .. })
        ));
        let unknown = OptionSpec {
            target: OptionTarget::Field,
            name: "future",
            number: 51013,
            scalar_type: "bytes",
        };
        assert_eq!(
            RawOptions::new().try_push(unknown, RawOptionValue::U32(1)),
            Err(RawOptionError::UnknownSpec { number: 51013 })
        );

        let mut unchecked = RawOptions::new();
        unchecked.push(spec(51005), RawOptionValue::Bool(true));
        assert!(matches!(
            unchecked.try_encode(),
            Err(RawOptionError::TypeMismatch { .. })
        ));

        let method_option = option_spec(51012).expect("pinned method option");
        let wrong_target = OptionSpec {
            target: OptionTarget::Field,
            ..method_option
        };
        assert_eq!(
            RawOptions::new().try_push(wrong_target, RawOptionValue::String("/run".into())),
            Err(RawOptionError::UnknownSpec { number: 51012 })
        );
    }

    #[test]
    fn raw_options_survive_prost_decode_loss() {
        let bytes = descriptor_with_options();
        let set = FileDescriptorSet::decode(bytes.as_slice()).expect("decode fixture");
        let file = first(&bytes, 1);
        let message = first(&file, 4);
        let field = first(&message, 2);
        let options = first(&field, 8);
        let numbers: Vec<_> = parse(&options)
            .into_iter()
            .map(|field| field.number)
            .collect();
        assert_eq!(
            numbers,
            vec![
                51001, 51002, 51003, 51005, 51006, 51007, 51008, 51009, 51010
            ]
        );
        assert_eq!(
            options,
            (FieldOptionValues {
                nonzero_fixed_bytes: Some(16),
                required_message: Some(true),
                positive_uint64: Some(true),
                min_items: Some(1),
                max_items: Some(8),
                nonempty_max_bytes: Some(64),
                max_uint64: Some(u64::MAX),
                known_nonzero_enum: Some(true),
                nonempty_max_item_bytes: Some(32),
            })
            .encode()
        );
        assert_eq!(
            (FieldOptionValues {
                nonzero_fixed_bytes: Some(16),
                required_message: Some(true),
                positive_uint64: Some(true),
                min_items: Some(1),
                max_items: Some(8),
                nonempty_max_bytes: Some(64),
                max_uint64: Some(u64::MAX),
                known_nonzero_enum: Some(true),
                nonempty_max_item_bytes: Some(32),
            })
            .raw()
            .fields()
            .iter()
            .map(|option| option.spec.number)
            .collect::<Vec<_>>(),
            numbers
        );
        assert!(
            set.file[0].message_type[0].field[0]
                .options
                .as_ref()
                .is_none_or(|value| value.packed.is_none())
        );
    }

    #[test]
    fn prost_normalization_cannot_hide_custom_option_loss() {
        let original = descriptor_with_options();
        let normalized = FileDescriptorSet::decode(original.as_slice())
            .expect("decode descriptor")
            .encode_to_vec();
        let report = compare_bytes(&original, &normalized).expect("compare descriptors");
        assert!(!report.semantic_compatible, "{report:?}");
        assert!(
            report
                .differences
                .iter()
                .any(|difference| difference.kind == DifferenceKind::CustomOption)
        );
    }

    #[test]
    fn generated_source_compiles_with_vendored_protoc() {
        let root = std::env::temp_dir().join(format!(
            "acyclic-sdk-contract-options-{}",
            std::process::id()
        ));
        let options_dir = root.join("acyclic/validation/v1");
        fs::create_dir_all(&options_dir).expect("create proto dir");
        fs::write(options_dir.join("options.proto"), options_proto()).expect("write options proto");
        fs::write(root.join("sample.proto"), r#"syntax = "proto2";
package acyclic.options.fixture;
import "acyclic/validation/v1/options.proto";
message Request { optional bytes id = 1 [(acyclic.validation.v1.nonzero_fixed_bytes) = 16]; oneof choice { option (acyclic.validation.v1.required_oneof) = true; string name = 2; } }
enum Outcome { PARTIAL = 0 [(acyclic.validation.v1.partial_terminal) = true]; }
service OptionsService { rpc Run(Request) returns (Request) { option (acyclic.validation.v1.http_path) = "/v1/options/run"; } }
"#).expect("write sample proto");
        let descriptor = root.join("sample.descriptor.bin");
        let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc");
        let include = protoc_bin_vendored::include_path().expect("vendored includes");
        let output = Command::new(protoc)
            .arg(format!("--proto_path={}", root.display()))
            .arg(format!("--proto_path={}", include.display()))
            .arg(format!("--descriptor_set_out={}", descriptor.display()))
            .arg("--include_imports")
            .arg("sample.proto")
            .current_dir(&root)
            .output()
            .expect("run protoc");
        assert!(
            output.status.success(),
            "protoc failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let compiled_bytes = fs::read(descriptor).expect("descriptor");
        let compiled = FileDescriptorSet::decode(compiled_bytes.as_slice())
            .expect("decode compiled descriptor");
        let sample = compiled
            .file
            .iter()
            .find(|file| file.name.as_deref() == Some("sample.proto"))
            .expect("sample file");
        assert_eq!(sample.service[0].method[0].name.as_deref(), Some("Run"));
        assert!(
            sample
                .dependency
                .iter()
                .any(|name| name == "acyclic/validation/v1/options.proto")
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn generated_options_match_active_proto_semantics() {
        let active_path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../proto/validation/v1/options.proto");
        let active_source = fs::read_to_string(active_path).expect("active options proto");
        let root = std::env::temp_dir().join(format!(
            "acyclic-sdk-contract-options-semantic-{}",
            std::process::id()
        ));
        let active_descriptor = compile_options_source(&root.join("active"), &active_source);
        let generated_descriptor =
            compile_options_source(&root.join("generated"), &options_proto());
        compare_bytes(&active_descriptor, &active_descriptor)
            .expect("compare active options descriptor");
        compare_bytes(&generated_descriptor, &generated_descriptor)
            .expect("compare generated options descriptor");
        let report = compare_bytes(&active_descriptor, &generated_descriptor)
            .expect("compare active and generated options descriptors");
        assert!(report.semantic_compatible, "{report:?}");
        assert!(
            report.exact_bytes_equal,
            "canonical option descriptors must match exactly"
        );
        let _ = fs::remove_dir_all(root);
    }

    fn compile_options_source(root: &Path, source: &str) -> Vec<u8> {
        let source_dir = root.join("acyclic/validation/v1");
        fs::create_dir_all(&source_dir).expect("create options source directory");
        fs::write(source_dir.join("options.proto"), source).expect("write options source");
        let output_path = root.join("options.descriptor.bin");
        let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc");
        let include = protoc_bin_vendored::include_path().expect("vendored includes");
        let output = Command::new(protoc)
            .arg(format!("--proto_path={}", root.display()))
            .arg(format!("--proto_path={}", include.display()))
            .arg(format!("--descriptor_set_out={}", output_path.display()))
            .arg("acyclic/validation/v1/options.proto")
            .current_dir(root)
            .output()
            .expect("run vendored protoc");
        assert!(
            output.status.success(),
            "protoc failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        fs::read(output_path).expect("read options descriptor")
    }
}
