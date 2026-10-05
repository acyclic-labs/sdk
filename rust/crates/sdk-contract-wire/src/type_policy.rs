//! Rust-owned semantic type policy for every generated SDK.
//!
//! Protobuf descriptors preserve the transport contract, but a descriptor
//! alone cannot express the strongest useful API type in each target
//! language.  This module is the one additional, Rust-authored metadata
//! boundary for that information.  Generators must consume these tables and
//! may not replace them with per-language hand-written contracts.
//!
//! The policy deliberately distinguishes three things:
//!
//! * wire storage, which must remain compatible with the archived descriptor;
//! * semantic constraints, some of which are stricter than Rust's type
//!   system (for example a fixed digest length or a non-empty value); and
//! * the idiomatic target-language representation used to expose those
//!   constraints to a compiler or static checker.
//!
//! Unknown enum numbers and unknown oneof arms are always preserved.  A
//! target may expose a closed, exhaustive convenience view only in addition
//! to the open wire representation; it must never discard an unknown value.

use std::{fs, path::{Path, PathBuf}};

use prost::Message;
use prost_types::{DescriptorProto, FileDescriptorSet, field_descriptor_proto::Type as FieldType};

use crate::family_registry::{FAMILY_VIEWS, FamilyModel};

/// A descriptor-resolved request field used by every language emitter.
///
/// This is derived from the Rust-owned family descriptors at generation time;
/// it is intentionally independent of the semantic override table below.
/// Ordinary fields therefore remain present in generated request models even
/// when they do not need a nominal refinement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedRequestField {
    pub family: String,
    pub rpc: String,
    pub root_message: String,
    pub message_path: String,
    pub field: String,
    pub number: i32,
    pub json_name: String,
    pub type_name: Option<String>,
    /// True when the descriptor type is a protobuf map-entry message. Target
    /// generators must project this as a native map instead of inventing a
    /// public wrapper for the compiler-generated entry type.
    pub map_entry: bool,
    pub wire_type: Option<i32>,
    pub label: Option<i32>,
    pub oneof_index: Option<i32>,
    /// Rust descriptor oneof identity used to generate a nominal target union
    /// while retaining the original wire bytes for unknown arms.
    pub oneof_name: Option<String>,
    pub proto3_optional: bool,
    pub semantic_type: Option<String>,
    pub validation_rules: Vec<String>,
    pub validation_constraints: Vec<ResolvedValidationConstraint>,
}

/// Rust-owned operation identity retained alongside the field inventory.
///
/// Empty request messages are valid operations, so consumers must enumerate
/// this model rather than inferring the RPC set from resolved fields.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedRpcMethod {
    pub family: String,
    pub rpc: String,
    pub service: String,
    pub method: String,
    pub input_message: String,
    pub output_message: String,
    pub client_streaming: bool,
    pub server_streaming: bool,
}

/// Validation evidence attached to a descriptor field. Rule entries are safe
/// for target generators to project into constructors or refinements. A
/// cross-field entry remains visible to runtime validation and cannot be
/// reduced to a scalar wrapper without losing contract meaning.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResolvedValidationConstraint {
    Rule(SemanticRule),
    /// A structured operation rule whose meaning is owned by the Rust
    /// validator and must be projected as an operation-aware constructor or
    /// runtime check.  It is deliberately distinct from a generic
    /// cross-field string so generators cannot silently drop the rule.
    Operation(OperationRule),
    CrossField(String),
    Unresolved(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationRule {
    /// The request scope must advertise the capability for this operation.
    Capability(&'static str),
    /// Bucket deletion is admitted only when the Rust provider observes no
    /// remaining objects in the named bucket.
    BucketMustBeEmpty,
    /// Multipart completion requires strictly increasing part numbers within
    /// the native Rust provider's hard bound.
    OrderedParts {
        max_items: u32,
        max_part_number: u32,
    },
    /// Optional preconditions are an atomic oneof with validated payloads.
    AtomicPrecondition,
    /// Every stream record is bounded by the canonical Rust wire limit.
    MaxRecordBytes(u32),
    /// Every stream mutation command is bounded by the canonical Rust wire
    /// envelope limit.
    MaxCommandBytes(u32),
    /// A Rust-owned policy identity whose target language projection must
    /// retain the original validation and target metadata even when no
    /// single scalar refinement can represent it.
    Policy(&'static str),
}

/// Where the Rust contract requires an operation rule to be enforced.
///
/// This classification is part of the Rust source model.  Generators use it
/// to emit local checks, provider-state requirements, or response checks;
/// they must not silently turn any of these rules into documentation-only
/// strings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationEnforcement {
    ClientLocal,
    ProviderState,
    ResponseInvariant,
    /// The Rust source has named a rule that has no qualified projection yet.
    /// Generators must surface this state and fail the qualification gate;
    /// it must never be downgraded to a provider or documentation-only rule.
    Unsupported,
}

/// The concrete logical target of an operation validation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OperationTarget {
    pub path: &'static str,
    pub enforcement: OperationEnforcement,
}

/// A Rust-owned operation validation that cannot be represented by a scalar
/// field refinement.  Keeping the policy identity beside the structured rule
/// lets generators project the rule onto the real operation method instead of
/// serializing a descriptive string and silently dropping enforcement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResolvedOperationRule {
    pub family: &'static str,
    pub rpc: &'static str,
    pub validation: &'static str,
    pub rule: OperationRule,
    pub target: OperationTarget,
}

/// Resolve every reachable request field from the single Rust contract model.
///
/// A fresh vector is returned so generators can sort or group fields without
/// mutating shared model state.  Descriptor lookup is global across the
/// registered families because request fields commonly use protocol messages
/// declared in a dependency file.
pub fn resolved_request_fields() -> Result<Vec<ResolvedRequestField>, String> {
    resolve_rpc_fields(true)
}

/// Resolve every reachable response field from the same Rust contract model.
///
/// The output uses the same record as request fields so language emitters can
/// share one renderer while selecting the direction they are projecting.
pub fn resolved_response_fields() -> Result<Vec<ResolvedRequestField>, String> {
    resolve_rpc_fields(false)
}

/// Resolve every RPC identity, including methods with empty request messages.
pub fn resolved_rpc_methods() -> Result<Vec<ResolvedRpcMethod>, String> {
    let files = rust_descriptor_files()?;
    let mut methods = Vec::new();
    for family in FAMILY_VIEWS {
        let Some(file) = files.iter().find(|file| {
            file.package.as_deref() == Some(family.package())
                && file.name.as_deref() == Some(family.file_name())
        }) else {
            return Err(format!(
                "{} Rust descriptor file {} is missing",
                family.name,
                family.file_name()
            ));
        };
        for service in &file.service {
            let service_name = service.name.as_deref().unwrap_or_default();
            for method in &service.method {
                let method_name = method.name.as_deref().unwrap_or_default();
                let input_message = method
                    .input_type
                    .as_deref()
                    .ok_or_else(|| format!("{} method {method_name} has no input", family.name))?;
                let output_message = method
                    .output_type
                    .as_deref()
                    .ok_or_else(|| format!("{} method {method_name} has no output", family.name))?;
                methods.push(ResolvedRpcMethod {
                    family: family.name.to_owned(),
                    rpc: format!("{}.{}/{}", family.package(), service_name, method_name),
                    service: service_name.to_owned(),
                    method: method_name.to_owned(),
                    input_message: input_message.trim_start_matches('.').to_owned(),
                    output_message: output_message.trim_start_matches('.').to_owned(),
                    client_streaming: method.client_streaming.unwrap_or(false),
                    server_streaming: method.server_streaming.unwrap_or(false),
                });
            }
        }
    }
    methods.sort_by(|left, right| left.rpc.cmp(&right.rpc));
    Ok(methods)
}

/// Resolve all structured operation rules directly from the Rust operation
/// policies.  This inventory is independent of field attachment because rules
/// such as bucket emptiness and capability admission are properties of the
/// operation as a whole.  Generators must consume this API for operation-level
/// validation and must fail closed when a rule has no projection.
pub fn resolved_operation_rules() -> Vec<ResolvedOperationRule> {
    let mut rules = Vec::new();
    for family in FAMILY_VIEWS {
        for policy in family.operation_policies {
            for validation in policy.validations {
                if let Some(rule) = operation_rule(validation) {
                    rules.push(ResolvedOperationRule {
                        family: family.name,
                        rpc: policy.rpc,
                        validation,
                        rule,
                        target: operation_target(validation),
                    });
                } else {
                    // Keep every Rust-authored policy visible to generators,
                    // including the 82 operation-only validations that do
                    // not attach to a single descriptor field.
                    rules.push(ResolvedOperationRule {
                        family: family.name,
                        rpc: policy.rpc,
                        validation,
                        rule: OperationRule::Policy(validation),
                        target: operation_target(validation),
                    });
                }
            }
        }
    }
    rules.sort_by(|left, right| (left.rpc, left.validation).cmp(&(right.rpc, right.validation)));
    rules
}

fn resolve_rpc_fields(request: bool) -> Result<Vec<ResolvedRequestField>, String> {
    let files = rust_descriptor_files()?;

    let mut messages = std::collections::BTreeMap::<String, DescriptorProto>::new();
    for file in &files {
        let package = file.package.as_deref().unwrap_or_default();
        for message in &file.message_type {
            collect_messages(package, None, message, &mut messages);
        }
    }

    let mut fields = Vec::new();
    for family in FAMILY_VIEWS {
        let Some(package_file) = files.iter().find(|file| {
            file.package.as_deref() == Some(family.package())
                && file.name.as_deref() == Some(family.file_name())
        }) else {
            return Err(format!(
                "{} Rust descriptor file {} is missing",
                family.name,
                family.file_name()
            ));
        };
        for service in &package_file.service {
            let service_name = service.name.as_deref().unwrap_or_default();
            for method in &service.method {
                let method_name = method.name.as_deref().unwrap_or_default();
                let reference = if request {
                    method.input_type.as_deref()
                } else {
                    method.output_type.as_deref()
                };
                let Some(reference) = reference else {
                    return Err(format!(
                        "{} method {method_name} has no {} message",
                        family.name,
                        if request { "input" } else { "output" }
                    ));
                };
                let root_name = reference.trim_start_matches('.');
                let Some(message) = messages.get(root_name) else {
                    return Err(format!(
                        "{} method {method_name} {} {root_name} is missing from Rust descriptors",
                        family.name,
                        if request { "input" } else { "output" },
                    ));
                };
                let rpc = format!("{}.{}/{}", family.package(), service_name, method_name);
                let mut active = std::collections::BTreeSet::new();
                active.insert(root_name.to_owned());
                collect_reachable_fields(
                    family.name,
                    &rpc,
                    root_name,
                    root_name,
                    message,
                    &messages,
                    &mut fields,
                    &mut active,
                )?;
            }
        }
    }
    fields.sort_by(|left, right| {
        (
            left.family.as_str(),
            left.rpc.as_str(),
            left.message_path.as_str(),
            left.number,
        )
            .cmp(&(
                right.family.as_str(),
                right.rpc.as_str(),
                right.message_path.as_str(),
                right.number,
            ))
    });
    fields.dedup();
    Ok(fields)
}

fn rust_descriptor_files() -> Result<Vec<prost_types::FileDescriptorProto>, String> {
    let mut files = Vec::new();
    for family in FAMILY_VIEWS {
        let descriptor = match family.model {
            FamilyModel::ContractSpec(_) | FamilyModel::Filesystem(_) | FamilyModel::Harness(_) => {
                family.model.descriptor()
            }
        };
        let set = FileDescriptorSet::decode(descriptor.as_slice())
            .map_err(|error| format!("{} descriptor decode failed: {error}", family.name))?;
        files.extend(set.file);
    }
    Ok(files)
}

fn collect_messages(
    package: &str,
    parent: Option<&str>,
    message: &DescriptorProto,
    output: &mut std::collections::BTreeMap<String, DescriptorProto>,
) {
    let Some(name) = message.name.as_deref() else {
        return;
    };
    let full_name = match parent {
        Some(parent) => format!("{parent}.{name}"),
        None if package.is_empty() => name.to_owned(),
        None => format!("{package}.{name}"),
    };
    output.insert(full_name.clone(), message.clone());
    for nested in &message.nested_type {
        collect_messages(package, Some(&full_name), nested, output);
    }
}

fn collect_reachable_fields(
    family: &str,
    rpc: &str,
    root_message: &str,
    message_path: &str,
    message: &DescriptorProto,
    messages: &std::collections::BTreeMap<String, DescriptorProto>,
    output: &mut Vec<ResolvedRequestField>,
    active: &mut std::collections::BTreeSet<String>,
) -> Result<(), String> {
    for field in &message.field {
        let Some(field_name) = field.name.as_deref() else {
            continue;
        };
        let semantic_type_id = semantic_binding_for_field(
            family,
            message_path,
            field_name,
            field.r#type,
            field.type_name.as_deref(),
        );
        let map_entry = field
            .type_name
            .as_deref()
            .map(|reference| reference.trim_start_matches('.'))
            .and_then(|reference| messages.get(reference))
            .and_then(|message| message.options.as_ref())
            .and_then(|options| options.map_entry)
            .unwrap_or(false);
        let oneof_name = field
            .oneof_index
            .and_then(|index| usize::try_from(index).ok())
            .and_then(|index| message.oneof_decl.get(index))
            .and_then(|declaration| declaration.name.clone());
        output.push(ResolvedRequestField {
            family: family.to_owned(),
            rpc: rpc.to_owned(),
            root_message: root_message.to_owned(),
            message_path: message_path.to_owned(),
            field: field_name.to_owned(),
            number: field.number.unwrap_or_default(),
            json_name: field
                .json_name
                .clone()
                .unwrap_or_else(|| field_name.to_owned()),
            type_name: field.type_name.clone(),
            map_entry,
            wire_type: field.r#type,
            label: field.label,
            oneof_index: field.oneof_index,
            oneof_name,
            proto3_optional: field.proto3_optional.unwrap_or(false),
            semantic_type: semantic_type_id.clone(),
            validation_rules: validation_rules_for_field(family, rpc, message_path, field_name),
            validation_constraints: validation_constraints_for_field(
                family,
                rpc,
                message_path,
                field_name,
                field.r#type,
                semantic_type_id.as_deref(),
            ),
        });
        if field.r#type == Some(FieldType::Message as i32)
            || field.r#type == Some(FieldType::Group as i32)
        {
            if let Some(reference) = field.type_name.as_deref() {
                let reference = reference.trim_start_matches('.');
                let Some(nested) = messages.get(reference) else {
                    if is_known_external_message(reference) {
                        continue;
                    }
                    return Err(format!(
                        "{family} {rpc} field {}.{} references missing message {reference}",
                        message_path, field_name
                    ));
                };
                if active.insert(reference.to_owned()) {
                    collect_reachable_fields(
                        family,
                        rpc,
                        root_message,
                        reference,
                        nested,
                        messages,
                        output,
                        active,
                    )?;
                    active.remove(reference);
                }
            }
        }
    }
    Ok(())
}

fn is_known_external_message(name: &str) -> bool {
    name.starts_with("google.protobuf.")
}

fn semantic_binding_for_field(
    family: &str,
    message_path: &str,
    field: &str,
    wire_type: Option<i32>,
    type_name: Option<&str>,
) -> Option<String> {
    let message = message_path.rsplit('.').next().unwrap_or(message_path);
    if let Some(semantic_type) = PUBLIC_FIELD_BINDINGS
        .iter()
        .find(|binding| {
            binding.family == family && binding.message == message && binding.wire_field == field
        })
        .map(|binding| binding.semantic_type.to_owned())
    {
        if semantic_binding_compatible(&semantic_type, wire_type, type_name) {
            return Some(semantic_type);
        }
    }

    // A semantic field mapping is reusable across every protobuf message in a
    // family when the wire field identity is unambiguous.  This is necessary
    // for wrapper values such as Machines.IdempotencyKey: the native Rust
    // validator owns the UUID/non-empty invariant, while the same message
    // appears in create, checkpoint, fork, recovery, and mutation requests.
    // Do not guess when a family has multiple meanings for the same wire name
    // (for example `limit` has distinct machine and event bounds).
    let candidates = PUBLIC_FIELD_BINDINGS
        .iter()
        .filter(|binding| binding.family == family && binding.wire_field == field)
        .map(|binding| binding.semantic_type)
        .collect::<std::collections::BTreeSet<_>>();
    if candidates.len() == 1 {
        candidates
            .into_iter()
            .find(|candidate| semantic_binding_compatible(candidate, wire_type, type_name))
            .map(str::to_owned)
    } else {
        None
    }
}

fn semantic_binding_compatible(
    semantic_id: &str,
    wire_type: Option<i32>,
    type_name: Option<&str>,
) -> bool {
    let Some(semantic) = semantic_type(semantic_id) else {
        return false;
    };
    let Some(wire) = wire_type.and_then(|kind| FieldType::try_from(kind).ok()) else {
        return false;
    };
    match semantic.wire_kind {
        WireValueKind::Message => matches!(wire, FieldType::Message | FieldType::Group),
        WireValueKind::String => matches!(wire, FieldType::String),
        WireValueKind::Bytes => {
            matches!(wire, FieldType::Bytes)
                || (matches!(wire, FieldType::Message | FieldType::Group)
                    && matches!(
                        semantic.rust_name,
                        "MachineId" | "CheckpointId" | "OperationId"
                    )
                    && type_name.and_then(|name| name.rsplit('.').next())
                        == Some(semantic.rust_name))
        }
        WireValueKind::Boolean => matches!(wire, FieldType::Bool),
        WireValueKind::SignedInteger | WireValueKind::UnsignedInteger => matches!(
            wire,
            FieldType::Int32
                | FieldType::Sint32
                | FieldType::Sfixed32
                | FieldType::Uint32
                | FieldType::Fixed32
                | FieldType::Int64
                | FieldType::Sint64
                | FieldType::Sfixed64
                | FieldType::Uint64
                | FieldType::Fixed64
        ),
        WireValueKind::Enum => matches!(wire, FieldType::Enum),
        WireValueKind::Timestamp => matches!(wire, FieldType::Message | FieldType::Group),
        WireValueKind::Oneof => false,
    }
}

fn canonical_rpc_identity(rpc: &str) -> &str {
    rpc.trim_start_matches('/')
}

fn operation_policy_for_rpc(
    view: &crate::family_registry::FamilyView,
    rpc: &str,
) -> Option<&'static crate::OperationPolicy> {
    let rpc = canonical_rpc_identity(rpc);
    view.operation_policies
        .iter()
        .find(|policy| canonical_rpc_identity(policy.rpc) == rpc)
}

fn validation_targets_field(family: &str, message: &str, field: &str, validation: &str) -> bool {
    let Some(head) = validation.split('.').next() else {
        return false;
    };
    if head == field {
        return true;
    }
    PUBLIC_FIELD_BINDINGS.iter().any(|binding| {
        binding.family == family
            && binding.message == message
            && binding.wire_field == field
            && policy_field_name_matches(binding.field, head)
    })
}

fn policy_field_name_matches(binding_field: &str, policy_field: &str) -> bool {
    binding_field == policy_field
        // The Machines operation policies use the historical `page_limit`
        // name for both list and event pagination.  The wire model exposes
        // the same `limit` field, while the Rust semantic model keeps the
        // stronger event-specific bound explicit.
        || (binding_field == "event_page_limit" && policy_field == "page_limit")
}

fn validation_rules_for_field(
    family: &str,
    rpc: &str,
    message_path: &str,
    field: &str,
) -> Vec<String> {
    let Some(view) = crate::family_registry::family_view(family) else {
        return Vec::new();
    };
    let message = message_path.rsplit('.').next().unwrap_or(message_path);
    operation_policy_for_rpc(view, rpc)
        .map(|policy| {
            policy
                .validations
                .iter()
                .filter(|validation| validation_targets_field(family, message, field, validation))
                .map(|validation| (*validation).to_owned())
                .collect()
        })
        .unwrap_or_default()
}

fn validation_constraints_for_field(
    family: &str,
    rpc: &str,
    message_path: &str,
    field: &str,
    wire_type: Option<i32>,
    semantic_type_id: Option<&str>,
) -> Vec<ResolvedValidationConstraint> {
    let Some(view) = crate::family_registry::family_view(family) else {
        return Vec::new();
    };
    let Some(policy) = operation_policy_for_rpc(view, rpc) else {
        return Vec::new();
    };
    policy
        .validations
        .iter()
        .filter(|validation| {
            validation_targets_field(
                family,
                message_path.rsplit('.').next().unwrap_or(message_path),
                field,
                validation,
            )
        })
        .flat_map(|validation| validation_constraint(validation, wire_type, semantic_type_id))
        .collect()
}

fn validation_constraint(
    validation: &str,
    wire_type: Option<i32>,
    semantic_type_id: Option<&str>,
) -> Vec<ResolvedValidationConstraint> {
    if let Some(rule) = operation_rule(validation) {
        return vec![ResolvedValidationConstraint::Operation(rule)];
    }
    let suffix = validation.rsplit('.').next().unwrap_or(validation);
    if suffix == "nonzero"
        && matches!(
            wire_type,
            Some(kind) if kind == FieldType::Message as i32 || kind == FieldType::Group as i32
        )
    {
        let Some(semantic_id) = semantic_type_id else {
            return vec![ResolvedValidationConstraint::Unresolved(
                validation.to_owned(),
            )];
        };
        let Some(semantic) = semantic_type(semantic_id) else {
            return vec![ResolvedValidationConstraint::Unresolved(
                validation.to_owned(),
            )];
        };
        if semantic.rules.is_empty() {
            return vec![ResolvedValidationConstraint::Unresolved(
                validation.to_owned(),
            )];
        }
        return semantic
            .rules
            .iter()
            .copied()
            .map(ResolvedValidationConstraint::Rule)
            .collect();
    }
    let rules = match suffix {
        "non_empty_utf8" => Some(vec![SemanticRule::NonEmpty, SemanticRule::Utf8]),
        "non_empty_bytes" | "non_empty" | "nonempty" => Some(vec![SemanticRule::NonEmpty]),
        "present" => Some(vec![SemanticRule::ExplicitPresence]),
        "positive" => Some(vec![SemanticRule::StrictlyPositive]),
        "nonzero" => Some(vec![if wire_type == Some(FieldType::Bytes as i32) {
            SemanticRule::NonEmpty
        } else {
            SemanticRule::StrictlyPositive
        }]),
        "non_negative" => Some(vec![SemanticRule::NonNegative]),
        "length_16" | "16_bytes" => Some(vec![SemanticRule::FixedLength(16)]),
        "length_32" | "32_bytes" => Some(vec![SemanticRule::FixedLength(32)]),
        "immutable_digest" => Some(vec![
            SemanticRule::FixedLength(32),
            SemanticRule::Sha256Digest,
        ]),
        "max_stream_items" => Some(vec![
            SemanticRule::StrictlyPositive,
            SemanticRule::MaxItems(1024),
        ]),
        _ => None,
    };
    rules
        .map(|rules| {
            rules
                .into_iter()
                .map(ResolvedValidationConstraint::Rule)
                .collect()
        })
        .unwrap_or_else(|| {
            let cross_field_suffixes = [
                "bounded",
                "exact",
                "identity",
                "matches",
                "required",
                "valid",
                "supported",
                "proven",
                "declared",
                "preserving",
                "contiguous",
                "monotonic",
                "capability",
            ];
            if cross_field_suffixes.contains(&suffix) {
                vec![ResolvedValidationConstraint::CrossField(
                    validation.to_owned(),
                )]
            } else {
                vec![ResolvedValidationConstraint::Unresolved(
                    validation.to_owned(),
                )]
            }
        })
}

/// Resolve operation-level validations from the same Rust-native constants
/// and provider semantics used by runtime admission.  These rules often
/// mention a message or repeated field, but cannot be reduced to a scalar
/// wrapper without losing state, oneof, ordering, or capability meaning.
fn operation_rule(validation: &str) -> Option<OperationRule> {
    match validation {
        "scope.capability.operation_cancel" => Some(OperationRule::Capability("operation:cancel")),
        "scope.capability.operation_observe" => {
            Some(OperationRule::Capability("operation:observe"))
        }
        "bucket.empty" => Some(OperationRule::BucketMustBeEmpty),
        "parts.ordered_exact" => Some(OperationRule::OrderedParts {
            max_items: OBJECTS_MAX_MULTIPART_ITEMS,
            max_part_number: OBJECTS_MAX_MULTIPART_ITEMS,
        }),
        "preconditions.atomic" => Some(OperationRule::AtomicPrecondition),
        "records.max_bytes" => Some(OperationRule::MaxRecordBytes(STREAM_MAX_RECORD_BYTES)),
        "mutations.max_command_bytes" => {
            Some(OperationRule::MaxCommandBytes(STREAM_MAX_COMMAND_BYTES))
        }
        _ => None,
    }
}

/// Resolve a policy's logical target and enforcement boundary from the
/// Rust-authored validation identity.  The explicit targets cover nested
/// request values and provider/response invariants; the fallback retains the
/// complete policy identity so a newly added rule cannot disappear from a
/// generated SDK.
pub fn operation_target(validation: &'static str) -> OperationTarget {
    let (path, enforcement) = match validation {
        "bucket.empty" => ("bucket", OperationEnforcement::ProviderState),
        "bucket.name.non_empty" => ("bucket.name", OperationEnforcement::ClientLocal),
        "object.key.non_empty" => ("object.key", OperationEnforcement::ClientLocal),
        "request_identity.nonzero" => ("request_identity", OperationEnforcement::ClientLocal),
        "action.present" => ("action", OperationEnforcement::ClientLocal),
        "message.bounded" => ("message", OperationEnforcement::ClientLocal),
        "contract.valid" => ("contract", OperationEnforcement::ClientLocal),
        "limits.valid" => ("limits", OperationEnforcement::ClientLocal),
        "scope.capability.operation_cancel" => {
            ("scope.capability", OperationEnforcement::ClientLocal)
        }
        "scope.capability.operation_observe" => {
            ("scope.capability", OperationEnforcement::ClientLocal)
        }
        "response.identity.matches" | "status.identity.matches" => {
            ("response.identity", OperationEnforcement::ResponseInvariant)
        }
        "delivery.identity.preserving" => {
            ("delivery.identity", OperationEnforcement::ResponseInvariant)
        }
        "admission.identity.matches" => (
            "admission.identity",
            OperationEnforcement::ResponseInvariant,
        ),
        "protocol.identity.exact" | "protocol.version.exact" => {
            ("protocol", OperationEnforcement::ClientLocal)
        }
        "descriptor_digest.matches" | "workspace.identity.matches" => {
            ("identity", OperationEnforcement::ClientLocal)
        }
        "join.plan_identity.matches" => ("join.plan_identity", OperationEnforcement::ClientLocal),
        "workspace.selector.required" => ("workspace.selector", OperationEnforcement::ClientLocal),
        "workspace.reference.required" => {
            ("workspace.reference", OperationEnforcement::ClientLocal)
        }
        "generation.reference.required" => {
            ("generation.reference", OperationEnforcement::ClientLocal)
        }
        "generation.references.required" => {
            ("generation.references", OperationEnforcement::ClientLocal)
        }
        "owner.required" => ("owner", OperationEnforcement::ClientLocal),
        "scope.required" => ("scope", OperationEnforcement::ClientLocal),
        "required_capability.nonempty" | "required_capability.supported" => {
            ("required_capability", OperationEnforcement::ClientLocal)
        }
        "operation_id.16_bytes" | "operation_id.nonempty" => {
            ("operation_id", OperationEnforcement::ClientLocal)
        }
        "idempotency_key.nonempty" | "operation.idempotency_key.16_bytes" => {
            ("idempotency_key", OperationEnforcement::ClientLocal)
        }
        "mutation.oneof" => ("mutation", OperationEnforcement::ClientLocal),
        "transaction.bounded" => ("transaction", OperationEnforcement::ClientLocal),
        // The shape is checked locally when available; atomic state admission is provider-owned.
        "preconditions.atomic" => ("preconditions", OperationEnforcement::ProviderState),
        "upload.completion_frame" => ("upload.completion_frame", OperationEnforcement::ClientLocal),
        "part_number.positive" => ("part_number", OperationEnforcement::ClientLocal),
        "limit.max_stream_items" => ("limit", OperationEnforcement::ClientLocal),
        "expected_configuration_revision.non_negative" => (
            "expected_configuration_revision",
            OperationEnforcement::ClientLocal,
        ),
        "image.immutable_digest" => ("image", OperationEnforcement::ClientLocal),
        "source.present" => ("source", OperationEnforcement::ClientLocal),
        "maximum_output.positive" => ("maximum_output", OperationEnforcement::ClientLocal),
        "parts.ordered_exact" => ("parts", OperationEnforcement::ClientLocal),
        "records.max_bytes" => ("records", OperationEnforcement::ClientLocal),
        "mutations.max_command_bytes" => ("mutations", OperationEnforcement::ClientLocal),
        _ if is_current_policy_validation(validation)
            && (validation.ends_with(".valid")
            || validation.ends_with(".bounded")
            || validation.ends_with(".supported")
            || validation.ends_with(".proven")
            || validation.ends_with(".declared")
            || validation.ends_with(".required")
            || validation.ends_with(".nonempty")
            || validation.ends_with(".non_empty")
            || validation.ends_with(".non_empty_utf8")
            || validation.ends_with(".non_empty_bytes")
            || validation.ends_with(".nonzero")
            || validation.ends_with(".16_bytes")
            || validation.ends_with(".32_bytes")
            || validation.ends_with(".length_16")
            || validation.ends_with(".length_32")
            || validation.ends_with(".exact")
            || validation.ends_with(".preserving")
            || validation.ends_with(".contiguous")
            || validation.ends_with(".monotonic")) =>
        {
            (validation, OperationEnforcement::ClientLocal)
        }
        _ => (validation, OperationEnforcement::Unsupported),
    };
    OperationTarget { path, enforcement }
}

/// Return the enforcement boundary for a descriptor-attached validation rule.
/// Descriptor resolution owns these strings in `String` values, so this
/// borrowing API lets emitters apply the same Rust policy without manufacturing
/// a leaked `'static` target path merely to classify a field check.
pub fn operation_enforcement(validation: &str) -> OperationEnforcement {
    match validation {
        "bucket.empty" => OperationEnforcement::ProviderState,
        "response.identity.matches"
        | "status.identity.matches"
        | "delivery.identity.preserving"
        | "admission.identity.matches" => OperationEnforcement::ResponseInvariant,
        "scope.capability.operation_cancel"
        | "scope.capability.operation_observe"
        | "bucket.name.non_empty"
        | "object.key.non_empty"
        | "request_identity.nonzero"
        | "action.present"
        | "message.bounded"
        | "contract.valid"
        | "limits.valid"
        | "protocol.identity.exact"
        | "protocol.version.exact"
        | "descriptor_digest.matches"
        | "workspace.identity.matches"
        | "join.plan_identity.matches"
        | "workspace.selector.required"
        | "workspace.reference.required"
        | "generation.reference.required"
        | "generation.references.required"
        | "owner.required"
        | "scope.required"
        | "required_capability.nonempty"
        | "required_capability.supported"
        | "operation_id.16_bytes"
        | "operation_id.nonempty"
        | "idempotency_key.nonempty"
        | "operation.idempotency_key.16_bytes"
        | "mutation.oneof"
        | "transaction.bounded"
        | "upload.completion_frame"
        | "part_number.positive"
        | "limit.max_stream_items"
        | "expected_configuration_revision.non_negative"
        | "image.immutable_digest"
        | "source.present"
        | "maximum_output.positive"
        | "parts.ordered_exact"
        | "records.max_bytes"
        | "mutations.max_command_bytes" => OperationEnforcement::ClientLocal,
        _ if is_current_policy_validation(validation)
            && (validation.ends_with(".valid")
            || validation.ends_with(".bounded")
            || validation.ends_with(".supported")
            || validation.ends_with(".proven")
            || validation.ends_with(".declared")
            || validation.ends_with(".required")
            || validation.ends_with(".nonempty")
            || validation.ends_with(".non_empty")
            || validation.ends_with(".non_empty_utf8")
            || validation.ends_with(".non_empty_bytes")
            || validation.ends_with(".nonzero")
            || validation.ends_with(".16_bytes")
            || validation.ends_with(".32_bytes")
            || validation.ends_with(".length_16")
            || validation.ends_with(".length_32")
            || validation.ends_with(".exact")
            || validation.ends_with(".preserving")
            || validation.ends_with(".contiguous")
            || validation.ends_with(".monotonic")) =>
        {
            OperationEnforcement::ClientLocal
        }
        _ => OperationEnforcement::Unsupported,
    }
}

/// Suffix classification is only a convenience for identities already
/// authored by the Rust operation policy tables.  A newly introduced or
/// caller-supplied identity must remain unsupported until it receives an
/// explicit Rust target and enforcement boundary; otherwise a name such as
/// `future.aggregate.valid` could be emitted as a local check without any
/// qualified semantics behind it.
fn is_current_policy_validation(validation: &str) -> bool {
    FAMILY_VIEWS.iter().any(|family| {
        family
            .operation_policies
            .iter()
            .flat_map(|policy| policy.validations.iter().copied())
            .any(|candidate| candidate == validation)
    })
}

/// Every language target currently inventoried by the generation pipeline.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TypePolicyLanguage {
    Rust,
    TypeScript,
    Python,
    Go,
    Java,
    CSharp,
    Swift,
    Cpp,
    Ruby,
    Php,
    Dart,
    Kotlin,
    Scala,
    Elixir,
    Ballerina,
    ObjectiveC,
    Erlang,
    Ocaml,
    CommonLisp,
    Ada,
    C,
    Clojure,
    Crystal,
    Elm,
    Gdscript,
    Julia,
    Nim,
    Perl,
    Powershell,
    R,
    Bash,
    Haskell,
    Lua,
}

impl TypePolicyLanguage {
    /// Stable inventory identifier used by generators and receipts.
    pub const fn id(self) -> &'static str {
        match self {
            Self::Rust => "rust",
            Self::TypeScript => "typescript",
            Self::Python => "python",
            Self::Go => "go",
            Self::Java => "java",
            Self::CSharp => "csharp",
            Self::Swift => "swift",
            Self::Cpp => "cpp",
            Self::Ruby => "ruby",
            Self::Php => "php",
            Self::Dart => "dart",
            Self::Kotlin => "kotlin",
            Self::Scala => "scala",
            Self::Elixir => "elixir",
            Self::Ballerina => "ballerina",
            Self::ObjectiveC => "objective-c",
            Self::Erlang => "erlang",
            Self::Ocaml => "ocaml",
            Self::CommonLisp => "common-lisp",
            Self::Ada => "ada",
            Self::C => "c",
            Self::Clojure => "clojure",
            Self::Crystal => "crystal",
            Self::Elm => "elm",
            Self::Gdscript => "gdscript",
            Self::Julia => "julia",
            Self::Nim => "nim",
            Self::Perl => "perl",
            Self::Powershell => "powershell",
            Self::R => "r",
            Self::Bash => "bash",
            Self::Haskell => "haskell",
            Self::Lua => "lua",
        }
    }

    /// The complete target set, excluding the non-language documentation
    /// target.  Keep this list in Rust so the generator cannot silently omit
    /// a language when the external inventory changes.
    pub const ALL: &'static [Self] = &[
        Self::Rust,
        Self::TypeScript,
        Self::Python,
        Self::Go,
        Self::Java,
        Self::CSharp,
        Self::Swift,
        Self::Cpp,
        Self::Ruby,
        Self::Php,
        Self::Dart,
        Self::Kotlin,
        Self::Scala,
        Self::Elixir,
        Self::Ballerina,
        Self::ObjectiveC,
        Self::Erlang,
        Self::Ocaml,
        Self::CommonLisp,
        Self::Ada,
        Self::C,
        Self::Clojure,
        Self::Crystal,
        Self::Elm,
        Self::Gdscript,
        Self::Julia,
        Self::Nim,
        Self::Perl,
        Self::Powershell,
        Self::R,
        Self::Bash,
        Self::Haskell,
        Self::Lua,
    ];
}

/// Wire representation of a semantic value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WireValueKind {
    String,
    Bytes,
    SignedInteger,
    UnsignedInteger,
    Boolean,
    Timestamp,
    Enum,
    Message,
    Oneof,
}

/// A semantic rule that generated APIs must expose when the target can do so.
///
/// Rules are intentionally identifiers rather than free-form generator
/// instructions.  Their meaning is stable, and each language profile below
/// states the construct used to enforce them.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SemanticRule {
    NonEmpty,
    Utf8,
    NonNegative,
    StrictlyPositive,
    FixedLength(u16),
    MaxBytes(u32),
    MaxItems(u32),
    BoundedInteger { min: i64, max: i64 },
    Sha256Digest,
    Immutable,
    Monotonic,
    CanonicalResourceName,
    ExactOneof,
    ExplicitPresence,
    PreserveUnknownEnum,
    PreserveUnknownOneof,
}

/// Canonical limits shared by the Rust contract model and native family
/// implementations.  Native crates must reference these values rather than
/// repeating literals so generated clients and runtime validation cannot drift.
pub const MACHINE_PAGE_LIMIT_MAX: u32 = 256;
pub const MACHINE_EVENT_PAGE_LIMIT_MAX: u32 = 1_024;
/// Stream operation limits shared with the native validator.
pub const STREAM_MAX_RECORD_BYTES: u32 = 65_536;
pub const STREAM_MAX_COMMAND_BYTES: u32 = 1_056_768;
/// Objects multipart ordering and cardinality bound.
pub const OBJECTS_MAX_MULTIPART_ITEMS: u32 = 10_000;

/// One Rust-owned semantic type.  `rust_name` is documentation and generator
/// provenance; the wire kind remains explicit so a projection cannot change
/// the protocol while adding a stronger local type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SemanticType {
    pub id: &'static str,
    pub rust_name: &'static str,
    pub wire_kind: WireValueKind,
    pub rules: &'static [SemanticRule],
}

/// Explicit link from a Rust model field name to its semantic type.
///
/// The protobuf field keeps its original wire name and number; this table adds
/// the source-owned meaning that generated targets use for nominal wrappers,
/// refinement constructors, and exhaustive/open union views.  A family scope
/// prevents an incidental field name such as `name` or `key` from acquiring a
/// meaning from another contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FieldSemanticType {
    pub family: &'static str,
    pub field: &'static str,
    pub semantic_type: &'static str,
}

/// Exact public protobuf location for a semantic field.
///
/// The semantic field names above are stable Rust vocabulary.  This second
/// Rust-owned table records where each value is actually constructed or
/// decoded in the public SDK facade, so a generator cannot satisfy typing
/// qualification with a detached sidecar model.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicFieldDirection {
    Request,
    Response,
    NestedMessage,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PublicFieldBinding {
    pub family: &'static str,
    pub field: &'static str,
    pub semantic_type: &'static str,
    pub module: &'static str,
    pub message: &'static str,
    pub wire_field: &'static str,
    pub direction: PublicFieldDirection,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicNestedFieldKind {
    Text,
    Message(&'static str),
}

/// Rust-owned descriptions of production requests whose nested message has a
/// refined field.  These routes keep the nested semantic wrapper in the
/// actual public client signature instead of leaving it as a detached helper.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PublicNestedRoute {
    pub family: &'static str,
    pub operation: &'static str,
    pub module: &'static str,
    pub request_message: &'static str,
    pub nested_message: &'static str,
    pub nested_field: &'static str,
    pub semantic_field: &'static str,
    pub client_attribute: &'static str,
    pub rpc: &'static str,
    pub response: &'static str,
    pub fields: &'static [(&'static str, PublicNestedFieldKind)],
}

impl PublicFieldBinding {
    /// Service client attribute used by generated remote facades.
    pub fn client_attribute(self) -> Option<&'static str> {
        match (self.module, self.message) {
            ("actors", "InvokeActorRequest") => Some("actors"),
            ("workers", "SelectDeploymentRequest")
            | ("workers", "InspectJobRequest")
            | ("workers", "InvokeVersionRequest") => Some("workers"),
            ("stream", "AppendRequest")
            | ("stream", "ForkRequest")
            | ("stream", "ReadRequest")
            | ("stream", "ReadCommitRequest") => Some("stream"),
            ("objects", "GetObjectRequest") | ("objects", "ListObjectsRequest") => Some("objects"),
            ("objects", "ListPartsRequest") => Some("multipart"),
            ("inference", "InspectRunRequest") => Some("inference.runs"),
            ("inference", "InspectContextRequest") => Some("inference.contexts"),
            ("inference", "InspectWarmRequest") => Some("inference.warm_contexts"),
            ("inference", "InspectEvaluationRequest") => Some("inference.evaluations"),
            ("machines", "CreateMachineRequest")
            | ("machines", "InspectMachineRequest")
            | ("machines", "InspectCheckpointRequest")
            | ("machines", "OperationRequest")
            | ("machines", "ListMachinesRequest")
            | ("machines", "EventsRequest")
            | ("machines", "QualifyImageRequest") => Some("machines"),
            ("filesystem", "ReadRequest") => Some("filesystem"),
            _ => None,
        }
    }

    /// RPC method corresponding to the concrete request message.
    pub fn rpc(self) -> Option<&'static str> {
        match (self.module, self.message) {
            (_, "InvokeActorRequest") => Some("InvokeActor"),
            (_, "SelectDeploymentRequest") => Some("SelectDeployment"),
            (_, "InspectJobRequest") => Some("InspectJob"),
            (_, "InvokeVersionRequest") => Some("InvokeVersion"),
            (_, "AppendRequest") => Some("Append"),
            (_, "ForkRequest") => Some("Fork"),
            (_, "ReadRequest") => Some("Read"),
            (_, "ReadCommitRequest") => Some("ReadCommit"),
            (_, "GetObjectRequest") => Some("GetObject"),
            (_, "ListObjectsRequest") => Some("ListObjects"),
            (_, "ListPartsRequest") => Some("ListParts"),
            (_, "InspectRunRequest") => Some("Inspect"),
            (_, "InspectContextRequest") => Some("Inspect"),
            (_, "InspectWarmRequest") => Some("Inspect"),
            (_, "InspectEvaluationRequest") => Some("Inspect"),
            (_, "CreateMachineRequest") => Some("Create"),
            (_, "InspectMachineRequest") => Some("InspectMachine"),
            (_, "InspectCheckpointRequest") => Some("InspectCheckpoint"),
            (_, "OperationRequest") => Some("InspectOperation"),
            (_, "ListMachinesRequest") => Some("ListMachines"),
            (_, "EventsRequest") => Some("Events"),
            (_, "QualifyImageRequest") => Some("QualifyImage"),
            _ => None,
        }
    }
}

/// A Rust-owned discriminated union projection.  The open `unknown` arm is
/// part of the wire contract, while the `known` arm gives every target a
/// statically visible payload-bearing variant for values understood by the
/// current SDK revision.  Targets may add family-specific known arms later,
/// but they must preserve this open pair when decoding newer senders.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WireUnionVariant {
    pub union: &'static str,
    pub variant: &'static str,
    pub tag: &'static str,
    pub payload_wire_kind: WireValueKind,
}

const NON_EMPTY_UTF8: &[SemanticRule] = &[SemanticRule::NonEmpty, SemanticRule::Utf8];
const NON_EMPTY_BYTES: &[SemanticRule] = &[SemanticRule::NonEmpty];
const UUID_BYTES: &[SemanticRule] = &[SemanticRule::NonEmpty, SemanticRule::FixedLength(16)];
const DIGEST: &[SemanticRule] = &[SemanticRule::FixedLength(32), SemanticRule::Sha256Digest];
const FIXED_32_BYTES: &[SemanticRule] = &[SemanticRule::FixedLength(32)];
const NON_NEGATIVE: &[SemanticRule] = &[SemanticRule::NonNegative];
const PRESENT_ONEOF: &[SemanticRule] =
    &[SemanticRule::ExactOneof, SemanticRule::PreserveUnknownOneof];
const IMMUTABLE_MESSAGE: &[SemanticRule] = &[SemanticRule::Immutable];

/// Shared semantic vocabulary used by all families.  Family models refer to
/// these names from their Rust validation metadata instead of introducing a
/// second target-language contract.
pub const SEMANTIC_TYPES: &[SemanticType] = &[
    SemanticType {
        id: "actor_id",
        rust_name: "ActorId",
        wire_kind: WireValueKind::String,
        rules: NON_EMPTY_UTF8,
    },
    SemanticType {
        id: "method",
        rust_name: "MethodName",
        wire_kind: WireValueKind::String,
        rules: NON_EMPTY_UTF8,
    },
    SemanticType {
        id: "path",
        rust_name: "ResourcePath",
        wire_kind: WireValueKind::String,
        rules: NON_EMPTY_UTF8,
    },
    SemanticType {
        id: "source",
        rust_name: "SourceName",
        wire_kind: WireValueKind::String,
        rules: NON_EMPTY_UTF8,
    },
    SemanticType {
        id: "destination",
        rust_name: "DestinationName",
        wire_kind: WireValueKind::String,
        rules: NON_EMPTY_UTF8,
    },
    SemanticType {
        id: "bucket_name",
        rust_name: "BucketName",
        wire_kind: WireValueKind::String,
        rules: NON_EMPTY_UTF8,
    },
    SemanticType {
        id: "object_key",
        rust_name: "ObjectKey",
        wire_kind: WireValueKind::String,
        rules: NON_EMPTY_UTF8,
    },
    SemanticType {
        id: "alias",
        rust_name: "VersionAlias",
        wire_kind: WireValueKind::String,
        rules: NON_EMPTY_UTF8,
    },
    SemanticType {
        id: "job_id",
        rust_name: "JobId",
        wire_kind: WireValueKind::String,
        rules: NON_EMPTY_UTF8,
    },
    // Machines identities are UUID-backed prost bytes, not UTF-8 strings.
    // Keep this explicit so generated clients cannot accept values that the
    // native Rust facade would reject or silently reinterpret.
    SemanticType {
        id: "machine_id",
        rust_name: "MachineId",
        wire_kind: WireValueKind::Bytes,
        rules: UUID_BYTES,
    },
    SemanticType {
        id: "operation_id",
        rust_name: "OperationId",
        wire_kind: WireValueKind::Bytes,
        rules: UUID_BYTES,
    },
    SemanticType {
        id: "workspace_id",
        rust_name: "WorkspaceId",
        wire_kind: WireValueKind::Bytes,
        rules: UUID_BYTES,
    },
    SemanticType {
        id: "checkpoint_id",
        rust_name: "CheckpointId",
        wire_kind: WireValueKind::Bytes,
        rules: UUID_BYTES,
    },
    SemanticType {
        id: "idempotency_key_bytes",
        rust_name: "IdempotencyKeyBytes",
        wire_kind: WireValueKind::Bytes,
        rules: NON_EMPTY_BYTES,
    },
    SemanticType {
        id: "idempotency_key_text",
        rust_name: "IdempotencyKeyText",
        wire_kind: WireValueKind::String,
        rules: NON_EMPTY_UTF8,
    },
    SemanticType {
        id: "idempotency_key_message",
        rust_name: "IdempotencyKey",
        wire_kind: WireValueKind::Message,
        rules: UUID_BYTES,
    },
    SemanticType {
        id: "opaque_text",
        rust_name: "OpaqueText",
        wire_kind: WireValueKind::String,
        rules: NON_EMPTY_UTF8,
    },
    SemanticType {
        id: "upload_id",
        rust_name: "UploadId",
        wire_kind: WireValueKind::String,
        rules: NON_EMPTY_UTF8,
    },
    SemanticType {
        id: "version_sha256",
        rust_name: "Sha256Digest",
        wire_kind: WireValueKind::Bytes,
        rules: DIGEST,
    },
    SemanticType {
        id: "sha256_digest",
        rust_name: "Sha256Digest",
        wire_kind: WireValueKind::Bytes,
        rules: DIGEST,
    },
    SemanticType {
        id: "revision_digest",
        rust_name: "RevisionDigest",
        wire_kind: WireValueKind::Bytes,
        rules: FIXED_32_BYTES,
    },
    SemanticType {
        id: "immutable_image",
        rust_name: "Image",
        wire_kind: WireValueKind::Message,
        rules: IMMUTABLE_MESSAGE,
    },
    SemanticType {
        id: "revision",
        rust_name: "Revision",
        wire_kind: WireValueKind::UnsignedInteger,
        rules: NON_NEGATIVE,
    },
    SemanticType {
        id: "run_id",
        rust_name: "RunId",
        wire_kind: WireValueKind::Bytes,
        rules: &[SemanticRule::FixedLength(16)],
    },
    SemanticType {
        id: "evaluation_id",
        rust_name: "EvaluationId",
        wire_kind: WireValueKind::Bytes,
        rules: &[SemanticRule::FixedLength(16)],
    },
    SemanticType {
        id: "page_limit",
        rust_name: "PageLimit",
        wire_kind: WireValueKind::UnsignedInteger,
        rules: &[SemanticRule::StrictlyPositive, SemanticRule::MaxItems(1000)],
    },
    SemanticType {
        id: "stream_page_limit",
        rust_name: "StreamPageLimit",
        wire_kind: WireValueKind::UnsignedInteger,
        rules: &[SemanticRule::StrictlyPositive, SemanticRule::MaxItems(1024)],
    },
    SemanticType {
        id: "machine_page_limit",
        rust_name: "MachinePageLimit",
        wire_kind: WireValueKind::UnsignedInteger,
        rules: &[
            SemanticRule::StrictlyPositive,
            SemanticRule::MaxItems(MACHINE_PAGE_LIMIT_MAX),
        ],
    },
    SemanticType {
        id: "machine_event_page_limit",
        rust_name: "MachineEventPageLimit",
        wire_kind: WireValueKind::UnsignedInteger,
        rules: &[
            SemanticRule::StrictlyPositive,
            SemanticRule::MaxItems(MACHINE_EVENT_PAGE_LIMIT_MAX),
        ],
    },
    SemanticType {
        id: "commit_id",
        rust_name: "CommitId",
        wire_kind: WireValueKind::Bytes,
        rules: &[SemanticRule::NonEmpty],
    },
    SemanticType {
        id: "enum_value",
        rust_name: "OpenEnumValue",
        wire_kind: WireValueKind::Enum,
        rules: &[SemanticRule::PreserveUnknownEnum],
    },
    SemanticType {
        id: "oneof_arm",
        rust_name: "WireChoice",
        wire_kind: WireValueKind::Oneof,
        rules: PRESENT_ONEOF,
    },
];

/// Rust-owned field mappings consumed by every target generator.
///
/// Fields absent from this table are ordinary wire values.  A generator must
/// fail closed if it sees a constrained validation rule without a mapping;
/// it may never silently project a constrained field as an unbranded scalar.
pub const FIELD_SEMANTIC_TYPES: &[FieldSemanticType] = &[
    FieldSemanticType {
        family: "actors",
        field: "actor_id",
        semantic_type: "actor_id",
    },
    FieldSemanticType {
        family: "actors",
        field: "method",
        semantic_type: "method",
    },
    FieldSemanticType {
        family: "workers",
        field: "alias",
        semantic_type: "alias",
    },
    FieldSemanticType {
        family: "workers",
        field: "version_sha256",
        semantic_type: "version_sha256",
    },
    FieldSemanticType {
        family: "workers",
        field: "idempotency_key",
        semantic_type: "idempotency_key_text",
    },
    FieldSemanticType {
        family: "workers",
        field: "job_id",
        semantic_type: "job_id",
    },
    FieldSemanticType {
        family: "workers",
        field: "method",
        semantic_type: "method",
    },
    FieldSemanticType {
        family: "stream",
        field: "idempotency_key",
        semantic_type: "idempotency_key_bytes",
    },
    FieldSemanticType {
        family: "stream",
        field: "path",
        semantic_type: "path",
    },
    FieldSemanticType {
        family: "stream",
        field: "source",
        semantic_type: "source",
    },
    FieldSemanticType {
        family: "stream",
        field: "destination",
        semantic_type: "destination",
    },
    FieldSemanticType {
        family: "stream",
        field: "limit",
        semantic_type: "stream_page_limit",
    },
    FieldSemanticType {
        family: "stream",
        field: "commit_id",
        semantic_type: "commit_id",
    },
    FieldSemanticType {
        family: "objects",
        field: "key",
        semantic_type: "object_key",
    },
    FieldSemanticType {
        family: "objects",
        field: "etag",
        semantic_type: "opaque_text",
    },
    FieldSemanticType {
        family: "objects",
        field: "idempotency_key",
        semantic_type: "idempotency_key_text",
    },
    FieldSemanticType {
        family: "objects",
        field: "upload_id",
        semantic_type: "upload_id",
    },
    FieldSemanticType {
        family: "objects",
        field: "page_size",
        semantic_type: "page_limit",
    },
    FieldSemanticType {
        family: "inference",
        field: "run_id",
        semantic_type: "run_id",
    },
    FieldSemanticType {
        family: "inference",
        field: "revision",
        semantic_type: "revision_digest",
    },
    FieldSemanticType {
        family: "inference",
        field: "commitment",
        semantic_type: "sha256_digest",
    },
    FieldSemanticType {
        family: "inference",
        field: "evaluation_id",
        semantic_type: "evaluation_id",
    },
    FieldSemanticType {
        family: "inference",
        field: "spec_digest",
        semantic_type: "sha256_digest",
    },
    FieldSemanticType {
        family: "machines",
        field: "image",
        semantic_type: "immutable_image",
    },
    FieldSemanticType {
        family: "machines",
        field: "idempotency_key",
        semantic_type: "idempotency_key_message",
    },
    FieldSemanticType {
        family: "machines",
        field: "machine_id",
        semantic_type: "machine_id",
    },
    FieldSemanticType {
        family: "machines",
        field: "checkpoint_id",
        semantic_type: "checkpoint_id",
    },
    FieldSemanticType {
        family: "machines",
        field: "operation_id",
        semantic_type: "operation_id",
    },
    FieldSemanticType {
        family: "machines",
        field: "page_limit",
        semantic_type: "machine_page_limit",
    },
    FieldSemanticType {
        family: "machines",
        field: "event_page_limit",
        semantic_type: "machine_event_page_limit",
    },
    FieldSemanticType {
        family: "filesystem",
        field: "path",
        semantic_type: "path",
    },
    FieldSemanticType {
        family: "harness",
        field: "path",
        semantic_type: "path",
    },
];

/// Every mapped semantic field is attached to a real public request, response,
/// or nested protobuf message.  Keep this list in Rust beside the semantic
/// vocabulary; generated targets use it to emit lossless `to_wire` and
/// `from_wire` bridges.
pub const PUBLIC_FIELD_BINDINGS: &[PublicFieldBinding] = &[
    PublicFieldBinding {
        family: "actors",
        field: "actor_id",
        semantic_type: "actor_id",
        module: "actors",
        message: "InvokeActorRequest",
        wire_field: "actor_id",
        direction: PublicFieldDirection::Request,
    },
    PublicFieldBinding {
        family: "actors",
        field: "method",
        semantic_type: "method",
        module: "actors",
        message: "InvokeActorRequest",
        wire_field: "method",
        direction: PublicFieldDirection::Request,
    },
    PublicFieldBinding {
        family: "workers",
        field: "alias",
        semantic_type: "alias",
        module: "workers",
        message: "SelectDeploymentRequest",
        wire_field: "alias",
        direction: PublicFieldDirection::Request,
    },
    PublicFieldBinding {
        family: "workers",
        field: "version_sha256",
        semantic_type: "version_sha256",
        module: "workers",
        message: "SelectDeploymentRequest",
        wire_field: "version_sha256",
        direction: PublicFieldDirection::Request,
    },
    PublicFieldBinding {
        family: "workers",
        field: "idempotency_key",
        semantic_type: "idempotency_key_text",
        module: "workers",
        message: "SelectDeploymentRequest",
        wire_field: "idempotency_key",
        direction: PublicFieldDirection::Request,
    },
    PublicFieldBinding {
        family: "workers",
        field: "job_id",
        semantic_type: "job_id",
        module: "workers",
        message: "InspectJobRequest",
        wire_field: "job_id",
        direction: PublicFieldDirection::Request,
    },
    PublicFieldBinding {
        family: "workers",
        field: "method",
        semantic_type: "method",
        module: "workers",
        message: "InvokeVersionRequest",
        wire_field: "method",
        direction: PublicFieldDirection::Request,
    },
    PublicFieldBinding {
        family: "stream",
        field: "idempotency_key",
        semantic_type: "idempotency_key_bytes",
        module: "stream",
        message: "AppendRequest",
        wire_field: "idempotency_key",
        direction: PublicFieldDirection::Request,
    },
    PublicFieldBinding {
        family: "stream",
        field: "path",
        semantic_type: "path",
        module: "stream",
        message: "AppendRequest",
        wire_field: "path",
        direction: PublicFieldDirection::Request,
    },
    PublicFieldBinding {
        family: "stream",
        field: "source",
        semantic_type: "source",
        module: "stream",
        message: "ForkRequest",
        wire_field: "source",
        direction: PublicFieldDirection::Request,
    },
    PublicFieldBinding {
        family: "stream",
        field: "destination",
        semantic_type: "destination",
        module: "stream",
        message: "ForkRequest",
        wire_field: "destination",
        direction: PublicFieldDirection::Request,
    },
    PublicFieldBinding {
        family: "stream",
        field: "limit",
        semantic_type: "stream_page_limit",
        module: "stream",
        message: "ReadRequest",
        wire_field: "limit",
        direction: PublicFieldDirection::Request,
    },
    PublicFieldBinding {
        family: "stream",
        field: "commit_id",
        semantic_type: "commit_id",
        module: "stream",
        message: "ReadCommitRequest",
        wire_field: "commit_id",
        direction: PublicFieldDirection::Request,
    },
    PublicFieldBinding {
        family: "objects",
        field: "key",
        semantic_type: "object_key",
        module: "objects",
        message: "GetObjectRequest",
        wire_field: "object_key",
        direction: PublicFieldDirection::Request,
    },
    PublicFieldBinding {
        family: "objects",
        field: "etag",
        semantic_type: "opaque_text",
        module: "objects",
        message: "ObjectInfo",
        wire_field: "etag",
        direction: PublicFieldDirection::Response,
    },
    PublicFieldBinding {
        family: "objects",
        field: "idempotency_key",
        semantic_type: "idempotency_key_text",
        module: "objects",
        message: "MutationIdentity",
        wire_field: "idempotency_key",
        direction: PublicFieldDirection::NestedMessage,
    },
    PublicFieldBinding {
        family: "objects",
        field: "upload_id",
        semantic_type: "upload_id",
        module: "objects",
        message: "ListPartsRequest",
        wire_field: "upload_id",
        direction: PublicFieldDirection::Request,
    },
    PublicFieldBinding {
        family: "objects",
        field: "page_size",
        semantic_type: "page_limit",
        module: "objects",
        message: "ListObjectsRequest",
        wire_field: "page_size",
        direction: PublicFieldDirection::Request,
    },
    PublicFieldBinding {
        family: "inference",
        field: "run_id",
        semantic_type: "run_id",
        module: "inference",
        message: "InspectRunRequest",
        wire_field: "run_id",
        direction: PublicFieldDirection::Request,
    },
    PublicFieldBinding {
        family: "inference",
        field: "revision",
        semantic_type: "revision_digest",
        module: "inference",
        message: "InspectContextRequest",
        wire_field: "revision",
        direction: PublicFieldDirection::Request,
    },
    PublicFieldBinding {
        family: "inference",
        field: "commitment",
        semantic_type: "sha256_digest",
        module: "inference",
        message: "InspectWarmRequest",
        wire_field: "commitment",
        direction: PublicFieldDirection::Request,
    },
    PublicFieldBinding {
        family: "inference",
        field: "evaluation_id",
        semantic_type: "evaluation_id",
        module: "inference",
        message: "InspectEvaluationRequest",
        wire_field: "evaluation_id",
        direction: PublicFieldDirection::Request,
    },
    PublicFieldBinding {
        family: "inference",
        field: "spec_digest",
        semantic_type: "sha256_digest",
        module: "inference",
        message: "EvaluationSpec",
        wire_field: "spec_digest",
        direction: PublicFieldDirection::NestedMessage,
    },
    PublicFieldBinding {
        family: "machines",
        field: "image",
        semantic_type: "immutable_image",
        module: "machines",
        message: "CreateMachineRequest",
        wire_field: "image",
        direction: PublicFieldDirection::Request,
    },
    PublicFieldBinding {
        family: "machines",
        field: "image",
        semantic_type: "immutable_image",
        module: "machines",
        message: "QualifyImageRequest",
        wire_field: "image",
        direction: PublicFieldDirection::Request,
    },
    PublicFieldBinding {
        family: "machines",
        field: "idempotency_key",
        semantic_type: "idempotency_key_message",
        module: "machines",
        message: "CreateMachineRequest",
        wire_field: "idempotency_key",
        direction: PublicFieldDirection::Request,
    },
    PublicFieldBinding {
        family: "machines",
        field: "machine_id",
        semantic_type: "machine_id",
        module: "machines",
        message: "InspectMachineRequest",
        wire_field: "machine",
        direction: PublicFieldDirection::Request,
    },
    PublicFieldBinding {
        family: "machines",
        field: "checkpoint_id",
        semantic_type: "checkpoint_id",
        module: "machines",
        message: "InspectCheckpointRequest",
        wire_field: "checkpoint",
        direction: PublicFieldDirection::Request,
    },
    PublicFieldBinding {
        family: "machines",
        field: "operation_id",
        semantic_type: "operation_id",
        module: "machines",
        message: "OperationRequest",
        wire_field: "operation",
        direction: PublicFieldDirection::Request,
    },
    PublicFieldBinding {
        family: "machines",
        field: "page_limit",
        semantic_type: "machine_page_limit",
        module: "machines",
        message: "ListMachinesRequest",
        wire_field: "limit",
        direction: PublicFieldDirection::Request,
    },
    PublicFieldBinding {
        family: "machines",
        field: "event_page_limit",
        semantic_type: "machine_event_page_limit",
        module: "machines",
        message: "EventsRequest",
        wire_field: "limit",
        direction: PublicFieldDirection::Request,
    },
    PublicFieldBinding {
        family: "filesystem",
        field: "path",
        semantic_type: "path",
        module: "filesystem",
        message: "ReadRequest",
        wire_field: "path",
        direction: PublicFieldDirection::Request,
    },
    PublicFieldBinding {
        family: "harness",
        field: "path",
        semantic_type: "path",
        module: "harness",
        message: "FileRef",
        wire_field: "normalized_path",
        direction: PublicFieldDirection::Response,
    },
];

const CREATE_BUCKET_FIELDS: &[(&str, PublicNestedFieldKind)] =
    &[("name", PublicNestedFieldKind::Text)];
const CREATE_EVALUATION_FIELDS: &[(&str, PublicNestedFieldKind)] = &[
    (
        "identity",
        PublicNestedFieldKind::Message("RequestIdentity"),
    ),
    ("spec", PublicNestedFieldKind::Message("EvaluationSpec")),
];

pub const PUBLIC_NESTED_ROUTES: &[PublicNestedRoute] = &[
    PublicNestedRoute {
        family: "objects",
        operation: "create_bucket",
        module: "objects",
        request_message: "CreateBucketRequest",
        nested_message: "MutationIdentity",
        nested_field: "mutation",
        semantic_field: "idempotency_key",
        client_attribute: "buckets",
        rpc: "CreateBucket",
        response: "Bucket",
        fields: CREATE_BUCKET_FIELDS,
    },
    PublicNestedRoute {
        family: "inference",
        operation: "create_evaluation",
        module: "inference",
        request_message: "CreateEvaluationRequest",
        nested_message: "EvaluationSpec",
        nested_field: "spec",
        semantic_field: "spec_digest",
        client_attribute: "inference.evaluations",
        rpc: "Create",
        response: "EvaluationView",
        fields: CREATE_EVALUATION_FIELDS,
    },
];

/// Discriminants and payload kinds for every open union emitted by the type
/// policy.  This is deliberately authored beside the Rust semantic model so
/// target generators cannot silently collapse a oneof into `any`/`object`.
pub const WIRE_UNION_VARIANTS: &[WireUnionVariant] = &[
    WireUnionVariant {
        union: "wire_choice",
        variant: "KnownOneof",
        tag: "known",
        payload_wire_kind: WireValueKind::Message,
    },
    WireUnionVariant {
        union: "wire_choice",
        variant: "UnknownOneof",
        tag: "unknown",
        payload_wire_kind: WireValueKind::Bytes,
    },
];

/// How a target should expose optional fields, unions and open enums.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TypeProjectionProfile {
    pub language: TypePolicyLanguage,
    /// Nominal type mechanism for semantic strings, bytes and integer IDs.
    pub nominal_types: &'static str,
    /// Exhaustive/discriminated union mechanism, if the language provides one.
    pub unions: &'static str,
    /// Static refinement mechanism for `SemanticRule` values.
    pub refinements: &'static str,
    /// Presence/nullability representation; absence and explicit defaults must
    /// remain distinguishable whenever the descriptor says they are.
    pub presence: &'static str,
    /// Representation of unknown enum numbers and oneof arms.
    pub unknown_values: &'static str,
    /// Numeric representation that preserves protobuf uint64/int64 values.
    pub integers: &'static str,
    /// Static checker/compiler invoked by the qualification lane.
    pub checker: &'static str,
}

/// Rust-owned strongest practical type profile for every inventoried language.
///
/// The wording is intentionally generator-facing and testable: it names the
/// language feature to emit, not a suggestion to hand-author a facade.
pub const TYPE_PROJECTION_PROFILES: &[TypeProjectionProfile] = &[
    TypeProjectionProfile {
        language: TypePolicyLanguage::Rust,
        nominal_types: "newtype structs with private fields",
        unions: "enum with non_exhaustive unknown arm",
        refinements: "TryFrom constructors and typed errors",
        presence: "Option<T> plus prost presence",
        unknown_values: "raw enum number and UnknownOneof payload",
        integers: "u64/i64",
        checker: "cargo check + clippy",
    },
    TypeProjectionProfile {
        language: TypePolicyLanguage::TypeScript,
        nominal_types: "branded string/number/Uint8Array types",
        unions: "discriminated readonly unions with never exhaustiveness",
        refinements: "user-defined asserts and branded constructors",
        presence: "exactOptionalPropertyTypes and explicit null",
        unknown_values: "Unknown(raw number) enum arm and UnknownOneof object",
        integers: "bigint for uint64; never number",
        checker: "tsc --strict + eslint type rules",
    },
    TypeProjectionProfile {
        language: TypePolicyLanguage::Python,
        nominal_types: "NewType plus Annotated validators",
        unions: "Literal discriminants, TypedDict and Never",
        refinements: "Annotated metadata with runtime constructor",
        presence: "NotRequired versus Optional under strict mode",
        unknown_values: "IntEnum fallback carrying raw integer and UnknownOneof",
        integers: "int with range validators; no float coercion",
        checker: "pyright strict + mypy strict",
    },
    TypeProjectionProfile {
        language: TypePolicyLanguage::Go,
        nominal_types: "defined string/byte/integer types with constructors",
        unions: "tagged structs and sealed interfaces where possible",
        refinements: "constructor errors and validation methods",
        presence: "pointers/wrappers for explicit presence",
        unknown_values: "enum raw value plus UnknownOneof bytes",
        integers: "uint64/int64; never float",
        checker: "go vet + go test + staticcheck",
    },
    TypeProjectionProfile {
        language: TypePolicyLanguage::Java,
        nominal_types: "records/value wrappers and private constructors",
        unions: "sealed interfaces plus records",
        refinements: "factory methods returning typed validation errors",
        presence: "optional wrappers only where presence is explicit",
        unknown_values: "UNRECOGNIZED plus raw number and UnknownOneof",
        integers: "long with unsigned helper/value wrapper",
        checker: "javac -Xlint + Error Prone",
    },
    TypeProjectionProfile {
        language: TypePolicyLanguage::CSharp,
        nominal_types: "readonly record structs and required members",
        unions: "sealed record hierarchy with exhaustive switch",
        refinements: "static factories and nullable flow analysis",
        presence: "nullable annotations plus Optional wrapper",
        unknown_values: "raw enum integer and UnknownOneof record",
        integers: "ulong/long; checked conversions",
        checker: "dotnet build nullable=enable + analyzers",
    },
    TypeProjectionProfile {
        language: TypePolicyLanguage::Swift,
        nominal_types: "struct wrappers with failable initializers",
        unions: "enum associated values",
        refinements: "init? / throws and validation types",
        presence: "Optional only for explicit presence",
        unknown_values: "unknown(rawValue:) enum case and unknown oneof",
        integers: "UInt64/Int64",
        checker: "swiftc -warnings-as-errors",
    },
    TypeProjectionProfile {
        language: TypePolicyLanguage::Cpp,
        nominal_types: "strong typedef structs and explicit constructors",
        unions: "std::variant with Unknown alternative",
        refinements: "expected<T, Error> and constexpr checks",
        presence: "std::optional only for explicit presence",
        unknown_values: "raw enum value and variant UnknownOneof",
        integers: "std::uint64_t/std::int64_t",
        checker: "clang++ -std=c++20 -Werror",
    },
    TypeProjectionProfile {
        language: TypePolicyLanguage::Ruby,
        nominal_types: "value objects with private constructors",
        unions: "tagged immutable value objects",
        refinements: "constructor validation and typed result objects",
        presence: "explicit sentinel versus nil",
        unknown_values: "open enum raw integer wrapper and unknown oneof",
        integers: "Integer with no narrowing",
        checker: "ruby syntax + runtime contract tests",
    },
    TypeProjectionProfile {
        language: TypePolicyLanguage::Php,
        nominal_types: "readonly value objects and enums",
        unions: "sealed-style interfaces plus readonly records",
        refinements: "named constructors and exceptions",
        presence: "nullability plus explicit presence wrapper",
        unknown_values: "Unknown(raw enum integer) and UnknownOneof",
        integers: "int with string fallback for uint64",
        checker: "phpstan level max + PHPUnit",
    },
    TypeProjectionProfile {
        language: TypePolicyLanguage::Dart,
        nominal_types: "sealed class value objects",
        unions: "sealed classes with pattern matching",
        refinements: "factory constructors and Result types",
        presence: "nullable only when descriptor permits",
        unknown_values: "unknown raw enum integer and UnknownOneof subclass",
        integers: "int (64-bit) with checked JSON",
        checker: "dart analyze --fatal-infos + test",
    },
    TypeProjectionProfile {
        language: TypePolicyLanguage::Kotlin,
        nominal_types: "@JvmInline value classes",
        unions: "sealed interfaces/data classes",
        refinements: "require/factory returning typed failures",
        presence: "nullable only for explicit presence",
        unknown_values: "UNRECOGNIZED(raw) and UnknownOneof",
        integers: "ULong/Long with protobuf adapters",
        checker: "kotlinc + detekt",
    },
    TypeProjectionProfile {
        language: TypePolicyLanguage::Scala,
        nominal_types: "opaque types and case classes",
        unions: "sealed traits with case classes",
        refinements: "refined constructors and Either",
        presence: "Option only for explicit presence",
        unknown_values: "open enum Unknown case carrying raw integer",
        integers: "Long with UInt64 value class",
        checker: "scalac -Xfatal-warnings + munit",
    },
    TypeProjectionProfile {
        language: TypePolicyLanguage::Elixir,
        nominal_types: "typed structs and opaque typespecs",
        unions: "tagged tuples with @type",
        refinements: "guards and changesets",
        presence: "absent key versus nil",
        unknown_values: "raw enum integer and unknown tagged tuple",
        integers: "integer with range guards",
        checker: "mix format + dialyzer + ExUnit",
    },
    TypeProjectionProfile {
        language: TypePolicyLanguage::Ballerina,
        nominal_types: "distinct types and readonly records",
        unions: "typed unions with match exhaustiveness",
        refinements: "distinct constructors and error values",
        presence: "optional fields versus nil",
        unknown_values: "open enum raw integer and unknown union member",
        integers: "int/decimal with explicit uint64 codec",
        checker: "bal build + tests",
    },
    TypeProjectionProfile {
        language: TypePolicyLanguage::ObjectiveC,
        nominal_types: "class wrappers and NS_SWIFT_NAME annotations",
        unions: "tagged classes with Unknown subclass",
        refinements: "nullable designated initializers",
        presence: "nullable annotations plus presence bit",
        unknown_values: "raw enum number and unknown object",
        integers: "uint64_t/int64_t",
        checker: "clang -Weverything",
    },
    TypeProjectionProfile {
        language: TypePolicyLanguage::Erlang,
        nominal_types: "opaque types and dialyzer specs",
        unions: "tagged tuples",
        refinements: "guards and error tuples",
        presence: "absent map key versus undefined",
        unknown_values: "raw enum integer and unknown tuple",
        integers: "integer guards",
        checker: "rebar3 dialyzer + EUnit",
    },
    TypeProjectionProfile {
        language: TypePolicyLanguage::Ocaml,
        nominal_types: "private type aliases and modules",
        unions: "polymorphic variants or closed variants",
        refinements: "smart constructors returning result",
        presence: "option only for explicit presence",
        unknown_values: "open Unknown polymorphic variant carrying raw number",
        integers: "int64/int32 exact codecs",
        checker: "dune build + warnings-as-errors",
    },
    TypeProjectionProfile {
        language: TypePolicyLanguage::CommonLisp,
        nominal_types: "deftype plus constructor predicates",
        unions: "tagged structures",
        refinements: "type declarations and condition types",
        presence: "sentinel distinct from NIL",
        unknown_values: "raw enum integer and unknown structure",
        integers: "(unsigned-byte 64)/(signed-byte 64)",
        checker: "SBCL compile + runtime vectors",
    },
    TypeProjectionProfile {
        language: TypePolicyLanguage::Ada,
        nominal_types: "subtypes and private types",
        unions: "variant records",
        refinements: "range constraints and predicates",
        presence: "presence discriminant",
        unknown_values: "raw enum representation and unknown variant",
        integers: "Interfaces.Unsigned_64/Integer_64",
        checker: "gnatmake -gnatwae",
    },
    TypeProjectionProfile {
        language: TypePolicyLanguage::C,
        nominal_types: "opaque structs and constructor functions",
        unions: "tagged unions with explicit Unknown",
        refinements: "status-returning validators",
        presence: "has_field bit plus value",
        unknown_values: "raw enum integer and unknown union tag",
        integers: "uint64_t/int64_t",
        checker: "clang -Wall -Wextra -Werror",
    },
    TypeProjectionProfile {
        language: TypePolicyLanguage::Clojure,
        nominal_types: "specs and namespaced maps",
        unions: "tagged maps",
        refinements: "clojure.spec.alpha",
        presence: "contains? versus nil",
        unknown_values: "raw enum integer and unknown tag",
        integers: "long/bigint exact",
        checker: "clj-kondo + spec tests",
    },
    TypeProjectionProfile {
        language: TypePolicyLanguage::Crystal,
        nominal_types: "struct wrappers and aliases",
        unions: "union types with tagged wrappers",
        refinements: "constructor validation",
        presence: "Nil union only when explicit",
        unknown_values: "raw enum integer and unknown wrapper",
        integers: "UInt64/Int64",
        checker: "crystal build --error-trace",
    },
    TypeProjectionProfile {
        language: TypePolicyLanguage::Elm,
        nominal_types: "custom types and opaque modules",
        unions: "custom type constructors",
        refinements: "smart constructors returning Result",
        presence: "Maybe only for explicit presence",
        unknown_values: "Unknown(raw Int) constructor",
        integers: "Int with Bytes codec for uint64",
        checker: "elm make",
    },
    TypeProjectionProfile {
        language: TypePolicyLanguage::Gdscript,
        nominal_types: "typed classes and exported typed fields",
        unions: "tagged classes",
        refinements: "runtime validators with typed return",
        presence: "explicit has-field flag",
        unknown_values: "raw enum integer and unknown class",
        integers: "int with checked codec",
        checker: "godot --headless --check-only",
    },
    TypeProjectionProfile {
        language: TypePolicyLanguage::Julia,
        nominal_types: "primitive/value structs",
        unions: "Union types with tagged structs",
        refinements: "inner constructors and type assertions",
        presence: "Missing versus Nothing",
        unknown_values: "raw enum integer and unknown struct",
        integers: "UInt64/Int64",
        checker: "Julia package tests",
    },
    TypeProjectionProfile {
        language: TypePolicyLanguage::Nim,
        nominal_types: "distinct types",
        unions: "object variants",
        refinements: "validated constructors and concepts",
        presence: "Option[T] for explicit presence",
        unknown_values: "raw enum integer and unknown object",
        integers: "uint64/int64",
        checker: "nim check --warnings:all",
    },
    TypeProjectionProfile {
        language: TypePolicyLanguage::Perl,
        nominal_types: "checked constructors and immutable objects",
        unions: "tagged hashes/classes",
        refinements: "Type::Tiny constraints",
        presence: "exists versus undef",
        unknown_values: "raw enum integer and unknown tag",
        integers: "Math::Int64 or strings for uint64",
        checker: "perl -c + prove",
    },
    TypeProjectionProfile {
        language: TypePolicyLanguage::Powershell,
        nominal_types: "classes with typed properties",
        unions: "tagged classes",
        refinements: "ValidateScript/argument transformation",
        presence: "nullable property plus presence marker",
        unknown_values: "raw enum integer and unknown class",
        integers: "UInt64/Int64",
        checker: "PSScriptAnalyzer + Pester",
    },
    TypeProjectionProfile {
        language: TypePolicyLanguage::R,
        nominal_types: "S3/S4 classes with validators",
        unions: "S4 class unions",
        refinements: "validity methods",
        presence: "NULL versus explicit value",
        unknown_values: "raw enum integer and unknown S4 class",
        integers: "bit64::integer64",
        checker: "R CMD check",
    },
    TypeProjectionProfile {
        language: TypePolicyLanguage::Bash,
        nominal_types: "validated shell variables at process boundary",
        unions: "tagged JSON objects",
        refinements: "fail-closed validation functions",
        presence: "jq has() versus null",
        unknown_values: "raw enum integer and unknown JSON tag",
        integers: "decimal strings; never shell arithmetic for uint64",
        checker: "shellcheck + bats",
    },
    TypeProjectionProfile {
        language: TypePolicyLanguage::Haskell,
        nominal_types: "newtype and smart constructors",
        unions: "ADTs, GADTs and pattern exhaustiveness",
        refinements: "refined types and smart constructors",
        presence: "Maybe only for explicit presence",
        unknown_values: "Unknown(raw Int) constructor and unknown ADT arm",
        integers: "Word64/Int64",
        checker: "GHC -Wall -Werror + hlint",
    },
    TypeProjectionProfile {
        language: TypePolicyLanguage::Lua,
        nominal_types: "validated userdata/table constructors",
        unions: "tagged tables",
        refinements: "runtime constructors",
        presence: "key absence versus nil",
        unknown_values: "raw enum integer and unknown table tag",
        integers: "decimal strings for uint64",
        checker: "luacheck + runtime vectors",
    },
];

/// Find the profile for a language, allowing generators to fail closed when a
/// newly inventoried target has no type policy yet.
pub fn type_projection_profile(language: TypePolicyLanguage) -> &'static TypeProjectionProfile {
    TYPE_PROJECTION_PROFILES
        .iter()
        .find(|profile| profile.language == language)
        .expect("every inventoried language must have a Rust-owned type policy")
}

/// A public-surface defect found in a generated SDK artifact.
///
/// The checker deliberately looks only at public facade files.  Protobuf
/// messages are valid at a private transport boundary, but leaking them from
/// a public convenience method defeats the Rust-owned semantic policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GeneratedSurfaceViolation {
    pub language: &'static str,
    pub path: String,
    pub line: usize,
    pub reason: &'static str,
}

/// Generated facade families emitted by the Rust product generator.  The
/// TypeScript package has a separate package generator and supplies its own
/// required list when it invokes the same audit.
pub const REQUIRED_PRODUCT_SURFACES: &[&str] =
    &["python", "go", "jvm", "csharp", "swift", "cpp"];

/// Check generated public facades for known type-erasing shapes.
///
/// This is intentionally source based: it runs immediately after generation,
/// before any language-specific compiler is invoked, and therefore catches a
/// facade that happens to compile while discarding Rust-owned type semantics.
/// The language generators remain responsible for producing the source; this
/// function only defines the Rust-owned acceptance boundary.
pub fn audit_generated_public_surfaces(
    artifact_root: &Path,
) -> Result<Vec<GeneratedSurfaceViolation>, String> {
    if !artifact_root.is_dir() {
        return Err(format!("generated artifact root does not exist: {}", artifact_root.display()));
    }

    let mut files = Vec::new();
    collect_surface_files(artifact_root, &mut files);
    let mut violations = Vec::new();

    for path in files {
        let Some(language) = surface_language(&path) else {
            continue;
        };
        let source = fs::read_to_string(&path)
            .map_err(|error| format!("read generated facade {}: {error}", path.display()))?;
        let relative = path
            .strip_prefix(artifact_root)
            .unwrap_or(&path)
            .display()
            .to_string();

        let imported_proto_names = if language == "typescript" {
            typescript_imported_proto_names(&source)
        } else {
            std::collections::BTreeSet::new()
        };
        for (line_number, line) in source.lines().enumerate() {
            let line_number = line_number + 1;
            let reason = match language {
                "typescript"
                    if typescript_raw_signature(
                        &source,
                        line_number,
                        &imported_proto_names,
                    ) =>
                {
                    Some("public TypeScript client method exposes a raw protobuf request/response")
                }
                "python" if python_public_raw_stub(line) => {
                    Some("public Python client attribute exposes a raw gRPC stub")
                }
                "python"
                    if line.contains("IdempotencyKeyValue: TypeAlias = object")
                        || line.contains("ImageValue: TypeAlias = Annotated[object") =>
                {
                    Some("Python semantic identity is erased to object")
                }
                "python" if python_erased_oneof_payload(line) => {
                    Some("Python oneof payload is erased to object")
                }
                "python" if python_raw_public_return(&source, line_number, line) => {
                    Some("public Python route returns the raw transport response")
                }
                "go" if go_raw_protobuf_response(line) =>
                {
                    Some("public Go client method returns a raw protobuf response pointer")
                }
                "go"
                    if line.contains("type IdempotencyKey any")
                        || line.contains("type Image any")
                        || line.contains("NewIdempotencyKey(value any)")
                        || line.contains("NewImage(value any)") =>
                {
                    Some("Go semantic identity is erased to any")
                }
                "go" if go_erased_oneof_payload(line) => {
                    Some("Go oneof payload is erased to any")
                }
                "jvm"
                    if jvm_opaque_message_projection(line) => {
                    Some("public JVM response getter exposes an opaque message wrapper")
                }
                "jvm"
                    if jvm_erased_semantic_identity(line) => {
                    Some("JVM semantic identity is erased to protobuf Message")
                }
                "jvm"
                    if jvm_erased_known_oneof(line) => {
                    Some("JVM known oneof payload is erased to ByteString")
                }
                "jvm"
                    if jvm_raw_open_enum(line) => {
                    Some("JVM enum projection exposes only an untyped raw integer")
                }
                "jvm" if jvm_raw_public_accessor(line) || jvm_raw_public_wire_record(line) =>
                    Some("public JVM response getter exposes a raw protobuf message"),
                "swift" if line.contains("public let wire: RustWireMessage") => {
                    Some("public Swift wrapper exposes an opaque RustWireMessage")
                }
                "cpp"
                    if line.contains("RustWireMessage wire;") && !line.contains("private:") =>
                {
                    Some("public C++ wrapper exposes an opaque RustWireMessage")
                }
                "csharp" if csharp_raw_public_wire_record(line) => {
                    Some("public C# facade record exposes a raw protobuf message")
                }
                "csharp" if csharp_timestamp_precision_loss(line) => {
                    Some("C# timestamp projection can lose protobuf nanosecond precision")
                }
                "haskell" if haskell_raw_semantic_identity(line) => {
                    Some("Haskell semantic identity is erased to raw ByteString")
                }
                "haskell" if haskell_public_constructor_exposure(line) => {
                    Some("Haskell semantic wrapper exposes a bypassable raw constructor")
                }
                "haskell" if haskell_erased_known_oneof(line) => {
                    Some("Haskell known oneof payload is erased to ByteString")
                }
                "haskell" if haskell_raw_open_enum(line) => {
                    Some("Haskell enum projection exposes only an untyped raw integer")
                }
                _ => None,
            };
            if let Some(reason) = reason {
                violations.push(GeneratedSurfaceViolation {
                    language,
                    path: relative.clone(),
                    line: line_number,
                    reason,
                });
            }
        }
    }

    Ok(violations)
}

fn typescript_imported_proto_names(source: &str) -> std::collections::BTreeSet<String> {
    source
        .lines()
        .filter(|line| line.contains("import") && (line.contains("_pb") || line.contains("/proto/")))
        .flat_map(|line| {
            line.split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
                .filter(|token| token.ends_with("Request") || token.ends_with("Response"))
                .map(str::to_owned)
        })
        .collect()
}

fn typescript_raw_signature(
    source: &str,
    line_number: usize,
    imported_proto_names: &std::collections::BTreeSet<String>,
) -> bool {
    if imported_proto_names.is_empty() {
        return false;
    }
    let lines = source.lines().collect::<Vec<_>>();
    let start = line_number.saturating_sub(1);
    if !lines
        .get(start)
        .is_some_and(|line| line.contains("request:"))
    {
        return false;
    }
    let end = (start + 6).min(lines.len());
    let normalized = lines[start..end].join(" ");
    let Some(request_start) = normalized.find("request:") else {
        return false;
    };
    let request = normalized[request_start + "request:".len()..]
        .split(|character: char| character == ')' || character == ',' || character.is_whitespace())
        .find(|token| !token.is_empty())
        .unwrap_or_default();
    let Some(response_start) = normalized.find("Promise<") else {
        return false;
    };
    let response = normalized[response_start + "Promise<".len()..]
        .split(|character: char| character == '>' || character == ',' || character.is_whitespace())
        .find(|token| !token.is_empty())
        .unwrap_or_default();
    imported_proto_names.contains(request) && imported_proto_names.contains(response)
}

fn python_raw_public_return(source: &str, line_number: usize, line: &str) -> bool {
    let Some(returned) = line.split("return await self.").nth(1) else {
        return false;
    };
    if returned.starts_with('_') {
        return false;
    }
    let lines = source.lines().collect::<Vec<_>>();
    let mut method = None;
    for candidate in lines[..line_number.saturating_sub(1)].iter().rev() {
        let trimmed = candidate.trim_start();
        if trimmed.starts_with("async def ") || trimmed.starts_with("def ") {
            method = trimmed
                .split_whitespace()
                .nth(1)
                .map(|name| name.split('(').next().unwrap_or(name));
            break;
        }
    }
    method.is_some_and(|name| !name.starts_with('_'))
}

fn python_public_raw_stub(line: &str) -> bool {
    let trimmed = line.trim_start();
    trimmed.starts_with("self.")
        && !trimmed.starts_with("self._")
        && trimmed.contains("_pb2_grpc.")
        && trimmed.contains("Stub(")
}

fn python_erased_oneof_payload(line: &str) -> bool {
    let trimmed = line.trim_start();
    (trimmed.starts_with("payload: object")
        || trimmed.starts_with("def known_oneof(payload: object)"))
        && !trimmed.starts_with("#")
}

fn go_erased_oneof_payload(line: &str) -> bool {
    let trimmed = line.trim_start();
    trimmed.starts_with("type KnownOneof struct") && trimmed.contains("Payload any")
        || trimmed.starts_with("func NewKnownOneof(payload any)")
        || trimmed.starts_with("Payload any `json:\"payload")
}

fn go_raw_protobuf_response(line: &str) -> bool {
    if !line.contains("func (client *Client)") || !line.contains("Response, error)") {
        return false;
    }
    line.split('*').skip(1).any(|tail| {
        tail.chars()
            .next()
            .is_some_and(|character| character.is_ascii_lowercase())
            && tail.contains('.')
    })
}

fn csharp_raw_public_wire_record(line: &str) -> bool {
    let trimmed = line.trim_start();
    trimmed.starts_with("public sealed record ")
        && trimmed.contains(" Wire)")
        && (trimmed.contains("Acyclic.") || trimmed.contains("Inference."))
}

fn csharp_timestamp_precision_loss(line: &str) -> bool {
    line.contains("DateTimeOffset") && line.contains("ToDateTimeOffset()")
}

fn haskell_raw_semantic_identity(line: &str) -> bool {
    let trimmed = line.trim_start();
    (trimmed.starts_with("type IdempotencyKey") || trimmed.starts_with("type Image"))
        && trimmed.contains("ByteString")
}

fn haskell_public_constructor_exposure(line: &str) -> bool {
    let trimmed = line.trim_start();
    ["IdempotencyKey", "Image", "WireEnum", "KnownOneof"]
        .iter()
        .any(|name| trimmed.contains(&format!("{name}(..)")))
}

fn haskell_erased_known_oneof(line: &str) -> bool {
    let trimmed = line.trim_start();
    (trimmed.starts_with("data KnownOneof")
        || trimmed.starts_with("newtype KnownOneof")
        || trimmed.contains("KnownOneof {"))
        && trimmed.contains("ByteString")
}

fn haskell_raw_open_enum(line: &str) -> bool {
    let trimmed = line.trim_start();
    trimmed.starts_with("type WireEnum")
        && (trimmed.contains("Int") || trimmed.contains("Word"))
}

fn jvm_raw_public_wire_record(line: &str) -> bool {
    let trimmed = line.trim_start();
    let record_declaration = trimmed.starts_with("public record ")
        || trimmed.starts_with("data class ")
        || trimmed.starts_with("final case class ");
    record_declaration
        && (trimmed.contains("(acyclic.")
            || trimmed.contains("(inference.")
            || trimmed.contains("value: acyclic.")
            || trimmed.contains("value: inference."))
}

fn jvm_opaque_message_projection(line: &str) -> bool {
    line.contains("RustSemanticTypes.WireMessage")
        || line.contains("RustSemanticTypesKotlin.WireMessage")
        || line.contains("RustSemanticTypesScala.WireMessage")
}

fn jvm_raw_public_accessor(line: &str) -> bool {
    for declaration in line.split(['}', ';']) {
        let Some(public_start) = declaration.find("public ") else {
            continue;
        };
        let public = &declaration[public_start..];
        let Some(open) = public.find('(') else {
            continue;
        };
        let signature = &public[..open];
        if signature.contains("toWire") || signature.contains("fromWire") {
            continue;
        }
        if signature.contains("public acyclic.")
            || signature.contains("public inference.")
            || signature.contains("public java.util.List<acyclic.")
            || signature.contains("public java.util.List<inference.")
        {
            return true;
        }
    }
    for declaration in line.split(['}', ';']) {
        if let Some(kotlin_start) = declaration.find("fun ") {
            let kotlin = &declaration[kotlin_start..];
            if !kotlin.contains("toWire") && !kotlin.contains("fromWire") {
                if let Some(colon) = kotlin.find(':') {
                    let result = &kotlin[colon + 1..];
                    if result.contains("acyclic.") || result.contains("inference.") {
                        return true;
                    }
                }
            }
        }
        if let Some(scala_start) = declaration.find("def ") {
            let scala = &declaration[scala_start..];
            if !scala.contains("toWire") && !scala.contains("fromWire") {
                if let Some(colon) = scala.find(':') {
                    let result = &scala[colon + 1..];
                    if result.contains("acyclic.") || result.contains("inference.") {
                        return true;
                    }
                }
            }
        }
    }
    false
}

fn jvm_erased_semantic_identity(line: &str) -> bool {
    line.contains("IdempotencyKey") && line.contains("com.google.protobuf.Message")
}

fn jvm_erased_known_oneof(line: &str) -> bool {
    (line.contains("record Known(")
        || line.contains("data class Known")
        || line.contains("case class Known"))
        && line.contains("ByteString")
}

fn jvm_raw_open_enum(line: &str) -> bool {
    line.contains("record WireEnum(int value)")
        || line.contains("data class WireEnum(val value: Int)")
        || line.contains("case class WireEnum(value: Int)")
}

/// Check that each generated facade contains the Rust-owned type features
/// needed to represent open values, presence, unions, and nominal identities.
/// Marker checks are a fast acceptance gate; language compilers and runtime
/// conformance tests remain the semantic proof for each target.
pub fn audit_generated_type_features(
    artifact_root: &Path,
) -> Result<Vec<GeneratedSurfaceViolation>, String> {
    if !artifact_root.is_dir() {
        return Err(format!("generated artifact root does not exist: {}", artifact_root.display()));
    }
    let mut files = Vec::new();
    collect_surface_files(artifact_root, &mut files);
    let mut violations = Vec::new();
    for path in files {
        let Some(language) = surface_language(&path) else {
            continue;
        };
        let source = fs::read_to_string(&path)
            .map_err(|error| format!("read generated facade {}: {error}", path.display()))?;
        let relative = path
            .strip_prefix(artifact_root)
            .unwrap_or(&path)
            .display()
            .to_string();
        for (marker, feature) in required_type_feature_markers(language) {
            if !contains_non_comment_marker(&source, marker) {
                violations.push(GeneratedSurfaceViolation {
                    language,
                    path: relative.clone(),
                    line: 0,
                    reason: feature,
                });
            }
        }
    }
    Ok(violations)
}

fn contains_non_comment_marker(source: &str, marker: &str) -> bool {
    source.lines().any(|line| {
        let trimmed = line.trim_start();
        !trimmed.starts_with("//")
            && !trimmed.starts_with('#')
            && !trimmed.starts_with("/*")
            && !trimmed.starts_with('*')
            && line.contains(marker)
    })
}

fn required_type_feature_markers(language: &str) -> &'static [(&'static str, &'static str)] {
    match language {
        "typescript" => &[
            ("Unknown", "TypeScript facade lacks an open unknown-value projection"),
            ("oneof", "TypeScript facade lacks a discriminated oneof projection"),
            ("undefined", "TypeScript facade lacks explicit presence representation"),
            ("IdempotencyKey", "TypeScript facade lacks a nominal semantic identity"),
        ],
        "python" => &[
            ("UnknownOneof", "Python facade lacks an unknown-oneof projection"),
            ("| None", "Python facade lacks explicit optional presence representation"),
            ("NewType", "Python facade lacks nominal semantic identities"),
        ],
        "go" => &[
            ("UnknownOneof", "Go facade lacks an unknown-oneof projection"),
            ("IdempotencyKey", "Go facade lacks a nominal semantic identity"),
        ],
        "jvm" => &[
            ("Unknown", "JVM facade lacks an open unknown-value projection"),
            ("Optional", "JVM facade lacks explicit presence representation"),
            ("Oneof", "JVM facade lacks a discriminated oneof projection"),
        ],
        "csharp" => &[
            ("Unknown", "C# facade lacks an open unknown-value projection"),
            ("Optional", "C# facade lacks explicit presence representation"),
            ("oneof", "C# facade lacks a discriminated oneof projection"),
            ("IdempotencyKey", "C# facade lacks a nominal semantic identity"),
        ],
        "swift" => &[
            ("unknown", "Swift facade lacks an open unknown-value projection"),
            ("Optional", "Swift facade lacks explicit presence representation"),
            ("IdempotencyKey", "Swift facade lacks a nominal semantic identity"),
        ],
        "cpp" => &[
            ("UnknownOneof", "C++ facade lacks an unknown-oneof projection"),
            ("std::optional", "C++ facade lacks explicit presence representation"),
            ("std::variant", "C++ facade lacks a discriminated union projection"),
            ("IdempotencyKey", "C++ facade lacks a nominal semantic identity"),
        ],
        "ruby" => &[
            ("Unknown", "Ruby facade lacks an open unknown-value projection"),
            ("IdempotencyKey", "Ruby facade lacks a nominal semantic identity"),
        ],
        "php" => &[
            ("Unknown", "PHP facade lacks an open unknown-value projection"),
            ("IdempotencyKey", "PHP facade lacks a nominal semantic identity"),
        ],
        "dart" => &[
            ("Unknown", "Dart facade lacks an open unknown-value projection"),
            ("IdempotencyKey", "Dart facade lacks a nominal semantic identity"),
        ],
        "haskell" => &[
            ("Unknown", "Haskell facade lacks an open unknown-value projection"),
            ("Maybe", "Haskell facade lacks explicit optional presence representation"),
            ("IdempotencyKey", "Haskell facade lacks a nominal semantic identity"),
            ("KnownOneof", "Haskell facade lacks a discriminated oneof projection"),
        ],
        _ => &[],
    }
}

/// Run the public-surface audit while also requiring every expected facade
/// family to be present.  A missing generated file is a generation failure,
/// never an empty passing report.
pub fn audit_required_generated_public_surfaces(
    artifact_root: &Path,
    required_languages: &[&str],
) -> Result<Vec<GeneratedSurfaceViolation>, String> {
    let mut files = Vec::new();
    if !artifact_root.is_dir() {
        return Err(format!("generated artifact root does not exist: {}", artifact_root.display()));
    }
    collect_surface_files(artifact_root, &mut files);
    let mut present = std::collections::BTreeSet::new();
    for path in &files {
        if let Some(language) = surface_language(path) {
            present.insert(language);
        }
    }
    let missing = required_languages
        .iter()
        .filter(|language| !present.contains(**language))
        .copied()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(format!(
            "missing generated public SDK surfaces in {}: {}",
            artifact_root.display(),
            missing.join(", ")
        ));
    }
    audit_generated_public_surfaces(artifact_root)
}

fn collect_surface_files(root: &Path, files: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_surface_files(&path, files);
        } else if surface_language(&path).is_some() {
            files.push(path);
        }
    }
}

fn surface_language(path: &Path) -> Option<&'static str> {
    let name = path.file_name()?.to_str()?;
    if name.ends_with("-metadata.ts") || name == "RustTypedClients.ts" {
        Some("typescript")
    } else if name == "remote.py" {
        Some("python")
    } else if name == "client.go" {
        Some("go")
    } else if name == "RustTypedResponses.java"
        || name == "RustTypedResponses.kt"
        || name == "RustTypedResponses.scala"
        || name == "RustSemanticTypes.java"
        || name == "RustSemanticTypes.kt"
        || name == "RustSemanticTypes.scala"
    {
        Some("jvm")
    } else if name == "RustTypedClients.swift" {
        Some("swift")
    } else if name == "rust_typed_clients.hpp" {
        Some("cpp")
    } else if name == "RustTypedClients.cs" {
        Some("csharp")
    } else if name == "generated_typed.rb" {
        Some("ruby")
    } else if name == "RustTyped.php" {
        Some("php")
    } else if name == "generated_typed.dart" {
        Some("dart")
    } else if name == "RustTypedClients.hs"
        || name == "RustSemanticTypes.hs"
        || name == "generated_typed.hs"
        || name == "RustTypedClients.lhs"
        || name == "RustSemanticTypes.lhs"
    {
        Some("haskell")
    } else {
        None
    }
}

/// Find a semantic type by its stable Rust-owned identifier.
pub fn semantic_type(id: &str) -> Option<&'static SemanticType> {
    SEMANTIC_TYPES.iter().find(|item| item.id == id)
}

/// Resolve a Rust model field's semantic type for a contract family.
pub fn field_semantic_type(family: &str, field: &str) -> Option<&'static SemanticType> {
    FIELD_SEMANTIC_TYPES
        .iter()
        .find(|mapping| mapping.family == family && mapping.field == field)
        .and_then(|mapping| semantic_type(mapping.semantic_type))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_language_has_a_strong_projection_profile() {
        assert_eq!(
            TypePolicyLanguage::ALL.len(),
            TYPE_PROJECTION_PROFILES.len()
        );
        for language in TypePolicyLanguage::ALL {
            let profile = type_projection_profile(*language);
            assert_eq!(profile.language, *language);
            assert!(!profile.nominal_types.is_empty());
            assert!(!profile.unions.is_empty());
            assert!(!profile.refinements.is_empty());
            assert!(!profile.presence.is_empty());
            assert!(!profile.unknown_values.is_empty());
            assert!(!profile.integers.is_empty());
        }
    }

    #[test]
    fn generated_surface_audit_rejects_public_type_erasure() {
        let root = std::env::temp_dir().join(format!(
            "acyclic-generated-surface-audit-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("jvm")).expect("audit fixture directory");
        fs::create_dir_all(root.join("go")).expect("audit fixture directory");
        fs::create_dir_all(root.join("python")).expect("audit fixture directory");
        fs::create_dir_all(root.join("csharp")).expect("audit fixture directory");
        fs::write(
            root.join("typescript-metadata.ts"),
            "import { PublishRequest, PublishResponse } from \"./generated/proto/workers_pb\";\npublish(request: PublishRequest): Promise<PublishResponse> { }\n",
        )
        .expect("typescript fixture");
        fs::write(
            root.join("python").join("remote.py"),
            "IdempotencyKeyValue: TypeAlias = object\n        self.actors = actors_pb2_grpc.ActorsServiceStub(self._channel)\n    async def invoke_actor(self):\n        return await self.actors.InvokeActor(request)\n",
        )
        .expect("python fixture");
        fs::write(
            root.join("go").join("client.go"),
            "func (client *Client) Select(ctx context.Context) (*workersv1.SelectResponse, error) { }\ntype Image any\n",
        )
        .expect("go fixture");
        fs::write(
            root.join("jvm").join("RustTypedResponses.java"),
            "public acyclic.protocol.v1.Protocol.HandshakeResponse protocol() { }\npublic record RawJavaResponse(acyclic.protocol.v1.Protocol.HandshakeResponse value) { public acyclic.protocol.v1.Protocol.HandshakeResponse toWire() { return value; } }\n",
        )
        .expect("jvm fixture");
        fs::write(
            root.join("jvm").join("RustTypedResponses.kt"),
            "data class RawKotlinResponse(val value: acyclic.protocol.v1.Protocol.HandshakeResponse) { fun toWire(): acyclic.protocol.v1.Protocol.HandshakeResponse = value }\n",
        )
        .expect("kotlin fixture");
        fs::write(
            root.join("jvm").join("RustTypedResponses.scala"),
            "final case class RawScalaResponse(value: acyclic.protocol.v1.Protocol.HandshakeResponse) { def toWire: acyclic.protocol.v1.Protocol.HandshakeResponse = value }\n",
        )
        .expect("scala fixture");
        fs::write(
            root.join("csharp").join("RustTypedClients.cs"),
            "public sealed record RawResponse(Acyclic.Protocol.V1.HandshakeResponse Wire);\npublic sealed record TypedResponse(ProtocolHandshakeResponseValue Value);\n",
        )
        .expect("csharp fixture");

        let findings = audit_generated_public_surfaces(&root).expect("audit fixture");
        assert!(findings.iter().any(|finding| finding.language == "typescript"));
        assert!(findings.iter().any(|finding| finding.language == "python"));
        assert!(findings.iter().any(|finding| finding.language == "go"));
        assert!(findings.iter().any(|finding| finding.language == "jvm"));
        assert!(findings.iter().any(|finding| finding.language == "csharp"));
        assert!(findings.iter().all(|finding| finding.line <= 4));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn generated_surface_audit_distinguishes_nominal_and_transport_signatures() {
        let root = std::env::temp_dir().join(format!(
            "acyclic-generated-surface-audit-signatures-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("python")).expect("audit fixture directory");
        fs::create_dir_all(root.join("go")).expect("audit fixture directory");
        fs::write(
            root.join("workers-metadata.ts"),
            "import { WireRequest, WireResponse } from \"./generated/proto/workers_pb\";\ninterface Client {\n  raw(\n    request: WireRequest,\n  ): Promise<WireResponse>;\n  typed(request: TypedRequest): Promise<TypedResponse>;\n}\n",
        )
        .expect("typescript fixture");
        fs::write(
            root.join("python").join("remote.py"),
            "    async def private(self):\n        return await self._channel(request)\n    async def public(self):\n        return await self.actors(request)\n",
        )
        .expect("python fixture");
        fs::write(
            root.join("go").join("client.go"),
            "func (client *Client) Typed(ctx context.Context) (TypedResponse, error) { }\nfunc (client *Client) Raw(ctx context.Context) (*workersv1.Response, error) { }\n",
        )
        .expect("go fixture");

        let findings = audit_generated_public_surfaces(&root).expect("audit fixture");
        assert_eq!(
            findings
                .iter()
                .filter(|finding| finding.language == "typescript")
                .count(),
            1,
            "only the imported protobuf TypeScript signature should fail"
        );
        assert_eq!(
            findings
                .iter()
                .filter(|finding| finding.language == "python")
                .count(),
            1,
            "private Python transport helpers should be allowed"
        );
        assert_eq!(
            findings
                .iter()
                .filter(|finding| finding.language == "go")
                .count(),
            1,
            "nominal Go response pointers should be allowed"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn generated_surface_audit_rejects_erased_oneof_payloads() {
        let root = std::env::temp_dir().join(format!(
            "acyclic-generated-surface-audit-oneof-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("python")).expect("audit fixture directory");
        fs::create_dir_all(root.join("go")).expect("audit fixture directory");
        fs::write(
            root.join("python").join("remote.py"),
            "@dataclass(frozen=True)\nclass KnownOneof:\n    payload: object\ndef known_oneof(payload: object) -> KnownOneof:\n    return KnownOneof(payload=payload)\n",
        )
        .expect("python fixture");
        fs::write(
            root.join("go").join("client.go"),
            "type KnownOneof struct { Tag string; Payload any }\nfunc NewKnownOneof(payload any) KnownOneof { return KnownOneof{Payload: payload} }\n",
        )
        .expect("go fixture");

        let findings = audit_generated_public_surfaces(&root).expect("audit fixture");
        assert!(findings.iter().any(|finding| {
            finding.language == "python"
                && finding.reason == "Python oneof payload is erased to object"
        }));
        assert!(findings.iter().any(|finding| {
            finding.language == "go" && finding.reason == "Go oneof payload is erased to any"
        }));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn generated_surface_audit_rejects_lossy_timestamps_and_opaque_messages() {
        let root = std::env::temp_dir().join(format!(
            "acyclic-generated-surface-audit-precision-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("csharp")).expect("audit fixture directory");
        fs::create_dir_all(root.join("jvm")).expect("audit fixture directory");
        fs::create_dir_all(root.join("haskell-positive")).expect("audit fixture directory");
        fs::write(
            root.join("csharp").join("RustTypedClients.cs"),
            "public sealed record ObjectInfo(DateTimeOffset? LastModified);\nmessage.LastModified?.ToDateTimeOffset();\n",
        )
        .expect("csharp fixture");
        fs::write(
            root.join("jvm").join("RustTypedResponses.java"),
            "public record ObjectResponse(acyclic.objects.v2.Objects.ObjectInfo value) { public RustSemanticTypes.WireMessage metadata() { return RustSemanticTypes.WireMessage.of(value.getMetadata()); } public acyclic.objects.v2.Objects.ObjectInfo nested() { return value.getObject(); } public java.util.List<acyclic.objects.v2.Objects.Part> parts() { return value.getPartsList(); } public acyclic.objects.v2.Objects.ObjectInfo toWire() { return value; } }\npublic record IdempotencyKey(com.google.protobuf.Message value) {}\npublic record Known(com.google.protobuf.ByteString payload) {}\npublic record WireEnum(int value) {}\n",
        )
        .expect("jvm fixture");
        fs::write(
            root.join("jvm").join("RustTypedResponses.kt"),
            "class ObjectResponse { fun toWire(): acyclic.objects.v2.Objects.ObjectInfo = value; fun nested(): acyclic.objects.v2.Objects.ObjectInfo = value }\n",
        )
        .expect("kotlin fixture");
        fs::write(
            root.join("jvm").join("RustTypedResponses.scala"),
            "final class ObjectResponse { def toWire: acyclic.objects.v2.Objects.ObjectInfo = value; def nested: acyclic.objects.v2.Objects.ObjectInfo = value }\n",
        )
        .expect("scala fixture");
        fs::write(
            root.join("RustSemanticTypes.hs"),
            "module Raw (IdempotencyKey(..), WireEnum(..)) where\ntype IdempotencyKey = ByteString\ndata KnownOneof = KnownOneof { payload :: ByteString }\ntype WireEnum = Int32\n",
        )
        .expect("haskell fixture");
        fs::write(
            root.join("haskell-positive").join("RustSemanticTypes.hs"),
            "module RustSemanticTypes (IdempotencyKey, WireEnum, KnownOneof) where\nnewtype IdempotencyKey = IdempotencyKey ByteString\ndata KnownOneof = KnownOneof (ValidatedPayload)\nnewtype WireEnum = WireEnum Int32\n",
        )
        .expect("haskell positive fixture");
        fs::create_dir_all(root.join("swift")).expect("swift fixture directory");
        fs::write(
            root.join("swift").join("RustTypedClients.swift"),
            "public struct ObjectResponse { public let wire: RustWireMessage }\n",
        )
        .expect("swift fixture");
        fs::create_dir_all(root.join("cpp").join("include")).expect("cpp fixture directory");
        fs::write(
            root.join("cpp").join("include").join("rust_typed_clients.hpp"),
            "struct ObjectResponse { RustWireMessage wire; };\n",
        )
        .expect("cpp fixture");

        let findings = audit_generated_public_surfaces(&root).expect("audit fixture");
        assert!(findings.iter().any(|finding| {
            finding.language == "csharp"
                && finding.reason == "C# timestamp projection can lose protobuf nanosecond precision"
        }));
        assert!(findings.iter().any(|finding| {
            finding.language == "jvm"
                && finding.reason == "public JVM response getter exposes an opaque message wrapper"
        }));
        assert!(findings.iter().any(|finding| {
            finding.language == "jvm"
                && finding.reason == "JVM semantic identity is erased to protobuf Message"
        }));
        assert!(findings.iter().any(|finding| {
            finding.language == "jvm"
                && finding.reason == "JVM known oneof payload is erased to ByteString"
        }));
        assert!(findings.iter().any(|finding| {
            finding.language == "jvm"
                && finding.reason == "JVM enum projection exposes only an untyped raw integer"
        }));
        assert!(findings.iter().any(|finding| {
            finding.language == "haskell"
                && finding.reason == "Haskell semantic identity is erased to raw ByteString"
        }));
        assert!(findings.iter().any(|finding| {
            finding.language == "haskell"
                && finding.reason == "Haskell known oneof payload is erased to ByteString"
        }));
        assert!(findings.iter().any(|finding| {
            finding.language == "haskell"
                && finding.reason == "Haskell enum projection exposes only an untyped raw integer"
        }));
        assert!(findings.iter().any(|finding| {
            finding.language == "haskell"
                && finding.reason == "Haskell semantic wrapper exposes a bypassable raw constructor"
        }));
        assert!(findings
            .iter()
            .filter(|finding| finding.path.contains("haskell-positive"))
            .next()
            .is_none(), "opaque validated Haskell newtypes should pass");
        assert!(findings.iter().any(|finding| {
            finding.language == "swift"
                && finding.reason == "public Swift wrapper exposes an opaque RustWireMessage"
        }));
        assert!(findings.iter().any(|finding| {
            finding.language == "cpp"
                && finding.reason == "public C++ wrapper exposes an opaque RustWireMessage"
        }));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn generated_surface_audit_allows_private_wire_boundaries() {
        let root = std::env::temp_dir().join(format!(
            "acyclic-generated-surface-audit-private-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("audit fixture directory");
        fs::write(
            root.join("RustTypedClients.cs"),
            "private RustWireMessage wire;\n",
        )
        .expect("private wire fixture");
        assert!(
            audit_generated_public_surfaces(&root)
                .expect("audit fixture")
                .is_empty()
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn required_surface_audit_rejects_missing_language_families() {
        let root = std::env::temp_dir().join(format!(
            "acyclic-generated-surface-audit-missing-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("audit fixture directory");
        fs::write(root.join("RustTypedClients.cs"), "// generated\n")
            .expect("csharp fixture");
        let error = audit_required_generated_public_surfaces(&root, REQUIRED_PRODUCT_SURFACES)
            .expect_err("missing generated surfaces must fail closed");
        assert!(error.contains("python"));
        assert!(error.contains("go"));
        assert!(error.contains("jvm"));
        assert!(error.contains("swift"));
        assert!(error.contains("cpp"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn generated_feature_audit_rejects_missing_unknown_presence_and_identity_markers() {
        let root = std::env::temp_dir().join(format!(
            "acyclic-generated-feature-audit-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("audit fixture directory");
        fs::write(root.join("RustTypedClients.swift"), "public struct Wire {}\n")
            .expect("swift fixture");
        let findings = audit_generated_type_features(&root).expect("feature audit fixture");
        assert!(findings.iter().any(|finding| finding.language == "swift"));
        assert!(findings.iter().any(|finding| finding.reason.contains("unknown")));
        assert!(findings.iter().any(|finding| finding.reason.contains("identity")));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn generated_feature_audit_accepts_all_required_markers() {
        let root = std::env::temp_dir().join(format!(
            "acyclic-generated-feature-audit-valid-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("audit fixture directory");
        fs::write(
            root.join("RustTypedClients.swift"),
            "unknown Optional IdempotencyKey\n",
        )
        .expect("swift fixture");
        assert!(audit_generated_type_features(&root)
            .expect("feature audit fixture")
            .is_empty());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn every_projection_preserves_unknown_wire_values() {
        for profile in TYPE_PROJECTION_PROFILES {
            assert!(
                profile.unknown_values.contains("raw") || profile.unknown_values.contains("Raw"),
                "{}: {}",
                profile.language.id(),
                profile.unknown_values
            );
            assert!(
                profile.unknown_values.contains("unknown")
                    || profile.unknown_values.contains("Unknown"),
                "{}: {}",
                profile.language.id(),
                profile.unknown_values
            );
        }
        assert!(SEMANTIC_TYPES.iter().any(|item| {
            item.rules.contains(&SemanticRule::PreserveUnknownEnum)
                || item.wire_kind == WireValueKind::Enum
        }));
        assert!(
            SEMANTIC_TYPES
                .iter()
                .any(|item| item.rules.contains(&SemanticRule::PreserveUnknownOneof))
        );
    }

    #[test]
    fn stronger_than_rust_constraints_are_in_the_rust_model() {
        assert!(
            semantic_type("version_sha256")
                .expect("digest")
                .rules
                .contains(&SemanticRule::FixedLength(32))
        );
        assert!(
            semantic_type("page_limit")
                .expect("page limit")
                .rules
                .contains(&SemanticRule::MaxItems(1000))
        );
        assert!(
            semantic_type("machine_page_limit")
                .expect("machine page limit")
                .rules
                .contains(&SemanticRule::MaxItems(MACHINE_PAGE_LIMIT_MAX))
        );
        assert!(
            semantic_type("machine_event_page_limit")
                .expect("machine event page limit")
                .rules
                .contains(&SemanticRule::MaxItems(MACHINE_EVENT_PAGE_LIMIT_MAX))
        );
        assert!(
            semantic_type("oneof_arm")
                .expect("oneof")
                .rules
                .contains(&SemanticRule::ExactOneof)
        );
    }

    #[test]
    fn every_field_mapping_resolves_to_a_rust_semantic_type() {
        for mapping in FIELD_SEMANTIC_TYPES {
            assert!(
                semantic_type(mapping.semantic_type).is_some(),
                "{} {} maps to missing semantic type {}",
                mapping.family,
                mapping.field,
                mapping.semantic_type
            );
            assert!(field_semantic_type(mapping.family, mapping.field).is_some());
        }
    }

    #[test]
    fn every_field_mapping_has_a_concrete_public_wire_location() {
        assert!(PUBLIC_FIELD_BINDINGS.len() >= FIELD_SEMANTIC_TYPES.len());
        for mapping in FIELD_SEMANTIC_TYPES {
            let binding = PUBLIC_FIELD_BINDINGS
                .iter()
                .find(|candidate| {
                    candidate.family == mapping.family && candidate.field == mapping.field
                })
                .expect("mapped field must be attached to a public protobuf model");
            assert_eq!(binding.semantic_type, mapping.semantic_type);
            assert!(!binding.module.is_empty());
            assert!(!binding.message.is_empty());
            assert!(!binding.wire_field.is_empty());
        }
        for binding in PUBLIC_FIELD_BINDINGS {
            assert!(
                semantic_type(binding.semantic_type).is_some(),
                "public binding {}.{} maps to missing semantic type {}",
                binding.family,
                binding.field,
                binding.semantic_type
            );
        }
    }

    #[test]
    fn every_request_binding_has_a_generated_client_route() {
        for binding in PUBLIC_FIELD_BINDINGS {
            if binding.direction == PublicFieldDirection::Request {
                assert!(
                    binding.client_attribute().is_some(),
                    "missing service for {}.{}",
                    binding.family,
                    binding.field
                );
                assert!(
                    binding.rpc().is_some(),
                    "missing rpc for {}.{}",
                    binding.family,
                    binding.field
                );
            }
        }
    }

    #[test]
    fn nested_bindings_have_production_route_metadata() {
        assert_eq!(PUBLIC_NESTED_ROUTES.len(), 2);
        for route in PUBLIC_NESTED_ROUTES {
            assert!(!route.operation.is_empty());
            assert!(!route.client_attribute.is_empty());
            assert!(!route.rpc.is_empty());
            assert!(PUBLIC_FIELD_BINDINGS.iter().any(|binding| {
                binding.family == route.family
                    && binding.message == route.nested_message
                    && binding.field == route.semantic_field
                    && binding.direction == PublicFieldDirection::NestedMessage
            }));
        }
    }

    #[test]
    fn field_mappings_retain_each_wire_kind_before_projection() {
        assert_eq!(
            field_semantic_type("workers", "idempotency_key")
                .unwrap()
                .wire_kind,
            WireValueKind::String
        );
        assert_eq!(
            field_semantic_type("stream", "idempotency_key")
                .unwrap()
                .wire_kind,
            WireValueKind::Bytes
        );
        assert_eq!(
            field_semantic_type("machines", "idempotency_key")
                .unwrap()
                .wire_kind,
            WireValueKind::Message
        );
        assert_eq!(
            field_semantic_type("machines", "image").unwrap().wire_kind,
            WireValueKind::Message
        );
        assert_eq!(
            field_semantic_type("inference", "revision")
                .unwrap()
                .wire_kind,
            WireValueKind::Bytes
        );
    }

    #[test]
    fn open_union_projection_has_explicit_known_and_unknown_discriminants() {
        let variants = WIRE_UNION_VARIANTS
            .iter()
            .filter(|variant| variant.union == "wire_choice")
            .collect::<Vec<_>>();
        assert_eq!(variants.len(), 2);
        assert_eq!(variants[0].tag, "known");
        assert_eq!(variants[1].tag, "unknown");
        assert_eq!(variants[1].payload_wire_kind, WireValueKind::Bytes);
    }

    #[test]
    fn descriptor_field_inventory_is_complete_and_fail_closed() {
        let methods = resolved_rpc_methods().expect("all RPC identities resolve");
        let requests = resolved_request_fields().expect("all request graphs resolve");
        let responses = resolved_response_fields().expect("all response graphs resolve");
        assert!(methods.len() >= 106, "RPC inventory is unexpectedly small");
        assert!(
            requests.len() >= 600,
            "request field inventory is unexpectedly small"
        );
        assert!(
            responses.len() >= 600,
            "response field inventory is unexpectedly small"
        );
        assert!(
            methods
                .iter()
                .any(|method| method.input_message.ends_with("ListModelsRequest"))
        );
        for field in requests.iter().chain(responses.iter()) {
            assert!(!field.family.is_empty());
            assert!(!field.rpc.is_empty());
            assert!(!field.root_message.is_empty());
            assert!(!field.message_path.is_empty());
            assert!(!field.field.is_empty());
            assert!(field.number > 0);
            if field.wire_type == Some(FieldType::Message as i32)
                || field.wire_type == Some(FieldType::Group as i32)
            {
                assert!(field.type_name.is_some());
            }
        }
    }

    #[test]
    fn resolver_attaches_logical_alias_bindings_and_policy_constraints() {
        let requests = resolved_request_fields().expect("all request graphs resolve");
        let machines = requests
            .iter()
            .find(|field| {
                field.family == "machines"
                    && field.root_message.ends_with("ListMachinesRequest")
                    && field.field == "limit"
            })
            .expect("ListMachinesRequest.limit is present");
        assert_eq!(
            machines.semantic_type.as_deref(),
            Some("machine_page_limit")
        );
        assert!(
            machines
                .validation_rules
                .iter()
                .any(|rule| rule == "page_limit.bounded")
        );
        assert!(machines.validation_constraints.iter().any(|constraint| {
            matches!(constraint, ResolvedValidationConstraint::CrossField(rule) if rule == "page_limit.bounded")
        }));

        let events = requests
            .iter()
            .find(|field| {
                field.family == "machines"
                    && field.root_message.ends_with("EventsRequest")
                    && field.field == "limit"
            })
            .expect("EventsRequest.limit is present");
        assert_eq!(
            events.semantic_type.as_deref(),
            Some("machine_event_page_limit")
        );
        assert!(
            events
                .validation_rules
                .iter()
                .any(|rule| rule == "page_limit.bounded")
        );

        let identity = requests
            .iter()
            .find(|field| {
                field.family == "machines"
                    && field.root_message.ends_with("InspectMachineRequest")
                    && field.field == "machine"
            })
            .expect("InspectMachineRequest.machine is present");
        assert_eq!(identity.semantic_type.as_deref(), Some("machine_id"));
        assert!(identity.validation_constraints.iter().any(|constraint| {
            matches!(
                constraint,
                ResolvedValidationConstraint::Rule(SemanticRule::FixedLength(16))
            )
        }));
        assert!(!identity.validation_constraints.iter().any(|constraint| {
            matches!(
                constraint,
                ResolvedValidationConstraint::Rule(SemanticRule::StrictlyPositive)
            )
        }));
    }

    #[test]
    fn resolver_reuses_unambiguous_machine_wrapper_semantics_across_requests() {
        let requests = resolved_request_fields().expect("all request graphs resolve");
        for request in [
            "CheckpointMachineRequest",
            "MachineMutationRequest",
            "RecoverRequest",
        ] {
            let field = requests
                .iter()
                .find(|field| {
                    field.family == "machines"
                        && field.root_message.ends_with(request)
                        && field.field == "idempotency_key"
                })
                .unwrap_or_else(|| panic!("{request}.idempotency_key is present"));
            assert_eq!(
                field.semantic_type.as_deref(),
                Some("idempotency_key_message")
            );
            assert!(field.validation_constraints.iter().any(|constraint| {
                matches!(
                    constraint,
                    ResolvedValidationConstraint::Rule(SemanticRule::FixedLength(16))
                )
            }));
        }
    }

    #[test]
    fn operation_rules_are_structured_and_fail_closed_for_current_contracts() {
        let requests = resolved_request_fields().expect("all request graphs resolve");
        let mut rules = std::collections::BTreeSet::new();
        for field in requests {
            for constraint in field.validation_constraints {
                match constraint {
                    ResolvedValidationConstraint::Operation(rule) => {
                        rules.insert(format!("{rule:?}"));
                    }
                    ResolvedValidationConstraint::Unresolved(rule) => {
                        panic!("current Rust operation policy is unresolved: {rule}");
                    }
                    ResolvedValidationConstraint::Rule(_)
                    | ResolvedValidationConstraint::CrossField(_) => {}
                }
            }
        }
        assert!(rules.iter().any(|rule| rule.contains("Capability")));
        assert!(rules.iter().any(|rule| rule.contains("BucketMustBeEmpty")));
        assert!(rules.iter().any(|rule| rule.contains("OrderedParts")));
        assert!(rules.iter().any(|rule| rule.contains("AtomicPrecondition")));
        assert!(
            rules
                .iter()
                .any(|rule| rule.contains("MaxRecordBytes(65536)"))
        );
        assert!(
            rules
                .iter()
                .any(|rule| rule.contains("MaxCommandBytes(1056768)"))
        );
    }

    #[test]
    fn structured_operation_inventory_retains_rust_policy_identity() {
        let rules = resolved_operation_rules();
        assert!(!rules.is_empty());
        assert!(rules.iter().any(|rule| {
            rule.family == "objects"
                && rule.rpc.ends_with("/CompleteMultipart")
                && rule.validation == "parts.ordered_exact"
                && matches!(
                    rule.rule,
                    OperationRule::OrderedParts {
                        max_items: OBJECTS_MAX_MULTIPART_ITEMS,
                        max_part_number: OBJECTS_MAX_MULTIPART_ITEMS,
                    }
                )
        }));
        assert!(rules.iter().any(|rule| {
            rule.family == "stream"
                && rule.validation == "records.max_bytes"
                && matches!(
                    rule.rule,
                    OperationRule::MaxRecordBytes(STREAM_MAX_RECORD_BYTES)
                )
        }));
        assert!(
            rules
                .iter()
                .all(|rule| !rule.rpc.is_empty() && !rule.validation.is_empty())
        );
    }

    #[test]
    fn every_rust_operation_policy_has_a_target_and_enforcement_boundary() {
        let rules = resolved_operation_rules();
        assert!(
            rules.len() >= 200,
            "all Rust-authored operation validations must be inventoried"
        );
        assert!(rules.iter().all(|rule| !rule.target.path.is_empty()));
        assert!(
            rules
                .iter()
                .all(|rule| rule.target.enforcement != OperationEnforcement::Unsupported),
            "every current Rust operation policy must have an explicit target projection"
        );
        assert!(rules.iter().any(|rule| {
            rule.validation == "bucket.empty"
                && rule.target.path == "bucket"
                && rule.target.enforcement == OperationEnforcement::ProviderState
        }));
        assert!(rules.iter().any(|rule| {
            rule.validation == "bucket.name.non_empty"
                && rule.target.path == "bucket.name"
                && rule.target.enforcement == OperationEnforcement::ClientLocal
        }));
        assert!(rules.iter().any(|rule| {
            rule.validation == "object.key.non_empty" && rule.target.path == "object.key"
        }));
        assert!(rules.iter().any(|rule| {
            rule.validation == "request_identity.nonzero" && rule.target.path == "request_identity"
        }));
        assert!(
            rules.iter().any(|rule| {
                rule.validation == "action.present" && rule.target.path == "action"
            })
        );
        assert!(rules.iter().any(|rule| {
            rule.validation == "idempotency_key.non_empty_utf8"
                && rule.target.enforcement == OperationEnforcement::ClientLocal
        }));
    }

    #[test]
    fn operation_target_classification_does_not_hide_unknown_policy_names() {
        let target = operation_target("future.aggregate.valid");
        assert_eq!(target.path, "future.aggregate.valid");
        assert_eq!(target.enforcement, OperationEnforcement::Unsupported);
        let provider = operation_target("provider.unknown_rule");
        assert_eq!(provider.enforcement, OperationEnforcement::Unsupported);
        assert_eq!(
            operation_enforcement("future.aggregate.valid"),
            OperationEnforcement::Unsupported
        );
        assert_eq!(
            operation_enforcement("model_capabilities.bounded"),
            OperationEnforcement::ClientLocal
        );
    }

    #[test]
    fn rpc_matching_accepts_descriptor_leading_slash_without_erasing_rules() {
        assert_eq!(
            canonical_rpc_identity("/acyclic.machines.v1.MachinesService/Events"),
            "acyclic.machines.v1.MachinesService/Events"
        );
        let view = crate::family_registry::family_view("machines").expect("Machines family");
        assert!(
            operation_policy_for_rpc(view, "/acyclic.machines.v1.MachinesService/Events").is_some()
        );
    }

    #[test]
    fn unknown_validation_suffix_is_explicitly_unresolved() {
        let constraints =
            validation_constraint("request.future_rule", Some(FieldType::String as i32), None);
        assert!(matches!(
            constraints.as_slice(),
            [ResolvedValidationConstraint::Unresolved(rule)] if rule == "request.future_rule"
        ));
    }
}
