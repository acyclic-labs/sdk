use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use acyclic_sdk_contract_options::options_proto;
use acyclic_sdk_contract_wire::{
    BindingFamily, actors_descriptor, actors_proto, descriptor_set_with_docs,
    family_registry::family_view,
    filesystem::{filesystem_descriptor, filesystem_proto},
    generate_csharp_typed_facade, generate_embedded_facades, generate_jvm_semantic_types,
    generate_jvm_typed_clients, generate_jvm_typed_requests, generate_jvm_typed_responses,
    generate_portable_typed_facades, generate_product_bindings, generate_remote_facades,
    generate_swift_cpp_typed_facades, generate_type_policy_qualification_tests,
    harness::{harness_descriptor, harness_proto},
    inference::{inference_descriptor, inference_proto},
    machines::{machines_descriptor, machines_proto},
    objects::{objects_descriptor, objects_proto},
    protocol::{protocol_descriptor, protocol_proto},
    semantic_oracle,
    stream::{stream_descriptor, stream_proto},
    transport_control::{control_descriptor, control_proto},
    type_policy::{
        FIELD_SEMANTIC_TYPES, PUBLIC_FIELD_BINDINGS, PublicFieldDirection, SEMANTIC_TYPES,
        SemanticRule, TYPE_PROJECTION_PROFILES, TypePolicyLanguage, WIRE_UNION_VARIANTS,
        WireValueKind,
    },
    workers::{workers_descriptor, workers_proto},
};
use base64::Engine;
use prost::Message;
use prost_types::FileDescriptorSet;
use serde_json::Value;
use sha2::{Digest, Sha256};

const PROTO_PATH: &str = "actors/v1/actors.proto";
const DESCRIPTOR_PATH: &str = "actors/v1/actors.fds.bin";
const STREAM_PROTO_PATH: &str = "stream/v2/stream.proto";
const STREAM_DESCRIPTOR_PATH: &str = "stream/v2/stream.fds.bin";
const OBJECTS_PROTO_PATH: &str = "objects/v2/objects.proto";
const OBJECTS_DESCRIPTOR_PATH: &str = "objects/v2/objects.fds.bin";
const WORKERS_PROTO_PATH: &str = "workers/v1/workers.proto";
const WORKERS_DESCRIPTOR_PATH: &str = "workers/v1/workers.fds.bin";
const FILESYSTEM_PROTO_PATH: &str = "filesystem/v2/filesystem.proto";
const FILESYSTEM_DESCRIPTOR_PATH: &str = "filesystem/v2/filesystem.fds.bin";
const INFERENCE_PROTO_PATH: &str = "inference/v1/inference.proto";
const INFERENCE_DESCRIPTOR_PATH: &str = "inference/v1/inference.fds.bin";
const MACHINES_PROTO_PATH: &str = "machines/v1/machines.proto";
const MACHINES_DESCRIPTOR_PATH: &str = "machines/v1/machines.fds.bin";
const HARNESS_PROTO_PATH: &str = "harness/v2/harness.proto";
const HARNESS_DESCRIPTOR_PATH: &str = "harness/v2/harness.fds.bin";
const PROTOCOL_PROTO_PATH: &str = "protocol/v1/protocol.proto";
const PROTOCOL_DESCRIPTOR_PATH: &str = "protocol/v1/protocol.fds.bin";
const CONTROL_PROTO_PATH: &str = "transport/v1/transport.proto";
const CONTROL_DESCRIPTOR_PATH: &str = "transport/v1/transport.fds.bin";
const VALIDATION_OPTIONS_PROTO_PATH: &str = "validation/v1/options.proto";
const AUTHORITY_MANIFEST: &str = "rust-authority.json";
const RUST_FAMILY_GOLDENS: &str = "rust-family-goldens.json";
const TYPE_POLICY_PATH: &str = "type-policy.json";
const FILESYSTEM_PRODUCT_DESCRIPTOR: &str =
    "rust/crates/filesystem/src/generated/rust-model-filesystem-v2.bin";
const HARNESS_PRODUCT_DESCRIPTOR: &str =
    "rust/crates/harness/src/generated/rust-model-harness-v2.bin";
const HARNESS_PRODUCT_ARCHIVE: &str = "rust/crates/harness/src/generated/harness-archived-v2.bin";
const INFERENCE_PRODUCT_DESCRIPTOR: &str = "rust/crates/inference/inference_model_descriptor.bin";
const INFERENCE_CONTRACT_PRODUCT_DESCRIPTOR: &str =
    "rust/crates/inference-contract/inference_model_descriptor.bin";
const INFERENCE_PRODUCT_DOC_DESCRIPTOR: &str =
    "rust/crates/inference/inference_model_descriptor_docs.bin";
const INFERENCE_CONTRACT_PRODUCT_DOC_DESCRIPTOR: &str =
    "rust/crates/inference-contract/inference_model_descriptor_docs.bin";
const INFERENCE_PRODUCT_ARCHIVE: &str = "rust/crates/inference/inference_descriptor.bin";
const MACHINES_PRODUCT_DESCRIPTOR: &str =
    "rust/crates/machines/src/generated/acyclic-machines-v1.model.bin";
const MACHINES_PRODUCT_DOC_DESCRIPTOR: &str =
    "rust/crates/machines/src/generated/acyclic-machines-v1.model.docs.bin";
const MACHINES_PRODUCT_ARCHIVE: &str = "rust/crates/machines/src/generated/acyclic-machines-v1.bin";
const HARNESS_ARCHIVED_FIXTURE: &[u8] =
    include_bytes!("../../tests/fixtures/harness-v2.descriptor.bin");
const INFERENCE_ARCHIVED_FIXTURE: &[u8] =
    include_bytes!("../../tests/fixtures/inference-v1.descriptor.bin");
const MACHINES_ARCHIVED_FIXTURE: &[u8] =
    include_bytes!("../../tests/fixtures/machines-v1.descriptor.bin");
const ACTORS_ARCHIVED_DESCRIPTOR: &[u8] =
    include_bytes!("../../../actors/src/generated/acyclic-actors-v1.bin");
const WORKERS_ARCHIVED_DESCRIPTOR: &[u8] =
    include_bytes!("../../../workers/src/generated/acyclic-workers-v1.bin");
const OBJECTS_ARCHIVED_DESCRIPTOR: &[u8] =
    include_bytes!("../../../objects/src/generated/acyclic-objects-v2.bin");
const STREAM_ARCHIVED_DESCRIPTOR: &[u8] =
    include_bytes!("../../../stream/proto/stream/v2/stream_descriptor.bin");
const FILESYSTEM_ARCHIVED_DESCRIPTOR: &[u8] =
    include_bytes!("../../../filesystem/src/generated/acyclic-filesystem-v2.bin");
const PROTOCOL_ARCHIVED_FIXTURE: &[u8] =
    include_bytes!("../../tests/fixtures/protocol-v1.descriptor.bin");

fn public_field_direction_name(direction: PublicFieldDirection) -> &'static str {
    match direction {
        PublicFieldDirection::Request => "request",
        PublicFieldDirection::Response => "response",
        PublicFieldDirection::NestedMessage => "nested_message",
        PublicFieldDirection::EmbeddedOnly => "embedded_only",
    }
}

fn resolved_field_identity_json(field: &acyclic_sdk_contract_wire::ResolvedRequestField) -> Value {
    serde_json::json!({
        "family": field.family, "rpc": field.rpc, "root_message": field.root_message,
        "message_path": field.message_path, "field": field.field, "number": field.number,
        "json_name": field.json_name, "type_name": field.type_name,
        "oneof_name": field.oneof_name, "oneof_index": field.oneof_index,
        "proto3_optional": field.proto3_optional,
    })
}

fn type_policy_json() -> Vec<u8> {
    let language_profiles = TypePolicyLanguage::ALL
        .iter()
        .map(|language| {
            let profile = TYPE_PROJECTION_PROFILES
                .iter()
                .find(|candidate| candidate.language == *language)
                .expect("every Rust-owned language has a type projection profile");
            serde_json::json!({
                "language": language.id(),
                "nominal_types": profile.nominal_types,
                "unions": profile.unions,
                "refinements": profile.refinements,
                "presence": profile.presence,
                "unknown_values": profile.unknown_values,
                "integers": profile.integers,
                "checker": profile.checker,
            })
        })
        .collect::<Vec<_>>();
    let semantic_types = SEMANTIC_TYPES
        .iter()
        .map(|semantic_type| {
            let wire_kind = match semantic_type.wire_kind {
                WireValueKind::String => "string",
                WireValueKind::Bytes => "bytes",
                WireValueKind::SignedInteger => "signed_integer",
                WireValueKind::UnsignedInteger => "unsigned_integer",
                WireValueKind::Boolean => "boolean",
                WireValueKind::Timestamp => "timestamp",
                WireValueKind::Enum => "enum",
                WireValueKind::Message => "message",
                WireValueKind::Oneof => "oneof",
            };
            let rules = semantic_type
                .rules
                .iter()
                .map(|rule| match rule {
                    SemanticRule::NonEmpty => serde_json::json!({ "kind": "non_empty" }),
                    SemanticRule::Utf8 => serde_json::json!({ "kind": "utf8" }),
                    SemanticRule::NonNegative => serde_json::json!({ "kind": "non_negative" }),
                    SemanticRule::StrictlyPositive => {
                        serde_json::json!({ "kind": "strictly_positive" })
                    }
                    SemanticRule::FixedLength(length) => {
                        serde_json::json!({ "kind": "fixed_length", "length": length })
                    }
                    SemanticRule::MaxBytes(max) => {
                        serde_json::json!({ "kind": "max_bytes", "max": max })
                    }
                    SemanticRule::MaxItems(max) => {
                        serde_json::json!({ "kind": "max_items", "max": max })
                    }
                    SemanticRule::BoundedInteger { min, max } => {
                        serde_json::json!({ "kind": "bounded_integer", "min": min, "max": max })
                    }
                    SemanticRule::Sha256Digest => serde_json::json!({ "kind": "sha256_digest" }),
                    SemanticRule::Immutable => serde_json::json!({ "kind": "immutable" }),
                    SemanticRule::Monotonic => serde_json::json!({ "kind": "monotonic" }),
                    SemanticRule::CanonicalResourceName => {
                        serde_json::json!({ "kind": "canonical_resource_name" })
                    }
                    SemanticRule::ExactOneof => serde_json::json!({ "kind": "exact_oneof" }),
                    SemanticRule::ExplicitPresence => {
                        serde_json::json!({ "kind": "explicit_presence" })
                    }
                    SemanticRule::PreserveUnknownEnum => {
                        serde_json::json!({ "kind": "preserve_unknown_enum" })
                    }
                    SemanticRule::PreserveUnknownOneof => {
                        serde_json::json!({ "kind": "preserve_unknown_oneof" })
                    }
                })
                .collect::<Vec<_>>();
            serde_json::json!({
                "id": semantic_type.id,
                "rust_name": semantic_type.rust_name,
                "wire_kind": wire_kind,
                "rules": rules,
            })
        })
        .collect::<Vec<_>>();
    let field_mappings = FIELD_SEMANTIC_TYPES
        .iter()
        .map(|mapping| {
            serde_json::json!({
                "family": mapping.family,
                "field": mapping.field,
                "semantic_type": mapping.semantic_type,
            })
        })
        .collect::<Vec<_>>();
    let union_variants = WIRE_UNION_VARIANTS
        .iter()
        .map(|variant| {
            let payload_wire_kind = match variant.payload_wire_kind {
                WireValueKind::String => "string",
                WireValueKind::Bytes => "bytes",
                WireValueKind::SignedInteger => "signed_integer",
                WireValueKind::UnsignedInteger => "unsigned_integer",
                WireValueKind::Boolean => "boolean",
                WireValueKind::Timestamp => "timestamp",
                WireValueKind::Enum => "enum",
                WireValueKind::Message => "message",
                WireValueKind::Oneof => "oneof",
            };
            serde_json::json!({
                "union": variant.union,
                "variant": variant.variant,
                "tag": variant.tag,
                "payload_wire_kind": payload_wire_kind,
            })
        })
        .collect::<Vec<_>>();
    let public_field_bindings = PUBLIC_FIELD_BINDINGS
        .iter()
        .map(|binding| {
            serde_json::json!({
                "family": binding.family,
                "field": binding.field,
                "semantic_type": binding.semantic_type,
                "module": binding.module,
                "message": binding.message,
                "wire_field": binding.wire_field,
                "direction": public_field_direction_name(binding.direction),
            })
        })
        .collect::<Vec<_>>();
    let rpc_methods = acyclic_sdk_contract_wire::resolved_rpc_methods()
        .expect("Rust RPC descriptor inventory must resolve")
        .into_iter().map(|method| serde_json::json!({
            "family": method.family, "rpc": method.rpc,
            "service": method.service, "method": method.method,
            "input_message": method.input_message, "output_message": method.output_message,
            "client_streaming": method.client_streaming, "server_streaming": method.server_streaming,
        })).collect::<Vec<_>>();
    let enum_fields = acyclic_sdk_contract_wire::resolved_enum_fields()
        .expect("Rust enum descriptor inventory must resolve")
        .into_iter()
        .map(|item| {
            serde_json::json!({
                "field": resolved_field_identity_json(&item.field),
                "enum_type": item.enum_type,
                "values": item.values.into_iter().map(|value| serde_json::json!({
                    "name": value.name, "number": value.number,
                })).collect::<Vec<_>>(),
                "preserves_unknown_numeric": item.preserves_unknown_numeric,
            })
        })
        .collect::<Vec<_>>();
    let oneof_members = acyclic_sdk_contract_wire::resolved_oneof_members()
        .expect("Rust oneof descriptor inventory must resolve")
        .into_iter()
        .map(|item| {
            serde_json::json!({
                "field": resolved_field_identity_json(&item.field),
                "payload_protobuf_type": item.payload_kind.as_str_name(),
                "payload_type": item.payload_type,
                "preserves_unknown_members": item.preserves_unknown_members,
            })
        })
        .collect::<Vec<_>>();
    let presence_fields = acyclic_sdk_contract_wire::resolved_presence_fields()
        .expect("Rust presence descriptor inventory must resolve")
        .into_iter().map(|item| serde_json::json!({
            "field": resolved_field_identity_json(&item.field),
            "kind": match item.kind {
                acyclic_sdk_contract_wire::ResolvedPresenceKind::Message => "message",
                acyclic_sdk_contract_wire::ResolvedPresenceKind::Oneof => "oneof",
                acyclic_sdk_contract_wire::ResolvedPresenceKind::ExplicitOptional => "explicit_optional",
            },
        })).collect::<Vec<_>>();
    let document = serde_json::json!({
        "schema": "acyclic.sdk.type-policy.v1",
        "source": "rust/crates/sdk-contract-wire/src/type_policy.rs",
        "languages": language_profiles,
        "semantic_types": semantic_types,
        "field_mappings": field_mappings,
        "public_field_bindings": public_field_bindings,
        "union_variants": union_variants,
        "rpc_methods": rpc_methods,
        "enum_fields": enum_fields,
        "oneof_members": oneof_members,
        "presence_fields": presence_fields,
    });
    serde_json::to_vec_pretty(&document).expect("type policy JSON is serializable")
}

// Keep the provenance revision tied to the Rust model itself.  A Git commit
// can remain unchanged while a worktree is edited, so a commit-only marker is
// not sufficient for generation or package input validation.
const MODEL_SOURCES: &[(&str, &[u8])] = &[
    (
        "rust/crates/sdk-contract-wire/src/family_registry.rs",
        include_bytes!("../family_registry.rs"),
    ),
    (
        "rust/crates/sdk-contract-wire/src/credential.rs",
        include_bytes!("../credential.rs"),
    ),
    (
        "rust/crates/sdk-contract-wire/src/type_policy.rs",
        include_bytes!("../type_policy.rs"),
    ),
    (
        "rust/crates/sdk-contract-wire/src/lib.rs",
        include_bytes!("../lib.rs"),
    ),
    (
        "rust/crates/sdk-contract-wire/src/bindings.rs",
        include_bytes!("../bindings.rs"),
    ),
    (
        "rust/crates/sdk-contract-wire/src/stream.rs",
        include_bytes!("../stream.rs"),
    ),
    (
        "rust/crates/sdk-contract-wire/src/workers.rs",
        include_bytes!("../workers.rs"),
    ),
    (
        "rust/crates/sdk-contract-wire/src/objects.rs",
        include_bytes!("../objects.rs"),
    ),
    (
        "rust/crates/sdk-contract-wire/src/filesystem.rs",
        include_bytes!("../filesystem.rs"),
    ),
    (
        "rust/crates/sdk-contract-wire/src/harness.rs",
        include_bytes!("../harness.rs"),
    ),
    (
        "rust/crates/sdk-contract-wire/src/inference.rs",
        include_bytes!("../inference.rs"),
    ),
    (
        "rust/crates/sdk-contract-wire/src/machines.rs",
        include_bytes!("../machines.rs"),
    ),
    (
        "rust/crates/sdk-contract-wire/src/transport_control.rs",
        include_bytes!("../transport_control.rs"),
    ),
    (
        "rust/crates/sdk-contract-wire/src/protocol.rs",
        include_bytes!("../protocol.rs"),
    ),
    (
        "rust/crates/sdk-contract-wire/src/bin/sdk-contract-wire.rs",
        include_bytes!("sdk-contract-wire.rs"),
    ),
    (
        "rust/crates/sdk-contract-wire/Cargo.toml",
        include_bytes!("../../Cargo.toml"),
    ),
    (
        "rust/crates/sdk-contract-wire/Cargo.lock",
        include_bytes!("../../Cargo.lock"),
    ),
    (
        "rust/crates/sdk-contract-options/src/lib.rs",
        include_bytes!("../../../sdk-contract-options/src/lib.rs"),
    ),
    (
        "rust/crates/sdk-contract-validation/src/lib.rs",
        include_bytes!("../../../sdk-contract-validation/src/lib.rs"),
    ),
    (
        "rust/crates/sdk-contract-options/Cargo.toml",
        include_bytes!("../../../sdk-contract-options/Cargo.toml"),
    ),
    (
        "rust/crates/sdk-contract-options/Cargo.lock",
        include_bytes!("../../../sdk-contract-options/Cargo.lock"),
    ),
    (
        "rust/crates/sdk-contract-validation/Cargo.toml",
        include_bytes!("../../../sdk-contract-validation/Cargo.toml"),
    ),
    (
        "rust/crates/sdk-contract-validation/Cargo.lock",
        include_bytes!("../../../sdk-contract-validation/Cargo.lock"),
    ),
];

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let command = args.next().ok_or("expected generate or check")?;
    let mut out = None;
    let mut root = None;
    let mut evidence = None;
    while let Some(argument) = args.next() {
        if argument == "--out" {
            out = Some(PathBuf::from(
                args.next().ok_or("--out requires a directory")?,
            ));
        } else if argument == "--root" {
            root = Some(PathBuf::from(
                args.next()
                    .ok_or("--root requires a repository directory")?,
            ));
        } else if argument == "--evidence" {
            evidence = Some(PathBuf::from(
                args.next().ok_or("--evidence requires a JSON file")?,
            ));
        } else {
            return Err(format!("unknown argument: {argument}").into());
        }
    }

    match command.as_str() {
        "generate" => generate(
            &out.ok_or("generate requires --out")?,
            root.as_deref(),
            evidence.as_deref(),
        ),
        "check" => check(
            &out.ok_or("check requires --out")?,
            root.as_deref(),
            evidence.as_deref(),
        ),
        "generate-products" => {
            let root = root.ok_or("generate-products requires --root")?;
            let destination = out.as_deref().unwrap_or(root.as_path());
            generate_products(&root, destination)
        }
        "check-products" => {
            let root = root.ok_or("check-products requires --root")?;
            let destination = out.as_deref().unwrap_or(root.as_path());
            check_products(&root, destination)
        }
        _ => Err(format!(
            "unknown command: {command}; expected generate, check, generate-products, or check-products"
        )
        .into()),
    }
}

fn product_artifacts(root: &Path) -> Result<Vec<(String, Vec<u8>)>, Box<dyn Error>> {
    let mut artifacts = vec![
        (
            "generated/sdk/type-policy.json".to_owned(),
            type_policy_json(),
        ),
        (
            "python/src/acyclic_sdk/py.typed".to_owned(),
            b"Rust-generated semantic type projections.\n".to_vec(),
        ),
        (
            FILESYSTEM_PRODUCT_DESCRIPTOR.to_owned(),
            filesystem_descriptor(),
        ),
        (HARNESS_PRODUCT_DESCRIPTOR.to_owned(), harness_descriptor()),
        (
            HARNESS_PRODUCT_ARCHIVE.to_owned(),
            HARNESS_ARCHIVED_FIXTURE.to_vec(),
        ),
        (
            INFERENCE_PRODUCT_DESCRIPTOR.to_owned(),
            inference_descriptor(),
        ),
        (
            INFERENCE_CONTRACT_PRODUCT_DESCRIPTOR.to_owned(),
            inference_descriptor(),
        ),
        (
            INFERENCE_PRODUCT_DOC_DESCRIPTOR.to_owned(),
            descriptor_set_with_docs(BindingFamily::Inference, &inference_descriptor())?,
        ),
        (
            INFERENCE_CONTRACT_PRODUCT_DOC_DESCRIPTOR.to_owned(),
            descriptor_set_with_docs(BindingFamily::Inference, &inference_descriptor())?,
        ),
        (
            INFERENCE_PRODUCT_ARCHIVE.to_owned(),
            INFERENCE_ARCHIVED_FIXTURE.to_vec(),
        ),
        (
            MACHINES_PRODUCT_DESCRIPTOR.to_owned(),
            machines_descriptor(),
        ),
        (
            MACHINES_PRODUCT_DOC_DESCRIPTOR.to_owned(),
            descriptor_set_with_docs(BindingFamily::Machines, &machines_descriptor())?,
        ),
        (
            MACHINES_PRODUCT_ARCHIVE.to_owned(),
            MACHINES_ARCHIVED_FIXTURE.to_vec(),
        ),
    ];
    let root_key = sha256_hex(root.to_string_lossy().as_bytes());
    let staging = std::env::temp_dir().join(format!(
        "acyclic-sdk-product-bindings-{}-{}",
        std::process::id(),
        &root_key[..16]
    ));
    let _ = fs::remove_dir_all(&staging);
    for family in [
        BindingFamily::Actors,
        BindingFamily::Workers,
        BindingFamily::Objects,
    ] {
        let family_root = staging.join(family.name());
        generate_product_bindings(family, &family_root)?;
        let (canonical, package) = match family {
            BindingFamily::Actors => (
                "acyclic/actors/v1/acyclic.actors.v1.rs",
                "rust/crates/actors/src/generated/acyclic.actors.v1.rs",
            ),
            BindingFamily::Workers => (
                "acyclic/workers/v1/acyclic.workers.v1.rs",
                "rust/crates/workers/src/generated/acyclic.workers.v1.rs",
            ),
            BindingFamily::Objects => (
                "acyclic/objects/v2/acyclic.objects.v2.rs",
                "rust/crates/objects/src/generated/acyclic.objects.v2.rs",
            ),
            _ => unreachable!(),
        };
        let canonical_path = family_root.join(canonical);
        let source = fs::read(&canonical_path)?;
        artifacts.push((format!("generated/rust/{canonical}"), source.clone()));
        artifacts.push((package.to_owned(), source));
        let tonic = canonical.trim_end_matches(".rs").to_owned() + ".tonic.rs";
        let tonic_path = family_root.join(&tonic);
        let tonic_source = fs::read(&tonic_path)?;
        artifacts.push((format!("generated/rust/{tonic}"), tonic_source.clone()));
        let package_tonic = package.trim_end_matches(".rs").to_owned() + ".tonic.rs";
        artifacts.push((package_tonic, tonic_source));
        if matches!(family, BindingFamily::Actors | BindingFamily::Workers) {
            for control_file in ["acyclic.protocol.v1.rs", "acyclic.transport.v1.rs"] {
                artifacts.push((
                    format!("rust/crates/{}/src/generated/{control_file}", family.name()),
                    fs::read(family_root.join(control_file))?,
                ));
            }
            artifacts.push((
                format!(
                    "rust/crates/{}/src/generated/platform-client-methods.rs",
                    family.name()
                ),
                fs::read(family_root.join("platform-client-methods.rs"))?,
            ));
        }
    }
    for facade in generate_remote_facades() {
        artifacts.push((facade.path.to_owned(), facade.source.into_bytes()));
    }
    for (path, source) in generate_jvm_semantic_types() {
        artifacts.push((path.to_owned(), source.into_bytes()));
    }
    for (path, source) in generate_jvm_typed_requests() {
        artifacts.push((path.to_owned(), source.into_bytes()));
    }
    for (path, source) in generate_jvm_typed_clients() {
        artifacts.push((path.to_owned(), source.into_bytes()));
    }
    for (path, source) in generate_jvm_typed_responses() {
        artifacts.push((path.to_owned(), source.into_bytes()));
    }
    let (path, source) = generate_csharp_typed_facade();
    artifacts.push((path.to_owned(), source.into_bytes()));
    for (path, source) in generate_swift_cpp_typed_facades() {
        artifacts.push((path.to_owned(), source.into_bytes()));
    }
    for facade in generate_portable_typed_facades() {
        artifacts.push((facade.path.to_owned(), facade.source.into_bytes()));
    }
    for facade in generate_embedded_facades() {
        artifacts.push((facade.path.to_owned(), facade.source.into_bytes()));
    }
    for (path, source) in generate_type_policy_qualification_tests() {
        artifacts.push((path.to_owned(), source.into_bytes()));
    }
    let _ = fs::remove_dir_all(staging);
    Ok(artifacts)
}

fn generate_products(root: &Path, destination: &Path) -> Result<(), Box<dyn Error>> {
    for (relative, expected) in product_artifacts(root)? {
        let path = destination.join(relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&path, expected)?;
        println!("generated {}", path.display());
    }
    Ok(())
}

fn check_products(root: &Path, destination: &Path) -> Result<(), Box<dyn Error>> {
    let mut drifted = Vec::new();
    for (relative, expected) in product_artifacts(root)? {
        let path = destination.join(relative);
        match fs::read(&path) {
            Ok(actual) if actual == expected => println!("checked {}", path.display()),
            Ok(_) => drifted.push((path, None)),
            Err(error) => drifted.push((path, Some(error.to_string()))),
        }
    }
    if !drifted.is_empty() {
        for (path, error) in &drifted {
            match error {
                Some(error) => eprintln!(
                    "product artifact drifted: {} (cannot read: {error})",
                    path.display()
                ),
                None => eprintln!("product artifact drifted: {}", path.display()),
            }
        }
        return Err(format!("{} product artifacts drifted", drifted.len()).into());
    }
    Ok(())
}

fn generate(
    out: &Path,
    source_root: Option<&Path>,
    evidence_path: Option<&Path>,
) -> Result<(), Box<dyn Error>> {
    generate_source(out, VALIDATION_OPTIONS_PROTO_PATH, options_proto())?;
    generate_contract(
        out,
        PROTO_PATH,
        DESCRIPTOR_PATH,
        actors_proto(),
        actors_descriptor(),
    )?;
    generate_contract(
        out,
        STREAM_PROTO_PATH,
        STREAM_DESCRIPTOR_PATH,
        stream_proto(),
        stream_descriptor(),
    )?;
    generate_contract(
        out,
        OBJECTS_PROTO_PATH,
        OBJECTS_DESCRIPTOR_PATH,
        objects_proto(),
        objects_descriptor(),
    )?;
    generate_contract(
        out,
        WORKERS_PROTO_PATH,
        WORKERS_DESCRIPTOR_PATH,
        workers_proto(),
        workers_descriptor(),
    )?;
    generate_contract(
        out,
        FILESYSTEM_PROTO_PATH,
        FILESYSTEM_DESCRIPTOR_PATH,
        filesystem_proto(),
        filesystem_descriptor(),
    )?;
    generate_contract(
        out,
        HARNESS_PROTO_PATH,
        HARNESS_DESCRIPTOR_PATH,
        harness_proto(),
        harness_descriptor(),
    )?;
    generate_contract(
        out,
        PROTOCOL_PROTO_PATH,
        PROTOCOL_DESCRIPTOR_PATH,
        protocol_proto(),
        protocol_descriptor(),
    )?;
    generate_contract(
        out,
        CONTROL_PROTO_PATH,
        CONTROL_DESCRIPTOR_PATH,
        control_proto(),
        control_descriptor(),
    )?;
    generate_contract(
        out,
        INFERENCE_PROTO_PATH,
        INFERENCE_DESCRIPTOR_PATH,
        inference_proto(),
        inference_descriptor(),
    )?;
    generate_contract(
        out,
        MACHINES_PROTO_PATH,
        MACHINES_DESCRIPTOR_PATH,
        machines_proto(),
        machines_descriptor(),
    )?;
    let manifest = authority_manifest(out, source_root, evidence_path)?;
    fs::write(out.join(AUTHORITY_MANIFEST), &manifest)?;
    fs::write(out.join(TYPE_POLICY_PATH), type_policy_json())?;
    let manifest_hash = sha256_hex(manifest.as_bytes());
    fs::write(
        out.join(RUST_FAMILY_GOLDENS),
        rust_family_goldens_json(&manifest_hash),
    )?;
    println!("generated {}", out.join(AUTHORITY_MANIFEST).display());
    println!("generated {}", out.join(RUST_FAMILY_GOLDENS).display());
    Ok(())
}

fn check(
    out: &Path,
    source_root: Option<&Path>,
    evidence_path: Option<&Path>,
) -> Result<(), Box<dyn Error>> {
    check_source(out, VALIDATION_OPTIONS_PROTO_PATH, options_proto())?;
    check_contract(
        out,
        PROTO_PATH,
        DESCRIPTOR_PATH,
        actors_proto(),
        actors_descriptor(),
    )?;
    check_contract(
        out,
        STREAM_PROTO_PATH,
        STREAM_DESCRIPTOR_PATH,
        stream_proto(),
        stream_descriptor(),
    )?;
    check_contract(
        out,
        OBJECTS_PROTO_PATH,
        OBJECTS_DESCRIPTOR_PATH,
        objects_proto(),
        objects_descriptor(),
    )?;
    check_contract(
        out,
        WORKERS_PROTO_PATH,
        WORKERS_DESCRIPTOR_PATH,
        workers_proto(),
        workers_descriptor(),
    )?;
    check_contract(
        out,
        FILESYSTEM_PROTO_PATH,
        FILESYSTEM_DESCRIPTOR_PATH,
        filesystem_proto(),
        filesystem_descriptor(),
    )?;
    check_contract(
        out,
        HARNESS_PROTO_PATH,
        HARNESS_DESCRIPTOR_PATH,
        harness_proto(),
        harness_descriptor(),
    )?;
    check_contract(
        out,
        PROTOCOL_PROTO_PATH,
        PROTOCOL_DESCRIPTOR_PATH,
        protocol_proto(),
        protocol_descriptor(),
    )?;
    check_contract(
        out,
        CONTROL_PROTO_PATH,
        CONTROL_DESCRIPTOR_PATH,
        control_proto(),
        control_descriptor(),
    )?;
    check_contract(
        out,
        INFERENCE_PROTO_PATH,
        INFERENCE_DESCRIPTOR_PATH,
        inference_proto(),
        inference_descriptor(),
    )?;
    check_contract(
        out,
        MACHINES_PROTO_PATH,
        MACHINES_DESCRIPTOR_PATH,
        machines_proto(),
        machines_descriptor(),
    )?;
    reject_extra_artifacts(out)?;
    let type_policy = fs::read(out.join(TYPE_POLICY_PATH)).map_err(|error| {
        format!(
            "cannot read {}: {error}",
            out.join(TYPE_POLICY_PATH).display()
        )
    })?;
    if type_policy != type_policy_json() {
        return Err(format!(
            "Rust type policy is stale or does not bind the Rust model: {}",
            out.join(TYPE_POLICY_PATH).display()
        )
        .into());
    }
    let manifest = fs::read_to_string(out.join(AUTHORITY_MANIFEST)).map_err(|error| {
        format!(
            "cannot read {}: {error}",
            out.join(AUTHORITY_MANIFEST).display()
        )
    })?;
    let expected = authority_manifest(out, source_root, evidence_path)?;
    if manifest != expected {
        return Err(format!(
            "authority manifest is stale or does not bind Rust artifacts: {}",
            out.join(AUTHORITY_MANIFEST).display()
        )
        .into());
    }
    let goldens = fs::read_to_string(out.join(RUST_FAMILY_GOLDENS)).map_err(|error| {
        format!(
            "cannot read {}: {error}",
            out.join(RUST_FAMILY_GOLDENS).display()
        )
    })?;
    let expected_goldens = rust_family_goldens_json(&sha256_hex(expected.as_bytes()));
    if goldens != expected_goldens {
        return Err(format!(
            "Rust family goldens are stale or do not bind the authority manifest: {}",
            out.join(RUST_FAMILY_GOLDENS).display()
        )
        .into());
    }
    println!("checked {}", out.join(AUTHORITY_MANIFEST).display());
    println!("checked {}", out.join(RUST_FAMILY_GOLDENS).display());
    Ok(())
}

fn reject_extra_artifacts(out: &Path) -> Result<(), Box<dyn Error>> {
    let expected = [
        VALIDATION_OPTIONS_PROTO_PATH,
        PROTO_PATH,
        DESCRIPTOR_PATH,
        STREAM_PROTO_PATH,
        STREAM_DESCRIPTOR_PATH,
        OBJECTS_PROTO_PATH,
        OBJECTS_DESCRIPTOR_PATH,
        WORKERS_PROTO_PATH,
        WORKERS_DESCRIPTOR_PATH,
        FILESYSTEM_PROTO_PATH,
        FILESYSTEM_DESCRIPTOR_PATH,
        HARNESS_PROTO_PATH,
        HARNESS_DESCRIPTOR_PATH,
        PROTOCOL_PROTO_PATH,
        PROTOCOL_DESCRIPTOR_PATH,
        CONTROL_PROTO_PATH,
        CONTROL_DESCRIPTOR_PATH,
        INFERENCE_PROTO_PATH,
        INFERENCE_DESCRIPTOR_PATH,
        MACHINES_PROTO_PATH,
        MACHINES_DESCRIPTOR_PATH,
        AUTHORITY_MANIFEST,
        RUST_FAMILY_GOLDENS,
        TYPE_POLICY_PATH,
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<BTreeSet<_>>();

    fn visit(
        root: &Path,
        current: &Path,
        files: &mut BTreeSet<String>,
    ) -> Result<(), Box<dyn Error>> {
        for entry in fs::read_dir(current)? {
            let path = entry?.path();
            if path.is_dir() {
                visit(root, &path, files)?;
            } else {
                files.insert(
                    path.strip_prefix(root)?
                        .to_string_lossy()
                        .replace('\\', "/"),
                );
            }
        }
        Ok(())
    }

    let mut actual = BTreeSet::new();
    visit(out, out, &mut actual)?;
    if let Some(extra) = actual.difference(&expected).next() {
        return Err(format!("extra generated artifact: {}", out.join(extra).display()).into());
    }
    Ok(())
}

/// Build the authority marker from the files just emitted by this exporter.
///
/// The source and descriptor digests make it impossible for a caller to
/// relabel a copied legacy `.proto` as Rust-owned. `canonical_schema` is the
/// normalized source-of-truth descriptor role; runtime handshake descriptors
/// remain external compatibility fixtures until a protocol transition adopts
/// the new bytes explicitly.
fn authority_manifest(
    out: &Path,
    source_root: Option<&Path>,
    evidence_path: Option<&Path>,
) -> Result<String, Box<dyn Error>> {
    let families = [
        (
            PROTO_PATH,
            DESCRIPTOR_PATH,
            "rust/crates/actors/src/generated/acyclic-actors-v1.bin",
            ACTORS_ARCHIVED_DESCRIPTOR,
        ),
        (
            STREAM_PROTO_PATH,
            STREAM_DESCRIPTOR_PATH,
            "rust/crates/stream/proto/stream/v2/stream_descriptor.bin",
            STREAM_ARCHIVED_DESCRIPTOR,
        ),
        (
            OBJECTS_PROTO_PATH,
            OBJECTS_DESCRIPTOR_PATH,
            "rust/crates/objects/src/generated/acyclic-objects-v2.bin",
            OBJECTS_ARCHIVED_DESCRIPTOR,
        ),
        (
            WORKERS_PROTO_PATH,
            WORKERS_DESCRIPTOR_PATH,
            "rust/crates/workers/src/generated/acyclic-workers-v1.bin",
            WORKERS_ARCHIVED_DESCRIPTOR,
        ),
        (
            FILESYSTEM_PROTO_PATH,
            FILESYSTEM_DESCRIPTOR_PATH,
            "rust/crates/filesystem/src/generated/acyclic-filesystem-v2.bin",
            FILESYSTEM_ARCHIVED_DESCRIPTOR,
        ),
        (
            HARNESS_PROTO_PATH,
            HARNESS_DESCRIPTOR_PATH,
            "rust/crates/harness/src/generated/harness-archived-v2.bin",
            HARNESS_ARCHIVED_FIXTURE,
        ),
        (
            PROTOCOL_PROTO_PATH,
            PROTOCOL_DESCRIPTOR_PATH,
            "rust/crates/sdk-contract-wire/tests/fixtures/protocol-v1.descriptor.bin",
            PROTOCOL_ARCHIVED_FIXTURE,
        ),
        (
            INFERENCE_PROTO_PATH,
            INFERENCE_DESCRIPTOR_PATH,
            "rust/crates/inference/inference_descriptor.bin",
            INFERENCE_ARCHIVED_FIXTURE,
        ),
        (
            MACHINES_PROTO_PATH,
            MACHINES_DESCRIPTOR_PATH,
            "rust/crates/machines/src/generated/acyclic-machines-v1.bin",
            MACHINES_ARCHIVED_FIXTURE,
        ),
    ];
    let source_revision = model_source_revision();
    let source_git_sha = source_git_sha(source_root)?;
    let evidence = load_typed_wire_evidence(evidence_path, &source_git_sha, source_root)?;
    let mut entries = String::new();
    let mut known_rpcs = BTreeSet::new();
    for (index, (source, descriptor, handshake_descriptor, handshake_bytes)) in
        families.iter().enumerate()
    {
        let source_bytes = fs::read(out.join(source))?;
        let descriptor_bytes = fs::read(out.join(descriptor))?;
        if index != 0 {
            entries.push_str(",\n");
        }
        let (rpc_shapes, rpc_methods, family_rpcs) =
            rpc_shapes_json(&descriptor_bytes, &evidence.entries)?;
        known_rpcs.extend(family_rpcs);
        entries.push_str(&format!(
            "    {{\n      \"source\": \"{source}\",\n      \"source_sha256\": \"{}\",\n      \"descriptor\": \"{descriptor}\",\n      \"descriptor_sha256\": \"{}\",\n      \"schema_descriptor\": \"{descriptor}\",\n      \"schema_descriptor_sha256\": \"{}\",\n      \"descriptor_role\": \"canonical_schema\",\n      \"handshake_descriptor\": \"{handshake_descriptor}\",\n      \"handshake_descriptor_sha256\": \"{}\",\n      \"handshake_descriptor_role\": \"preserved_runtime_fixture\",\n      \"rpc_shapes\": {},\n      \"rpc_methods\": {}\n    }}",
            sha256_hex(&source_bytes),
            sha256_hex(&descriptor_bytes),
            sha256_hex(&descriptor_bytes),
            sha256_hex(handshake_bytes),
            rpc_shapes,
            rpc_methods,
        ));
    }
    if let Some(unknown) = evidence
        .entries
        .keys()
        .find(|rpc| !known_rpcs.contains(*rpc))
    {
        return Err(
            format!("typed wire evidence RPC is absent from Rust descriptors: {unknown}").into(),
        );
    }
    let control_plane = serde_json::json!({
        "source": CONTROL_PROTO_PATH,
        "source_sha256": sha256_hex(&fs::read(out.join(CONTROL_PROTO_PATH))?),
        "descriptor": CONTROL_DESCRIPTOR_PATH,
        "descriptor_sha256": sha256_hex(&fs::read(out.join(CONTROL_DESCRIPTOR_PATH))?),
        "descriptor_role": "independent_control_schema",
        "handshake_rpc": acyclic_sdk_contract_wire::transport_control::HANDSHAKE_RPC_PATH,
        "family_metadata_key": acyclic_sdk_contract_wire::transport_control::FAMILY_METADATA_KEY,
        "non_mutating": true
    });
    Ok(format!(
        "{{\n  \"schema\": \"acyclic.sdk.rust-authority.v1\",\n  \"authority\": \"rust\",\n  \"schema_root\": \"rust/crates/sdk-contract-wire\",\n  \"source_git_sha\": \"{source_git_sha}\",\n  \"source_git_sha_kind\": \"git-revision\",\n  \"source_revision\": \"{source_revision}\",\n  \"source_revision_kind\": \"rust-model-sha256\",\n  \"source_files\": {source_files},\n  \"source_file_hashes\": {source_file_hashes},\n  \"exporter\": \"acyclic-sdk-contract-wire@{version}\",\n  \"typed_wire_evidence\": {typed_wire_evidence},\n  \"control_plane\": {control_plane},\n  \"families\": [\n{entries}\n  ]\n}}\n",
        source_files = model_source_files_json(),
        source_file_hashes = model_source_hashes_json(),
        source_git_sha = source_git_sha,
        version = env!("CARGO_PKG_VERSION"),
        typed_wire_evidence = evidence.manifest_json
    ))
}

#[derive(Debug)]
struct TypedWireEvidence {
    entries: BTreeMap<String, Value>,
    manifest_json: String,
}

fn load_typed_wire_evidence(
    path: Option<&Path>,
    expected_source_revision: &str,
    source_root: Option<&Path>,
) -> Result<TypedWireEvidence, Box<dyn Error>> {
    let Some(path) = path else {
        return Ok(TypedWireEvidence {
            entries: BTreeMap::new(),
            manifest_json: "null".to_owned(),
        });
    };
    let bytes = fs::read(path)?;
    let document: Value = serde_json::from_slice(&bytes)?;
    if document.get("schema").and_then(Value::as_str) != Some("acyclic.sdk.transport-fixtures.v1") {
        return Err("evidence must be an sdk-examples transport-fixtures manifest".into());
    }
    if document.get("generator").and_then(Value::as_str) != Some("acyclic-sdk-examples@0.2.0") {
        return Err("evidence must be emitted by the pinned Rust sdk-examples producer".into());
    }
    let source = document
        .get("source")
        .and_then(Value::as_object)
        .ok_or("evidence must contain source metadata")?;
    if source.get("path").and_then(Value::as_str) != Some("rust/crates/sdk-examples") {
        return Err("evidence source.path must identify rust/crates/sdk-examples".into());
    }
    let source_sha256 = source
        .get("sha256")
        .and_then(Value::as_str)
        .ok_or("evidence source.sha256 is missing")?;
    if !is_sha256(source_sha256) {
        return Err("evidence source.sha256 must be a sha256 digest".into());
    }
    let model_source_digest = source
        .get("model_source_digest")
        .and_then(Value::as_str)
        .ok_or("evidence source.model_source_digest is missing")?;
    if !is_sha256(model_source_digest) {
        return Err("evidence source.model_source_digest must be a sha256 digest".into());
    }
    let build_recipe_sha256 = source
        .get("build_recipe_sha256")
        .and_then(Value::as_str)
        .ok_or("evidence source.build_recipe_sha256 is missing")?;
    if !is_sha256(build_recipe_sha256) {
        return Err("evidence source.build_recipe_sha256 must be a sha256 digest".into());
    }
    let fixture_server = document
        .pointer("/qualification/fixture_server")
        .ok_or("evidence must contain qualification.fixture_server")?;
    if fixture_server.get("source_sha256").and_then(Value::as_str) != Some(source_sha256) {
        return Err("evidence fixture_server.source_sha256 does not match source.sha256".into());
    }
    if fixture_server.get("command").and_then(Value::as_str)
        != Some(
            "cargo run --manifest-path rust/crates/sdk-examples/Cargo.toml --bin fixture-server -- --port 0",
        )
    {
        return Err(
            "evidence fixture_server.command is not the Rust fixture-server command".into(),
        );
    }
    let source_files = source
        .get("files")
        .and_then(Value::as_array)
        .ok_or("evidence source.files is missing")?;
    if source_files.is_empty() {
        return Err("evidence source.files must not be empty".into());
    }
    if let Some(root) = source_root {
        for file in source_files {
            let relative = file
                .as_str()
                .ok_or("evidence source.files entries must be strings")?;
            let relative_path = Path::new(relative);
            if relative_path.is_absolute()
                || relative_path
                    .components()
                    .any(|component| matches!(component, std::path::Component::ParentDir))
            {
                return Err(format!("evidence source file escapes Rust root: {relative}").into());
            }
            if !root.join(relative_path).is_file() {
                return Err(format!("evidence source file is missing: {relative}").into());
            }
        }
    }
    let evidence = document
        .pointer("/qualification/typed_wire_evidence")
        .or_else(|| document.pointer("/typed_wire_evidence"))
        .and_then(Value::as_array)
        .ok_or("evidence must contain qualification.typed_wire_evidence")?;
    let source_revision = document
        .pointer("/source/revision")
        .and_then(Value::as_str)
        .ok_or("evidence must contain source.revision")?;
    if source_revision.len() != 40 || !source_revision.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err("evidence source.revision must be a 40-character Git OID".into());
    }
    if source_revision != expected_source_revision {
        return Err(format!(
            "evidence source.revision {source_revision} does not match Rust source {expected_source_revision}"
        )
        .into());
    }
    let mut entries = BTreeMap::new();
    for item in evidence {
        let object = item
            .as_object()
            .ok_or("typed wire evidence entry must be an object")?;
        let rpc = object
            .get("rpc")
            .and_then(Value::as_str)
            .ok_or("typed wire evidence entry is missing rpc")?;
        if object
            .get("source")
            .and_then(Value::as_str)
            .is_none_or(|source| !source.starts_with("rust/crates/sdk-examples/"))
        {
            return Err(format!("typed wire evidence {rpc} is not Rust-source-bound").into());
        }
        let request = object
            .get("request")
            .ok_or(format!("typed wire evidence {rpc} is missing request"))?;
        let response = object
            .get("response")
            .ok_or(format!("typed wire evidence {rpc} is missing response"))?;
        validate_wire_bytes(request, &format!("{rpc} request"))?;
        if response.get("status").and_then(Value::as_str).is_none()
            && (response
                .get("bytes_base64")
                .and_then(Value::as_str)
                .is_none()
                || response.get("sha256").and_then(Value::as_str).is_none())
        {
            return Err(format!(
                "typed wire evidence {rpc} response is neither status- nor byte-bound"
            )
            .into());
        }
        if response.get("status").and_then(Value::as_str).is_none() {
            validate_wire_bytes(response, &format!("{rpc} response"))?;
        }
        if entries.insert(rpc.to_owned(), item.clone()).is_some() {
            return Err(format!("duplicate typed wire evidence for {rpc}").into());
        }
    }
    let manifest = serde_json::json!({
        "schema": "acyclic.sdk.rust-typed-wire-evidence.v1",
        "path": path.to_string_lossy(),
        "sha256": sha256_hex(&bytes),
        "source_revision": source_revision,
        "count": entries.len(),
        "entries": entries.values().collect::<Vec<_>>(),
    });
    Ok(TypedWireEvidence {
        entries,
        manifest_json: serde_json::to_string(&manifest)?,
    })
}

fn validate_wire_bytes(value: &Value, label: &str) -> Result<(), Box<dyn Error>> {
    let encoded = value
        .get("bytes_base64")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{label} is missing bytes_base64"))?;
    let expected = value
        .get("sha256")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{label} is missing sha256"))?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|error| format!("{label} has invalid base64: {error}"))?;
    let observed = format!("sha256:{}", sha256_hex(&bytes));
    if expected != observed {
        return Err(
            format!("{label} sha256 {expected} does not match decoded bytes {observed}").into(),
        );
    }
    Ok(())
}

fn source_git_sha(source_root: Option<&Path>) -> Result<String, Box<dyn Error>> {
    let root = source_root
        .map(Path::to_path_buf)
        .or_else(|| env::var_os("ACYCLIC_SDK_SOURCE_ROOT").map(PathBuf::from))
        .or_else(|| env::current_dir().ok())
        .ok_or("cannot determine Rust source root for Git provenance")?;
    let output = Command::new("git")
        .args(["-C", root.to_string_lossy().as_ref(), "rev-parse", "HEAD"])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "cannot determine Rust source Git revision in {}: {}",
            root.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    let revision = String::from_utf8(output.stdout)?.trim().to_owned();
    if revision.len() != 40 || !revision.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(
            format!("Rust source Git revision is not a 40-character SHA: {revision}").into(),
        );
    }
    Ok(revision.to_lowercase())
}

fn rpc_shapes_json(
    descriptor_bytes: &[u8],
    typed_wire_evidence: &BTreeMap<String, Value>,
) -> Result<(String, String, BTreeSet<String>), Box<dyn Error>> {
    let descriptor = FileDescriptorSet::decode(descriptor_bytes)?;
    let mut shapes = BTreeSet::new();
    let mut methods = Vec::new();
    for file in &descriptor.file {
        let package = file.package.as_deref().unwrap_or_default();
        for service in &file.service {
            let service_name = service.name.as_deref().unwrap_or_default();
            for method in &service.method {
                let method_name = method.name.as_deref().unwrap_or_default();
                let shape = match (
                    method.client_streaming.unwrap_or(false),
                    method.server_streaming.unwrap_or(false),
                ) {
                    (false, false) => "unary",
                    (true, false) => "client",
                    (false, true) => "server",
                    (true, true) => "bidi",
                };
                shapes.insert(shape);
                let rpc = format!("{package}.{service_name}/{method_name}");
                let response = method
                    .output_type
                    .clone()
                    .unwrap_or_else(|| "<missing>".to_owned())
                    .trim_start_matches('.')
                    .to_owned();
                let request = method
                    .input_type
                    .clone()
                    .unwrap_or_else(|| "<missing>".to_owned())
                    .trim_start_matches('.')
                    .to_owned();
                if let Some(evidence) = typed_wire_evidence.get(&rpc) {
                    let evidence_request = evidence
                        .pointer("/request/type")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            format!("typed wire evidence {rpc} request type is missing")
                        })?;
                    if evidence_request != request {
                        return Err(format!(
                            "typed wire evidence {rpc} request type {evidence_request} does not match Rust descriptor {request}"
                        )
                        .into());
                    }
                    let evidence_response = evidence
                        .pointer("/response/type")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            format!("typed wire evidence {rpc} response type is missing")
                        })?;
                    if evidence_response != response {
                        return Err(format!(
                            "typed wire evidence {rpc} response type {evidence_response} does not match Rust descriptor {response}"
                        )
                        .into());
                    }
                }
                let response_fields = message_fields(&descriptor, &response);
                let validations = family_for_package(&package)
                    .and_then(family_view)
                    .and_then(|family| {
                        family
                            .operation_policies
                            .iter()
                            .find(|policy| policy.rpc == rpc)
                    })
                    .map(|policy| policy.validations.to_vec())
                    .unwrap_or_default();
                let response_rules = validations
                    .iter()
                    .filter(|validation| {
                        !validation.starts_with("request_identity")
                            && (validation.contains("response")
                                || validation.contains("identity")
                                || validation.contains("status")
                                || validation.contains("terminal")
                                || validation.contains("cursor")
                                || validation.contains("delivery")
                                || validation.contains("outcome"))
                    })
                    .map(|validation| format!("\"{validation}\""))
                    .collect::<Vec<_>>()
                    .join(",");
                let semantic_expectations = semantic_oracle::expectation_json(&rpc);
                let fields = response_fields
                    .iter()
                    .map(|field| format!("\"{field}\""))
                    .collect::<Vec<_>>()
                    .join(",");
                let all_validations = validations
                    .iter()
                    .map(|validation| format!("\"{validation}\""))
                    .collect::<Vec<_>>()
                    .join(",");
                let semantic_expectations_field = semantic_expectations
                    .map(|value| format!(",\"semantic_expectations\":{value}"))
                    .unwrap_or_default();
                let typed_wire_evidence_field = typed_wire_evidence
                    .get(&rpc)
                    .map(|value| {
                        format!(
                            ",\"typed_wire_evidence\":{}",
                            serde_json::to_string(value).expect("evidence is JSON")
                        )
                    })
                    .unwrap_or_default();
                methods.push(format!(
                    "{{\"rpc\":\"{rpc}\",\"shape\":\"{shape}\",\"request\":\"{request}\",\"response\":\"{response}\",\"response_fields\":[{fields}],\"allow_empty_response\":{},\"validations\":[{all_validations}],\"response_rules\":[{response_rules}]{semantic_expectations_field}{typed_wire_evidence_field}}}",
                    response_fields.is_empty()
                ));
            }
        }
    }
    let known_rpcs: BTreeSet<String> = methods
        .iter()
        .filter_map(|method| {
            method
                .split("\"rpc\":\"")
                .nth(1)
                .and_then(|value| value.split('\"').next())
                .map(str::to_owned)
        })
        .collect();
    if let Some(unknown) = typed_wire_evidence
        .keys()
        .find(|rpc| !known_rpcs.contains(*rpc))
    {
        return Err(
            format!("typed wire evidence RPC is absent from Rust descriptors: {unknown}").into(),
        );
    }
    Ok((
        format!(
            "[{}]",
            shapes
                .into_iter()
                .map(|shape| format!("\"{shape}\""))
                .collect::<Vec<_>>()
                .join(",")
        ),
        format!("[{}]", methods.join(",")),
        known_rpcs,
    ))
}

fn is_sha256(value: &str) -> bool {
    let value = value.strip_prefix("sha256:").unwrap_or(value);
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn family_for_package(package: &str) -> Option<&'static str> {
    match package {
        "acyclic.actors.v1" => Some("actors"),
        "acyclic.workers.v1" => Some("workers"),
        "acyclic.objects.v1" | "acyclic.objects.v2" => Some("objects"),
        "acyclic.stream.v2" => Some("stream"),
        "acyclic.filesystem.v2" => Some("filesystem"),
        "acyclic.harness.v2" => Some("harness"),
        "inference.customer.v1" => Some("inference"),
        "acyclic.machines.v1" => Some("machines"),
        _ => None,
    }
}

fn message_fields(descriptor: &FileDescriptorSet, type_name: &str) -> Vec<String> {
    for file in &descriptor.file {
        let package = file.package.as_deref().unwrap_or_default();
        if let Some(message) = find_message(&file.message_type, package, type_name) {
            return message
                .field
                .iter()
                .filter_map(|field| field.json_name.clone().or_else(|| field.name.clone()))
                .collect();
        }
    }
    Vec::new()
}

fn find_message<'a>(
    messages: &'a [prost_types::DescriptorProto],
    prefix: &str,
    type_name: &str,
) -> Option<&'a prost_types::DescriptorProto> {
    for message in messages {
        let name = message.name.as_deref().unwrap_or_default();
        let qualified = if prefix.is_empty() {
            name.to_owned()
        } else {
            format!("{prefix}.{name}")
        };
        if qualified == type_name {
            return Some(message);
        }
        if let Some(found) = find_message(&message.nested_type, &qualified, type_name) {
            return Some(found);
        }
    }
    None
}

fn model_source_revision() -> String {
    let mut canonical = Vec::new();
    for (path, bytes) in MODEL_SOURCES {
        canonical.extend_from_slice(path.as_bytes());
        canonical.push(0);
        canonical.extend_from_slice(bytes);
        canonical.push(0);
    }
    sha256_hex(&canonical)
}

fn model_source_files_json() -> String {
    let files = MODEL_SOURCES
        .iter()
        .map(|(path, _)| format!("\"{path}\""))
        .collect::<Vec<_>>();
    format!("[{}]", files.join(", "))
}

fn model_source_hashes_json() -> String {
    let hashes = MODEL_SOURCES
        .iter()
        .map(|(path, bytes)| format!("\"{path}\": \"{}\"", sha256_hex(bytes)))
        .collect::<Vec<_>>();
    format!("{{{}}}", hashes.join(", "))
}

/// Emit the cross-language wire and protobuf-JSON fixtures from the Rust
/// authority.  Keeping this artifact beside the authority manifest prevents
/// language packages from reaching into another package's test tree for
/// compatibility inputs.
fn rust_family_goldens_json(authority_manifest_sha256: &str) -> String {
    format!(
        r#"[
  {{"family":"actors","kind":"uint64","authority_manifest_sha256":"{authority_manifest_sha256}","message":"acyclic.actors.v1.ActorLimits","field":"handlerTimeoutMillis","value":"18446744073709551615","wire_hex":"08ffffffffffffffffff01","json":"{{\"handlerTimeoutMillis\":\"18446744073709551615\"}}"}},
  {{"family":"stream","kind":"uint64","authority_manifest_sha256":"{authority_manifest_sha256}","message":"acyclic.stream.v2.Record","field":"sequence","value":"18446744073709551615","wire_hex":"08ffffffffffffffffff01","json":"{{\"sequence\":\"18446744073709551615\"}}"}},
  {{"family":"objects","kind":"uint64","authority_manifest_sha256":"{authority_manifest_sha256}","message":"acyclic.objects.v2.ObjectInfo","field":"size","value":"18446744073709551615","wire_hex":"10ffffffffffffffffff01","json":"{{\"size\":\"18446744073709551615\"}}"}},
  {{"family":"workers","kind":"uint64","authority_manifest_sha256":"{authority_manifest_sha256}","message":"acyclic.workers.v1.CodeVersion","field":"sizeBytes","value":"18446744073709551615","wire_hex":"10ffffffffffffffffff01","json":"{{\"sizeBytes\":\"18446744073709551615\"}}"}},
  {{"family":"filesystem","kind":"uint64","authority_manifest_sha256":"{authority_manifest_sha256}","message":"acyclic.filesystem.v2.WorkspaceContextSnapshot","field":"revision","value":"18446744073709551615","wire_hex":"10ffffffffffffffffff01","json":"{{\"revision\":\"18446744073709551615\"}}"}},
  {{"family":"harness","kind":"uint64","authority_manifest_sha256":"{authority_manifest_sha256}","message":"acyclic.harness.v2.OperationStatus","field":"revision","value":"18446744073709551615","wire_hex":"38ffffffffffffffffff01","json":"{{\"revision\":\"18446744073709551615\"}}"}},
  {{"family":"inference","kind":"uint64","authority_manifest_sha256":"{authority_manifest_sha256}","message":"inference.customer.v1.ModelCapability","field":"maximumContext","value":"18446744073709551615","wire_hex":"18ffffffffffffffffff01","json":"{{\"maximumContext\":\"18446744073709551615\"}}"}},
  {{"family":"machines","kind":"uint64","authority_manifest_sha256":"{authority_manifest_sha256}","message":"acyclic.machines.v1.SuspensionPolicy","field":"afterIdleMs","value":"18446744073709551615","wire_hex":"10ffffffffffffffffff01","json":"{{\"afterIdleMs\":\"18446744073709551615\"}}"}},
  {{"family":"protocol","kind":"string","authority_manifest_sha256":"{authority_manifest_sha256}","message":"acyclic.protocol.v1.ProtocolIdentity","field":"version","value":"rust-golden","wire_hex":"0a0b727573742d676f6c64656e","json":"{{\"version\":\"rust-golden\"}}"}}
]
"#
    )
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn generate_contract(
    out: &Path,
    proto_relative: &str,
    descriptor_relative: &str,
    source: String,
    descriptor: Vec<u8>,
) -> Result<(), Box<dyn Error>> {
    let proto_path = out.join(proto_relative);
    let descriptor_path = out.join(descriptor_relative);
    if let Some(parent) = proto_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&proto_path, source)?;
    fs::write(&descriptor_path, descriptor)?;
    println!("generated {}", proto_path.display());
    println!("generated {}", descriptor_path.display());
    Ok(())
}

fn generate_source(out: &Path, relative: &str, source: String) -> Result<(), Box<dyn Error>> {
    let path = out.join(relative);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&path, source)?;
    println!("generated {}", path.display());
    Ok(())
}

fn check_source(out: &Path, relative: &str, expected: String) -> Result<(), Box<dyn Error>> {
    let path = out.join(relative);
    let actual = fs::read_to_string(&path)
        .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
    if actual != expected {
        return Err(format!("generated protobuf source drifted: {}", path.display()).into());
    }
    println!("checked {}", path.display());
    Ok(())
}

fn check_contract(
    out: &Path,
    proto_relative: &str,
    descriptor_relative: &str,
    expected_proto: String,
    expected_descriptor: Vec<u8>,
) -> Result<(), Box<dyn Error>> {
    let proto_path = out.join(proto_relative);
    let descriptor_path = out.join(descriptor_relative);
    let actual_proto = fs::read_to_string(&proto_path)
        .map_err(|error| format!("cannot read {}: {error}", proto_path.display()))?;
    let actual_descriptor = fs::read(&descriptor_path)
        .map_err(|error| format!("cannot read {}: {error}", descriptor_path.display()))?;
    if actual_proto != expected_proto {
        return Err(format!(
            "generated protobuf source drifted: {}",
            proto_path.display()
        )
        .into());
    }
    if actual_descriptor != expected_descriptor {
        return Err(format!(
            "generated descriptor drifted: {}",
            descriptor_path.display()
        )
        .into());
    }
    println!(
        "checked {} and {}",
        proto_path.display(),
        descriptor_path.display()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_policy_preserves_embedded_fields_without_claiming_remote_bindings() {
        let document: Value = serde_json::from_slice(&type_policy_json()).expect("type policy");
        let fields = document["public_field_bindings"]
            .as_array()
            .expect("bindings");
        assert_eq!(fields.len(), PUBLIC_FIELD_BINDINGS.len());
        for (binding, exported) in PUBLIC_FIELD_BINDINGS.iter().zip(fields) {
            assert_eq!(exported["field"], binding.field);
            if binding.direction == PublicFieldDirection::EmbeddedOnly {
                assert_eq!(exported["direction"], "embedded_only");
            }
        }
        assert_eq!(
            public_field_direction_name(PublicFieldDirection::EmbeddedOnly),
            "embedded_only"
        );
    }

    fn evidence_item() -> Value {
        serde_json::json!({
            "family": "actors",
            "operation": "CreateActor",
            "rpc": "acyclic.actors.v1.ActorsService/CreateActor",
            "source": "rust/crates/sdk-examples/src/fixtures/actors_workers.rs",
            "request": {
                "type": "acyclic.actors.v1.CreateActorRequest",
                "bytes_base64": "Cg==",
                "sha256": "sha256:01ba4719c80b6fe911b091a7c05124b64eeece964e09c058ef8f9805daca546b"
            },
            "response": {
                "type": "acyclic.actors.v1.CreateActorResponse",
                "bytes_base64": "Cg==",
                "sha256": "sha256:01ba4719c80b6fe911b091a7c05124b64eeece964e09c058ef8f9805daca546b"
            },
            "state": { "actor_id": "fixture-actor", "revision": "1" }
        })
    }

    fn write_evidence(name: &str, entries: Vec<Value>) -> PathBuf {
        let path = env::temp_dir().join(format!(
            "sdk-contract-wire-{name}-{}.json",
            std::process::id()
        ));
        let document = serde_json::json!({
            "schema": "acyclic.sdk.transport-fixtures.v1",
            "generator": "acyclic-sdk-examples@0.2.0",
            "source": {
                "revision": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "path": "rust/crates/sdk-examples",
                "sha256": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "model_source_digest": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "build_recipe_sha256": "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                "files": ["rust/crates/sdk-examples/src/fixtures/actors_workers.rs"]
            },
            "qualification": {
                "typed_wire_evidence": entries,
                "fixture_server": {
                    "command": "cargo run --manifest-path rust/crates/sdk-examples/Cargo.toml --bin fixture-server -- --port 0",
                    "source_sha256": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                }
            }
        });
        fs::write(&path, serde_json::to_vec(&document).expect("test JSON"))
            .expect("write test evidence");
        path
    }

    #[test]
    fn exported_policy_retains_descriptor_enum_union_and_presence_shapes() {
        let document: Value = serde_json::from_slice(&type_policy_json()).expect("policy JSON");
        assert_eq!(
            document["rpc_methods"].as_array().unwrap().len(),
            acyclic_sdk_contract_wire::resolved_rpc_methods()
                .unwrap()
                .len()
        );
        let enums = document["enum_fields"].as_array().expect("enum inventory");
        assert!(!enums.is_empty());
        assert!(
            enums
                .iter()
                .all(|item| item["preserves_unknown_numeric"] == true
                    && !item["values"].as_array().unwrap().is_empty())
        );
        let members = document["oneof_members"]
            .as_array()
            .expect("oneof inventory");
        assert!(
            members
                .iter()
                .any(|item| item["payload_protobuf_type"] == "TYPE_MESSAGE"
                    && item["payload_type"].is_string())
        );
        assert!(
            members
                .iter()
                .all(|item| item["field"]["number"].is_number()
                    && item["field"]["oneof_name"].is_string()
                    && item["preserves_unknown_members"] == true)
        );
        let presence = document["presence_fields"]
            .as_array()
            .expect("presence inventory");
        for kind in ["message", "oneof", "explicit_optional"] {
            assert!(presence.iter().any(|item| item["kind"] == kind));
        }
    }

    #[test]
    fn evidence_loader_accepts_byte_bound_fixture_output() {
        let path = write_evidence("valid", vec![evidence_item()]);
        let loaded = load_typed_wire_evidence(
            Some(&path),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            None,
        )
        .expect("valid evidence");
        assert_eq!(loaded.entries.len(), 1);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn evidence_loader_rejects_tampered_bytes_hash() {
        let mut item = evidence_item();
        item["request"]["sha256"] = Value::String("sha256:forged".to_owned());
        let path = write_evidence("tampered", vec![item]);
        let error = load_typed_wire_evidence(
            Some(&path),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            None,
        )
        .expect_err("tampered evidence must fail");
        assert!(error.to_string().contains("sha256"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn evidence_loader_rejects_forged_source_hash() {
        let path = write_evidence("source-hash", vec![evidence_item()]);
        let mut document: Value =
            serde_json::from_slice(&fs::read(&path).expect("test JSON")).expect("decode test JSON");
        document["source"]["sha256"] = Value::String(
            "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd".to_owned(),
        );
        fs::write(
            &path,
            serde_json::to_vec(&document).expect("encode test JSON"),
        )
        .expect("rewrite test JSON");
        let error = load_typed_wire_evidence(
            Some(&path),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            None,
        )
        .expect_err("forged source hash must fail closed");
        assert!(error.to_string().contains("fixture_server.source_sha256"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn evidence_loader_rejects_duplicate_rpc_records() {
        let item = evidence_item();
        let path = write_evidence("duplicate", vec![item.clone(), item]);
        let error = load_typed_wire_evidence(
            Some(&path),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            None,
        )
        .expect_err("duplicate RPC evidence must fail");
        assert!(error.to_string().contains("duplicate"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn descriptor_rejects_wrong_request_type() {
        let mut entries = BTreeMap::new();
        entries.insert(
            "acyclic.actors.v1.ActorsService/CreateActor".to_owned(),
            evidence_item(),
        );
        entries
            .get_mut("acyclic.actors.v1.ActorsService/CreateActor")
            .expect("evidence")["request"]["type"] = Value::String("forged.Request".to_owned());
        let error = rpc_shapes_json(&actors_descriptor(), &entries)
            .expect_err("wrong request type must fail closed");
        assert!(error.to_string().contains("request type"));
    }

    #[test]
    fn descriptor_rejects_wrong_response_type() {
        let mut entries = BTreeMap::new();
        entries.insert(
            "acyclic.actors.v1.ActorsService/CreateActor".to_owned(),
            evidence_item(),
        );
        entries
            .get_mut("acyclic.actors.v1.ActorsService/CreateActor")
            .expect("evidence")["response"]["type"] = Value::String("forged.Response".to_owned());
        let error = rpc_shapes_json(&actors_descriptor(), &entries)
            .expect_err("wrong response type must fail closed");
        assert!(error.to_string().contains("response type"));
    }

    #[test]
    fn descriptor_rejects_unknown_rpc_evidence() {
        let mut entries = BTreeMap::new();
        entries.insert(
            "acyclic.actors.v1.ActorsService/NotInRust".to_owned(),
            evidence_item(),
        );
        let error = rpc_shapes_json(&actors_descriptor(), &entries)
            .expect_err("unknown RPC evidence must fail closed");
        assert!(error.to_string().contains("absent from Rust descriptors"));
    }
}
