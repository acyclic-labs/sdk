use std::collections::BTreeSet;
use std::env;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};

use acyclic_sdk_contract_options::options_proto;
use acyclic_sdk_contract_wire::{
    BindingFamily, actors_descriptor, actors_proto, descriptor_set_with_docs,
    filesystem::{filesystem_descriptor, filesystem_proto},
    generate_product_bindings, generate_remote_facades,
    harness::{harness_descriptor, harness_proto},
    inference::{inference_descriptor, inference_proto},
    machines::{machines_descriptor, machines_proto},
    objects::{objects_descriptor, objects_proto},
    protocol::{protocol_descriptor, protocol_proto},
    stream::{stream_descriptor, stream_proto},
    workers::{workers_descriptor, workers_proto},
};
use prost::Message;
use prost_types::FileDescriptorSet;
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
const VALIDATION_OPTIONS_PROTO_PATH: &str = "validation/v1/options.proto";
const AUTHORITY_MANIFEST: &str = "rust-authority.json";
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
        } else {
            return Err(format!("unknown argument: {argument}").into());
        }
    }

    match command.as_str() {
        "generate" => generate(&out.ok_or("generate requires --out")?),
        "check" => check(&out.ok_or("check requires --out")?),
        "generate-products" => generate_products(&root.ok_or("generate-products requires --root")?),
        "check-products" => check_products(&root.ok_or("check-products requires --root")?),
        _ => Err(format!(
            "unknown command: {command}; expected generate, check, generate-products, or check-products"
        )
        .into()),
    }
}

fn product_artifacts(root: &Path) -> Result<Vec<(String, Vec<u8>)>, Box<dyn Error>> {
    let mut artifacts = vec![
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
    }
    for facade in generate_remote_facades() {
        artifacts.push((facade.path.to_owned(), facade.source.into_bytes()));
    }
    let _ = fs::remove_dir_all(staging);
    Ok(artifacts)
}

fn generate_products(root: &Path) -> Result<(), Box<dyn Error>> {
    for (relative, expected) in product_artifacts(root)? {
        let path = root.join(relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&path, expected)?;
        println!("generated {}", path.display());
    }
    Ok(())
}

fn check_products(root: &Path) -> Result<(), Box<dyn Error>> {
    let mut drifted = Vec::new();
    for (relative, expected) in product_artifacts(root)? {
        let path = root.join(relative);
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

fn generate(out: &Path) -> Result<(), Box<dyn Error>> {
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
    let manifest = authority_manifest(out)?;
    fs::write(out.join(AUTHORITY_MANIFEST), manifest)?;
    println!("generated {}", out.join(AUTHORITY_MANIFEST).display());
    Ok(())
}

fn check(out: &Path) -> Result<(), Box<dyn Error>> {
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
    let manifest = fs::read_to_string(out.join(AUTHORITY_MANIFEST)).map_err(|error| {
        format!(
            "cannot read {}: {error}",
            out.join(AUTHORITY_MANIFEST).display()
        )
    })?;
    let expected = authority_manifest(out)?;
    if manifest != expected {
        return Err(format!(
            "authority manifest is stale or does not bind Rust artifacts: {}",
            out.join(AUTHORITY_MANIFEST).display()
        )
        .into());
    }
    println!("checked {}", out.join(AUTHORITY_MANIFEST).display());
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
        INFERENCE_PROTO_PATH,
        INFERENCE_DESCRIPTOR_PATH,
        MACHINES_PROTO_PATH,
        MACHINES_DESCRIPTOR_PATH,
        AUTHORITY_MANIFEST,
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
fn authority_manifest(out: &Path) -> Result<String, Box<dyn Error>> {
    let families = [
        (PROTO_PATH, DESCRIPTOR_PATH),
        (STREAM_PROTO_PATH, STREAM_DESCRIPTOR_PATH),
        (OBJECTS_PROTO_PATH, OBJECTS_DESCRIPTOR_PATH),
        (WORKERS_PROTO_PATH, WORKERS_DESCRIPTOR_PATH),
        (FILESYSTEM_PROTO_PATH, FILESYSTEM_DESCRIPTOR_PATH),
        (HARNESS_PROTO_PATH, HARNESS_DESCRIPTOR_PATH),
        (PROTOCOL_PROTO_PATH, PROTOCOL_DESCRIPTOR_PATH),
        (INFERENCE_PROTO_PATH, INFERENCE_DESCRIPTOR_PATH),
        (MACHINES_PROTO_PATH, MACHINES_DESCRIPTOR_PATH),
    ];
    let source_revision = model_source_revision();
    let mut entries = String::new();
    for (index, (source, descriptor)) in families.iter().enumerate() {
        let source_bytes = fs::read(out.join(source))?;
        let descriptor_bytes = fs::read(out.join(descriptor))?;
        if index != 0 {
            entries.push_str(",\n");
        }
        let (rpc_shapes, rpc_methods) = rpc_shapes_json(&descriptor_bytes)?;
        entries.push_str(&format!(
            "    {{\n      \"source\": \"{source}\",\n      \"source_sha256\": \"{}\",\n      \"descriptor\": \"{descriptor}\",\n      \"descriptor_sha256\": \"{}\",\n      \"schema_descriptor\": \"{descriptor}\",\n      \"schema_descriptor_sha256\": \"{}\",\n      \"descriptor_role\": \"canonical_schema\",\n      \"handshake_descriptor\": null,\n      \"handshake_descriptor_role\": \"preserved_runtime_fixture\",\n      \"rpc_shapes\": {},\n      \"rpc_methods\": {}\n    }}",
            sha256_hex(&source_bytes),
            sha256_hex(&descriptor_bytes),
            sha256_hex(&descriptor_bytes),
            rpc_shapes,
            rpc_methods,
        ));
    }
    Ok(format!(
        "{{\n  \"schema\": \"acyclic.sdk.rust-authority.v1\",\n  \"authority\": \"rust\",\n  \"schema_root\": \"rust/crates/sdk-contract-wire\",\n  \"source_revision\": \"{source_revision}\",\n  \"source_revision_kind\": \"rust-model-sha256\",\n  \"source_files\": {source_files},\n  \"source_file_hashes\": {source_file_hashes},\n  \"exporter\": \"acyclic-sdk-contract-wire@{version}\",\n  \"families\": [\n{entries}\n  ]\n}}\n",
        source_files = model_source_files_json(),
        source_file_hashes = model_source_hashes_json(),
        version = env!("CARGO_PKG_VERSION")
    ))
}

fn rpc_shapes_json(descriptor_bytes: &[u8]) -> Result<(String, String), Box<dyn Error>> {
    let descriptor = FileDescriptorSet::decode(descriptor_bytes)?;
    let mut shapes = BTreeSet::new();
    let mut methods = Vec::new();
    for file in descriptor.file {
        let package = file.package.unwrap_or_default();
        for service in file.service {
            let service_name = service.name.unwrap_or_default();
            for method in service.method {
                let method_name = method.name.unwrap_or_default();
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
                methods.push(format!(
                    "{{\"rpc\":\"{package}.{service_name}/{method_name}\",\"shape\":\"{shape}\"}}"
                ));
            }
        }
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
    ))
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
