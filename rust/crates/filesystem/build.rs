//! Generates transport from the Rust model descriptor artifact and builds the
//! Darwin bridge.
//!
//! The model descriptor is the default binding input. The separately retained
//! `acyclic-filesystem-v2.bin` archive remains the deployed handshake identity;
//! its parity with the immutable fixture is checked by the model migration
//! prototype. The model artifact is committed under `src/generated` so a
//! packaged crate does not need the private nested contract workspace.

use prost::Message;

const MODEL_DESCRIPTOR_ENV: &str = "ACYCLIC_FS_MODEL_DESCRIPTOR";
const MODEL_DESCRIPTOR: &[u8] = include_bytes!("src/generated/rust-model-filesystem-v2.bin");

// The model descriptor is deliberately source-info-free.  These Rust sources
// own the generation-only documentation overlay; track them explicitly so a
// changed model comment rebuilds the product bindings once the shared overlay
// helper is consumed here.
const RUST_MODEL_SOURCES: &[&str] = &[
    "../sdk-contract-wire/src/bindings.rs",
    "../sdk-contract-wire/src/filesystem.rs",
    "../sdk-contract-wire/src/harness.rs",
    "../sdk-contract-wire/src/lib.rs",
    "../sdk-contract-wire/src/protocol.rs",
    "../sdk-contract-options/src/lib.rs",
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let model_bytes = match std::env::var_os(MODEL_DESCRIPTOR_ENV) {
        Some(path) => {
            let path = std::path::PathBuf::from(path);
            println!("cargo:rerun-if-changed={}", path.display());
            std::fs::read(path)?
        }
        None => MODEL_DESCRIPTOR.to_vec(),
    };
    let model_with_docs = acyclic_sdk_contract_wire::descriptor_set_with_docs(
        acyclic_sdk_contract_wire::BindingFamily::Filesystem,
        &model_bytes,
    )?;
    let descriptors = prost_types::FileDescriptorSet::decode(model_with_docs.as_slice())?;
    let prost = tonic_prost_build::Config::new();
    // Browser clients use `tonic-web-wasm-client` as their transport.  Keep
    // generating the canonical client for wasm32 so the browser facade uses
    // the same Rust-owned service contract as native hosted clients.
    let build_client = true;
    let native_transport = std::env::var("CARGO_CFG_TARGET_ARCH")?.as_str() != "wasm32";
    tonic_prost_build::configure()
        .build_client(build_client)
        .build_server(native_transport)
        .compile_fds_with_config(descriptors, prost)?;
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src/generated/rust-model-filesystem-v2.bin");
    println!("cargo:rerun-if-changed=src/generated/acyclic-filesystem-v2.bin");
    for source in RUST_MODEL_SOURCES {
        println!("cargo:rerun-if-changed={source}");
    }
    println!("cargo:rerun-if-env-changed={MODEL_DESCRIPTOR_ENV}");
    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_NATIVE_MOUNT");

    // A build script runs on the host, so the target comes from Cargo.
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos")
        && std::env::var_os("CARGO_FEATURE_NATIVE_MOUNT").is_some()
    {
        build_darwin_mount();
    }
    Ok(())
}

fn build_darwin_mount() {
    const SOURCES: &[&str] = &[
        "vendor/darwinfuse/src/nfs4_xdr.c",
        "vendor/darwinfuse/src/rpc.c",
        "vendor/darwinfuse/src/nfs4_server.c",
        "vendor/darwinfuse/src/nfs4_ops.c",
        "vendor/darwinfuse/src/inode_table.c",
        "vendor/darwinfuse/src/fuse_opt.c",
        "vendor/darwinfuse/src/darwinfuse.c",
        "src/native_mount/darwin_mount_bridge.c",
    ];
    // The whole vendored tree, so a changed header also rebuilds.
    println!("cargo:rerun-if-changed=vendor/darwinfuse");
    println!("cargo:rerun-if-changed=src/native_mount/darwin_mount_bridge.c");
    cc::Build::new()
        .files(SOURCES)
        .include("vendor/darwinfuse/include")
        .include("vendor/darwinfuse/src")
        .define("_FILE_OFFSET_BITS", "64")
        .flag_if_supported("-std=c11")
        .flag_if_supported("-Wno-unused-parameter")
        .warnings(true)
        .compile("acyclic_fs_darwin_mount");
}
