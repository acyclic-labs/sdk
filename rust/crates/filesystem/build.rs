//! Generates transport from the committed descriptor and builds the Darwin bridge.

use prost::Message;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let descriptors = prost_types::FileDescriptorSet::decode(
        include_bytes!("src/generated/acyclic-filesystem-v2.bin").as_slice(),
    )?;
    let mut prost = tonic_prost_build::Config::new();
    prost.extern_path(".acyclic.harness.v1", "crate::wire::harness::v1");
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_fds_with_config(descriptors, prost)?;
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src/generated/acyclic-filesystem-v2.bin");
    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_NATIVE_MOUNT");

    #[cfg(target_os = "macos")]
    if std::env::var_os("CARGO_FEATURE_NATIVE_MOUNT").is_some() {
        build_darwin_mount();
    }
    Ok(())
}

#[cfg(target_os = "macos")]
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
    println!("cargo:rerun-if-changed=vendor/darwinfuse/LICENSE");
    for source in SOURCES {
        println!("cargo:rerun-if-changed={source}");
    }
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
