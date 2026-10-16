//! Generates transport from the packaged protocol and builds the Darwin bridge.

#[path = "build/case_fold.rs"]
mod case_fold;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rustc-check-cfg=cfg(kani)");
    println!("cargo:rustc-check-cfg=cfg(acyclic_loom)");
    println!("cargo:rustc-check-cfg=cfg(fuzzing)");
    case_fold::generate()?;
    let native_transport = std::env::var("CARGO_CFG_TARGET_ARCH")? != "wasm32";
    acyclic_proto_build::compile(
        acyclic_proto_build::config()?,
        acyclic_proto_build::services(native_transport),
        &["proto/filesystem/v2/filesystem.proto"],
        "acyclic-filesystem-v2.bin",
    )?;
    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_NATIVE_MOUNT");
    if std::env::var_os("CARGO_FEATURE_NAPI_TYPES").is_some() {
        // napi-derive emits the shared enums into this external directory.
        // A native build without declaration output must not leave Cargo's
        // cached dependency usable when a declaration build requests it.
        println!("cargo:rerun-if-env-changed=NAPI_TYPE_DEF_TMP_FOLDER");
        println!("cargo:rerun-if-env-changed=NAPI_FORCE_BUILD_ACYCLIC_FS");
    }

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
        .flag("-std=c11")
        .flag_if_supported("-Wno-unused-parameter")
        .warnings(true)
        .compile("acyclic_fs_darwin_mount");
}
