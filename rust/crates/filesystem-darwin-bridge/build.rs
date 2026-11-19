//! Builds the vendored Darwin mount bridge for macOS.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        build_darwin_mount()?;
    }
    Ok(())
}

fn build_darwin_mount() -> Result<(), Box<dyn std::error::Error>> {
    const SOURCES: &[&str] = &[
        "vendor/darwinfuse/src/nfs4_xdr.c",
        "vendor/darwinfuse/src/rpc.c",
        "vendor/darwinfuse/src/nfs4_server.c",
        "vendor/darwinfuse/src/nfs4_ops.c",
        "vendor/darwinfuse/src/inode_table.c",
        "vendor/darwinfuse/src/darwinfuse.c",
        "src/native_mount/darwin_mount_bridge.c",
        "src/native_mount/darwin_mount_owner_spawn.c",
        "src/native_mount/darwin_mount_retirement.c",
    ];
    // The whole vendored tree, so a changed header also rebuilds.
    println!("cargo:rerun-if-changed=vendor/darwinfuse");
    println!("cargo:rerun-if-changed=src/native_mount/darwin_mount_bridge.c");
    println!("cargo:rerun-if-changed=src/native_mount/darwin_mount_retirement.c");
    println!("cargo:rerun-if-changed=src/native_mount/darwin_mount_owner.c");
    println!("cargo:rerun-if-changed=src/native_mount/darwin_mount_owner.h");
    println!("cargo:rerun-if-changed=src/native_mount/darwin_mount_owner_spawn.c");
    println!("cargo:rustc-link-lib=framework=CoreFoundation");
    println!("cargo:rustc-link-lib=framework=DiskArbitration");
    let mut build = cc::Build::new();
    if std::env::var_os("CARGO_FEATURE_TEST_SUPPORT").is_some() {
        build.file("src/native_mount/darwin_mount/tests/retirement_model.c");
    }
    println!("cargo:rerun-if-changed=src/native_mount/darwin_mount/tests/retirement_model.c");
    build
        .files(SOURCES)
        .include("vendor/darwinfuse/include")
        .include("vendor/darwinfuse/src")
        .include("src/native_mount")
        .define("_FILE_OFFSET_BITS", "64")
        .flag("-std=c11")
        .flag_if_supported("-Wno-unused-parameter")
        .warnings(true)
        .try_compile("acyclic_fs_darwin_mount")?;

    let output = std::path::PathBuf::from(
        std::env::var_os("OUT_DIR")
            .ok_or_else(|| std::io::Error::other("Cargo did not set OUT_DIR"))?,
    )
    .join("darwin-mount-owner");
    let compiler = build.try_get_compiler()?;
    let status = compiler
        .to_command()
        .args([
            "src/native_mount/darwin_mount_owner.c",
            "src/native_mount/darwin_mount_retirement.c",
            "-framework",
            "CoreFoundation",
            "-framework",
            "DiskArbitration",
            "-o",
        ])
        .arg(output)
        .status()?;
    if !status.success() {
        return Err(
            std::io::Error::other(format!("Darwin mount owner compiler failed: {status}")).into(),
        );
    }
    Ok(())
}
