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
    ];
    // The mount owner retires its mount through the one shared Disk Arbitration retirement.
    const RETIREMENT: &str = "../darwin-mount-retire/src/retirement.c";
    // The whole vendored tree, so a changed header also rebuilds.
    println!("cargo:rerun-if-changed=vendor/darwinfuse");
    println!("cargo:rerun-if-changed=src/native_mount/darwin_mount_bridge.c");
    println!("cargo:rerun-if-changed={RETIREMENT}");
    println!("cargo:rerun-if-changed=src/native_mount/darwin_mount_owner.c");
    println!("cargo:rerun-if-changed=src/native_mount/darwin_mount_owner.h");
    println!("cargo:rerun-if-changed=src/native_mount/darwin_mount_owner_spawn.c");
    let mut build = cc::Build::new();
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
            RETIREMENT,
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
