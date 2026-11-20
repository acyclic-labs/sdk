//! Builds the Disk Arbitration retirement for macOS.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=src/retirement.c");
    println!("cargo:rerun-if-changed=src/retirement_model.c");
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("macos") {
        return Ok(());
    }
    println!("cargo:rustc-link-lib=framework=CoreFoundation");
    println!("cargo:rustc-link-lib=framework=DiskArbitration");
    let mut build = cc::Build::new();
    build.file("src/retirement.c");
    if std::env::var_os("CARGO_FEATURE_TEST_SUPPORT").is_some() {
        build.file("src/retirement_model.c");
    }
    build
        .flag("-std=c11")
        .flag_if_supported("-Wno-unused-parameter")
        .warnings(true)
        .try_compile("acyclic_darwin_mount_retire")?;
    Ok(())
}
