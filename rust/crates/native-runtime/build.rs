//! Builds the Linux process runner (`runner/main.rs`): a static image
//! without the C library, which the library embeds and execs once per owner.
//! Other targets embed nothing. A Linux architecture without the runner's
//! system call table fails the build rather than run without custody.

use std::env;
use std::error::Error;
use std::path::PathBuf;
use std::process::Command;

fn main() -> Result<(), Box<dyn Error>> {
    for path in [
        "runner/main.rs",
        "runner/sys.rs",
        "src/process_tree/record.rs",
        "src/process_tree/warden.rs",
    ] {
        println!("cargo:rerun-if-changed={path}");
    }
    if env::var("CARGO_CFG_TARGET_OS")? != "linux" {
        return Ok(());
    }
    let arch = env::var("CARGO_CFG_TARGET_ARCH")?;
    if !matches!(arch.as_str(), "x86_64" | "aarch64") {
        return Err(format!("the process runner has no system call table for {arch}").into());
    }
    let output = PathBuf::from(env::var_os("OUT_DIR").ok_or("OUT_DIR is not set")?).join("runner");
    let rustc = env::var_os("RUSTC").ok_or("RUSTC is not set")?;
    let sysroot = Command::new(&rustc).args(["--print", "sysroot"]).output()?;
    let sysroot = String::from_utf8(sysroot.stdout)?;
    // The toolchain's own linker, named for its ELF flavour: it links any
    // target with no C toolchain.
    let linker = PathBuf::from(sysroot.trim())
        .join("lib/rustlib")
        .join(env::var("HOST")?)
        .join("bin/rust-lld");
    let status = Command::new(&rustc)
        .args([
            "--edition",
            "2024",
            "--crate-type",
            "bin",
            "--crate-name",
            "runner",
        ])
        .args(["--target", &env::var("TARGET")?])
        .args([
            "-C",
            "panic=abort",
            "-C",
            "opt-level=s",
            "-C",
            "debuginfo=0",
        ])
        .args([
            "-C",
            "relocation-model=static",
            "-C",
            "target-feature=+crt-static",
        ])
        .args(["-C", "linker-flavor=ld.lld", "-C", "strip=symbols"])
        .arg("-C")
        .arg(format!("linker={}", linker.display()))
        .args(["-C", "link-arg=--gc-sections"])
        .arg("-o")
        .arg(&output)
        .arg("runner/main.rs")
        .status()?;
    if !status.success() {
        return Err(format!("the runner failed to build: {status}").into());
    }
    Ok(())
}
