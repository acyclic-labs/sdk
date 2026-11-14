//! Package the pinned compiler for the execution target, not the build host.
//!
//! The bundle is copied into `OUT_DIR`, so the library embeds bytes and never a
//! path into another package's directory. Cargo finds the pinned files beside
//! each vendored package; Bazel stages the same files and names them in
//! `ACYCLIC_PROTOC_EXECUTABLE` and `ACYCLIC_PROTOC_INCLUDE`.
use std::{
    env, fs, io,
    path::{Path, PathBuf},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=ACYCLIC_PROTOC_EXECUTABLE");
    println!("cargo:rerun-if-env-changed=ACYCLIC_PROTOC_INCLUDE");
    let out = PathBuf::from(env::var_os("OUT_DIR").ok_or("OUT_DIR is not set")?);
    let platform = (
        env::var("CARGO_CFG_TARGET_OS")?,
        env::var("CARGO_CFG_TARGET_ARCH")?,
    );
    let executable = env::var_os("ACYCLIC_PROTOC_EXECUTABLE")
        .map(PathBuf::from)
        .or_else(|| match (platform.0.as_str(), platform.1.as_str()) {
            ("linux", "x86") => Some(protoc_bin_vendored_linux_x86_32::protoc_bin_path()),
            ("linux", "x86_64") => Some(protoc_bin_vendored_linux_x86_64::protoc_bin_path()),
            ("linux", "aarch64") => Some(protoc_bin_vendored_linux_aarch_64::protoc_bin_path()),
            ("linux", "powerpc64") => Some(protoc_bin_vendored_linux_ppcle_64::protoc_bin_path()),
            ("linux", "s390x") => Some(protoc_bin_vendored_linux_s390_64::protoc_bin_path()),
            ("macos", "x86_64") => Some(protoc_bin_vendored_macos_x86_64::protoc_bin_path()),
            ("macos", "aarch64") => Some(protoc_bin_vendored_macos_aarch_64::protoc_bin_path()),
            ("windows", _) => Some(protoc_bin_vendored_win32::protoc_bin_path()),
            _ => None,
        });
    let mut source = match executable {
        Some(path) => {
            fs::copy(path, out.join("protoc"))?;
            "const EXECUTABLE: &[u8] = include_bytes!(concat!(env!(\"OUT_DIR\"), \"/protoc\"));\n"
                .to_owned()
        }
        None => "const EXECUTABLE: &[u8] = &[];\n".into(),
    };
    // These includes are byte-identical across all vendored platform packages.
    source.push_str("const INCLUDES: &[(&str, &[u8])] = &[\n");
    let root = env::var_os("ACYCLIC_PROTOC_INCLUDE")
        .map_or_else(protoc_bin_vendored_win32::include_path, PathBuf::from);
    includes(&root, &root, &out.join("include"), &mut source)?;
    source.push_str("] ;\n");
    fs::write(out.join("compiler.rs"), source)?;
    Ok(())
}

fn includes(root: &Path, directory: &Path, copy: &Path, source: &mut String) -> io::Result<()> {
    let mut entries = fs::read_dir(directory)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        if fs::metadata(&path)?.is_dir() {
            includes(root, &path, copy, source)?;
        } else {
            let relative = path.strip_prefix(root).map_err(io::Error::other)?;
            let relative = relative.to_string_lossy().replace('\\', "/");
            let target = copy.join(&relative);
            fs::create_dir_all(
                target
                    .parent()
                    .ok_or_else(|| io::Error::other("include parent"))?,
            )?;
            fs::copy(&path, &target)?;
            source.push_str(&format!(
                "({relative:?}, include_bytes!(concat!(env!(\"OUT_DIR\"), \"/include/\", {relative:?}))),\n"
            ));
        }
    }
    Ok(())
}
