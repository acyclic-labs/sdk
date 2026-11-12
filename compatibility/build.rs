//! Package the pinned compiler for the execution target, not the build host.
use std::{env, fs, io, path::Path};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = std::path::PathBuf::from(env::var("OUT_DIR")?);
    let platform = (
        env::var("CARGO_CFG_TARGET_OS")?,
        env::var("CARGO_CFG_TARGET_ARCH")?,
    );
    let executable = match (platform.0.as_str(), platform.1.as_str()) {
        ("linux", "x86") => Some(protoc_bin_vendored_linux_x86_32::protoc_bin_path()),
        ("linux", "x86_64") => Some(protoc_bin_vendored_linux_x86_64::protoc_bin_path()),
        ("linux", "aarch64") => Some(protoc_bin_vendored_linux_aarch_64::protoc_bin_path()),
        ("linux", "powerpc64") => Some(protoc_bin_vendored_linux_ppcle_64::protoc_bin_path()),
        ("linux", "s390x") => Some(protoc_bin_vendored_linux_s390_64::protoc_bin_path()),
        ("macos", "x86_64") => Some(protoc_bin_vendored_macos_x86_64::protoc_bin_path()),
        ("macos", "aarch64") => Some(protoc_bin_vendored_macos_aarch_64::protoc_bin_path()),
        ("windows", _) => Some(protoc_bin_vendored_win32::protoc_bin_path()),
        _ => None,
    };
    let mut source: String = match executable {
        Some(path) => {
            fs::copy(path, output.join("protoc.bin"))?;
            "const EXECUTABLE: &[u8] = include_bytes!(concat!(env!(\"OUT_DIR\"), \"/protoc.bin\"));\n".into()
        }
        None => "const EXECUTABLE: &[u8] = &[];\n".into(),
    };
    // These includes are byte-identical across all vendored platform packages.
    source.push_str("const INCLUDES: &[(&str, &[u8])] = &[\n");
    let root = protoc_bin_vendored_win32::include_path();
    includes(&root, &root, &output, &mut source)?;
    source.push_str("] ;\n");
    fs::write(output.join("compiler.rs"), source)?;
    println!("cargo:rerun-if-changed=build.rs");
    Ok(())
}

fn includes(root: &Path, directory: &Path, output: &Path, source: &mut String) -> io::Result<()> {
    let mut entries = fs::read_dir(directory)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            includes(root, &path, output, source)?;
        } else {
            let relative = path.strip_prefix(root).map_err(io::Error::other)?;
            let relative = relative.to_string_lossy().replace('\\', "/");
            let destination = output.join("includes").join(&relative);
            if let Some(parent) = destination.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(path, destination)?;
            let generated = format!("/includes/{relative}");
            source.push_str(&format!(
                "({relative:?}, include_bytes!(concat!(env!(\"OUT_DIR\"), {generated:?}))),\n"
            ));
        }
    }
    Ok(())
}
