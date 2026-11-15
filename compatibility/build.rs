//! Package the pinned compiler for the execution target, not the build host.
//!
//! `PROTOC` and `PROTOC_INCLUDE` name the compiler and its standard imports, as
//! they do for the library at run time. Bazel always sets both, from the files
//! of the vendored package for the configured platform. Under Cargo they are
//! usually unset, and the vendored package for the target names its own files.
//! A bare `PROTOC` name resolves through `PATH` to the first runnable file, as a
//! command would run it, and a changed `PATH` reruns the script.
//! Every copied file is watched, so a replaced compiler or edited include at
//! the same path rebuilds the bundle.
//! The crate's own shared protocols travel the same way. The bundle is copied
//! into `OUT_DIR`, so the library embeds bytes, never a path that is valid only
//! while this script runs or only inside this package.
//! Each copy carries bytes, never the source's mode, and is renamed over the
//! previous run's file, so the script reruns over its own output even when a
//! source, and so an earlier copy, is read-only.
use std::{
    env, fs, io,
    path::{Path, PathBuf},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=proto");
    println!("cargo:rerun-if-env-changed=PROTOC");
    println!("cargo:rerun-if-env-changed=PROTOC_INCLUDE");
    let out = PathBuf::from(env::var_os("OUT_DIR").ok_or("OUT_DIR is not set")?);
    let platform = (
        env::var("CARGO_CFG_TARGET_OS")?,
        env::var("CARGO_CFG_TARGET_ARCH")?,
    );
    let named = env::var_os("PROTOC")
        .map(PathBuf::from)
        .map(on_path)
        .transpose()?;
    let executable = named.or_else(|| match (platform.0.as_str(), platform.1.as_str()) {
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
            println!("cargo:rerun-if-changed={}", path.display());
            place(&path, &out.join("protoc"))?;
            "const EXECUTABLE: &[u8] = include_bytes!(concat!(env!(\"OUT_DIR\"), \"/protoc\"));\n"
                .to_owned()
        }
        None => "const EXECUTABLE: &[u8] = &[];\n".into(),
    };
    // These includes are byte-identical across all vendored platform packages.
    source.push_str("const INCLUDES: &[(&str, &[u8])] = &[\n");
    let root = env::var_os("PROTOC_INCLUDE")
        .map_or_else(protoc_bin_vendored_win32::include_path, PathBuf::from);
    println!("cargo:rerun-if-changed={}", root.display());
    embed(&root, &root, &out, "include", &mut source)?;
    source.push_str("];\nconst SHARED: &[(&str, &[u8])] = &[\n");
    let shared =
        PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").ok_or("CARGO_MANIFEST_DIR is not set")?)
            .join("proto");
    embed(&shared, &shared, &out, "shared", &mut source)?;
    source.push_str("];\n");
    fs::write(out.join("compiler.rs"), source)?;
    Ok(())
}

/// The executable a bare name runs, found on `PATH`; a path is returned as given.
fn on_path(named: PathBuf) -> io::Result<PathBuf> {
    if named.components().count() > 1 {
        return Ok(named);
    }
    // Another PATH can name another compiler under the same name.
    println!("cargo:rerun-if-env-changed=PATH");
    let mut suffixed = named.clone().into_os_string();
    suffixed.push(env::consts::EXE_SUFFIX);
    env::split_paths(&env::var_os("PATH").unwrap_or_default())
        .flat_map(|directory| [directory.join(&named), directory.join(&suffixed)])
        .find(|candidate| runnable(candidate))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("PROTOC names {}, which is not on PATH", named.display()),
            )
        })
}

/// Whether a command lookup would run `candidate`: a file, and on Unix one with an execute bit.
fn runnable(candidate: &Path) -> bool {
    fs::metadata(candidate).is_ok_and(|metadata| {
        #[cfg(unix)]
        let executable =
            std::os::unix::fs::PermissionsExt::mode(&metadata.permissions()) & 0o111 != 0;
        #[cfg(not(unix))]
        let executable = true;
        metadata.is_file() && executable
    })
}

/// Writes the bytes of `from` to `to`: a new file beside it, created with the
/// default mode rather than `from`'s, renamed into place. `fs::copy` would carry
/// a read-only source's mode to `to` and then fail to open that file on the
/// next run.
fn place(from: &Path, to: &Path) -> io::Result<()> {
    let mut partial = to.as_os_str().to_owned();
    partial.push(".partial");
    let partial = PathBuf::from(partial);
    fs::write(&partial, fs::read(from)?)?;
    fs::rename(&partial, to)
}

/// Copies every file under `directory` into `OUT_DIR/<name>` and lists it, by its
/// path relative to `root`, as embedded bytes.
fn embed(
    root: &Path,
    directory: &Path,
    out: &Path,
    name: &str,
    source: &mut String,
) -> io::Result<()> {
    let mut entries = fs::read_dir(directory)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        if fs::metadata(&path)?.is_dir() {
            embed(root, &path, out, name, source)?;
        } else {
            let relative = path.strip_prefix(root).map_err(io::Error::other)?;
            let relative = relative.to_string_lossy().replace('\\', "/");
            let target = out.join(name).join(&relative);
            fs::create_dir_all(
                target
                    .parent()
                    .ok_or_else(|| io::Error::other("embedded file parent"))?,
            )?;
            place(&path, &target)?;
            source.push_str(&format!(
                "({relative:?}, include_bytes!(concat!(env!(\"OUT_DIR\"), \"/{name}/\", {relative:?}))),\n"
            ));
        }
    }
    Ok(())
}
