use std::{
    fs, io,
    path::{Path, PathBuf},
};

include!(concat!(env!("OUT_DIR"), "/compiler.rs"));

/// Owns the pinned compiler, standard imports and shared protocols for this
/// execution platform.
///
/// The bundle travels with the executable, including after cross-compilation
/// and relocation. `PROTOC` and `PROTOC_INCLUDE` independently override the
/// compiler paths. Keep this owner alive until compilation finishes; its private
/// temporary files are removed when it is dropped.
pub struct Compiler {
    executable: PathBuf,
    includes: PathBuf,
    shared: PathBuf,
    _bundle: tempfile::TempDir,
}

impl Compiler {
    /// Resolve overrides and materialize the bundled compiler inputs.
    ///
    /// # Errors
    /// Returns unsupported-platform or filesystem errors.
    pub fn new() -> io::Result<Self> {
        let bundle = tempfile::tempdir()?;
        let executable = if let Some(path) = std::env::var_os("PROTOC") {
            PathBuf::from(path)
        } else {
            if EXECUTABLE.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "no bundled protoc for this platform; set PROTOC",
                ));
            }
            let path = bundle
                .path()
                .join(format!("protoc{}", std::env::consts::EXE_SUFFIX));
            fs::write(&path, EXECUTABLE)?;
            fs::set_permissions(&path, fs::metadata(std::env::current_exe()?)?.permissions())?;
            path
        };
        let includes = if let Some(path) = std::env::var_os("PROTOC_INCLUDE") {
            PathBuf::from(path)
        } else {
            materialize(&bundle.path().join("include"), INCLUDES)?
        };
        let shared = materialize(&bundle.path().join("shared"), SHARED)?;
        Ok(Self {
            executable,
            includes,
            shared,
            _bundle: bundle,
        })
    }

    /// Executable path, valid while this compiler is alive.
    #[must_use]
    pub fn executable(&self) -> &Path {
        &self.executable
    }

    /// Standard protobuf import root, valid while this compiler is alive.
    #[must_use]
    pub fn includes(&self) -> &Path {
        &self.includes
    }

    /// Import root for canonical identities shared by independently published
    /// services, valid while this compiler is alive.
    #[must_use]
    pub fn shared(&self) -> &Path {
        &self.shared
    }
}

/// Writes embedded files under `root` and returns it.
fn materialize(root: &Path, files: &[(&str, &[u8])]) -> io::Result<PathBuf> {
    for (relative, bytes) in files {
        let path = root.join(relative);
        fs::create_dir_all(
            path.parent()
                .ok_or_else(|| io::Error::other("missing embedded file parent"))?,
        )?;
        fs::write(path, bytes)?;
    }
    Ok(root.to_owned())
}
