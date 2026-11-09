use std::{
    fs, io,
    path::{Path, PathBuf},
};

include!(concat!(env!("OUT_DIR"), "/compiler.rs"));

/// Owns the pinned compiler and standard imports for this execution platform.
///
/// The bundle travels with the executable, including after cross-compilation
/// and relocation. `PROTOC` and `PROTOC_INCLUDE` independently override its
/// paths. Keep this owner alive until compilation finishes; its private
/// temporary files are removed when it is dropped.
pub struct Compiler {
    executable: PathBuf,
    includes: PathBuf,
    _bundle: Option<tempfile::TempDir>,
}

impl Compiler {
    /// Resolve overrides and materialize any required bundled compiler inputs.
    ///
    /// # Errors
    /// Returns unsupported-platform or filesystem errors.
    pub fn new() -> io::Result<Self> {
        let executable = std::env::var_os("PROTOC").map(PathBuf::from);
        let includes = std::env::var_os("PROTOC_INCLUDE").map(PathBuf::from);
        let bundle = if executable.is_none() || includes.is_none() {
            Some(tempfile::tempdir()?)
        } else {
            None
        };
        let executable = if let Some(path) = executable {
            path
        } else {
            if EXECUTABLE.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "no bundled protoc for this platform; set PROTOC",
                ));
            }
            let path = bundle
                .as_ref()
                .ok_or_else(|| io::Error::other("missing compiler bundle"))?
                .path()
                .join(format!("protoc{}", std::env::consts::EXE_SUFFIX));
            fs::write(&path, EXECUTABLE)?;
            fs::set_permissions(&path, fs::metadata(std::env::current_exe()?)?.permissions())?;
            path
        };
        let includes = if let Some(path) = includes {
            path
        } else {
            let root = bundle
                .as_ref()
                .ok_or_else(|| io::Error::other("missing compiler bundle"))?
                .path()
                .join("include");
            for (relative, bytes) in INCLUDES {
                let path = root.join(relative);
                fs::create_dir_all(
                    path.parent()
                        .ok_or_else(|| io::Error::other("missing include parent"))?,
                )?;
                fs::write(path, bytes)?;
            }
            root
        };
        Ok(Self {
            executable,
            includes,
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
}
