//! One executable search policy for every native host adapter.
use std::{
    ffi::{OsStr, OsString},
    io,
    path::{Component, Path, PathBuf},
    process::Command,
};

/// An explicit executable search snapshot. Resolution preserves launcher
/// symlinks and returns an absolute invocation path, so spawning never performs
/// a second, potentially different PATH search.
pub struct ExecutableSearch {
    cwd: PathBuf,
    path: Option<OsString>,
    extensions: Vec<OsString>,
}

impl ExecutableSearch {
    /// Uses the supplied PATH and extension policy. `None` means exact names;
    /// Windows environment adapters supply PATHEXT or its platform default.
    #[must_use]
    pub fn new(cwd: &Path, path: Option<OsString>, extensions: Option<&OsStr>) -> Self {
        let extensions = extensions
            .and_then(OsStr::to_str)
            .into_iter()
            .flat_map(|value| value.split(';'))
            .filter(|extension| {
                extension.starts_with('.')
                    && extension.len() > 1
                    && !extension.contains(['/', '\\', ':', '\0'])
            })
            .map(|extension| OsString::from(extension.to_ascii_lowercase()))
            .collect();
        Self {
            cwd: cwd.to_path_buf(),
            path,
            extensions,
        }
    }

    /// Captures only the variables needed for lookup through the host's own
    /// environment seam. No process-global environment mutation is necessary.
    #[must_use]
    pub fn from_environment(cwd: &Path, environment: impl Fn(&str) -> Option<OsString>) -> Self {
        let extensions = cfg!(windows)
            .then(|| environment("PATHEXT").unwrap_or_else(|| ".COM;.EXE;.BAT;.CMD".into()));
        Self::new(cwd, environment("PATH"), extensions.as_deref())
    }

    /// Resolves exact or PATH-relative names, including Windows batch launchers.
    pub fn resolve(&self, program: impl AsRef<OsStr>) -> io::Result<PathBuf> {
        self.find(program.as_ref(), |candidate| {
            runnable(candidate).then(|| candidate.to_owned())
        })
    }

    /// Creates a command with the resolved absolute program. Rust's process
    /// implementation owns Windows batch argument encoding; callers never
    /// interpolate the executable path or its arguments into shell source.
    pub fn command(&self, program: impl AsRef<OsStr>) -> io::Result<Command> {
        self.resolve(program).map(Command::new)
    }

    /// Finds a directly accessible native image for process containment. An
    /// activation alias or other non-image does not hide a later PATH entry.
    pub fn resolve_native(&self, program: impl AsRef<OsStr>) -> io::Result<PathBuf> {
        self.find(program.as_ref(), |candidate| {
            #[cfg(windows)]
            {
                crate::executable::resolve_native_executable(candidate).ok()
            }
            #[cfg(not(windows))]
            {
                runnable(candidate).then(|| candidate.to_owned())
            }
        })
    }

    fn find(
        &self,
        program: &OsStr,
        mut admit: impl FnMut(&Path) -> Option<PathBuf>,
    ) -> io::Result<PathBuf> {
        let name = Path::new(program);
        if program.is_empty()
            || program.as_encoded_bytes().contains(&0)
            || !self.cwd.is_absolute()
            || (!name.is_absolute()
                && matches!(name.components().next(), Some(Component::Prefix(_))))
        {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        if name.is_absolute() || name.components().count() > 1 {
            if let Some(path) = self.in_directory(&self.cwd, name, &mut admit) {
                return Ok(path);
            }
        } else if let Some(path) = &self.path {
            for directory in std::env::split_paths(path) {
                if let Some(path) = self.in_directory(&self.cwd.join(directory), name, &mut admit) {
                    return Ok(path);
                }
            }
        }
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            "executable was not found in its configured search path",
        ))
    }

    fn in_directory(
        &self,
        directory: &Path,
        name: &Path,
        admit: &mut impl FnMut(&Path) -> Option<PathBuf>,
    ) -> Option<PathBuf> {
        let candidate = directory.join(name);
        if (self.extensions.is_empty() || name.extension().is_some())
            && let Some(path) = admit(&candidate)
        {
            return Some(path);
        }
        for extension in &self.extensions {
            let mut candidate = candidate.as_os_str().to_owned();
            candidate.push(extension);
            if let Some(path) = admit(Path::new(&candidate)) {
                return Some(path);
            }
        }
        None
    }
}

fn runnable(candidate: &Path) -> bool {
    std::fs::metadata(candidate).is_ok_and(|metadata| {
        if !metadata.is_file() {
            return false;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            metadata.permissions().mode() & 0o111 != 0
        }
        #[cfg(not(unix))]
        {
            true
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_executable(path: &Path) {
        std::fs::write(path, []).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
    }

    #[test]
    fn search_preserves_directory_then_extension_order_and_literal_paths() {
        let root = tempfile::tempdir().unwrap();
        let first = root.path().join("first & %");
        let second = root.path().join("second");
        std::fs::create_dir(&first).unwrap();
        std::fs::create_dir(&second).unwrap();
        fixture_executable(&second.join("codex.cmd"));
        fixture_executable(&second.join("codex"));
        #[cfg(unix)]
        std::fs::write(first.join("codex"), []).unwrap();
        let path = Some(std::env::join_paths([&first, &second]).unwrap());
        let search = ExecutableSearch::new(
            root.path(),
            path.clone(),
            Some(OsStr::new(".COM;.EXE;.BAT;.CMD")),
        );
        assert_eq!(search.resolve("codex").unwrap(), second.join("codex.cmd"));
        assert_eq!(
            ExecutableSearch::new(root.path(), path, None)
                .resolve("codex")
                .unwrap(),
            second.join("codex")
        );
        fixture_executable(&first.join("codex.exe"));
        assert_eq!(search.resolve("codex").unwrap(), first.join("codex.exe"));
        assert_eq!(
            search.resolve(first.join("codex.exe")).unwrap(),
            first.join("codex.exe")
        );
        assert!(search.resolve("absent").is_err());
    }

    #[test]
    fn relative_search_paths_use_the_bound_cwd_without_resolving_launcher_symlinks() {
        let root = tempfile::tempdir().unwrap();
        let bin = root.path().join("bin");
        std::fs::create_dir(&bin).unwrap();
        let image = bin.join("launcher");
        #[cfg(unix)]
        std::os::unix::fs::symlink("/bin/sh", &image).unwrap();
        #[cfg(not(unix))]
        fixture_executable(&image);
        let search = ExecutableSearch::new(root.path(), Some("bin".into()), None);
        assert_eq!(search.resolve("launcher").unwrap(), image);
        assert_eq!(search.resolve("bin/launcher").unwrap(), image);
        assert!(
            ExecutableSearch::new(root.path(), None, None)
                .resolve("launcher")
                .is_err()
        );
        assert!(search.resolve(OsStr::new("invalid\0name")).is_err());
    }

    #[cfg(windows)]
    #[test]
    fn native_search_rejects_an_activation_alias_before_a_later_direct_image() {
        let root = tempfile::tempdir().unwrap();
        let first = root.path().join("first");
        let second = root.path().join("second");
        std::fs::create_dir(&first).unwrap();
        std::fs::create_dir(&second).unwrap();
        std::fs::write(first.join("shell.exe"), []).unwrap();
        std::fs::hard_link(std::env::current_exe().unwrap(), second.join("shell.exe")).unwrap();
        let search = ExecutableSearch::new(
            root.path(),
            Some(std::env::join_paths([&first, &second]).unwrap()),
            Some(OsStr::new(".EXE")),
        );
        assert_eq!(
            search.resolve_native("shell").unwrap(),
            second.join("shell.exe").canonicalize().unwrap()
        );
    }
}
