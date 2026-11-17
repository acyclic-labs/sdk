use std::path::{Path, PathBuf};

// A declared tool may be a runfiles alias; its companions are beside the
// resolved executable. Bare tool names still use the host search path.
pub(crate) fn resolve(path: impl AsRef<Path>) -> PathBuf {
    let path = path.as_ref();
    if path.components().count() > 1 || path.is_absolute() {
        return path.canonicalize().expect("resolve declared fixture tool");
    }
    std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default())
        .flat_map(|directory| {
            [
                directory.join(path),
                directory.join(format!(
                    "{}{}",
                    path.display(),
                    std::env::consts::EXE_SUFFIX
                )),
            ]
        })
        .find_map(|candidate| candidate.canonicalize().ok().filter(|path| path.is_file()))
        .expect("find fixture tool on PATH")
}
