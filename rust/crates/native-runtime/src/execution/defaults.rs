//! Explicit runtime dependencies for the shared workspace policy. These are
//! read grants, never ambient drive or home-directory access.

use std::{io, path::PathBuf};

/// Native runtime paths granted read/execute access by the workspace policy.
/// Missing optional platform directories are omitted; all returned roots are
/// subsequently pinned and validated by `ExecutionGrant::composed`.
pub(super) fn runtime_read_roots() -> io::Result<Vec<PathBuf>> {
    #[cfg(target_os = "linux")]
    let mut paths = [
        "/usr",
        "/bin",
        "/sbin",
        "/lib",
        "/lib64",
        "/etc/ld.so.cache",
        // Crypto runtimes read their public configuration during startup.
        // Grant the files, never the adjacent private-key directories.
        "/etc/ssl/openssl.cnf",
        "/etc/pki/tls/openssl.cnf",
    ]
    .into_iter()
    .map(PathBuf::from)
    .collect::<Vec<_>>();
    #[cfg(target_os = "macos")]
    let mut paths = [
        "/System/Library",
        "/usr",
        "/bin",
        "/sbin",
        "/Library/Apple/System/Library",
        // dyld's libignition bootstrap resolves shared caches through these
        // graft points on snapshot and live-filesystem boots.
        "/System/Volumes/Preboot/Cryptexes/App/System",
        "/System/Volumes/Preboot/Cryptexes/Incoming/OS",
        "/System/Volumes/Preboot/Cryptexes/OS",
        "/System/Cryptexes/App",
        "/System/Cryptexes/OS",
        "/dev/null",
    ]
    .into_iter()
    .map(PathBuf::from)
    .collect::<Vec<_>>();
    #[cfg(windows)]
    let mut paths = vec![PathBuf::from(std::env::var_os("SystemRoot").ok_or_else(
        || {
            io::Error::new(
                io::ErrorKind::NotFound,
                "SystemRoot is required by the native runtime",
            )
        },
    )?)];
    #[cfg(not(any(windows, target_os = "linux", target_os = "macos")))]
    let paths = Vec::<PathBuf>::new();
    #[cfg(any(windows, target_os = "linux", target_os = "macos"))]
    if let Ok(selected) = crate::platform_shell() {
        // Use the host's once-selected shell, never a model-supplied program
        // or PATH. Its installation directory includes native runtime files
        // (for example PowerShell 7's .NET files beside pwsh.exe).
        // A literal native invocation need not have a shell on PATH. A missing
        // shell adds no authority; its own launch resolver still reports that
        // absence if the caller requests it. A selected path must still bind.
        let shell = selected.canonicalize()?;
        paths.push(
            shell
                .parent()
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "native shell has no runtime directory",
                    )
                })?
                .to_path_buf(),
        );
    }
    paths
        .into_iter()
        .filter_map(|path| match path.try_exists() {
            Ok(true) => Some(Ok(path)),
            Ok(false) => None,
            Err(error) => Some(Err(error)),
        })
        .collect()
}
