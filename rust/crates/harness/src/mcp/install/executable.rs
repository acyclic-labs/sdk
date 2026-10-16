use std::{
    ffi::OsString,
    io,
    path::{Path, PathBuf},
};

pub(super) fn resolve(
    command: &str,
    search_path: Option<OsString>,
    cwd: &Path,
) -> io::Result<PathBuf> {
    // Keep the absolute launcher selected by PATH/cwd. Resolving its symlinks can
    // change argv[0]-dependent behavior, including Python virtualenv selection.
    which::which_in(command, search_path, cwd).map_err(|_| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "MCP executable was not found in its configured search path",
        )
    })
}
