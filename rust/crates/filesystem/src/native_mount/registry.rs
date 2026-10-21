use super::*;

// Process configuration is initialized before bounded invocations, just like
// the callback runtime. SDK retirement never unlinks this directory or file.
static DIRECTORY: std::sync::OnceLock<Result<PathBuf, Arc<std::io::Error>>> =
    std::sync::OnceLock::new();

pub(super) fn initialize() -> Result<(), NativeMountError> {
    DIRECTORY.get_or_init(|| {
        let directory = persistent_base()
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "native mount registry requires the user's persistent data directory",
                )
            })
            .and_then(|base| {
                let directory = base.join("acyclic").join("native-mount");
                std::fs::create_dir_all(&directory)?;
                let metadata = std::fs::symlink_metadata(&directory)?;
                if !metadata.is_dir() || metadata.file_type().is_symlink() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "native mount registry is not an ordinary directory",
                    ));
                }
                directory.canonicalize()
            });
        directory.map_err(Arc::new)
    });
    directory().map(|_| ())
}

fn directory() -> Result<&'static Path, NativeMountError> {
    match DIRECTORY
        .get()
        .ok_or(NativeMountError::ServicesNotInitialized)?
    {
        Ok(directory) => Ok(directory),
        Err(error) => Err(NativeMountError::ServiceUnavailable(Arc::clone(error))),
    }
}

// Ordinary control-path queries run before any bounded native invocation.
pub(super) fn path() -> Result<PathBuf, NativeMountError> {
    #[cfg(test)]
    #[cfg(all(acyclic_fs_native_loom, target_os = "linux"))]
    if let Some(kernel) = super::disconnected::kernel::current()
        && let Some(directory) = kernel.registry_directory()
    {
        return Ok(directory.join(NATIVE_REGISTRY_NAME));
    }
    initialize()?;
    Ok(directory()?.join(NATIVE_REGISTRY_NAME))
}

pub(super) fn open<W: crate::native_host::NativeHostWork>(
    work: &mut W,
) -> Result<fence_io::MountLockFile, NativeMountError>
where
    NativeMountError: From<W::Error>,
{
    #[cfg(test)]
    #[cfg(all(acyclic_fs_native_loom, target_os = "linux"))]
    if let Some(kernel) = super::disconnected::kernel::current()
        && let Some(directory) = kernel.registry_directory()
    {
        return open_in_directory(directory, work);
    }
    open_in_directory(directory()?, work)
}

fn open_in_directory<W: crate::native_host::NativeHostWork>(
    directory: &Path,
    work: &mut W,
) -> Result<fence_io::MountLockFile, NativeMountError>
where
    NativeMountError: From<W::Error>,
{
    let (directory, _) = crate::native_host::open_root_directory(directory, work)?;
    Ok(crate::native_host::open_leaf(
        &directory,
        Path::new(NATIVE_REGISTRY_NAME),
        &crate::native_host::NativeOpenOptions::ReadWriteCreate,
        work,
    )?
    .into())
}

// Match the service's persistent state-root selection before consulting OS
// defaults. Known Folder lookup ignores an explicitly supplied LOCALAPPDATA.
fn persistent_base() -> Option<PathBuf> {
    #[cfg(windows)]
    if let Some(directory) =
        std::env::var_os("LOCALAPPDATA").filter(|directory| !directory.is_empty())
    {
        return Some(directory.into());
    }
    #[cfg(not(windows))]
    {
        if let Some(directory) =
            std::env::var_os("XDG_STATE_HOME").filter(|directory| !directory.is_empty())
        {
            return Some(directory.into());
        }
        if let Some(directory) = std::env::var_os("HOME").filter(|directory| !directory.is_empty())
        {
            return Some(PathBuf::from(directory).join(".local").join("state"));
        }
    }
    dirs::data_local_dir()
}

#[cfg(test)]
mod tests;
