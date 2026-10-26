use super::*;
use crate::NativeTreeMaterializationError as Error;
use crate::native_host::{HostRoot, NativeOpenOptions};

/// Runs bounded I/O against one witnessed native directory.
pub fn native_file_io<T>(
    source_root: &Path,
    source_identity: NativeRootIdentity,
    path: &Path,
    write: bool,
    parents: bool,
    work: &NativeMaterializationWork,
    operation: impl FnOnce(&mut std::fs::File, &NativeMaterializationWork) -> Result<T, Error>,
) -> Result<T, Error> {
    if path.as_os_str().is_empty()
        || path
            .components()
            .any(|part| !matches!(part, std::path::Component::Normal(_)))
    {
        return Err(Error::InvalidLayout);
    }
    let root = HostRoot::open(source_root, &mut &*work)?;
    if root.identity() != source_identity {
        return Err(Error::ExternalMutation);
    }
    let parent_path = path.parent().ok_or(Error::InvalidLayout)?;
    let leaf = Path::new(path.file_name().ok_or(Error::InvalidLayout)?);
    #[cfg(unix)]
    let directory = crate::native_host::directory_path(
        &root.directory,
        parent_path,
        write && parents,
        &mut &*work,
    )?;
    #[cfg(windows)]
    let directory = crate::native_host::with_wide_path(parent_path, &mut &*work, |path, work| {
        crate::native_host::directory_path(&root.directory, path, write && parents, work)
    })?;
    let options = if !write {
        NativeOpenOptions::Read
    } else if parents {
        NativeOpenOptions::ReadWriteCreate
    } else {
        NativeOpenOptions::ReadWrite
    };
    let mut file = crate::native_host::open_leaf(&directory, leaf, &options, &mut &*work)?;
    let before = crate::native_host::stat_file(&file, &mut &*work)?;
    if write && before.nlink() != 1 {
        return Err(Error::InvalidLayout);
    }
    let result = operation(&mut file, work);
    let after = crate::native_host::stat_file(&file, &mut &*work)?;
    let parent = HostRoot::from_parts(directory, root.identity());
    #[cfg(unix)]
    let named = parent.stat(leaf, &mut &*work)?;
    #[cfg(windows)]
    let named = crate::native_host::with_wide_path(leaf, &mut &*work, |path, work| {
        parent.stat(path, work)
    })?;
    if before.identity() != after.identity()
        || after.identity() != named.identity()
        || (write && after.nlink() != 1)
    {
        return Err(Error::ExternalMutation);
    }
    result
}
