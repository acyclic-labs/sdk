use super::*;

pub(crate) fn unix_name<W: NativeHostWork, T>(
    name: &OsStr,
    work: &mut W,
    operation: impl FnOnce(&std::ffi::CStr, &mut W) -> Result<T, W::Error>,
) -> Result<T, W::Error> {
    use std::os::unix::ffi::OsStrExt as _;
    let bytes = name.as_bytes();
    for byte in bytes {
        work.started_io(ITEM)?;
        if *byte == 0 {
            return Err(W::invalid_request());
        }
    }
    let (mut owned, allocation) = buffer(
        bytes
            .len()
            .checked_add(1)
            .ok_or(crate::WorkError::Overflow)?,
        work,
    )?;
    let result = (|| {
        work.started_io(copy_work(bytes.len() as u64))?;
        owned.extend_from_slice(bytes);
        owned.push(0);
        operation(
            std::ffi::CStr::from_bytes_with_nul(&owned).expect("validated terminated name"),
            work,
        )
    })();
    drop(owned);
    work.release(allocation)?;
    result
}

pub(super) fn create_unix_directory_leaf<W: NativeHostWork>(
    parent: &Dir,
    leaf: &std::ffi::CStr,
    work: &mut W,
) -> Result<(), W::Error> {
    // SAFETY: held parent and terminated leaf remain live during each attempt.
    unix_mutation(work, || unsafe {
        libc::mkdirat(parent.as_raw_fd(), leaf.as_ptr(), 0o777)
    })
}

pub(crate) fn open_unix_directory<W: NativeHostWork>(
    parent: &Dir,
    name: &OsStr,
    create: bool,
    work: &mut W,
) -> Result<Dir, W::Error> {
    unix_name(name, work, |name, work| {
        let flags = libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW;
        #[cfg(target_os = "linux")]
        let flags = flags | libc::O_NOATIME;
        let open =
            |work: &mut W| open_unix(parent.as_raw_fd(), name, flags, work).map(Dir::from_std_file);
        match open(work) {
            Err(error) if create && W::io_error_kind(&error) == Some(io::ErrorKind::NotFound) => {}
            result => return result,
        }
        match create_unix_directory_leaf(parent, name, work) {
            Err(error) if W::io_error_kind(&error) != Some(io::ErrorKind::AlreadyExists) => {
                return Err(error);
            }
            _ => {}
        }
        open(work)
    })
}

pub(super) fn open_unix_directory_path<W: NativeHostWork>(
    root: &Dir,
    path: &Path,
    create: bool,
    work: &mut W,
) -> Result<Dir, W::Error> {
    let mut current = clone_directory(root, work)?;
    for component in path.components() {
        work.started_io(ITEM)?;
        let std::path::Component::Normal(name) = component else {
            return Err(W::invalid_request());
        };
        current = open_unix_directory(&current, name, create, work)?;
    }
    Ok(current)
}

pub(super) fn with_unix_parent_leaf<W: NativeHostWork, T>(
    root: &Dir,
    path: &Path,
    work: &mut W,
    operation: impl FnOnce(&Dir, &std::ffi::CStr, &mut W) -> Result<T, W::Error>,
) -> Result<T, W::Error> {
    let leaf = path.file_name().ok_or_else(W::invalid_request)?;
    let parent = open_unix_directory_path(
        root,
        path.parent().ok_or_else(W::invalid_request)?,
        false,
        work,
    )?;
    unix_name(leaf, work, |leaf, work| operation(&parent, leaf, work))
}
