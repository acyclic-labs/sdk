use super::*;

pub(crate) fn unix_name<W: NativeHostWork, T>(
    name: &OsStr,
    work: &mut W,
    operation: impl FnOnce(&std::ffi::CStr, &mut W) -> Result<T, W::Error>,
) -> Result<T, W::Error> {
    let (owned, allocation) = prepare_unix_name(name, work)?;
    let result = std::ffi::CStr::from_bytes_with_nul(&owned)
        .map_err(|_| W::invalid_request())
        .and_then(|name| operation(name, work));
    drop(owned);
    work.release(allocation)?;
    result
}

pub(crate) fn prepare_unix_name<W: NativeHostWork>(
    name: &OsStr,
    work: &mut W,
) -> Result<(Vec<u8>, W::Allocation), W::Error> {
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
    if let Err(error) = work.started_io(copy_work(bytes.len() as u64)) {
        drop(owned);
        work.release(allocation)?;
        return Err(error);
    }
    owned.extend_from_slice(bytes);
    owned.push(0);
    Ok((owned, allocation))
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
