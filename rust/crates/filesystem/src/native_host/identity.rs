use super::*;

pub(crate) fn directory_identity<W: NativeHostWork>(
    directory: &Dir,
    work: &mut W,
) -> Result<NativeRootIdentity, W::Error> {
    work.attempt_io(READ, || held_root_identity(directory))
}

#[cfg(unix)]
pub(crate) fn directory_has_ancestor<W: NativeHostWork>(
    directory: &Dir,
    ancestor: NativeRootIdentity,
    work: &mut W,
) -> Result<bool, W::Error> {
    let examined = ITEM;
    work.admit(examined)?;
    work.record(examined)?;
    let mut identity = loop {
        match directory_identity(directory, work) {
            Err(error) if W::io_error_kind(&error) == Some(io::ErrorKind::Interrupted) => continue,
            result => break result?,
        }
    };
    if identity == ancestor {
        return Ok(true);
    }
    let mut current = None::<Dir>;
    loop {
        work.admit(examined)?;
        work.record(examined)?;
        let child = current.as_ref().unwrap_or(directory);
        let parent = Dir::from_std_file(open_unix(
            child.as_raw_fd(),
            c"..",
            cwd_metadata_flags(),
            work,
        )?);
        let parent_identity = loop {
            match directory_identity(&parent, work) {
                Err(error) if W::io_error_kind(&error) == Some(io::ErrorKind::Interrupted) => {
                    continue;
                }
                result => break result?,
            }
        };
        if parent_identity == ancestor {
            return Ok(true);
        }
        if parent_identity == identity {
            return Ok(false);
        }
        identity = parent_identity;
        current = Some(parent);
    }
}

pub(crate) fn file_identity_and_length<W: NativeHostWork>(
    file: &File,
    work: &mut W,
) -> Result<(NativeRootIdentity, u64), W::Error> {
    #[cfg(unix)]
    {
        let stat = loop {
            match fstat(file.as_raw_fd(), work) {
                Err(error) if W::io_error_kind(&error) == Some(io::ErrorKind::Interrupted) => {
                    continue;
                }
                result => break result?,
            }
        };
        let length = u64::try_from(stat.st_size)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "negative file length"))?;
        Ok((
            NativeRootIdentity {
                device: stat.st_dev as u64,
                object: stat.st_ino as u64,
            },
            length,
        ))
    }
    #[cfg(windows)]
    {
        work.attempt_io(READ, || {
            let information = windows_file_information(file)?;
            Ok((
                NativeRootIdentity {
                    device: u64::from(information.dwVolumeSerialNumber),
                    object: (u64::from(information.nFileIndexHigh) << 32)
                        | u64::from(information.nFileIndexLow),
                },
                (u64::from(information.nFileSizeHigh) << 32) | u64::from(information.nFileSizeLow),
            ))
        })
    }
}

pub(crate) fn entry_identity<W: NativeHostWork>(
    parent: &Dir,
    name: &Path,
    work: &mut W,
) -> Result<Option<NativeRootIdentity>, W::Error> {
    require_leaf(name)?;
    #[cfg(unix)]
    {
        unix_name(name.as_os_str(), work, |name, work| {
            loop {
                let result = work.attempt_io(READ, || {
                    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
                    // SAFETY: held parent, terminated leaf, and correctly sized output stay live; the entry itself is never followed.
                    if unsafe {
                        libc::fstatat(
                            parent.as_raw_fd(),
                            name.as_ptr(),
                            stat.as_mut_ptr(),
                            libc::AT_SYMLINK_NOFOLLOW,
                        )
                    } != 0
                    {
                        return Err(io::Error::last_os_error());
                    }
                    let stat = unsafe { stat.assume_init() };
                    Ok(NativeRootIdentity {
                        device: stat.st_dev as u64,
                        object: stat.st_ino as u64,
                    })
                });
                match result {
                    Ok(identity) => return Ok(Some(identity)),
                    Err(error) if W::io_error_kind(&error) == Some(io::ErrorKind::NotFound) => {
                        return Ok(None);
                    }
                    Err(error) if W::io_error_kind(&error) == Some(io::ErrorKind::Interrupted) => {
                        continue;
                    }
                    Err(error) => return Err(error),
                }
            }
        })
    }
    #[cfg(windows)]
    {
        use windows::Wdk::Storage::FileSystem::{
            FILE_OPEN_REPARSE_POINT, FILE_SYNCHRONOUS_IO_NONALERT,
        };
        use windows::Win32::Storage::FileSystem::{FILE_READ_ATTRIBUTES, SYNCHRONIZE};
        let file = match open_windows_held(
            parent,
            name,
            FILE_READ_ATTRIBUTES | SYNCHRONIZE,
            FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT,
            work,
        ) {
            Ok(file) => file,
            Err(error) if W::io_error_kind(&error) == Some(io::ErrorKind::NotFound) => {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        file_identity_and_length(&file, work).map(|(identity, _)| Some(identity))
    }
}
