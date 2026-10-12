use super::*;

pub(crate) fn directory_identity<W: NativeHostWork>(
    directory: &Dir,
    work: &mut W,
) -> Result<NativeRootIdentity, W::Error> {
    #[cfg(unix)]
    {
        let stat = fstat(directory.as_raw_fd(), work)?;
        if stat.st_mode & libc::S_IFMT != libc::S_IFDIR {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "host root is not a real directory",
            )
            .into());
        }
        Ok(NativeRootIdentity {
            device: stat.st_dev as u64,
            object: stat.st_ino as u64,
        })
    }
    #[cfg(windows)]
    work.attempt_io(READ, || {
        let info = windows_file_information(directory)?;
        use windows::Win32::Storage::FileSystem::{
            FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_REPARSE_POINT,
        };
        if info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY.0 == 0
            || info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT.0 != 0
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "host root is not a real directory",
            ));
        }
        Ok(NativeRootIdentity {
            device: info.dwVolumeSerialNumber as u64,
            object: (u64::from(info.nFileIndexHigh) << 32) | u64::from(info.nFileIndexLow),
        })
    })
}

pub(crate) fn file_identity_and_length<W: NativeHostWork>(
    file: &File,
    work: &mut W,
) -> Result<(NativeRootIdentity, u64), W::Error> {
    #[cfg(unix)]
    {
        let stat = fstat(file.as_raw_fd(), work)?;
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
    work.attempt_io(READ, || {
        let info = windows_file_information(file)?;
        Ok((
            NativeRootIdentity {
                device: info.dwVolumeSerialNumber as u64,
                object: (u64::from(info.nFileIndexHigh) << 32) | u64::from(info.nFileIndexLow),
            },
            (u64::from(info.nFileSizeHigh) << 32) | u64::from(info.nFileSizeLow),
        ))
    })
}

pub(crate) fn entry_identity<W: NativeHostWork>(
    parent: &Dir,
    name: &Path,
    work: &mut W,
) -> Result<Option<NativeRootIdentity>, W::Error> {
    require_leaf(name)?;
    #[cfg(unix)]
    let result = unix_name(name.as_os_str(), work, |name, work| {
        loop {
            let result = work.attempt_io(READ, || {
                let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
                // SAFETY: the held parent, terminated leaf and complete output remain live.
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
                Err(e) if W::io_error_kind(&e) == Some(io::ErrorKind::Interrupted) => continue,
                result => break result,
            }
        }
    });
    #[cfg(windows)]
    let result = {
        use windows::{
            Wdk::Storage::FileSystem::{FILE_OPEN_REPARSE_POINT, FILE_SYNCHRONOUS_IO_NONALERT},
            Win32::Storage::FileSystem::{FILE_READ_ATTRIBUTES, SYNCHRONIZE},
        };
        open_windows_held(
            parent,
            name,
            FILE_READ_ATTRIBUTES | SYNCHRONIZE,
            FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT,
            work,
        )
        .and_then(|file| file_identity_and_length(&file, work).map(|(identity, _)| identity))
    };
    match result {
        Ok(identity) => Ok(Some(identity)),
        Err(e) if W::io_error_kind(&e) == Some(io::ErrorKind::NotFound) => Ok(None),
        Err(e) => Err(e),
    }
}

#[cfg(unix)]
pub(crate) fn directory_has_ancestor<W: NativeHostWork>(
    directory: &Dir,
    ancestor: NativeRootIdentity,
    work: &mut W,
) -> Result<bool, W::Error> {
    work.started_io(ITEM)?;
    let mut identity = directory_identity(directory, work)?;
    if identity == ancestor {
        return Ok(true);
    }
    let mut current = None;
    loop {
        work.admit(ITEM)?;
        work.record(ITEM)?;
        let parent = Dir::from_std_file(open_unix(
            current.as_ref().unwrap_or(directory).as_raw_fd(),
            c"..",
            cwd_metadata_flags(),
            work,
        )?);
        let next = directory_identity(&parent, work)?;
        if next == ancestor {
            return Ok(true);
        }
        if next == identity {
            return Ok(false);
        }
        identity = next;
        current = Some(parent);
    }
}
