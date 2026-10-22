use super::*;
use std::os::windows::{
    ffi::OsStrExt as _,
    io::{AsRawHandle as _, FromRawHandle as _},
};

pub(super) fn status_error(status: win::NTSTATUS) -> io::Error {
    if status.0 == 0xC000_0056_u32.cast_signed() {
        return io::ErrorKind::NotFound.into();
    }
    // SAFETY: translates a status without accessing memory.
    io::Error::from_raw_os_error(unsafe { win::RtlNtStatusToDosError(status) }.cast_signed())
}

pub(super) fn wide_units<W: NativeHostWork>(name: &OsStr, work: &mut W) -> Result<usize, W::Error> {
    let mut count = 0usize;
    for _ in name.encode_wide() {
        work.started_io(ITEM)?;
        count = count.checked_add(1).ok_or(crate::WorkError::Overflow)?;
    }
    Ok(count)
}

pub(crate) fn with_wide_path<W: NativeHostWork, T>(
    path: &Path,
    work: &mut W,
    operation: impl FnOnce(&[u16], &mut W) -> Result<T, W::Error>,
) -> Result<T, W::Error> {
    with_wide_units(path.as_os_str().encode_wide(), work, operation)
}

pub(crate) fn with_wide_units<W: NativeHostWork, T>(
    units: impl Iterator<Item = u16> + Clone,
    work: &mut W,
    operation: impl FnOnce(&[u16], &mut W) -> Result<T, W::Error>,
) -> Result<T, W::Error> {
    let (name, allocation) = prepare_wide_units(units, work)?;
    let result = operation(&name, work);
    drop(name);
    work.release(allocation)?;
    result
}

pub(crate) fn prepare_wide_units<W: NativeHostWork>(
    units: impl Iterator<Item = u16> + Clone,
    work: &mut W,
) -> Result<(Vec<u16>, W::Allocation), W::Error> {
    let mut length = 0usize;
    for _ in units.clone() {
        work.started_io(ITEM)?;
        length = length.checked_add(1).ok_or(crate::WorkError::Overflow)?;
    }
    let encoded = WorkCounters {
        bytes_encoded: length.checked_mul(2).ok_or(crate::WorkError::Overflow)? as u64,
        ..WorkCounters::UNCHARGED
    };
    work.admit(encoded)?;
    let (mut name, allocation) = buffer(length, work)?;
    name.extend(units.map(|u| if u == 47 { 92 } else { u }));
    if let Err(error) = work.record(encoded) {
        drop(name);
        work.release(allocation)?;
        return Err(error);
    }
    Ok((name, allocation))
}

pub(super) fn windows_leaf_name<W: NativeHostWork>(
    name: &[u16],
    work: &mut W,
) -> Result<win::UNICODE_STRING, W::Error> {
    work.check()?;
    if matches!(name, [] | [46] | [46, 46]) {
        return Err(W::invalid_request());
    }
    for unit in name {
        work.started_io(ITEM)?;
        if matches!(unit, 0 | 47 | 58 | 92) {
            return Err(W::invalid_request());
        }
    }
    let length = u16::try_from(
        name.len()
            .checked_mul(2)
            .ok_or(crate::WorkError::Overflow)?,
    )
    .map_err(|_| W::invalid_request())?;
    Ok(win::UNICODE_STRING {
        Length: length,
        MaximumLength: length,
        Buffer: windows::core::PWSTR(name.as_ptr().cast_mut()),
    })
}

pub(super) fn windows_open_name<W: NativeHostWork>(
    path: &[u16],
    work: &mut W,
) -> Result<win::UNICODE_STRING, W::Error> {
    Ok(if path == [46] {
        work.check()?;
        // NT names the held directory with an empty string, never ".".
        win::UNICODE_STRING {
            Buffer: windows::core::PWSTR(path.as_ptr().cast_mut()),
            ..Default::default()
        }
    } else {
        windows_leaf_name(path, work)?
    })
}

pub(crate) fn open_windows_at<W: NativeHostWork>(
    parent: &Dir,
    path: &[u16],
    access: fs::FILE_ACCESS_RIGHTS,
    disposition: nt::NTCREATEFILE_CREATE_DISPOSITION,
    options: nt::NTCREATEFILE_CREATE_OPTIONS,
    work: &mut W,
) -> Result<File, W::Error> {
    let name = windows_open_name(path, work)?;
    open_windows_named(parent, &name, access, disposition, options, work)
}

pub(super) fn open_windows_named<W: NativeHostWork>(
    parent: &Dir,
    name: &win::UNICODE_STRING,
    access: fs::FILE_ACCESS_RIGHTS,
    disposition: nt::NTCREATEFILE_CREATE_DISPOSITION,
    options: nt::NTCREATEFILE_CREATE_OPTIONS,
    work: &mut W,
) -> Result<File, W::Error> {
    open_windows_named_with_options(
        parent,
        name,
        access,
        disposition,
        options | nt::FILE_OPEN_REPARSE_POINT,
        win::OBJ_CASE_INSENSITIVE,
        work,
    )
}

fn open_windows_named_with_options<W: NativeHostWork>(
    parent: &Dir,
    name: &win::UNICODE_STRING,
    access: fs::FILE_ACCESS_RIGHTS,
    disposition: nt::NTCREATEFILE_CREATE_DISPOSITION,
    options: nt::NTCREATEFILE_CREATE_OPTIONS,
    attributes: win::OBJECT_ATTRIBUTE_FLAGS,
    work: &mut W,
) -> Result<File, W::Error> {
    let attributes = windows::Wdk::Foundation::OBJECT_ATTRIBUTES {
        Length: u32::try_from(std::mem::size_of::<
            windows::Wdk::Foundation::OBJECT_ATTRIBUTES,
        >())
        .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?,
        RootDirectory: win::HANDLE(parent.as_raw_handle()),
        ObjectName: name,
        Attributes: attributes,
        ..Default::default()
    };
    work.attempt_io(
        if disposition == nt::FILE_OPEN {
            READ
        } else {
            WRITE
        },
        || {
            let mut block = windows::Win32::System::IO::IO_STATUS_BLOCK::default();
            let mut handle = win::HANDLE::default();
            // SAFETY: the held parent, validated leaf, and outputs remain live for this call.
            #[cfg(test)]
            super::stat_probe::event("NtCreateFile");
            let status = unsafe {
                nt::NtCreateFile(
                    &mut handle,
                    access,
                    &attributes,
                    &mut block,
                    None,
                    fs::FILE_ATTRIBUTE_NORMAL,
                    fs::FILE_SHARE_READ | fs::FILE_SHARE_WRITE | fs::FILE_SHARE_DELETE,
                    disposition,
                    options,
                    None,
                    0,
                )
            };
            if (attributes.Attributes & win::OBJ_DONT_REPARSE).0 != 0
                && status.0 == 0xC000_050B_u32.cast_signed()
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "host root is not a real directory",
                ));
            }
            if status.is_err() {
                return Err(status_error(status));
            }
            // SAFETY: successful NtCreateFile transfers a fresh owned handle.
            Ok(unsafe { File::from_raw_handle(handle.0) })
        },
    )
}

pub(super) fn windows_stat_information<W: NativeHostWork>(
    file: &impl std::os::windows::io::AsHandle,
    work: &mut W,
) -> Result<nt::FILE_STAT_INFORMATION, W::Error> {
    work.attempt_io(READ, || {
        let mut block = windows::Win32::System::IO::IO_STATUS_BLOCK::default();
        let mut information = nt::FILE_STAT_INFORMATION::default();
        // SAFETY: the held handle and fixed-size output remain live for the query.
        #[cfg(test)]
        super::stat_probe::event("NtQueryInformationFile");
        let status = unsafe {
            nt::NtQueryInformationFile(
                win::HANDLE(file.as_handle().as_raw_handle()),
                &mut block,
                (&raw mut information).cast(),
                u32::try_from(std::mem::size_of_val(&information))
                    .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?,
                nt::FileStatInformation,
            )
        };
        if status.is_err() {
            return Err(status_error(status));
        }
        Ok(information)
    })
}

pub(crate) fn stat_windows_file<W: NativeHostWork>(
    file: &impl std::os::windows::io::AsHandle,
    volume: u32,
    work: &mut W,
) -> Result<HostStat, W::Error> {
    Ok(HostStat::from_stat_information(
        &windows_stat_information(file, work)?,
        volume,
    )?)
}

pub(crate) fn stat_windows_name(
    directory: &Dir,
    name: &win::UNICODE_STRING,
    volume: u32,
    include_reparse: bool,
) -> io::Result<Option<HostStat>> {
    let attributes = windows::Wdk::Foundation::OBJECT_ATTRIBUTES {
        Length: u32::try_from(std::mem::size_of::<
            windows::Wdk::Foundation::OBJECT_ATTRIBUTES,
        >())
        .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?,
        RootDirectory: win::HANDLE(directory.as_raw_handle()),
        ObjectName: std::ptr::from_ref(name),
        Attributes: win::OBJ_CASE_INSENSITIVE | win::OBJ_DONT_REPARSE,
        ..Default::default()
    };
    let mut block = windows::Win32::System::IO::IO_STATUS_BLOCK::default();
    let mut information = nt::FILE_STAT_INFORMATION::default();
    // SAFETY: the held directory, name, and fixed-size output outlive the query.
    #[cfg(test)]
    super::stat_probe::event("NtQueryInformationByName");
    let status = unsafe {
        nt::NtQueryInformationByName(
            &attributes,
            &mut block,
            (&raw mut information).cast(),
            u32::try_from(std::mem::size_of_val(&information))
                .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?,
            nt::FileStatInformation,
        )
    };
    if status.0 == 0xC000_050B_u32.cast_signed() {
        return Ok(None);
    }
    if status.is_err() {
        return Err(status_error(status));
    }
    if !include_reparse && information.ReparseTag != 0 {
        return Ok(None);
    }
    Ok(Some(HostStat::from_stat_information(&information, volume)?))
}

#[cfg(any(feature = "native-watch", feature = "native-mount", test))]
pub(crate) fn query_windows_stat<W: NativeHostWork>(
    work: &mut W,
    charge: WorkCounters,
    query: impl FnOnce() -> io::Result<Option<HostStat>>,
    fallback: impl FnOnce(&mut W) -> Result<HostStat, W::Error>,
) -> Result<HostStat, W::Error> {
    match work.attempt_io(charge, || match query() {
        Err(error) if matches!(error.raw_os_error(), Some(1 | 50 | 87 | 124)) => Ok(None),
        result => result,
    })? {
        Some(stat) => Ok(stat),
        None => fallback(work),
    }
}

pub(crate) fn stat_windows_leaf<W: NativeHostWork>(
    parent: &Dir,
    name: &[u16],
    work: &mut W,
) -> Result<HostStat, W::Error> {
    let volume = u32::try_from(directory_identity(parent, work)?.device)
        .map_err(|_| W::invalid_request())?;
    let file = open_windows_at(
        parent,
        name,
        fs::FILE_READ_ATTRIBUTES | fs::SYNCHRONIZE,
        nt::FILE_OPEN,
        nt::FILE_SYNCHRONOUS_IO_NONALERT,
        work,
    )?;
    stat_windows_file(&file, volume, work)
}

#[cfg(any(test, feature = "native-watch"))]
pub(crate) fn open_windows_directory<W: NativeHostWork>(
    parent: &Dir,
    name: &[u16],
    create: bool,
    work: &mut W,
) -> Result<(Dir, NativeRootIdentity), W::Error> {
    let disposition = if create {
        nt::FILE_OPEN_IF
    } else {
        nt::FILE_OPEN
    };
    let directory = Dir::from_std_file(open_windows_at(
        parent,
        name,
        fs::FILE_GENERIC_READ,
        disposition,
        nt::FILE_DIRECTORY_FILE | nt::FILE_SYNCHRONOUS_IO_NONALERT,
        work,
    )?);
    let identity = directory_identity(&directory, work)?;
    Ok((directory, identity))
}

/// Opens one real directory component, retaining the original read access check.
/// The NT open rejects reparse points itself, so a caller that does not need an
/// identity need not query one merely to reject the same reparse point again.
pub(crate) fn open_windows_directory_handle<W: NativeHostWork>(
    parent: &Dir,
    name: &[u16],
    create: bool,
    work: &mut W,
) -> Result<Dir, W::Error> {
    let name = windows_open_name(name, work)?;
    open_windows_named_with_options(
        parent,
        &name,
        fs::FILE_GENERIC_READ,
        if create {
            nt::FILE_OPEN_IF
        } else {
            nt::FILE_OPEN
        },
        nt::FILE_DIRECTORY_FILE | nt::FILE_SYNCHRONOUS_IO_NONALERT,
        win::OBJ_CASE_INSENSITIVE | win::OBJ_DONT_REPARSE,
        work,
    )
    .map(Dir::from_std_file)
}

pub(super) fn windows_set_information<T: ?Sized>(
    file: &impl std::os::windows::io::AsRawHandle,
    information: &T,
    class: nt::FILE_INFORMATION_CLASS,
) -> io::Result<()> {
    let mut block = windows::Win32::System::IO::IO_STATUS_BLOCK::default();
    // SAFETY: the held handle and initialized input remain live for this call.
    let status = unsafe {
        nt::NtSetInformationFile(
            win::HANDLE(file.as_raw_handle()),
            &mut block,
            std::ptr::from_ref(information).cast(),
            u32::try_from(std::mem::size_of_val(information))
                .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?,
            class,
        )
    };
    if status.is_err() {
        return Err(status_error(status));
    }
    Ok(())
}

pub(crate) fn keep_windows_created_file<W: NativeHostWork>(
    file: &File,
    work: &mut W,
) -> Result<(), W::Error> {
    work.attempt_io(WRITE, || {
        windows_set_information(file, &8_u32, nt::FILE_INFORMATION_CLASS(64))
    })
}

pub(crate) fn remove_windows_leaf<W: NativeHostWork>(
    parent: &Dir,
    name: &[u16],
    directory: bool,
    expected: Option<NativeRootIdentity>,
    work: &mut W,
) -> Result<(), W::Error> {
    let file = open_windows_at(
        parent,
        name,
        fs::DELETE | fs::FILE_READ_ATTRIBUTES | fs::SYNCHRONIZE,
        nt::FILE_OPEN,
        nt::FILE_SYNCHRONOUS_IO_NONALERT
            | if directory {
                nt::FILE_DIRECTORY_FILE
            } else {
                nt::FILE_NON_DIRECTORY_FILE
            },
        work,
    )?;
    if let Some(expected) = expected
        && file_identity_and_length(&file, work)?.0 != expected
    {
        return Err(io::Error::other("held deletion target identity changed").into());
    }
    work.attempt_io(WRITE, || {
        windows_set_information(
            &file,
            &nt::FILE_DISPOSITION_INFORMATION { DeleteFile: true },
            nt::FileDispositionInformation,
        )
    })
}

pub(crate) fn set_windows_name<W: NativeHostWork>(
    file: &File,
    parent: &Dir,
    name: &[u16],
    flags: u32,
    class: nt::FILE_INFORMATION_CLASS,
    work: &mut W,
) -> Result<(), W::Error> {
    windows_leaf_name(name, work)?;
    let copied = copy_work(
        (name.len() as u64)
            .checked_mul(2)
            .ok_or(crate::WorkError::Overflow)?,
    );
    work.admit(copied)?;
    let (packet, allocation, length) = windows_name_packet(parent, name, flags, work)?;
    let result = work.record(copied).and_then(|()| {
        work.attempt_io(WRITE, || {
            // SAFETY: initialized aligned storage covers the header and flexible name.
            let bytes = unsafe {
                std::slice::from_raw_parts(
                    packet.as_ptr().cast::<u8>(),
                    (length as usize).max(std::mem::size_of::<nt::FILE_RENAME_INFORMATION>()),
                )
            };
            windows_set_information(file, bytes, class)
        })
    });
    drop(packet);
    work.release(allocation)?;
    result
}

#[cfg(test)]
#[cfg(feature = "native-watch")]
#[path = "tests_windows_stat.rs"]
mod stat_tests;
