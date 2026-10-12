use super::*;
use std::os::windows::io::{AsHandle as _, AsRawHandle as _, FromRawHandle as _};
use windows::{
    Wdk::Storage::FileSystem as nt,
    Win32::{Foundation as win, Storage::FileSystem as fs},
};

/// Allows attributes removed by ProjFS hydration.
pub(crate) const fn same_windows_attributes(before: u32, after: u32) -> bool {
    const SPARSE: u32 = 0x0000_0200;
    const REPARSE_POINT: u32 = 0x0000_0400;
    const RECALL_ON_OPEN: u32 = 0x0004_0000;
    const RECALL_ON_DATA_ACCESS: u32 = 0x0040_0000;
    const ARCHIVE: u32 = 0x0000_0020;
    const PROJFS_MARKS: u32 = SPARSE | REPARSE_POINT | RECALL_ON_OPEN | RECALL_ON_DATA_ACCESS;
    let marks = before & PROJFS_MARKS;
    let hydration = if before & RECALL_ON_DATA_ACCESS == 0 {
        0
    } else {
        ARCHIVE
    };
    let ignored = marks | hydration;
    before & !ignored == after & !ignored && after & PROJFS_MARKS & !marks == 0
}

pub(super) fn status_error(status: windows::Win32::Foundation::NTSTATUS) -> io::Error {
    const DELETE_PENDING: windows::Win32::Foundation::NTSTATUS =
        windows::Win32::Foundation::NTSTATUS(0xC000_0056_u32.cast_signed());
    if status == DELETE_PENDING {
        return io::ErrorKind::NotFound.into();
    }
    // SAFETY: a pure status-code translation.
    let code = unsafe { windows::Win32::Foundation::RtlNtStatusToDosError(status) };
    i32::try_from(code).map_or_else(
        |_| io::Error::other(format!("{status:?}")),
        io::Error::from_raw_os_error,
    )
}

pub(super) fn wide_units<W: NativeHostWork>(name: &OsStr, work: &mut W) -> Result<usize, W::Error> {
    use std::os::windows::ffi::OsStrExt as _;
    let mut length = 0usize;
    for _ in name.encode_wide() {
        work.started_io(ITEM)?;
        length = length.checked_add(1).ok_or(crate::WorkError::Overflow)?;
    }
    Ok(length)
}

pub(crate) fn with_wide_path<W: NativeHostWork, T>(
    path: &Path,
    work: &mut W,
    operation: impl FnOnce(&[u16], &mut W) -> Result<T, W::Error>,
) -> Result<T, W::Error> {
    use std::os::windows::ffi::OsStrExt as _;
    work.check()?;
    let length = wide_units(path.as_os_str(), work)?;
    let encoded = WorkCounters {
        bytes_encoded: length.checked_mul(2).ok_or(crate::WorkError::Overflow)? as u64,
        ..WorkCounters::UNCHARGED
    };
    work.admit(encoded)?;
    let (mut name, allocation) = buffer(length, work)?;
    let result = (|| {
        name.extend(
            path.as_os_str()
                .encode_wide()
                .map(|u| if u == 47 { 92 } else { u }),
        );
        work.record(encoded)?;
        operation(&name, work)
    })();
    drop(name);
    work.release(allocation)?;
    result
}

pub(super) fn relative_kernel_name<W: NativeHostWork>(
    path: &[u16],
    work: &mut W,
) -> Result<Option<win::UNICODE_STRING>, W::Error> {
    work.check()?;
    if path.is_empty() {
        return Ok(None);
    }
    if path != [46] {
        work.started_io(ITEM)?;
        let mut begin = 0;
        for (i, unit) in path.iter().enumerate() {
            work.admit(ITEM)?;
            work.record(ITEM)?;
            if matches!(unit, 0 | 47 | 58) {
                return Ok(None);
            }
            if *unit == 92 {
                if matches!(&path[begin..i], [] | [46] | [46, 46]) {
                    return Ok(None);
                }
                begin = i + 1;
                work.admit(ITEM)?;
                work.record(ITEM)?;
            }
        }
        if matches!(&path[begin..], [] | [46] | [46, 46]) {
            return Ok(None);
        }
    }
    // NT names the held directory with an empty string, never ".".
    let name = if path == [46] { &[][..] } else { path };
    let bytes = name
        .len()
        .checked_mul(2)
        .ok_or(crate::WorkError::Overflow)?;
    let length = u16::try_from(bytes).map_err(|_| W::invalid_request())?;
    Ok(Some(win::UNICODE_STRING {
        Length: length,
        MaximumLength: length,
        Buffer: windows::core::PWSTR(name.as_ptr().cast_mut()),
    }))
}

pub(super) fn stat_windows_name<W: NativeHostWork>(
    directory: &Dir,
    name: &win::UNICODE_STRING,
    volume: u32,
    work: &mut W,
) -> Result<Option<HostStat>, W::Error> {
    let attributes = windows::Wdk::Foundation::OBJECT_ATTRIBUTES {
        Length: std::mem::size_of::<windows::Wdk::Foundation::OBJECT_ATTRIBUTES>() as u32,
        RootDirectory: win::HANDLE(directory.as_raw_handle()),
        ObjectName: std::ptr::from_ref(name),
        Attributes: win::OBJ_CASE_INSENSITIVE | win::OBJ_DONT_REPARSE,
        ..Default::default()
    };
    let mut block = windows::Win32::System::IO::IO_STATUS_BLOCK::default();
    let mut information = nt::FILE_STAT_INFORMATION::default();
    work.started_io(READ)?;
    // SAFETY: the held directory, name, and fixed-size output outlive the query.
    let status = unsafe {
        nt::NtQueryInformationByName(
            &attributes,
            &mut block,
            (&raw mut information).cast(),
            std::mem::size_of_val(&information) as u32,
            nt::FileStatInformation,
        )
    };
    if status.0 == 0xC000_050B_u32.cast_signed() {
        return Ok(None);
    }
    if status.is_err() {
        return Err(status_error(status).into());
    }
    if information.ReparseTag != 0 {
        return Ok(None);
    }
    Ok(Some(HostStat::from_stat_information(&information, volume)?))
}

pub(super) fn open_windows_wide_at<W: NativeHostWork>(
    directory: &Dir,
    path: &[u16],
    access: fs::FILE_ACCESS_RIGHTS,
    options: nt::NTCREATEFILE_CREATE_OPTIONS,
    work: &mut W,
) -> Result<Option<cap_std::fs::File>, W::Error> {
    let Some(name) = relative_kernel_name(path, work)? else {
        return Ok(None);
    };
    work.started_io(READ)?;
    match open_windows_name(
        directory,
        &name,
        access,
        nt::FILE_OPEN,
        options,
        win::OBJ_CASE_INSENSITIVE | win::OBJ_DONT_REPARSE,
    ) {
        Ok(file) => Ok(Some(cap_std::fs::File::from_std(file))),
        Err(status) if status.0 == 0xC000_050B_u32.cast_signed() => Ok(None),
        Err(status) => Err(status_error(status).into()),
    }
}

pub(super) fn open_windows_name(
    directory: &Dir,
    name: &win::UNICODE_STRING,
    access: fs::FILE_ACCESS_RIGHTS,
    disposition: nt::NTCREATEFILE_CREATE_DISPOSITION,
    options: nt::NTCREATEFILE_CREATE_OPTIONS,
    flags: win::OBJECT_ATTRIBUTE_FLAGS,
) -> Result<File, win::NTSTATUS> {
    let attributes = windows::Wdk::Foundation::OBJECT_ATTRIBUTES {
        Length: std::mem::size_of::<windows::Wdk::Foundation::OBJECT_ATTRIBUTES>() as u32,
        RootDirectory: win::HANDLE(directory.as_raw_handle()),
        ObjectName: std::ptr::from_ref(name),
        Attributes: flags,
        ..Default::default()
    };
    let mut block = windows::Win32::System::IO::IO_STATUS_BLOCK::default();
    let mut handle = win::HANDLE::default();
    // SAFETY: the held directory, name, and output storage outlive NtCreateFile.
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
    if status.is_err() {
        return Err(status);
    }
    // SAFETY: successful NtCreateFile transfers a fresh owned handle.
    Ok(unsafe { File::from_raw_handle(handle.0) })
}

pub(super) fn windows_stat_information<W: NativeHostWork>(
    file: &impl std::os::windows::io::AsHandle,
    work: &mut W,
) -> Result<nt::FILE_STAT_INFORMATION, W::Error> {
    work.attempt_io(READ, || {
        let mut block = windows::Win32::System::IO::IO_STATUS_BLOCK::default();
        let mut information = nt::FILE_STAT_INFORMATION::default();
        // SAFETY: the held handle and complete output outlive this query.
        let status = unsafe {
            nt::NtQueryInformationFile(
                win::HANDLE(file.as_handle().as_raw_handle()),
                &mut block,
                (&raw mut information).cast(),
                std::mem::size_of_val(&information) as u32,
                nt::FileStatInformation,
            )
        };
        if status.is_err() {
            return Err(status_error(status));
        }
        Ok(information)
    })
}

pub(super) fn stat_windows_file<W: NativeHostWork>(
    file: &impl std::os::windows::io::AsHandle,
    volume: u32,
    work: &mut W,
) -> Result<HostStat, W::Error> {
    Ok(HostStat::from_stat_information(
        &windows_stat_information(file, work)?,
        volume,
    )?)
}

pub(crate) fn stat_windows_leaf<W: NativeHostWork>(
    directory: &Dir,
    path: &[u16],
    work: &mut W,
) -> Result<HostStat, W::Error> {
    let volume = u32::try_from(directory_identity(directory, work)?.device)
        .map_err(|_| io::Error::other("native volume serial exceeds u32"))?;
    let name = windows_leaf_name(path, work)?;
    if let Some(stat) = stat_windows_name(directory, &name, volume, work)? {
        return Ok(stat);
    }
    let file = open_windows_leaf_handle(
        directory,
        path,
        fs::FILE_READ_ATTRIBUTES | fs::SYNCHRONIZE,
        None,
        work,
    )?;
    stat_windows_file(&file, volume, work)
}

pub(super) fn wide_parent_leaf(path: &[u16]) -> Result<(&[u16], &[u16]), DemandError> {
    let (parent, leaf) = match path.iter().rposition(|u| *u == 92) {
        Some(i) => (&path[..i], &path[i + 1..]),
        None => (&[][..], path),
    };
    if leaf.is_empty() {
        return Err(DemandError::InvalidRequest);
    }
    Ok((parent, leaf))
}

impl HostRoot {
    pub(crate) fn open_dir_held_wide<W: NativeHostWork>(
        &self,
        path: &[u16],
        work: &mut W,
    ) -> Result<Dir, W::Error> {
        let mut directory = clone_directory(&self.directory, work)?;
        if !path.is_empty() {
            for name in path.split(|u| *u == 92) {
                work.started_io(ITEM)?;
                directory = open_windows_directory(&directory, name, work)?.0;
            }
        }
        Ok(directory)
    }
    pub(crate) fn stat_wide<W: NativeHostWork>(
        &self,
        path: &[u16],
        work: &mut W,
    ) -> Result<HostStat, W::Error> {
        let volume = self.volume_serial_number()?;
        if path.is_empty() {
            return stat_windows_file(&self.directory, volume, work);
        }
        if let Some(name) = relative_kernel_name(path, work)? {
            if let Some(stat) = stat_windows_name(&self.directory, &name, volume, work)? {
                return Ok(stat);
            }
        }
        let (parent, leaf) = wide_parent_leaf(path).map_err(|_| W::invalid_request())?;
        let parent = self.open_dir_held_wide(parent, work)?;
        let file = open_windows_wide_at(
            &parent,
            leaf,
            fs::FILE_READ_ATTRIBUTES | fs::SYNCHRONIZE,
            nt::FILE_OPEN_REPARSE_POINT | nt::FILE_SYNCHRONOUS_IO_NONALERT,
            work,
        )?
        .ok_or_else(W::invalid_request)?
        .into_std();
        stat_windows_file(&file, volume, work)
    }
    pub(crate) fn read_dir_stats_wide<W: NativeHostWork>(
        &self,
        path: &[u16],
        work: &mut W,
    ) -> Result<(HostStatReader, W::Allocation), W::Error> {
        let directory = self.open_dir_held_wide(path, work)?;
        read_dir_held_with_volume(&directory, self.volume_serial_number()?, work)
    }
    pub(crate) fn open_file_wide<W: NativeHostWork>(
        &self,
        path: &[u16],
        synchronization: nt::NTCREATEFILE_CREATE_OPTIONS,
        work: &mut W,
    ) -> Result<cap_std::fs::File, W::Error> {
        let options = nt::FILE_NON_DIRECTORY_FILE | nt::FILE_OPEN_REPARSE_POINT | synchronization;
        if let Some(file) =
            open_windows_wide_at(&self.directory, path, fs::FILE_GENERIC_READ, options, work)?
        {
            return Ok(file);
        }
        let (parent, leaf) = wide_parent_leaf(path).map_err(|_| W::invalid_request())?;
        let parent = self.open_dir_held_wide(parent, work)?;
        open_windows_wide_at(&parent, leaf, fs::FILE_GENERIC_READ, options, work)?
            .ok_or_else(W::invalid_request)
    }
    pub(crate) fn open_file_yielding_wide<W: SourceDemandReadWork>(
        &self,
        path: &[u16],
        work: &mut W,
    ) -> Result<YieldingFile, DemandError> {
        let mut opened = |flags| {
            open_windows_wide_at(
                &self.directory,
                path,
                fs::FILE_GENERIC_READ,
                nt::FILE_NON_DIRECTORY_FILE | nt::FILE_OPEN_REPARSE_POINT | flags,
                work,
            )
        };
        let file = match opened(nt::FILE_OPEN_REQUIRING_OPLOCK) {
            Ok(file) => file,
            Err(DemandError::Io(_)) => opened(nt::NTCREATEFILE_CREATE_OPTIONS(0))?,
            Err(error) => return Err(error),
        };
        match file {
            Some(file) => YieldingFile::new(file.into_std(), work),
            None => YieldingFile::plain(
                self.open_file_wide(path, nt::NTCREATEFILE_CREATE_OPTIONS(0), work)?
                    .into_std(),
                work,
            ),
        }
    }
    pub(crate) fn read_link_wide<W: NativeHostWork>(
        &self,
        path: &[u16],
        maximum_bytes: u64,
        work: &mut W,
    ) -> Result<(Vec<u16>, W::Allocation), W::Error> {
        let (parent, leaf) = wide_parent_leaf(path).map_err(|_| W::invalid_request())?;
        read_link_held_wide(
            &self.open_dir_held_wide(parent, work)?,
            leaf,
            maximum_bytes,
            work,
        )
    }
    pub(crate) fn read_source_link<W: SourceDemandReadWork>(
        &self,
        path: &[u16],
        maximum_bytes: u64,
        work: &mut W,
    ) -> Result<acyclic_stream::OwnedBytes, DemandError> {
        let (parent, leaf) = wide_parent_leaf(path)?;
        read_source_link_held_wide(
            &self.open_dir_held_wide(parent, work)?,
            leaf,
            maximum_bytes,
            work,
        )
    }
}

pub(crate) fn reports_every_change<W: NativeHostWork<Error = DemandError>>(
    file: &impl std::os::windows::io::AsRawHandle,
    work: &mut W,
) -> Result<bool, DemandError> {
    let mut name = [0_u16; 64];
    work.started_io(READ)?;
    // SAFETY: the held handle and complete output buffer outlive the call.
    unsafe {
        fs::GetVolumeInformationByHandleW(
            win::HANDLE(file.as_raw_handle()),
            None,
            None,
            None,
            None,
            Some(&mut name),
        )
    }
    .map_err(io::Error::from)?;
    let mut length = 0;
    for unit in &name {
        work.started_io(ITEM)?;
        if *unit == 0 {
            break;
        }
        length += 1;
    }
    Ok(name[..length] == [78, 84, 70, 83])
}

pub(crate) fn open_windows_directory<W: NativeHostWork>(
    parent: &Dir,
    name: &[u16],
    work: &mut W,
) -> Result<(Dir, NativeRootIdentity), W::Error> {
    if matches!(name, [] | [46] | [46, 46]) {
        return Err(W::invalid_request());
    }
    let file = open_windows_wide_at(
        parent,
        name,
        fs::FILE_GENERIC_READ,
        nt::FILE_DIRECTORY_FILE | nt::FILE_OPEN_REPARSE_POINT | nt::FILE_SYNCHRONOUS_IO_NONALERT,
        work,
    )?
    .ok_or_else(W::invalid_request)?;
    let directory = Dir::from_std_file(file.into_std());
    let identity = directory_identity(&directory, work).map_err(|e| {
        if W::io_error_kind(&e) == Some(io::ErrorKind::InvalidInput) {
            W::invalid_request()
        } else {
            e
        }
    })?;
    Ok((directory, identity))
}
