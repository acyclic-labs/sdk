use super::*;

/// Compares attributes while allowing ProjFS hydration marks to change.
#[cfg(windows)]
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

/// Translates native status, treating a pending deletion as absent.
#[cfg(windows)]
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

#[cfg(windows)]
pub(super) fn wide_units<W: NativeHostWork>(name: &OsStr, work: &mut W) -> Result<usize, W::Error> {
    use std::os::windows::ffi::OsStrExt as _;
    let mut units = 0usize;
    for _ in name.encode_wide() {
        work.started_io(ITEM)?;
        units = units.checked_add(1).ok_or(crate::WorkError::Overflow)?;
    }
    Ok(units)
}

#[cfg(windows)]
pub(crate) fn with_wide_path<W: NativeHostWork, T>(
    path: &Path,
    work: &mut W,
    operation: impl FnOnce(&[u16], &mut W) -> Result<T, W::Error>,
) -> Result<T, W::Error> {
    use std::os::windows::ffi::OsStrExt as _;
    work.check()?;
    let units = wide_units(path.as_os_str(), work)?;
    let bytes = units.checked_mul(2).ok_or(crate::WorkError::Overflow)? as u64;
    let encoded = WorkCounters {
        bytes_encoded: bytes,
        ..WorkCounters::UNCHARGED
    };
    work.admit(encoded)?;
    let (mut name, allocation) = buffer(units, work)?;
    let result = (|| {
        name.extend(path.as_os_str().encode_wide().map(|unit| {
            if unit == u16::from(b'/') {
                u16::from(b'\\')
            } else {
                unit
            }
        }));
        work.record(encoded)?;
        operation(&name, work)
    })();
    drop(name);
    work.release(allocation)?;
    result
}

#[cfg(windows)]
pub(super) fn windows_wide_name<W: NativeHostWork, T>(
    path: &[u16],
    work: &mut W,
    invalid_length: impl FnOnce() -> W::Error,
    operation: impl FnOnce(
        &mut windows::Win32::Foundation::UNICODE_STRING,
        &mut W,
    ) -> Result<T, W::Error>,
) -> Result<Option<T>, W::Error> {
    work.check()?;
    if path.is_empty() {
        return Ok(None);
    }
    if path != [u16::from(b'.')] {
        let mut begin = 0usize;
        let item = ITEM;
        work.admit(item)?;
        work.record(item)?;
        for (index, unit) in path.iter().enumerate() {
            work.admit(item)?;
            work.record(item)?;
            if matches!(*unit, 0 | 47 | 58) {
                return Ok(None);
            }
            if *unit == 92 {
                let component = &path[begin..index];
                if component.is_empty() || component == [46] || component == [46, 46] {
                    return Ok(None);
                }
                begin = index + 1;
                work.admit(item)?;
                work.record(item)?;
            }
        }
        let component = &path[begin..];
        if component.is_empty() || component == [46] || component == [46, 46] {
            return Ok(None);
        }
    }
    // NT opens the held directory with an empty name; it does not resolve ".".
    let path = if path == [46] { &[][..] } else { path };
    let length = u16::try_from(
        path.len()
            .checked_mul(2)
            .ok_or(crate::WorkError::Overflow)?,
    )
    .map_err(|_| invalid_length())?;
    let mut string = windows::Win32::Foundation::UNICODE_STRING {
        Length: length,
        MaximumLength: length,
        Buffer: windows::core::PWSTR(path.as_ptr().cast_mut()),
    };
    operation(&mut string, work).map(Some)
}

#[cfg(windows)]
pub(super) fn stat_windows_wide_at<W: NativeHostWork>(
    directory: &Dir,
    path: &[u16],
    volume: u32,
    work: &mut W,
) -> Result<Option<HostStat>, W::Error> {
    Ok(
        windows_wide_name(path, work, W::invalid_request, |name, work| {
            stat_windows_name(directory, name, volume, work)
        })?
        .flatten(),
    )
}

#[cfg(windows)]
pub(super) fn stat_windows_name<W: NativeHostWork>(
    directory: &Dir,
    name: &windows::Win32::Foundation::UNICODE_STRING,
    volume: u32,
    work: &mut W,
) -> Result<Option<HostStat>, W::Error> {
    use std::os::windows::io::{AsHandle as _, AsRawHandle as _};
    use windows::Wdk::Foundation::OBJECT_ATTRIBUTES;
    use windows::Wdk::Storage::FileSystem::{
        FILE_STAT_INFORMATION, FileStatInformation, NtQueryInformationByName,
    };
    use windows::Win32::Foundation::{HANDLE, NTSTATUS, OBJ_CASE_INSENSITIVE, OBJ_DONT_REPARSE};
    use windows::Win32::System::IO::IO_STATUS_BLOCK;
    const REPARSE: NTSTATUS = NTSTATUS(0xC000_050B_u32.cast_signed());
    let attributes = OBJECT_ATTRIBUTES {
        Length: std::mem::size_of::<OBJECT_ATTRIBUTES>() as u32,
        RootDirectory: HANDLE(directory.as_handle().as_raw_handle()),
        ObjectName: &raw const *name,
        Attributes: OBJ_CASE_INSENSITIVE | OBJ_DONT_REPARSE,
        ..Default::default()
    };
    let mut block = IO_STATUS_BLOCK::default();
    let mut information = FILE_STAT_INFORMATION::default();
    native_read(work)?;
    // SAFETY: held capability and fixed-size query storage outlive the call.
    let status = unsafe {
        NtQueryInformationByName(
            &raw const attributes,
            &raw mut block,
            (&raw mut information).cast(),
            std::mem::size_of::<FILE_STAT_INFORMATION>() as u32,
            FileStatInformation,
        )
    };
    if status == REPARSE {
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

#[cfg(windows)]
pub(super) fn open_windows_wide_at<W: NativeHostWork>(
    directory: &Dir,
    path: &[u16],
    access: windows::Win32::Storage::FileSystem::FILE_ACCESS_RIGHTS,
    options: windows::Wdk::Storage::FileSystem::NTCREATEFILE_CREATE_OPTIONS,
    work: &mut W,
    invalid_length: impl FnOnce() -> W::Error,
) -> Result<Option<cap_std::fs::File>, W::Error> {
    use windows::Win32::Foundation::{NTSTATUS, OBJ_CASE_INSENSITIVE, OBJ_DONT_REPARSE};
    const REPARSE: NTSTATUS = NTSTATUS(0xC000_050B_u32.cast_signed());
    Ok(windows_wide_name(path, work, invalid_length, |name, work| {
        native_read(work)?;
        match open_windows_name(
            directory,
            name,
            access,
            windows::Wdk::Storage::FileSystem::FILE_OPEN,
            options,
            OBJ_CASE_INSENSITIVE | OBJ_DONT_REPARSE,
        ) {
            Ok(file) => Ok(Some(cap_std::fs::File::from_std(file))),
            Err(REPARSE) => Ok(None),
            Err(status) => Err(status_error(status).into()),
        }
    })?
    .flatten())
}

#[cfg(windows)]
pub(super) fn open_windows_name(
    directory: &Dir,
    name: &windows::Win32::Foundation::UNICODE_STRING,
    access: windows::Win32::Storage::FileSystem::FILE_ACCESS_RIGHTS,
    disposition: windows::Wdk::Storage::FileSystem::NTCREATEFILE_CREATE_DISPOSITION,
    options: windows::Wdk::Storage::FileSystem::NTCREATEFILE_CREATE_OPTIONS,
    flags: windows::Win32::Foundation::OBJECT_ATTRIBUTE_FLAGS,
) -> Result<File, windows::Win32::Foundation::NTSTATUS> {
    use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _};
    use windows::Wdk::Foundation::OBJECT_ATTRIBUTES;
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::Storage::FileSystem::{
        FILE_ATTRIBUTE_NORMAL, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
    };
    let attributes = OBJECT_ATTRIBUTES {
        Length: std::mem::size_of::<OBJECT_ATTRIBUTES>() as u32,
        RootDirectory: HANDLE(directory.as_raw_handle()),
        ObjectName: std::ptr::from_ref(name),
        Attributes: flags,
        ..Default::default()
    };
    let mut block = windows::Win32::System::IO::IO_STATUS_BLOCK::default();
    let mut handle = HANDLE::default();
    // SAFETY: the held directory, validated name, and complete output storage outlive this call.
    let status = unsafe {
        windows::Wdk::Storage::FileSystem::NtCreateFile(
            &raw mut handle,
            access,
            &raw const attributes,
            &raw mut block,
            None,
            FILE_ATTRIBUTE_NORMAL,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            disposition,
            options,
            None,
            0,
        )
    };
    if status.is_err() {
        return Err(status);
    }
    // SAFETY: successful creation/open transferred ownership of a fresh handle.
    Ok(unsafe { File::from_raw_handle(handle.0) })
}

#[cfg(windows)]
pub(super) fn windows_stat_information<W: NativeHostWork>(
    file: &impl std::os::windows::io::AsHandle,
    work: &mut W,
) -> Result<windows::Wdk::Storage::FileSystem::FILE_STAT_INFORMATION, W::Error> {
    use std::os::windows::io::AsRawHandle as _;
    use windows::Wdk::Storage::FileSystem::{
        FILE_STAT_INFORMATION, FileStatInformation, NtQueryInformationFile,
    };
    work.attempt_io(READ, || {
        let mut block = windows::Win32::System::IO::IO_STATUS_BLOCK::default();
        let mut information = FILE_STAT_INFORMATION::default();
        // SAFETY: the held handle and fixed-size output survive this synchronous query.
        let status = unsafe {
            NtQueryInformationFile(
                windows::Win32::Foundation::HANDLE(file.as_handle().as_raw_handle()),
                &raw mut block,
                (&raw mut information).cast(),
                std::mem::size_of::<FILE_STAT_INFORMATION>() as u32,
                FileStatInformation,
            )
        };
        if status.is_err() {
            return Err(status_error(status));
        }
        Ok(information)
    })
}

#[cfg(windows)]
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

#[cfg(windows)]
pub(crate) fn stat_windows_leaf<W: NativeHostWork>(
    directory: &Dir,
    path: &[u16],
    work: &mut W,
) -> Result<HostStat, W::Error> {
    use windows::Win32::Storage::FileSystem::{FILE_READ_ATTRIBUTES, SYNCHRONIZE};
    let volume = u32::try_from(directory_identity(directory, work)?.device)
        .map_err(|_| io::Error::other("native volume serial exceeds u32"))?;
    let name = windows_leaf_name(path, work)?;
    if let Some(stat) = stat_windows_name(directory, &name, volume, work)? {
        return Ok(stat);
    }
    let file = open_windows_leaf_handle(
        directory,
        path,
        FILE_READ_ATTRIBUTES | SYNCHRONIZE,
        None,
        work,
    )?;
    stat_windows_file(&file, volume, work)
}

#[cfg(windows)]
pub(super) fn wide_parent_leaf(path: &[u16]) -> Result<(&[u16], &[u16]), DemandError> {
    let (parent, leaf) = match path.iter().rposition(|unit| *unit == u16::from(b'\\')) {
        Some(index) => (&path[..index], &path[index + 1..]),
        None => (&[][..], path),
    };
    if leaf.is_empty() {
        return Err(DemandError::InvalidRequest);
    }
    Ok((parent, leaf))
}

#[cfg(windows)]
impl HostRoot {
    pub(crate) fn open_dir_held_wide<W: NativeHostWork>(
        &self,
        path: &[u16],
        work: &mut W,
    ) -> Result<(Dir, NativeRootIdentity), W::Error> {
        let mut current = clone_directory(&self.directory, work)?;
        let mut identity = self.identity;
        if path.is_empty() {
            return Ok((current, identity));
        }
        for component in path.split(|unit| *unit == u16::from(b'\\')) {
            let item = ITEM;
            work.admit(item)?;
            work.record(item)?;
            let (next, next_identity) = self.open_dir_component_wide(&current, component, work)?;
            identity = next_identity;
            current = next;
        }
        Ok((current, identity))
    }
    pub(crate) fn open_dir_component_wide<W: NativeHostWork>(
        &self,
        parent: &Dir,
        name: &[u16],
        work: &mut W,
    ) -> Result<(Dir, NativeRootIdentity), W::Error> {
        use windows::Wdk::Storage::FileSystem::{
            FILE_DIRECTORY_FILE, FILE_OPEN_REPARSE_POINT, FILE_SYNCHRONOUS_IO_NONALERT,
        };
        use windows::Win32::Storage::FileSystem::{
            FILE_ATTRIBUTE_REPARSE_POINT, FILE_GENERIC_READ,
        };
        if name.is_empty() || name == [46] || name == [46, 46] {
            return Err(W::invalid_request());
        }
        let file = open_windows_wide_at(
            parent,
            name,
            FILE_GENERIC_READ,
            FILE_DIRECTORY_FILE | FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT,
            work,
            W::invalid_request,
        )?
        .ok_or_else(W::invalid_request)?
        .into_std();
        let stat = self.stat_file(&file, work)?;
        if stat.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT.0 != 0 {
            return Err(W::invalid_request());
        }
        let identity = NativeRootIdentity {
            device: u64::from(stat.volume_serial_number()),
            object: stat.file_index(),
        };
        Ok((Dir::from_std_file(file), identity))
    }
    pub(crate) fn stat_wide<W: NativeHostWork>(
        &self,
        path: &[u16],
        work: &mut W,
    ) -> Result<HostStat, W::Error> {
        use windows::Wdk::Storage::FileSystem::{
            FILE_OPEN_REPARSE_POINT, FILE_SYNCHRONOUS_IO_NONALERT,
        };
        use windows::Win32::Storage::FileSystem::{FILE_READ_ATTRIBUTES, SYNCHRONIZE};
        let volume = self.volume_serial_number()?;
        if path.is_empty() {
            return stat_windows_file(&self.directory, volume, work);
        }
        if let Some(stat) = stat_windows_wide_at(&self.directory, path, volume, work)? {
            return Ok(stat);
        }
        let (parent, leaf) = wide_parent_leaf(path).map_err(|_| W::invalid_request())?;
        let parent = self
            .open_dir_held_wide(parent, work)
            .map(|(directory, _)| directory)?;
        let file = open_windows_wide_at(
            &parent,
            leaf,
            FILE_READ_ATTRIBUTES | SYNCHRONIZE,
            FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT,
            work,
            W::invalid_request,
        )?
        .ok_or_else(W::invalid_request)?
        .into_std();
        self.stat_file(&file, work)
    }
    pub(crate) fn read_dir_stats_wide<W: NativeHostWork>(
        &self,
        path: &[u16],
        work: &mut W,
    ) -> Result<(HostStatReader, W::Allocation), W::Error> {
        let volume_serial_number = self.volume_serial_number()?;
        let directory = self
            .open_dir_held_wide(path, work)
            .map(|(directory, _)| directory)?;
        read_dir_held_with_volume(&directory, volume_serial_number, work)
    }
    pub(super) fn open_file_wide<W: NativeHostWork>(
        &self,
        path: &[u16],
        synchronization: windows::Wdk::Storage::FileSystem::NTCREATEFILE_CREATE_OPTIONS,
        work: &mut W,
    ) -> Result<cap_std::fs::File, W::Error> {
        use windows::Wdk::Storage::FileSystem::{FILE_NON_DIRECTORY_FILE, FILE_OPEN_REPARSE_POINT};
        use windows::Win32::Storage::FileSystem::FILE_GENERIC_READ;
        let options = FILE_NON_DIRECTORY_FILE | FILE_OPEN_REPARSE_POINT | synchronization;
        if let Some(file) = open_windows_wide_at(
            &self.directory,
            path,
            FILE_GENERIC_READ,
            options,
            work,
            W::invalid_request,
        )? {
            return Ok(file);
        }
        let (parent, leaf) = wide_parent_leaf(path).map_err(|_| W::invalid_request())?;
        let parent = self
            .open_dir_held_wide(parent, work)
            .map(|(directory, _)| directory)?;
        open_windows_wide_at(
            &parent,
            leaf,
            FILE_GENERIC_READ,
            options,
            work,
            W::invalid_request,
        )?
        .ok_or_else(W::invalid_request)
    }
    pub(crate) fn open_file_positional_wide<W: NativeHostWork>(
        &self,
        path: &[u16],
        work: &mut W,
    ) -> Result<cap_std::fs::File, W::Error> {
        self.open_file_wide(
            path,
            windows::Wdk::Storage::FileSystem::NTCREATEFILE_CREATE_OPTIONS(0),
            work,
        )
    }
    pub(crate) fn open_file_yielding_wide<W: crate::demand::SourceDemandReadWork>(
        &self,
        path: &[u16],
        work: &mut W,
    ) -> Result<YieldingFile, DemandError> {
        use windows::Wdk::Storage::FileSystem::{
            FILE_NON_DIRECTORY_FILE, FILE_OPEN_REPARSE_POINT, FILE_OPEN_REQUIRING_OPLOCK,
            NTCREATEFILE_CREATE_OPTIONS,
        };
        use windows::Win32::Storage::FileSystem::FILE_GENERIC_READ;
        let options = FILE_NON_DIRECTORY_FILE | FILE_OPEN_REPARSE_POINT;
        let file = match open_windows_wide_at(
            &self.directory,
            path,
            FILE_GENERIC_READ,
            options | FILE_OPEN_REQUIRING_OPLOCK,
            work,
            || DemandError::InvalidRequest,
        ) {
            Ok(file) => file,
            Err(DemandError::Io(_)) => open_windows_wide_at(
                &self.directory,
                path,
                FILE_GENERIC_READ,
                options | NTCREATEFILE_CREATE_OPTIONS(0),
                work,
                || DemandError::InvalidRequest,
            )?,
            Err(error) => return Err(error),
        };
        match file {
            Some(file) => YieldingFile::new(file.into_std(), work),
            None => YieldingFile::plain(
                self.open_file_wide(path, NTCREATEFILE_CREATE_OPTIONS(0), work)?
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
        let parent = self
            .open_dir_held_wide(parent, work)
            .map(|(directory, _)| directory)?;
        read_link_held_wide(&parent, leaf, maximum_bytes, work)
    }
    pub(crate) fn read_source_link<W: crate::demand::SourceDemandReadWork>(
        &self,
        path: &[u16],
        maximum_bytes: u64,
        work: &mut W,
    ) -> Result<acyclic_stream::OwnedBytes, DemandError> {
        let (parent, leaf) = wide_parent_leaf(path).map_err(|_| DemandError::InvalidRequest)?;
        let parent = self
            .open_dir_held_wide(parent, work)
            .map(|(directory, _)| directory)?;
        read_source_link_held_wide(&parent, leaf, maximum_bytes, work)
    }
}

#[cfg(windows)]
pub(crate) fn reports_every_change<W: NativeHostWork<Error = DemandError>>(
    file: &impl std::os::windows::io::AsRawHandle,
    work: &mut W,
) -> Result<bool, DemandError> {
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::Storage::FileSystem::GetVolumeInformationByHandleW;
    let mut name = [0_u16; 64];
    native_read(work)?;
    // SAFETY: a live handle and an output buffer that outlives the call.
    unsafe {
        GetVolumeInformationByHandleW(
            HANDLE(file.as_raw_handle()),
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
        let examined = ITEM;
        work.admit(examined)?;
        let terminator = *unit == 0;
        work.record(examined)?;
        if terminator {
            break;
        }
        length += 1;
    }
    Ok(name.get(..length) == Some(&[78, 84, 70, 83][..]))
}
