use super::*;

#[cfg(windows)]
pub(crate) struct StagedWindowsFile {
    pub(super) parent: Dir,
    /// Deletes the staged name when closed.
    pub(super) guard: File,
}

#[cfg(windows)]
impl Drop for StagedWindowsFile {
    fn drop(&mut self) {
        use windows::Win32::Storage::FileSystem::{
            FILE_DISPOSITION_FLAG_DELETE, FILE_DISPOSITION_FLAG_IGNORE_READONLY_ATTRIBUTE,
            FILE_DISPOSITION_FLAG_ON_CLOSE, FILE_DISPOSITION_FLAG_POSIX_SEMANTICS,
        };
        let set = |flags: u32| {
            windows_set_information(
                &self.guard,
                &flags,
                windows::Wdk::Storage::FileSystem::FILE_INFORMATION_CLASS(64),
            )
        };
        if set(FILE_DISPOSITION_FLAG_ON_CLOSE.0).is_ok()
            && set(FILE_DISPOSITION_FLAG_DELETE.0
                | FILE_DISPOSITION_FLAG_POSIX_SEMANTICS.0
                | FILE_DISPOSITION_FLAG_IGNORE_READONLY_ATTRIBUTE.0)
            .is_err()
        {
            let _ = set(FILE_DISPOSITION_FLAG_ON_CLOSE.0 | FILE_DISPOSITION_FLAG_DELETE.0);
        }
    }
}

#[cfg(windows)]
impl StagedWindowsFile {
    pub(crate) fn create(parent: Dir) -> io::Result<Self> {
        use cap_std::fs::OpenOptionsExt as _;
        use windows::Win32::Storage::FileSystem::{
            DELETE, FILE_FLAG_DELETE_ON_CLOSE, FILE_READ_ATTRIBUTES, FILE_SHARE_DELETE,
            FILE_SHARE_READ, FILE_SHARE_WRITE, SYNCHRONIZE,
        };
        let name =
            std::path::PathBuf::from(format!("{STAGING_PREFIX}{}", uuid::Uuid::new_v4().simple()));
        let mut options = OpenOptions::new();
        options
            .write(true)
            .create_new(true)
            .access_mode(DELETE.0 | FILE_READ_ATTRIBUTES.0 | SYNCHRONIZE.0)
            .share_mode((FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE).0)
            .custom_flags(FILE_FLAG_DELETE_ON_CLOSE.0);
        let guard = parent.open_with(&name, &options)?.into_std();
        Ok(Self { parent, guard })
    }
    pub(crate) fn publish(&self, name: &OsStr) -> io::Result<()> {
        with_wide_path(Path::new(name), &mut ordinary_work(), |name, work| {
            set_windows_name(
                &self.guard,
                &self.parent,
                name,
                false,
                windows::Wdk::Storage::FileSystem::FileLinkInformation,
                work,
            )
        })
        .map_err(ordinary_error)
    }
}

/// Whether a file of `length` bytes can be block cloned at all.
#[cfg(windows)]
pub(super) fn clonable_windows_length(length: u64) -> bool {
    length >= 4 * 1024 && i64::try_from(length).is_ok()
}

#[cfg(windows)]
pub(super) fn windows_leaf_name<W: NativeHostWork>(
    path: &[u16],
    work: &mut W,
) -> Result<windows::Win32::Foundation::UNICODE_STRING, W::Error> {
    work.check()?;
    if path.is_empty() || path == [46] || path == [46, 46] {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid native leaf").into());
    }
    for unit in path {
        let delta = ITEM;
        work.admit(delta)?;
        let invalid = matches!(*unit, 0 | 47 | 58 | 92);
        work.record(delta)?;
        if invalid {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid native leaf").into());
        }
    }
    let bytes = path
        .len()
        .checked_mul(2)
        .ok_or(crate::WorkError::Overflow)?;
    let length = u16::try_from(bytes).map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidInput, "native leaf exceeds NT length")
    })?;
    Ok(windows::Win32::Foundation::UNICODE_STRING {
        Length: length,
        MaximumLength: length,
        Buffer: windows::core::PWSTR(path.as_ptr().cast_mut()),
    })
}

#[cfg(windows)]
pub(crate) fn create_windows_wide_at<W: NativeHostWork>(
    directory: &Dir,
    path: &[u16],
    directory_file: bool,
    overlapped: bool,
    open_if: bool,
    delete_on_close: bool,
    work: &mut W,
) -> Result<File, W::Error> {
    use windows::Wdk::Storage::FileSystem::{
        FILE_CREATE, FILE_DELETE_ON_CLOSE, FILE_DIRECTORY_FILE, FILE_NON_DIRECTORY_FILE,
        FILE_OPEN_IF, FILE_OPEN_REPARSE_POINT, FILE_SYNCHRONOUS_IO_NONALERT,
        NTCREATEFILE_CREATE_OPTIONS,
    };
    use windows::Win32::Foundation::{OBJ_CASE_INSENSITIVE, OBJ_DONT_REPARSE};
    use windows::Win32::Storage::FileSystem::{DELETE, FILE_GENERIC_READ, FILE_GENERIC_WRITE};
    let name = windows_leaf_name(path, work)?;
    let options = (if directory_file {
        FILE_DIRECTORY_FILE
    } else {
        FILE_NON_DIRECTORY_FILE
    }) | FILE_OPEN_REPARSE_POINT
        | if overlapped {
            NTCREATEFILE_CREATE_OPTIONS(0)
        } else {
            FILE_SYNCHRONOUS_IO_NONALERT
        }
        | if delete_on_close {
            FILE_DELETE_ON_CLOSE
        } else {
            NTCREATEFILE_CREATE_OPTIONS(0)
        };
    let access = FILE_GENERIC_WRITE
        | FILE_GENERIC_READ
        | if delete_on_close {
            DELETE
        } else {
            Default::default()
        };
    work.attempt_io(WRITE, || {
        open_windows_name(
            directory,
            &name,
            access,
            if open_if { FILE_OPEN_IF } else { FILE_CREATE },
            options,
            OBJ_CASE_INSENSITIVE | OBJ_DONT_REPARSE,
        )
        .map_err(status_error)
    })
}

#[cfg(windows)]
pub(crate) fn create_windows_directory<W: NativeHostWork>(
    directory: &Dir,
    name: &[u16],
    work: &mut W,
) -> Result<File, W::Error> {
    let file = create_windows_wide_at(directory, name, true, false, true, false, work)?;
    let information = windows_stat_information(&file, work)?;
    if information.FileAttributes
        & windows::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT.0
        != 0
    {
        return Err(W::invalid_request());
    }
    Ok(file)
}

#[cfg(windows)]
pub(super) fn windows_set_information<T>(
    file: &File,
    information: &T,
    class: windows::Wdk::Storage::FileSystem::FILE_INFORMATION_CLASS,
) -> io::Result<()> {
    use std::os::windows::io::AsRawHandle as _;
    let mut block = windows::Win32::System::IO::IO_STATUS_BLOCK::default();
    // SAFETY: correctly sized initialized information and held handle outlive the call.
    let status = unsafe {
        windows::Wdk::Storage::FileSystem::NtSetInformationFile(
            windows::Win32::Foundation::HANDLE(file.as_raw_handle()),
            &raw mut block,
            std::ptr::from_ref(information).cast(),
            std::mem::size_of::<T>() as u32,
            class,
        )
    };
    if status.is_err() {
        return Err(status_error(status));
    }
    Ok(())
}

#[cfg(windows)]
pub(crate) fn keep_windows_created_file<W: NativeHostWork>(
    file: &File,
    work: &mut W,
) -> Result<(), W::Error> {
    work.attempt_io(WRITE, || {
        windows_set_information(
            file,
            &8_u32,
            windows::Wdk::Storage::FileSystem::FILE_INFORMATION_CLASS(64),
        )
    })
}

#[cfg(windows)]
pub(crate) fn open_windows_leaf_handle<W: NativeHostWork>(
    parent: &Dir,
    name: &[u16],
    access: windows::Win32::Storage::FileSystem::FILE_ACCESS_RIGHTS,
    directory: Option<bool>,
    work: &mut W,
) -> Result<File, W::Error> {
    use windows::Wdk::Storage::FileSystem::{
        FILE_DIRECTORY_FILE, FILE_NON_DIRECTORY_FILE, FILE_OPEN, FILE_OPEN_REPARSE_POINT,
        FILE_SYNCHRONOUS_IO_NONALERT, NTCREATEFILE_CREATE_OPTIONS,
    };
    let name = windows_leaf_name(name, work)?;
    let options = FILE_OPEN_REPARSE_POINT
        | FILE_SYNCHRONOUS_IO_NONALERT
        | match directory {
            Some(true) => FILE_DIRECTORY_FILE,
            Some(false) => FILE_NON_DIRECTORY_FILE,
            None => NTCREATEFILE_CREATE_OPTIONS(0),
        };
    work.attempt_io(READ, || {
        open_windows_name(
            parent,
            &name,
            access,
            FILE_OPEN,
            options,
            windows::Win32::Foundation::OBJ_CASE_INSENSITIVE,
        )
        .map_err(status_error)
    })
}

#[cfg(windows)]
pub(crate) fn create_windows_symbolic_link<W: NativeHostWork>(
    parent: &Dir,
    name: &[u16],
    target: &[u16],
    directory: bool,
    created: &mut Option<File>,
    on_created: impl FnOnce(),
    on_reparse_set: impl FnOnce(),
    work: &mut W,
) -> Result<(), W::Error> {
    if created.is_some() {
        return Err(W::invalid_request());
    }
    with_windows_symlink_packet(target, work, |buffer, work| {
        *created = Some(create_windows_wide_at(
            parent, name, directory, false, false, true, work,
        )?);
        on_created();
        let file = created
            .as_ref()
            .expect("FILE_CREATE capability installed before reparse mutation");
        set_windows_symlink_packet(file, buffer, work)?;
        on_reparse_set();
        keep_windows_created_file(file, work)
    })
}

#[cfg(windows)]
pub(super) fn with_windows_symlink_packet<W: NativeHostWork, T>(
    target: &[u16],
    work: &mut W,
    operation: impl FnOnce(&[u8], &mut W) -> Result<T, W::Error>,
) -> Result<T, W::Error> {
    use windows::Win32::Storage::FileSystem::MAXIMUM_REPARSE_DATA_BUFFER_SIZE;
    use windows::Win32::System::SystemServices::IO_REPARSE_TAG_SYMLINK;
    if target.is_empty() {
        return Err(W::invalid_request());
    }
    for unit in target {
        let examined = ITEM;
        work.admit(examined)?;
        let invalid = *unit == 0;
        work.record(examined)?;
        if invalid {
            return Err(W::invalid_request());
        }
    }
    let slash = u16::from(b'\\');
    let drive = target.len() >= 3 && target[1] == u16::from(b':') && target[2] == slash;
    let unc = target.starts_with(&[slash, slash])
        && !target.starts_with(&[slash, slash, u16::from(b'?'), slash]);
    let extended = target.starts_with(&[slash, slash, u16::from(b'?'), slash]);
    let nt = target.starts_with(&[slash, u16::from(b'?'), u16::from(b'?'), slash]);
    if !drive
        && !unc
        && !extended
        && !nt
        && (target[0] == slash || (target.len() >= 2 && target[1] == u16::from(b':')))
    {
        return Err(W::invalid_request());
    }
    let prefix: &[u16] = if unc {
        &[92, 63, 63, 92, 85, 78, 67, 92]
    } else if drive || extended {
        &[92, 63, 63, 92]
    } else {
        &[]
    };
    let substituted = if unc {
        &target[2..]
    } else if extended {
        &target[4..]
    } else {
        target
    };
    let substitute_units = prefix
        .len()
        .checked_add(substituted.len())
        .ok_or(crate::WorkError::Overflow)?;
    let substitute_bytes = substitute_units
        .checked_mul(2)
        .ok_or(crate::WorkError::Overflow)?;
    let print_bytes = target
        .len()
        .checked_mul(2)
        .ok_or(crate::WorkError::Overflow)?;
    let size = 20_usize
        .checked_add(substitute_bytes)
        .and_then(|size| size.checked_add(print_bytes))
        .and_then(|size| size.checked_add(4))
        .ok_or(crate::WorkError::Overflow)?;
    if size > MAXIMUM_REPARSE_DATA_BUFFER_SIZE as usize {
        return Err(W::invalid_request());
    }
    let encoded = WorkCounters {
        bytes_encoded: size as u64,
        ..WorkCounters::UNCHARGED
    };
    work.admit(encoded)?;
    let mut storage =
        std::mem::MaybeUninit::<[u8; MAXIMUM_REPARSE_DATA_BUFFER_SIZE as usize]>::uninit();
    let pointer = storage.as_mut_ptr().cast::<u8>();
    let mut header = [0_u8; 20];
    header[..4].copy_from_slice(&IO_REPARSE_TAG_SYMLINK.to_le_bytes());
    header[4..6].copy_from_slice(&((size - 8) as u16).to_le_bytes());
    header[10..12].copy_from_slice(&(substitute_bytes as u16).to_le_bytes());
    header[12..14].copy_from_slice(&((substitute_bytes + 2) as u16).to_le_bytes());
    header[14..16].copy_from_slice(&(print_bytes as u16).to_le_bytes());
    header[16..20].copy_from_slice(&(u32::from(!(drive || unc || extended || nt))).to_le_bytes());
    // SAFETY: the complete header fits the fixed ABI storage; the untouched suffix remains MaybeUninit until each following UTF-16 unit is written.
    unsafe {
        pointer.copy_from_nonoverlapping(header.as_ptr(), header.len());
    }
    let mut offset = 20;
    for unit in prefix
        .iter()
        .chain(substituted)
        .copied()
        .chain(std::iter::once(0))
        .chain(target.iter().copied())
        .chain(std::iter::once(0))
    {
        // SAFETY: checked exact size includes both terminated target names.
        unsafe {
            pointer
                .add(offset)
                .cast::<[u8; 2]>()
                .write(unit.to_le_bytes());
        }
        offset += 2;
    }
    // SAFETY: every byte in this exact prefix has now been initialized.
    let buffer = unsafe { std::slice::from_raw_parts(pointer, size) };
    work.record(encoded)?;
    operation(buffer, work)
}

#[cfg(windows)]
pub(super) fn set_windows_symlink_packet<W: NativeHostWork>(
    file: &File,
    buffer: &[u8],
    work: &mut W,
) -> Result<(), W::Error> {
    use std::os::windows::io::AsRawHandle as _;
    work.attempt_io(WRITE, || {
        // SAFETY: the real creation handle and fully initialized exact ABI packet survive this same synchronous mutation for both work shapes.
        unsafe {
            windows::Win32::System::IO::DeviceIoControl(
                windows::Win32::Foundation::HANDLE(file.as_raw_handle()),
                windows::Win32::System::Ioctl::FSCTL_SET_REPARSE_POINT,
                Some(buffer.as_ptr().cast()),
                buffer.len() as u32,
                None,
                0,
                None,
                None,
            )
        }
        .map_err(Into::into)
    })
}

#[cfg(windows)]
pub(crate) fn open_windows_leaf_with_options<W: NativeHostWork>(
    parent: &Dir,
    name: &[u16],
    options: &NativeOpenOptions,
    work: &mut W,
) -> Result<File, W::Error> {
    use windows::Win32::Storage::FileSystem::{FILE_ATTRIBUTE_REPARSE_POINT, FILE_GENERIC_READ};
    let file = match options {
        NativeOpenOptions::Read => {
            open_windows_leaf_handle(parent, name, FILE_GENERIC_READ, Some(false), work)?
        }
        NativeOpenOptions::ReadWrite => {
            use windows::Win32::Storage::FileSystem::FILE_GENERIC_WRITE;
            open_windows_leaf_handle(
                parent,
                name,
                FILE_GENERIC_READ | FILE_GENERIC_WRITE,
                Some(false),
                work,
            )?
        }
        NativeOpenOptions::CreateNew => {
            create_windows_wide_at(parent, name, false, false, false, false, work)?
        }
        NativeOpenOptions::ReadWriteCreateNew => {
            create_windows_wide_at(parent, name, false, false, false, false, work)?
        }
        NativeOpenOptions::ReadWriteCreate => {
            create_windows_wide_at(parent, name, false, false, true, false, work)?
        }
    };
    let information = windows_stat_information(&file, work)?;
    if information.FileAttributes & FILE_ATTRIBUTE_REPARSE_POINT.0 != 0 {
        return Err(W::invalid_request());
    }
    Ok(file)
}
#[cfg(windows)]
pub(crate) fn remove_windows_leaf<W: NativeHostWork>(
    parent: &Dir,
    name: &[u16],
    directory: bool,
    expected: Option<crate::NativeRootIdentity>,
    work: &mut W,
) -> Result<(), W::Error> {
    use std::os::windows::io::AsRawHandle as _;
    use windows::Wdk::Storage::FileSystem::{
        FILE_DISPOSITION_INFORMATION, FileDispositionInformation,
    };
    use windows::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, DELETE, FILE_READ_ATTRIBUTES, GetFileInformationByHandle,
        SYNCHRONIZE,
    };
    let file = open_windows_leaf_handle(
        parent,
        name,
        DELETE | FILE_READ_ATTRIBUTES | SYNCHRONIZE,
        Some(directory),
        work,
    )?;
    if let Some(expected) = expected {
        let observed = work.attempt_io(READ, || {
            let mut information = BY_HANDLE_FILE_INFORMATION::default();
            // SAFETY: this is the same held DELETE handle that will receive the disposition.
            unsafe {
                GetFileInformationByHandle(
                    windows::Win32::Foundation::HANDLE(file.as_raw_handle()),
                    &raw mut information,
                )
                .map_err(io::Error::from)?;
            }
            Ok(crate::NativeRootIdentity {
                device: u64::from(information.dwVolumeSerialNumber),
                object: u64::from(information.nFileIndexHigh) << 32
                    | u64::from(information.nFileIndexLow),
            })
        })?;
        if observed != expected {
            return Err(io::Error::other("held deletion target identity changed").into());
        }
    }
    work.attempt_io(WRITE, || {
        windows_set_information(
            &file,
            &FILE_DISPOSITION_INFORMATION { DeleteFile: true },
            FileDispositionInformation,
        )
    })
}

#[cfg(windows)]
pub(crate) fn rename_windows_leaf<W: crate::demand::SourceDemandReadWork>(
    from: &Dir,
    name: &[u16],
    to: &Dir,
    to_name: &[u16],
    work: &mut W,
) -> Result<(), DemandError> {
    use windows::Win32::Storage::FileSystem::{DELETE, FILE_READ_ATTRIBUTES, SYNCHRONIZE};
    let file = open_windows_leaf_handle(
        from,
        name,
        DELETE | FILE_READ_ATTRIBUTES | SYNCHRONIZE,
        None,
        work,
    )?;
    for unit in to_name {
        work.charge(ITEM)?;
        if *unit == 92 {
            return Err(DemandError::InvalidRequest);
        }
    }
    if to_name == [46] || to_name == [46, 46] {
        return Err(DemandError::InvalidRequest);
    }
    set_windows_name(
        &file,
        to,
        to_name,
        true,
        windows::Wdk::Storage::FileSystem::FileRenameInformation,
        work,
    )
}

#[cfg(windows)]
impl HostRoot {
    pub(crate) fn create_overlapped_file_wide<W: crate::demand::SourceDemandReadWork>(
        &self,
        path: &[u16],
        work: &mut W,
    ) -> Result<File, DemandError> {
        self.create_windows_file_wide(path, true, false, work)
    }
    pub(super) fn create_windows_file_wide<W: crate::demand::SourceDemandReadWork>(
        &self,
        path: &[u16],
        overlapped: bool,
        temporary: bool,
        work: &mut W,
    ) -> Result<File, DemandError> {
        let (parent, leaf) = wide_parent_leaf(path)?;
        let parent = self
            .open_dir_held_wide(parent, work)
            .map(|(directory, _)| directory)?;
        create_windows_wide_at(&parent, leaf, false, overlapped, false, temporary, work)
    }
    pub(crate) fn create_dir_wide<W: crate::demand::SourceDemandReadWork>(
        &self,
        path: &[u16],
        work: &mut W,
    ) -> Result<(), DemandError> {
        let (parent, leaf) = wide_parent_leaf(path)?;
        let parent = self
            .open_dir_held_wide(parent, work)
            .map(|(directory, _)| directory)?;
        create_windows_wide_at(&parent, leaf, true, false, false, false, work)?;
        Ok(())
    }
    pub(crate) fn create_dir_all_held_wide<W: crate::demand::SourceDemandReadWork>(
        &self,
        path: &[u16],
        work: &mut W,
    ) -> Result<HostRoot, DemandError> {
        let mut current = clone_directory(&self.directory, work)?;
        if path.is_empty() {
            return Ok(Self {
                directory: current,
                identity: self.identity,
            });
        }
        for component in path.split(|unit| *unit == 92) {
            work.charge(ITEM)?;
            if component.is_empty() || component == [46] || component == [46, 46] {
                return Err(DemandError::InvalidRequest);
            }
            let file = create_windows_directory(&current, component, work)?;
            current = Dir::from_std_file(file);
        }
        let identity = directory_identity(&current, work)?;
        Ok(Self {
            directory: current,
            identity: identity,
        })
    }
    pub(crate) fn hard_link_wide<W: crate::demand::SourceDemandReadWork>(
        &self,
        source: &[u16],
        destination: &[u16],
        work: &mut W,
    ) -> Result<(), DemandError> {
        use windows::Wdk::Storage::FileSystem::{
            FILE_OPEN_REPARSE_POINT, FILE_SYNCHRONOUS_IO_NONALERT,
        };
        use windows::Win32::Storage::FileSystem::{DELETE, FILE_READ_ATTRIBUTES, SYNCHRONIZE};
        let (source_parent, source_leaf) = wide_parent_leaf(source)?;
        let source_parent = self
            .open_dir_held_wide(source_parent, work)
            .map(|(directory, _)| directory)?;
        let source = open_windows_wide_at(
            &source_parent,
            source_leaf,
            DELETE | FILE_READ_ATTRIBUTES | SYNCHRONIZE,
            FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT,
            work,
            || DemandError::InvalidRequest,
        )?
        .ok_or(DemandError::InvalidRequest)?
        .into_std();
        let (parent, leaf) = wide_parent_leaf(destination)?;
        let parent = self
            .open_dir_held_wide(parent, work)
            .map(|(directory, _)| directory)?;
        set_windows_name(
            &source,
            &parent,
            leaf,
            false,
            windows::Wdk::Storage::FileSystem::FileLinkInformation,
            work,
        )
    }
    pub(crate) fn clone_file_from_wide<W: crate::demand::SourceDemandReadWork>(
        &self,
        source_root: &HostRoot,
        source: &[u16],
        destination: &[u16],
        work: &mut W,
    ) -> Result<bool, DemandError> {
        let source = source_root
            .open_file_wide(
                source,
                windows::Wdk::Storage::FileSystem::FILE_SYNCHRONOUS_IO_NONALERT,
                work,
            )?
            .into_std();
        let stat = source_root.stat_file(&source, work)?;
        if !stat.file_type().is_file()
            || stat.file_attributes()
                & windows::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT.0
                != 0
        {
            return Err(DemandError::InvalidRequest);
        }
        let length = stat.len();
        if !clonable_windows_length(length) {
            return Ok(false);
        }
        let target = self.create_windows_file_wide(destination, false, true, work)?;
        let cloned = clone_windows_file(&source, &target, length, work);
        if matches!(cloned, Ok(true)) {
            keep_windows_created_file(&target, work)?;
        }
        drop(target);
        cloned
    }
    pub(crate) fn symlink_file_wide<W: crate::demand::SourceDemandReadWork>(
        &self,
        target: &[u16],
        destination: &[u16],
        work: &mut W,
    ) -> Result<(), DemandError> {
        let file = self.create_windows_file_wide(destination, false, true, work)?;
        with_windows_symlink_packet(target, work, |buffer, work| {
            set_windows_symlink_packet(&file, buffer, work)
        })?;
        keep_windows_created_file(&file, work)
    }
}

#[cfg(windows)]
pub(super) fn set_windows_name<W: NativeHostWork>(
    file: &File,
    parent: &Dir,
    name: &[u16],
    replace: bool,
    class: windows::Wdk::Storage::FileSystem::FILE_INFORMATION_CLASS,
    work: &mut W,
) -> Result<(), W::Error> {
    use windows::Wdk::Storage::FileSystem::FILE_RENAME_INFORMATION;
    windows_wide_name(name, work, W::invalid_request)?.ok_or_else(W::invalid_request)?;
    let copied = WorkCounters {
        bytes_copied: (name.len() as u64)
            .checked_mul(2)
            .ok_or(crate::WorkError::Overflow)?,
        ..WorkCounters::UNCHARGED
    };
    work.admit(copied)?;
    let (storage, allocation, length) =
        windows_name_packet(parent, name.iter().copied(), name.len(), replace, work)?;
    let result = (|| {
        work.record(copied)?;
        work.attempt_io(WRITE, || {
            windows_name_packet_once(
                file,
                &storage,
                length.max(std::mem::size_of::<FILE_RENAME_INFORMATION>() as u32),
                class,
            )
        })
    })();
    drop(storage);
    work.release(allocation)?;
    result
}
