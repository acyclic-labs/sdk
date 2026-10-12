use super::*;
use windows::{
    Wdk::Storage::FileSystem as nt,
    Win32::{Foundation as win, Storage::FileSystem as fs},
};

pub(crate) struct StagedWindowsFile {
    pub(super) parent: Dir,
    pub(super) guard: File,
}
impl Drop for StagedWindowsFile {
    fn drop(&mut self) {
        let set =
            |flags| windows_set_information(&self.guard, &flags, nt::FILE_INFORMATION_CLASS(64));
        if set(fs::FILE_DISPOSITION_FLAG_ON_CLOSE.0).is_ok()
            && set(fs::FILE_DISPOSITION_FLAG_DELETE.0
                | fs::FILE_DISPOSITION_FLAG_POSIX_SEMANTICS.0
                | fs::FILE_DISPOSITION_FLAG_IGNORE_READONLY_ATTRIBUTE.0)
            .is_err()
        {
            let _ = set(fs::FILE_DISPOSITION_FLAG_ON_CLOSE.0 | fs::FILE_DISPOSITION_FLAG_DELETE.0);
        }
    }
}
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
                nt::FileLinkInformation,
                work,
            )
        })
        .map_err(ordinary_error)
    }
}

pub(super) fn clonable_windows_length(length: u64) -> bool {
    length >= 4 * 1024 && i64::try_from(length).is_ok()
}

pub(super) fn windows_leaf_name<W: NativeHostWork>(
    name: &[u16],
    work: &mut W,
) -> Result<win::UNICODE_STRING, W::Error> {
    work.check()?;
    if matches!(name, [] | [46] | [46, 46]) {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid native leaf").into());
    }
    for unit in name {
        work.admit(ITEM)?;
        let invalid = matches!(unit, 0 | 47 | 58 | 92);
        work.record(ITEM)?;
        if invalid {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid native leaf").into());
        }
    }
    let bytes = name
        .len()
        .checked_mul(2)
        .ok_or(crate::WorkError::Overflow)?;
    let length = u16::try_from(bytes).map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidInput, "native leaf exceeds NT length")
    })?;
    Ok(win::UNICODE_STRING {
        Length: length,
        MaximumLength: length,
        Buffer: windows::core::PWSTR(name.as_ptr().cast_mut()),
    })
}

pub(crate) fn create_windows_wide_at<W: NativeHostWork>(
    directory: &Dir,
    path: &[u16],
    directory_file: bool,
    overlapped: bool,
    open_if: bool,
    delete_on_close: bool,
    work: &mut W,
) -> Result<File, W::Error> {
    let name = windows_leaf_name(path, work)?;
    let options = (if directory_file {
        nt::FILE_DIRECTORY_FILE
    } else {
        nt::FILE_NON_DIRECTORY_FILE
    }) | nt::FILE_OPEN_REPARSE_POINT
        | if overlapped {
            nt::NTCREATEFILE_CREATE_OPTIONS(0)
        } else {
            nt::FILE_SYNCHRONOUS_IO_NONALERT
        }
        | if delete_on_close {
            nt::FILE_DELETE_ON_CLOSE
        } else {
            nt::NTCREATEFILE_CREATE_OPTIONS(0)
        };
    let access = fs::FILE_GENERIC_READ
        | fs::FILE_GENERIC_WRITE
        | if delete_on_close {
            fs::DELETE
        } else {
            fs::FILE_ACCESS_RIGHTS(0)
        };
    work.attempt_io(WRITE, || {
        open_windows_name(
            directory,
            &name,
            access,
            if open_if {
                nt::FILE_OPEN_IF
            } else {
                nt::FILE_CREATE
            },
            options,
            win::OBJ_CASE_INSENSITIVE | win::OBJ_DONT_REPARSE,
        )
        .map_err(status_error)
    })
}

pub(crate) fn create_windows_directory<W: NativeHostWork>(
    parent: &Dir,
    name: &[u16],
    work: &mut W,
) -> Result<File, W::Error> {
    let file = create_windows_wide_at(parent, name, true, false, true, false, work)?;
    if windows_stat_information(&file, work)?.FileAttributes & fs::FILE_ATTRIBUTE_REPARSE_POINT.0
        != 0
    {
        return Err(W::invalid_request());
    }
    Ok(file)
}

pub(super) fn windows_set_information<T: ?Sized>(
    file: &File,
    information: &T,
    class: nt::FILE_INFORMATION_CLASS,
) -> io::Result<()> {
    use std::os::windows::io::AsRawHandle as _;
    let mut block = windows::Win32::System::IO::IO_STATUS_BLOCK::default();
    // SAFETY: the held handle and initialized information outlive this call.
    let status = unsafe {
        nt::NtSetInformationFile(
            win::HANDLE(file.as_raw_handle()),
            &mut block,
            std::ptr::from_ref(information).cast(),
            std::mem::size_of_val(information) as u32,
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

pub(crate) fn open_windows_leaf_handle<W: NativeHostWork>(
    parent: &Dir,
    name: &[u16],
    access: fs::FILE_ACCESS_RIGHTS,
    directory: Option<bool>,
    work: &mut W,
) -> Result<File, W::Error> {
    let name = windows_leaf_name(name, work)?;
    let options = nt::FILE_OPEN_REPARSE_POINT
        | nt::FILE_SYNCHRONOUS_IO_NONALERT
        | match directory {
            Some(true) => nt::FILE_DIRECTORY_FILE,
            Some(false) => nt::FILE_NON_DIRECTORY_FILE,
            None => nt::NTCREATEFILE_CREATE_OPTIONS(0),
        };
    work.attempt_io(READ, || {
        open_windows_name(
            parent,
            &name,
            access,
            nt::FILE_OPEN,
            options,
            win::OBJ_CASE_INSENSITIVE,
        )
        .map_err(status_error)
    })
}

fn set_windows_symbolic_link<W: NativeHostWork>(
    file: &File,
    target: &[u16],
    work: &mut W,
) -> Result<(), W::Error> {
    use std::os::windows::io::AsRawHandle as _;
    if target.is_empty() {
        return Err(W::invalid_request());
    }
    for unit in target {
        work.admit(ITEM)?;
        let invalid = *unit == 0;
        work.record(ITEM)?;
        if invalid {
            return Err(W::invalid_request());
        }
    }
    let drive = target.len() >= 3 && target[1..3] == [58, 92];
    let extended = target.starts_with(&[92, 92, 63, 92]);
    let unc = target.starts_with(&[92, 92]) && !extended;
    let nt_name = target.starts_with(&[92, 63, 63, 92]);
    let absolute = drive || extended || unc || nt_name;
    if !absolute && (target[0] == 92 || (target.len() >= 2 && target[1] == 58)) {
        return Err(W::invalid_request());
    }
    let prefix: &[u16] = if unc {
        &[92, 63, 63, 92, 85, 78, 67, 92]
    } else if drive || extended {
        &[92, 63, 63, 92]
    } else {
        &[]
    };
    let substitute = if unc {
        &target[2..]
    } else if extended {
        &target[4..]
    } else {
        target
    };
    let substitute_bytes = prefix
        .len()
        .checked_add(substitute.len())
        .and_then(|n| n.checked_mul(2))
        .ok_or(crate::WorkError::Overflow)?;
    let print_bytes = target
        .len()
        .checked_mul(2)
        .ok_or(crate::WorkError::Overflow)?;
    let size = 24usize
        .checked_add(substitute_bytes)
        .and_then(|n| n.checked_add(print_bytes))
        .ok_or(crate::WorkError::Overflow)?;
    if size > fs::MAXIMUM_REPARSE_DATA_BUFFER_SIZE as usize {
        return Err(W::invalid_request());
    }
    let encoded = WorkCounters {
        bytes_encoded: size as u64,
        ..WorkCounters::UNCHARGED
    };
    work.admit(encoded)?;
    let mut storage =
        std::mem::MaybeUninit::<[u8; fs::MAXIMUM_REPARSE_DATA_BUFFER_SIZE as usize]>::uninit();
    let pointer = storage.as_mut_ptr().cast::<u8>();
    let mut header = [0u8; 20];
    header[..4].copy_from_slice(
        &windows::Win32::System::SystemServices::IO_REPARSE_TAG_SYMLINK.to_le_bytes(),
    );
    header[4..6].copy_from_slice(&((size - 8) as u16).to_le_bytes());
    header[10..12].copy_from_slice(&(substitute_bytes as u16).to_le_bytes());
    header[12..14].copy_from_slice(&((substitute_bytes + 2) as u16).to_le_bytes());
    header[14..16].copy_from_slice(&(print_bytes as u16).to_le_bytes());
    header[16..20].copy_from_slice(&u32::from(!absolute).to_le_bytes());
    // SAFETY: the checked packet size fits storage, and each consumed byte is initialized.
    unsafe {
        pointer.copy_from_nonoverlapping(header.as_ptr(), 20);
        for (index, unit) in prefix
            .iter()
            .chain(substitute)
            .copied()
            .chain(std::iter::once(0))
            .chain(target.iter().copied())
            .chain(std::iter::once(0))
            .enumerate()
        {
            pointer
                .add(20 + index * 2)
                .cast::<[u8; 2]>()
                .write(unit.to_le_bytes());
        }
    }
    work.record(encoded)?;
    work.attempt_io(WRITE, || {
        // SAFETY: the initialized packet and held handle outlive DeviceIoControl.
        unsafe {
            windows::Win32::System::IO::DeviceIoControl(
                win::HANDLE(file.as_raw_handle()),
                windows::Win32::System::Ioctl::FSCTL_SET_REPARSE_POINT,
                Some(pointer.cast()),
                size as u32,
                None,
                0,
                None,
                None,
            )
        }
        .map_err(Into::into)
    })
}

pub(crate) fn open_windows_leaf_with_options<W: NativeHostWork>(
    parent: &Dir,
    name: &[u16],
    options: &NativeOpenOptions,
    work: &mut W,
) -> Result<File, W::Error> {
    let file = match options {
        NativeOpenOptions::Read | NativeOpenOptions::ReadWrite => open_windows_leaf_handle(
            parent,
            name,
            fs::FILE_GENERIC_READ
                | if matches!(options, NativeOpenOptions::ReadWrite) {
                    fs::FILE_GENERIC_WRITE
                } else {
                    fs::FILE_ACCESS_RIGHTS(0)
                },
            Some(false),
            work,
        )?,
        _ => create_windows_wide_at(
            parent,
            name,
            false,
            false,
            matches!(options, NativeOpenOptions::ReadWriteCreate),
            false,
            work,
        )?,
    };
    if windows_stat_information(&file, work)?.FileAttributes & fs::FILE_ATTRIBUTE_REPARSE_POINT.0
        != 0
    {
        return Err(W::invalid_request());
    }
    Ok(file)
}

pub(crate) fn remove_windows_leaf<W: NativeHostWork>(
    parent: &Dir,
    name: &[u16],
    directory: bool,
    expected: Option<NativeRootIdentity>,
    work: &mut W,
) -> Result<(), W::Error> {
    let file = open_windows_leaf_handle(
        parent,
        name,
        fs::DELETE | fs::FILE_READ_ATTRIBUTES | fs::SYNCHRONIZE,
        Some(directory),
        work,
    )?;
    if let Some(expected) = expected {
        if file_identity_and_length(&file, work)?.0 != expected {
            return Err(io::Error::other("held deletion target identity changed").into());
        }
    }
    work.attempt_io(WRITE, || {
        windows_set_information(
            &file,
            &nt::FILE_DISPOSITION_INFORMATION { DeleteFile: true },
            nt::FileDispositionInformation,
        )
    })
}

pub(crate) fn rename_windows_leaf<W: SourceDemandReadWork>(
    from: &Dir,
    name: &[u16],
    to: &Dir,
    to_name: &[u16],
    work: &mut W,
) -> Result<(), DemandError> {
    let file = open_windows_leaf_handle(
        from,
        name,
        fs::DELETE | fs::FILE_READ_ATTRIBUTES | fs::SYNCHRONIZE,
        None,
        work,
    )?;
    for unit in to_name {
        work.started_io(ITEM)?;
        if *unit == 92 {
            return Err(DemandError::InvalidRequest);
        }
    }
    if to_name == [46] || to_name == [46, 46] {
        return Err(DemandError::InvalidRequest);
    }
    set_windows_name(&file, to, to_name, true, nt::FileRenameInformation, work)
}

impl HostRoot {
    pub(crate) fn create_windows_file_wide<W: SourceDemandReadWork>(
        &self,
        path: &[u16],
        overlapped: bool,
        temporary: bool,
        work: &mut W,
    ) -> Result<File, DemandError> {
        let (parent, leaf) = wide_parent_leaf(path)?;
        create_windows_wide_at(
            &self.open_dir_held_wide(parent, work)?,
            leaf,
            false,
            overlapped,
            false,
            temporary,
            work,
        )
    }
    pub(crate) fn create_dir_wide<W: SourceDemandReadWork>(
        &self,
        path: &[u16],
        work: &mut W,
    ) -> Result<(), DemandError> {
        let (parent, leaf) = wide_parent_leaf(path)?;
        create_windows_wide_at(
            &self.open_dir_held_wide(parent, work)?,
            leaf,
            true,
            false,
            false,
            false,
            work,
        )?;
        Ok(())
    }
    pub(crate) fn create_dir_all_held_wide<W: SourceDemandReadWork>(
        &self,
        path: &[u16],
        work: &mut W,
    ) -> Result<HostRoot, DemandError> {
        let mut directory = clone_directory(&self.directory, work)?;
        if path.is_empty() {
            return Ok(Self {
                directory,
                identity: self.identity,
            });
        }
        for name in path.split(|u| *u == 92) {
            work.started_io(ITEM)?;
            if matches!(name, [] | [46] | [46, 46]) {
                return Err(DemandError::InvalidRequest);
            }
            directory = Dir::from_std_file(create_windows_directory(&directory, name, work)?);
        }
        let identity = directory_identity(&directory, work)?;
        Ok(Self {
            directory,
            identity,
        })
    }
    pub(crate) fn hard_link_wide<W: SourceDemandReadWork>(
        &self,
        source: &[u16],
        destination: &[u16],
        work: &mut W,
    ) -> Result<(), DemandError> {
        let (parent, name) = wide_parent_leaf(source)?;
        let parent = self.open_dir_held_wide(parent, work)?;
        let file = open_windows_wide_at(
            &parent,
            name,
            fs::DELETE | fs::FILE_READ_ATTRIBUTES | fs::SYNCHRONIZE,
            nt::FILE_OPEN_REPARSE_POINT | nt::FILE_SYNCHRONOUS_IO_NONALERT,
            work,
        )?
        .ok_or(DemandError::InvalidRequest)?
        .into_std();
        let (parent, name) = wide_parent_leaf(destination)?;
        set_windows_name(
            &file,
            &self.open_dir_held_wide(parent, work)?,
            name,
            false,
            nt::FileLinkInformation,
            work,
        )
    }
    pub(crate) fn clone_file_from_wide<W: SourceDemandReadWork>(
        &self,
        source_root: &HostRoot,
        source: &[u16],
        destination: &[u16],
        work: &mut W,
    ) -> Result<bool, DemandError> {
        let source = source_root
            .open_file_wide(source, nt::FILE_SYNCHRONOUS_IO_NONALERT, work)?
            .into_std();
        let stat = source_root.stat_file(&source, work)?;
        if !stat.file_type().is_file()
            || stat.file_attributes() & fs::FILE_ATTRIBUTE_REPARSE_POINT.0 != 0
        {
            return Err(DemandError::InvalidRequest);
        }
        if !clonable_windows_length(stat.len()) {
            return Ok(false);
        }
        let target = self.create_windows_file_wide(destination, false, true, work)?;
        let result = clone_windows_file(&source, &target, stat.len(), work);
        if matches!(result, Ok(true)) {
            keep_windows_created_file(&target, work)?;
        }
        result
    }
    pub(crate) fn symlink_file_wide<W: SourceDemandReadWork>(
        &self,
        target: &[u16],
        destination: &[u16],
        work: &mut W,
    ) -> Result<(), DemandError> {
        let file = self.create_windows_file_wide(destination, false, true, work)?;
        set_windows_symbolic_link(&file, target, work)?;
        keep_windows_created_file(&file, work)
    }
}

pub(super) fn set_windows_name<W: NativeHostWork>(
    file: &File,
    parent: &Dir,
    name: &[u16],
    replace: bool,
    class: nt::FILE_INFORMATION_CLASS,
    work: &mut W,
) -> Result<(), W::Error> {
    relative_kernel_name(name, work)?.ok_or_else(W::invalid_request)?;
    let copied = copy_work(
        (name.len() as u64)
            .checked_mul(2)
            .ok_or(crate::WorkError::Overflow)?,
    );
    work.admit(copied)?;
    let (packet, allocation, length) =
        windows_name_packet(parent, name.iter().copied(), name.len(), replace, work)?;
    let result = (|| {
        work.record(copied)?;
        work.attempt_io(WRITE, || {
            // SAFETY: the initialized packet covers the fixed header and flexible name.
            let bytes = unsafe {
                std::slice::from_raw_parts(
                    packet.as_ptr().cast::<u8>(),
                    length.max(std::mem::size_of::<nt::FILE_RENAME_INFORMATION>() as u32) as usize,
                )
            };
            windows_set_information(file, bytes, class)
        })
    })();
    drop(packet);
    work.release(allocation)?;
    result
}
