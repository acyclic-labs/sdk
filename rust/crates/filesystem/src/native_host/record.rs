use super::*;

pub(crate) struct PreparedMaterializationRecord {
    file: File,
    #[cfg(unix)]
    parent: Dir,
    #[cfg(unix)]
    temporary_name: NativeOwned<Vec<u8>>,
    #[cfg(unix)]
    current_name: NativeOwned<Vec<u8>>,
    #[cfg(windows)]
    rename_packet: NativeOwned<Vec<u64>>,
    #[cfg(windows)]
    rename_length: u32,
    #[cfg(windows)]
    parent_sync: File,
    credit: NativeWorkReservation,
    cleanup: Option<NativeCleanupWrite>,
}

impl PreparedMaterializationRecord {
    pub(crate) fn prepare(
        parent_path: &Path,
        current: &Path,
        temporary: &Path,
        work: &NativeMaterializationWork,
        maximum_bytes: usize,
    ) -> Result<Self, NativeTreeMaterializationError> {
        use crate::materializer::NativeTreeMaterializationError as E;
        require_leaf(current)?;
        require_leaf(temporary)?;
        if current == temporary {
            return Err(E::InvalidLayout);
        }
        let parent =
            open_root_directory(parent_path, &mut &*work).map(|(directory, _)| directory)?;
        if entry_identity(&parent, current, &mut &*work)?.is_none() {
            return Err(io::Error::from(io::ErrorKind::NotFound).into());
        }
        match remove_leaf(&parent, temporary, false, &mut &*work) {
            Ok(()) => {}
            Err(E::Io(error)) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        #[cfg(unix)]
        let temporary_name = prepared_materialization_name(temporary, work)?;
        #[cfg(unix)]
        let current_name = prepared_materialization_name(current, work)?;
        #[cfg(windows)]
        let parent_sync = {
            use windows::Wdk::Storage::FileSystem::{
                FILE_DIRECTORY_FILE, FILE_SYNCHRONOUS_IO_NONALERT,
            };
            use windows::Win32::Storage::FileSystem::{FILE_GENERIC_READ, FILE_GENERIC_WRITE};
            open_windows_held(
                &parent,
                Path::new("."),
                FILE_GENERIC_READ | FILE_GENERIC_WRITE,
                FILE_DIRECTORY_FILE | FILE_SYNCHRONOUS_IO_NONALERT,
                &mut &*work,
            )?
        };
        #[cfg(windows)]
        let (rename_packet, rename_length) =
            prepared_materialization_rename_packet(&parent_sync, current, work)?;
        let credit = work.reserve_work(WorkCounters {
            backend_write_operations: (maximum_bytes as u64)
                .checked_add(1)
                .ok_or(crate::WorkError::Overflow)?,
            object_bytes_written: maximum_bytes as u64,
            durability_operations: 2,
            ..WorkCounters::UNCHARGED
        })?;
        let cleanup = work.reserve_cleanup_write()?;
        #[cfg(unix)]
        let file = open_unix(
            parent.as_raw_fd(),
            std::ffi::CStr::from_bytes_with_nul(&temporary_name.data)
                .expect("prepared temporary leaf is terminated"),
            libc::O_RDWR
                | libc::O_CREAT
                | libc::O_EXCL
                | libc::O_NOFOLLOW
                | libc::O_CLOEXEC
                | libc::O_NONBLOCK,
            &mut &*work,
        )?;
        #[cfg(windows)]
        let file = with_wide_path(temporary, &mut &*work, |name, work| {
            create_windows_wide_at(&parent, name, false, false, false, true, work)
        })?;
        let prepared = Self {
            file,
            #[cfg(unix)]
            parent,
            #[cfg(unix)]
            temporary_name,
            #[cfg(unix)]
            current_name,
            #[cfg(windows)]
            rename_packet,
            #[cfg(windows)]
            rename_length,
            #[cfg(windows)]
            parent_sync,
            credit,
            cleanup: Some(cleanup),
        };
        #[cfg(windows)]
        keep_windows_created_file(&prepared.file, &mut &*work)?;
        work.admit(WorkCounters::UNCHARGED)?;
        Ok(prepared)
    }
    pub(crate) fn complete(mut self, bytes: &[u8]) -> Result<(), NativeTreeMaterializationError> {
        use crate::materializer::NativeTreeMaterializationError as E;
        use std::io::Write as _;
        if bytes.len() as u64 > self.credit.remaining_work().object_bytes_written {
            return Err(E::InvalidLayout);
        }
        let file = &mut self.file;
        let mut offset = 0;
        while offset < bytes.len() {
            self.credit.record_completed(WRITE)?;
            let count = file.write(&bytes[offset..]).map_err(E::Io)?;
            if count > bytes.len() - offset {
                return Err(E::InvalidLayout);
            }
            self.credit.record_completed(WorkCounters {
                object_bytes_written: count as u64,
                ..WorkCounters::UNCHARGED
            })?;
            if count == 0 {
                return Err(io::Error::from(io::ErrorKind::WriteZero).into());
            }
            offset += count;
        }
        self.credit.record_completed(WorkCounters {
            durability_operations: 1,
            ..WorkCounters::UNCHARGED
        })?;
        sync_held_descriptor_once(file)?;
        self.credit.record_completed(WRITE)?;
        #[cfg(unix)]
        {
            // SAFETY: this retained name was validated/terminated at prepare.
            let temporary =
                unsafe { std::ffi::CStr::from_bytes_with_nul_unchecked(&self.temporary_name.data) };
            // SAFETY: preparation validated and terminated these owned names; borrowing them performs no late scan, encoding or allocation.
            let current =
                unsafe { std::ffi::CStr::from_bytes_with_nul_unchecked(&self.current_name.data) };
            rename_unix_names_once(&self.parent, temporary, &self.parent, current, true)?;
        }
        #[cfg(windows)]
        windows_name_packet_once(
            file,
            &self.rename_packet.data,
            self.rename_length,
            windows::Wdk::Storage::FileSystem::FileRenameInformation,
        )?;
        drop(self.cleanup.take());
        self.credit.record_completed(WorkCounters {
            durability_operations: 1,
            ..WorkCounters::UNCHARGED
        })?;
        #[cfg(unix)]
        sync_held_descriptor_once(&self.parent)?;
        #[cfg(windows)]
        sync_held_descriptor_once(&self.parent_sync)?;
        Ok(())
    }
}

impl Drop for PreparedMaterializationRecord {
    fn drop(&mut self) {
        let Some(cleanup) = self.cleanup.take() else {
            return;
        };
        #[cfg(unix)]
        let _ = cleanup.attempt(|| {
            // SAFETY: preparation validated and terminated this retained name.
            let name =
                unsafe { std::ffi::CStr::from_bytes_with_nul_unchecked(&self.temporary_name.data) };
            // SAFETY: the exact held parent and prepared name remain alive; no cancelled allocator/path traversal can gate this cleanup.
            if unsafe { libc::unlinkat(self.parent.as_raw_fd(), name.as_ptr(), 0) } == 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        });
        #[cfg(windows)]
        let _ = cleanup.attempt(|| {
            use windows::Wdk::Storage::FileSystem::{
                FILE_DISPOSITION_INFORMATION, FileDispositionInformation,
            };
            windows_set_information(
                &self.file,
                &FILE_DISPOSITION_INFORMATION { DeleteFile: true },
                FileDispositionInformation,
            )
        });
    }
}

#[cfg(unix)]
pub(super) fn prepared_materialization_name(
    leaf: &Path,
    work: &NativeMaterializationWork,
) -> Result<NativeOwned<Vec<u8>>, NativeTreeMaterializationError> {
    use std::os::unix::ffi::OsStrExt as _;
    let bytes = leaf.as_os_str().as_bytes();
    for byte in bytes {
        work.charge(ITEM)?;
        if *byte == 0 {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
    }
    let copy = WorkCounters {
        bytes_copied: bytes.len() as u64,
        ..WorkCounters::UNCHARGED
    };
    work.admit(copy)?;
    let mut name = native_buffer::<u8>(
        bytes
            .len()
            .checked_add(1)
            .ok_or(crate::WorkError::Overflow)?,
        work,
    )?;
    name.data.extend_from_slice(bytes);
    name.data.push(0);
    work.record_admitted(copy)?;
    Ok(name)
}

#[cfg(windows)]
pub(super) fn prepared_materialization_rename_packet(
    parent: &File,
    leaf: &Path,
    work: &NativeMaterializationWork,
) -> Result<(NativeOwned<Vec<u64>>, u32), NativeTreeMaterializationError> {
    use std::os::windows::ffi::OsStrExt as _;
    let units = wide_units(leaf.as_os_str(), &mut &*work)?;
    let encoded = WorkCounters {
        bytes_encoded: (units as u64)
            .checked_mul(2)
            .ok_or(crate::WorkError::Overflow)?,
        ..WorkCounters::UNCHARGED
    };
    work.admit(encoded)?;
    let (data, allocation, length) = windows_name_packet(
        parent,
        leaf.as_os_str().encode_wide(),
        units,
        true,
        &mut &*work,
    )?;
    let packet = NativeOwned {
        data,
        _allocation: allocation,
    };
    work.record_admitted(encoded)?;
    Ok((packet, length))
}

#[cfg(windows)]
pub(super) fn windows_name_packet<W: NativeHostWork>(
    parent: &impl std::os::windows::io::AsRawHandle,
    name: impl Iterator<Item = u16>,
    units: usize,
    replace: bool,
    work: &mut W,
) -> Result<(Vec<u64>, W::Allocation, u32), W::Error> {
    use std::os::windows::io::AsRawHandle as _;
    use windows::Wdk::Storage::FileSystem::FILE_RENAME_INFORMATION;
    let name_bytes = units.checked_mul(2).ok_or(crate::WorkError::Overflow)?;
    let offset = std::mem::offset_of!(FILE_RENAME_INFORMATION, FileName);
    let length = offset
        .checked_add(name_bytes)
        .ok_or(crate::WorkError::Overflow)?;
    let length_u32 = u32::try_from(length).map_err(|_| crate::WorkError::Overflow)?;
    let words = length
        .max(std::mem::size_of::<FILE_RENAME_INFORMATION>())
        .div_ceil(8);
    let (mut data, allocation) = buffer::<u64, _>(words, work)?;
    data.resize(words, 0);
    let information = data.as_mut_ptr().cast::<FILE_RENAME_INFORMATION>();
    // SAFETY: aligned owned storage covers the native header and the bounded UTF-16 tail.
    let tail = unsafe {
        (*information).Anonymous.ReplaceIfExists = replace;
        (*information).RootDirectory = windows::Win32::Foundation::HANDLE(parent.as_raw_handle());
        (*information).FileNameLength =
            u32::try_from(name_bytes).map_err(|_| crate::WorkError::Overflow)?;
        std::slice::from_raw_parts_mut(information.cast::<u8>().add(offset).cast::<u16>(), units)
    };
    for (destination, unit) in tail.iter_mut().zip(name) {
        if unit == 0 {
            drop(data);
            work.release(allocation)?;
            return Err(W::invalid_request());
        }
        *destination = unit;
    }
    Ok((data, allocation, length_u32))
}

#[cfg(unix)]
pub(super) fn rename_unix_names_once(
    from: &Dir,
    source: &std::ffi::CStr,
    to: &Dir,
    destination: &std::ffi::CStr,
    replace: bool,
) -> io::Result<()> {
    // SAFETY: actual held parents and prepared names outlive this syscall.
    #[cfg(target_os = "linux")]
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            from.as_raw_fd(),
            source.as_ptr(),
            to.as_raw_fd(),
            destination.as_ptr(),
            if replace { 0 } else { libc::RENAME_NOREPLACE },
        )
    };
    #[cfg(target_os = "macos")]
    let result = unsafe {
        libc::renameatx_np(
            from.as_raw_fd(),
            source.as_ptr(),
            to.as_raw_fd(),
            destination.as_ptr(),
            if replace { 0 } else { libc::RENAME_EXCL },
        )
    } as libc::c_long;
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(windows)]
pub(super) fn windows_name_packet_once(
    file: &File,
    buffer: &[u64],
    length: u32,
    class: windows::Wdk::Storage::FileSystem::FILE_INFORMATION_CLASS,
) -> io::Result<()> {
    use std::os::windows::io::AsRawHandle as _;
    use windows::Wdk::Storage::FileSystem::NtSetInformationFile;
    let mut block = windows::Win32::System::IO::IO_STATUS_BLOCK::default();
    // SAFETY: the held DELETE-capable file and complete aligned prebuilt packet remain alive; the parent handle stored in the packet is pinned.
    let status = unsafe {
        NtSetInformationFile(
            windows::Win32::Foundation::HANDLE(file.as_raw_handle()),
            &raw mut block,
            buffer.as_ptr().cast(),
            length,
            class,
        )
    };
    if status.is_err() {
        Err(status_error(status))
    } else {
        Ok(())
    }
}

#[cfg(unix)]
pub(super) fn sync_held_descriptor_once(file: &impl std::os::fd::AsRawFd) -> io::Result<()> {
    // SAFETY: the exact held file/directory descriptor remains live.
    if unsafe { libc::fsync(file.as_raw_fd()) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(windows)]
pub(super) fn sync_held_descriptor_once(
    file: &impl std::os::windows::io::AsRawHandle,
) -> io::Result<()> {
    // SAFETY: this held file/directory handle has genuine write access.
    unsafe {
        windows::Win32::Storage::FileSystem::FlushFileBuffers(windows::Win32::Foundation::HANDLE(
            file.as_raw_handle(),
        ))
    }
    .map_err(|error| io::Error::from_raw_os_error(error.code().0 & 0xffff))
}
