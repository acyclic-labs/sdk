use super::*;

pub(crate) struct PreparedMaterializationRecord {
    pub(super) file: File,
    pub(super) _parent: Dir,
    #[cfg(unix)]
    pub(super) temporary_name: NativeOwned<Vec<u8>>,
    #[cfg(unix)]
    pub(super) current_name: NativeOwned<Vec<u8>>,
    #[cfg(windows)]
    pub(super) rename_packet: NativeOwned<Vec<u64>>,
    #[cfg(windows)]
    pub(super) rename_length: u32,
    #[cfg(windows)]
    pub(super) parent_sync: File,
    pub(super) maximum_bytes: usize,
    pub(super) credit: crate::materializer::NativeWorkReservation,
    pub(super) cleanup: Option<crate::materializer::NativeCleanupWrite>,
    pub(super) _allocation: crate::materializer::NativeAllocation,
}

impl PreparedMaterializationRecord {
    pub(crate) fn prepare(
        parent_path: &Path, current: &Path, previous: &Path, temporary: &Path,
        has_current: bool, work: &crate::materializer::NativeMaterializationWork, maximum_bytes: usize,
    ) -> Result<Self, crate::materializer::NativeTreeMaterializationError> {
        use crate::materializer::NativeTreeMaterializationError as E;
        require_leaf(current)?;
        require_leaf(previous)?;
        require_leaf(temporary)?;
        if !has_current || current == previous || current == temporary || previous == temporary {
            return Err(E::InvalidLayout);
        }
        let allocation = work.claim(std::mem::size_of::<Self>() as u64, 0, crate::WorkCounters::UNCHARGED)?;
        let parent = open_root_directory(parent_path, &mut &*work).map(|(directory, _)| directory)?;
        if entry_identity(&parent, current, &mut &*work)?.is_none() {
            return Err(io::Error::from(io::ErrorKind::NotFound).into());
        }
        match remove_leaf(&parent, temporary, &mut &*work) {
            Ok(()) => {}
            Err(E::Io(error)) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        #[cfg(unix)]
        let temporary_name = prepared_materialization_name(temporary, work)?;
        #[cfg(unix)]
        let current_name = prepared_materialization_name(current, work)?;
        #[cfg(windows)]
        let (rename_packet, rename_length) = prepared_materialization_rename_packet(&parent, current, work)?;
        #[cfg(windows)]
        let parent_sync = {
            use windows::Wdk::Storage::FileSystem::{FILE_DIRECTORY_FILE, FILE_SYNCHRONOUS_IO_NONALERT};
            use windows::Win32::Storage::FileSystem::{FILE_GENERIC_READ, FILE_GENERIC_WRITE};
            open_windows_held(&parent, Path::new("."), FILE_GENERIC_READ | FILE_GENERIC_WRITE,
                FILE_DIRECTORY_FILE | FILE_SYNCHRONOUS_IO_NONALERT, &mut &*work)?
        };
        let credit = work.reserve_work(WorkCounters {
            backend_write_operations: (maximum_bytes as u64).checked_add(1).ok_or(crate::WorkError::Overflow)?,
            object_bytes_written: maximum_bytes as u64,
            durability_operations: 2,
            ..WorkCounters::UNCHARGED
        })?;
        let cleanup = work.reserve_cleanup_write()?;
        #[cfg(unix)]
        let file = open_unix(parent.as_raw_fd(),
            std::ffi::CStr::from_bytes_with_nul(&temporary_name.data).expect("prepared temporary leaf is terminated"),
            libc::O_RDWR | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
            || work.charge(WorkCounters { backend_write_operations: 1, ..WorkCounters::UNCHARGED }))?;
        #[cfg(windows)]
        let file = with_wide_path(temporary, &mut &*work, |name, work| {
            create_windows_provisional_file(&parent, name, work)
        })?;
        let prepared = Self {
            file, _parent: parent,
            #[cfg(unix)] temporary_name,
            #[cfg(unix)] current_name,
            #[cfg(windows)] rename_packet,
            #[cfg(windows)] rename_length,
            #[cfg(windows)] parent_sync,
            maximum_bytes, credit, cleanup: Some(cleanup), _allocation: allocation,
        };
        #[cfg(windows)]
        keep_windows_created_file(&prepared.file, &mut &*work)?;
        work.admit(WorkCounters::UNCHARGED)?;
        Ok(prepared)
    }
    pub(crate) fn complete(mut self, bytes: &[u8]) -> Result<(), crate::materializer::NativeTreeMaterializationError> {
        use std::io::Write as _;
        use crate::materializer::NativeTreeMaterializationError as E;
        if bytes.len() > self.maximum_bytes { return Err(E::InvalidLayout); }
        let file = &mut self.file;
        let mut offset = 0;
        while offset < bytes.len() {
            self.credit.record_completed(WorkCounters { backend_write_operations: 1, ..WorkCounters::UNCHARGED })?;
            let count = file.write(&bytes[offset..]).map_err(E::Io)?;
            if count > bytes.len() - offset { return Err(E::InvalidLayout); }
            self.credit.record_completed(WorkCounters { object_bytes_written: count as u64, ..WorkCounters::UNCHARGED })?;
            if count == 0 { return Err(io::Error::from(io::ErrorKind::WriteZero).into()); }
            offset += count;
        }
        self.credit.record_completed(WorkCounters { durability_operations: 1, ..WorkCounters::UNCHARGED })?;
        sync_held_descriptor_once(file)?;
        self.credit.record_completed(WorkCounters { backend_write_operations: 1, ..WorkCounters::UNCHARGED })?;
        #[cfg(unix)]
        {
            // SAFETY: this retained name was validated/terminated at prepare.
            let temporary = unsafe { std::ffi::CStr::from_bytes_with_nul_unchecked(&self.temporary_name.data) };
            // SAFETY: preparation validated and terminated these owned names; borrowing them performs no late scan, encoding or allocation.
            let current = unsafe { std::ffi::CStr::from_bytes_with_nul_unchecked(&self.current_name.data) };
            rename_unix_names_once(&self._parent, temporary, &self._parent, current, true)?;
        }
        #[cfg(windows)]
        windows_rename_packet_once(file, &self.rename_packet.data, self.rename_length)?;
        drop(self.cleanup.take());
        self.credit.record_completed(WorkCounters { durability_operations: 1, ..WorkCounters::UNCHARGED })?;
        #[cfg(unix)]
        sync_held_descriptor_once(&self._parent)?;
        #[cfg(windows)]
        sync_held_descriptor_once(&self.parent_sync)?;
        Ok(())
    }
}

impl Drop for PreparedMaterializationRecord {
    fn drop(&mut self) {
        let Some(cleanup) = self.cleanup.take() else { return; };
        #[cfg(unix)]
        let _ = cleanup.attempt(|| {
            // SAFETY: preparation validated and terminated this retained name.
            let name = unsafe { std::ffi::CStr::from_bytes_with_nul_unchecked(&self.temporary_name.data) };
            // SAFETY: the exact held parent and prepared name remain alive; no cancelled allocator/path traversal can gate this cleanup.
            if unsafe { libc::unlinkat(self._parent.as_raw_fd(), name.as_ptr(), 0) } == 0 {
                Ok(())
            } else { Err(io::Error::last_os_error()) }
        });
        #[cfg(windows)]
        let _ = cleanup.attempt(|| {
            use windows::Wdk::Storage::FileSystem::{FILE_DISPOSITION_INFORMATION, FileDispositionInformation};
            windows_set_information(&self.file,
                &FILE_DISPOSITION_INFORMATION { DeleteFile: true }, FileDispositionInformation)
        });
    }
}

#[cfg(unix)]
pub(super) fn prepared_materialization_name(
    leaf: &Path, work: &crate::materializer::NativeMaterializationWork,
) -> Result<NativeOwned<Vec<u8>>, crate::materializer::NativeTreeMaterializationError> {
    use std::os::unix::ffi::OsStrExt as _;
    let bytes = leaf.as_os_str().as_bytes();
    for byte in bytes {
        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
        if *byte == 0 { return Err(crate::materializer::NativeTreeMaterializationError::InvalidLayout); }
    }
    let copy = WorkCounters { bytes_copied: bytes.len() as u64, ..WorkCounters::UNCHARGED };
    work.admit(copy)?;
    let mut name = native_buffer::<u8>(bytes.len().checked_add(1).ok_or(crate::WorkError::Overflow)?, work)?;
    name.data.extend_from_slice(bytes);
    name.data.push(0);
    work.record_admitted(copy)?;
    Ok(name)
}

#[cfg(windows)]
pub(super) fn prepared_materialization_rename_packet(
    parent: &Dir, leaf: &Path, work: &crate::materializer::NativeMaterializationWork,
) -> Result<(NativeOwned<Vec<u64>>, u32), crate::materializer::NativeTreeMaterializationError> {
    use std::os::windows::ffi::OsStrExt as _;
    use std::os::windows::io::{AsHandle as _, AsRawHandle as _};
    use windows::Wdk::Storage::FileSystem::FILE_RENAME_INFORMATION;
    let units = wide_units_with(leaf.as_os_str(), || work.charge(
        WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED },
    ))?;
    let name_bytes = units.checked_mul(2).ok_or(crate::WorkError::Overflow)?;
    let header = std::mem::offset_of!(FILE_RENAME_INFORMATION, FileName);
    let length = header.checked_add(name_bytes).ok_or(crate::WorkError::Overflow)?;
    let words = length.max(std::mem::size_of::<FILE_RENAME_INFORMATION>()).div_ceil(8);
    let encoded = WorkCounters { bytes_encoded: name_bytes as u64, ..WorkCounters::UNCHARGED };
    work.admit(encoded)?;
    let mut buffer = native_buffer::<u64>(words, work)?;
    buffer.data.resize(words, 0);
    let information = buffer.data.as_mut_ptr().cast::<FILE_RENAME_INFORMATION>();
    // SAFETY: pre-funded aligned storage contains the full header/name tail.
    unsafe {
        information.cast::<u8>().write(1);
        std::ptr::addr_of_mut!((*information).RootDirectory).write(windows::Win32::Foundation::HANDLE(parent.as_handle().as_raw_handle()));
        std::ptr::addr_of_mut!((*information).FileNameLength).write(u32::try_from(name_bytes).map_err(|_| crate::WorkError::Overflow)?);
        let tail = buffer.data.as_mut_ptr().cast::<u8>().add(header).cast::<u16>();
        for (index, unit) in leaf.as_os_str().encode_wide().enumerate() {
            if unit == 0 { return Err(crate::materializer::NativeTreeMaterializationError::InvalidLayout); }
            tail.add(index).write(unit);
        }
    }
    work.record_admitted(encoded)?;
    Ok((buffer, u32::try_from(length).map_err(|_| crate::WorkError::Overflow)?))
}

#[cfg(unix)]
pub(super) fn rename_unix_names_once(
    from: &Dir, source: &std::ffi::CStr, to: &Dir, destination: &std::ffi::CStr, replace: bool,
) -> io::Result<()> {
    // SAFETY: actual held parents and prepared names outlive this syscall.
    #[cfg(target_os = "linux")]
    let result = unsafe { libc::syscall(libc::SYS_renameat2, from.as_raw_fd(), source.as_ptr(),
        to.as_raw_fd(), destination.as_ptr(), if replace { 0 } else { libc::RENAME_NOREPLACE }) };
    #[cfg(target_os = "macos")]
    let result = unsafe { libc::renameatx_np(from.as_raw_fd(), source.as_ptr(), to.as_raw_fd(),
        destination.as_ptr(), if replace { 0 } else { libc::RENAME_EXCL }) } as libc::c_long;
    if result == 0 { Ok(()) } else { Err(io::Error::last_os_error()) }
}

#[cfg(windows)]
pub(super) fn windows_rename_packet_once(file: &File, buffer: &[u64], length: u32) -> io::Result<()> {
    use std::os::windows::io::AsRawHandle as _;
    use windows::Wdk::Storage::FileSystem::{FileRenameInformation, NtSetInformationFile};
    let mut block = windows::Win32::System::IO::IO_STATUS_BLOCK::default();
    // SAFETY: the held DELETE-capable file and complete aligned prebuilt packet remain alive; the parent handle stored in the packet is pinned.
    let status = unsafe { NtSetInformationFile(windows::Win32::Foundation::HANDLE(file.as_raw_handle()),
        &raw mut block, buffer.as_ptr().cast(), length, FileRenameInformation) };
    if status.is_err() { Err(status_error(status)) } else { Ok(()) }
}

#[cfg(unix)]
pub(super) fn sync_held_descriptor_once(file: &impl std::os::fd::AsRawFd) -> io::Result<()> {
    // SAFETY: the exact held file/directory descriptor remains live.
    if unsafe { libc::fsync(file.as_raw_fd()) } == 0 { Ok(()) } else { Err(io::Error::last_os_error()) }
}

#[cfg(windows)]
pub(super) fn sync_held_descriptor_once(file: &impl std::os::windows::io::AsRawHandle) -> io::Result<()> {
    // SAFETY: this held file/directory handle has genuine write access.
    unsafe { windows::Win32::Storage::FileSystem::FlushFileBuffers(
        windows::Win32::Foundation::HANDLE(file.as_raw_handle()),
    ) }.map_err(|error| io::Error::from_raw_os_error(error.code().0 & 0xffff))
}
