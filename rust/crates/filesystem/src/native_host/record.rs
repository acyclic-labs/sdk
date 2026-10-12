use super::*;

// Namespace record names already fit its 96-byte ASCII key/suffix buffer.
const RECORD_NAME_BYTES: usize = 97;
#[cfg(windows)]
const RECORD_PACKET_WORDS: usize = (std::mem::offset_of!(
    windows::Wdk::Storage::FileSystem::FILE_RENAME_INFORMATION,
    FileName
) + 2 * (RECORD_NAME_BYTES - 1))
    .div_ceil(8);

pub(crate) struct PreparedMaterializationRecord {
    file: File,
    parent: Dir,
    #[cfg(unix)]
    names: [[u8; RECORD_NAME_BYTES]; 2],
    #[cfg(unix)]
    lengths: [usize; 2],
    #[cfg(windows)]
    packet: [u64; RECORD_PACKET_WORDS],
    #[cfg(windows)]
    packet_length: u32,
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
        use NativeTreeMaterializationError as E;
        require_leaf(current)?;
        require_leaf(temporary)?;
        if current == temporary {
            return Err(E::InvalidLayout);
        }
        let mut names = [[0; RECORD_NAME_BYTES]; 2];
        let mut lengths = [0; 2];
        for (index, path) in [temporary, current].into_iter().enumerate() {
            let bytes = path.as_os_str().as_encoded_bytes();
            if bytes.len() >= RECORD_NAME_BYTES {
                return Err(E::InvalidLayout);
            }
            for byte in bytes {
                work.charge(ITEM)?;
                if !byte.is_ascii() || *byte == 0 {
                    return Err(E::InvalidLayout);
                }
            }
            #[cfg(unix)]
            work.admit(copy_work(bytes.len() as u64))?;
            names[index][..bytes.len()].copy_from_slice(bytes);
            #[cfg(unix)]
            work.record_admitted(copy_work(bytes.len() as u64))?;
            lengths[index] = bytes.len() + 1;
        }
        let (parent, _) = open_root_directory(parent_path, &mut &*work)?;
        if entry_identity(&parent, current, &mut &*work)?.is_none() {
            return Err(io::Error::from(io::ErrorKind::NotFound).into());
        }
        match remove_leaf(&parent, temporary, false, &mut &*work) {
            Ok(()) => (),
            Err(E::Io(error)) if error.kind() == io::ErrorKind::NotFound => (),
            Err(error) => return Err(error),
        }
        #[cfg(windows)]
        let parent = {
            use windows::Wdk::Storage::FileSystem::{
                FILE_DIRECTORY_FILE, FILE_SYNCHRONOUS_IO_NONALERT,
            };
            use windows::Win32::Storage::FileSystem::{FILE_GENERIC_READ, FILE_GENERIC_WRITE};
            Dir::from_std_file(open_windows_held(
                &parent,
                Path::new("."),
                FILE_GENERIC_READ | FILE_GENERIC_WRITE,
                FILE_DIRECTORY_FILE | FILE_SYNCHRONOUS_IO_NONALERT,
                &mut &*work,
            )?)
        };
        #[cfg(windows)]
        let (packet, packet_length) = {
            use std::os::windows::io::AsRawHandle as _;
            use windows::Wdk::Storage::FileSystem::FILE_RENAME_INFORMATION;
            let mut packet = [0u64; RECORD_PACKET_WORDS];
            let offset = std::mem::offset_of!(FILE_RENAME_INFORMATION, FileName);
            let units = lengths[1] - 1;
            let encoding = WorkCounters {
                bytes_encoded: (units * 2) as u64,
                ..WorkCounters::UNCHARGED
            };
            work.admit(encoding)?;
            // SAFETY: aligned fixed storage covers the header and this admitted ASCII tail.
            unsafe {
                let information = packet.as_mut_ptr().cast::<FILE_RENAME_INFORMATION>();
                (*information).Anonymous.ReplaceIfExists = true;
                (*information).RootDirectory =
                    windows::Win32::Foundation::HANDLE(parent.as_raw_handle());
                (*information).FileNameLength = (units * 2) as u32;
                let tail = std::slice::from_raw_parts_mut(
                    information.cast::<u8>().add(offset).cast::<u16>(),
                    units,
                );
                for (output, byte) in tail.iter_mut().zip(&names[1]) {
                    *output = u16::from(*byte);
                }
            }
            work.record_admitted(encoding)?;
            (packet, (offset + units * 2) as u32)
        };
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
            std::ffi::CStr::from_bytes_with_nul(&names[0][..lengths[0]])
                .map_err(|_| E::InvalidLayout)?,
            libc::O_RDWR
                | libc::O_CREAT
                | libc::O_EXCL
                | libc::O_NOFOLLOW
                | libc::O_CLOEXEC
                | libc::O_NONBLOCK,
            &mut &*work,
        )?;
        #[cfg(windows)]
        let file = {
            let units = lengths[0] - 1;
            let encoding = WorkCounters {
                bytes_encoded: (units * 2) as u64,
                ..WorkCounters::UNCHARGED
            };
            work.admit(encoding)?;
            let mut wide = [0u16; RECORD_NAME_BYTES];
            for (output, byte) in wide.iter_mut().zip(&names[0][..units]) {
                *output = u16::from(*byte);
            }
            work.record_admitted(encoding)?;
            create_windows_wide_at(
                &parent,
                &wide[..units],
                false,
                false,
                false,
                true,
                &mut &*work,
            )?
        };
        let prepared = Self {
            file,
            parent,
            credit,
            cleanup: Some(cleanup),
            #[cfg(unix)]
            names,
            #[cfg(unix)]
            lengths,
            #[cfg(windows)]
            packet,
            #[cfg(windows)]
            packet_length,
        };
        #[cfg(windows)]
        keep_windows_created_file(&prepared.file, &mut &*work)?;
        work.admit(WorkCounters::UNCHARGED)?;
        Ok(prepared)
    }

    pub(crate) fn complete(mut self, bytes: &[u8]) -> Result<(), NativeTreeMaterializationError> {
        use std::io::Write as _;
        if bytes.len() as u64 > self.credit.remaining_work().object_bytes_written {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        let mut remaining = bytes;
        while !remaining.is_empty() {
            self.credit.record_completed(WRITE)?;
            let count = self.file.write(remaining)?;
            if count > remaining.len() {
                return Err(NativeTreeMaterializationError::InvalidLayout);
            }
            self.credit.record_completed(WorkCounters {
                object_bytes_written: count as u64,
                ..WorkCounters::UNCHARGED
            })?;
            if count == 0 {
                return Err(io::Error::from(io::ErrorKind::WriteZero).into());
            }
            remaining = &remaining[count..];
        }
        self.credit.record_completed(WorkCounters {
            durability_operations: 1,
            ..WorkCounters::UNCHARGED
        })?;
        sync_held_descriptor_once(&self.file)?;
        self.credit.record_completed(WRITE)?;
        #[cfg(unix)]
        {
            // SAFETY: prepare validated and terminated both fixed names.
            let temporary = unsafe {
                std::ffi::CStr::from_bytes_with_nul_unchecked(&self.names[0][..self.lengths[0]])
            };
            let current = unsafe {
                std::ffi::CStr::from_bytes_with_nul_unchecked(&self.names[1][..self.lengths[1]])
            };
            rename_unix_names_once(&self.parent, temporary, &self.parent, current, true)?;
        }
        #[cfg(windows)]
        // SAFETY: prepare initialized the entire packet and bounded its length.
        windows_set_information(
            &self.file,
            unsafe {
                std::slice::from_raw_parts(
                    (&self.packet).as_ptr().cast::<u8>(),
                    self.packet_length as usize,
                )
            },
            windows::Wdk::Storage::FileSystem::FileRenameInformation,
        )?;
        drop(self.cleanup.take());
        self.credit.record_completed(WorkCounters {
            durability_operations: 1,
            ..WorkCounters::UNCHARGED
        })?;
        sync_held_descriptor_once(&self.parent)?;
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
            // SAFETY: exact held parent and prepared terminated name remain live.
            let name = unsafe {
                std::ffi::CStr::from_bytes_with_nul_unchecked(&self.names[0][..self.lengths[0]])
            };
            if unsafe { libc::unlinkat(self.parent.as_raw_fd(), name.as_ptr(), 0) } == 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        });
        #[cfg(windows)]
        let _ = cleanup.attempt(|| {
            windows_set_information(
                &self.file,
                &windows::Wdk::Storage::FileSystem::FILE_DISPOSITION_INFORMATION {
                    DeleteFile: true,
                },
                windows::Wdk::Storage::FileSystem::FileDispositionInformation,
            )
        });
    }
}

#[cfg(windows)]
pub(super) fn windows_name_packet<W: NativeHostWork>(
    parent: &impl std::os::windows::io::AsRawHandle,
    name: impl Iterator<Item = u16>,
    units: usize,
    replace: bool,
    work: &mut W,
) -> Result<(Vec<u64>, W::Allocation, u32), W::Error> {
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
