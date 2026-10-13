use super::*;

const RECORD_NAME_BYTES: usize = 97;
#[cfg(windows)]
const RECORD_PACKET_WORDS: usize = (std::mem::offset_of!(nt::FILE_RENAME_INFORMATION, FileName)
    + 2 * (RECORD_NAME_BYTES - 1))
    .div_ceil(8);

pub(crate) struct PreparedMaterializationRecord {
    file: File,
    parent: Dir,
    #[cfg(unix)]
    names: [[u8; RECORD_NAME_BYTES]; 2],
    #[cfg(windows)]
    packet: [u64; RECORD_PACKET_WORDS],
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
        #[cfg(unix)]
        let mut names = [[0u8; RECORD_NAME_BYTES]; 2];
        #[cfg(windows)]
        let mut names = [[0u16; RECORD_NAME_BYTES]; 2];
        for (output, path) in names.iter_mut().zip([temporary, current]) {
            let bytes = path.as_os_str().as_encoded_bytes();
            if bytes.len() >= RECORD_NAME_BYTES {
                return Err(E::InvalidLayout);
            }
            #[cfg(unix)]
            let copied = copy_work(bytes.len() as u64);
            #[cfg(windows)]
            let copied = WorkCounters {
                bytes_encoded: (bytes.len() * 2) as u64,
                ..WorkCounters::UNCHARGED
            };
            work.admit(copied)?;
            for (output, byte) in output.iter_mut().zip(bytes) {
                work.charge(ITEM)?;
                if !byte.is_ascii() || *byte == 0 {
                    return Err(E::InvalidLayout);
                }
                *output = (*byte).into();
            }
            work.record_admitted(copied)?;
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
        let parent = Dir::from_std_file(open_windows_at(
            &parent,
            &[46],
            fs::FILE_GENERIC_READ | fs::FILE_GENERIC_WRITE,
            nt::FILE_OPEN,
            nt::FILE_DIRECTORY_FILE | nt::FILE_SYNCHRONOUS_IO_NONALERT,
            &mut &*work,
        )?);
        #[cfg(windows)]
        let packet = {
            use std::os::windows::io::AsRawHandle as _;
            let mut packet = [0u64; RECORD_PACKET_WORDS];
            let units = current.as_os_str().as_encoded_bytes().len();
            work.charge(copy_work((units * 2) as u64))?;
            // SAFETY: aligned fixed storage covers the header and bounded native name.
            unsafe {
                let header = packet.as_mut_ptr().cast::<nt::FILE_RENAME_INFORMATION>();
                (*header).Anonymous.ReplaceIfExists = true;
                (*header).RootDirectory = win::HANDLE(parent.as_raw_handle());
                (*header).FileNameLength = (units * 2) as u32;
                std::slice::from_raw_parts_mut(
                    header
                        .cast::<u8>()
                        .add(std::mem::offset_of!(nt::FILE_RENAME_INFORMATION, FileName))
                        .cast::<u16>(),
                    units,
                )
                .copy_from_slice(&names[1][..units]);
            }
            packet
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
            record_name(&names[0]),
            libc::O_RDWR
                | libc::O_CREAT
                | libc::O_EXCL
                | libc::O_NOFOLLOW
                | libc::O_CLOEXEC
                | libc::O_NONBLOCK,
            &mut &*work,
        )?;
        #[cfg(windows)]
        let file = open_windows_at(
            &parent,
            &names[0][..temporary.as_os_str().as_encoded_bytes().len()],
            fs::FILE_GENERIC_READ | fs::FILE_GENERIC_WRITE | fs::DELETE,
            nt::FILE_CREATE,
            nt::FILE_NON_DIRECTORY_FILE
                | nt::FILE_SYNCHRONOUS_IO_NONALERT
                | nt::FILE_DELETE_ON_CLOSE,
            &mut &*work,
        )?;
        let prepared = Self {
            file,
            parent,
            credit,
            cleanup: Some(cleanup),
            #[cfg(unix)]
            names,
            #[cfg(windows)]
            packet,
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
            if count == 0 {
                return Err(io::Error::from(io::ErrorKind::WriteZero).into());
            }
            self.credit.record_completed(WorkCounters {
                object_bytes_written: count as u64,
                ..WorkCounters::UNCHARGED
            })?;
            remaining = &remaining[count..];
        }
        let sync = WorkCounters {
            durability_operations: 1,
            ..WorkCounters::UNCHARGED
        };
        self.credit.record_completed(sync)?;
        sync_held_descriptor_once(&self.file)?;
        self.credit.record_completed(WRITE)?;
        #[cfg(unix)]
        rename_unix_names_once(
            &self.parent,
            record_name(&self.names[0]),
            &self.parent,
            record_name(&self.names[1]),
            acyclic_native_runtime::RenameMode::Replace,
        )?;
        #[cfg(windows)]
        {
            // SAFETY: prepare initialized this header and its bounded flexible name.
            let bytes = unsafe {
                let header = &*self.packet.as_ptr().cast::<nt::FILE_RENAME_INFORMATION>();
                std::slice::from_raw_parts(
                    self.packet.as_ptr().cast::<u8>(),
                    std::mem::offset_of!(nt::FILE_RENAME_INFORMATION, FileName)
                        + header.FileNameLength as usize,
                )
            };
            windows_set_information(&self.file, bytes, nt::FileRenameInformation)?;
        }
        drop(self.cleanup.take());
        self.credit.record_completed(sync)?;
        sync_held_descriptor_once(&self.parent)?;
        Ok(())
    }
}

#[cfg(unix)]
fn record_name(bytes: &[u8; RECORD_NAME_BYTES]) -> &std::ffi::CStr {
    std::ffi::CStr::from_bytes_until_nul(bytes).expect("prepared terminated ASCII name")
}

impl Drop for PreparedMaterializationRecord {
    fn drop(&mut self) {
        let Some(cleanup) = self.cleanup.take() else {
            return;
        };
        #[cfg(unix)]
        let _ = cleanup.attempt(|| {
            // SAFETY: the held parent and prepared name remain live.
            if unsafe {
                libc::unlinkat(
                    self.parent.as_raw_fd(),
                    record_name(&self.names[0]).as_ptr(),
                    0,
                )
            } == 0
            {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        });
        #[cfg(windows)]
        let _ = cleanup.attempt(|| {
            windows_set_information(
                &self.file,
                &nt::FILE_DISPOSITION_INFORMATION { DeleteFile: true },
                nt::FileDispositionInformation,
            )
        });
    }
}

#[cfg(windows)]
pub(super) fn windows_name_packet<W: NativeHostWork>(
    parent: &impl std::os::windows::io::AsRawHandle,
    name: &[u16],
    replace: bool,
    work: &mut W,
) -> Result<(Vec<u64>, W::Allocation, u32), W::Error> {
    use nt::FILE_RENAME_INFORMATION;
    let name_bytes = name
        .len()
        .checked_mul(2)
        .ok_or(crate::WorkError::Overflow)?;
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
        (*information).RootDirectory = win::HANDLE(parent.as_raw_handle());
        (*information).FileNameLength =
            u32::try_from(name_bytes).map_err(|_| crate::WorkError::Overflow)?;
        std::slice::from_raw_parts_mut(
            information.cast::<u8>().add(offset).cast::<u16>(),
            name.len(),
        )
    };
    tail.copy_from_slice(name);
    Ok((data, allocation, length_u32))
}

#[cfg(unix)]
pub(super) fn rename_unix_names_once(
    from: &Dir,
    source: &std::ffi::CStr,
    to: &Dir,
    destination: &std::ffi::CStr,
    mode: acyclic_native_runtime::RenameMode,
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
            if mode == acyclic_native_runtime::RenameMode::Replace {
                0
            } else {
                libc::RENAME_NOREPLACE
            },
        )
    };
    #[cfg(target_os = "macos")]
    let result = unsafe {
        libc::renameatx_np(
            from.as_raw_fd(),
            source.as_ptr(),
            to.as_raw_fd(),
            destination.as_ptr(),
            if mode == acyclic_native_runtime::RenameMode::Replace {
                0
            } else {
                libc::RENAME_EXCL
            },
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
    unsafe { fs::FlushFileBuffers(win::HANDLE(file.as_raw_handle())) }
        .map_err(|error| io::Error::from_raw_os_error(error.code().0 & 0xffff))
}
