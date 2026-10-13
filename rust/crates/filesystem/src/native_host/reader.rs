use super::*;

/// Reads native names and facts from one held directory.
pub struct HostStatReader {
    directory: Dir,
    buffer: Vec<u64>,
    next: usize,
    filled: usize,
    #[cfg(windows)]
    volume_serial_number: u32,
    #[cfg(target_os = "macos")]
    base: libc::off_t,
}

impl HostStatReader {
    pub(super) const BUFFER_BYTES: usize = 64 * 1024;
    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        (self.buffer.capacity() * 8) as u64
    }
    pub(crate) fn next<W: NativeHostWork>(
        &mut self,
        work: &mut W,
    ) -> Result<Option<(HostListedEntry, W::Allocation)>, W::Error> {
        #[cfg(unix)]
        use std::os::unix::ffi::OsStringExt as _;
        loop {
            work.check()?;
            if self.next == self.filled {
                if self.filled == 0 {
                    return Ok(None);
                }
                work.started_io(READ)?;
                self.filled = match self.refill() {
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                    value => value?,
                };
                self.next = 0;
                if self.filled == 0 {
                    return Ok(None);
                }
            }
            work.started_io(ITEM)?;
            let (end, native_name) = self.name()?;
            #[cfg(unix)]
            let raw_name = native_name.to_bytes();
            #[cfg(windows)]
            let raw_name = native_name;
            if raw_name == [46] || raw_name == [46, 46] {
                self.next = end;
                continue;
            }
            let copied = copy_work(std::mem::size_of_val(raw_name) as u64);
            work.admit(copied)?;
            let (mut name, allocation) = buffer(raw_name.len(), work)?;
            name.extend_from_slice(raw_name);
            #[cfg(unix)]
            let name = OsString::from_vec(name);
            let result = (|| {
                work.record(copied)?;
                #[cfg(unix)]
                {
                    stat_at(self.directory.as_raw_fd(), native_name, work).map(Some)
                }
                #[cfg(windows)]
                {
                    stat_windows_name(
                        &self.directory,
                        &windows_leaf_name(&name, work)?,
                        self.volume_serial_number,
                        work,
                    )
                }
            })();
            let stat = match result {
                Ok(stat) => stat,
                Err(error) if W::io_error_kind(&error) == Some(io::ErrorKind::NotFound) => None,
                Err(error) => {
                    drop(name);
                    work.release(allocation)?;
                    if W::io_error_kind(&error).is_some() {
                        #[cfg(unix)]
                        {
                            self.next = end;
                        }
                        #[cfg(windows)]
                        {
                            self.next = 0;
                            self.filled = 0;
                        }
                    }
                    return Err(error);
                }
            };
            self.next = end;
            return Ok(Some((HostListedEntry { name, stat }, allocation)));
        }
    }
    #[cfg(windows)]
    fn refill(&mut self) -> io::Result<usize> {
        use std::os::windows::io::AsRawHandle as _;
        use windows::Win32::{
            Foundation::{ERROR_NO_MORE_FILES, HANDLE},
            Storage::FileSystem::{FileIdExtdDirectoryInfo, GetFileInformationByHandleEx},
        };
        // SAFETY: the held directory and aligned owned buffer outlive this synchronous query.
        let result = unsafe {
            GetFileInformationByHandleEx(
                HANDLE(self.directory.as_raw_handle()),
                FileIdExtdDirectoryInfo,
                self.buffer.as_mut_ptr().cast(),
                Self::BUFFER_BYTES as u32,
            )
        };
        match result {
            Ok(()) => Ok(Self::BUFFER_BYTES),
            Err(e) if e.code() == windows::core::HRESULT::from_win32(ERROR_NO_MORE_FILES.0) => {
                Ok(0)
            }
            Err(e) => Err(io::Error::from_raw_os_error(e.code().0 & 0xffff)),
        }
    }
    #[cfg(unix)]
    fn refill(&mut self) -> io::Result<usize> {
        // SAFETY: both calls write at most BUFFER_BYTES into the aligned owned buffer.
        #[cfg(target_os = "linux")]
        let count = unsafe {
            libc::syscall(
                libc::SYS_getdents64,
                self.directory.as_raw_fd(),
                self.buffer.as_mut_ptr(),
                Self::BUFFER_BYTES,
            )
        } as isize;
        #[cfg(target_os = "macos")]
        let count = unsafe {
            unsafe extern "C" {
                fn getdirentries64(
                    fd: libc::c_int,
                    buf: *mut libc::c_char,
                    size: libc::size_t,
                    base: *mut libc::off_t,
                ) -> libc::ssize_t;
            }
            getdirentries64(
                self.directory.as_raw_fd(),
                self.buffer.as_mut_ptr().cast(),
                Self::BUFFER_BYTES,
                &mut self.base,
            )
        };
        if count < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(count as usize)
    }
    #[cfg(windows)]
    fn name(&self) -> io::Result<(usize, &[u16])> {
        use fs::FILE_ID_EXTD_DIR_INFO;
        use std::mem::{align_of, offset_of};
        let malformed = || io::Error::new(io::ErrorKind::InvalidData, "malformed directory record");
        let header = offset_of!(FILE_ID_EXTD_DIR_INFO, FileName);
        let start = self.next.checked_add(header).ok_or_else(malformed)?;
        if !self
            .next
            .is_multiple_of(align_of::<FILE_ID_EXTD_DIR_INFO>())
            || start > self.filled
        {
            return Err(malformed());
        }
        // SAFETY: the fixed header fits aligned owned storage initialized by the query.
        let record = unsafe {
            self.buffer
                .as_ptr()
                .cast::<u8>()
                .add(self.next)
                .cast::<FILE_ID_EXTD_DIR_INFO>()
        };
        let (length, step) = unsafe {
            (
                std::ptr::addr_of!((*record).FileNameLength).read_unaligned() as usize,
                std::ptr::addr_of!((*record).NextEntryOffset).read_unaligned() as usize,
            )
        };
        let end = if step == 0 {
            self.filled
        } else {
            let end = self.next.checked_add(step).ok_or_else(malformed)?;
            if !step.is_multiple_of(align_of::<FILE_ID_EXTD_DIR_INFO>()) || end >= self.filled {
                return Err(malformed());
            }
            end
        };
        if length == 0
            || !length.is_multiple_of(2)
            || start.checked_add(length).is_none_or(|e| e > end)
        {
            return Err(malformed());
        }
        // SAFETY: the complete UTF-16 name lies within this record, before the next header.
        Ok((end, unsafe {
            std::slice::from_raw_parts(
                self.buffer.as_ptr().cast::<u8>().add(start).cast::<u16>(),
                length / 2,
            )
        }))
    }
    #[cfg(unix)]
    fn name(&self) -> io::Result<(usize, &std::ffi::CStr)> {
        let malformed = || io::Error::new(io::ErrorKind::InvalidData, "malformed directory record");
        #[cfg(target_os = "linux")]
        let header = 19;
        #[cfg(target_os = "macos")]
        let header = 21;
        // SAFETY: only the prefix initialized by the last successful query is exposed.
        let bytes =
            unsafe { std::slice::from_raw_parts(self.buffer.as_ptr().cast::<u8>(), self.filled) };
        let start = self.next.checked_add(header).ok_or_else(malformed)?;
        let fixed = bytes.get(self.next..start).ok_or_else(malformed)?;
        let length = u16::from_ne_bytes([fixed[16], fixed[17]]) as usize;
        if length <= header {
            return Err(malformed());
        }
        let end = self.next.checked_add(length).ok_or_else(malformed)?;
        let tail = bytes.get(start..end).ok_or_else(malformed)?;
        let terminator = tail.iter().position(|b| *b == 0).ok_or_else(malformed)?;
        let name = std::ffi::CStr::from_bytes_with_nul(&tail[..=terminator])
            .expect("record ends at its first NUL");
        if name.is_empty() || name.to_bytes().contains(&b'/') {
            return Err(malformed());
        }
        Ok((end, name))
    }
}

pub(crate) fn read_dir_held<W: NativeHostWork>(
    directory: &Dir,
    work: &mut W,
) -> Result<(HostStatReader, W::Allocation), W::Error> {
    #[cfg(windows)]
    let volume_serial_number = u32::try_from(directory_identity(directory, work)?.device)
        .map_err(|_| W::invalid_request())?;
    #[cfg(unix)]
    let file = {
        let flags = libc::O_RDONLY | libc::O_CLOEXEC | libc::O_DIRECTORY;
        #[cfg(target_os = "linux")]
        let flags = flags | libc::O_NOATIME;
        open_unix(directory.as_raw_fd(), c".", flags, work)?
    };
    #[cfg(windows)]
    let file = open_windows_at(
        directory,
        &[46],
        fs::FILE_GENERIC_READ,
        nt::FILE_OPEN,
        nt::FILE_DIRECTORY_FILE | nt::FILE_SYNCHRONOUS_IO_NONALERT,
        work,
    )?;
    let (mut buffer, allocation) = buffer::<u64, _>(HostStatReader::BUFFER_BYTES / 8, work)?;
    buffer.resize(HostStatReader::BUFFER_BYTES / 8, 0);
    Ok((
        HostStatReader {
            directory: Dir::from_std_file(file),
            buffer,
            next: HostStatReader::BUFFER_BYTES,
            filled: HostStatReader::BUFFER_BYTES,
            #[cfg(windows)]
            volume_serial_number,
            #[cfg(target_os = "macos")]
            base: 0,
        },
        allocation,
    ))
}
