use super::*;

/// Reads native names and facts from one held directory.
pub struct HostStatReader {
    directory: Dir,
    buffer: Vec<u64>,
    next: usize,
    filled: usize,
    #[cfg(windows)]
    volume_serial_number: u32,
}

impl HostStatReader {
    pub(super) const BUFFER_BYTES: usize = 64 * 1024;
    #[cfg(test)]
    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        (self.buffer.capacity() * 8) as u64
    }
    pub(crate) fn next<W: NativeHostWork>(
        &mut self,
        work: &mut W,
    ) -> Result<Option<(HostListedEntry, W::Allocation)>, W::Error> {
        self.next_with_handle::<W, true>(work)
    }
    pub(crate) fn next_metadata<W: NativeHostWork>(
        &mut self,
        work: &mut W,
    ) -> Result<Option<(HostListedEntry, W::Allocation)>, W::Error> {
        #[cfg(all(test, windows))]
        if super::stat_probe::old() {
            return self.next(work);
        }
        #[cfg(all(test, any(target_os = "linux", windows)))]
        if super::stat_probe::restore_walk() {
            return self.next(work);
        }
        #[cfg(target_os = "linux")]
        {
            self.next_with_handle::<W, false>(work)
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.next(work)
        }
    }
    fn next_with_handle<W: NativeHostWork, const HANDLE: bool>(
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
                    #[cfg(target_os = "linux")]
                    if !HANDLE {
                        return stat_at_metadata(self.directory.as_raw_fd(), native_name, work)
                            .map(Some);
                    }
                    stat_at(self.directory.as_raw_fd(), native_name, work).map(Some)
                }
                #[cfg(windows)]
                {
                    let name = windows_leaf_name(&name, work)?;
                    work.attempt_io(READ, || {
                        stat_windows_name(&self.directory, &name, self.volume_serial_number, false)
                    })
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
        #[cfg(test)]
        super::stat_probe::event("GetFileInformationByHandleEx");
        let result = unsafe {
            GetFileInformationByHandleEx(
                HANDLE(self.directory.as_raw_handle()),
                FileIdExtdDirectoryInfo,
                self.buffer.as_mut_ptr().cast(),
                u32::try_from(Self::BUFFER_BYTES)
                    .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?,
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
    #[cfg(target_os = "linux")]
    fn refill(&mut self) -> io::Result<usize> {
        #[cfg(test)]
        super::stat_probe::event("getdents64");
        // SAFETY: getdents64 writes at most BUFFER_BYTES into the aligned owned buffer.
        let count = unsafe {
            libc::syscall(
                libc::SYS_getdents64,
                self.directory.as_raw_fd(),
                self.buffer.as_mut_ptr(),
                Self::BUFFER_BYTES,
            )
        };
        if count < 0 {
            return Err(io::Error::last_os_error());
        }
        usize::try_from(count).map_err(|_| io::Error::other("directory byte count overflow"))
    }
    #[cfg(target_os = "macos")]
    fn refill(&mut self) -> io::Result<usize> {
        let mut attributes = libc::attrlist {
            bitmapcount: libc::ATTR_BIT_MAP_COUNT,
            reserved: 0,
            commonattr: libc::ATTR_CMN_RETURNED_ATTRS | libc::ATTR_CMN_NAME,
            volattr: 0,
            dirattr: 0,
            fileattr: 0,
            forkattr: 0,
        };
        // SAFETY: the held directory, attribute list and bounded aligned buffer remain live.
        let count = unsafe {
            libc::getattrlistbulk(
                self.directory.as_raw_fd(),
                std::ptr::from_mut(&mut attributes).cast(),
                self.buffer.as_mut_ptr().cast(),
                Self::BUFFER_BYTES,
                u64::from(libc::FSOPT_PACK_INVAL_ATTRS),
            )
        };
        if count < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the entire owned buffer is initialized, and the kernel query is bounded by it.
        let bytes = unsafe {
            std::slice::from_raw_parts(self.buffer.as_ptr().cast::<u8>(), Self::BUFFER_BYTES)
        };
        let mut filled = 0;
        for _ in 0..count {
            filled += Self::macos_record(bytes, filled)?.len();
        }
        Ok(filled)
    }

    #[cfg(target_os = "macos")]
    fn macos_record(bytes: &[u8], start: usize) -> io::Result<&[u8]> {
        let malformed = || io::Error::new(io::ErrorKind::InvalidData, "malformed directory record");
        let tail = bytes.get(start..).ok_or_else(malformed)?;
        let prefix = tail.get(..4).ok_or_else(malformed)?;
        let length = u32::from_ne_bytes(prefix.try_into().map_err(|_| malformed())?) as usize;
        if length < 32 || !length.is_multiple_of(8) {
            return Err(malformed());
        }
        tail.get(..length).ok_or_else(malformed)
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
            || self
                .next
                .checked_add(size_of::<FILE_ID_EXTD_DIR_INFO>())
                .is_none_or(|end| end > self.filled)
        {
            return Err(malformed());
        }
        // SAFETY: the complete aligned record header fits initialized owned storage.
        let record = unsafe {
            &*self
                .buffer
                .as_ptr()
                .cast::<u8>()
                .add(self.next)
                .cast::<FILE_ID_EXTD_DIR_INFO>()
        };
        let (length, step) = (
            record.FileNameLength as usize,
            record.NextEntryOffset as usize,
        );
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
    #[cfg(target_os = "linux")]
    fn name(&self) -> io::Result<(usize, &std::ffi::CStr)> {
        let malformed = || io::Error::new(io::ErrorKind::InvalidData, "malformed directory record");
        let header = 19;
        // SAFETY: only the prefix initialized by the last successful query is exposed.
        let bytes =
            unsafe { std::slice::from_raw_parts(self.buffer.as_ptr().cast::<u8>(), self.filled) };
        let start = self.next.checked_add(header).ok_or_else(malformed)?;
        let fixed = bytes.get(self.next..start).ok_or_else(malformed)?;
        let length = usize::from(u16::from_ne_bytes(
            fixed
                .get(16..18)
                .ok_or_else(malformed)?
                .try_into()
                .map_err(|_| malformed())?,
        ));
        if length <= header {
            return Err(malformed());
        }
        let end = self.next.checked_add(length).ok_or_else(malformed)?;
        let tail = bytes.get(start..end).ok_or_else(malformed)?;
        let name = std::ffi::CStr::from_bytes_until_nul(tail).map_err(|_| malformed())?;
        if name.is_empty() || name.to_bytes().contains(&b'/') {
            return Err(malformed());
        }
        Ok((end, name))
    }

    #[cfg(target_os = "macos")]
    fn name(&self) -> io::Result<(usize, &std::ffi::CStr)> {
        let malformed = || io::Error::new(io::ErrorKind::InvalidData, "malformed directory record");
        // SAFETY: only initialized bytes from the last bounded query are exposed.
        let bytes =
            unsafe { std::slice::from_raw_parts(self.buffer.as_ptr().cast::<u8>(), self.filled) };
        let record = Self::macos_record(bytes, self.next)?;
        // SAFETY: the checked record contains the length, returned attributes and name reference.
        let (attributes, reference) = unsafe {
            (
                std::ptr::read_unaligned(record.as_ptr().add(4).cast::<libc::attribute_set_t>()),
                std::ptr::read_unaligned(record.as_ptr().add(24).cast::<libc::attrreference_t>()),
            )
        };
        if attributes.commonattr & libc::ATTR_CMN_NAME == 0 {
            return Err(malformed());
        }
        let offset = usize::try_from(reference.attr_dataoffset).map_err(|_| malformed())?;
        let start = 24_usize.checked_add(offset).ok_or_else(malformed)?;
        let length = usize::try_from(reference.attr_length).map_err(|_| malformed())?;
        let end = start.checked_add(length).ok_or_else(malformed)?;
        if start < 32 {
            return Err(malformed());
        }
        let name =
            std::ffi::CStr::from_bytes_with_nul(record.get(start..end).ok_or_else(malformed)?)
                .map_err(|_| malformed())?;
        if name.is_empty() || name.to_bytes().contains(&b'/') {
            return Err(malformed());
        }
        Ok((self.next + record.len(), name))
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
        },
        allocation,
    ))
}
