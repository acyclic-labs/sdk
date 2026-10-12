use super::*;

/// Reads entry facts from one held directory.
pub struct HostStatReader {
    pub(super) directory: Dir,
    pub(super) buffer: Vec<u64>,
    #[cfg(unix)]
    pub(super) next: usize,
    #[cfg(windows)]
    pub(super) next: Option<usize>,
    #[cfg(unix)]
    pub(super) filled: usize,
    #[cfg(windows)]
    pub(super) volume_serial_number: u32,
    pub(super) exhausted: bool,
    #[cfg(target_os = "macos")]
    pub(super) base: libc::off_t,
}

#[cfg(windows)]
impl HostStatReader {
    pub(super) fn refill(&mut self) -> io::Result<bool> {
        use std::os::windows::io::{AsHandle as _, AsRawHandle as _};
        use windows::Win32::Foundation::{ERROR_NO_MORE_FILES, HANDLE};
        use windows::Win32::Storage::FileSystem::{
            FileIdExtdDirectoryInfo, GetFileInformationByHandleEx,
        };
        if self.exhausted {
            return Ok(false);
        }
        let bytes = u32::try_from(self.buffer.len() * std::mem::size_of::<u64>())
            .map_err(|_| io::Error::other("enumeration buffer size"))?;
        // SAFETY: the held directory handle and the owned buffer outlive this synchronous call, and `bytes` is exactly the buffer's length.
        let result = unsafe {
            GetFileInformationByHandleEx(
                HANDLE(self.directory.as_handle().as_raw_handle()),
                FileIdExtdDirectoryInfo,
                self.buffer.as_mut_ptr().cast(),
                bytes,
            )
        };
        match result {
            Ok(()) => {
                self.next = Some(0);
                Ok(true)
            }
            Err(error)
                if error.code() == windows::core::HRESULT::from_win32(ERROR_NO_MORE_FILES.0) =>
            {
                self.exhausted = true;
                Ok(false)
            }
            Err(error) => Err(io::Error::from_raw_os_error(error.code().0 & 0xffff)),
        }
    }
}

#[cfg(windows)]
pub(super) fn decode_host_directory_record(
    buffer: &[u64],
    offset: usize,
) -> io::Result<(Option<usize>, &[u16])> {
    use std::mem::{align_of, offset_of, size_of};
    use windows::Win32::Storage::FileSystem::FILE_ID_EXTD_DIR_INFO;
    let malformed = || io::Error::new(io::ErrorKind::InvalidData, "malformed directory record");
    let total = buffer.len().checked_mul(size_of::<u64>()).ok_or_else(malformed)?;
    let header = offset_of!(FILE_ID_EXTD_DIR_INFO, FileName);
    if !offset.is_multiple_of(align_of::<FILE_ID_EXTD_DIR_INFO>())
        || offset.checked_add(header).is_none_or(|end| end > total)
    {
        return Err(malformed());
    }
    // SAFETY: only the two checked fixed-header fields are read.
    let record = unsafe { buffer.as_ptr().cast::<u8>().add(offset).cast::<FILE_ID_EXTD_DIR_INFO>() };
    let (name_bytes, step) = unsafe {
        (
            std::ptr::addr_of!((*record).FileNameLength).read_unaligned(),
            std::ptr::addr_of!((*record).NextEntryOffset).read_unaligned(),
        )
    };
    let name_bytes = usize::try_from(name_bytes).map_err(|_| malformed())?;
    let step = usize::try_from(step).map_err(|_| malformed())?;
    let next = if step == 0 {
        None
    } else {
        let next = offset.checked_add(step).ok_or_else(malformed)?;
        if !step.is_multiple_of(align_of::<FILE_ID_EXTD_DIR_INFO>()) || next >= total {
            return Err(malformed());
        }
        Some(next)
    };
    let end = next.unwrap_or(total);
    let start = offset.checked_add(header).ok_or_else(malformed)?;
    if name_bytes == 0 || !name_bytes.is_multiple_of(size_of::<u16>())
        || start.checked_add(name_bytes).is_none_or(|name_end| name_end > end)
    {
        return Err(malformed());
    }
    // SAFETY: every UTF-16 unit is initialized owned storage inside this record, not merely somewhere in a following record's buffer.
    let name = unsafe {
        std::slice::from_raw_parts(buffer.as_ptr().cast::<u8>().add(start).cast::<u16>(), name_bytes / 2)
    };
    Ok((next, name))
}

#[cfg(windows)]
impl HostStatReader {
    pub(crate) fn next_wide<W: NativeHostWork>(
        &mut self, work: &mut W,
    ) -> Result<Option<(HostListedWideEntry, W::Allocation)>, W::Error> {
        loop {
            work.check()?;
            let Some(offset) = self.next else {
                if self.exhausted { return Ok(None); }
                native_read(work)?;
                if !self.refill()? { return Ok(None); }
                continue;
            };
            let item = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
            work.admit(item)?;
            work.record(item)?;
            let (next, name) = decode_host_directory_record(&self.buffer, offset)?;
            let dot = u16::from(b'.');
            if name == [dot] || name == [dot, dot] { self.next = next; continue; }
            let bytes = name.len() as u64 * 2;
            let delta = WorkCounters { bytes_copied: bytes, ..WorkCounters::UNCHARGED };
            work.admit(delta)?;
            let (mut copied, allocation) = buffer(name.len(), work)?;
            copied.extend_from_slice(name);
            let name = copied;
            let stat = match (|| {
                work.record(delta)?;
                Ok(windows_wide_name(&name, work,
                    || io::Error::new(io::ErrorKind::InvalidInput, "native name exceeds NT length").into(),
                    |string, work| stat_windows_name(&self.directory, string, self.volume_serial_number, work))?
                    .flatten())
            })() {
                Ok(stat) => stat,
                Err(error) if W::io_error_kind(&error) == Some(io::ErrorKind::NotFound) => None,
                Err(error) => {
                    drop(name);
                    work.release(allocation)?;
                    if W::io_error_kind(&error).is_some() { self.exhausted = true; self.next = None; }
                    return Err(error);
                }
            };
            self.next = next;
            return Ok(Some((HostListedWideEntry { name, stat }, allocation)));
        }
    }
}

impl HostStatReader {
    const BUFFER_BYTES: usize = 64 * 1024;
    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        (self.buffer.len() * std::mem::size_of::<u64>()) as u64
    }
}

pub(crate) fn read_dir_held<W: NativeHostWork>(
    directory: &Dir, work: &mut W,
) -> Result<(HostStatReader, W::Allocation), W::Error> {
    #[cfg(unix)]
    {
        let directory = Dir::from_std_file(open_unix(directory.as_raw_fd(), c".",
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_DIRECTORY
                | { #[cfg(target_os = "linux")] { libc::O_NOATIME } #[cfg(not(target_os = "linux"))] { 0 } },
            || native_read(work))?);
        let (mut buffer, allocation) = buffer::<u64, _>(HostStatReader::BUFFER_BYTES / 8, work)?;
        buffer.resize(HostStatReader::BUFFER_BYTES / 8, 0);
        Ok((HostStatReader {
            directory, buffer, next: 0, filled: 0, exhausted: false,
            #[cfg(target_os = "macos")]
            base: 0,
        }, allocation))
    }
    #[cfg(windows)]
    {
        let volume = u32::try_from(directory_identity(directory, work)?.device)
            .map_err(|_| io::Error::other("native volume serial exceeds u32"))?;
        read_dir_held_with_volume(directory, volume, work)
    }
}

#[cfg(windows)]
pub(super) fn read_dir_held_with_volume<W: NativeHostWork>(directory: &Dir, volume_serial_number: u32, work: &mut W)
    -> Result<(HostStatReader, W::Allocation), W::Error> {
    use windows::Wdk::Storage::FileSystem::{FILE_DIRECTORY_FILE, FILE_SYNCHRONOUS_IO_NONALERT};
    use windows::Win32::Storage::FileSystem::FILE_GENERIC_READ;
    let file = open_windows_wide_at(directory, &[46], FILE_GENERIC_READ,
        FILE_DIRECTORY_FILE | FILE_SYNCHRONOUS_IO_NONALERT, work, W::invalid_request)?
        .ok_or_else(W::invalid_request)?;
    let directory = Dir::from_std_file(file.into_std());
    let (mut buffer, allocation) = buffer::<u64, _>(HostStatReader::BUFFER_BYTES / 8, work)?;
    buffer.resize(HostStatReader::BUFFER_BYTES / 8, 0);
    Ok((HostStatReader { directory, volume_serial_number, buffer, next: None, exhausted: false }, allocation))
}

#[cfg(unix)]
impl HostStatReader {
    pub(crate) fn next<W: NativeHostWork>(
        &mut self, work: &mut W,
    ) -> Result<Option<(HostListedEntry, W::Allocation)>, W::Error> {
        use std::os::unix::ffi::{OsStrExt as _, OsStringExt as _};
        loop {
            work.check()?;
            if self.next == self.filled {
                if self.exhausted { return Ok(None); }
                native_read(work)?;
                // SAFETY: the descriptor and aligned owned buffer are live; neither platform call writes more than its specified length.
                #[cfg(target_os = "linux")]
                let count = unsafe {
                    libc::syscall(libc::SYS_getdents64, self.directory.as_raw_fd(),
                        self.buffer.as_mut_ptr(), Self::BUFFER_BYTES)
                } as isize;
                #[cfg(target_os = "macos")]
                let count = unsafe {
                    unsafe extern "C" {
                        fn getdirentries64(fd: libc::c_int, buf: *mut libc::c_char, size: libc::size_t,
                            base: *mut libc::off_t) -> libc::ssize_t;
                    }
                    getdirentries64(self.directory.as_raw_fd(), self.buffer.as_mut_ptr().cast(),
                        Self::BUFFER_BYTES, &raw mut self.base)
                };
                if count < 0 {
                    let error = io::Error::last_os_error();
                    if error.kind() == io::ErrorKind::Interrupted { continue; }
                    return Err(error.into());
                }
                self.next = 0;
                self.filled = usize::try_from(count).map_err(|_| io::Error::other("negative directory refill size"))?;
                if self.filled == 0 { self.exhausted = true; return Ok(None); }
            }
            let item = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
            work.admit(item)?;
            work.record(item)?;
            let bytes = unsafe {
                std::slice::from_raw_parts(self.buffer.as_ptr().cast::<u8>(), self.filled)
            };
            #[cfg(target_os = "linux")]
            let (length_offset, name_offset) = (16, 19);
            #[cfg(target_os = "macos")]
            let (length_offset, name_offset) = (16, 21);
            let malformed = || io::Error::new(io::ErrorKind::InvalidData, "malformed directory record");
            let header = bytes.get(self.next..self.next + name_offset).ok_or_else(malformed)?;
            let length = usize::from(u16::from_ne_bytes([header[length_offset], header[length_offset + 1]]));
            if length <= name_offset { return Err(malformed().into()); }
            let end = self.next.checked_add(length).ok_or_else(malformed)?;
            let tail = bytes.get(self.next + name_offset..end).ok_or_else(malformed)?;
            let name_length = tail.iter().position(|byte| *byte == 0).ok_or_else(malformed)?;
            let name = &tail[..name_length];
            if name == b"." || name == b".." { self.next = end; continue; }
            if name.is_empty() || name.contains(&b'/') { return Err(malformed().into()); }
            let delta = WorkCounters { bytes_copied: name.len() as u64, ..WorkCounters::UNCHARGED };
            work.admit(delta)?;
            let (mut copied, allocation) = buffer(name.len(), work)?;
            copied.extend_from_slice(name);
            let name = OsString::from_vec(copied);
            let stat = match (|| {
                work.record(delta)?;
                stat_leaf(&self.directory, OsStr::from_bytes(name.as_bytes()), work)
            })() {
                Ok(stat) => Some(stat),
                Err(error) if W::io_error_kind(&error) == Some(io::ErrorKind::NotFound) => None,
                Err(error) => {
                    drop(name);
                    work.release(allocation)?;
                    if W::io_error_kind(&error).is_some() { self.next = end; }
                    return Err(error);
                }
            };
            self.next = end;
            return Ok(Some((HostListedEntry { name, stat }, allocation)));
        }
    }
}

#[cfg(all(test, windows))]
#[test]
pub(super) fn directory_record_decoder_checks_each_flexible_record_boundary() {
    use std::mem::{offset_of, size_of};
    use windows::Win32::Storage::FileSystem::FILE_ID_EXTD_DIR_INFO;
    let header = offset_of!(FILE_ID_EXTD_DIR_INFO, FileName);
    let stride = (header + size_of::<u16>()).next_multiple_of(size_of::<u64>());
    let mut buffer = vec![0_u64; stride * 2 / size_of::<u64>()];
    let write_record = |buffer: &mut [u64], offset: usize, next: u32, name_bytes: u32| {
        // SAFETY: these tests allocate both complete fixed headers; only the fixed fields and one UTF-16 name unit are written, without a struct reference or an unchecked flexible-tail access.
        unsafe {
            let bytes = buffer.as_mut_ptr().cast::<u8>().add(offset);
            let record = bytes.cast::<FILE_ID_EXTD_DIR_INFO>();
            std::ptr::addr_of_mut!((*record).NextEntryOffset).write_unaligned(next);
            std::ptr::addr_of_mut!((*record).FileNameLength).write_unaligned(name_bytes);
            bytes.add(header).cast::<u16>().write_unaligned(u16::from(b'a'));
        }
    };
    write_record(&mut buffer, 0, u32::try_from(stride).expect("small record"), 2);
    write_record(&mut buffer, stride, 0, 2);
    let (next, name) = decode_host_directory_record(&buffer, 0).expect("first record");
    assert_eq!(next, Some(stride));
    assert_eq!(name, [u16::from(b'a')]);
    assert_eq!(decode_host_directory_record(&buffer, stride).expect("last record").0, None);
    for (next, name_bytes) in [
        (8, 2), (u32::MAX, 2), (u32::try_from(stride * 2).expect("small buffer"), 2),
        (u32::try_from(stride).expect("small record"), 0),
        (u32::try_from(stride).expect("small record"), 3),
        (u32::try_from(stride).expect("small record"), u32::MAX),
        (u32::try_from(stride).expect("small record"), u32::try_from(stride - header + 2).expect("short name")),
    ] {
        write_record(&mut buffer, 0, next, name_bytes);
        assert!(decode_host_directory_record(&buffer, 0).is_err(), "{next}/{name_bytes}");
    }
    assert!(decode_host_directory_record(&buffer, usize::MAX).is_err());
    assert!(decode_host_directory_record(&buffer, 1).is_err());
    write_record(&mut buffer, 0, 0, 2);
}
