use super::{Backend, File, InheritedOutput, Synchronous, io};
use std::os::windows::io::AsRawHandle as _;
use windows_sys::Win32::{
    Foundation::{ERROR_SUCCESS, GetLastError, SetLastError},
    Storage::FileSystem::{FILE_TYPE_CHAR, FILE_TYPE_UNKNOWN, GetFileType},
    System::Console::{GetConsoleMode, WriteConsoleW},
};

pub(super) fn admit(file: File) -> io::Result<InheritedOutput> {
    // SAFETY: both queries borrow the original live handle. They submit no IO,
    // change no console mode and retain no pointer after returning.
    #[expect(unsafe_code, reason = "classifies an owned inherited handle")]
    let console = unsafe {
        SetLastError(ERROR_SUCCESS);
        let kind = GetFileType(file.as_raw_handle());
        if kind == FILE_TYPE_UNKNOWN {
            let error = GetLastError();
            return Err(if error == ERROR_SUCCESS {
                io::Error::new(
                    io::ErrorKind::Unsupported,
                    "unknown inherited output handle",
                )
            } else {
                io::Error::from_raw_os_error(error as i32)
            });
        }
        let mut mode = 0;
        kind == FILE_TYPE_CHAR && GetConsoleMode(file.as_raw_handle(), &mut mode) != 0
    };
    if !console && crate::windows::is_overlapped(&file)? {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "inherited output requires a synchronous handle; overlapped handles retain their original IOCP owner",
        ));
    }
    Ok(InheritedOutput {
        backend: Backend::Synchronous(Synchronous { file, console }),
    })
}

pub(super) fn write_console(file: &File, bytes: &[u8]) -> io::Result<()> {
    // A real console accepts UTF-16, independently of its global code page.
    // Redirected files/pipes preserve the caller's bytes instead. Validate
    // before emitting any part of an invalid console record.
    let text = std::str::from_utf8(bytes)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let mut characters = text.chars().peekable();
    let mut encoded = [0u16; 1024];
    loop {
        let mut length = 0;
        while let Some(character) = characters.peek() {
            let width = character.len_utf16();
            if length + width > encoded.len() {
                break;
            }
            character.encode_utf16(&mut encoded[length..length + width]);
            length += width;
            characters.next();
        }
        if length == 0 {
            return Ok(());
        }
        let mut offset = 0;
        while offset < length {
            let mut written = 0;
            // SAFETY: this synchronous call borrows a live owned handle and
            // the initialized UTF-16 prefix on this same worker's stack. The
            // original UTF-8 buffer and its credit remain owned until return.
            #[expect(unsafe_code, reason = "writes Unicode to the original console")]
            let result = unsafe {
                WriteConsoleW(
                    file.as_raw_handle(),
                    encoded[offset..length].as_ptr().cast(),
                    (length - offset) as u32,
                    &mut written,
                    std::ptr::null_mut(),
                )
            };
            if result == 0 {
                return Err(io::Error::last_os_error());
            }
            if written == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            if written as usize > length - offset {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "console returned an invalid completion length",
                ));
            }
            offset += written as usize;
        }
    }
}
