use std::{fs, io, os::windows::fs::OpenOptionsExt, os::windows::io::AsRawHandle, path::Path};
use windows_sys::Win32::{
    Foundation::{ERROR_IO_INCOMPLETE, ERROR_IO_PENDING},
    Storage::FileSystem::FILE_FLAG_OVERLAPPED,
    System::{
        Ioctl::{
            FSCTL_REQUEST_OPLOCK, OPLOCK_LEVEL_CACHE_READ, REQUEST_OPLOCK_CURRENT_VERSION,
            REQUEST_OPLOCK_INPUT_BUFFER, REQUEST_OPLOCK_INPUT_FLAG_REQUEST,
            REQUEST_OPLOCK_OUTPUT_BUFFER,
        },
        IO::{CancelIoEx, DeviceIoControl, GetOverlappedResult, OVERLAPPED},
    },
};

// A read-only oplock completes before an incompatible write can proceed and
// requires no acknowledgment. This handle owns no other overlapped operations.
pub(crate) struct ReadLease {
    file: fs::File,
    request: *mut Request,
}

struct Request {
    overlapped: OVERLAPPED,
    input: REQUEST_OPLOCK_INPUT_BUFFER,
    output: REQUEST_OPLOCK_OUTPUT_BUFFER,
}

impl ReadLease {
    #[allow(unsafe_code, reason = "Win32 acquires an oplock with overlapped I/O")]
    pub(crate) fn request(path: &Path) -> Option<Self> {
        let file = fs::OpenOptions::new()
            .read(true)
            .custom_flags(FILE_FLAG_OVERLAPPED)
            .open(path)
            .ok()?;
        let version = u16::try_from(REQUEST_OPLOCK_CURRENT_VERSION).ok()?;
        let input_length = u16::try_from(size_of::<REQUEST_OPLOCK_INPUT_BUFFER>()).ok()?;
        let output_length = u32::try_from(size_of::<REQUEST_OPLOCK_OUTPUT_BUFFER>()).ok()?;
        let request = Box::into_raw(Box::new(Request {
            overlapped: OVERLAPPED::default(),
            input: REQUEST_OPLOCK_INPUT_BUFFER {
                StructureVersion: version,
                StructureLength: input_length,
                RequestedOplockLevel: OPLOCK_LEVEL_CACHE_READ,
                Flags: REQUEST_OPLOCK_INPUT_FLAG_REQUEST,
            },
            output: REQUEST_OPLOCK_OUTPUT_BUFFER::default(),
        }));
        // SAFETY: the request stays at the same address until its sole I/O operation
        // completes. The file supplies the completion signal for that operation.
        let result = unsafe {
            DeviceIoControl(
                file.as_raw_handle(),
                FSCTL_REQUEST_OPLOCK,
                (&raw const (*request).input).cast(),
                u32::from(input_length),
                (&raw mut (*request).output).cast(),
                output_length,
                std::ptr::null_mut(),
                &raw mut (*request).overlapped,
            )
        };
        if result == 0 && last_error_is(ERROR_IO_PENDING) {
            Some(Self { file, request })
        } else {
            // SAFETY: there is no outstanding operation referring to this storage.
            drop(unsafe { Box::from_raw(request) });
            None
        }
    }

    #[allow(unsafe_code, reason = "Win32 reports the read oplock's completion")]
    pub(crate) fn pending(&self) -> bool {
        let mut transferred = 0;
        // SAFETY: the owned handle and request remain live throughout the call.
        let result = unsafe {
            GetOverlappedResult(
                self.file.as_raw_handle(),
                &raw const (*self.request).overlapped,
                &raw mut transferred,
                0,
            )
        };
        result == 0 && last_error_is(ERROR_IO_INCOMPLETE)
    }
}

impl Drop for ReadLease {
    #[allow(
        unsafe_code,
        reason = "Win32 must retire the oplock before freeing its buffer"
    )]
    fn drop(&mut self) {
        let handle = self.file.as_raw_handle();
        let mut transferred = 0;
        // SAFETY: the request was submitted on this handle. Cancellation can race with
        // completion, so wait for the terminal result before freeing the request.
        unsafe {
            let overlapped = &raw const (*self.request).overlapped;
            CancelIoEx(handle, overlapped);
            GetOverlappedResult(handle, overlapped, &raw mut transferred, 1);
            drop(Box::from_raw(self.request));
        }
    }
}

fn last_error_is(expected: u32) -> bool {
    io::Error::last_os_error()
        .raw_os_error()
        .map(i32::cast_unsigned)
        == Some(expected)
}
