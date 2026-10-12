//! Positional I/O on a held Windows handle.
use crate::{
    NativeByteReservation, NativeOperationKind, NativeOperationReservation, NativeReadMeter,
    OwnedRead, OwnedWrite,
};
use bytes::Bytes;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use std::{fs::File, io, mem::zeroed};
use windows_sys::{
    Wdk::Storage::FileSystem::{
        FILE_MODE_INFORMATION, FILE_SYNCHRONOUS_IO_ALERT, FILE_SYNCHRONOUS_IO_NONALERT,
        FileModeInformation, NtQueryInformationFile,
    },
    Win32::{
        Foundation::{
            ERROR_HANDLE_EOF, ERROR_IO_PENDING, GetLastError, HANDLE, INVALID_HANDLE_VALUE,
        },
        Storage::FileSystem::{
            FILE_END_OF_FILE_INFO, FILE_FLAG_OVERLAPPED, FILE_GENERIC_READ, FILE_GENERIC_WRITE,
            FILE_ID_INFO, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, FileEndOfFileInfo,
            FileIdInfo, FlushFileBuffers, GetFileInformationByHandleEx, ReOpenFile, ReadFile,
            SetFileInformationByHandle, WriteFile,
        },
        System::{
            IO::{DeviceIoControl, GetOverlappedResult, IO_STATUS_BLOCK, OVERLAPPED},
            Threading::CreateEventW,
        },
    },
};

fn create_event() -> Result<OwnedHandle, i32> {
    // SAFETY: unnamed manual-reset event with default security.
    let event = unsafe { CreateEventW(std::ptr::null(), 1, 0, std::ptr::null()) };
    if event.is_null() {
        Err(io::Error::last_os_error().raw_os_error().unwrap_or(-1))
    } else {
        // SAFETY: successful creation transfers sole ownership of the event.
        Ok(unsafe { OwnedHandle::from_raw_handle(event) })
    }
}
thread_local! {
    static IN_PLACE_EVENT: Result<OwnedHandle, i32> = create_event();
}

fn transfer_in_place(
    file: &impl AsRawHandle,
    offset: u64,
    issue: impl FnOnce(HANDLE, *mut OVERLAPPED) -> i32,
    meter: Option<(&dyn NativeReadMeter, NativeOperationKind, Option<usize>)>,
) -> io::Result<usize> {
    let mut operations = meter
        .map(|(meter, kind, _)| NativeOperationReservation::new(meter, kind, 2))
        .transpose()?;
    IN_PLACE_EVENT.with(|event| {
        let event = event
            .as_ref()
            .map_err(|code| io::Error::from_raw_os_error(*code))?;
        let handle = file.as_raw_handle();
        // SAFETY: zero is the documented initial OVERLAPPED state.
        let mut overlapped: OVERLAPPED = unsafe { zeroed() };
        overlapped.Anonymous.Anonymous.Offset = offset as u32;
        overlapped.Anonymous.Anonymous.OffsetHigh = (offset >> 32) as u32;
        // The low bit excludes this synchronous wait from completion-port delivery.
        overlapped.hEvent = event.as_raw_handle().map_addr(|address| address | 1);
        let mut bytes = match meter {
            Some((meter, kind, Some(maximum))) => {
                Some(NativeByteReservation::new(meter, kind, maximum)?)
            }
            Some((meter, kind, None)) => {
                match kind {
                    NativeOperationKind::Read => meter.admit_bytes(0)?,
                    NativeOperationKind::Write => meter.admit_write_bytes(0)?,
                    NativeOperationKind::Durability => {}
                }
                None
            }
            None => None,
        };
        let issue = || {
            let issued = issue(handle, &raw mut overlapped);
            // Capture the OS error before a meter callback can change last-error state.
            (issued, (issued == 0).then(|| unsafe { GetLastError() }))
        };
        let ((_, error), issue_charge) = match operations.as_mut() {
            Some(operations) => operations.attempt(issue),
            None => (issue(), Ok(())),
        };
        match error {
            Some(ERROR_IO_PENDING) | None => {}
            Some(ERROR_HANDLE_EOF) => {
                issue_charge?;
                return Ok(0);
            }
            Some(code) => {
                issue_charge?;
                return Err(io::Error::from_raw_os_error(code as i32));
            }
        }
        let mut transferred = 0;
        let mut complete = || {
            // SAFETY: the handle, OVERLAPPED and payload remain live until this wait settles.
            if unsafe {
                GetOverlappedResult(handle, &raw const overlapped, &raw mut transferred, 1)
            } == 0
            {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        };
        // Completion uses allowance reserved before issue, even after cancellation.
        let (completed, completion_charge) = match operations.as_mut() {
            Some(operations) => operations.attempt(complete),
            None => (complete(), Ok(())),
        };
        if let Some(bytes) = bytes.as_mut() {
            bytes.record(transferred as usize)?;
        }
        issue_charge?;
        completion_charge?;
        match completed {
            Err(error) if error.raw_os_error() != Some(ERROR_HANDLE_EOF as i32) => Err(error),
            _ => Ok(transferred as usize),
        }
    })
}

pub(crate) fn control_in_place(
    file: &impl AsRawHandle,
    code: u32,
    input: &[u8],
    output: &mut [u8],
    meter: Option<(&dyn NativeReadMeter, NativeOperationKind)>,
) -> io::Result<usize> {
    let input_length = u32::try_from(input.len()).map_err(|_| io::ErrorKind::InvalidInput)?;
    let output_length = u32::try_from(output.len()).map_err(|_| io::ErrorKind::InvalidInput)?;
    transfer_in_place(
        file,
        0,
        |handle, overlapped| {
            // SAFETY: both borrowed buffers remain live through terminal completion.
            unsafe {
                DeviceIoControl(
                    handle,
                    code,
                    input.as_ptr().cast(),
                    input_length,
                    output.as_mut_ptr().cast(),
                    output_length,
                    std::ptr::null_mut(),
                    overlapped,
                )
            }
        },
        meter.map(|(meter, kind)| (meter, kind, None)),
    )
}

pub(super) fn overlapped_range(offset: u64, length: usize) -> io::Result<u32> {
    let length = u32::try_from(length).map_err(|_| io::ErrorKind::InvalidInput)?;
    if offset == u64::MAX {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    offset
        .checked_add(u64::from(length))
        .ok_or(io::ErrorKind::InvalidInput)?;
    Ok(length)
}

pub(super) fn is_overlapped(file: &File) -> bool {
    let mut status = IO_STATUS_BLOCK::default();
    let mut mode = FILE_MODE_INFORMATION::default();
    // SAFETY: the live handle and initialized output buffers outlive the mode query.
    (unsafe {
        NtQueryInformationFile(
            file.as_raw_handle(),
            &raw mut status,
            (&raw mut mode).cast(),
            std::mem::size_of::<FILE_MODE_INFORMATION>() as u32,
            FileModeInformation,
        )
    }) >= 0
        && mode.Mode & (FILE_SYNCHRONOUS_IO_ALERT | FILE_SYNCHRONOUS_IO_NONALERT) == 0
}

pub(super) fn reopen_overlapped(file: &File, access: u32) -> io::Result<File> {
    // SAFETY: reopens the same file object without path traversal.
    let handle = unsafe {
        ReOpenFile(
            file.as_raw_handle(),
            access,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            FILE_FLAG_OVERLAPPED,
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a successful reopen returns one owned handle.
    Ok(unsafe { File::from_raw_handle(handle) })
}

pub(crate) fn with_overlapped<T>(
    file: &File,
    access: u32,
    known_overlapped: bool,
    meter: Option<&dyn NativeReadMeter>,
    transfer: impl FnOnce(&File) -> io::Result<T>,
) -> io::Result<T> {
    if known_overlapped {
        return transfer(file);
    }
    if let Some(meter) = meter {
        meter.admit_read(0)?;
    }
    if is_overlapped(file) {
        transfer(file)
    } else {
        if let Some(meter) = meter {
            meter.admit_read(0)?;
        }
        transfer(&reopen_overlapped(file, access)?)
    }
}

fn read_once(
    file: &File,
    offset: u64,
    destination: &mut [u8],
    meter: Option<&dyn NativeReadMeter>,
) -> io::Result<usize> {
    let length = overlapped_range(offset, destination.len())?;
    if length == 0 {
        return Ok(0);
    }
    if let Some(meter) = meter {
        meter.admit_bytes(destination.len())?;
    }
    let count = transfer_in_place(
        file,
        offset,
        |handle, overlapped| {
            // SAFETY: destination is writable for length bytes until the request settles.
            unsafe {
                ReadFile(
                    handle,
                    destination.as_mut_ptr(),
                    length,
                    std::ptr::null_mut(),
                    overlapped,
                )
            }
        },
        meter.map(|meter| (meter, NativeOperationKind::Read, Some(length as usize))),
    )?;
    if count > destination.len() {
        return Err(io::Error::other("read completion exceeded buffer"));
    }
    Ok(count)
}

pub(crate) fn read_at(file: &File, offset: u64, destination: &mut [u8]) -> io::Result<usize> {
    overlapped_range(offset, destination.len())?;
    if destination.is_empty() {
        return Ok(0);
    }
    with_overlapped(file, FILE_GENERIC_READ, false, None, |file| {
        read_once(file, offset, destination, None)
    })
}

pub(crate) fn read_at_known_overlapped(
    file: &File,
    offset: u64,
    destination: &mut [u8],
    meter: &dyn NativeReadMeter,
) -> io::Result<usize> {
    read_once(file, offset, destination, Some(meter))
}

pub(crate) fn read_batch_in_place(file: &File, reads: &[OwnedRead]) -> io::Result<Vec<Bytes>> {
    let mut result = Vec::new();
    result
        .try_reserve_exact(reads.len())
        .map_err(io::Error::other)?;
    for read in reads {
        overlapped_range(read.offset, read.length)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(read.length)
            .map_err(io::Error::other)?;
        let length = overlapped_range(read.offset, read.length)?;
        let count = if length == 0 {
            0
        } else {
            transfer_in_place(
                file,
                read.offset,
                |handle, overlapped| {
                    // SAFETY: spare capacity holds length writable bytes until terminal completion.
                    unsafe {
                        ReadFile(
                            handle,
                            bytes.as_mut_ptr(),
                            length,
                            std::ptr::null_mut(),
                            overlapped,
                        )
                    }
                },
                None,
            )?
        };
        if count > read.length {
            return Err(io::Error::other("read completion exceeded buffer"));
        }
        // SAFETY: the terminal read initialized count bytes in this buffer.
        unsafe {
            bytes.set_len(count);
        }
        result.push(Bytes::from(bytes));
    }
    Ok(result)
}

pub(crate) fn write_at_known_overlapped(
    file: &File,
    offset: u64,
    bytes: &[u8],
    meter: &dyn NativeReadMeter,
) -> io::Result<usize> {
    let length = overlapped_range(offset, bytes.len())?;
    if length == 0 {
        return Ok(0);
    }
    transfer_in_place(
        file,
        offset,
        |handle, overlapped| {
            // SAFETY: bytes and OVERLAPPED remain live through terminal completion.
            unsafe {
                WriteFile(
                    handle,
                    bytes.as_ptr(),
                    length,
                    std::ptr::null_mut(),
                    overlapped,
                )
            }
        },
        Some((meter, NativeOperationKind::Write, Some(bytes.len()))),
    )
}

fn write_all_in_place(file: &File, mut offset: u64, mut bytes: &[u8]) -> io::Result<()> {
    while !bytes.is_empty() {
        let length = overlapped_range(offset, bytes.len())?;
        let count = transfer_in_place(
            file,
            offset,
            |handle, overlapped| {
                // SAFETY: the borrowed suffix remains live through terminal completion.
                unsafe {
                    WriteFile(
                        handle,
                        bytes.as_ptr(),
                        length,
                        std::ptr::null_mut(),
                        overlapped,
                    )
                }
            },
            None,
        )?;
        if count == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        if count > bytes.len() {
            return Err(io::Error::other("write completion exceeded buffer"));
        }
        offset += count as u64;
        bytes = &bytes[count..];
    }
    Ok(())
}

pub(crate) fn write_batch_in_place(file: &File, writes: &[OwnedWrite]) -> io::Result<()> {
    writes
        .iter()
        .try_for_each(|write| write_all_in_place(file, write.offset, &write.bytes))
}

pub(crate) fn write_all_at(file: &File, offset: u64, bytes: &[u8]) -> io::Result<()> {
    overlapped_range(offset, bytes.len())?;
    with_overlapped(file, FILE_GENERIC_WRITE, false, None, |file| {
        write_all_in_place(file, offset, bytes)
    })
}

pub(crate) fn set_len_once(file: &File, size: u64) -> io::Result<()> {
    let info = FILE_END_OF_FILE_INFO {
        EndOfFile: i64::try_from(size).map_err(|_| io::ErrorKind::InvalidInput)?,
    };
    // SAFETY: the initialized buffer and held handle remain live through the syscall.
    if unsafe {
        SetFileInformationByHandle(
            file.as_raw_handle(),
            FileEndOfFileInfo,
            (&raw const info).cast(),
            std::mem::size_of::<FILE_END_OF_FILE_INFO>() as u32,
        )
    } == 0
    {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

pub(crate) fn sync_once(file: &File) -> io::Result<()> {
    // SAFETY: this one syscall acts only on the held handle.
    if unsafe { FlushFileBuffers(file.as_raw_handle()) } == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

pub(crate) fn file_identity(file: &File) -> io::Result<crate::FileIdentity> {
    // SAFETY: FILE_ID_INFO is a POD output initialized to zero.
    let mut info: FILE_ID_INFO = unsafe { zeroed() };
    // SAFETY: the handle and initialized FILE_ID_INFO output remain live through the syscall.
    if unsafe {
        GetFileInformationByHandleEx(
            file.as_raw_handle(),
            FileIdInfo,
            (&raw mut info).cast(),
            std::mem::size_of::<FILE_ID_INFO>() as u32,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok((info.VolumeSerialNumber, info.FileId.Identifier))
}
