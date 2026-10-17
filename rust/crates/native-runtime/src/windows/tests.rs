use super::driver::{submit_read, submit_write};
use super::io::*;
use crate::{NativeFilePayload, OwnedRead, OwnedWrite};
use bytes::Bytes;
use std::os::windows::fs::OpenOptionsExt as _;
use std::{
    fs::OpenOptions,
    io::{self, Seek as _, SeekFrom, Write as _},
    sync::{Arc, mpsc},
    time::Duration,
};
use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OVERLAPPED;

#[test]
fn borrowed_positional_io_uses_an_overlapped_handle_directly() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let open = |name, flags| {
        OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .custom_flags(flags)
            .open(directory.path().join(name))
    };
    let synchronous = open("synchronous", 0)?;
    let overlapped = open("overlapped", FILE_FLAG_OVERLAPPED)?;
    assert!(!is_overlapped(&synchronous));
    assert!(is_overlapped(&overlapped));
    for file in [&synchronous, &overlapped] {
        write_all_at(file, 3, b"positional")?;
        let mut bytes = [0; 8];
        assert_eq!(read_at(file, 5, &mut bytes)?, 8);
        assert_eq!(&bytes, b"sitional");
        assert_eq!(read_at(file, 13, &mut bytes)?, 0);
    }
    assert_eq!((&synchronous).stream_position()?, 0);
    Ok(())
}

#[test]
fn positional_batches_complete_without_changing_source_cursor() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(directory.path().join("positional"))?;
    file.write_all(b"abcdefghijklmnop")?;
    file.seek(SeekFrom::Start(13))?;
    let file = Arc::new(NativeFilePayload {
        file,
        _allocation: None,
    });
    let (sender, receiver) = mpsc::sync_channel(1);
    submit_write(
        Arc::clone(&file),
        false,
        vec![OwnedWrite {
            offset: 4,
            bytes: Bytes::from_static(b"WXYZ"),
        }],
        Box::new(move |result| {
            let _ = sender.send(result);
        }),
    )?;
    receiver
        .recv_timeout(Duration::from_secs(5))
        .map_err(io::Error::other)??;
    let requests = vec![
        OwnedRead {
            offset: 2,
            length: 2,
        },
        OwnedRead {
            offset: 4,
            length: 0,
        },
        OwnedRead {
            offset: 4,
            length: 6,
        },
        OwnedRead {
            offset: 14,
            length: 8,
        },
    ];
    let (sender, receiver) = mpsc::sync_channel(1);
    submit_read(
        Arc::clone(&file),
        false,
        requests,
        Box::new(move |result| {
            let _ = sender.send(result);
        }),
    )?;
    let result = receiver
        .recv_timeout(Duration::from_secs(5))
        .map_err(io::Error::other)??;
    assert_eq!(
        result,
        [
            Bytes::from_static(b"cd"),
            Bytes::new(),
            Bytes::from_static(b"WXYZij"),
            Bytes::from_static(b"op"),
        ]
    );
    assert_eq!((&**file).stream_position()?, 13);
    Ok(())
}

#[test]
fn input_handles_classify_by_how_they_can_be_awaited() -> io::Result<()> {
    use super::terminal::{InputKind, input_kind};
    use std::os::windows::io::{FromRawHandle as _, OwnedHandle};
    use windows_sys::Win32::Foundation::{HANDLE, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::Storage::FileSystem::PIPE_ACCESS_OUTBOUND;
    use windows_sys::Win32::System::Pipes::{CreateNamedPipeW, CreatePipe, PIPE_TYPE_BYTE};
    let directory = tempfile::tempdir()?;
    // A file may sit on a slow share and a character device may be a serial
    // port: neither is read on the runtime.
    let disk = std::fs::File::create(directory.path().join("disk"))?;
    assert_eq!(input_kind(&disk), InputKind::Blocking);
    assert_eq!(
        input_kind(&OpenOptions::new().read(true).open("NUL")?),
        InputKind::Blocking
    );
    let (mut read, mut write): (HANDLE, HANDLE) = (std::ptr::null_mut(), std::ptr::null_mut());
    // SAFETY: both outputs are valid for writes; the handles are owned below.
    if unsafe { CreatePipe(&raw mut read, &raw mut write, std::ptr::null(), 0) } == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: CreatePipe returned two fresh handles that nothing else owns.
    let (read, _write) = unsafe {
        (
            std::fs::File::from_raw_handle(read),
            OwnedHandle::from_raw_handle(write),
        )
    };
    assert_eq!(input_kind(&read), InputKind::Blocking);
    for (flags, kind) in [
        (0, InputKind::Blocking),
        (FILE_FLAG_OVERLAPPED, InputKind::Overlapped),
    ] {
        let name = format!(
            r"\\.\pipe\acyclic-input-kind-{}-{flags}",
            std::process::id()
        );
        let wide: Vec<u16> = name.encode_utf16().chain([0]).collect();
        // SAFETY: the name is NUL-terminated and outlives the call.
        let server = unsafe {
            CreateNamedPipeW(
                wide.as_ptr(),
                PIPE_ACCESS_OUTBOUND,
                PIPE_TYPE_BYTE,
                1,
                0,
                0,
                0,
                std::ptr::null(),
            )
        };
        if server == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: a successful create returns one handle that nothing else owns.
        let _server = unsafe { OwnedHandle::from_raw_handle(server) };
        let client = OpenOptions::new()
            .read(true)
            .custom_flags(flags)
            .open(&name)?;
        assert_eq!(input_kind(&client), kind);
    }
    Ok(())
}
