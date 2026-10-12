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
