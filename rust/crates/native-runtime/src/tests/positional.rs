use super::*;

#[test]
fn positional_io_preserves_cursor_and_bounds_ranges() -> io::Result<()> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("positional");
    let file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(path)?;
    write_all_at(&file, 2, &[0x22; 3], None)?;
    write_all_at(&file, 16, &[0x33; 5], None)?;
    sync_file(&file, Durability::Full, None)?;

    let mut bytes = [0_u8; 24];
    assert_eq!(read_at(&file, 0, &mut bytes)?, 21);
    assert_eq!(&bytes[..8], &[0, 0, 0x22, 0x22, 0x22, 0, 0, 0]);
    assert_eq!(&bytes[8..16], &[0; 8]);
    assert_eq!(&bytes[16..21], &[0x33; 5]);
    assert_eq!(read_at(&file, 21, &mut bytes)?, 0);

    complete(NativeFile::from_file(file.try_clone()?, None).map_err(|(error, _)| error)?.write_all_batch_async(vec![
            OwnedWrite {
                offset: 32,
                bytes: Bytes::from_static(&[0x44; 5]),
            },
            OwnedWrite {
                offset: 48,
                bytes: Bytes::from_static(&[0x55; 7]),
            },
        ],
    ))?;
    let mut batch = [0_u8; 23];
    assert_eq!(read_at(&file, 32, &mut batch)?, 23);
    assert_eq!(&batch[..5], &[0x44; 5]);
    assert_eq!(&batch[5..16], &[0; 11]);
    assert_eq!(&batch[16..], &[0x55; 7]);

    let mut cursor = file.try_clone()?;
    cursor.seek(SeekFrom::End(0))?;
    let selected = complete(read_batch_async(
        file.try_clone()?,
        vec![OwnedRead {
            offset: 16,
            length: 5,
        }],
    ))?;
    assert_eq!(selected, [Bytes::from_static(&[0x33; 5])]);
    assert_eq!(cursor.stream_position()?, 55);

    complete(NativeFile::from_file(file.try_clone()?, None).map_err(|(error, _)| error)?.write_all_batch_async(vec![OwnedWrite {
            offset: 4,
            bytes: Bytes::from_static(b"cursor-independent"),
        }],
    ))?;
    let mut written = [0_u8; 18];
    assert_eq!(read_at(&file, 4, &mut written)?, written.len());
    assert_eq!(&written, b"cursor-independent");
    assert_eq!(cursor.stream_position()?, 55);
    Ok(())
}

#[test]
fn batch_larger_than_native_queue_preserves_every_write() -> io::Result<()> {
    let temporary = tempfile::NamedTempFile::new()?;
    let file = temporary.reopen()?;
    let writes: Vec<_> = (0_u8..17)
        .map(|value| OwnedWrite {
            offset: u64::from(value) * 4,
            bytes: Bytes::from(vec![value; 3]),
        })
        .collect();
    complete(NativeFile::from_file(file.try_clone()?, None).map_err(|(error, _)| error)?.write_all_batch_async(writes))?;

    let mut actual = [0_u8; 67];
    assert_eq!(read_at(&file, 0, &mut actual)?, actual.len());
    for value in 0_u8..17 {
        let start = usize::from(value) * 4;
        assert_eq!(actual.get(start..start + 3), Some([value; 3].as_slice()));
        if value != 16 {
            assert_eq!(actual.get(start + 3), Some(&0));
        }
    }
    Ok(())
}

#[test]
fn overlapping_batch_writes_fail_before_any_write() -> io::Result<()> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("overlapping-batch");
    let file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)?;
    std::fs::write(&path, b"unchanged")?;
    let result = complete(NativeFile::from_file(file, None).map_err(|(error, _)| error)?.write_all_batch_async(vec![
            OwnedWrite {
                offset: 0,
                bytes: Bytes::from_static(b"first"),
            },
            OwnedWrite {
                offset: 3,
                bytes: Bytes::from_static(b"second"),
            },
        ],
    ));
    assert!(matches!(result, Err(error) if error.kind() == io::ErrorKind::InvalidInput));
    assert_eq!(std::fs::read(&path)?, b"unchanged");
    Ok(())
}

#[cfg(windows)]
#[test]
fn native_write_batch_preserves_empty_writes_and_port_reuse() -> io::Result<()> {
    let temporary = tempfile::NamedTempFile::new()?;
    let file = temporary.reopen()?;
    for value in 0_u8..8 {
        complete(NativeFile::from_file(file.try_clone()?, None).map_err(|(error, _)| error)?.write_all_batch_async(vec![
                OwnedWrite {
                    offset: u64::from(value) * 2,
                    bytes: Bytes::copy_from_slice(&[value]),
                },
                OwnedWrite {
                    offset: u64::from(value) * 2 + 1,
                    bytes: Bytes::new(),
                },
            ],
        ))?;
    }
    let mut actual = [0_u8; 15];
    assert_eq!(read_at(&file, 0, &mut actual)?, actual.len());
    for value in 0_u8..8 {
        assert_eq!(actual.get(usize::from(value) * 2), Some(&value));
    }
    Ok(())
}

#[cfg(windows)]
#[test]
#[allow(
    unsafe_code,
    reason = "the test drives a raw overlapped handle through the unsafe constructor"
)]
fn inline_overlapped_io_matches_the_driver() -> io::Result<()> {
    use std::os::windows::fs::OpenOptionsExt as _;
    use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OVERLAPPED;

    let temporary = tempfile::tempdir()?;
    let file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .custom_flags(FILE_FLAG_OVERLAPPED)
        .open(temporary.path().join("in-place"))?;
    // SAFETY: the handle was just opened overlapped, is attached to no
    // port, and is transferred once.
    let native = unsafe { NativeFile::from_overlapped_file_unchecked(file, None).map_err(|(error, _)| error)? };
    let reads = || {
        [(5, 8), (1, 3), (0, 0), (12, 4), (30, 4)]
            .map(|(offset, length)| OwnedRead { offset, length })
            .to_vec()
    };
    let expected = [
        Bytes::from_static(b"fgh\0\0xy"),
        Bytes::from_static(b"bcd"),
        Bytes::new(),
        Bytes::new(),
        Bytes::new(),
    ];
    let write = |offset, bytes| {
        complete(native.write_all_batch_async(vec![OwnedWrite { offset, bytes }]))
    };
    {
        let _inline = InlineBlocking::enter();
        write(0, Bytes::from_static(b"abcdefgh"))?;
        write(10, Bytes::from_static(b"xy"))?;
        assert_eq!(complete(native.read_batch_async(reads()))?, expected);
    }
    assert_eq!(complete(native.read_batch_async(reads()))?, expected);
    write(0, Bytes::from_static(b"A"))?;
    {
        let _inline = InlineBlocking::enter();
        write(1, Bytes::from_static(b"B"))?;
        let read = complete(native.read_batch_async(vec![OwnedRead {
            offset: 0,
            length: 3,
        }]))?;
        assert_eq!(read, [Bytes::from_static(b"ABc")]);
    }
    assert_eq!(
        complete(native.read_batch_async(vec![OwnedRead {
            offset: 0,
            length: 3,
        }]))?,
        [Bytes::from_static(b"ABc")]
    );
    Ok(())
}

#[cfg(not(target_vendor = "apple"))]
#[test]
fn barrier_durability_rejects_targets_without_an_exact_primitive() -> io::Result<()> {
    let temporary = tempfile::tempdir()?;
    let file = File::create(temporary.path().join("barrier"))?;
    assert!(matches!(
        sync_file(&file, Durability::Barrier, None),
        Err(error) if error.kind() == io::ErrorKind::Unsupported
    ));
    assert!(matches!(
        sync_data(&file, Durability::Barrier),
        Err(error) if error.kind() == io::ErrorKind::Unsupported
    ));
    Ok(())
}

#[test]
fn native_read_batch_preserves_order_and_short_reads() -> io::Result<()> {
    let temporary = tempfile::NamedTempFile::new()?;
    let file = temporary.reopen()?;
    write_all_at(&file, 0, b"abcdefgh", None)?;
    let reads = complete(read_batch_async(
        file,
        vec![
            OwnedRead {
                offset: 5,
                length: 8,
            },
            OwnedRead {
                offset: 1,
                length: 3,
            },
        ],
    ))?;
    assert_eq!(
        reads,
        [Bytes::from_static(b"fgh"), Bytes::from_static(b"bcd")]
    );
    Ok(())
}

#[cfg(target_vendor = "apple")]
#[test]
fn apple_batches_preserve_unlinked_open_files() -> io::Result<()> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("unlinked-open-file");
    let file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)?;
    std::fs::remove_file(path)?;
    complete(NativeFile::from_file(file.try_clone()?, None).map_err(|(error, _)| error)?.write_all_batch_async(vec![OwnedWrite {
            offset: 3,
            bytes: Bytes::from_static(b"kept"),
        }],
    ))?;
    assert_eq!(
        complete(read_batch_async(
            file,
            vec![OwnedRead {
                offset: 3,
                length: 4,
            }],
        ))?,
        [Bytes::from_static(b"kept")]
    );
    Ok(())
}

#[test]
fn invalid_positional_batches_leave_contents_and_cursor_unchanged() -> io::Result<()> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("invalid-io-uring-offset");
    let mut file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)?;
    write_all_at(&file, 0, b"stable", None)?;
    file.seek(SeekFrom::Start(3))?;

    let mut byte = [0_u8; 1];
    assert!(matches!(
        read_at(&file, u64::MAX, &mut byte),
        Err(error) if error.kind() == io::ErrorKind::InvalidInput
    ));
    assert!(matches!(
        write_all_at(&file, u64::MAX, b"x", None),
        Err(error) if error.kind() == io::ErrorKind::InvalidInput
    ));
    assert!(matches!(
        complete(read_batch_async(
            file.try_clone()?,
            vec![
                OwnedRead {
                    offset: 0,
                    length: 1,
                },
                OwnedRead {
                    offset: u64::MAX,
                    length: 1,
                },
            ],
        )),
        Err(error) if error.kind() == io::ErrorKind::InvalidInput
    ));
    assert!(matches!(
        complete(NativeFile::from_file(file.try_clone()?, None).map_err(|(error, _)| error)?.write_all_batch_async(vec![
                OwnedWrite {
                    offset: 0,
                    bytes: Bytes::from_static(b"changed"),
                },
                OwnedWrite {
                    offset: u64::MAX,
                    bytes: Bytes::from_static(b"x"),
                },
            ],
        )),
        Err(error) if error.kind() == io::ErrorKind::InvalidInput
    ));
    assert_eq!(file.stream_position()?, 3);
    assert_eq!(std::fs::read(path)?, b"stable");
    Ok(())
}

#[cfg(windows)]
#[test]
fn native_read_batch_preserves_empty_and_eof_ranges() -> io::Result<()> {
    let temporary = tempfile::NamedTempFile::new()?;
    let file = temporary.reopen()?;
    write_all_at(&file, 0, b"abc", None)?;
    let reads = complete(read_batch_async(
        file.try_clone()?,
        vec![
            OwnedRead {
                offset: 0,
                length: 0,
            },
            OwnedRead {
                offset: 3,
                length: 4,
            },
            OwnedRead {
                offset: 30,
                length: 4,
            },
        ],
    ))?;
    assert_eq!(reads, [Bytes::new(), Bytes::new(), Bytes::new()]);
    for (offset, expected) in b"abc".iter().copied().enumerate() {
        let repeated = complete(read_batch_async(
            file.try_clone()?,
            vec![OwnedRead {
                offset: offset as u64,
                length: 1,
            }],
        ))?;
        assert_eq!(repeated, [Bytes::copy_from_slice(&[expected])]);
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn native_read_batch_windows_requests_beyond_the_ring() -> io::Result<()> {
    let file = tempfile::tempfile()?;
    let expected: Vec<u8> = (0..17).collect();
    write_all_at(&file, 0, &expected, None)?;
    let requests = (0_u64..17)
        .map(|offset| OwnedRead { offset, length: 1 })
        .collect();
    let actual = complete(read_batch_async(file, requests))?;
    assert_eq!(
        actual,
        expected
            .into_iter()
            .map(|value| Bytes::copy_from_slice(&[value]))
            .collect::<Vec<_>>()
    );
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn native_read_batch_windows_one_range_beyond_the_ring() -> io::Result<()> {
    let file = tempfile::tempfile()?;
    let expected = Bytes::from(vec![0x69; 16 * 1024 * 1024 + 1]);
    write_all_at(&file, 3, &expected, None)?;
    let actual = complete(read_batch_async(
        file,
        vec![OwnedRead {
            offset: 3,
            length: expected.len(),
        }],
    ))?;
    assert_eq!(actual, [expected]);
    Ok(())
}
