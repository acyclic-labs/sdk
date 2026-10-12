use super::*;

pub(crate) async fn read_body_at(
    root: &Arc<LocalRoot>,
    expected_digest: &[u8; 32],
    expected_length: usize,
    location: &LocalBodyLocation,
    start: usize,
    end: usize,
    mode: crate::body::BodyReadMode,
) -> Result<acyclic_stream::OwnedBytes, NativeObjectsError> {
    let backend = match location {
        LocalBodyLocation::Segment { .. } => "segment",
        LocalBodyLocation::Journal { .. } => "journal",
    };
    let span = crate::obs::span!(
        TRACE,
        "acyclic.objects.physical.read",
        backend,
        bytes = end.saturating_sub(start),
        body_bytes = expected_length,
    );
    crate::obs::traced(span, async {
        match mode {
            crate::body::BodyReadMode::Ordinary => read_body_at_inner_async(
                root,
                expected_digest,
                expected_length,
                location,
                start,
                end,
            )
            .await
            .map_err(Into::into),
            crate::body::BodyReadMode::Native(meter) => {
                read_body_at_native_inner(
                    root,
                    expected_digest,
                    expected_length,
                    location,
                    start,
                    end,
                    meter,
                )
                .await
            }
        }
    })
    .await
}

async fn read_body_at_inner_async(
    root: &Arc<LocalRoot>,
    expected_digest: &[u8; 32],
    expected_length: usize,
    location: &LocalBodyLocation,
    start: usize,
    end: usize,
) -> Result<acyclic_stream::OwnedBytes, Error> {
    if start > end || end > expected_length {
        return Err(Error::from(Unavailable));
    }
    let (id, offset) = match location {
        LocalBodyLocation::Segment { id, offset } => (id, offset),
        LocalBodyLocation::Journal { offset } => {
            let (root, offset, digest) = (Arc::clone(root), *offset, *expected_digest);
            let body = acyclic_native_runtime::run_blocking_io(
                move || root.read_journal_body(offset, &digest, expected_length),
                None,
            )
            .expect("unmetered task construction cannot fail")
            .await
            .map_err(|_| Error::from(Unavailable))??;
            return Ok(body.slice(start..end));
        }
    };
    let segment = segment_path(&root.path, id);
    let (file, file_length) = acyclic_native_runtime::run_blocking_io(
        move || {
            let file = File::open(segment)?;
            let length = file.metadata()?.len();
            Ok::<_, std::io::Error>((file, length))
        },
        None,
    )
    .expect("unmetered task construction cannot fail")
    .await
    .map_err(|_| Error::from(Unavailable))?
    .map_err(|_| Error::from(Unavailable))?;
    let selected_length = end.saturating_sub(start);
    let selected_offset = offset
        .checked_add(u64::try_from(start).map_err(|_| Error::from(Unavailable))?)
        .ok_or(Error::from(Unavailable))?;
    let maximum_prefix = SEGMENT_HEADER_BYTES
        .checked_add(MAXIMUM_SEGMENT_BODIES * SEGMENT_RECORD_BYTES)
        .ok_or(Error::from(Unavailable))?;
    let reads = acyclic_native_runtime::read_batch_async(
        file,
        vec![
            acyclic_native_runtime::OwnedRead {
                offset: 0,
                length: maximum_prefix,
            },
            acyclic_native_runtime::OwnedRead {
                offset: selected_offset,
                length: selected_length,
            },
        ],
    )
    .await
    .map_err(|_| Error::from(Unavailable))?;
    let mut reads = reads.into_iter();
    let prefix = reads.next().ok_or(Error::from(Unavailable))?;
    let selected = reads.next().ok_or(Error::from(Unavailable))?;
    let table = segment_table(&prefix)?;
    if selected.len() != selected_length {
        return Err(Error::from(Unavailable));
    }
    if !segment_table_matches(
        table,
        file_length,
        *offset,
        expected_digest,
        u64::try_from(expected_length).map_err(|_| Error::from(Unavailable))?,
        &mut 0,
    )? {
        return Err(Error::from(Unavailable));
    }
    // Runtime batch buffers are opaque here; preserve honest unknown capacity.
    Ok(selected.into())
}

async fn read_body_at_native_inner(
    root: &Arc<LocalRoot>,
    expected_digest: &[u8; 32],
    expected_length: usize,
    location: &LocalBodyLocation,
    start: usize,
    end: usize,
    meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
) -> Result<acyclic_stream::OwnedBytes, crate::v1::NativeObjectsError> {
    use crate::v1::{Error, NativeObjectsError, wire::ErrorCode::Unavailable};
    let unavailable = || NativeObjectsError::from(Error::from(Unavailable));
    meter.check()?;
    if start > end || end > expected_length {
        return Err(unavailable());
    }
    if matches!(location, LocalBodyLocation::Journal { .. })
        && expected_length > MAXIMUM_INLINE_BODY_BYTES
    {
        return Err(unavailable());
    }
    let length = match location {
        LocalBodyLocation::Journal { .. } => expected_length,
        LocalBodyLocation::Segment { .. } => end - start,
    };
    let storage = crate::body::prepare_native_bytes(length, &meter)?;
    let maximum_prefix = SEGMENT_HEADER_BYTES + MAXIMUM_SEGMENT_BODIES * SEGMENT_RECORD_BYTES;
    let prefix = match location {
        LocalBodyLocation::Segment { .. } => {
            Some(crate::body::prepare_native_bytes(maximum_prefix, &meter)?)
        }
        LocalBodyLocation::Journal { .. } => None,
    };
    let (root, location) = (Arc::clone(root), location.clone());
    let open_meter = Arc::clone(&meter);
    let opened = acyclic_native_runtime::run_blocking_io(
        move || {
            open_meter.check()?;
            let file = match &location {
                LocalBodyLocation::Journal { .. } => {
                    let mut journal = root
                        .journal
                        .write()
                        .map_err(|_| std::io::Error::from(std::io::ErrorKind::Other))?;
                    if journal.is_none() {
                        let path = local_path(&root.path, JOURNAL_FILE, Some(&open_meter))?;
                        open_meter.admit_read(0)?;
                        *journal = Some(File::open(&path.value)?);

                        drop(path);
                    }
                    open_meter.admit_read(0)?;
                    journal
                        .as_ref()
                        .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::NotFound))?
                        .try_clone()?
                }
                LocalBodyLocation::Segment { id, .. } => {
                    const HEX: &[u8; 16] = b"0123456789abcdef";
                    let mut name = [0u8; 72];
                    open_meter.admit_codec(64, 0, 0)?;
                    for (index, byte) in id.iter().copied().enumerate() {
                        name[index * 2] = HEX[(byte >> 4) as usize];
                        name[index * 2 + 1] = HEX[(byte & 15) as usize];
                    }
                    name[64..].copy_from_slice(b".segment");
                    open_meter.record_codec(64, 0, 0)?;
                    let name = std::str::from_utf8(&name)
                        .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidData))?;
                    let path = local_path(&root.path, "segments", Some(&open_meter))?;
                    let file_path = local_path(&path.value, name, Some(&open_meter))?;
                    open_meter.admit_read(0)?;
                    let file = File::open(&file_path.value)?;
                    drop(path);
                    file
                }
            };
            open_meter.admit_read(0)?;
            let file_length = file.metadata()?.len();
            let file = acyclic_native_runtime::NativeFile::from_file(
                file,
                Some(acyclic_native_runtime::NativeFileMeter::Owned(open_meter)),
            )
            .map_err(|(error, file)| {
                drop(file);
                error
            })?;
            Ok::<_, NativeObjectsError>((file, file_length, location))
        },
        Some((Arc::clone(&meter), false)),
    )?;
    let (file, file_length, location) = opened.await.map_err(|_| unavailable())??;
    match location {
        LocalBodyLocation::Journal { offset } => {
            let (storage, actual) = file
                .read_prepared_bounded_async(
                    offset,
                    storage,
                    expected_length,
                    true,
                    Arc::clone(&meter),
                )?
                .await?;
            let body = storage.freeze(actual).map_err(|_| unavailable())?;
            meter.admit_codec(0, 0, actual as u64)?;
            let digest = blake3::hash(body.as_slice());
            meter.record_codec(0, 0, actual as u64)?;
            if actual != expected_length || digest.as_bytes() != expected_digest {
                return Err(unavailable());
            }
            Ok(body.slice(start..end))
        }
        LocalBodyLocation::Segment { offset, .. } => {
            let prefix = prefix.ok_or_else(unavailable)?;
            let (prefix, actual) = file
                .read_prepared_bounded_async(0, prefix, maximum_prefix, false, Arc::clone(&meter))?
                .await?;
            let prefix = prefix.freeze(actual).map_err(|_| unavailable())?;
            let table = segment_table(&prefix)?;
            let count = table.len() / SEGMENT_RECORD_BYTES;
            meter.admit_examination(count as u64, 0)?;
            let mut examined = 0;
            let matches = segment_table_matches(
                table,
                file_length,
                offset,
                expected_digest,
                expected_length as u64,
                &mut examined,
            );
            meter.record_examination(examined, 0)?;
            let matches = matches.map_err(|_| unavailable())?;
            if !matches {
                return Err(unavailable());
            }
            let selected_offset = offset.checked_add(start as u64).ok_or_else(unavailable)?;
            let (storage, actual) = file
                .read_prepared_bounded_async(selected_offset, storage, length, true, meter)?
                .await?;
            storage.freeze(actual).map_err(|_| unavailable())
        }
    }
}

pub(super) fn read_journal_body(
    journal: &File,
    offset: u64,
    expected_digest: &[u8; 32],
    expected_length: usize,
) -> Result<acyclic_stream::OwnedBytes, Error> {
    if expected_length > MAXIMUM_INLINE_BODY_BYTES {
        return Err(Error::from(Unavailable));
    }
    let mut body = vec![0_u8; expected_length];
    read_exact_at_unsequenced(journal, offset, &mut body)?;
    if blake3::hash(&body).as_bytes() != expected_digest {
        return Err(Error::from(Unavailable));
    }
    Ok(body.into())
}

pub(super) fn read_exact_at_unsequenced(
    file: &File,
    offset: u64,
    bytes: &mut [u8],
) -> Result<(), Error> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt as _;
        file.read_exact_at(bytes, offset)
            .map_err(|_| Error::from(Unavailable))
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt as _;
        let mut offset = offset;
        let mut bytes = bytes;
        while !bytes.is_empty() {
            let count = file
                .seek_read(bytes, offset)
                .map_err(|_| Error::from(Unavailable))?;
            if count == 0 {
                return Err(Error::from(Unavailable));
            }
            offset = offset
                .checked_add(count as u64)
                .ok_or(Error::from(Unavailable))?;
            bytes = bytes.get_mut(count..).ok_or(Error::from(Unavailable))?;
        }
        Ok(())
    }
}
