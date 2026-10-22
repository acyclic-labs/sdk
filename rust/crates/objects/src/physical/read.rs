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
            #[cfg(test)]
            crate::body::work::record("journal", *offset, expected_length);
            let (root, offset, digest) = (Arc::clone(root), *offset, *expected_digest);
            let body = acyclic_native_runtime::run_blocking_io(
                move || root.read_journal_body(offset, &digest, expected_length),
                None,
            )
            .map_err(|_| Error::from(Unavailable))?
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
    .map_err(|_| Error::from(Unavailable))?
    .await
    .map_err(|_| Error::from(Unavailable))?
    .map_err(|_| Error::from(Unavailable))?;
    let selected_length = end.saturating_sub(start);
    let selected_offset = offset
        .checked_add(u64::try_from(start).map_err(|_| Error::from(Unavailable))?)
        .ok_or(Error::from(Unavailable))?;
    let prefix_length = segment_prefix_length(*offset);
    #[cfg(test)]
    {
        crate::body::work::record("read", 0, prefix_length);
        crate::body::work::record("read", selected_offset, selected_length);
    }
    let reads = acyclic_native_runtime::read_batch_async(
        file,
        vec![
            acyclic_native_runtime::OwnedRead {
                offset: 0,
                length: prefix_length,
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
    let (storage, header) = crate::body::native_body_storage(length, &meter)?;
    let prefix = match location {
        LocalBodyLocation::Segment { offset, .. } => {
            let prefix_length = segment_prefix_length(*offset);
            Some((
                crate::body::native_storage(prefix_length, &meter)?,
                prefix_length,
            ))
        }
        LocalBodyLocation::Journal { .. } => None,
    };
    let (file, file_length, location) = open_native_body(root, location, &meter).await?;
    match location {
        LocalBodyLocation::Journal { offset } => {
            let storage =
                read_native(&file, offset, storage, expected_length, true, &meter).await?;
            let actual = storage.0.len();
            meter.admit_codec(0, 0, actual as u64)?;
            let digest = blake3::hash(&storage.0);
            meter.record_codec(0, 0, actual as u64)?;
            if actual != expected_length || digest.as_bytes() != expected_digest {
                return Err(unavailable());
            }
            Ok(crate::body::publish_native_storage(storage, header)?.slice(start..end))
        }
        LocalBodyLocation::Segment { offset, .. } => {
            let (prefix, prefix_length) = prefix.ok_or_else(unavailable)?;
            let prefix = read_native(&file, 0, prefix, prefix_length, false, &meter).await?;
            let table = segment_table(&prefix.0)?;
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
            drop(prefix);
            if !matches {
                return Err(unavailable());
            }
            let selected_offset = offset.checked_add(start as u64).ok_or_else(unavailable)?;
            let storage =
                read_native(&file, selected_offset, storage, length, true, &meter).await?;
            crate::body::publish_native_storage(storage, header)
        }
    }
}

/// Bytes before a segment body that can hold the segment header and table:
/// every table precedes its first body, so a read never needs bytes at or
/// after the body's own offset.
fn segment_prefix_length(offset: u64) -> usize {
    let maximum = SEGMENT_HEADER_BYTES + MAXIMUM_SEGMENT_BODIES * SEGMENT_RECORD_BYTES;
    #[cfg(test)]
    if crate::body::work::restores_old_work() {
        return maximum;
    }
    usize::try_from(offset).map_or(maximum, |offset| offset.min(maximum))
}

/// The one native positional read: it fills admitted spare capacity in place.
async fn read_native(
    file: &acyclic_native_runtime::NativeFile,
    offset: u64,
    storage: crate::body::NativeStorage,
    length: usize,
    require_full: bool,
    meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
) -> Result<crate::body::NativeStorage, NativeObjectsError> {
    #[cfg(test)]
    crate::body::work::record("read", offset, length);
    let (storage, _) = file
        .read_spare_bounded_async(offset, storage, length, require_full, Arc::clone(meter))?
        .await?;
    Ok(storage)
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

async fn open_native_body(
    root: &Arc<LocalRoot>,
    location: &LocalBodyLocation,
    meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
) -> Result<(acyclic_native_runtime::NativeFile, u64, LocalBodyLocation), NativeObjectsError> {
    let (root, location) = (Arc::clone(root), location.clone());
    let open_meter = Arc::clone(meter);
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
                        let file = File::open(&path.value)?;
                        *journal = Some(acyclic_native_runtime::CachedFile::new_metered(
                            file,
                            &path.value,
                            false,
                            false,
                            Some(open_meter.as_ref()),
                        )?);

                        drop(path);
                    }
                    let lease = journal
                        .as_ref()
                        .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::NotFound))?
                        .lease_metered(Some(open_meter.as_ref()))?;
                    open_meter.admit_read(0)?;
                    lease.file().try_clone()?
                }
                LocalBodyLocation::Segment { id, .. } => {
                    const HEX: &[u8; 16] = b"0123456789abcdef";
                    let mut name = [0u8; 72];
                    open_meter.admit_codec(64, 0, 0)?;
                    for (pair, byte) in name.as_chunks_mut::<2>().0.iter_mut().zip(id) {
                        let [high, low] = pair;
                        *high = *HEX
                            .get(usize::from(byte >> 4))
                            .ok_or(std::io::Error::from(std::io::ErrorKind::InvalidData))?;
                        *low = *HEX
                            .get(usize::from(byte & 15))
                            .ok_or(std::io::Error::from(std::io::ErrorKind::InvalidData))?;
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
        Some((Arc::clone(meter), false)),
    )?;
    opened
        .await
        .map_err(|_| NativeObjectsError::from(Error::from(Unavailable)))?
}
