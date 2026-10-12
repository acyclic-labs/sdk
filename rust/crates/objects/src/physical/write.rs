use super::*;

pub(crate) fn persist_segment<B: AsRef<[u8]>>(
    root: &Path,
    bodies: &[([u8; 32], B)],
    durability: LocalDurability,
    publication: &mut Publication,
) -> Result<([u8; 32], Vec<u64>), NativeObjectsError> {
    let published = write_segment(root, bodies, durability, publication)?;
    sync_segment_directory(root, durability, publication)?;
    Ok(published)
}

pub(crate) fn sync_segment_directory(
    root: &Path,
    durability: LocalDurability,
    publication: &mut Publication,
) -> Result<(), NativeObjectsError> {
    let parent = publication_path(root, "segments", publication)?;
    if let Some((meter, _)) = &publication.native {
        meter.check()?;
        meter.admit_read(0)?;
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt as _;
            options.write(true).share_mode(7).custom_flags(0x0200_0000);
        }
        let directory = options.open(&parent)?;
        acyclic_native_runtime::sync_file(
            &directory,
            native_durability(durability),
            Some(meter.as_ref()),
        )?;

        Ok(())
    } else {
        sync_parent(&parent, durability).map_err(native_write_error)
    }
}

pub(crate) fn sync_publication(
    file: &File,
    durability: LocalDurability,
    publication: &Publication,
) -> Result<(), NativeObjectsError> {
    acyclic_native_runtime::sync_file(
        file,
        native_durability(durability),
        publication.native.as_ref().map(|(meter, _)| meter.as_ref()),
    )?;
    Ok(())
}

pub(crate) fn write_publication(
    file: &mut File,
    offset: u64,
    bytes: &[u8],
    publication: &mut Publication,
) -> Result<(), NativeObjectsError> {
    if let Some((meter, _)) = &publication.native {
        acyclic_native_runtime::write_all_at(file, offset, bytes, Some((meter.as_ref(), false)))?;
        let end = offset
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| std::io::Error::other("publication offset overflow"))?;
        loop {
            meter.check()?;
            meter.admit_write(0)?;
            match file.seek(std::io::SeekFrom::Start(end)) {
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                result => {
                    result?;
                    break;
                }
            }
        }
        meter.check()?;
    } else {
        file.write_all(bytes)?;
    }
    Ok(())
}

pub(crate) fn open_publication(
    path: &Path,
    publication: &Publication,
) -> Result<File, NativeObjectsError> {
    publication_namespace(publication, || {
        OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(path)
    })
}

pub(crate) fn write_segment<B: AsRef<[u8]>>(
    root: &Path,
    bodies: &[([u8; 32], B)],
    durability: LocalDurability,
    publication: &mut Publication,
) -> Result<([u8; 32], Vec<u64>), NativeObjectsError> {
    let span = crate::obs::span!(TRACE, "acyclic.objects.segment.write", items = bodies.len());
    crate::obs::scoped(&span, || {
        write_segment_inner(root, bodies, durability, publication)
    })
}

pub(super) fn native_write_error(error: PhysicalError) -> NativeObjectsError {
    match error {
        PhysicalError::Io(error) => NativeObjectsError::from(NativeObjectsCause::Admission(error)),
        PhysicalError::Invalid(_) | PhysicalError::Corrupt => NativeObjectsError::from(
            NativeObjectsCause::Service(crate::v1::wire::ErrorCode::Unavailable.into()),
        ),
    }
}

pub(super) fn publication_path(
    parent: &Path,
    name: &str,
    publication: &mut Publication,
) -> Result<PathBuf, NativeObjectsError> {
    let separator = std::path::MAIN_SEPARATOR_STR;
    let capacity = parent
        .as_os_str()
        .len()
        .checked_add(separator.len())
        .and_then(|length| length.checked_add(name.len()))
        .ok_or_else(|| native_write_error(PhysicalError::Invalid("publication path overflow")))?;
    publication.allocate(capacity)?;
    let mut path = std::ffi::OsString::new();
    path.try_reserve_exact(capacity)
        .map_err(|error| publication.allocation_failed(error))?;
    publication.retain_capacity(capacity, path.capacity())?;
    publication.codec(0, capacity, 0, || {
        path.push(parent);
        path.push(separator);
        path.push(name);
        PathBuf::from(path)
    })
}

pub(super) fn publication_string(
    capacity: usize,
    publication: &mut Publication,
) -> Result<String, NativeObjectsError> {
    publication.allocate(capacity)?;
    let mut value = String::new();
    value
        .try_reserve_exact(capacity)
        .map_err(|error| publication.allocation_failed(error))?;
    publication.retain_capacity(capacity, value.capacity())?;
    Ok(value)
}

pub(super) fn publication_namespace<T>(
    publication: &Publication,
    work: impl FnOnce() -> std::io::Result<T>,
) -> Result<T, NativeObjectsError> {
    if let Some((meter, _)) = &publication.native {
        meter.check()?;
        meter.admit_write(0)?;
    }
    let result = work()?;
    publication.check()?;
    Ok(result)
}

pub(super) fn publication_probe<T>(
    publication: &Publication,
    work: impl FnOnce() -> std::io::Result<T>,
) -> Result<T, NativeObjectsError> {
    if let Some((meter, _)) = &publication.native {
        meter.check()?;
        meter.admit_read(0)?;
    }
    let result = work()?;
    publication.check()?;
    Ok(result)
}

pub(super) fn write_segment_inner<B: AsRef<[u8]>>(
    root: &Path,
    bodies: &[([u8; 32], B)],
    durability: LocalDurability,
    publication: &mut Publication,
) -> Result<([u8; 32], Vec<u64>), NativeObjectsError> {
    let invalid = || native_write_error(PhysicalError::Invalid("invalid object segment"));
    publication.check()?;
    if bodies.is_empty() || bodies.len() > MAXIMUM_SEGMENT_BODIES {
        return Err(invalid());
    }
    if bodies.len() > 1
        && bodies
            .iter()
            .map(|(_, body)| body.as_ref().len())
            .try_fold(0_usize, usize::checked_add)
            .is_none_or(|bytes| bytes > MAXIMUM_SEGMENT_BYTES)
    {
        return Err(invalid());
    }
    let count = u32::try_from(bodies.len()).map_err(|_| invalid())?;
    let table_bytes = bodies
        .len()
        .checked_mul(SEGMENT_RECORD_BYTES)
        .ok_or_else(invalid)?;
    let metadata_capacity = SEGMENT_HEADER_BYTES
        .checked_add(table_bytes)
        .ok_or_else(invalid)?;
    let mut offsets = Vec::new();
    publication.reserve(&mut offsets, bodies.len())?;
    let mut metadata = Vec::new();
    publication.reserve(&mut metadata, metadata_capacity)?;
    publication.codec(metadata_capacity, metadata_capacity, 0, || {
        metadata.extend_from_slice(SEGMENT_MAGIC);
        metadata.extend_from_slice(&count.to_le_bytes());
        for (digest, body) in bodies {
            metadata.extend_from_slice(digest);
            metadata.extend_from_slice(&(body.as_ref().len() as u64).to_le_bytes());
        }
    })?;
    let mut position = metadata_capacity as u64;
    for (_, body) in bodies {
        offsets.push(position);
        position = position
            .checked_add(body.as_ref().len() as u64)
            .ok_or_else(invalid)?;
    }
    let mut hasher = blake3::Hasher::new();
    publication.codec(0, 0, metadata.len(), || {
        hasher.update(&metadata);
    })?;
    for (_, body) in bodies {
        publication.codec(0, 0, body.as_ref().len(), || {
            hasher.update(body.as_ref());
        })?;
    }
    let id = *hasher.finalize().as_bytes();
    let parent = publication_path(root, "segments", publication)?;
    let mut identity = publication_string(64, publication)?;
    publication.codec(64, 0, 0, || {
        const DIGITS: &[u8; 16] = b"0123456789abcdef";
        for byte in id {
            identity.push(char::from(DIGITS[usize::from(byte >> 4)]));
            identity.push(char::from(DIGITS[usize::from(byte & 15)]));
        }
    })?;
    let mut name = publication_string(72, publication)?;
    publication.codec(72, 72, 0, || {
        name.push_str(&identity);
        name.push_str(".segment");
    })?;
    let destination = publication_path(&parent, &name, publication)?;
    let existing = publication_probe(publication, || fs::metadata(&destination));
    match existing {
        Ok(_) => {
            validate_segment_publication(&destination, &id, position, publication)?;
            return Ok((id, offsets));
        }
        Err(error)
            if !error.has_payload()
                && matches!(&error.cause, NativeObjectsCause::Admission(cause) if cause.kind() == std::io::ErrorKind::NotFound) =>
            {}
        Err(error) => return Err(error),
    }
    let mut opened = None;
    for _ in 0..1_024 {
        use std::fmt::Write as _;
        let nonce = TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let name_length = 1
            + 64
            + 1
            + (pid.max(1).ilog10() as usize + 1)
            + 1
            + (nonce.max(1).ilog10() as usize + 1)
            + 4;
        let mut temporary_name = publication_string(name_length, publication)?;
        publication.codec(name_length, name_length, 0, || {
            let _ = write!(temporary_name, ".{identity}.{pid}-{nonce}.tmp");
        })?;
        let temporary = publication_path(&parent, &temporary_name, publication)?;
        let result = publication_namespace(publication, || {
            let mut options = OpenOptions::new();
            options.create_new(true).write(true);
            #[cfg(windows)]
            if publication.native.is_some() {
                use std::os::windows::fs::OpenOptionsExt as _;
                options.custom_flags(0x4000_0000);
            }
            options.open(&temporary)
        });
        match result {
            Ok(file) => {
                opened = Some((temporary, file));
                break;
            }
            Err(error)
                if !error.has_payload()
                    && matches!(&error.cause, NativeObjectsCause::Admission(cause) if cause.kind() == std::io::ErrorKind::AlreadyExists) =>
                {}
            Err(error) => return Err(error),
        }
    }
    let (temporary, mut file) = opened.ok_or_else(invalid)?;
    let write_result = (|| -> Result<(), NativeObjectsError> {
        if let Some((meter, _)) = &publication.native {
            acyclic_native_runtime::write_all_at(
                &file,
                0,
                &metadata,
                Some((meter.as_ref(), true)),
            )?;
            for ((_, body), offset) in bodies.iter().zip(&offsets) {
                acyclic_native_runtime::write_all_at(
                    &file,
                    *offset,
                    body.as_ref(),
                    Some((meter.as_ref(), true)),
                )?;
            }
        } else {
            file.write_all(&metadata)?;
            for (_, body) in bodies {
                file.write_all(body.as_ref())?;
            }
        }
        sync_publication(&file, durability, publication)
    })();
    drop(file);
    if let Err(error) = write_result {
        let _ = publication_namespace(publication, || fs::remove_file(&temporary));
        return Err(error);
    }
    let published = acyclic_native_runtime::publish_new(
        &temporary,
        &destination,
        publication.native.as_ref().map(|(meter, _)| meter),
    )
    .map_err(NativeObjectsError::from);
    match published {
        Ok(()) => {}
        Err(error)
            if !error.has_payload()
                && matches!(&error.cause, NativeObjectsCause::Admission(cause) if cause.kind() == std::io::ErrorKind::AlreadyExists) =>
        {
            validate_segment_publication(&destination, &id, position, publication)?
        }
        Err(error) => {
            let _ = publication_namespace(publication, || fs::remove_file(&temporary));
            return Err(error);
        }
    }
    match publication_namespace(publication, || fs::remove_file(&temporary)) {
        Ok(()) => {}
        Err(error)
            if !error.has_payload()
                && matches!(&error.cause, NativeObjectsCause::Admission(cause) if cause.kind() == std::io::ErrorKind::NotFound) =>
            {}
        Err(error) => return Err(error),
    }
    Ok((id, offsets))
}

pub(crate) fn local_path(
    root: &Path,
    suffix: &str,
    meter: Option<&Arc<dyn acyclic_native_runtime::NativeReadMeter>>,
) -> Result<crate::v1::Guarded<PathBuf>, NativeObjectsError> {
    let maximum = root
        .as_os_str()
        .len()
        .checked_add(suffix.len())
        .and_then(|length| length.checked_add(1))
        .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?;
    let mut allocation = meter
        .map(|meter| acyclic_native_runtime::ReadAllocation::new(meter, maximum))
        .transpose()?;
    let mut path = PathBuf::new();
    path.try_reserve_exact(maximum)
        .map_err(|error| match meter {
            Some(meter) => NativeObjectsError::from(NativeObjectsCause::Admission(
                meter.allocation_failed(error),
            )),
            None => NativeObjectsError::from(NativeObjectsCause::Service(
                crate::v1::wire::ErrorCode::Unavailable.into(),
            )),
        })?;
    if let Some(allocation) = &mut allocation {
        allocation.reconcile_capacity(path.capacity())?;
    }
    if let Some(meter) = meter {
        meter.admit_codec(0, maximum as u64, 0)?;
    }
    path.push(root);
    path.push(suffix);
    if let Some(meter) = meter {
        meter.record_codec(0, path.as_os_str().len() as u64, 0)?;
    }
    Ok(crate::v1::Guarded {
        value: path,
        _allocation: allocation,
    })
}

pub(crate) fn native_durability(durability: LocalDurability) -> acyclic_native_runtime::Durability {
    match durability {
        LocalDurability::FullFlush => acyclic_native_runtime::Durability::Full,
        LocalDurability::Barrier => acyclic_native_runtime::Durability::Barrier,
    }
}

pub(super) fn sync_parent(path: &Path, durability: LocalDurability) -> Result<(), PhysicalError> {
    acyclic_native_runtime::sync_parent(path, native_durability(durability))?;
    Ok(())
}
