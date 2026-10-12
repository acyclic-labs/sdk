use super::*;

type ValidatedSegmentRecords = BTreeMap<(u64, [u8; 32]), u64>;

pub(super) fn validate_segment_publication(
    path: &Path,
    expected_id: &[u8; 32],
    expected_length: u64,
    publication: &mut Publication,
) -> Result<(), NativeObjectsError> {
    let mut file = publication_probe(publication, || File::open(path))?;
    let length = publication_probe(publication, || file.metadata())?.len();
    if length != expected_length {
        return Err(native_write_error(PhysicalError::Corrupt));
    }
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        if let Some((meter, _)) = &publication.native {
            meter.check()?;
            meter.admit_read(buffer.len())?;
        }
        let read = file.read(&mut buffer)?;
        if let Some((meter, _)) = &publication.native {
            meter.record_read(read)?;
        }
        if read == 0 {
            break;
        }
        publication.codec(0, 0, read, || {
            hasher.update(&buffer[..read]);
        })?;
    }
    if hasher.finalize().as_bytes() != expected_id {
        return Err(native_write_error(PhysicalError::Corrupt));
    }
    Ok(())
}

pub(crate) fn validate_referenced_segments(
    root: &Path,
    bodies: &BTreeSet<LocalBodyReference>,
    maximum_object_bytes: u64,
) -> Result<BTreeSet<[u8; 32]>, PhysicalError> {
    let span = crate::obs::span!(
        TRACE,
        "acyclic.objects.segment.validate",
        items = bodies.len(),
    );
    crate::obs::scoped(&span, || {
        validate_referenced_segments_inner(root, bodies, maximum_object_bytes)
    })
}

pub(super) fn validate_referenced_segments_inner(
    root: &Path,
    bodies: &BTreeSet<LocalBodyReference>,
    maximum_object_bytes: u64,
) -> Result<BTreeSet<[u8; 32]>, PhysicalError> {
    let mut validated_segments = BTreeMap::new();
    for body in bodies {
        let (id, offset) = match &body.location {
            LocalBodyLocation::Segment { id, offset } => (id, offset),
            LocalBodyLocation::Journal { .. } => continue,
        };
        if !validated_segments.contains_key(id) {
            let records =
                validate_segment_records(&segment_path(root, id), id, maximum_object_bytes)?;
            validated_segments.insert(*id, records);
        }
        let expected_length = u64::try_from(body.length).map_err(|_| PhysicalError::Corrupt)?;
        if validated_segments
            .get(id)
            .and_then(|records| records.get(&(*offset, body.digest)))
            != Some(&expected_length)
        {
            return Err(PhysicalError::Corrupt);
        }
    }
    Ok(validated_segments.into_keys().collect())
}

pub(super) fn validate_segment_records(
    path: &Path,
    expected_id: &[u8; 32],
    maximum_object_bytes: u64,
) -> Result<ValidatedSegmentRecords, PhysicalError> {
    let mut file = File::open(path).map_err(|_| PhysicalError::Corrupt)?;
    let actual_length = file.metadata().map_err(|_| PhysicalError::Corrupt)?.len();
    let mut hasher = blake3::Hasher::new();
    let mut magic = [0_u8; SEGMENT_MAGIC.len()];
    file.read_exact(&mut magic)
        .map_err(|_| PhysicalError::Corrupt)?;
    if magic != *SEGMENT_MAGIC {
        return Err(PhysicalError::Corrupt);
    }
    hasher.update(&magic);
    let mut count = [0_u8; 4];
    file.read_exact(&mut count)
        .map_err(|_| PhysicalError::Corrupt)?;
    hasher.update(&count);
    let count = usize::try_from(u32::from_le_bytes(count)).map_err(|_| PhysicalError::Corrupt)?;
    if count == 0 || count > MAXIMUM_SEGMENT_BODIES {
        return Err(PhysicalError::Corrupt);
    }
    let table_bytes = count
        .checked_mul(SEGMENT_RECORD_BYTES)
        .ok_or(PhysicalError::Corrupt)?;
    let body_start = SEGMENT_HEADER_BYTES
        .checked_add(table_bytes)
        .ok_or(PhysicalError::Corrupt)?;
    let mut metadata = Vec::new();
    metadata
        .try_reserve_exact(count)
        .map_err(|_| PhysicalError::Corrupt)?;
    for _ in 0..count {
        let mut digest = [0_u8; 32];
        file.read_exact(&mut digest)
            .map_err(|_| PhysicalError::Corrupt)?;
        hasher.update(&digest);
        let mut length = [0_u8; 8];
        file.read_exact(&mut length)
            .map_err(|_| PhysicalError::Corrupt)?;
        hasher.update(&length);
        metadata.push((digest, u64::from_le_bytes(length)));
    }
    if file.stream_position().map_err(|_| PhysicalError::Corrupt)?
        != u64::try_from(body_start).map_err(|_| PhysicalError::Corrupt)?
    {
        return Err(PhysicalError::Corrupt);
    }
    let mut records = BTreeMap::new();
    let mut buffer = [0_u8; 64 * 1024];
    let mut body_bytes = 0_u64;
    for (digest, length) in metadata {
        let offset = file.stream_position().map_err(|_| PhysicalError::Corrupt)?;
        if records.insert((offset, digest), length).is_some() {
            return Err(PhysicalError::Corrupt);
        }
        body_bytes = body_bytes
            .checked_add(length)
            .ok_or(PhysicalError::Corrupt)?;
        let maximum_body_bytes = if count == 1 {
            maximum_object_bytes
        } else {
            MAXIMUM_SEGMENT_BYTES as u64
        };
        if body_bytes > maximum_body_bytes {
            return Err(PhysicalError::Corrupt);
        }
        let mut body_hasher = blake3::Hasher::new();
        let mut remaining = length;
        while remaining != 0 {
            let selected = usize::try_from(remaining.min(buffer.len() as u64))
                .map_err(|_| PhysicalError::Corrupt)?;
            let bytes = buffer.get_mut(..selected).ok_or(PhysicalError::Corrupt)?;
            file.read_exact(bytes).map_err(|_| PhysicalError::Corrupt)?;
            hasher.update(bytes);
            body_hasher.update(bytes);
            remaining -= u64::try_from(selected).map_err(|_| PhysicalError::Corrupt)?;
        }
        if body_hasher.finalize().as_bytes() != &digest {
            return Err(PhysicalError::Corrupt);
        }
    }
    if file.stream_position().map_err(|_| PhysicalError::Corrupt)? != actual_length
        || hasher.finalize().as_bytes() != expected_id
    {
        return Err(PhysicalError::Corrupt);
    }
    Ok(records)
}

pub(super) fn segment_table(prefix: &[u8]) -> Result<&[u8], Error> {
    if prefix.get(..SEGMENT_MAGIC.len()) != Some(SEGMENT_MAGIC) {
        return Err(Error::from(Unavailable));
    }
    let count = u32::from_le_bytes(
        prefix
            .get(SEGMENT_MAGIC.len()..SEGMENT_HEADER_BYTES)
            .ok_or(Error::from(Unavailable))?
            .try_into()
            .map_err(|_| Error::from(Unavailable))?,
    ) as usize;
    if count == 0 || count > MAXIMUM_SEGMENT_BODIES {
        return Err(Error::from(Unavailable));
    }
    prefix
        .get(SEGMENT_HEADER_BYTES..SEGMENT_HEADER_BYTES + count * SEGMENT_RECORD_BYTES)
        .ok_or(Error::from(Unavailable))
}

pub(super) fn segment_table_matches(
    table: &[u8],
    file_length: u64,
    expected_offset: u64,
    expected_digest: &[u8; 32],
    expected_length: u64,
    examined: &mut u64,
) -> Result<bool, Error> {
    let mut offset = u64::try_from(
        SEGMENT_HEADER_BYTES
            .checked_add(table.len())
            .ok_or(Error::from(Unavailable))?,
    )
    .map_err(|_| Error::from(Unavailable))?;
    for record in table.as_chunks::<SEGMENT_RECORD_BYTES>().0 {
        *examined += 1;
        let digest: [u8; 32] = record
            .get(..32)
            .ok_or(Error::from(Unavailable))?
            .try_into()
            .map_err(|_| Error::from(Unavailable))?;
        let length = u64::from_le_bytes(
            record
                .get(32..SEGMENT_RECORD_BYTES)
                .ok_or(Error::from(Unavailable))?
                .try_into()
                .map_err(|_| Error::from(Unavailable))?,
        );
        let end = offset.checked_add(length).ok_or(Error::from(Unavailable))?;
        if end > file_length {
            return Err(Error::from(Unavailable));
        }
        if offset == expected_offset && digest == *expected_digest && length == expected_length {
            return Ok(true);
        }
        offset = end;
    }
    Ok(false)
}
