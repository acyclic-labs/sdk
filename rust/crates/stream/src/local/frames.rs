fn validate_limits(limits: LocalStreamLimits) -> Result<(), LocalStreamError> {
    let memory = limits.memory;
    if memory.paths == Some(0)
        || memory.path_bytes == Some(0)
        || memory.records == Some(0)
        || memory.payload_bytes == Some(0)
        || memory.commits == Some(0)
        || memory.idempotency_results == Some(0)
        || limits.compaction_operations == 0
        || limits.compaction_bytes
            < u64::try_from(HEADER_BYTES).map_err(|_| LocalStreamError::InvalidLimits)?
    {
        Err(LocalStreamError::InvalidLimits)
    } else {
        Ok(())
    }
}

fn encode_limits(limits: LocalStreamLimits) -> Result<[u8; LIMITS_BYTES], LocalStreamError> {
    let memory = [
        limits.memory.paths,
        limits.memory.path_bytes,
        limits.memory.records,
        limits.memory.payload_bytes,
        limits.memory.commits,
        limits.memory.idempotency_results,
    ]
    .into_iter()
    .map(|value| {
        // Zero is outside the admitted finite capacities and denotes None.
        value
            .map(u64::try_from)
            .transpose()
            .map(|value| value.unwrap_or_default())
            .map_err(|_| LocalStreamError::InvalidLimits)
    });
    let values = memory.chain([
        Ok(limits.compaction_operations),
        Ok(limits.compaction_bytes),
    ]);
    let mut encoded = [0; LIMITS_BYTES];
    for (slot, value) in encoded.as_chunks_mut::<8>().0.iter_mut().zip(values) {
        slot.copy_from_slice(&value?.to_le_bytes());
    }
    Ok(encoded)
}

fn encode_header(
    limits: LocalStreamLimits,
    epoch: u64,
) -> Result<[u8; HEADER_BYTES], LocalStreamError> {
    let mut encoded = [0; HEADER_BYTES];
    let (magic, rest) = encoded.split_at_mut(HEADER_MAGIC.len());
    let (capacities, generation) = rest.split_at_mut(LIMITS_BYTES);
    magic.copy_from_slice(HEADER_MAGIC);
    capacities.copy_from_slice(&encode_limits(limits)?);
    generation.copy_from_slice(&epoch.to_le_bytes());
    Ok(encoded)
}

/// The epoch of a whole journal header written for `limits`.
fn versioned_header<'a>(
    encoded: &'a [u8],
    domain: &[u8],
    version: &[u8],
    format: &'static str,
) -> Result<&'a [u8], LocalStreamError> {
    crate::persistence::header(encoded, domain, version).map_err(|error| match error.category() {
        StreamCategory::Unsupported => LocalStreamError::UnsupportedVersion { format },
        _ => LocalStreamError::Corrupt,
    })
}

fn decode_header(header: &[u8], limits: LocalStreamLimits) -> Result<u64, LocalStreamError> {
    let rest = versioned_header(header, b"ACYCLIC-STREAM-LOCAL-V", b"2\0", "journal")?;
    let (found, epoch) = rest
        .split_at_checked(LIMITS_BYTES)
        .ok_or(LocalStreamError::Corrupt)?;
    if found != encode_limits(limits)?.as_slice() {
        return Err(LocalStreamError::Corrupt);
    }
    Ok(u64::from_le_bytes(
        epoch.try_into().map_err(|_| LocalStreamError::Corrupt)?,
    ))
}

/// Admits a complete header or a canonical interrupted header write.
fn read_header(
    mut reader: impl Read,
    length: u64,
    limits: LocalStreamLimits,
    epoch: u64,
) -> Result<Option<u64>, LocalStreamError> {
    let mut header = [0; HEADER_BYTES];
    if length >= u64::try_from(HEADER_BYTES).map_err(|_| LocalStreamError::InvalidLimits)? {
        reader
            .read_exact(&mut header)
            .map_err(|_| LocalStreamError::Corrupt)?;
        return decode_header(&header, limits).map(Some);
    }
    let partial = header
        .get_mut(..usize::try_from(length).map_err(|_| LocalStreamError::Corrupt)?)
        .ok_or(LocalStreamError::Corrupt)?;
    reader.read_exact(partial)?;
    if encode_header(limits, epoch)?.starts_with(partial) {
        Ok(None)
    } else {
        Err(LocalStreamError::Corrupt)
    }
}
