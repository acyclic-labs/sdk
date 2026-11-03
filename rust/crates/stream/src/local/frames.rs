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
    for (slot, value) in encoded.chunks_exact_mut(8).zip(values) {
        slot.copy_from_slice(&value?.to_le_bytes());
    }
    Ok(encoded)
}

fn encode_header(limits: LocalStreamLimits, epoch: u64) -> Result<Vec<u8>, LocalStreamError> {
    let mut encoded = Vec::with_capacity(HEADER_BYTES);
    encoded.extend_from_slice(HEADER_MAGIC);
    encoded.extend_from_slice(&encode_limits(limits)?);
    encoded.extend_from_slice(&epoch.to_le_bytes());
    Ok(encoded)
}

/// The epoch of a whole journal header written for `limits`.
fn versioned_header<'a>(
    encoded: &'a [u8],
    domain: &[u8],
    version: &[u8],
    format: &'static str,
) -> Result<&'a [u8], LocalStreamError> {
    crate::persistence::header(encoded, domain, version).map_err(|error| match error {
        StreamError::Unsupported => LocalStreamError::UnsupportedVersion { format },
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
