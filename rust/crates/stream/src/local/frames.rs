fn validate_limits(limits: LocalStreamLimits) -> Result<(), LocalStreamError> {
    let memory = limits.memory;
    if memory.paths == 0
        || memory.path_bytes == 0
        || memory.records == 0
        || memory.payload_bytes == 0
        || memory.commits == 0
        || memory.idempotency_results == 0
        || limits.journal_operations == 0
        || limits.journal_bytes
            < u64::try_from(HEADER_BYTES).map_err(|_| LocalStreamError::InvalidLimits)?
    {
        Err(LocalStreamError::InvalidLimits)
    } else {
        Ok(())
    }
}

fn encode_limits(limits: LocalStreamLimits) -> Result<Vec<u8>, LocalStreamError> {
    let mut encoded = Vec::with_capacity(LIMITS_BYTES);
    for value in [
        limits.memory.paths,
        limits.memory.path_bytes,
        limits.memory.records,
        limits.memory.payload_bytes,
        limits.memory.commits,
        limits.memory.idempotency_results,
    ] {
        encoded.extend_from_slice(
            &u64::try_from(value)
                .map_err(|_| LocalStreamError::InvalidLimits)?
                .to_le_bytes(),
        );
    }
    encoded.extend_from_slice(&limits.journal_operations.to_le_bytes());
    encoded.extend_from_slice(&limits.journal_bytes.to_le_bytes());
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
    if found != encode_limits(limits)? {
        return Err(LocalStreamError::Corrupt);
    }
    Ok(u64::from_le_bytes(
        epoch.try_into().map_err(|_| LocalStreamError::Corrupt)?,
    ))
}
