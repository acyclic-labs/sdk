use super::*;

pub(super) struct Snapshot {
    pub(super) epoch: u64,
    pub(super) state: Vec<u8>,
}

pub(super) fn encode_snapshot(
    limits: LocalStreamLimits,
    epoch: u64,
    state: &[u8],
    meter: Option<&Arc<dyn NativeReadMeter>>,
) -> Result<MemoryOutput<Vec<u8>>, LocalStreamError> {
    let capacity = SNAPSHOT_HEADER_BYTES.checked_add(state.len())
        .and_then(|length| length.checked_add(32)).ok_or(LocalStreamError::InvalidLimits)?;
    let mut encoded = crate::memory::vec(meter, capacity)?;
    if let Some(meter) = meter { meter.admit_codec(capacity as u64, capacity as u64, 0)?; }
    encoded.value.extend_from_slice(SNAPSHOT_MAGIC);
    encoded.value.extend_from_slice(&encode_limits(limits)?);
    encoded.value.extend_from_slice(&epoch.to_le_bytes());
    encoded.value.extend_from_slice(state);
    if let Some(meter) = meter {
        meter.record_codec(encoded.value.len() as u64, encoded.value.len() as u64, 0)?;
        meter.check()?;
    }
    let checksum = checksum(SNAPSHOT_DOMAIN, &[&encoded.value], meter, None)?;
    encoded.value.extend_from_slice(&checksum);
    if let Some(meter) = meter {
        meter.record_codec(32, 32, 0)?;
        meter.check()?;
    }
    Ok(encoded)
}

/// The snapshot below `root`, if one was installed.
pub(super) fn read_snapshot(
    root: &Path,
    limits: LocalStreamLimits,
) -> Result<Option<Snapshot>, LocalStreamError> {
    let encoded = match std::fs::read(root.join(SNAPSHOT_FILE)) {
        Ok(encoded) => encoded,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let (body, expected_checksum) = encoded
        .split_last_chunk::<32>()
        .ok_or(LocalStreamError::Corrupt)?;
    if checksum(SNAPSHOT_DOMAIN, &[body], None, None)? != *expected_checksum {
        return Err(LocalStreamError::Corrupt);
    }
    let rest = body
        .strip_prefix(SNAPSHOT_MAGIC)
        .ok_or(LocalStreamError::Corrupt)?;
    let (found, rest) = rest
        .split_at_checked(LIMITS_BYTES)
        .ok_or(LocalStreamError::Corrupt)?;
    if found != encode_limits(limits)? {
        return Err(LocalStreamError::Corrupt);
    }
    let (epoch, rest) = rest
        .split_first_chunk::<8>()
        .ok_or(LocalStreamError::Corrupt)?;
    let state = rest;
    Ok(Some(Snapshot {
        epoch: u64::from_le_bytes(*epoch),
        state: state.to_vec(),
    }))
}
