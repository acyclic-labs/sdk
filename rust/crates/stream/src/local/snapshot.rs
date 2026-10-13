struct Snapshot {
    epoch: u64,
    state: Vec<u8>,
}

fn snapshot_checksum(body: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"acyclic-stream-local-snapshot-v1\0");
    hasher.update(body);
    hasher.finalize().into()
}

fn encode_snapshot(
    limits: LocalStreamLimits,
    epoch: u64,
    state: &[u8],
) -> Result<Vec<u8>, LocalStreamError> {
    let mut encoded = Vec::with_capacity(SNAPSHOT_HEADER_BYTES + state.len() + 32);
    encoded.extend_from_slice(SNAPSHOT_MAGIC);
    encoded.extend_from_slice(&encode_limits(limits)?);
    encoded.extend_from_slice(&epoch.to_le_bytes());
    encoded.extend_from_slice(state);
    let checksum = snapshot_checksum(&encoded);
    encoded.extend_from_slice(&checksum);
    Ok(encoded)
}

/// The snapshot below `root`, if one was installed. It is written whole
fn read_snapshot(
    root: &Path,
    limits: LocalStreamLimits,
) -> Result<Option<Snapshot>, LocalStreamError> {
    let encoded = match std::fs::read(root.join(SNAPSHOT_FILE)) {
        Ok(encoded) => encoded,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let (body, checksum) = encoded
        .split_last_chunk::<32>()
        .ok_or(LocalStreamError::Corrupt)?;
    if snapshot_checksum(body) != *checksum {
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
