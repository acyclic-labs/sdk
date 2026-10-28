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

/// The store's revision incarnation. It survives a reopen after a run whose
/// every revision was device-flushed before issue, so tokens and replayed
/// receipts keep the incarnation they were committed at. A fresh or wiped
/// store issues a new one, so tokens from earlier data refuse; so does a store
/// whose record was removed, either by hand or by a run that issued revisions
/// a crash can lose (see [`forget_incarnation`]). A damaged record is corrupt.
fn incarnation(root: &Path, fresh: bool) -> Result<u128, LocalStreamError> {
    let path = root.join(INCARNATION_FILE);
    if !fresh {
        match std::fs::read(&path) {
            Ok(recorded) => {
                let recorded = <[u8; 16]>::try_from(recorded.as_slice())
                    .map_err(|_| LocalStreamError::Corrupt)?;
                return Ok(u128::from_be_bytes(recorded));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    let incarnation = u128::from_be_bytes(uuid::Uuid::new_v4().into_bytes());
    let temporary = root.join(INCARNATION_TEMPORARY);
    {
        let mut file = File::create(&temporary)?;
        acyclic_native_runtime::write_file_all(&mut file, &incarnation.to_be_bytes())?;
        sync_file(&file, LocalDurability::FullFlush)?;
    }
    acyclic_native_runtime::rename_file(&temporary, &path)?;
    sync_directory(root, LocalDurability::FullFlush)?;
    Ok(incarnation)
}

/// Durably removes the incarnation record before the first frame whose
/// revision may be issued ahead of a device flush. A crash can then lose that
/// revision, and recovery would reissue its index to another write; with the
/// record gone, the next open issues a new incarnation instead, so every
/// token from this run refuses rather than naming different data.
fn forget_incarnation(root: &Path) -> Result<(), LocalStreamError> {
    let path = root.join(INCARNATION_FILE);
    match std::fs::remove_file(&path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    }
    sync_directory(root, LocalDurability::FullFlush)
}
