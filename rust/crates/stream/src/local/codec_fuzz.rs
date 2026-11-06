use super::*;

pub(crate) fn check(bytes: &[u8]) -> bool {
    let limits = LocalStreamLimits::default();
    let incarnation = bytes
        .iter()
        .take(16)
        .fold(0u128, |value, byte| (value << 8) | u128::from(*byte));
    let epoch = incarnation as u64;
    // Challenge admission with the actual writers as well as arbitrary bytes:
    // rejecting every input must not satisfy a decoder's round-trip oracle.
    let Ok(mut header) = encode_header(limits, epoch) else {
        return false;
    };
    if !decode_header(&header, limits).is_ok_and(|value| value == epoch) {
        return false;
    }
    let Ok(mut snapshot) = encode_snapshot(limits, epoch, bytes) else {
        return false;
    };
    let mut encoded_incarnation = encode_incarnation(incarnation);
    if !decode_snapshot(&snapshot, limits)
        .is_ok_and(|value| value.epoch == epoch && value.state == bytes)
        || !decode_incarnation(&encoded_incarnation).is_ok_and(|value| value == incarnation)
    {
        return false;
    }
    let changed = (incarnation as u8).max(1);
    // Persisted capacities belong to the opening configuration. Even a valid
    // container must refuse a different caller's limits.
    let other_limits = LocalStreamLimits {
        compaction_operations: if limits.compaction_operations == 1 {
            2
        } else {
            1
        },
        ..limits
    };
    if !matches!(
        decode_header(&header, other_limits),
        Err(LocalStreamError::Corrupt)
    ) || !matches!(
        decode_snapshot(&snapshot, other_limits),
        Err(LocalStreamError::Corrupt)
    ) {
        return false;
    }
    // Both containers share encode_limits. Challenge every other field through
    // the fixed-size header, without hashing the snapshot payload again.
    for field in 0..7 {
        let mut other_limits = limits;
        if field == 6 {
            other_limits.compaction_bytes = if limits.compaction_bytes == HEADER_BYTES as u64 {
                HEADER_BYTES as u64 + 1
            } else {
                HEADER_BYTES as u64
            };
        } else {
            let capacity = match field {
                0 => &mut other_limits.memory.paths,
                1 => &mut other_limits.memory.path_bytes,
                2 => &mut other_limits.memory.records,
                3 => &mut other_limits.memory.payload_bytes,
                4 => &mut other_limits.memory.commits,
                _ => &mut other_limits.memory.idempotency_results,
            };
            *capacity = Some((incarnation as usize).max(1));
        }
        if !matches!(
            decode_header(&header, other_limits),
            Err(LocalStreamError::Corrupt)
        ) {
            return false;
        }
    }
    // Corrupt the actual writer's checksum without changing its payload.
    let checksum_byte = snapshot.len() - 1;
    snapshot[checksum_byte] ^= changed;
    if !matches!(
        decode_snapshot(&snapshot, limits),
        Err(LocalStreamError::Corrupt)
    ) {
        return false;
    }
    snapshot[checksum_byte] ^= changed;
    // Version admission precedes payload interpretation, including checksums.
    header[HEADER_MAGIC.len() - 2] ^= changed;
    snapshot[SNAPSHOT_MAGIC.len() - 2] ^= changed;
    encoded_incarnation[INCARNATION_MAGIC.len() - 2] ^= changed;
    if !matches!(
        decode_header(&header, limits),
        Err(LocalStreamError::UnsupportedVersion { format: "journal" })
    ) || !matches!(
        decode_snapshot(&snapshot, limits),
        Err(LocalStreamError::UnsupportedVersion { format: "snapshot" })
    ) || !matches!(
        decode_incarnation(&encoded_incarnation),
        Err(LocalStreamError::UnsupportedVersion {
            format: "incarnation"
        })
    ) {
        return false;
    }
    if let Ok(epoch) = decode_header(bytes, limits) {
        if !encode_header(limits, epoch).is_ok_and(|encoded| encoded == bytes) {
            return false;
        }
    }
    if let Ok(value) = decode_snapshot(bytes, limits) {
        if !encode_snapshot(limits, value.epoch, &value.state).is_ok_and(|encoded| encoded == bytes)
        {
            return false;
        }
    }
    if let Ok(value) = decode_incarnation(bytes) {
        if encode_incarnation(value) != bytes {
            return false;
        }
    }
    let Ok(read) = read_frame(std::io::Cursor::new(bytes), bytes.len() as u64) else {
        return false;
    };
    if read.is_some() != frame_is_valid(bytes) {
        return false;
    }
    if let Some(frame) = read {
        // The checksum parser admits the enclosing frame independently of
        // command semantics, as recovery does. Arbitrary bytes reach both.
        if let Some(command) = frame.command.get(FRAME_TIME_BYTES..) {
            if !crate::codec_fuzz::command(command) {
                return false;
            }
        }
    }
    true
}

pub(crate) fn frame(command: &WriteRequest) -> Result<Vec<u8>, StreamError> {
    PreparedFrame::encode(command, 42, None)
        .map(|frame| frame.encoded)
        .map_err(|_| StreamError::InvalidArgument)
}

#[cfg(test)]
pub(crate) fn repeated_command_frame(command: &WriteRequest) -> Result<Vec<u8>, StreamError> {
    let encoded = command.encode_to_vec();
    let payload = crate::persistence::header(&encoded, b"ACYCLIC-STREAM-COMMAND-V", b"1\0")?;
    // Repeat the real writer's protobuf operation, then frame it with the
    // production checksum writer. A valid frame need not be a canonical command.
    let mut frame = Vec::new();
    JOURNAL_FRAME
        .encode(&[&42u64.to_le_bytes(), &encoded, payload], &mut frame)
        .map_err(|_| StreamError::InvalidArgument)?;
    Ok(frame)
}

pub(crate) fn header() -> Result<Vec<u8>, StreamError> {
    encode_header(LocalStreamLimits::default(), 1)
        .map(Vec::from)
        .map_err(|_| StreamError::InvalidArgument)
}

pub(crate) fn snapshot(state: &[u8]) -> Result<Vec<u8>, StreamError> {
    encode_snapshot(LocalStreamLimits::default(), 1, state)
        .map_err(|_| StreamError::InvalidArgument)
}

pub(crate) fn incarnation() -> Vec<u8> {
    encode_incarnation(7).to_vec()
}
