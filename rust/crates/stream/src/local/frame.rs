use super::*;

/// Only a closed prepared frame may omit fresh admission.
pub(super) fn record_frame(
    meter: Option<&Arc<dyn NativeReadMeter>>,
    late: &mut Option<&mut std::io::Result<()>>,
    encoded: u64, copied: u64, hashed: u64,
) -> std::io::Result<()> {
    if let Some(meter) = meter {
        let recorded = meter.record_codec(encoded, copied, hashed);
        if let Some(late) = late.as_deref_mut() {
            keep_observation(late, recorded);
        } else {
            recorded?;
            meter.check()?;
        }
    }
    Ok(())
}
pub(super) fn encode_frame(
    command: &Command,
    committed_at_millis: u64,
    meter: Option<&Arc<dyn NativeReadMeter>>,
) -> Result<MemoryOutput<Vec<u8>>, LocalStreamError> {
    if let Some(meter) = meter { meter.admit_examination(1, 0)?; }
    let journal = crate::wire_codec::CanonicalWire::Journal(command);
    if let Some(meter) = meter {
        meter.record_examination(1, 0)?;
        meter.check()?;
    }
    if let Some(meter) = meter {
        meter.admit_examination(1, 0)?;
    }
    let encoded_length = journal.encoded_len(None, meter)?;
    if let Some(meter) = meter {
        meter.record_examination(1, 0)?;
        meter.check()?;
    }
    if encoded_length > MAX_COMMAND_BYTES {
        return Err(LocalStreamError::InvalidLimits);
    }
    let capacity = encoded_length.checked_add(4 + FRAME_TIME_BYTES + FRAME_CHECKSUM_BYTES)
        .ok_or(LocalStreamError::InvalidLimits)?;
    let mut frame = crate::memory::vec(meter, capacity)?;
    write_frame(&mut frame.value, &journal, committed_at_millis, encoded_length,
        meter, None)?;
    Ok(frame)
}

pub(super) fn write_frame(
    encoded: &mut Vec<u8>,
    journal: &crate::wire_codec::CanonicalWire<'_>,
    committed_at_millis: u64,
    encoded_length: usize,
    meter: Option<&Arc<dyn NativeReadMeter>>,
    mut late: Option<&mut std::io::Result<()>>,
) -> Result<(), LocalStreamError> {
    let capacity = encoded_length.checked_add(4 + FRAME_TIME_BYTES + FRAME_CHECKSUM_BYTES)
        .ok_or(LocalStreamError::InvalidLimits)?;
    let length = u32::try_from(capacity - 4 - FRAME_CHECKSUM_BYTES)
        .map_err(|_| LocalStreamError::InvalidLimits)?.to_le_bytes();
    if capacity > encoded.capacity() { return Err(LocalStreamError::InvalidLimits); }
    encoded.clear();
    if late.is_none() && let Some(meter) = meter {
        meter.admit_codec(encoded_length as u64, capacity as u64, 0)?;
    }
    encoded.extend_from_slice(&length);
    encoded.extend_from_slice(&committed_at_millis.to_le_bytes());
    journal.encode(encoded).map_err(|_| LocalStreamError::InvalidLimits)?;
    record_frame(meter, &mut late, encoded_length as u64, (capacity - FRAME_CHECKSUM_BYTES) as u64, 0)?;
    let command_bytes = encoded.get(4..).ok_or(LocalStreamError::InvalidLimits)?;
    let checksum = checksum(FRAME_DOMAIN, &[&length, command_bytes], meter, late.as_deref_mut())?;
    encoded.extend_from_slice(&checksum);
    record_frame(meter, &mut late, 0, FRAME_CHECKSUM_BYTES as u64, 0)?;
    Ok(())
}

/// The largest frame the journal holds: length prefix, commit timestamp, command and checksum.
pub(super) fn maximum_frame_bytes() -> usize {
    4 + FRAME_TIME_BYTES + MAX_COMMAND_BYTES + FRAME_CHECKSUM_BYTES
}

/// The bytes of the frame whose length prefix is `length_bytes`.
pub(super) fn frame_bytes(length_bytes: &[u8; 4]) -> u64 {
    4 + u64::from(u32::from_le_bytes(*length_bytes)) + FRAME_CHECKSUM_BYTES as u64
}

/// The commit timestamp and command length a frame's prefix declares, if the journal can hold it.
pub(super) fn command_length(length_bytes: [u8; 4]) -> Option<usize> {
    usize::try_from(u32::from_le_bytes(length_bytes))
        .ok()
        .filter(|length| {
            (FRAME_TIME_BYTES + 1..=FRAME_TIME_BYTES + MAX_COMMAND_BYTES).contains(length)
        })
}

/// Reads one checksummed frame, returning `None` for a torn or invalid frame.
pub(super) fn read_frame(file: &mut File, remaining: u64) -> Result<Option<Vec<u8>>, LocalStreamError> {
    let mut length_bytes = [0_u8; 4];
    if remaining < 4 {
        return Ok(None);
    }
    file.read_exact(&mut length_bytes)?;
    let Some(length) = command_length(length_bytes) else {
        return Ok(None);
    };
    if remaining < frame_bytes(&length_bytes) {
        return Ok(None);
    }
    let mut encoded = vec![0_u8; length];
    file.read_exact(&mut encoded)?;
    let mut expected = [0_u8; FRAME_CHECKSUM_BYTES];
    file.read_exact(&mut expected)?;
    Ok((checksum(FRAME_DOMAIN, &[&length_bytes, &encoded], None, None)? == expected).then_some(encoded))
}

/// Whether a whole frame with a matching checksum starts `bytes`.
pub(super) fn frame_is_valid(bytes: &[u8]) -> bool {
    let Some(length_bytes) = bytes.first_chunk::<4>() else {
        return false;
    };
    let Some(length) = command_length(*length_bytes) else {
        return false;
    };
    let Some(encoded) = bytes.get(4..4 + length) else {
        return false;
    };
    bytes
        .get(4 + length..4 + length + FRAME_CHECKSUM_BYTES)
        .is_some_and(|expected| expected == checksum(FRAME_DOMAIN, &[length_bytes, encoded], None, None).expect("unmetered checksum"))
}

pub(super) fn validate_limits(limits: LocalStreamLimits) -> Result<(), LocalStreamError> {
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

pub(super) fn encode_limits(limits: LocalStreamLimits) -> Result<[u8; LIMITS_BYTES], LocalStreamError> {
    let memory = limits.memory;
    let values = [
        u64::try_from(memory.paths).map_err(|_| LocalStreamError::InvalidLimits)?,
        u64::try_from(memory.path_bytes).map_err(|_| LocalStreamError::InvalidLimits)?,
        u64::try_from(memory.records).map_err(|_| LocalStreamError::InvalidLimits)?,
        u64::try_from(memory.payload_bytes).map_err(|_| LocalStreamError::InvalidLimits)?,
        u64::try_from(memory.commits).map_err(|_| LocalStreamError::InvalidLimits)?,
        u64::try_from(memory.idempotency_results).map_err(|_| LocalStreamError::InvalidLimits)?,
        limits.journal_operations,
        limits.journal_bytes,
    ];
    let mut encoded = [0_u8; LIMITS_BYTES];
    for (destination, value) in encoded.chunks_exact_mut(8).zip(values) {
        destination.copy_from_slice(&value.to_le_bytes());
    }
    Ok(encoded)
}

pub(super) fn encode_header(limits: LocalStreamLimits, epoch: u64) -> Result<[u8; HEADER_BYTES], LocalStreamError> {
    let mut encoded = [0_u8; HEADER_BYTES];
    let (magic, rest) = encoded.split_at_mut(HEADER_MAGIC.len());
    magic.copy_from_slice(HEADER_MAGIC);
    let (configuration, time) = rest.split_at_mut(LIMITS_BYTES);
    configuration.copy_from_slice(&encode_limits(limits)?);
    time.copy_from_slice(&epoch.to_le_bytes());
    Ok(encoded)
}

/// The epoch of a whole journal header written for `limits`.
pub(super) fn decode_header(header: &[u8], limits: LocalStreamLimits) -> Option<u64> {
    let rest = header.strip_prefix(HEADER_MAGIC)?;
    let (found, epoch) = rest.split_at_checked(LIMITS_BYTES)?;
    (found == encode_limits(limits).ok()?).then_some(())?;
    Some(u64::from_le_bytes(epoch.try_into().ok()?))
}

pub(super) fn checksum(
    domain: &[u8], parts: &[&[u8]],
    meter: Option<&Arc<dyn NativeReadMeter>>, mut late: Option<&mut std::io::Result<()>>,
) -> std::io::Result<[u8; 32]> {
    let hashed = domain.len() + parts.iter().map(|part| part.len()).sum::<usize>();
    if late.is_none() && let Some(meter) = meter {
        meter.admit_codec(0, 0, hashed as u64)?;
    }
    let mut hasher = Sha256::new();
    hasher.update(domain);
    for part in parts { hasher.update(part); }
    let checksum = hasher.finalize().into();
    record_frame(meter, &mut late, 0, 0, hashed as u64)?;
    Ok(checksum)
}
