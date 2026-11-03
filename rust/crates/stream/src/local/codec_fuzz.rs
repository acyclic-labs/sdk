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
    // Version admission precedes payload interpretation, including checksums.
    let changed = (incarnation as u8).max(1);
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
            if let Ok(command) = decode_command(command) {
                let canonical = command.encode_to_vec();
                if decode_command(&canonical).is_err() {
                    return false;
                }
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

pub(crate) fn header() -> Result<Vec<u8>, StreamError> {
    encode_header(LocalStreamLimits::default(), 1).map_err(|_| StreamError::InvalidArgument)
}

pub(crate) fn snapshot(state: &[u8]) -> Result<Vec<u8>, StreamError> {
    encode_snapshot(LocalStreamLimits::default(), 1, state)
        .map_err(|_| StreamError::InvalidArgument)
}

pub(crate) fn incarnation() -> Vec<u8> {
    encode_incarnation(7).to_vec()
}
