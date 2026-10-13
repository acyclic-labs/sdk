/// One whole journal frame whose checksum matched.
struct JournalFrame {
    length: [u8; 4],
    command: Vec<u8>,
}

/// Reads the frame at the file's cursor, `remaining` bytes before its end,
fn read_frame(file: &mut File, remaining: u64) -> Result<Option<JournalFrame>, LocalStreamError> {
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
    let mut checksum = [0_u8; FRAME_CHECKSUM_BYTES];
    file.read_exact(&mut checksum)?;
    Ok(
        (frame_checksum(&length_bytes, &encoded) == checksum).then_some(JournalFrame {
            length: length_bytes,
            command: encoded,
        }),
    )
}

/// Whether a whole frame with a matching checksum starts `bytes`.
fn frame_is_valid(bytes: &[u8]) -> bool {
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
        .is_some_and(|checksum| checksum == frame_checksum(length_bytes, encoded))
}
