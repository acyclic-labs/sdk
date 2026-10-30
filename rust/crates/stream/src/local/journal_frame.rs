/// One whole journal frame whose checksum matched.
struct JournalFrame {
    length: [u8; 4],
    command: Vec<u8>,
}

/// Reads the frame at the file's cursor, `remaining` bytes before its end,
fn read_frame(file: &File, remaining: u64) -> Result<Option<JournalFrame>, LocalStreamError> {
    let Some(command) = JOURNAL_FRAME.read(file, remaining)? else {
        return Ok(None);
    };
    let length = u32::try_from(command.len())
        .map_err(|_| LocalStreamError::Corrupt)?
        .to_le_bytes();
    Ok(Some(JournalFrame { length, command }))
}

/// Whether a whole frame with a matching checksum starts `bytes`.
fn frame_is_valid(bytes: &[u8]) -> bool {
    JOURNAL_FRAME.is_valid(bytes)
}
