use super::*;

pub(crate) fn check(bytes: &[u8]) -> bool {
    let _ = decode_header(bytes, LocalStreamLimits::default());
    if let Ok(value) = decode_snapshot(bytes, LocalStreamLimits::default()) {
        if !encode_snapshot(LocalStreamLimits::default(), value.epoch, &value.state)
            .is_ok_and(|encoded| encoded == bytes)
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
