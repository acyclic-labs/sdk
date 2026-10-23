//! Borrowed canonical validation, including the prepared native commit ingress.
use super::{RecordHeaders, StreamError};

pub(super) fn validate(mut bytes: &[u8], headers: &RecordHeaders) -> Result<(), StreamError> {
    super::bounded(bytes, headers)?;
    if headers.keys().is_empty() {
        return Err(StreamError::InvalidArgument);
    }
    field(&mut bytes, 10)?; // Optional shared context.
    for expected in headers.keys() {
        let mut entry = field(&mut bytes, 18)?.ok_or(StreamError::InvalidArgument)?;
        let key = field(&mut entry, 10)?.unwrap_or_default();
        let operation = field(&mut entry, 18)?.ok_or(StreamError::InvalidArgument)?;
        let operation = std::str::from_utf8(operation).map_err(|_| StreamError::InvalidArgument)?;
        field(&mut entry, 26)?; // Optional path-local metadata.
        if key != expected.as_ref() || operation.chars().any(char::is_control) || !entry.is_empty()
        {
            return Err(StreamError::InvalidArgument);
        }
    }
    if !bytes.is_empty() {
        return Err(StreamError::InvalidArgument);
    }
    Ok(())
}

/// These bounded messages contain only length-delimited fields with one-byte
/// tags. Prost omits empty scalar fields; accepting them would change retry
/// identity after projection. Every accepted length is minimally encoded.
fn field<'a>(bytes: &mut &'a [u8], tag: u8) -> Result<Option<&'a [u8]>, StreamError> {
    let Some((&actual, rest)) = bytes.split_first() else {
        return Ok(None);
    };
    if actual != tag {
        return Ok(None);
    }
    *bytes = rest;
    let before = bytes.len();
    let length = prost::encoding::decode_varint(bytes).map_err(|_| StreamError::InvalidArgument)?;
    let encoded = before - bytes.len();
    if length == 0 || prost::encoding::encoded_len_varint(length) != encoded {
        return Err(StreamError::InvalidArgument);
    }
    let length = usize::try_from(length).map_err(|_| StreamError::LimitExceeded)?;
    let (value, rest) = bytes
        .split_at_checked(length)
        .ok_or(StreamError::InvalidArgument)?;
    *bytes = rest;
    Ok(Some(value))
}
