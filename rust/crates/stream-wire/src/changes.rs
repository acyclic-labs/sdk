//! Canonical decoding for retained and current Stream change records.

use crate::headers::MAX_RECORD_KEY_BYTES;

/// Rejection while decoding a change record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChangeDecodeError {
    /// The bytes or their supplied index are noncanonical.
    Invalid,
    /// A key or aggregate budget was exceeded.
    Limit,
}

struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    /// A minimally encoded varint; any other encoding would change identity.
    fn number(&mut self) -> Result<usize, ChangeDecodeError> {
        let before = self.0.len();
        let value =
            prost::encoding::decode_varint(&mut self.0).map_err(|_| ChangeDecodeError::Invalid)?;
        if prost::encoding::encoded_len_varint(value) != before - self.0.len() {
            return Err(ChangeDecodeError::Invalid);
        }
        usize::try_from(value).map_err(|_| ChangeDecodeError::Limit)
    }
    fn bytes(&mut self) -> Result<&'a [u8], ChangeDecodeError> {
        let length = self.number()?;
        let (value, rest) = self
            .0
            .split_at_checked(length)
            .ok_or(ChangeDecodeError::Invalid)?;
        self.0 = rest;
        Ok(value)
    }

    // The v1 producer used canonical protobuf: ordered length-delimited fields,
    // minimal lengths and omitted empty scalar fields. Keep the retained bytes.
    fn field(&mut self, tag: u8) -> Result<Option<&'a [u8]>, ChangeDecodeError> {
        if self.0.first() != Some(&tag) {
            return Ok(None);
        }
        self.0 = self.0.get(1..).ok_or(ChangeDecodeError::Invalid)?;
        let value = self.bytes()?;
        if value.is_empty() {
            return Err(ChangeDecodeError::Invalid);
        }
        Ok(Some(value))
    }
}

/// Visits a retained protobuf change record and returns its context.
///
/// # Errors
/// Returns a grammar rejection or a rejection from the visitor.
pub fn visit_v1<'a>(
    bytes: &'a [u8],
    mut visit: impl FnMut(&[u8], &'a str, &'a [u8]) -> Result<(), ChangeDecodeError>,
) -> Result<&'a [u8], ChangeDecodeError> {
    let mut reader = Reader(bytes);
    let context = reader.field(10)?.unwrap_or_default();
    let mut previous: Option<&[u8]> = None;
    let mut count = 0;
    while let Some(bytes) = reader.field(18)? {
        let mut entry = Reader(bytes);
        let key = entry.field(10)?.unwrap_or_default();
        let operation = entry.field(18)?.ok_or(ChangeDecodeError::Invalid)?;
        let operation = std::str::from_utf8(operation).map_err(|_| ChangeDecodeError::Invalid)?;
        let metadata = entry.field(26)?.unwrap_or_default();
        count += 1;
        if key.len() > MAX_RECORD_KEY_BYTES
            || previous.is_some_and(|previous| previous >= key)
            || operation.chars().any(char::is_control)
            || !entry.0.is_empty()
        {
            return Err(ChangeDecodeError::Invalid);
        }
        visit(key, operation, metadata)?;
        previous = Some(key);
    }
    if count == 0 || !reader.0.is_empty() {
        return Err(ChangeDecodeError::Invalid);
    }
    Ok(context)
}

/// Visits each entry of a canonical value with its full key, rejecting every
/// noncanonical encoding.
///
/// # Errors
/// Returns a grammar or key-budget rejection, or a rejection from the visitor.
pub fn visit_v2<'a, K: AsRef<[u8]>>(
    bytes: &'a [u8],
    expected: Option<&[K]>,
    mut remaining_keys: Option<usize>,
    mut visit: impl FnMut(&[u8], &'a str, &'a [u8]) -> Result<(), ChangeDecodeError>,
) -> Result<&'a [u8], ChangeDecodeError> {
    let mut reader = Reader(bytes);
    let context = reader.bytes()?;
    let count = reader.number()?;
    if count == 0 {
        return Err(ChangeDecodeError::Invalid);
    }
    // Native validation already owns the admitted immutable full keys. Compare
    // their prefix/suffix slices directly, without a second allocation. Owned
    // decoders reconstruct into their own scratch alongside their owned output.
    let mut reconstructed = Vec::new();
    for index in 0..count {
        let common = reader.number()?;
        let suffix = reader.bytes()?;
        let operation = reader.bytes()?;
        let metadata = reader.bytes()?;
        let total = common
            .checked_add(suffix.len())
            .ok_or(ChangeDecodeError::Invalid)?;
        if total > MAX_RECORD_KEY_BYTES {
            return Err(ChangeDecodeError::Limit);
        }
        if let Some(remaining) = &mut remaining_keys {
            *remaining = remaining
                .checked_sub(total)
                .ok_or(ChangeDecodeError::Limit)?;
        }
        let previous = match expected {
            Some(keys) if index != 0 => keys
                .get(index - 1)
                .ok_or(ChangeDecodeError::Invalid)?
                .as_ref(),
            Some(_) => &[],
            None => reconstructed.as_slice(),
        };
        // The shared length is exactly the common prefix, so the suffix's
        // first byte must differ from the predecessor's next one; together
        // with that byte's order this makes keys strictly increasing.
        let canonical = if index == 0 {
            common == 0
        } else {
            common <= previous.len()
                && match (
                    suffix.first(),
                    previous.get(common..).and_then(<[u8]>::first),
                ) {
                    (Some(next), Some(previous)) => next > previous,
                    (Some(_), None) => true,
                    (None, _) => false,
                }
        };
        let operation = std::str::from_utf8(operation).map_err(|_| ChangeDecodeError::Invalid)?;
        if !canonical || operation.is_empty() || operation.chars().any(char::is_control) {
            return Err(ChangeDecodeError::Invalid);
        }
        if let Some(keys) = expected {
            let key = keys.get(index).ok_or(ChangeDecodeError::Invalid)?.as_ref();
            if key.len() != total
                || key.get(..common) != previous.get(..common)
                || key.get(common..) != Some(suffix)
            {
                return Err(ChangeDecodeError::Invalid);
            }
            visit(key, operation, metadata)?;
        } else {
            reconstructed.truncate(common);
            reconstructed.extend_from_slice(suffix);
            visit(&reconstructed, operation, metadata)?;
        }
    }
    if !reader.0.is_empty() {
        return Err(ChangeDecodeError::Invalid);
    }
    Ok(context)
}
