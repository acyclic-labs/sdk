use super::{Channel, Error, FileKey, FileValue, Result, Write};
use acyclic_stream::OwnedBytes;
use serde::{Deserialize, Serialize};

const MAGIC: &[u8; 8] = b"ACYTASK1";
const PREFIX: usize = 12;

#[derive(Serialize, Deserialize)]
struct Entry {
    key: FileKey,
    channel: Option<Channel>,
}

pub(super) fn encode(write: &Write) -> Result<OwnedBytes> {
    let entry = Entry {
        key: write.key.clone(),
        channel: write.channel,
    };
    let metadata = serde_json::to_vec(&entry).map_err(|_| Error::InvalidSegment)?;
    let total = PREFIX
        .checked_add(metadata.len())
        .and_then(|n| n.checked_add(write.bytes.len()))
        .ok_or(Error::LimitExceeded)?;
    if total > acyclic_stream::MAX_RECORD_BYTES {
        return Err(Error::LimitExceeded);
    }
    let mut bytes = Vec::with_capacity(total);
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(
        &u32::try_from(metadata.len())
            .map_err(|_| Error::LimitExceeded)?
            .to_le_bytes(),
    );
    bytes.extend_from_slice(&metadata);
    bytes.extend_from_slice(&write.bytes);
    Ok(bytes.into())
}

pub(super) fn decode(bytes: &OwnedBytes) -> Result<FileValue> {
    if bytes.get(..8) != Some(MAGIC.as_slice()) {
        return Err(Error::InvalidSegment);
    }
    let length = bytes
        .get(8..PREFIX)
        .and_then(|v| v.try_into().ok())
        .map(u32::from_le_bytes)
        .ok_or(Error::InvalidSegment)? as usize;
    let offset = PREFIX.checked_add(length).ok_or(Error::InvalidSegment)?;
    let metadata = bytes.get(PREFIX..offset).ok_or(Error::InvalidSegment)?;
    let entry: Entry = serde_json::from_slice(metadata).map_err(|_| Error::InvalidSegment)?;
    let file = FileValue {
        key: entry.key,
        channel: entry.channel,
        bytes: bytes.slice(offset..),
    };
    file.validate()?;
    Ok(file)
}
