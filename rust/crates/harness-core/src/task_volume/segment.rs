use super::{Channel, Error, FileKey, FileValue, NativeWork, Result, Write};
use acyclic_stream::OwnedBytes;
use serde::{Deserialize, Serialize};

const MAGIC: &[u8; 8] = b"ACYTASK1";
const PREFIX: usize = 12;

#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
#[path = "segment_tests.rs"]
mod tests;

#[derive(Serialize, Deserialize)]
struct Entry {
    key: FileKey,
    channel: Option<Channel>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    requester: Option<crate::TaskId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    publication: Option<crate::OperationId>,
}

/// Exact framed size, for rejecting oversized requests before publication.
#[doc(hidden)]
pub fn record_bytes(file: &FileValue) -> Result<usize> {
    let metadata = serde_json::to_vec(&Entry {
        key: file.key.clone(),
        channel: file.channel,
        requester: file.requester,
        publication: file.publication,
    })
    .map_err(|_| Error::InvalidSegment)?;
    PREFIX
        .checked_add(metadata.len())
        .and_then(|size| size.checked_add(file.bytes.len()))
        .ok_or(Error::LimitExceeded)
}

pub(super) fn encode(write: &Write, work: NativeWork<'_>) -> Result<(OwnedBytes, FileValue)> {
    let entry = Entry {
        key: write.key.clone(),
        channel: write.channel,
        requester: write.requester,
        publication: write.publication,
    };
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let io = work
        .map(crate::executor::PreparationIo::from_native_work)
        .transpose()?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let metadata = if let Some(io) = &io {
        io.encode(&entry, acyclic_stream::MAX_RECORD_BYTES as u64, false)?
    } else {
        serde_json::to_vec(&entry)
            .map_err(|_| Error::InvalidSegment)?
            .into()
    };
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let metadata: OwnedBytes = {
        let _ = work;
        serde_json::to_vec(&entry)
            .map_err(|_| Error::InvalidSegment)?
            .into()
    };
    let total = PREFIX
        .checked_add(metadata.len())
        .and_then(|n| n.checked_add(write.bytes.len()))
        .ok_or(Error::LimitExceeded)?;
    if total > acyclic_stream::MAX_RECORD_BYTES {
        return Err(Error::LimitExceeded);
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = &io {
        io.retain_owned(total as u64).map_err(crate::Error::from)?;
        io.allocation_attempt(total as u64)?;
        io.charge(acyclic_fs::WorkCounters {
            bytes_copied: total as u64,
            ..Default::default()
        })
        .map_err(crate::Error::from)?;
    }
    let mut bytes = Vec::new();
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    bytes.try_reserve_exact(total).map_err(crate::Error::from)?;
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    bytes
        .try_reserve_exact(total)
        .map_err(|_| Error::LimitExceeded)?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = &io {
        io.retain_owned((bytes.capacity() - total) as u64)
            .map_err(crate::Error::from)?;
    }
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(
        &u32::try_from(metadata.len())
            .map_err(|_| Error::LimitExceeded)?
            .to_le_bytes(),
    );
    bytes.extend_from_slice(&metadata);
    bytes.extend_from_slice(&write.bytes);
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let bytes = crate::executor::with_history_io(io.as_ref(), |history| {
        crate::store::operations::owned_bytes_from_retained_vec(bytes, history)
            .map(|(bytes, _)| bytes)
    })?;
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let bytes: OwnedBytes = bytes.into();
    // The input was already validated. Reuse its typed metadata rather than
    // serializing and decoding another owned copy before publication.
    let file = FileValue {
        key: entry.key,
        channel: entry.channel,
        requester: entry.requester,
        publication: entry.publication,
        bytes: bytes.slice(PREFIX + metadata.len()..),
    };
    Ok((bytes, file))
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
        requester: entry.requester,
        publication: entry.publication,
        bytes: bytes.slice(offset..),
    };
    file.validate()?;
    Ok(file)
}
