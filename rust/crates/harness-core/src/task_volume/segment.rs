use super::{Channel, Error, FileKey, FileValue, NativeWork, Result, Write};
use acyclic_stream::OwnedBytes;
use serde::{Deserialize, Serialize};

const MAGIC: &[u8; 8] = b"ACYTASK1";
const PREFIX: usize = 12;

#[cfg(test)]
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[path = "segment_tests.rs"]
mod tests;

#[derive(Serialize, Deserialize, crate::HeapLayout)]
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
    let metadata = encode_json(&entry, work)?;
    let total = PREFIX
        .checked_add(metadata.len())
        .and_then(|n| n.checked_add(write.bytes.len()))
        .ok_or(Error::LimitExceeded)?;
    if total > acyclic_stream::MAX_RECORD_BYTES {
        return Err(Error::LimitExceeded);
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = &io {
        let slots = (2 * std::mem::size_of::<FileValue>()) as u64;
        io.retain_owned(total as u64 + slots)
            .map_err(crate::Error::from)?;
        io.allocation_attempt(total as u64)?;
        io.allocation_attempt(slots)?;
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

// File bodies and frame metadata use the same precharged codec. A native
// caller's original work ledger owns the buffer before serialization starts.
pub(super) fn encode_json<T: Serialize>(value: &T, work: NativeWork<'_>) -> Result<OwnedBytes> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        return Ok(
            crate::executor::PreparationIo::from_native_work(work)?.encode(
                value,
                acyclic_stream::MAX_RECORD_BYTES as u64,
                false,
            )?,
        );
    }
    let _ = work;
    serde_json::to_vec(value)
        .map(Into::into)
        .map_err(|_| Error::InvalidSegment)
}

pub(super) fn decode(bytes: &OwnedBytes) -> Result<FileValue> {
    let (metadata, offset) = parts(bytes)?;
    let entry: Entry = serde_json::from_slice(metadata).map_err(|_| Error::InvalidSegment)?;
    decoded(entry, bytes, offset)
}

fn parts(bytes: &OwnedBytes) -> Result<(&[u8], usize)> {
    if bytes.get(..8) != Some(MAGIC.as_slice()) {
        return Err(Error::InvalidSegment);
    }
    let length = bytes
        .get(8..PREFIX)
        .and_then(|v| v.try_into().ok())
        .map(u32::from_le_bytes)
        .ok_or(Error::InvalidSegment)? as usize;
    let offset = PREFIX.checked_add(length).ok_or(Error::InvalidSegment)?;
    Ok((
        bytes.get(PREFIX..offset).ok_or(Error::InvalidSegment)?,
        offset,
    ))
}

fn decoded(entry: Entry, bytes: &OwnedBytes, offset: usize) -> Result<FileValue> {
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

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl crate::filesystem::PreparationRecord for Entry {}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn decode_native(
    bytes: &OwnedBytes,
    io: &crate::filesystem::PreparationIo,
) -> crate::Result<(FileValue, acyclic_fs::NativeAllocation)> {
    let (metadata, offset) = parts(bytes)?;
    let (entry, mut hold) = io.decode::<Entry>(
        std::marker::PhantomData,
        metadata,
        crate::wire::ErrorCode::Storage,
    )?;
    // Closed metadata owns only strings/Arc strings. Four source lengths cover
    // their retained bytes and Arc counters without retaining decoder scratch.
    let retained = (metadata.len() as u64)
        .checked_mul(4)
        .and_then(|n| n.checked_add(std::mem::size_of::<Entry>() as u64))
        .ok_or(Error::LimitExceeded)?;
    hold.substitute(retained)?;
    let file = decoded(entry, bytes, offset)?;
    hold.into_retained()?;
    Ok((file, io.take_native_custody(retained)?))
}

// A mailbox transition is one record, including in a bounded replay slice or
// a remote follow. Two separately committed file records would expose a partial
// claim to readers even when the provider commits their batch atomically.
const PAIR_MAGIC: &[u8; 8] = b"ACYPAIR1";

pub(super) fn encode_pair(
    writes: &[Write; 2],
    work: NativeWork<'_>,
) -> Result<(OwnedBytes, Files)> {
    validate_pair(&writes[0], &writes[1])?;
    let (left, mut first) = encode(&writes[0], work)?;
    let (right, mut second) = encode(&writes[1], work)?;
    let total = PREFIX
        .checked_add(left.len())
        .and_then(|n| n.checked_add(right.len()))
        .ok_or(Error::LimitExceeded)?;
    if total > acyclic_stream::MAX_RECORD_BYTES {
        return Err(Error::LimitExceeded);
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let io = work
        .map(crate::executor::PreparationIo::from_native_work)
        .transpose()?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = &io {
        let slots = (2 * std::mem::size_of::<FileValue>()) as u64;
        io.retain_owned(total as u64 + slots)
            .map_err(crate::Error::from)?;
        io.allocation_attempt(total as u64)?;
        io.allocation_attempt(slots)?;
        io.charge(acyclic_fs::WorkCounters {
            bytes_copied: total as u64,
            ..Default::default()
        })
        .map_err(crate::Error::from)?;
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(total)
        .map_err(|_| Error::LimitExceeded)?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = &io {
        io.retain_owned((bytes.capacity() - total) as u64)
            .map_err(crate::Error::from)?;
    }
    bytes.extend_from_slice(PAIR_MAGIC);
    bytes.extend_from_slice(
        &u32::try_from(left.len())
            .map_err(|_| Error::LimitExceeded)?
            .to_le_bytes(),
    );
    bytes.extend_from_slice(&left);
    bytes.extend_from_slice(&right);
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let bytes = crate::executor::with_history_io(io.as_ref(), |history| {
        crate::store::operations::owned_bytes_from_retained_vec(bytes, history)
            .map(|(bytes, _)| bytes)
    })?;
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let bytes: OwnedBytes = bytes.into();
    first.bytes = bytes.slice(PREFIX + left.len() - first.bytes.len()..PREFIX + left.len());
    second.bytes = bytes.slice(total - second.bytes.len()..);
    Ok((
        bytes,
        Files {
            first,
            second: Some(second),
        },
    ))
}

pub(super) struct Files {
    first: FileValue,
    second: Option<FileValue>,
}
impl Files {
    pub(super) fn one(first: FileValue) -> Self {
        Self {
            first,
            second: None,
        }
    }
    pub(super) fn iter(&self) -> impl Iterator<Item = &FileValue> {
        std::iter::once(&self.first).chain(self.second.as_ref())
    }
}
impl IntoIterator for Files {
    type Item = FileValue;
    type IntoIter = std::iter::Chain<std::iter::Once<FileValue>, std::option::IntoIter<FileValue>>;
    fn into_iter(self) -> Self::IntoIter {
        std::iter::once(self.first).chain(self.second)
    }
}

pub(super) fn decode_files(bytes: &OwnedBytes) -> Result<Files> {
    if bytes.get(..8) != Some(PAIR_MAGIC.as_slice()) {
        return Ok(Files {
            first: decode(bytes)?,
            second: None,
        });
    }
    let length = bytes
        .get(8..PREFIX)
        .and_then(|b| b.try_into().ok())
        .map(u32::from_le_bytes)
        .ok_or(Error::InvalidSegment)? as usize;
    let middle = PREFIX
        .checked_add(length)
        .filter(|end| *end <= bytes.len())
        .ok_or(Error::InvalidSegment)?;
    // decode() accepts only an ordinary file, so nested pairs cannot expand.
    let first = decode(&bytes.slice(PREFIX..middle))?;
    let second = decode(&bytes.slice(middle..))?;
    validate_pair(&first, &second)?;
    Ok(Files {
        first,
        second: Some(second),
    })
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
type NativeFile = (FileValue, acyclic_fs::NativeAllocation);

// Replay the shared mailbox frame under the original caller's metadata custody.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn decode_native_files(
    bytes: &OwnedBytes,
    io: &crate::filesystem::PreparationIo,
) -> crate::Result<(NativeFile, Option<NativeFile>)> {
    if bytes.get(..8) != Some(PAIR_MAGIC.as_slice()) {
        return Ok((decode_native(bytes, io)?, None));
    }
    let length = bytes
        .get(8..PREFIX)
        .and_then(|b| b.try_into().ok())
        .map(u32::from_le_bytes)
        .ok_or(Error::InvalidSegment)? as usize;
    let middle = PREFIX
        .checked_add(length)
        .filter(|end| *end <= bytes.len())
        .ok_or(Error::InvalidSegment)?;
    let first = decode_native(&bytes.slice(PREFIX..middle), io)?;
    let second = decode_native(&bytes.slice(middle..), io)?;
    validate_pair(&first.0, &second.0)?;
    Ok((first, Some(second)))
}

fn validate_pair(first: &FileValue, second: &FileValue) -> Result<()> {
    use super::{ChildFile, ChildStatus, ChildTurnResult};
    let (
        FileKey::Child {
            reference: left,
            file: a,
        },
        FileKey::Child {
            reference: right,
            file: b,
        },
    ) = (&first.key, &second.key)
    else {
        return Err(Error::InvalidSegment);
    };
    if left != right || first.publication.is_none() || first.publication != second.publication {
        return Err(Error::InvalidSegment);
    }
    match (a, b) {
        (ChildFile::Status, ChildFile::Delivered) => {
            let status = ChildStatus::parse(&first.bytes)?;
            let delivered: (crate::TaskId, u64) =
                serde_json::from_slice(&second.bytes).map_err(|_| Error::InvalidSegment)?;
            if status.settled || delivered != (status.task, status.inputs) || status.inputs == 0 {
                return Err(Error::InvalidSegment);
            }
        }
        (ChildFile::Result(turn), ChildFile::Status) => {
            let result = ChildTurnResult::parse(&first.bytes)?;
            let status = ChildStatus::parse(&second.bytes)?;
            if !status.settled
                || result.turn != *turn
                || result.turn != status.inputs
                || result.task != status.task
            {
                return Err(Error::InvalidSegment);
            }
        }
        _ => return Err(Error::InvalidSegment),
    }
    Ok(())
}
