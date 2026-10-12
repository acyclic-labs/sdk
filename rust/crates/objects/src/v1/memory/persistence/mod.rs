use super::super::response;
use super::*;
use crate::LocalObjectsLimits;
use crate::body::LocalBodyLocation;
use crate::physical::native_durability;
use fs2::FileExt;
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

const MAGIC: &[u8] = b"ACYCLIC-OBJECTS-V1-LOCAL\0\x01";
pub(super) const RECORD_LIMIT: usize = 2 * 1024 * 1024;
const MINIMUM_MAINTENANCE_INLINE_BYTES: u64 = 8 * 1024 * 1024;

/// Failure to open or recover local Objects storage.
#[derive(Debug, thiserror::Error)]
pub enum LocalOpenError {
    /// Invalid capacity or durability configuration.
    #[error("invalid local Objects v1 configuration")]
    Invalid,
    /// A complete durable record or referenced body is corrupt.
    #[error("corrupt local Objects v1 store")]
    Corrupt,
    /// Another owner has this exact root open.
    #[error("local Objects v1 store already has an owner")]
    AlreadyOwned,
    /// A native worker or cursor entropy source could not complete.
    #[error("local Objects v1 unavailable")]
    Unavailable,
    /// Host I/O failed.
    #[error("local Objects v1 I/O failed: {0}")]
    Io(#[from] std::io::Error),
}

struct Tail {
    file: File,
    bytes: u64,
    operations: u64,
    inline_bytes: u64,
}
pub(super) struct Journal {
    root: Arc<crate::physical::LocalRoot>,
    tail: Mutex<Tail>,
    limits: LocalObjectsLimits,
    header: Header,
    poisoned: AtomicBool,
    _owner: File,
    _anchor: Option<acyclic_native_runtime::OwnershipAnchor>,
    #[cfg(test)]
    fault_bytes: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    fault_sync: AtomicBool,
    #[cfg(test)]
    fault_checkpoint: std::sync::atomic::AtomicU64,
}

impl Drop for Journal {
    fn drop(&mut self) {
        // Release ownership even if a child inherited the descriptor.
        let _ = FileExt::unlock(&self._owner);
    }
}

mod checkpoint;
mod delta;
mod inline;
mod journal;
mod maintenance;
mod opening;
mod protocol;
mod publication;
mod replay;
#[cfg(test)]
mod tests;

use delta::*;
pub(crate) use opening::open;
use protocol::*;
pub(crate) use publication::Publication;
use publication::{composite_publication, unpersisted};
use replay::*;

pub(super) fn service_error(error: NativeObjectsError) -> Error {
    match error {
        error if !error.has_payload() => match error.into_cause() {
            NativeObjectsCause::Service(error) => error,
            _ => Unavailable.into(),
        },
        _ => Unavailable.into(),
    }
}

fn open_error(error: NativeObjectsError) -> LocalOpenError {
    match error {
        error if !error.has_payload() => match error.into_cause() {
            NativeObjectsCause::Admission(error) => LocalOpenError::Io(error),
            _ => LocalOpenError::Unavailable,
        },
        _ => LocalOpenError::Unavailable,
    }
}

pub(super) fn response_kind<R>() -> Result<u32, Error> {
    match std::any::type_name::<R>().rsplit("::").next() {
        Some("Bucket") => Ok(1),
        Some("ObjectInfo") => Ok(2),
        Some("DeleteBucketResponse") => Ok(3),
        Some("DeleteObjectResponse") => Ok(4),
        Some("MultipartUpload") => Ok(5),
        Some("UploadedPart") => Ok(6),
        Some("AbortMultipartResponse") => Ok(7),
        _ => Err(Unavailable.into()),
    }
}

pub(super) fn put_record_bytes(query: &wire::PutObjectHeader, body: &OwnedBytes) -> usize {
    let inline = if body.len() <= inline::LIMIT {
        body.len()
    } else {
        0
    };
    inline
        .saturating_add(query.encoded_len().saturating_mul(3))
        .saturating_add(1024)
}

fn sync(file: &File, limits: LocalObjectsLimits) -> std::io::Result<()> {
    acyclic_native_runtime::sync_file(file, native_durability(limits.durability), None)
}

fn corrupt<T>(_: T) -> LocalOpenError {
    LocalOpenError::Corrupt
}
