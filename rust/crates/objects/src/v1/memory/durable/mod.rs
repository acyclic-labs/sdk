//! Canonical current-state records shared by durable Objects transports.
use super::super::response;
use super::*;
#[cfg(feature = "local")]
use crate::body::LocalBodyLocation;
use std::collections::BTreeSet;

pub(super) mod bodies;
pub(super) mod changes;
pub(super) mod packing;
pub(super) mod protocol;
mod publication;
pub(super) const PACK_LIMIT: usize = 64 * 1024;
pub(in crate::v1) const RECORD_LIMIT: usize = 2 * 1024 * 1024;
mod replay;
pub(super) mod snapshot;
use protocol::*;
pub(crate) use publication::Publication;
pub(super) use replay::apply;
#[derive(Debug)]
pub(super) struct RestoreError;

pub(in crate::v1::memory) fn response_kind<R>() -> Result<u32, Error> {
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
