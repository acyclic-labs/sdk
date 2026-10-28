//! Exact conversions between the public Rust types and their wire messages.
#[cfg(objects_server)]
use crate::Part;
use crate::blob::invalid;
use crate::{
    ByteRange, Collection, Delete, Digest, Error, Head, Kind, Manifest, Page, Put, Verified, wire,
};
#[cfg(objects_client)]
use crate::{Retained, Usage};

impl From<Digest> for wire::Digest {
    fn from(digest: Digest) -> Self {
        Self {
            blake3: digest.as_bytes().to_vec(),
        }
    }
}

pub(crate) fn digest(value: Option<&wire::Digest>) -> Result<Digest, Error> {
    Digest::from_slice(&value.ok_or_else(invalid)?.blake3)
}

pub(crate) fn digests(values: &[wire::Digest]) -> Result<Vec<Digest>, Error> {
    values.iter().map(|value| digest(Some(value))).collect()
}

pub(crate) fn wire_digests(values: &[Digest]) -> Vec<wire::Digest> {
    values.iter().copied().map(Into::into).collect()
}

#[cfg(objects_client)]
pub(crate) fn range(range: ByteRange) -> wire::ByteRange {
    wire::ByteRange {
        start: range.start,
        end: range.end,
    }
}

#[cfg(objects_server)]
pub(crate) fn from_range(range: Option<&wire::ByteRange>) -> ByteRange {
    range.map_or(ByteRange::FULL, |range| ByteRange {
        start: range.start,
        end: range.end,
    })
}

#[cfg(objects_client)]
pub(crate) fn manifest(manifest: &Manifest) -> wire::Manifest {
    wire::Manifest {
        parts: manifest
            .parts
            .iter()
            .map(|part| wire::Part {
                digest: Some(part.digest.into()),
                length: part.length,
            })
            .collect(),
        edges: wire_digests(&manifest.edges),
    }
}

#[cfg(objects_server)]
pub(crate) fn from_manifest(manifest: Option<&wire::Manifest>) -> Result<Manifest, Error> {
    let manifest = manifest.ok_or_else(invalid)?;
    Ok(Manifest {
        parts: manifest
            .parts
            .iter()
            .map(|part| {
                Ok(Part {
                    digest: digest(part.digest.as_ref())?,
                    length: part.length,
                })
            })
            .collect::<Result<_, Error>>()?,
        edges: digests(&manifest.edges)?,
    })
}

fn verified(value: &wire::Verified) -> Result<Verified, Error> {
    Ok(Verified {
        digest: digest(value.digest.as_ref())?,
        length: value.length,
        revision: None,
    })
}

#[cfg(objects_server)]
fn wire_verified(value: &Verified) -> wire::Verified {
    wire::Verified {
        digest: Some(value.digest.into()),
        length: value.length,
    }
}

pub(crate) fn put(value: &wire::PutResponse) -> Result<Put, Error> {
    use wire::put_response::Outcome;
    match value.outcome.as_ref().ok_or_else(invalid)? {
        Outcome::Created(value) => Ok(Put::Created {
            verified: verified(value)?,
        }),
        Outcome::AlreadyExists(value) => Ok(Put::AlreadyExists {
            verified: verified(value)?,
        }),
        Outcome::Unknown(true) => Ok(Put::Unknown),
        Outcome::Unknown(false) => Err(invalid()),
    }
}

#[cfg(objects_server)]
pub(crate) fn wire_put(value: &Put) -> wire::PutResponse {
    use wire::put_response::Outcome;
    wire::PutResponse {
        outcome: Some(match value {
            Put::Created { verified } => Outcome::Created(wire_verified(verified)),
            Put::AlreadyExists { verified } => Outcome::AlreadyExists(wire_verified(verified)),
            Put::Unknown => Outcome::Unknown(true),
        }),
    }
}

pub(crate) fn head(value: &wire::BlobInfo) -> Result<Head, Error> {
    Ok(Head {
        digest: digest(value.digest.as_ref())?,
        kind: match wire::Kind::try_from(value.kind).map_err(|_| invalid())? {
            wire::Kind::Blob => Kind::Blob,
            wire::Kind::Manifest => Kind::Manifest,
            wire::Kind::Unspecified => return Err(invalid()),
        },
        length: value.length,
        references: digests(&value.references)?,
        revision: None,
    })
}

#[cfg(objects_server)]
pub(crate) fn wire_head(value: &Head) -> wire::BlobInfo {
    wire::BlobInfo {
        digest: Some(value.digest.into()),
        kind: match value.kind {
            Kind::Blob => wire::Kind::Blob,
            Kind::Manifest => wire::Kind::Manifest,
        } as i32,
        length: value.length,
        references: wire_digests(&value.references),
    }
}

pub(crate) fn delete(value: &wire::DeleteBlobResponse) -> Result<Delete, Error> {
    match wire::DeleteStatus::try_from(value.status).map_err(|_| invalid())? {
        wire::DeleteStatus::Deleted => Ok(Delete::Deleted),
        wire::DeleteStatus::Absent => Ok(Delete::Absent),
        wire::DeleteStatus::Referenced => Ok(Delete::Referenced),
        wire::DeleteStatus::Unspecified => Err(invalid()),
    }
}

#[cfg(objects_server)]
pub(crate) fn wire_delete(value: Delete) -> wire::DeleteBlobResponse {
    wire::DeleteBlobResponse {
        status: match value {
            Delete::Deleted => wire::DeleteStatus::Deleted,
            Delete::Absent => wire::DeleteStatus::Absent,
            Delete::Referenced => wire::DeleteStatus::Referenced,
        } as i32,
    }
}

pub(crate) fn page(value: &wire::ListBlobsResponse) -> Result<Page, Error> {
    let page = Page {
        digests: digests(&value.digests)?,
        next: value
            .next
            .as_ref()
            .map(|next| digest(Some(next)))
            .transpose()?,
        revision: None,
    };
    if page.digests.windows(2).any(|pair| pair[0] >= pair[1])
        || page
            .next
            .is_some_and(|next| page.digests.last() != Some(&next))
    {
        return Err(invalid());
    }
    Ok(page)
}

#[cfg(objects_server)]
pub(crate) fn wire_page(value: &Page) -> wire::ListBlobsResponse {
    wire::ListBlobsResponse {
        digests: wire_digests(&value.digests),
        next: value.next.map(Into::into),
    }
}

#[cfg(objects_client)]
pub(crate) fn retained(value: &wire::RetainResponse) -> Retained {
    Retained {
        references: value.references,
        revision: None,
    }
}

#[cfg(objects_client)]
pub(crate) fn collection(value: &wire::CollectResponse) -> Collection {
    Collection {
        epoch: value.epoch,
        marked: value.marked,
        swept: value.swept,
        swept_bytes: value.swept_bytes,
        revision: None,
    }
}

#[cfg(objects_server)]
pub(crate) fn wire_collection(value: &Collection) -> wire::CollectResponse {
    wire::CollectResponse {
        epoch: value.epoch,
        marked: value.marked,
        swept: value.swept,
        swept_bytes: value.swept_bytes,
    }
}

#[cfg(objects_client)]
pub(crate) fn usage(value: &wire::UsageResponse) -> Usage {
    Usage {
        blobs: value.blobs,
        unique_bytes: value.unique_bytes,
    }
}

/// Checks a remote read against its request before exposing it.
pub(crate) fn object(
    value: wire::GetBlobResponse,
    requested: Digest,
    selection: ByteRange,
    maximum: u64,
) -> Result<crate::Object, Error> {
    let head = head(value.info.as_ref().ok_or_else(invalid)?)?;
    let selected = selection.resolve(head.length)?;
    if head.digest != requested
        || value.start != selected.start
        || value.body.len() as u64 != selected.end - selected.start
        || value.body.len() as u64 > maximum
        || (head.kind == Kind::Blob
            && selected.start == 0
            && selected.end == head.length
            && Digest::of(&value.body) != requested)
    {
        return Err(wire::ErrorCode::Unavailable.into());
    }
    Ok(crate::Object::new(head, value.start, value.body.into()))
}

/// Checks a put acknowledgement against the bytes it was for.
pub(crate) fn verified_put(
    value: &wire::PutResponse,
    requested: Digest,
    length: u64,
) -> Result<Put, Error> {
    let put = put(value)?;
    if put
        .verified()
        .is_some_and(|verified| verified.digest != requested || verified.length != length)
    {
        return Err(wire::ErrorCode::Unavailable.into());
    }
    Ok(put)
}

/// Checks an envelope against the digest it was read for.
pub(crate) fn headed(value: &wire::BlobInfo, requested: Digest) -> Result<Head, Error> {
    let head = head(value)?;
    if head.digest != requested {
        return Err(wire::ErrorCode::Unavailable.into());
    }
    Ok(head)
}

/// Checks a page against the cursor and limit it was listed for.
pub(crate) fn listed(
    value: &wire::ListBlobsResponse,
    after: Option<Digest>,
    limit: u32,
) -> Result<Page, Error> {
    let page = page(value)?;
    if page.digests.len() > limit as usize
        || after.is_some_and(|after| page.digests.first().is_some_and(|first| *first <= after))
    {
        return Err(wire::ErrorCode::Unavailable.into());
    }
    Ok(page)
}
