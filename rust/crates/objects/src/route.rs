//! The one wire dispatch over any [`Blobs`]: gRPC and binary adapters serve
//! each route through these functions, each paired with the revision its
//! result carries.
use crate::convert;
use crate::{Blobs, Digest, Error, RootId, wire};
use acyclic_stream::Revision;
use prost::Message;

/// A route's response and the revision committing or serving it.
pub type Served<T> = (T, Option<Revision>);

/// Verifies the supplied digest, then publishes the body.
///
/// # Errors
/// The provider's semantic failure.
pub async fn put_blob(
    blobs: &dyn Blobs,
    request: wire::PutBlobRequest,
) -> Result<Served<wire::PutResponse>, Error> {
    let expected = convert::digest(request.digest.as_ref())?;
    if Digest::of(&request.body) != expected {
        return Err(wire::ErrorCode::DigestMismatch.into());
    }
    let references = convert::digests(&request.references)?;
    let put = blobs.put(request.body.into(), references).await?;
    durable(blobs, &put).await?;
    Ok((convert::wire_put(&put), put_revision(&put)))
}

/// A remote put is acknowledged durable: its client has no commit to defer
/// the flush to.
async fn durable(blobs: &dyn Blobs, put: &crate::Put) -> Result<(), Error> {
    match put.verified() {
        Some(verified) => blobs.flush(Some(&[verified.digest])).await,
        None => Ok(()),
    }
}

fn put_revision(put: &crate::Put) -> Option<Revision> {
    put.verified()
        .and_then(|verified| verified.revision.clone())
}

/// Publishes a manifest of existing parts.
///
/// # Errors
/// The provider's semantic failure.
pub async fn put_manifest(
    blobs: &dyn Blobs,
    request: wire::PutManifestRequest,
) -> Result<Served<wire::PutResponse>, Error> {
    let manifest = convert::from_manifest(request.manifest.as_ref())?;
    let put = blobs.put_manifest(manifest).await?;
    durable(blobs, &put).await?;
    Ok((convert::wire_put(&put), put_revision(&put)))
}

/// Reads a logical range.
///
/// # Errors
/// The provider's semantic failure.
pub async fn get_blob(
    blobs: &dyn Blobs,
    request: wire::GetBlobRequest,
) -> Result<Served<wire::GetBlobResponse>, Error> {
    let digest = convert::digest(request.digest.as_ref())?;
    let range = convert::from_range(request.range.as_ref());
    let object = blobs.get(digest, range, request.maximum_bytes).await?;
    let revision = object.revision().cloned();
    Ok((
        wire::GetBlobResponse {
            info: Some(convert::wire_head(object.head())),
            start: object.start(),
            body: bytes::Bytes::from_owner(object.into_body()),
        },
        revision,
    ))
}

/// Reads an envelope.
///
/// # Errors
/// The provider's semantic failure.
pub async fn head_blob(
    blobs: &dyn Blobs,
    request: wire::HeadBlobRequest,
) -> Result<Served<wire::BlobInfo>, Error> {
    let digest = convert::digest(request.digest.as_ref())?;
    let head = blobs.head(digest).await?;
    Ok((convert::wire_head(&head), head.revision))
}

/// Deletes an unreferenced blob.
///
/// # Errors
/// The provider's semantic failure.
pub async fn delete_blob(
    blobs: &dyn Blobs,
    request: wire::DeleteBlobRequest,
) -> Result<Served<wire::DeleteBlobResponse>, Error> {
    let digest = convert::digest(request.digest.as_ref())?;
    let deletion = blobs.delete(digest).await?;
    Ok((convert::wire_delete(deletion.outcome), deletion.revision))
}

/// Lists digests after a cursor.
///
/// # Errors
/// The provider's semantic failure.
pub async fn list_blobs(
    blobs: &dyn Blobs,
    request: wire::ListBlobsRequest,
) -> Result<Served<wire::ListBlobsResponse>, Error> {
    let after = request
        .after
        .as_ref()
        .map(|after| convert::digest(Some(after)))
        .transpose()?;
    let page = blobs.list(after, request.limit).await?;
    Ok((convert::wire_page(&page), page.revision))
}

/// Appends a reference batch to a root.
///
/// # Errors
/// The provider's semantic failure.
pub async fn retain(
    blobs: &dyn Blobs,
    request: wire::RetainRequest,
) -> Result<Served<wire::RetainResponse>, Error> {
    let references = convert::digests(&request.references)?;
    let retained = blobs
        .retain(&RootId::new(request.root)?, references)
        .await?;
    Ok((
        wire::RetainResponse {
            references: retained.references,
        },
        retained.revision,
    ))
}

/// Releases a root.
///
/// # Errors
/// The provider's semantic failure.
pub async fn release(
    blobs: &dyn Blobs,
    request: wire::ReleaseRequest,
) -> Result<Served<wire::ReleaseResponse>, Error> {
    let released = blobs.release(&RootId::new(request.root)?).await?;
    Ok((
        wire::ReleaseResponse {
            existed: released.existed,
        },
        released.revision,
    ))
}

/// Runs one mark-and-sweep pass.
///
/// # Errors
/// The provider's semantic failure.
pub async fn collect(
    blobs: &dyn Blobs,
    _request: wire::CollectRequest,
) -> Result<Served<wire::CollectResponse>, Error> {
    let collection = blobs.collect().await?;
    Ok((convert::wire_collection(&collection), collection.revision))
}

/// Reports unique stored bytes.
///
/// # Errors
/// The provider's semantic failure.
pub async fn usage(
    blobs: &dyn Blobs,
    _request: wire::UsageRequest,
) -> Result<Served<wire::UsageResponse>, Error> {
    let usage = blobs.usage().await?;
    Ok((
        wire::UsageResponse {
            blobs: usage.blobs,
            unique_bytes: usage.unique_bytes,
        },
        None,
    ))
}

fn decode<T: Message + Default>(bytes: &[u8]) -> Result<T, Error> {
    T::decode(bytes).map_err(|_| wire::ErrorCode::InvalidArgument.into())
}

/// Serves one [`crate::HTTP_ROUTES`] route from its binary input message,
/// returning the binary output message.
///
/// # Errors
/// The route's semantic failure, or `InvalidArgument` for an unknown route or
/// a malformed input.
pub async fn invoke(blobs: &dyn Blobs, route: &str, input: &[u8]) -> Result<Vec<u8>, Error> {
    Ok(match route {
        "blobs/put" => put_blob(blobs, decode(input)?).await?.0.encode_to_vec(),
        "blobs/put-manifest" => put_manifest(blobs, decode(input)?).await?.0.encode_to_vec(),
        "blobs/get" => get_blob(blobs, decode(input)?).await?.0.encode_to_vec(),
        "blobs/head" => head_blob(blobs, decode(input)?).await?.0.encode_to_vec(),
        "blobs/delete" => delete_blob(blobs, decode(input)?).await?.0.encode_to_vec(),
        "blobs/list" => list_blobs(blobs, decode(input)?).await?.0.encode_to_vec(),
        "blobs/retain" => retain(blobs, decode(input)?).await?.0.encode_to_vec(),
        "blobs/release" => release(blobs, decode(input)?).await?.0.encode_to_vec(),
        "blobs/collect" => collect(blobs, decode(input)?).await?.0.encode_to_vec(),
        "blobs/usage" => usage(blobs, decode(input)?).await?.0.encode_to_vec(),
        _ => return Err(wire::ErrorCode::InvalidArgument.into()),
    })
}

/// Checks one [`crate::HTTP_ROUTES`] reply against the request it answers,
/// exactly as the Rust clients check theirs, for clients in other languages
/// that carry the binary messages themselves.
///
/// A put request's digest is trusted as the caller's own: the client hashed
/// the body when it built the request, and the service refuses a mismatch.
///
/// # Errors
/// `InvalidArgument` for an unknown route or malformed request, and
/// `Unavailable` for a reply that is malformed or does not answer it.
pub fn verify(route: &str, input: &[u8], output: &[u8]) -> Result<(), Error> {
    fn reply<T: Message + Default>(bytes: &[u8]) -> Result<T, Error> {
        T::decode(bytes).map_err(|_| wire::ErrorCode::Unavailable.into())
    }
    let checked = match route {
        "blobs/put" => {
            let request: wire::PutBlobRequest = decode(input)?;
            let digest = convert::digest(request.digest.as_ref())?;
            convert::verified_put(&reply(output)?, digest, request.body.len() as u64).map(drop)
        }
        "blobs/put-manifest" => {
            let request: wire::PutManifestRequest = decode(input)?;
            let manifest = convert::from_manifest(request.manifest.as_ref())?;
            let (digest, length) = (Digest::of(&manifest.encode()), manifest.length()?);
            convert::verified_put(&reply(output)?, digest, length).map(drop)
        }
        "blobs/get" => {
            let request: wire::GetBlobRequest = decode(input)?;
            let digest = convert::digest(request.digest.as_ref())?;
            let range = convert::from_range(request.range.as_ref());
            convert::object(reply(output)?, digest, range, request.maximum_bytes).map(drop)
        }
        "blobs/head" => {
            let request: wire::HeadBlobRequest = decode(input)?;
            let digest = convert::digest(request.digest.as_ref())?;
            convert::headed(&reply(output)?, digest).map(drop)
        }
        "blobs/delete" => convert::delete(&reply(output)?).map(drop),
        "blobs/list" => {
            let request: wire::ListBlobsRequest = decode(input)?;
            let after = request
                .after
                .as_ref()
                .map(|after| convert::digest(Some(after)))
                .transpose()?;
            convert::listed(&reply(output)?, after, request.limit).map(drop)
        }
        "blobs/retain" => reply::<wire::RetainResponse>(output).map(drop),
        "blobs/release" => reply::<wire::ReleaseResponse>(output).map(drop),
        "blobs/collect" => reply::<wire::CollectResponse>(output).map(drop),
        "blobs/usage" => reply::<wire::UsageResponse>(output).map(drop),
        _ => return Err(wire::ErrorCode::InvalidArgument.into()),
    };
    checked.map_err(|_| wire::ErrorCode::Unavailable.into())
}
