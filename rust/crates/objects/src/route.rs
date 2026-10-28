//! The one wire dispatch over any [`Blobs`]: gRPC and binary adapters serve
//! each route through these functions.
use crate::convert;
use crate::{Blobs, Digest, Error, RootId, wire};
use prost::Message;

/// Verifies the supplied digest, then publishes the body.
///
/// # Errors
/// The provider's semantic failure.
pub async fn put_blob(
    blobs: &dyn Blobs,
    request: wire::PutBlobRequest,
) -> Result<wire::PutResponse, Error> {
    let expected = convert::digest(request.digest.as_ref())?;
    if Digest::of(&request.body) != expected {
        return Err(wire::ErrorCode::DigestMismatch.into());
    }
    let references = convert::digests(&request.references)?;
    Ok(convert::wire_put(
        &blobs.put(request.body.into(), references).await?,
    ))
}

/// Publishes a manifest of existing parts.
///
/// # Errors
/// The provider's semantic failure.
pub async fn put_manifest(
    blobs: &dyn Blobs,
    request: wire::PutManifestRequest,
) -> Result<wire::PutResponse, Error> {
    let manifest = convert::from_manifest(request.manifest.as_ref())?;
    Ok(convert::wire_put(&blobs.put_manifest(manifest).await?))
}

/// Reads a logical range.
///
/// # Errors
/// The provider's semantic failure.
pub async fn get_blob(
    blobs: &dyn Blobs,
    request: wire::GetBlobRequest,
) -> Result<wire::GetBlobResponse, Error> {
    let digest = convert::digest(request.digest.as_ref())?;
    let range = convert::from_range(request.range.as_ref());
    let object = blobs.get(digest, range, request.maximum_bytes).await?;
    Ok(wire::GetBlobResponse {
        info: Some(convert::wire_head(object.head())),
        start: object.start(),
        body: object.into_body().to_vec(),
    })
}

/// Reads an envelope.
///
/// # Errors
/// The provider's semantic failure.
pub async fn head_blob(
    blobs: &dyn Blobs,
    request: wire::HeadBlobRequest,
) -> Result<wire::BlobInfo, Error> {
    let digest = convert::digest(request.digest.as_ref())?;
    Ok(convert::wire_head(&blobs.head(digest).await?))
}

/// Deletes an unreferenced blob.
///
/// # Errors
/// The provider's semantic failure.
pub async fn delete_blob(
    blobs: &dyn Blobs,
    request: wire::DeleteBlobRequest,
) -> Result<wire::DeleteBlobResponse, Error> {
    let digest = convert::digest(request.digest.as_ref())?;
    Ok(convert::wire_delete(blobs.delete(digest).await?))
}

/// Lists digests after a cursor.
///
/// # Errors
/// The provider's semantic failure.
pub async fn list_blobs(
    blobs: &dyn Blobs,
    request: wire::ListBlobsRequest,
) -> Result<wire::ListBlobsResponse, Error> {
    let after = request
        .after
        .as_ref()
        .map(|after| convert::digest(Some(after)))
        .transpose()?;
    Ok(convert::wire_page(&blobs.list(after, request.limit).await?))
}

/// Appends a reference batch to a root.
///
/// # Errors
/// The provider's semantic failure.
pub async fn retain(
    blobs: &dyn Blobs,
    request: wire::RetainRequest,
) -> Result<wire::RetainResponse, Error> {
    let references = convert::digests(&request.references)?;
    let retained = blobs
        .retain(&RootId::new(request.root)?, references)
        .await?;
    Ok(wire::RetainResponse {
        references: retained.references,
    })
}

/// Releases a root.
///
/// # Errors
/// The provider's semantic failure.
pub async fn release(
    blobs: &dyn Blobs,
    request: wire::ReleaseRequest,
) -> Result<wire::ReleaseResponse, Error> {
    let existed = blobs.release(&RootId::new(request.root)?).await?;
    Ok(wire::ReleaseResponse { existed })
}

/// Runs one mark-and-sweep pass.
///
/// # Errors
/// The provider's semantic failure.
pub async fn collect(
    blobs: &dyn Blobs,
    _request: wire::CollectRequest,
) -> Result<wire::CollectResponse, Error> {
    Ok(convert::wire_collection(&blobs.collect().await?))
}

/// Reports unique stored bytes.
///
/// # Errors
/// The provider's semantic failure.
pub async fn usage(
    blobs: &dyn Blobs,
    _request: wire::UsageRequest,
) -> Result<wire::UsageResponse, Error> {
    let usage = blobs.usage().await?;
    Ok(wire::UsageResponse {
        blobs: usage.blobs,
        unique_bytes: usage.unique_bytes,
    })
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
        "blobs/put" => put_blob(blobs, decode(input)?).await?.encode_to_vec(),
        "blobs/put-manifest" => put_manifest(blobs, decode(input)?).await?.encode_to_vec(),
        "blobs/get" => get_blob(blobs, decode(input)?).await?.encode_to_vec(),
        "blobs/head" => head_blob(blobs, decode(input)?).await?.encode_to_vec(),
        "blobs/delete" => delete_blob(blobs, decode(input)?).await?.encode_to_vec(),
        "blobs/list" => list_blobs(blobs, decode(input)?).await?.encode_to_vec(),
        "blobs/retain" => retain(blobs, decode(input)?).await?.encode_to_vec(),
        "blobs/release" => release(blobs, decode(input)?).await?.encode_to_vec(),
        "blobs/collect" => collect(blobs, decode(input)?).await?.encode_to_vec(),
        "blobs/usage" => usage(blobs, decode(input)?).await?.encode_to_vec(),
        _ => return Err(wire::ErrorCode::InvalidArgument.into()),
    })
}
