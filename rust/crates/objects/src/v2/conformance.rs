//! Black-box logical Objects conformance for deterministic, disposable test providers.
//!
//! The fixture must make its own completed mutations visible before the next read.
//! Production eventual consistency and live service acceptance require separate checks.

use super::{Error, ObjectsProvider, wire};
use bytes::Bytes;

/// Canonical inventory of the executable logical provider walkthrough.
pub const SUITE: &[u8] = include_bytes!("../../conformance/objects-v2.json");

/// A provider failure or a violated logical Objects invariant.
#[derive(Debug, thiserror::Error)]
pub enum Failure {
    /// The provider rejected an operation required by the fixture.
    #[error(transparent)]
    Provider(#[from] Error),
    /// A successful operation or expected failure contradicted the contract.
    #[error("Objects v2 conformance: {0}")]
    Invariant(&'static str),
}

fn ensure(condition: bool, message: &'static str) -> Result<(), Failure> {
    if condition {
        Ok(())
    } else {
        Err(Failure::Invariant(message))
    }
}

fn expected_error<T>(
    result: Result<T, Error>,
    code: wire::ErrorCode,
    message: &'static str,
) -> Result<(), Failure> {
    ensure(
        result.err().is_some_and(|error| error.code == code),
        message,
    )
}

fn put(bucket: &wire::BucketRef, key: &str) -> wire::PutObjectHeader {
    wire::PutObjectHeader {
        bucket: Some(bucket.clone()),
        object_key: key.into(),
        ..Default::default()
    }
}

fn get(bucket: &wire::BucketRef, key: &str) -> wire::GetObjectRequest {
    wire::GetObjectRequest {
        bucket: Some(bucket.clone()),
        object_key: key.into(),
        ..Default::default()
    }
}

fn identity(namespace: &str, operation: &str) -> Option<wire::MutationIdentity> {
    Some(wire::MutationIdentity {
        idempotency_key: format!(
            "{}-{operation}",
            blake3::hash(namespace.as_bytes()).to_hex()
        ),
    })
}

/// Exercises every public logical Objects operation in a fresh caller-owned namespace.
///
/// Includes current-key replacement, exact retry receipts after a later publication,
/// atomic conditions, selected reads, allocation bounds, query-bound pagination,
/// multipart completion and abort, and empty-bucket deletion. No public version,
/// snapshot, fork, or captured-listing semantics are assumed.
///
/// # Errors
/// Returns a typed provider failure or the exact failed invariant. The namespace
/// must be a valid unused bucket name; failed verification may leave fixture data.
#[allow(clippy::too_many_lines, clippy::cognitive_complexity)]
pub async fn verify<P: ObjectsProvider + ?Sized>(
    provider: &P,
    namespace: &str,
) -> Result<(), Failure> {
    let create = wire::CreateBucketRequest {
        name: namespace.into(),
        mutation: identity(namespace, "create"),
    };
    let created = provider.create_bucket(create.clone()).await?;
    let bucket = created.bucket.clone().ok_or(Failure::Invariant(
        "created bucket omitted its logical name",
    ))?;
    ensure(
        bucket.name == namespace,
        "create substituted the caller's logical bucket",
    )?;
    ensure(
        provider.create_bucket(create).await? == created,
        "bucket retry did not replay its exact response",
    )?;
    ensure(
        provider
            .head_bucket(wire::HeadBucketRequest {
                bucket: Some(bucket.clone()),
            })
            .await?
            == created,
        "head bucket differs from creation",
    )?;
    expected_error(
        provider
            .create_bucket(wire::CreateBucketRequest {
                name: if namespace == "other" {
                    "alternate"
                } else {
                    "other"
                }
                .into(),
                mutation: identity(namespace, "create"),
            })
            .await,
        wire::ErrorCode::IdempotencyMismatch,
        "bucket retry identity was rebound",
    )?;

    let body = Bytes::from(vec![b'x'; 2 * super::HTTP_BODY_FRAME_BYTES + 17]);
    let mut first_request = put(&bucket, "dir/value");
    first_request.preconditions = Some(wire::Preconditions {
        condition: Some(wire::preconditions::Condition::IfAbsent(true)),
    });
    first_request.mutation = identity(namespace, "put");
    let first = provider.put(first_request.clone(), body.clone()).await?;
    ensure(
        first.size == body.len() as u64 && !first.etag.is_empty(),
        "put omitted exact length or validator",
    )?;
    let mut denied = first_request.clone();
    denied.mutation = None;
    expected_error(
        provider.put(denied, body.clone()).await,
        wire::ErrorCode::PreconditionFailed,
        "IfAbsent overwrote a current key",
    )?;
    expected_error(
        provider
            .put(first_request.clone(), Bytes::from_static(b"different"))
            .await,
        wire::ErrorCode::IdempotencyMismatch,
        "put retry identity admitted different bytes",
    )?;

    let query = get(&bucket, "dir/value");
    let read = provider.get(query.clone(), body.len() as u64).await?;
    ensure(
        read.body == body && read.header.object.as_ref() == Some(&first),
        "get changed the complete representation or its metadata",
    )?;
    expected_error(
        provider.get(query.clone(), 1).await,
        wire::ErrorCode::QuotaExceeded,
        "read allocation bound was ignored",
    )?;
    let mut conditional = query.clone();
    conditional.if_none_match = first.etag.clone();
    expected_error(
        provider.get(conditional, u64::MAX).await,
        wire::ErrorCode::NotModified,
        "matching conditional read returned a body",
    )?;
    let mut ranged = query.clone();
    ranged.range = Some(wire::ByteRange {
        selection: Some(wire::byte_range::Selection::SuffixLength(17)),
    });
    let selected = provider.get(ranged, 17).await?;
    ensure(
        selected.body.as_ref() == [b'x'; 17] && selected.header.object.as_ref() == Some(&first),
        "suffix range changed the selected bytes or complete metadata",
    )?;
    ensure(
        provider
            .head(wire::HeadObjectRequest {
                bucket: Some(bucket.clone()),
                object_key: "dir/value".into(),
                ..Default::default()
            })
            .await?
            .object
            == Some(first.clone()),
        "head changed publication metadata",
    )?;

    let newer = provider
        .put(put(&bucket, "dir/value"), Bytes::from_static(b"new"))
        .await?;
    ensure(
        provider.put(first_request, body.clone()).await? == first,
        "retry after replacement lost its original receipt",
    )?;
    ensure(
        provider.get(query, 3).await?.body.as_ref() == b"new",
        "old retry restored superseded bytes",
    )?;
    expected_error(
        provider
            .delete(wire::DeleteObjectRequest {
                bucket: Some(bucket.clone()),
                object_key: "dir/value".into(),
                preconditions: Some(wire::Preconditions {
                    condition: Some(wire::preconditions::Condition::IfMatch(first.etag)),
                }),
                mutation: None,
            })
            .await,
        wire::ErrorCode::PreconditionFailed,
        "stale validator deleted a later value",
    )?;

    provider
        .put(put(&bucket, "dir/other"), Bytes::from_static(b"other"))
        .await?;
    let listing = wire::ListObjectsRequest {
        bucket: Some(bucket.clone()),
        prefix: "dir/".into(),
        page_size: 1,
        ..Default::default()
    };
    let page = provider.list(listing.clone()).await?;
    ensure(
        page.entries.len() == 1
            && page
                .entries
                .first()
                .is_some_and(|entry| entry.object_key == "dir/other")
            && !page.continuation_token.is_empty(),
        "first listing page is not ordered and bounded",
    )?;
    let mut continuation = listing;
    continuation.continuation_token = page.continuation_token;
    let mut rebound = continuation.clone();
    rebound.prefix = "different/".into();
    expected_error(
        provider.list(rebound).await,
        wire::ErrorCode::InvalidArgument,
        "listing cursor admitted another query",
    )?;
    let page = provider.list(continuation).await?;
    ensure(
        page.entries.len() == 1
            && page.entries.first().is_some_and(|entry| {
                entry.object_key == "dir/value" && entry.object.as_ref() == Some(&newer)
            })
            && page.continuation_token.is_empty(),
        "listing repeated history or omitted the current representation",
    )?;
    let grouped = provider
        .list(wire::ListObjectsRequest {
            bucket: Some(bucket.clone()),
            delimiter: "/".into(),
            ..Default::default()
        })
        .await?;
    ensure(
        grouped.entries.is_empty() && grouped.common_prefixes == ["dir/"],
        "delimiter grouping exposed nested objects",
    )?;

    let upload = provider
        .create_multipart(wire::CreateMultipartRequest {
            bucket: Some(bucket.clone()),
            object_key: "multipart".into(),
            mutation: identity(namespace, "multipart"),
            ..Default::default()
        })
        .await?;
    let part_header = wire::UploadPartHeader {
        bucket: Some(bucket.clone()),
        object_key: "multipart".into(),
        upload_id: upload.upload_id.clone(),
        part_number: 1,
        mutation: identity(namespace, "part"),
    };
    let part = provider
        .upload_part(part_header.clone(), body.clone())
        .await?;
    ensure(
        provider.upload_part(part_header, body.clone()).await? == part,
        "part retry changed its receipt",
    )?;
    ensure(
        provider
            .list_parts(wire::ListPartsRequest {
                bucket: Some(bucket.clone()),
                object_key: "multipart".into(),
                upload_id: upload.upload_id.clone(),
                ..Default::default()
            })
            .await?
            .parts
            == [part.clone()],
        "staged part listing differs from its receipt",
    )?;
    let complete = wire::CompleteMultipartRequest {
        bucket: Some(bucket.clone()),
        object_key: "multipart".into(),
        upload_id: upload.upload_id.clone(),
        parts: vec![part],
        mutation: identity(namespace, "complete"),
        ..Default::default()
    };
    let published = provider.complete_multipart(complete.clone()).await?;
    ensure(
        provider.complete_multipart(complete).await? == published,
        "completed upload retry did not recover its receipt",
    )?;
    ensure(
        provider
            .get(get(&bucket, "multipart"), body.len() as u64)
            .await?
            .body
            == body,
        "completed multipart bytes differ",
    )?;
    ensure(
        !provider
            .abort_multipart(wire::AbortMultipartRequest {
                bucket: Some(bucket.clone()),
                object_key: "multipart".into(),
                upload_id: upload.upload_id,
                mutation: None,
            })
            .await?
            .existed,
        "completed upload remained abortable",
    )?;

    let abandoned = provider
        .create_multipart(wire::CreateMultipartRequest {
            bucket: Some(bucket.clone()),
            object_key: "abandoned".into(),
            ..Default::default()
        })
        .await?;
    provider
        .upload_part(
            wire::UploadPartHeader {
                bucket: Some(bucket.clone()),
                object_key: "abandoned".into(),
                upload_id: abandoned.upload_id.clone(),
                part_number: 1,
                mutation: identity(namespace, "abandoned-part"),
            },
            body.clone(),
        )
        .await?;
    expected_error(
        provider
            .get(get(&bucket, "abandoned"), body.len() as u64)
            .await,
        wire::ErrorCode::NotFound,
        "staged multipart bytes became a public object",
    )?;
    ensure(
        provider
            .abort_multipart(wire::AbortMultipartRequest {
                bucket: Some(bucket.clone()),
                object_key: "abandoned".into(),
                upload_id: abandoned.upload_id,
                mutation: None,
            })
            .await?
            .existed,
        "active upload was not aborted",
    )?;
    expected_error(
        provider
            .get(get(&bucket, "abandoned"), body.len() as u64)
            .await,
        wire::ErrorCode::NotFound,
        "aborted multipart bytes became a public object",
    )?;
    expected_error(
        provider
            .delete_bucket(wire::DeleteBucketRequest {
                bucket: Some(bucket.clone()),
                mutation: None,
            })
            .await,
        wire::ErrorCode::PreconditionFailed,
        "nonempty bucket was deleted",
    )?;
    for key in ["dir/value", "dir/other", "multipart"] {
        ensure(
            provider
                .delete(wire::DeleteObjectRequest {
                    bucket: Some(bucket.clone()),
                    object_key: key.into(),
                    ..Default::default()
                })
                .await?
                .existed,
            "current key was not deleted",
        )?;
        expected_error(
            provider.get(get(&bucket, key), 1).await,
            wire::ErrorCode::NotFound,
            "deleted key exposed retained bytes",
        )?;
    }
    ensure(
        provider
            .list(wire::ListObjectsRequest {
                bucket: Some(bucket.clone()),
                ..Default::default()
            })
            .await?
            .entries
            .is_empty(),
        "deleted keys remained publicly listed",
    )?;
    ensure(
        provider
            .delete_bucket(wire::DeleteBucketRequest {
                bucket: Some(bucket.clone()),
                mutation: None,
            })
            .await?
            .existed,
        "empty bucket was not deleted",
    )?;
    expected_error(
        provider
            .head_bucket(wire::HeadBucketRequest {
                bucket: Some(bucket),
            })
            .await,
        wire::ErrorCode::NotFound,
        "deleted bucket remained public",
    )?;
    Ok(())
}
