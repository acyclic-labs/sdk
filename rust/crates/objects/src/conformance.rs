//! Black-box conformance for every Objects provider implementation.

use bytes::Bytes;

use crate::{Condition, GetRequest, ObjectsError, ObjectsProvider, PutRequest, ReadTarget, wire};

/// Exercises the complete provider contract under one caller-owned unique namespace.
///
/// The namespace must satisfy the public bucket-name grammar and must never have been used by
/// this provider. The suite intentionally retains created resources so it can verify permanent
/// identity, snapshots, forks, versions, delete markers, and idempotent recovery.
///
/// # Errors
///
/// Returns the provider's typed failure or a conformance invariant failure.
pub async fn verify(provider: &impl ObjectsProvider, namespace: &str) -> Result<(), ObjectsError> {
    let version_bucket = create(provider, &format!("{namespace}-versions"), "create-v").await?;
    ensure(
        provider
            .create_bucket(format!("{namespace}-other"), Some("create-v".into()))
            .await
            == Err(ObjectsError::IdempotencyMismatch),
        "bucket idempotency-key rebinding was accepted",
    )?;
    let first = put(
        provider,
        &version_bucket,
        "dir/a",
        b"one",
        Some(Condition::IfAbsent),
        Some("put-one"),
    )
    .await?;
    ensure(
        provider
            .put(PutRequest {
                bucket: version_bucket.clone(),
                object_key: "dir/a".into(),
                body: Bytes::from_static(b"wrong"),
                metadata: metadata(),
                condition: Some(Condition::IfAbsent),
                idempotency_key: None,
            })
            .await
            == Err(ObjectsError::PreconditionFailed),
        "IfAbsent did not reject an existing current version",
    )?;
    let replay = put(
        provider,
        &version_bucket,
        "dir/a",
        b"one",
        Some(Condition::IfAbsent),
        Some("put-one"),
    )
    .await?;
    ensure(replay == first, "idempotent put did not replay exactly")?;
    ensure(
        provider
            .put(PutRequest {
                bucket: version_bucket.clone(),
                object_key: "dir/a".into(),
                body: Bytes::from_static(b"different"),
                metadata: metadata(),
                condition: Some(Condition::IfAbsent),
                idempotency_key: Some("put-one".into()),
            })
            .await
            == Err(ObjectsError::IdempotencyMismatch),
        "idempotency-key rebinding was accepted",
    )?;
    let second = put(provider, &version_bucket, "dir/a", b"two", None, None).await?;
    ensure(
        first.version_id != second.version_id,
        "versions were not permanent",
    )?;
    ensure(
        first.etag == format!("\"{}\"", blake3::hash(b"one").to_hex()),
        "object validator did not bind exact bytes",
    )?;
    let ranged = provider
        .get(GetRequest {
            target: ReadTarget::Bucket(version_bucket.clone()),
            object_key: "dir/a".into(),
            version_id: Some(first.version_id.clone()),
            range: Some((1, Some(2))),
            if_match: Some(first.etag.clone()),
            if_none_match: None,
            maximum_bytes: 2,
        })
        .await?;
    ensure(
        ranged.body.as_ref() == b"ne",
        "exact-version range read differed",
    )?;
    ensure(
        provider
            .get(GetRequest {
                target: ReadTarget::Bucket(version_bucket.clone()),
                object_key: "dir/a".into(),
                version_id: Some(first.version_id.clone()),
                range: None,
                if_match: None,
                if_none_match: None,
                maximum_bytes: 2,
            })
            .await
            == Err(ObjectsError::Capacity),
        "read allocation bound was not enforced",
    )?;
    let grouped = provider
        .list(
            ReadTarget::Bucket(version_bucket.clone()),
            String::new(),
            Some("/".into()),
            false,
            10,
            None,
        )
        .await?;
    ensure(
        grouped.common_prefixes == vec!["dir/"],
        "delimiter listing did not group the exact prefix",
    )?;
    ensure(
        !provider
            .delete(
                version_bucket.clone(),
                "dir/a".into(),
                Some("missing-version".into()),
                None,
                Some("delete-missing-version".into()),
            )
            .await?
            .existed,
        "unknown exact version was reported as deleted",
    )?;
    ensure(
        provider
            .delete_bucket(&version_bucket, Some("delete-nonempty".into()))
            .await
            == Err(ObjectsError::PreconditionFailed),
        "nonempty bucket deletion was accepted",
    )?;

    let listing_bucket = create(provider, &format!("{namespace}-listing"), "create-l").await?;
    put(provider, &listing_bucket, "a", b"a", None, None).await?;
    put(provider, &listing_bucket, "b", b"b", None, None).await?;
    let first_page = provider
        .list(
            ReadTarget::Bucket(listing_bucket.clone()),
            String::new(),
            None,
            false,
            1,
            None,
        )
        .await?;
    put(provider, &listing_bucket, "aa", b"later", None, None).await?;
    let second_page = provider
        .list(
            ReadTarget::Bucket(listing_bucket),
            String::new(),
            None,
            false,
            1,
            first_page.continuation,
        )
        .await?;
    ensure(
        first_page
            .entries
            .first()
            .map(|entry| entry.object_key.as_str())
            == Some("a")
            && second_page
                .entries
                .first()
                .map(|entry| entry.object_key.as_str())
                == Some("b"),
        "listing continuation did not retain its immutable view",
    )?;

    let source = create(provider, &format!("{namespace}-source"), "create-s").await?;
    put(provider, &source, "a", b"before", None, None).await?;
    let snapshot = provider
        .snapshot(source.clone(), Some("snapshot".into()))
        .await?
        .snapshot
        .ok_or(ObjectsError::Unavailable)?;
    put(provider, &source, "a", b"after", None, None).await?;
    let fork = provider
        .fork(
            ReadTarget::Snapshot(snapshot.clone()),
            format!("{namespace}-fork"),
            Some("fork".into()),
        )
        .await?
        .bucket
        .ok_or(ObjectsError::Unavailable)?;
    let forked = get(provider, ReadTarget::Bucket(fork), "a").await?;
    ensure(
        forked.body.as_ref() == b"before",
        "fork did not retain snapshot state",
    )?;
    ensure(
        provider
            .destroy_snapshot(snapshot, Some("destroy-snapshot".into()))
            .await?,
        "existing snapshot was not destroyed",
    )?;

    let multipart_bucket = create(provider, &format!("{namespace}-multipart"), "create-m").await?;
    let upload = provider
        .create_multipart(
            multipart_bucket.clone(),
            "large".into(),
            metadata(),
            None,
            Some("create-upload".into()),
        )
        .await?;
    ensure(
        get(
            provider,
            ReadTarget::Bucket(multipart_bucket.clone()),
            "large",
        )
        .await
            == Err(ObjectsError::NotFound),
        "multipart staging became visible before completion",
    )?;
    let part = provider
        .upload_part(
            multipart_bucket.clone(),
            "large".into(),
            upload.upload_id.clone(),
            1,
            Bytes::from_static(b"body"),
            Some("upload-part".into()),
        )
        .await?;
    ensure(
        provider
            .list_parts(
                multipart_bucket.clone(),
                "large".into(),
                upload.upload_id.clone(),
            )
            .await?
            == vec![part.clone()],
        "multipart part listing differed",
    )?;
    let completed = provider
        .complete_multipart(
            multipart_bucket.clone(),
            "large".into(),
            upload.upload_id,
            vec![part],
            Some("complete-upload".into()),
        )
        .await?;
    ensure(completed.size == 4, "multipart size differed")?;
    ensure(
        get(
            provider,
            ReadTarget::Bucket(multipart_bucket.clone()),
            "large",
        )
        .await?
        .body
        .as_ref()
            == b"body",
        "multipart body differed",
    )?;
    let aborted = provider
        .create_multipart(
            multipart_bucket.clone(),
            "aborted".into(),
            metadata(),
            None,
            Some("create-aborted".into()),
        )
        .await?;
    ensure(
        provider
            .abort_multipart(
                multipart_bucket.clone(),
                "aborted".into(),
                aborted.upload_id.clone(),
                Some("abort".into()),
            )
            .await?,
        "existing multipart upload was not aborted",
    )?;
    ensure(
        !provider
            .abort_multipart(
                multipart_bucket.clone(),
                "aborted".into(),
                aborted.upload_id,
                Some("abort-again".into()),
            )
            .await?,
        "absent multipart upload was reported as aborted",
    )?;

    let marker = provider
        .delete(
            multipart_bucket.clone(),
            "large".into(),
            None,
            None,
            Some("delete-marker".into()),
        )
        .await?;
    ensure(
        marker.existed && marker.marker.is_some_and(|version| version.delete_marker),
        "delete marker was not published",
    )?;
    ensure(
        provider
            .delete(
                multipart_bucket.clone(),
                "large".into(),
                Some(completed.version_id),
                None,
                Some("delete-version".into()),
            )
            .await?
            .existed,
        "exact retained version was not deleted",
    )?;
    let empty = create(provider, &format!("{namespace}-empty"), "create-e").await?;
    ensure(
        provider
            .delete_bucket(&empty, Some("delete-empty".into()))
            .await?,
        "empty bucket was not deleted",
    )?;
    ensure(
        provider
            .delete_bucket(&empty, Some("delete-empty".into()))
            .await?,
        "bucket deletion did not replay exactly",
    )?;
    Ok(())
}

async fn create(
    provider: &impl ObjectsProvider,
    name: &str,
    idempotency_key: &str,
) -> Result<wire::BucketRef, ObjectsError> {
    let first = provider
        .create_bucket(name.into(), Some(idempotency_key.into()))
        .await?;
    let replay = provider
        .create_bucket(name.into(), Some(idempotency_key.into()))
        .await?;
    ensure(first == replay, "bucket creation did not replay exactly")?;
    let reference = first.bucket.ok_or(ObjectsError::Unavailable)?;
    ensure(
        provider.head_bucket(&reference).await?.bucket == Some(reference.clone()),
        "bucket head did not preserve exact identity",
    )?;
    Ok(reference)
}

async fn put(
    provider: &impl ObjectsProvider,
    bucket: &wire::BucketRef,
    object_key: &str,
    body: &'static [u8],
    condition: Option<Condition>,
    idempotency_key: Option<&str>,
) -> Result<wire::ObjectVersion, ObjectsError> {
    provider
        .put(PutRequest {
            bucket: bucket.clone(),
            object_key: object_key.into(),
            body: Bytes::from_static(body),
            metadata: metadata(),
            condition,
            idempotency_key: idempotency_key.map(str::to_owned),
        })
        .await
}

async fn get(
    provider: &impl ObjectsProvider,
    target: ReadTarget,
    object_key: &str,
) -> Result<crate::BufferedObject, ObjectsError> {
    provider
        .get(GetRequest {
            target,
            object_key: object_key.into(),
            version_id: None,
            range: None,
            if_match: None,
            if_none_match: None,
            maximum_bytes: 1024,
        })
        .await
}

fn metadata() -> wire::ObjectMetadata {
    wire::ObjectMetadata {
        content_type: "application/octet-stream".into(),
        ..wire::ObjectMetadata::default()
    }
}

fn ensure(condition: bool, message: &'static str) -> Result<(), ObjectsError> {
    if condition {
        Ok(())
    } else {
        Err(ObjectsError::Invalid(message))
    }
}
