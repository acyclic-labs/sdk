//! Ordered partial batch results and durable receipts against actual MinIO IO.
use super::s3::observed_store::ObservedStore;
use acyclic_objects::{
    ObjectsProvider,
    s3::{Capabilities, S3Objects},
    wire,
};
use acyclic_stream::OwnedBytes;
use object_store::{ObjectStore, path::Path};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

pub async fn verify(
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    capabilities: Capabilities,
) -> Result<(), Box<dyn std::error::Error>> {
    let observed = Arc::new(ObservedStore {
        inner: store,
        body_bytes: AtomicUsize::new(0),
        body_gets: AtomicUsize::new(0),
        metadata_bytes: AtomicUsize::new(0),
        head_updates: AtomicUsize::new(0),
    });
    let provider = S3Objects::open(observed.clone(), prefix.clone(), capabilities).await?;
    let bucket = provider
        .create_bucket(wire::CreateBucketRequest {
            name: "batch".into(),
            mutation: None,
        })
        .await?
        .bucket
        .ok_or("batch bucket missing")?;
    let other = provider
        .create_bucket(wire::CreateBucketRequest {
            name: "other".into(),
            mutation: None,
        })
        .await?
        .bucket
        .ok_or("second batch bucket missing")?;
    let base = provider
        .put(
            wire::PutObjectHeader {
                bucket: Some(bucket.clone()),
                object_key: "alpha".into(),
                ..Default::default()
            },
            OwnedBytes::from_static(b"base"),
        )
        .await?;
    let update = wire::PutObjectHeader {
        bucket: Some(bucket.clone()),
        object_key: "alpha".into(),
        mutation: Some(wire::MutationIdentity {
            idempotency_key: "batch-alpha".into(),
        }),
        preconditions: Some(wire::Preconditions {
            condition: Some(wire::preconditions::Condition::IfMatch(base.etag)),
        }),
        ..Default::default()
    };
    let mut conflict = update.clone();
    conflict.mutation = None;
    let mut invalid = update.clone();
    invalid.object_key.clear();
    invalid.mutation = None;
    let beta = wire::PutObjectHeader {
        bucket: Some(bucket.clone()),
        object_key: "beta".into(),
        mutation: Some(wire::MutationIdentity {
            idempotency_key: "batch-beta".into(),
        }),
        ..Default::default()
    };
    let mut cross_bucket = update.clone();
    cross_bucket.bucket = Some(other);
    observed.head_updates.store(0, Ordering::Relaxed);
    let results = provider
        .put_batch(vec![
            (update.clone(), OwnedBytes::from_static(b"updated")),
            (conflict, OwnedBytes::from_static(b"conflict")),
            (invalid, OwnedBytes::from_static(b"invalid")),
            (beta.clone(), OwnedBytes::from_static(b"beta")),
            (update.clone(), OwnedBytes::from_static(b"updated")),
            (cross_bucket, OwnedBytes::from_static(b"updated")),
        ])
        .await;
    assert_eq!(results.len(), 6);
    let alpha = results[0].clone()?;
    let beta_info = results[3].clone()?;
    assert_eq!(
        results[1].as_ref().unwrap_err().code,
        wire::ErrorCode::PreconditionFailed
    );
    assert_eq!(
        results[2].as_ref().unwrap_err().code,
        wire::ErrorCode::InvalidArgument
    );
    assert_eq!(results[4].as_ref().unwrap(), &alpha);
    assert_eq!(
        results[5].as_ref().unwrap_err().code,
        wire::ErrorCode::IdempotencyMismatch
    );
    assert_eq!(
        observed.head_updates.load(Ordering::Relaxed),
        1,
        "one batch must publish under one root CAS"
    );
    drop(provider);
    let reopened = S3Objects::open(observed, prefix, capabilities).await?;
    for (request, body, info) in [
        (update, b"updated".as_slice(), alpha),
        (beta, b"beta".as_slice(), beta_info),
    ] {
        assert_eq!(
            reopened
                .put(request.clone(), OwnedBytes::from(body.to_vec()))
                .await?,
            info
        );
        let object = reopened
            .get(
                wire::GetObjectRequest {
                    bucket: Some(bucket.clone()),
                    object_key: request.object_key,
                    ..Default::default()
                },
                64,
            )
            .await?;
        assert_eq!(object.body().as_ref(), body);
    }
    println!(
        "MinIO batch: one conditional root publication, ordered partial failures, intra-batch replay, cross-bucket identity and fresh-provider receipt replay passed"
    );
    Ok(())
}
