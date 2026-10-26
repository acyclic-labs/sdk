use acyclic_objects::{
    ObjectsProvider,
    s3::{Capabilities, S3Objects},
    wire,
};
use acyclic_stream::OwnedBytes;
use object_store::{ObjectStore, path::Path};
use std::{sync::Arc, time::Instant};

pub async fn verify(
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    capabilities: Capabilities,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let left = S3Objects::open(store.clone(), prefix.clone(), capabilities).await?;
    let right = S3Objects::open(store.clone(), prefix.clone(), capabilities).await?;
    let bucket = left
        .create_bucket(wire::CreateBucketRequest {
            name: "independent-clients".into(),
            mutation: None,
        })
        .await?
        .bucket
        .ok_or("bucket identity missing")?;
    let request = wire::PutObjectHeader {
        bucket: Some(bucket.clone()),
        object_key: "race".into(),
        preconditions: Some(wire::Preconditions {
            condition: Some(wire::preconditions::Condition::IfAbsent(true)),
        }),
        ..Default::default()
    };
    let body = OwnedBytes::from_static(b"race");
    let (a, b) = tokio::join!(
        left.put(request.clone(), body.clone()),
        right.put(request.clone(), body.clone())
    );
    let winner = one_winner(a, b)?;
    let mut replacement = request;
    replacement.preconditions = Some(wire::Preconditions {
        condition: Some(wire::preconditions::Condition::IfMatch(winner.etag)),
    });
    let (a, b) = tokio::join!(
        left.put(replacement.clone(), body.clone()),
        right.put(replacement, body)
    );
    one_winner(a, b)?;

    // This exceeds the physical multipart threshold, not merely the logical
    // multipart walkthrough's small fixture bodies.
    let body = OwnedBytes::from(vec![0x5a; 6 * 1024 * 1024 + 17]);
    let request = wire::PutObjectHeader {
        bucket: Some(bucket.clone()),
        object_key: "large".into(),
        mutation: Some(wire::MutationIdentity {
            idempotency_key: "large-replay".into(),
        }),
        ..Default::default()
    };
    let info = left.put(request.clone(), body.clone()).await?;
    let reopened = S3Objects::open(store, prefix, capabilities).await?;
    assert_eq!(reopened.put(request, body.clone()).await?, info);
    let mut query = wire::GetObjectRequest {
        bucket: Some(bucket.clone()),
        object_key: "large".into(),
        ..Default::default()
    };
    assert_eq!(
        reopened.get(query.clone(), body.len() as u64).await?.body(),
        &body
    );
    query.range = Some(wire::ByteRange {
        selection: Some(wire::byte_range::Selection::SuffixLength(17)),
    });
    assert_eq!(right.get(query, 17).await?.body().as_ref(), &[0x5a; 17]);
    left.delete(wire::DeleteObjectRequest {
        bucket: Some(bucket.clone()),
        object_key: "large".into(),
        ..Default::default()
    })
    .await?;
    assert!(matches!(
        right
            .head(wire::HeadObjectRequest {
                bucket: Some(bucket),
                object_key: "large".into(),
                ..Default::default()
            })
            .await,
        Err(acyclic_objects::Error {
            code: wire::ErrorCode::NotFound
        })
    ));
    println!(
        "S3 independent clients: create/update CAS races, immediate create/update/delete visibility, physical multipart, ranges and exact reopen replay: {:?}",
        start.elapsed()
    );
    Ok(())
}

fn one_winner(
    a: Result<wire::ObjectInfo, acyclic_objects::Error>,
    b: Result<wire::ObjectInfo, acyclic_objects::Error>,
) -> Result<wire::ObjectInfo, Box<dyn std::error::Error>> {
    match (a,b) {
        (Ok(info),Err(error)) | (Err(error),Ok(info)) if error.code == wire::ErrorCode::PreconditionFailed => Ok(info),
        _ => Err("step=s3-independent-cas: expected exactly one winner; recovery: inspect MinIO conditional PUT support and rerun storage-conformance".into()),
    }
}
