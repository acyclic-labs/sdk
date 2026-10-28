use acyclic_objects::{Blobs, ByteRange, Delete, Digest, Put, Store, s3::S3Medium, wire};
use acyclic_stream::OwnedBytes;
use object_store::{ObjectStore, path::Path};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};
#[path = "observed_store.rs"]
pub(super) mod observed_store;

fn account(
    store: Arc<dyn ObjectStore>,
    prefix: &Path,
) -> Result<acyclic_objects::Account<S3Medium>, acyclic_objects::Error> {
    Store::new(S3Medium::new(store, prefix.as_ref())).account(b"storage-conformance")
}

pub async fn verify(
    store: Arc<dyn ObjectStore>,
    prefix: Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let left = account(store.clone(), &prefix)?;
    let right = account(store.clone(), &prefix)?;
    let body = OwnedBytes::from_static(b"race");
    let (a, b) = tokio::join!(
        left.put(body.clone(), Vec::new()),
        right.put(body.clone(), Vec::new())
    );
    one_creator(a?, b?)?;

    // This exceeds the physical multipart threshold, not merely small fixture bodies.
    let body = OwnedBytes::from(vec![0x5a; 6 * 1024 * 1024 + 17]);
    let digest = Digest::of(&body);
    left.put(body.clone(), Vec::new()).await?;
    let reopened = account(store.clone(), &prefix)?;
    assert!(matches!(
        reopened.put(body.clone(), Vec::new()).await?,
        Put::AlreadyExists { verified } if verified.digest == digest
    ));
    assert_eq!(
        reopened
            .get(digest, ByteRange::FULL, body.len() as u64)
            .await?
            .body(),
        &body
    );
    let length = body.len() as u64;
    assert_eq!(
        right
            .get(digest, ByteRange::new(length - 17, length), 17)
            .await?
            .body()
            .as_ref(),
        &[0x5a; 17]
    );
    assert_eq!(left.delete(digest).await?, Delete::Deleted);
    assert!(matches!(
        right.head(digest).await,
        Err(acyclic_objects::Error {
            code: wire::ErrorCode::NotFound,
            ..
        })
    ));
    println!(
        "S3 independent clients: create race, immediate create/delete visibility, physical multipart, ranges and reopen: {:?}",
        start.elapsed()
    );
    super::batch::verify(store.clone(), prefix.clone().join("batch")).await?;
    cold_ranges(store, prefix.clone().join("cold-range")).await?;
    Ok(())
}

fn one_creator(a: Put, b: Put) -> Result<(), Box<dyn std::error::Error>> {
    match (a, b) {
        (Put::Created { verified: x }, Put::AlreadyExists { verified: y })
        | (Put::AlreadyExists { verified: x }, Put::Created { verified: y })
            if x == y => Ok(()),
        _ => Err("step=s3-independent-create: expected exactly one creator; recovery: inspect MinIO conditional PUT support and rerun storage-conformance".into()),
    }
}

async fn cold_ranges(
    store: Arc<dyn ObjectStore>,
    prefix: Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let observed = Arc::new(observed_store::ObservedStore {
        inner: store,
        body_bytes: AtomicUsize::new(0),
        body_gets: AtomicUsize::new(0),
        metadata_bytes: AtomicUsize::new(0),
        envelope_creates: AtomicUsize::new(0),
    });
    let provider = account(observed.clone(), &prefix)?;
    for generation in 1..=8u8 {
        let body = OwnedBytes::from(vec![generation; 1024 * 1024]);
        let digest = Digest::of(&body);
        provider.put(body, Vec::new()).await?;
        if generation != 1 && generation != 8 {
            continue;
        }
        observed.body_bytes.store(0, Ordering::Relaxed);
        observed.body_gets.store(0, Ordering::Relaxed);
        observed.metadata_bytes.store(0, Ordering::Relaxed);
        let fresh = account(observed.clone(), &prefix)?;
        let length = 1024 * 1024;
        let response = fresh
            .get(digest, ByteRange::new(length - 1, length), 1)
            .await?;
        assert_eq!(response.body().as_ref(), &[generation]);
        assert_eq!(
            observed.body_bytes.load(Ordering::Relaxed),
            1,
            "cold range fetched other blobs or unselected bytes"
        );
        assert_eq!(observed.body_gets.load(Ordering::Relaxed), 1);
        let metadata = observed.metadata_bytes.load(Ordering::Relaxed);
        assert!(
            metadata < 4096,
            "one blob required unbounded metadata: {metadata}"
        );
        println!(
            "MinIO cold suffix among {generation} x 1 MiB blobs: body GETs=1, received body bytes=1, received envelope bytes={metadata}"
        );
    }
    Ok(())
}
