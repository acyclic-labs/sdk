//! Ordered partial batch results and durable receipts against actual MinIO IO.
use super::s3::observed_store::ObservedStore;
use acyclic_objects::{Blobs, ByteRange, Digest, Put, Store, s3::S3Medium, wire};
use acyclic_stream::OwnedBytes;
use object_store::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

pub async fn verify(
    store: Arc<dyn acyclic_objects::s3::Bucket>,
    prefix: Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let observed = Arc::new(ObservedStore {
        inner: store,
        body_bytes: AtomicUsize::new(0),
        body_gets: AtomicUsize::new(0),
        metadata_bytes: AtomicUsize::new(0),
        envelope_creates: AtomicUsize::new(0),
    });
    let account = Store::new(S3Medium::new(observed.clone(), prefix.as_ref())).account(b"batch")?;
    let alpha = OwnedBytes::from_static(b"alpha");
    let beta = OwnedBytes::from_static(b"beta");
    let missing = Digest::of(b"never stored");
    let results = account
        .put_batch(vec![
            (alpha.clone(), Vec::new()),
            (OwnedBytes::from_static(b"dangling"), vec![missing]),
            (beta.clone(), vec![Digest::of(&alpha)]),
            (alpha.clone(), Vec::new()),
        ])
        .await;
    assert_eq!(results.len(), 4);
    assert!(matches!(results[0], Ok(Put::Created { .. })));
    assert_eq!(
        results[1].as_ref().unwrap_err().code,
        wire::ErrorCode::MissingReference
    );
    assert!(matches!(results[2], Ok(Put::Created { .. })));
    assert!(matches!(results[3], Ok(Put::AlreadyExists { .. })));
    assert_eq!(
        observed.envelope_creates.load(Ordering::Relaxed),
        2,
        "each new blob commits exactly one envelope; refusals and replays commit none"
    );
    drop(account);
    let reopened = Store::new(S3Medium::new(observed, prefix.as_ref())).account(b"batch")?;
    for body in [alpha, beta] {
        let object = reopened.get(Digest::of(&body), ByteRange::FULL, 64).await?;
        assert_eq!(object.body().as_ref(), body.as_ref());
        assert!(matches!(
            reopened.put(body, Vec::new()).await?,
            Put::AlreadyExists { .. }
        ));
    }
    println!(
        "MinIO batch: ordered partial failures, one envelope per new blob, intra-batch replay and fresh-provider receipts passed"
    );
    Ok(())
}
