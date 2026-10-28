use crate::{
    Blobs, ByteRange, Delete, Digest, Medium, MediumError, MemoryMedium, Put, RootId, Store,
    conformance, wire::ErrorCode,
};
use acyclic_stream::OwnedBytes;
use std::ops::Range;
use std::sync::atomic::{AtomicU8, Ordering};

#[tokio::test]
async fn memory_store_passes_the_shared_suite() -> Result<(), Box<dyn std::error::Error>> {
    let store = Store::new(MemoryMedium::default());
    conformance::verify(&store.account(b"first")?, &store.account(b"second")?, "memory").await?;
    Ok(())
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
#[tokio::test]
async fn local_store_passes_the_shared_suite_and_reopens() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let store = Store::new(crate::LocalMedium::open(root.path(), Default::default(), None).await?);
    conformance::verify(&store.account(b"first")?, &store.account(b"second")?, "local").await?;
    let body = OwnedBytes::from(vec![9; 128]);
    let digest = Digest::of(&body);
    store.account(b"first")?.put(body.clone(), Vec::new()).await?;
    assert!(matches!(
        crate::LocalMedium::open(root.path(), Default::default(), None).await,
        Err(crate::local::LocalOpenError::AlreadyOwned)
    ));
    drop(store);
    let reopened = Store::new(crate::LocalMedium::open(root.path(), Default::default(), None).await?);
    let object = reopened.account(b"first")?.get(digest, ByteRange::FULL, 128).await?;
    assert_eq!(object.body(), &body);
    Ok(())
}

/// Fails selected operations: bit 0 makes creates ambiguous, bit 1 fails reads of root batches.
#[derive(Default)]
struct Faulty {
    inner: MemoryMedium,
    faults: AtomicU8,
}

#[async_trait::async_trait]
impl Medium for Faulty {
    async fn create(&self, key: &str, bytes: OwnedBytes) -> Result<bool, MediumError> {
        if self.faults.load(Ordering::SeqCst) & 1 != 0 {
            return Err(MediumError::Ambiguous(std::io::ErrorKind::TimedOut.into()));
        }
        self.inner.create(key, bytes).await
    }
    async fn read(&self, key: &str, range: Option<Range<u64>>) -> Result<Option<OwnedBytes>, MediumError> {
        if self.faults.load(Ordering::SeqCst) & 2 != 0 && key.contains("/r/") {
            return Err(MediumError::Failed(std::io::ErrorKind::TimedOut.into()));
        }
        self.inner.read(key, range).await
    }
    async fn remove(&self, key: &str) -> Result<bool, MediumError> {
        self.inner.remove(key).await
    }
    async fn list(&self, prefix: &str, after: Option<&str>, limit: usize) -> Result<Vec<(String, u64)>, MediumError> {
        self.inner.list(prefix, after, limit).await
    }
}

#[tokio::test]
async fn ambiguous_publication_is_unknown_and_retry_safe() -> Result<(), Box<dyn std::error::Error>> {
    let store = Store::new(Faulty::default());
    let blobs = store.account(b"account")?;
    let body = OwnedBytes::from(vec![1; 64]);
    store.medium().faults.store(1, Ordering::SeqCst);
    assert_eq!(blobs.put(body.clone(), Vec::new()).await?, Put::Unknown);
    store.medium().faults.store(0, Ordering::SeqCst);
    assert!(matches!(blobs.put(body, Vec::new()).await?, Put::Created { .. }));
    Ok(())
}

#[tokio::test]
async fn incomplete_root_scan_suspends_collection() -> Result<(), Box<dyn std::error::Error>> {
    let store = Store::new(Faulty::default());
    let blobs = store.account(b"account")?;
    let Put::Created { verified } = blobs.put(vec![2; 64].into(), Vec::new()).await? else {
        return Err("not created".into());
    };
    blobs.retain(&RootId::new(*b"root")?, vec![verified.digest]).await?;
    store.medium().faults.store(2, Ordering::SeqCst);
    for _ in 0..3 {
        assert_eq!(blobs.collect().await.map_err(|error| error.code), Err(ErrorCode::Suspended));
    }
    assert_eq!(blobs.delete(verified.digest).await.map_err(|error| error.code), Err(ErrorCode::Suspended));
    store.medium().faults.store(0, Ordering::SeqCst);
    blobs.collect().await?;
    blobs.collect().await?;
    assert_eq!(blobs.head(verified.digest).await?.length, 64);
    assert_eq!(blobs.delete(verified.digest).await?, Delete::Referenced);
    Ok(())
}

#[tokio::test]
async fn unrooted_blobs_survive_exactly_one_collection() -> Result<(), Box<dyn std::error::Error>> {
    let blobs = crate::MemoryObjects::memory();
    let Put::Created { verified } = blobs.put(vec![3; 8].into(), Vec::new()).await? else {
        return Err("not created".into());
    };
    assert_eq!(blobs.collect().await?.swept, 0);
    assert_eq!(blobs.head(verified.digest).await?.length, 8);
    assert_eq!(blobs.collect().await?.swept, 1);
    assert_eq!(blobs.head(verified.digest).await.map_err(|error| error.code), Err(ErrorCode::NotFound));
    Ok(())
}

#[test]
fn manifest_and_envelope_encodings_round_trip() -> Result<(), crate::Error> {
    let manifest = crate::Manifest {
        parts: vec![crate::Part {
            digest: Digest::of(b"a"),
            length: 1,
        }],
        edges: vec![Digest::of(b"b")],
    };
    assert_eq!(crate::Manifest::decode(&manifest.encode())?, manifest);
    let mut trailing = manifest.encode();
    trailing.push(0);
    assert!(crate::Manifest::decode(&trailing).is_err());
    assert_eq!(ByteRange::FULL.resolve(0)?, 0..0);
    assert!(ByteRange::new(1, 1).resolve(4).is_err());
    Ok(())
}
