//! The same physical repair interleavings over each real medium.
use super::*;
use futures::FutureExt as _;

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// A payload key captured from the real engine, never an invented stored frame.
async fn payload(medium: &Shared, account: &[u8]) -> Result<String, Box<dyn std::error::Error>> {
    let prefix = format!("{}/p/", Digest::of(account));
    let keys = medium.inner.list(&prefix, None, 2).await?;
    assert_eq!(keys.len(), 1);
    Ok(keys[0].0.clone())
}

pub(crate) async fn verify(medium: impl Medium) -> TestResult {
    let medium = Shared::over(medium, false);
    let store = Store::new(medium.clone());
    let account = store.account(b"availability-repair")?;
    let other = store.account(b"availability-other")?;
    let body = OwnedBytes::from_static(b"same envelope and version after physical restoration");
    let digest = Digest::of(&body);
    let original = account.put(body.clone(), Vec::new()).await?;
    let original = original.verified().ok_or("original put was unknown")?;
    let key = payload(&medium, b"availability-repair").await?;
    let stored = medium.inner.read(&key, None).await?.ok_or("real payload")?;
    assert!(medium.inner.remove(&key).await?);
    assert_eq!(
        account
            .get(digest, ByteRange::FULL, u64::MAX)
            .await
            .err()
            .unwrap()
            .code,
        ErrorCode::NotFound
    );

    let (reached, release) = medium.park("create", key.clone());
    let writer = account.clone();
    let repair_body = body.clone();
    let repair = tokio::spawn(async move { writer.put(repair_body, Vec::new()).await });
    reached.notified().await;
    // The physical write is in flight at registration; its acknowledgement
    // has not yet published an invalidation. Verification races completion.
    let mut observation = account.availability(digest).await?;
    let mut unauthorized = other.availability(digest).await?;
    let initial = observation.cursor();
    assert!(observation.changed().now_or_never().is_none());
    release.notify_one();
    let repaired = repair.await??;
    assert!(matches!(repaired, Put::AlreadyExists { .. }));
    let repaired = repaired.verified().ok_or("repair was unknown")?;
    assert_eq!(repaired.revision, original.revision);
    assert_eq!(medium.inner.read(&key, None).await?.unwrap(), stored);
    let next = observation.changed().await?;
    assert_eq!(next.incarnation(), initial.incarnation());
    assert!(next.sequence() > initial.sequence());
    assert!(unauthorized.changed().now_or_never().is_none());
    assert_eq!(
        account.get(digest, ByteRange::FULL, u64::MAX).await?.body(),
        &body
    );

    // A remote physical producer shares the same scoped source. No catalog
    // mutation is required, and the event alone never decides verification.
    assert!(medium.inner.remove(&key).await?);
    assert!(medium.inner.create(&key, stored).await?);
    account.availability_source().restored(digest)?;
    observation.changed().await?;
    assert_eq!(
        account.get(digest, ByteRange::FULL, u64::MAX).await?.body(),
        &body
    );
    assert_eq!(account.head(digest).await?.revision, original.revision);

    // Cancellation after sending the physical write leaves its acknowledgment
    // uncertain. Closing the source ends the original wait instead of hanging.
    assert!(medium.inner.remove(&key).await?);
    let (reached, _release) = medium.park("create", key);
    let writer = account.clone();
    let repair = tokio::spawn(async move { writer.put(body, Vec::new()).await });
    reached.notified().await;
    repair.abort();
    assert!(repair.await.unwrap_err().is_cancelled());
    assert_eq!(
        observation.changed().await.unwrap_err().code,
        ErrorCode::Unavailable
    );
    assert!(
        matches!(account.availability(digest).await, Err(error) if error.code == ErrorCode::Unavailable)
    );
    assert!(unauthorized.changed().now_or_never().is_none());
    Ok(())
}

#[tokio::test]
async fn memory_physical_repair_has_race_free_availability() -> TestResult {
    verify(MemoryMedium::default()).await
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
#[tokio::test]
async fn local_physical_repair_has_race_free_availability() -> TestResult {
    let root = tempfile::tempdir()?;
    verify(crate::LocalMedium::open(root.path(), Default::default(), None).await?).await
}

#[tokio::test]
#[should_panic(expected = "catalog-only observation misses the physical repair")]
async fn negative_control_catalog_watch_misses_bytes_only_repair() {
    let medium = Shared::default();
    let account = Store::new(medium.clone()).account(b"catalog-only").unwrap();
    let body = OwnedBytes::from_static(b"actual unchanged catalog");
    let digest = Digest::of(&body);
    account.put(body.clone(), Vec::new()).await.unwrap();
    let before = account.head(digest).await.unwrap().revision;
    let key = payload(&medium, b"catalog-only").await.unwrap();
    medium.inner.remove(&key).await.unwrap();
    account.put(body, Vec::new()).await.unwrap();
    let after = account.head(digest).await.unwrap().revision;
    assert_ne!(
        before, after,
        "catalog-only observation misses the physical repair"
    );
}
