//! The same physical repair interleavings over each real medium.
use super::*;
use crate::Account;
use futures::FutureExt as _;

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// A payload key captured from the real engine, never an invented stored frame.
async fn payload(medium: &Shared, account: &[u8]) -> Result<String, Box<dyn std::error::Error>> {
    let prefix = format!("{}/p/", Digest::of(account));
    let keys = medium.inner.list(&prefix, None, 2).await?;
    assert_eq!(keys.len(), 1);
    Ok(keys[0].0.clone())
}

/// `account` reads back exactly `body`.
async fn holds<M: Medium>(account: &Account<M>, body: &OwnedBytes) -> TestResult {
    assert_eq!(
        account
            .get(Digest::of(body), ByteRange::FULL, u64::MAX)
            .await?
            .body(),
        body
    );
    Ok(())
}

/// Every body is verified on `account`.
async fn put_all<M: Medium>(account: &Account<M>, bodies: &[&OwnedBytes]) -> TestResult {
    for body in bodies {
        assert!(
            account
                .put((*body).clone(), Vec::new())
                .await?
                .verified()
                .is_some()
        );
    }
    Ok(())
}

/// Whether `keys` hold an envelope and a payload for `digest`.
fn stored_kinds<T>(keys: &[(String, T)], digest: Digest) -> (bool, bool) {
    let hex = digest.to_hex();
    let kind = |kind| {
        keys.iter()
            .any(|(key, _)| key.contains(kind) && key.contains(&hex))
    };
    (kind("/e/"), kind("/p/"))
}

pub(crate) async fn verify(medium: impl Medium + 'static) -> TestResult {
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

    let (mut reached, release) = medium.park("create", key.clone());
    let repair = account.put(body.clone(), Vec::new());
    tokio::pin!(repair);
    drive_to_park(repair.as_mut(), &mut reached).await;
    // The physical write is in flight at registration; its acknowledgement
    // has not yet published an invalidation. Verification races completion.
    let mut observation = account.availability(digest).await?;
    let mut unauthorized = other.availability(digest).await?;
    let initial = observation.cursor();
    assert!(observation.changed().now_or_never().is_none());
    release.send(()).expect("original repair remains alive");
    let repaired = repair.await?;
    assert!(matches!(repaired, Put::AlreadyExists { .. }));
    let repaired = repaired.verified().ok_or("repair was unknown")?;
    assert_eq!(repaired.revision, original.revision);
    assert_eq!(medium.inner.read(&key, None).await?.unwrap(), stored);
    let next = observation.changed().await?;
    assert_eq!(next.incarnation(), initial.incarnation());
    assert!(next.sequence() > initial.sequence());
    assert!(unauthorized.changed().now_or_never().is_none());
    holds(&account, &body).await?;
    restoration(&medium, &account, &mut observation, &key, stored, &body).await?;
    assert_eq!(account.head(digest).await?.revision, original.revision);
    cancellation(&medium, &account, observation, unauthorized, key, body).await
}

/// A remote physical producer shares the same scoped source. No catalog
/// mutation is required, and the event alone never decides verification.
async fn restoration(
    medium: &Shared,
    account: &Account<Shared>,
    observation: &mut crate::Availability,
    key: &str,
    stored: OwnedBytes,
    body: &OwnedBytes,
) -> TestResult {
    let digest = Digest::of(body);
    assert!(medium.inner.remove(key).await?);
    assert!(medium.inner.create(key, stored).await?);
    account.availability_source().restored(digest)?;
    observation.changed().await?;
    holds(account, body).await
}

/// Cancellation after sending the physical write leaves its acknowledgment
/// uncertain. Closing the source ends the original wait instead of hanging.
async fn cancellation(
    medium: &Shared,
    account: &Account<Shared>,
    mut observation: crate::Availability,
    mut unauthorized: crate::Availability,
    key: String,
    body: OwnedBytes,
) -> TestResult {
    let digest = Digest::of(&body);
    assert!(medium.inner.remove(&key).await?);
    let (mut reached, _release) = medium.park("create", key);
    {
        let repair = account.put(body, Vec::new());
        tokio::pin!(repair);
        drive_to_park(repair.as_mut(), &mut reached).await;
        // Dropping the original pending operation cancels its publication owner.
    }
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

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn local_physical_repair_has_race_free_availability() -> TestResult {
    let root = tempfile::tempdir()?;
    verify(crate::LocalMedium::open(root.path(), Default::default(), None).await?).await
}

async fn bounded_refusal_recovers(poison_refusal: bool) -> TestResult {
    let selected = OwnedBytes::from(vec![7; 128]);
    let filler = OwnedBytes::from(vec![9; 1024]);
    let refused = OwnedBytes::from(vec![11; 512]);
    let selected_digest = Digest::of(&selected);
    let filler_digest = Digest::of(&filler);
    let refused_digest = Digest::of(&refused);
    let probe = Store::new(MemoryMedium::default());
    put_all(
        &probe.account(b"bounded-repair")?,
        &[&selected, &filler, &refused],
    )
    .await?;
    // Size the budget from actual engine output: every envelope fits, but the
    // final payload is one byte over capacity. No encoded fixture is invented.
    let full: u64 = probe
        .medium()
        .list("", None, usize::MAX)
        .await?
        .iter()
        .map(|(_, bytes)| bytes)
        .sum();
    let medium = MemoryMedium::bounded(full - 1);
    let store = Store::new(medium.clone());
    let account = store.account(b"bounded-repair")?;
    put_all(&account, &[&selected, &filler]).await?;
    let mut observation = account.availability(selected_digest).await?;
    let mut retry_observation = account.availability(refused_digest).await?;
    assert!(matches!(
        account.put(refused.clone(), Vec::new()).await,
        Err(error) if error.code == ErrorCode::Unavailable
    ));
    let keys = medium.list("", None, usize::MAX).await?;
    assert_eq!(stored_kinds(&keys, refused_digest), (true, false));
    if poison_refusal {
        // Removed policy: poisoning on a definite, no-effect payload refusal.
        account.availability_source().fail();
    }
    assert!(
        observation.changed().now_or_never().is_none(),
        "definite refusal terminated unrelated observation"
    );
    assert!(retry_observation.changed().now_or_never().is_none());
    assert_eq!(account.delete(filler_digest).await?, Delete::Deleted);
    // The same Store must retain a live source, including a reopened account.
    let reopened = store.account(b"bounded-repair")?;
    assert!(matches!(
        reopened.put(refused.clone(), Vec::new()).await?,
        Put::AlreadyExists { .. }
    ));
    retry_observation.changed().await?;
    holds(&reopened, &refused).await?;
    let key = keys
        .iter()
        .find(|(key, _)| key.contains("/p/") && key.contains(&selected_digest.to_hex()))
        .ok_or("original selected payload")?
        .0
        .clone();
    assert!(medium.remove(&key).await?);
    assert!(matches!(
        reopened.put(selected.clone(), Vec::new()).await?,
        Put::AlreadyExists { .. }
    ));
    observation.changed().await?;
    holds(&reopened, &selected).await?;
    assert!(retry_observation.changed().now_or_never().is_none());
    Ok(())
}

#[tokio::test]
async fn definite_payload_refusal_preserves_observers_and_recovers_in_the_same_store() -> TestResult
{
    bounded_refusal_recovers(false).await
}

#[tokio::test]
#[should_panic(expected = "definite refusal terminated unrelated observation")]
async fn negative_control_poisoning_a_refused_write_ends_unrelated_observation() {
    bounded_refusal_recovers(true).await.unwrap();
}

#[tokio::test]
async fn ambiguous_payload_write_still_ends_its_original_observations() -> TestResult {
    let medium = Shared::default();
    let account = Store::new(medium.clone()).account(b"uncertain-payload")?;
    account.flush(None).await?;
    let digest = Digest::of(b"uncertain original payload");
    let mut observation = account.availability(digest).await?;
    *medium.ambiguous.lock().unwrap() = Some("/p/");
    assert!(matches!(
        account
            .put(
                OwnedBytes::from_static(b"uncertain original payload"),
                Vec::new()
            )
            .await?,
        Put::Unknown
    ));
    assert_eq!(
        observation.changed().await.unwrap_err().code,
        ErrorCode::Unavailable
    );
    Ok(())
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
