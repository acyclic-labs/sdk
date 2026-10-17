//! A durable receipt, unlike a commit acknowledgement, survives a power cut.
use super::*;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn path() -> StreamPath {
    StreamPath::new("receipts").unwrap()
}

async fn append(provider: &LocalStream, record: &'static [u8]) -> Result {
    let outcome = provider
        .append(AppendRequest {
            path: path(),
            records: vec![Bytes::from_static(record)],
            if_tail: None,
            idempotency_key: None,
        })
        .await?;
    assert!(matches!(outcome, crate::AppendOutcome::Committed(_)));
    Ok(())
}

/// Journal bytes the device holds: everything only while every frame is synced.
fn synced_bytes(provider: &LocalStream) -> Option<u64> {
    let journal = provider.inner.journal.lock().ok()?;
    (journal.flushed_sequence == Some(journal.appended)).then_some(journal.bytes)
}

/// Drops the provider and keeps only the journal bytes the device confirmed.
async fn power_cut(
    directory: &std::path::Path,
    provider: LocalStream,
    durable_bytes: u64,
) -> Result<LocalStream> {
    drop(provider);
    std::fs::OpenOptions::new()
        .write(true)
        .open(directory.join(JOURNAL_FILE))?
        .set_len(durable_bytes)?;
    Ok(LocalStream::open(directory, LocalStreamLimits::default()).await?)
}

/// `barrier` asks for a receipt after the deferred append; without it the
/// acknowledged record is all the writer holds, which is the negative control.
async fn survives_cut(barrier: bool) -> Result<u64> {
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    append(&provider, b"base").await?;
    let mut durable_bytes =
        synced_bytes(&provider).ok_or("required append left frames unsynced")?;
    deferring_durability(async {
        append(&provider, b"approval").await?;
        let observed = provider.watermarks(path()).await?;
        assert!(
            crate::DurableReceipt::covering(path(), &observed, 2).is_none(),
            "a deferred acknowledgement observed as durable"
        );
        if barrier {
            let receipt = provider
                .durable_receipt(path(), 2)
                .await?
                .ok_or("the barrier issued no receipt")?;
            assert_eq!((receipt.path(), receipt.through()), (&path(), 2));
            durable_bytes =
                synced_bytes(&provider).ok_or("receipt issued before the device sync")?;
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await?;
    let provider = power_cut(directory.path(), provider, durable_bytes).await?;
    Ok(provider.tail(path()).await?)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_receipt_covers_a_deferred_record_across_a_power_cut() -> Result {
    assert_eq!(survives_cut(true).await?, 2);
    Ok(())
}

/// Negative control: the acknowledgement alone, made under deferred
/// durability, is lost by the same cut.
#[tokio::test(flavor = "multi_thread")]
async fn negative_control_a_deferred_acknowledgement_is_lost_by_a_power_cut() -> Result {
    assert_eq!(survives_cut(false).await?, 1);
    Ok(())
}

#[cfg(target_vendor = "apple")]
async fn survives_barrier_cut(receipt: bool) -> Result<u64> {
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    append(&provider, b"base").await?;
    let mut flushed_bytes = synced_bytes(&provider).ok_or("base was not flushed")?;
    drop(provider);
    let provider = LocalStream::open(
        directory.path(),
        LocalStreamLimits {
            durability: LocalDurability::Barrier,
            ..LocalStreamLimits::default()
        },
    )
    .await?;
    append(&provider, b"approval").await?;
    assert_eq!(provider.inner.synced.borrow().ordered, 1);
    assert_eq!(provider.inner.synced.borrow().flushed, None);
    let marks = provider.watermarks(path()).await?;
    assert!(crate::DurableReceipt::covering(path(), &marks, 2).is_none());
    if receipt {
        let proof = provider
            .durable_receipt(path(), 2)
            .await?
            .ok_or("missing full flush")?;
        assert_eq!(proof.through(), 2);
        flushed_bytes = synced_bytes(&provider).ok_or("receipt was not flushed")?;
    }
    Ok(power_cut(directory.path(), provider, flushed_bytes)
        .await?
        .tail(path())
        .await?)
}

#[cfg(target_vendor = "apple")]
#[tokio::test(flavor = "multi_thread")]
async fn barrier_needs_a_device_flush_before_a_receipt_survives_power_loss() -> Result {
    assert_eq!(survives_barrier_cut(true).await?, 2);
    Ok(())
}

#[cfg(target_vendor = "apple")]
#[tokio::test(flavor = "multi_thread")]
async fn negative_control_barrier_acknowledgement_is_lost_by_power_loss() -> Result {
    assert_eq!(survives_barrier_cut(false).await?, 1);
    Ok(())
}
