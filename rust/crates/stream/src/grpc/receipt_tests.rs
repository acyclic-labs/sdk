use super::tests::{TestResult, client};
use super::*;

async fn append(provider: &impl StreamProvider, path: &StreamPath) -> TestResult {
    let outcome = provider
        .append(AppendRequest {
            path: path.clone(),
            records: vec![crate::OwnedBytes::from_static(b"approval")],
            if_tail: None,
            idempotency_key: None,
        })
        .await?;
    assert!(matches!(outcome, AppendOutcome::Committed(_)));
    Ok(())
}

#[tokio::test]
async fn remote_memory_receipts_preserve_the_retained_prefix() -> TestResult {
    let memory = crate::MemoryStream::default();
    let path = StreamPath::new("approval")?;
    append(&memory, &path).await?;
    let remote = client(memory.clone())?;
    // Acceptance alone makes no persistence claim, on either side.
    assert_eq!(remote.watermarks(path.clone()).await?.durable, 0);
    let receipt = remote
        .durable_receipt(path.clone(), 1)
        .await?
        .ok_or("missing receipt")?;
    assert_eq!((receipt.path(), receipt.through()), (&path, 1));
    assert!(remote.durable_receipt(path.clone(), 2).await?.is_none());
    assert_eq!(
        remote.durable_receipt(path.clone(), 1).await?,
        memory.durable_receipt(path, 1).await?
    );
    Ok(())
}

#[cfg(feature = "local")]
#[tokio::test(flavor = "multi_thread")]
async fn remote_local_receipts_flush_only_at_the_explicit_barrier() -> TestResult {
    let directory = tempfile::tempdir()?;
    let local =
        crate::LocalStream::open(directory.path(), crate::LocalStreamLimits::default()).await?;
    let path = StreamPath::new("approval")?;
    let remote = client(local.clone())?;
    crate::deferring_durability(async {
        append(&local, &path).await?;
        // Negative control: the transport cannot turn a deferred acknowledgement
        // or an ordinary accepted observation into durability.
        let marks = remote.watermarks(path.clone()).await?;
        assert_eq!((marks.accepted, marks.durable), (1, 0));
        assert!(crate::DurableReceipt::covering(path.clone(), &marks, 1).is_none());
        assert!(remote.durable_receipt(path.clone(), 1).await?.is_some());
        assert_eq!(remote.watermarks(path.clone()).await?.durable, 1);
        Ok(()) as TestResult
    })
    .await?;
    drop(remote);
    drop(local);
    Ok(())
}
