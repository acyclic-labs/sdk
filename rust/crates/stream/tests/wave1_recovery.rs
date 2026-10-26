#![cfg(all(feature = "local", not(acyclic_loom)))]
use acyclic_stream::{conformance, LocalStream, LocalStreamLimits, StreamProvider, StreamPath, StreamError, IdempotencyKey};
#[tokio::test]
async fn subtree_and_success_keys_survive_journal_and_snapshot_recovery() -> Result<(), Box<dyn std::error::Error>> {
    for compact in [false, true] {
        let directory = tempfile::tempdir()?;
        let limits = LocalStreamLimits { journal_operations: if compact { 3 } else { 1_000_000 }, ..LocalStreamLimits::default() };
        let provider = LocalStream::open(directory.path(), limits).await?;
        conformance::verify(&provider).await?;
        let key = IdempotencyKey::new(b"tree-adopt".to_vec())?;
        let observation = provider.inspect_idempotency(key.clone()).await?.unwrap();
        drop(provider);
        let provider = LocalStream::open(directory.path(), limits).await?;
        assert_eq!(provider.tail(StreamPath::new("conformance/tree/children/fork/a/events")?).await?, 2);
        assert_eq!(provider.tail(StreamPath::new("conformance/tree/children/fork/children/old/events")?).await, Err(StreamError::NotFound));
        assert_eq!(provider.tail(StreamPath::new("conformance/tree/children/old/events")?).await?, 1);
        assert_eq!(provider.inspect_idempotency(key).await?, Some(observation));
        assert!(provider.inspect_idempotency(IdempotencyKey::new(b"stream-stale-commit".to_vec())?).await?.is_none());
    }
    Ok(())
}
