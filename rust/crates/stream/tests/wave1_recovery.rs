//! Persistent namespace and successful retry recovery through the local journal.
#![cfg(not(loom))]
use acyclic_stream::{
    IdempotencyKey, LocalStream, LocalStreamLimits, StreamError, StreamPath, StreamProvider,
    StreamProviderExt as _, conformance,
};
#[tokio::test]
async fn subtree_and_success_keys_survive_journal_and_snapshot_recovery()
-> Result<(), Box<dyn std::error::Error>> {
    for compact in [false, true] {
        let directory = tempfile::tempdir()?;
        let limits = LocalStreamLimits {
            compaction_operations: if compact { 3 } else { 1_000_000 },
            ..LocalStreamLimits::default()
        };
        let provider = LocalStream::open(directory.path(), limits).await?;
        conformance::verify(&acyclic_stream::conformance::Fixture::new(provider.clone())).await?;
        let key = IdempotencyKey::new(b"tree-adopt".to_vec())?;
        let observation = provider.inspect_idempotency(key.clone()).await?.unwrap();
        drop(provider);
        let provider = LocalStream::open(directory.path(), limits).await?;
        assert_eq!(
            provider
                .tail(StreamPath::new("conformance/tree/children/fork/a/events")?)
                .await?,
            2
        );
        assert_eq!(
            provider
                .tail(StreamPath::new(
                    "conformance/tree/children/fork/children/old/events"
                )?)
                .await,
            Err(StreamError::NotFound)
        );
        assert_eq!(
            provider
                .tail(StreamPath::new("conformance/tree/children/old/events")?)
                .await?,
            1
        );
        assert_eq!(provider.inspect_idempotency(key).await?, Some(observation));
        assert!(
            provider
                .inspect_idempotency(IdempotencyKey::new(b"stream-stale-commit".to_vec())?)
                .await?
                .is_none()
        );
    }
    Ok(())
}
