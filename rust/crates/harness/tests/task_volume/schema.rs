use super::*;
use acyclic_harness::task_volume::{FileValue, SCHEMA};
use acyclic_stream::{AppendRequest, Record};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    // Packed segments are unchanged: every readable header must retain its history,
    // survive new writes, and decode identically through both public readers.
    #[test]
    fn readable_histories_reopen_and_accept_current_records(bytes in prop::collection::vec(any::<u8>(), 0..256)) {
        let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
        runtime.block_on(async move {
            for schema in 2..=SCHEMA {
              for key in [".tasks/t/status.json", "config/settings.json", "GOAL.md"] {
                let provider = Arc::new(MemoryStream::default());
                let path = StreamPath::new("schema-history")?;
                let header = serde_json::to_vec(&serde_json::json!({
                    "volume":uuid::Uuid::new_v4(), "epoch":uuid::Uuid::new_v4(), "schema":schema,
                }))?;
                provider.append(AppendRequest {path:path.clone(), records:vec![header.into()],
                    if_tail:Some(0), idempotency_key:None}).await?;
                let volume = TaskVolume::open(provider.clone(), path.clone()).await?;
                let snapshot = volume.snapshot(&Query::Root, 1).await?;
                let file = Write::new(FileKey::parse(key)?, bytes.clone());
                volume.append(snapshot.generation, &file).await?;
                let mut records = provider.read(ReadRequest {path:path.clone(), from:0, limit:2}).await?;
                let header: Record = records.next().await.ok_or("missing header")??;
                assert!(FileValue::from_record(&header)?.is_none());
                let record = records.next().await.ok_or("missing segment")??;
                assert_eq!(FileValue::from_record(&record)?, Some(file.clone()));
                let reopened = TaskVolume::open(provider, path).await?;
                assert_eq!(reopened.snapshot(&Query::Root, 1).await?.files, vec![file]);
              }
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        }).unwrap();
    }
}
