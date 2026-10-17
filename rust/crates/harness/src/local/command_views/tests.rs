use super::*;
use acyclic_stream::{MemoryStream, StreamPath};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // Only a change to the complete logical file may advance its projection.
    #[test]
    fn repeated_projections_preserve_the_cut(
        values in prop::collection::vec((prop::collection::vec(any::<u8>(), 0..256), any::<bool>()), 1..8),
        repeats in 1..5usize,
    ) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let volume = TaskVolume::open(Arc::new(MemoryStream::default()), StreamPath::new("projections").unwrap()).await.unwrap();
            let key = FileKey::parse(".ui/config/attention.json").unwrap();
            let query = Query::File(key.clone());
            let mut previous = None;
            let mut expected = volume.head().await.unwrap().position;
            for (bytes, identified) in values {
                let mut file = FileValue::new(key.clone(), bytes);
                file.publication = identified.then(|| OperationId::from_bytes([1; 16]));
                if previous.as_ref() != Some(&file) { expected += 1; }
                for _ in 0..repeats {
                    let cut = volume.snapshot(&query, 1).await.unwrap();
                    if cut.files.first() == Some(&file) {
                        use futures::FutureExt as _;
                        let future = publish_changed(&volume, &cut, &file);
                        #[cfg(feature = "test-support")]
                        let (result, examined) = crate::test_support::measure_task_volume_work(||
                            future.now_or_never());
                        #[cfg(not(feature = "test-support"))]
                        let result = future.now_or_never();
                        prop_assert!(result.is_some(), "unchanged projection waited for provider I/O");
                        result.unwrap().unwrap();
                        #[cfg(feature = "test-support")]
                        prop_assert_eq!(examined, 0, "unchanged projection re-entered the volume index");
                    } else {
                        publish_changed(&volume, &cut, &file).await.unwrap();
                    }
                    let after = volume.snapshot(&query, 1).await.unwrap();
                    prop_assert_eq!(after.generation.position, expected);
                    prop_assert_eq!(after.files.as_slice(), std::slice::from_ref(&file));
                }
                previous = Some(file);
            }
            Ok(())
        })?;
    }
}

#[tokio::test]
async fn unchanged_status_still_repairs_attention_after_interrupted_refresh() -> Result<()> {
    use crate::external_executor::{ExternalHost, request};
    let root = tempfile::tempdir().map_err(storage)?;
    let harness = LocalHarness::open(LocalHarnessConfig::external(root.path())).await?;
    let attach = request(
        ExternalHost::ClaudeCode,
        "SessionStart",
        json!({
            "session_id":"projection-repair", "cwd":root.path(),
        }),
    )?;
    harness.external_request(&attach).await?;
    let files = &harness.task_files.inner;
    let generation = files.head().await.map_err(storage)?;
    let attention = FileValue::new(
        FileKey::parse(".ui/config/attention.json").map_err(storage)?,
        serde_json::to_vec(&json!({"tasks":[attach.task_id()]})).map_err(storage)?,
    );
    files
        .append(generation, &attention)
        .await
        .map_err(storage)?;
    let before = files.head().await.map_err(storage)?;
    harness.refresh_command_task(attach.task_id()).await?;
    let after = files.head().await.map_err(storage)?;
    assert_eq!(
        after.position,
        before.position + 1,
        "only attention needed repair"
    );
    let repaired = files
        .snapshot(&Query::File(attention.key), 1)
        .await
        .map_err(storage)?;
    assert_eq!(
        serde_json::from_slice::<Value>(&repaired.files[0].bytes).map_err(storage)?,
        json!({"tasks":[]})
    );
    harness.refresh_command_task(attach.task_id()).await?;
    assert_eq!(
        files.head().await.map_err(storage)?,
        after,
        "idle refresh appended duplicate projections"
    );
    harness.close().await
}
