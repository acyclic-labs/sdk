use super::*;
use proptest::prelude::*;
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

async fn qualify(
    provider: &LocalStream,
    namespace: usize,
    operations: &[u8],
    early_durable: bool,
) -> Result {
    // Fresh paths have no accepted or durable records. Earlier traces may
    // leave pending frames in the same journal; only an explicit trace flush
    // can make this trace's new records durable. No setup barrier is needed.
    let paths = ["a", "b", "c"].map(|path| StreamPath::new(format!("{namespace}/{path}")).unwrap());
    let mut accepted = [0; 3];
    let mut durable = [0; 3];
    let mut frames = *provider.inner.revision.borrow();
    let mut flushed = frames;
    for &operation in operations {
        match operation % 4 {
            3 => {
                provider.flush()?;
                flushed = frames;
            }
            selected => {
                let selected = usize::from(selected);
                let (Some(path), Some(count)) = (paths.get(selected), accepted.get_mut(selected))
                else {
                    return Err("operation names no path".into());
                };
                deferring_durability(provider.append(AppendRequest {
                    path: path.clone(),
                    records: vec![(Bytes::from_static(b"accepted")).into()],
                    if_tail: None,
                    idempotency_key: crate::IdempotencyKey::unique(),
                }))
                .await?;
                *count += 1;
                frames += 1;
            }
        }
        assert_eq!(
            *provider.inner.revision.borrow(),
            frames,
            "publication count differs from accepted journal frames"
        );
        assert_eq!(
            provider
                .inner
                .journal
                .lock()
                .map_err(|_| "journal")?
                .appended,
            frames
        );
        if early_durable {
            provider.inner.synced.send_replace(SyncProgress {
                ordered: frames,
                flushed: Some(frames),
            });
        }
        for ((path, accepted), durable) in paths.iter().zip(accepted).zip(&mut durable) {
            if accepted == 0 {
                assert_eq!(
                    provider.watermarks(path.clone()).await,
                    Err(StreamError::NotFound)
                );
                continue;
            }
            if flushed == frames {
                *durable = accepted;
            }
            let actual = provider.watermarks(path.clone()).await?;
            assert_eq!(actual.accepted, accepted);
            assert_eq!(
                actual.durable, *durable,
                "durable watermark must reflect only a completed device cut"
            );
            assert!(actual.durable <= actual.accepted);
        }
    }
    Ok(())
}

// The independent reference tracks append and explicit-flush events, never
// the production watch comparison or the watermark result as its oracle.
#[test]
fn accepted_and_durable_watch_cuts_match_the_journal_and_reference() -> Result {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let directory = tempfile::tempdir()?;
    let provider = runtime.block_on(LocalStream::open(
        directory.path(),
        LocalStreamLimits::default(),
    ))?;
    let namespace = std::cell::Cell::new(0);
    proptest::test_runner::TestRunner::new(ProptestConfig {
        source_file: Some(file!()),
        test_name: Some(concat!(
            module_path!(),
            "::accepted_and_durable_watch_cuts_match_the_journal_and_reference"
        )),
        ..ProptestConfig::with_cases(64)
    })
    .run(&prop::collection::vec(any::<u8>(), 0..24), |operations| {
        let selected = namespace.get();
        namespace.set(selected + 1);
        runtime
            .block_on(qualify(&provider, selected, &operations, false))
            .unwrap();
        Ok(())
    })?;
    Ok(())
}
#[tokio::test]
#[should_panic(expected = "durable watermark must reflect only a completed device cut")]
async fn negative_control_publishes_a_durable_cut_before_the_device() {
    let directory = tempfile::tempdir().unwrap();
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default())
        .await
        .unwrap();
    qualify(&provider, 0, &[0], true).await.unwrap();
}
