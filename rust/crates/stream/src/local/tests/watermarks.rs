use super::*;
use proptest::prelude::*;
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn qualify(operations: &[u8], early_durable: bool) -> Result {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let directory = tempfile::tempdir()?;
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        let paths = ["a", "b", "c"].map(|path| StreamPath::new(path).unwrap());
        let mut accepted = [0; 3];
        let mut durable = [0; 3];
        let mut frames = 0;
        let mut flushed = 0;
        for &operation in operations {
            match operation % 4 {
                3 => {
                    provider.flush()?;
                    flushed = frames;
                }
                selected => {
                    let selected = usize::from(selected);
                    let (Some(path), Some(count)) =
                        (paths.get(selected), accepted.get_mut(selected))
                    else {
                        return Err("operation names no path".into());
                    };
                    deferring_durability(provider.append(AppendRequest {
                        path: path.clone(),
                        records: vec![Bytes::from_static(b"accepted")],
                        if_tail: None,
                        idempotency_key: None,
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
                provider.inner.synced.send_replace(frames);
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
    })
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // The independent reference tracks append and explicit-flush events, never
    // the production watch comparison or the watermark result as its oracle.
    #[test]
    fn accepted_and_durable_watch_cuts_match_the_journal_and_reference(
        operations in prop::collection::vec(any::<u8>(), 0..24),
    ) {
        qualify(&operations, false).unwrap();
    }
}
#[test]
#[should_panic(expected = "durable watermark must reflect only a completed device cut")]
fn negative_control_publishes_a_durable_cut_before_the_device() {
    qualify(&[0], true).unwrap();
}
