use super::super::*;

fn frames(
    count: u64,
    work: Option<Arc<crate::native::Work>>,
) -> Result<Vec<PreparedFrame>, Box<dyn std::error::Error>> {
    let time = SystemUnixMillisClock.now_unix_millis();
    (0..count)
        .map(|sequence| {
            Ok(PreparedFrame::encode(
                &Command::Append(super::keyed_append("batch", sequence)?),
                time,
                work.clone(),
            )?)
        })
        .collect()
}

#[tokio::test]
async fn owned_frame_batches_reopen_in_order_and_release_original_work()
-> Result<(), Box<dyn std::error::Error>> {
    for count in [1, 16, 256] {
        let root = tempfile::tempdir()?;
        let stream = LocalStream::open(root.path(), LocalStreamLimits::default()).await?;
        let meter = Arc::new(crate::test_meter::Meter::local());
        let work = crate::native::Work::new(meter.original())?;
        let frames = frames(count, Some(work.clone()))?;
        let bytes: u64 = frames.iter().map(|frame| frame.encoded.len() as u64).sum();
        // Exercise the physical boundary under its real publication fence,
        // then recover the canonical decisions from the resulting real file.
        let visibility = stream.inner.visibility.write().await;
        let publication = stream.inner.publication.write().await;
        stream.persist_frames(frames.into_iter()).await?;
        assert_eq!(stream.inner.required()?, count);
        stream.await_publication(count).await?;
        drop(publication);
        drop(visibility);
        drop(work);
        meter.assert_refunded();
        assert_eq!(
            meter.write_bytes.load(std::sync::atomic::Ordering::SeqCst),
            bytes
        );
        assert_eq!(
            std::fs::metadata(root.path().join(JOURNAL_FILE))?.len(),
            HEADER_BYTES as u64 + bytes
        );
        drop(stream);
        let reopened = LocalStream::open(root.path(), LocalStreamLimits::default()).await?;
        assert_eq!(reopened.tail(StreamPath::new("batch")?).await?, count);
        for sequence in 0..count {
            assert!(matches!(
                reopened.append(super::keyed_append("batch", sequence)?).await?,
                AppendOutcome::Committed(receipt) if receipt.tail == sequence + 1
            ));
        }
        assert_eq!(
            std::fs::metadata(root.path().join(JOURNAL_FILE))?.len(),
            HEADER_BYTES as u64 + bytes
        );
    }
    Ok(())
}

#[cfg(feature = "test-support")]
mod cuts {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    struct Cut {
        phase: u8,
        frame: AtomicU64,
        crashed: AtomicBool,
    }
    impl PersistenceCut for Cut {
        fn admit(&self) -> bool {
            let frame = self.frame.fetch_add(1, Ordering::SeqCst) + 1;
            self.phase != 0 || frame != 2
        }
        fn written(&self) -> bool {
            self.phase != 1 || self.frame.load(Ordering::SeqCst) != 2
        }
        fn acknowledge(&self) -> bool {
            self.phase != 2 || self.frame.load(Ordering::SeqCst) != 2
        }
        fn crashed(&self, simulated: Result<(), CutSimulationError>) {
            assert!(
                simulated.is_ok(),
                "power loss simulation failed: {simulated:?}"
            );
            self.crashed.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn a_batch_cut_recovers_only_its_flushed_prefix() -> Result<(), Box<dyn std::error::Error>>
    {
        for phase in 0..3 {
            let root = tempfile::tempdir()?;
            let stream = LocalStream::open(root.path(), LocalStreamLimits::default()).await?;
            let cut = Arc::new(Cut {
                phase,
                frame: AtomicU64::new(0),
                crashed: AtomicBool::new(false),
            });
            stream.cut_persistence(cut.clone());
            assert!(matches!(
                stream.persist_frames(frames(3, None)?.into_iter()).await,
                Err(StreamError::Unavailable)
            ));
            assert!(cut.crashed.load(Ordering::SeqCst));
            assert_eq!(cut.frame.load(Ordering::SeqCst), 2);
            drop(stream);
            let reopened = LocalStream::open(root.path(), LocalStreamLimits::default()).await?;
            assert_eq!(
                reopened.tail(StreamPath::new("batch")?).await?,
                if phase == 2 { 2 } else { 1 }
            );
        }
        Ok(())
    }
}
