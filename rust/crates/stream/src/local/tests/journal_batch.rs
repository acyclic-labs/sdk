use super::super::*;

fn frames(
    count: u64,
    work: Option<Arc<crate::native::Work>>,
) -> Result<Vec<PreparedFrame>, Box<dyn std::error::Error>> {
    let time = SystemUnixMillisClock.now_unix_millis();
    (0..count)
        .map(|sequence| {
            Ok(PreparedFrame::encode(
                &WriteRequest::Append(super::keyed_append("batch", sequence)?),
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

#[cfg(feature = "test-support")]
mod deadlines {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct Advance {
        clock: Arc<super::super::TestClock>,
        admitted: AtomicU64,
        at: u64,
    }
    impl PersistenceCut for Advance {
        fn admit(&self) -> bool {
            if self.admitted.fetch_add(1, Ordering::SeqCst) + 1 == self.at {
                self.clock.0.store(20, Ordering::SeqCst);
            }
            true
        }
        fn acknowledge(&self) -> bool {
            true
        }
        fn crashed(&self, _: Result<(), CutSimulationError>) {
            panic!("deadline admission is not a simulated power loss");
        }
    }

    #[tokio::test]
    async fn the_worker_checks_the_actual_deadline_before_accepting_a_proposal()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let clock = Arc::new(super::super::TestClock(AtomicU64::new(10)));
        let stream =
            LocalStream::open_with_clock(root.path(), LocalStreamLimits::default(), clock.clone())
                .await?;
        stream.cut_persistence(Arc::new(Advance {
            clock,
            admitted: AtomicU64::new(0),
            at: 1,
        }));
        let mut request = super::super::replay_work::request(b"deadline".to_vec());
        request.deadline_unix_millis = Some(20);
        assert!(matches!(
            stream.commit(request.clone()).await,
            Err(StreamError::DeadlineElapsed)
        ));
        assert_eq!(
            std::fs::metadata(root.path().join(JOURNAL_FILE))?.len(),
            HEADER_BYTES as u64
        );
        assert_eq!(
            stream
                .inspect_idempotency(request.idempotency_key.clone())
                .await?,
            None
        );
        request.deadline_unix_millis = Some(30);
        assert!(matches!(
            stream.commit(request).await?,
            CommitOutcome::Committed(_)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn an_expired_suffix_cannot_report_an_atomic_refusal_after_a_durable_prefix()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let clock = Arc::new(super::super::TestClock(AtomicU64::new(10)));
        let stream =
            LocalStream::open_with_clock(root.path(), LocalStreamLimits::default(), clock.clone())
                .await?;
        stream.cut_persistence(Arc::new(Advance {
            clock,
            admitted: AtomicU64::new(0),
            at: 2,
        }));
        let mut prepared = frames(3, None)?;
        for frame in &mut prepared {
            frame.deadline = Some(20);
        }
        assert!(matches!(
            stream.persist_frames(prepared.into_iter()).await,
            Err(StreamError::Unavailable)
        ));
        assert!(matches!(
            stream.check_available(),
            Err(StreamError::Unavailable)
        ));
        drop(stream);
        let reopened = LocalStream::open(root.path(), LocalStreamLimits::default()).await?;
        assert_eq!(reopened.tail(StreamPath::new("batch")?).await?, 1);
        let bytes = std::fs::metadata(root.path().join(JOURNAL_FILE))?.len();
        assert!(
            matches!(reopened.append(super::super::keyed_append("batch", 0)?).await?, AppendOutcome::Committed(receipt) if receipt.tail == 1)
        );
        assert_eq!(
            std::fs::metadata(root.path().join(JOURNAL_FILE))?.len(),
            bytes
        );
        Ok(())
    }
}
