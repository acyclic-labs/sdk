use super::*;
use crate::OwnedBytes as Bytes;
use crate::verification::{block_on, check, check_with_bound};
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

// Keep executor scheduling outside the model, but execute the entire production
// mutation and blocking journal write on its owning Loom actor. No state is mocked.
pub(super) async fn spawn<F: Future>(
    future: F,
) -> std::result::Result<F::Output, tokio::task::JoinError> {
    Ok(future.await)
}

pub(super) async fn spawn_blocking<T>(
    work: impl FnOnce() -> T,
) -> std::result::Result<T, tokio::task::JoinError> {
    Ok(work())
}

pub(super) fn fixture(omit_follow_wake: bool) -> Result<LocalStream> {
    let limits = LocalStreamLimits::default();
    let mut file = tempfile::tempfile()?;
    let header = encode_header(limits, 0)?;
    file.write_all(&header)?;
    let journal = Journal {
        lock: file.try_clone()?,
        sync: Arc::new(file.try_clone()?),
        file,
        epoch: 0,
        operations: 0,
        bytes: header.len() as u64,
        limits,
        appended: 0,
        required: 0,
        synced: 0,
        syncing: false,
        root: PathBuf::new(),
        torn_tail: None,
        _ownership_anchor: None,
    };
    let clock: Arc<dyn UnixMillisClock> = Arc::new(SystemUnixMillisClock);
    let commit_clock = Arc::new(PinnedCommitClock {
        source: clock.clone(),
        pinned: Mutex::new(None),
    });
    Ok(LocalStream {
        inner: Arc::new(LocalInner {
            provider: MemoryStream::new_with_commit_clock(
                limits.memory,
                clock,
                commit_clock.clone(),
            ),
            commit_clock,
            journal: Arc::new(Mutex::new(journal)),
            visibility: RwLock::new(()),
            publication: RwLock::new(()),
            synced: watch::channel(0).0,
            failed: watch::channel(false).0,
            revision: watch::channel(0).0,
            omit_follow_wake,
            durable_tails: Mutex::new(Default::default()),
            #[cfg(feature = "test-support")]
            persistence_cut: Default::default(),
            #[cfg(feature = "test-support")]
            durable_len: Default::default(),
        }),
        work: None,
    })
}

pub(crate) fn accepted_queries_and_publication_have_no_lost_wakeup_or_durability_wait(
    omit_wake: bool,
    preemptions: Option<usize>,
) {
    check_with_bound(preemptions, move || {
        let stream = fixture(omit_wake)?;
        let keeper = std::mem::ManuallyDrop::new(stream.clone());
        let path = StreamPath::new("model")?;
        let seed = block_on(deferring_durability(stream.append(AppendRequest {
            path: path.clone(),
            records: vec![Bytes::from_static(b"anchor")],
            if_tail: Some(0),
            idempotency_key: None,
        })))?;
        let AppendOutcome::Committed(seed) = seed else {
            return Err("seed conflict".into());
        };
        let writer = loom::thread::spawn({
            let stream = stream.clone();
            let path = path.clone();
            move || {
                block_on(deferring_durability(async {
                    stream
                        .append(AppendRequest {
                            path,
                            records: vec![Bytes::from_static(b"accepted")],
                            if_tail: Some(1),
                            idempotency_key: None,
                        })
                        .await?;
                    Ok(()) as Result
                }))
            }
        });
        block_on(async {
            let mut follow = stream
                .follow_checked(path.clone(), 1, seed.commit_id)
                .await?;
            let record = follow.next().await.ok_or("follow ended")??;
            assert_eq!(record.value.as_slice(), b"accepted");
            Ok(()) as Result
        })?;
        writer.join().map_err(|_| "writer panicked")??;
        // Acceptance has already been observed; checking its durable boundary
        // after join adds no competing publication or durability operation.
        let watermarks = block_on(stream.watermarks(path))?;
        assert_eq!(
            watermarks,
            crate::StreamWatermarks {
                accepted: 2,
                durable: 0
            }
        );
        drop(std::mem::ManuallyDrop::into_inner(keeper));
        Ok(())
    });
}

pub(crate) fn poison_wakes_checked_followers(omit_publication: bool) {
    check(move || {
        let stream = fixture(false)?;
        let keeper = std::mem::ManuallyDrop::new(stream.clone());
        let path = StreamPath::new("model")?;
        let seed = block_on(deferring_durability(stream.append(AppendRequest {
            path: path.clone(),
            records: vec![Bytes::from_static(b"anchor")],
            if_tail: Some(0),
            idempotency_key: None,
        })))?;
        let AppendOutcome::Committed(seed) = seed else {
            return Err("seed conflict".into());
        };
        let writer = loom::thread::spawn({
            let stream = stream.clone();
            move || {
                if omit_publication {
                    crate::verification::set_fault(
                        crate::verification::Fault::OmitPoisonPublication,
                    );
                }
                stream.inner.poison();
            }
        });
        block_on(async {
            match stream.follow_checked(path, 1, seed.commit_id).await {
                Ok(mut follow) => assert!(matches!(
                    follow.next().await,
                    Some(Err(StreamError::Unavailable))
                )),
                Err(error) => assert_eq!(error, StreamError::Unavailable),
            }
        });
        writer.join().map_err(|_| "writer panicked")?;
        drop(std::mem::ManuallyDrop::into_inner(keeper));
        Ok(())
    });
}

pub(crate) fn persist_wakes_parked_followers(omit_wake: bool) {
    struct Push(loom::sync::atomic::AtomicBool);
    impl std::task::Wake for Push {
        fn wake(self: Arc<Self>) {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }
    check(move || {
        let stream = fixture(omit_wake)?;
        let path = StreamPath::new("model")?;
        let seed = block_on(deferring_durability(stream.append(AppendRequest {
            path: path.clone(),
            records: vec![Bytes::from_static(b"anchor")],
            if_tail: Some(0),
            idempotency_key: None,
        })))?;
        let AppendOutcome::Committed(seed) = seed else {
            return Err("seed conflict".into());
        };
        let mut follow = block_on(stream.follow_checked(path.clone(), 1, seed.commit_id))?;
        let pushed = Arc::new(Push(loom::sync::atomic::AtomicBool::new(false)));
        let waker = std::task::Waker::from(pushed.clone());
        let mut context = std::task::Context::from_waker(&waker);
        assert!(follow.as_mut().poll_next(&mut context).is_pending());
        // The subscriber is genuinely parked before the production append starts.
        let writer = loom::thread::spawn({
            let stream = stream.clone();
            move || {
                block_on(deferring_durability(stream.append(AppendRequest {
                    path,
                    records: vec![Bytes::from_static(b"accepted")],
                    if_tail: Some(1),
                    idempotency_key: None,
                })))
            }
        });
        writer.join().map_err(|_| "writer panicked")??;
        assert!(
            pushed.0.load(std::sync::atomic::Ordering::SeqCst),
            "persist omitted publication wake"
        );
        let record = block_on(follow.next()).ok_or("follow ended")??;
        assert_eq!(record.value.as_slice(), b"accepted");
        let journal = stream.inner.journal.lock().map_err(|_| "journal lock")?;
        assert_eq!(journal.appended, 2);
        assert_eq!(journal.required, 0);
        assert_eq!(journal.synced, 0);
        assert!(journal.bytes > HEADER_BYTES as u64);
        Ok(())
    });
}
