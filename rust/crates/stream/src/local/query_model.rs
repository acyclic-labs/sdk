use super::*;
use crate::OwnedBytes as Bytes;
use crate::verification::{block_on, check, check_with_bound};
use futures::TryFutureExt as _;
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

// Use the same model executor as snapshot workers: production tasks own their
// future independently of the caller, including after its join is dropped.
pub(super) fn spawn<F: Future + Send + 'static>(
    future: F,
) -> impl Future<Output = std::result::Result<F::Output, StreamError>>
where
    F::Output: Send + 'static,
{
    let (completed, received) = tokio::sync::oneshot::channel();
    crate::snapshot::model::spawn(async move {
        let _ = completed.send(future.await);
    });
    async move { received.await.map_err(|_| StreamError::Unavailable) }
}

pub(super) async fn spawn_blocking<T>(
    work: impl FnOnce() -> T,
) -> std::result::Result<T, tokio::task::JoinError> {
    Ok(work())
}

pub(super) fn fixture(omit_follow_wake: bool) -> Result<LocalStream> {
    let limits = LocalStreamLimits::default();
    // The claim and the journal outlive their temporary directory: releasing
    // a removed claim file is a no-op, and the journal keeps its descriptor
    // while the cache holds no more than its capacity.
    let claims = tempfile::tempdir()?;
    let path = claims.path().join(JOURNAL_FILE);
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&path)?;
    let header = encode_header(limits, 0)?;
    file.write_all(&header)?;
    let journal = Journal {
        claim: acyclic_native_runtime::StoreClaim::acquire(&claims.path().join(LOCK_FILE))?,
        file: acyclic_native_runtime::CachedFile::new(file, &path, true, true)?,
        epoch: 0,
        operations: 0,
        bytes: header.len() as u64,
        limits,
        appended: 0,
        required: 0,
        synced: 0,
        flushed_sequence: Some(0),
        syncing: false,
        root: PathBuf::new(),
        torn_tail: None,
        // The model journal has no root and no incarnation record to forget.
        volatile: true,
        _ownership_anchor: None,
        #[cfg(feature = "test-support")]
        flushed: 0,
    };
    let clock: Arc<dyn UnixMillisClock> = Arc::new(SystemUnixMillisClock);
    let commit_clock = Arc::new(PinnedCommitClock {
        source: clock.clone(),
        pinned: Mutex::new(None),
    });
    Ok(LocalStream {
        handle: None,
        inner: Arc::new(LocalInner {
            replica_clock: Arc::new(crate::replicated::ReplicaClock::default()),
            durability: limits.durability,
            provider: MemoryStream::new_with_commit_clock(
                limits.memory,
                clock,
                commit_clock.clone(),
            ),
            commit_clock,
            journal: Arc::new(Mutex::new(journal)),
            visibility: RwLock::new(()),
            publication: RwLock::new(()),
            synced: watch::channel(SyncProgress {
                ordered: 0,
                flushed: Some(0),
            })
            .0,
            failed: watch::channel(false).0,
            revision: watch::channel(0).0,
            omit_follow_wake,
            retain_pending_read: false,
            durable_tails: Mutex::new(Default::default()),
            #[cfg(feature = "test-support")]
            persistence_cut: Default::default(),
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
            records: vec![Bytes::from_static(b"anchor").into()],
            if_tail: Some(0),
            idempotency_key: crate::IdempotencyKey::unique(),
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
                            records: vec![Bytes::from_static(b"accepted").into()],
                            if_tail: Some(1),
                            idempotency_key: crate::IdempotencyKey::unique(),
                        })
                        .await?;
                    Ok(()) as Result
                }))
            }
        });
        block_on(async {
            let mut follow = stream
                .follow(crate::FollowRequest {
                    when_absent: Default::default(),
                    path: path.clone(),
                    from: 1,
                    preceding: Some(seed.commit_id),
                    filter: crate::FollowFilter::default(),
                    progress: false,
                    at_least: Default::default(),
                })
                .map_ok(crate::records_only)
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
            records: vec![Bytes::from_static(b"anchor").into()],
            if_tail: Some(0),
            idempotency_key: crate::IdempotencyKey::unique(),
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
            match stream
                .follow(crate::FollowRequest {
                    when_absent: Default::default(),
                    path: path,
                    from: 1,
                    preceding: Some(seed.commit_id),
                    filter: crate::FollowFilter::default(),
                    progress: false,
                    at_least: Default::default(),
                })
                .map_ok(crate::records_only)
                .await
            {
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
            records: vec![Bytes::from_static(b"anchor").into()],
            if_tail: Some(0),
            idempotency_key: crate::IdempotencyKey::unique(),
        })))?;
        let AppendOutcome::Committed(seed) = seed else {
            return Err("seed conflict".into());
        };
        let mut follow = block_on(
            stream
                .follow(crate::FollowRequest {
                    when_absent: Default::default(),
                    path: path.clone(),
                    from: 1,
                    preceding: Some(seed.commit_id),
                    filter: crate::FollowFilter::default(),
                    progress: false,
                    at_least: Default::default(),
                })
                .map_ok(crate::records_only),
        )?;
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
                    records: vec![Bytes::from_static(b"accepted").into()],
                    if_tail: Some(1),
                    idempotency_key: crate::IdempotencyKey::unique(),
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
