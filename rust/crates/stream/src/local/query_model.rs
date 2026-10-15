use super::*;
use crate::OwnedBytes as Bytes;
use crate::verification::{block_on, check};
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

// No filesystem operations are under test: the real query path only samples journal counters.
fn fixture() -> Result<LocalStream> {
    let file = File::open(std::env::current_exe()?)?;
    let journal = Journal {
        lock: file.try_clone()?,
        sync: Arc::new(file.try_clone()?),
        file,
        epoch: 0,
        operations: 0,
        bytes: 0,
        limits: LocalStreamLimits::default(),
        appended: 0,
        required: 0,
        synced: 0,
        syncing: false,
        root: PathBuf::new(),
        torn_tail: None,
        _ownership_anchor: None,
    };
    Ok(LocalStream {
        inner: Arc::new(LocalInner {
            provider: MemoryStream::default(),
            commit_clock: Arc::new(PinnedCommitClock {
                source: Arc::new(SystemUnixMillisClock),
                pinned: Mutex::new(None),
            }),
            journal: Arc::new(Mutex::new(journal)),
            visibility: RwLock::new(()),
            synced: watch::channel(0).0,
            changed: watch::channel(Some(0)).0,
            durable_tails: Mutex::new(Default::default()),
        }),
        work: None,
    })
}

pub(crate) fn accepted_queries_and_publication_have_no_lost_wakeup_or_durability_wait() {
    check(|| {
        let stream = fixture()?;
        let path = StreamPath::new("model")?;
        let seed = block_on(stream.inner.provider.append(AppendRequest {
            path: path.clone(),
            records: vec![Bytes::from_static(b"anchor")],
            if_tail: Some(0),
            idempotency_key: None,
        }))?;
        let AppendOutcome::Committed(seed) = seed else {
            return Err("seed conflict".into());
        };
        let writer = loom::thread::spawn({
            let stream = stream.clone();
            let path = path.clone();
            move || {
                block_on(async {
                    let _visibility = stream.inner.visibility.write().await;
                    stream
                        .inner
                        .provider
                        .append(AppendRequest {
                            path,
                            records: vec![Bytes::from_static(b"accepted")],
                            if_tail: Some(1),
                            idempotency_key: None,
                        })
                        .await?;
                    {
                        let mut journal =
                            stream.inner.journal.lock().map_err(|_| "journal lock")?;
                        journal.appended = 1;
                        journal.required = 1;
                    }
                    stream.inner.published();
                    Ok(()) as Result
                })
            }
        });
        block_on(async {
            let mut follow = stream
                .follow_checked(path.clone(), 1, seed.commit_id)
                .await?;
            let record = follow.next().await.ok_or("follow ended")??;
            assert_eq!(record.value.as_slice(), b"accepted");
            let watermarks = stream.watermarks(path).await?;
            assert_eq!(
                watermarks,
                crate::StreamWatermarks {
                    accepted: 2,
                    durable: 0
                }
            );
            Ok(()) as Result
        })?;
        writer.join().map_err(|_| "writer panicked")??;
        Ok(())
    });
}

pub(crate) fn poison_wakes_checked_followers() {
    check(|| {
        let stream = fixture()?;
        let path = StreamPath::new("model")?;
        let seed = block_on(stream.inner.provider.append(AppendRequest {
            path: path.clone(),
            records: vec![Bytes::from_static(b"anchor")],
            if_tail: Some(0),
            idempotency_key: None,
        }))?;
        let AppendOutcome::Committed(seed) = seed else {
            return Err("seed conflict".into());
        };
        let writer = loom::thread::spawn({
            let stream = stream.clone();
            move || {
                stream.inner.poison();
                stream.inner.published();
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
        Ok(())
    });
}
