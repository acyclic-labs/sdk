use super::*;

#[cfg(feature = "test-support")]
#[tokio::test]
async fn a_poisoned_device_reports_the_correct_crash_simulation_lock()
-> Result<(), Box<dyn std::error::Error>> {
    #[derive(Default)]
    struct Cut(Mutex<Option<CutSimulationError>>);
    impl PersistenceCut for Cut {
        fn admit(&self) -> bool {
            true
        }
        fn acknowledge(&self) -> bool {
            true
        }
        fn crashed(&self, simulation: Result<(), CutSimulationError>) {
            *self.0.lock().unwrap() = simulation.err();
        }
    }
    let directory = tempfile::tempdir()?;
    let stream = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    let before = std::fs::read(directory.path().join(JOURNAL_FILE))?;
    let inner = stream.inner.clone();
    let failed = std::thread::spawn(move || {
        let _device = inner.device.lock().unwrap();
        panic!("poison the device gate");
    });
    assert!(failed.join().is_err());
    let cut = Cut::default();
    assert_eq!(stream.cut_crash(&cut).await, StreamError::Unavailable);
    assert!(matches!(
        cut.0.lock().unwrap().take(),
        Some(CutSimulationError::DeviceLock)
    ));
    assert_eq!(std::fs::read(directory.path().join(JOURNAL_FILE))?, before);
    Ok(())
}

// A real deferred group: the device can retain B while losing A before its
// shared flush completes. Recovery must retain the previously acknowledged seed.
#[tokio::test]
async fn reordered_unacknowledged_group_is_recoverable() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let limits = LocalStreamLimits::default();
    let stream = LocalStream::open(directory.path(), limits).await?;
    stream.append(keyed_append("seed", 0)?).await?;
    let start = usize::try_from(stream.inner.journal.lock().unwrap().bytes)?;
    deferring_durability(stream.append(keyed_append("a", 0)?)).await?;
    deferring_durability(stream.append(keyed_append("b", 0)?)).await?;
    drop(stream);
    let path = directory.path().join(JOURNAL_FILE);
    let mut image = std::fs::read(&path)?;
    image
        .get_mut(start..start + 4)
        .ok_or("fixture boundary")?
        .fill(0);
    std::fs::write(&path, image)?;
    let reopened = LocalStream::open(directory.path(), limits).await?;
    assert_eq!(reopened.tail(StreamPath::new("seed")?).await?, 1);
    assert_eq!(
        reopened.tail(StreamPath::new("a")?).await,
        Err(StreamError::NotFound)
    );
    assert_eq!(
        reopened.tail(StreamPath::new("b")?).await,
        Err(StreamError::NotFound)
    );
    Ok(())
}

#[tokio::test]
async fn an_empty_group_admits_a_command_with_room_for_exactly_one_seal()
-> Result<(), Box<dyn std::error::Error>> {
    for sealed_prefix in [false, true] {
        let directory = tempfile::tempdir()?;
        let prefix = Command::Append(keyed_append("first", 0)?);
        let command = Command::Append(keyed_append("other", 0)?);
        let prefix_bytes = PreparedFrame::encode(&prefix, 0, None)?.encoded.len();
        let command_bytes = PreparedFrame::encode(&command, 0, None)?.encoded.len();
        let limits = LocalStreamLimits {
            journal_bytes: (HEADER_BYTES
                + command_bytes
                + GROUP_SEAL_BYTES
                + if sealed_prefix {
                    prefix_bytes + GROUP_SEAL_BYTES
                } else {
                    0
                }) as u64,
            ..LocalStreamLimits::default()
        };
        let stream = LocalStream::open(directory.path(), limits).await?;
        if sealed_prefix {
            let frame = stream.prepare(&prefix)?;
            replay(&stream.inner.provider, prefix).await?;
            stream.persist(frame).await?;
            stream.flush()?;
        }
        let frame = stream.prepare(&command)?;
        replay(&stream.inner.provider, command).await?;
        stream.persist(frame).await?;
        stream.flush()?;
        assert_eq!(
            stream.inner.journal.lock().unwrap().bytes,
            limits.journal_bytes
        );
        drop(stream);
        let reopened = LocalStream::open(directory.path(), limits).await?;
        assert_eq!(reopened.tail(StreamPath::new("other")?).await?, 1);
    }
    Ok(())
}

#[tokio::test]
async fn capacity_reserved_before_acceptance_survives_a_racing_seal()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let first = Command::Append(keyed_append("first", 0)?);
    let second = Command::Append(keyed_append("other", 0)?);
    let first_bytes = PreparedFrame::encode(&first, 0, None)?.encoded.len();
    let second_bytes = PreparedFrame::encode(&second, 0, None)?.encoded.len();
    let limits = LocalStreamLimits {
        journal_bytes: (HEADER_BYTES + first_bytes + second_bytes + 2 * GROUP_SEAL_BYTES) as u64,
        ..LocalStreamLimits::default()
    };
    let stream = LocalStream::open(directory.path(), limits).await?;
    deferring_durability(stream.append(keyed_append("first", 0)?)).await?;
    let frame = stream.prepare(&second)?;
    stream.flush()?; // closes the old group after the new command's admission
    replay(&stream.inner.provider, second).await?;
    stream.persist(frame).await?;
    stream.flush()?;
    assert_eq!(
        stream.inner.journal.lock().unwrap().bytes,
        limits.journal_bytes
    );
    drop(stream);
    let reopened = LocalStream::open(directory.path(), limits).await?;
    assert_eq!(reopened.tail(StreamPath::new("first")?).await?, 1);
    assert_eq!(reopened.tail(StreamPath::new("other")?).await?, 1);
    Ok(())
}

#[tokio::test]
async fn deferred_native_seal_credit_refunds_on_replacement_and_flush()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let stream = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    let first = Arc::new(Meter::local());
    let second = Arc::new(Meter::local());
    drop(
        deferring_durability(stream.native_append(keyed_append("first", 0)?, first.original()))
            .await??,
    );
    assert_eq!(first.write_operations.load(Ordering::SeqCst), 1);
    assert_eq!(first.pending_write_operations.load(Ordering::SeqCst), 1);
    drop(
        deferring_durability(stream.native_append(keyed_append("other", 0)?, second.original()))
            .await??,
    );
    first.assert_refunded();
    assert_eq!(second.pending_write_operations.load(Ordering::SeqCst), 1);
    // A caller may cancel after its accepted deferred response. The original
    // ticket still performs physical IO; recording observes the late denial.
    second.cancelled.store(true, Ordering::SeqCst);
    stream.flush()?;
    assert_eq!(second.write_operations.load(Ordering::SeqCst), 2);
    second.assert_refunded();
    drop(stream);
    let reopened = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    assert_eq!(reopened.tail(StreamPath::new("other")?).await?, 1);
    Ok(())
}

#[tokio::test]
async fn multiple_near_limit_commands_share_one_bounded_group()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let stream = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    for path in ["a", "b"] {
        let request = AppendRequest {
            path: StreamPath::new(path)?,
            records: vec![Bytes::from(vec![7; crate::MAX_RECORD_BYTES]).into(); 16],
            if_tail: Some(0),
            idempotency_key: None,
        };
        deferring_durability(stream.append(request)).await?;
    }
    assert_eq!(
        stream.inner.journal.lock().unwrap().group_start,
        HEADER_BYTES as u64
    );
    stream.flush()?;
    let end = stream.inner.journal.lock().unwrap().group_start;
    assert!(end > 2 * (MAX_COMMAND_BYTES - 8192) as u64);
    drop(stream);
    let reopened = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    assert_eq!(reopened.tail(StreamPath::new("a")?).await?, 16);
    assert_eq!(reopened.tail(StreamPath::new("b")?).await?, 16);
    Ok(())
}

#[tokio::test]
async fn same_group_seal_does_not_turn_reordered_writes_into_corruption()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let limits = LocalStreamLimits::default();
    let stream = LocalStream::open(directory.path(), limits).await?;
    stream.append(keyed_append("seed", 0)?).await?;
    let start = usize::try_from(stream.inner.journal.lock().unwrap().bytes)?;
    deferring_durability(stream.append(keyed_append("a", 0)?)).await?;
    deferring_durability(stream.append(keyed_append("b", 0)?)).await?;
    // The seal itself can reach storage before its preceding frames.
    stream.inner.journal.lock().unwrap().seal()?;
    drop(stream);
    let path = directory.path().join(JOURNAL_FILE);
    let original = std::fs::read(&path)?;
    for fill in [0, 0x5a] {
        let mut image = original.clone();
        image
            .get_mut(start..start + 4)
            .ok_or("fixture boundary")?
            .fill(fill);
        std::fs::write(&path, &image)?;
        let reopened = LocalStream::open(directory.path(), limits).await?;
        assert_eq!(reopened.tail(StreamPath::new("seed")?).await?, 1);
        assert_eq!(
            reopened.tail(StreamPath::new("b")?).await,
            Err(StreamError::NotFound)
        );
        drop(reopened);
        assert_eq!(std::fs::metadata(&path)?.len(), start as u64);
    }
    Ok(())
}

#[tokio::test]
async fn every_group_suffix_cut_reseals_before_the_next_group()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let limits = LocalStreamLimits::default();
    let stream = LocalStream::open(directory.path(), limits).await?;
    stream.append(keyed_append("seed", 0)?).await?;
    let start = usize::try_from(stream.inner.journal.lock().unwrap().bytes)?;
    deferring_durability(stream.append(keyed_append("a", 0)?)).await?;
    deferring_durability(stream.append(keyed_append("b", 0)?)).await?;
    stream.inner.journal.lock().unwrap().seal()?;
    drop(stream);
    let path = directory.path().join(JOURNAL_FILE);
    let image = std::fs::read(&path)?;
    for cut in start..=image.len() {
        std::fs::write(&path, image.get(..cut).ok_or("fixture cut")?)?;
        let reopened = LocalStream::open(directory.path(), limits).await?;
        assert_eq!(
            reopened.tail(StreamPath::new("seed")?).await?,
            1,
            "cut {cut}"
        );
        {
            let journal = reopened.inner.journal.lock().unwrap();
            assert_eq!(
                journal.bytes, journal.group_start,
                "unsealed startup at cut {cut}"
            );
        }
        reopened.append(keyed_append("after", 0)?).await?;
        drop(reopened);
        let reopened = LocalStream::open(directory.path(), limits).await?;
        assert_eq!(reopened.tail(StreamPath::new("after")?).await?, 1);
    }
    Ok(())
}

#[tokio::test]
async fn unsupported_journal_version_is_rejected_without_mutation()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let limits = LocalStreamLimits::default();
    let path = directory.path().join(JOURNAL_FILE);
    let mut image = encode_header(limits, 0)?;
    image
        .first_chunk_mut::<24>()
        .ok_or("fixture header")?
        .copy_from_slice(b"ACYCLIC-STREAM-LOCAL-V1\0");
    std::fs::write(&path, &image)?;
    assert!(matches!(
        LocalStream::open(directory.path(), limits).await,
        Err(LocalStreamError::Corrupt)
    ));
    assert_eq!(std::fs::read(&path)?, image);
    assert!(!directory.path().join(SNAPSHOT_FILE).exists());
    Ok(())
}

#[test]
fn writers_overlap_the_device_and_share_the_next_flush_despite_cancellation()
-> Result<(), Box<dyn std::error::Error>> {
    let _serial = PERSIST_BLOCKER_TESTS
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let directory = tempfile::tempdir()?;
        let stream = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        let (started, waiting) = std::sync::mpsc::sync_channel(0);
        let (release, blocked) = std::sync::mpsc::sync_channel(0);
        *JOURNAL_PERSIST_BLOCKER.lock().unwrap() = Some((
            Arc::as_ptr(&stream.inner.journal) as usize,
            started,
            blocked,
        ));
        let first = tokio::spawn({
            let stream = stream.clone();
            async move { stream.append(keyed_append("first", 0)?).await }
        });
        tokio::task::spawn_blocking(move || waiting.recv()).await??;
        first.abort();
        let boundary = stream.inner.journal.lock().unwrap().group_start;
        let mut writers = Vec::new();
        for n in 0..16 {
            writers.push(tokio::spawn({
                let stream = stream.clone();
                async move {
                    stream
                        .append(keyed_append(&format!("writer/{n}"), 0)?)
                        .await
                }
            }));
        }
        // Always release the stalled IO, including when a regression prevents
        // new writers from reaching the journal, so the runtime cannot hang.
        let overlapped = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if stream.inner.journal.lock().unwrap().appended == 17 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        let explicit = tokio::task::spawn_blocking({
            let stream = stream.clone();
            move || stream.flush()
        });
        assert_eq!(
            stream.inner.journal.lock().unwrap().group_start,
            boundary,
            "next seal preceded the first flush"
        );
        release.send(())?;
        overlapped?;
        for writer in writers {
            writer.await??;
        }
        explicit.await??;
        {
            let journal = stream.inner.journal.lock().unwrap();
            assert_eq!(journal.appended, 17);
            assert_eq!(journal.synced, 17);
        }
        let mut file = File::open(directory.path().join(JOURNAL_FILE))?;
        let length = file.metadata()?.len();
        file.seek(SeekFrom::Start(HEADER_BYTES as u64))?;
        let mut offset = HEADER_BYTES as u64;
        let mut seals = 0;
        while offset < length {
            let frame = read_frame(&mut file, length - offset)?.ok_or("invalid completed frame")?;
            seals += usize::from(GroupSeal::decode(&frame.command).is_some());
            offset += frame_bytes(&frame.length);
        }
        assert_eq!(seals, 2, "17 commands should share two physical flushes");
        drop(stream);
        let reopened = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        assert_eq!(reopened.tail(StreamPath::new("first")?).await?, 1);
        for n in 0..16 {
            assert_eq!(
                reopened
                    .tail(StreamPath::new(format!("writer/{n}"))?)
                    .await?,
                1
            );
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    })
}

#[test]
fn compaction_and_explicit_flush_wait_for_the_inflight_group()
-> Result<(), Box<dyn std::error::Error>> {
    let _serial = PERSIST_BLOCKER_TESTS
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let directory = tempfile::tempdir()?;
        let limits = LocalStreamLimits {
            journal_operations: 4,
            ..LocalStreamLimits::default()
        };
        let stream = LocalStream::open(directory.path(), limits).await?;
        let (started, waiting) = std::sync::mpsc::sync_channel(0);
        let (release, blocked) = std::sync::mpsc::sync_channel(0);
        *JOURNAL_PERSIST_BLOCKER.lock().unwrap() = Some((
            Arc::as_ptr(&stream.inner.journal) as usize,
            started,
            blocked,
        ));
        let first = tokio::spawn({
            let stream = stream.clone();
            async move { stream.append(keyed_append("first", 0)?).await }
        });
        tokio::task::spawn_blocking(move || waiting.recv()).await??;
        let second = tokio::spawn({
            let stream = stream.clone();
            async move { stream.append(keyed_append("other", 0)?).await }
        });
        let accepted = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while stream.inner.journal.lock().unwrap().appended != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        let explicit = tokio::task::spawn_blocking({
            let stream = stream.clone();
            move || stream.flush()
        });
        assert_eq!(stream.inner.journal.lock().unwrap().epoch, 0);
        release.send(())?;
        accepted?;
        first.await??;
        second.await??;
        explicit.await??;
        assert_eq!(stream.inner.journal.lock().unwrap().epoch, 1);
        drop(stream);
        let reopened = LocalStream::open(directory.path(), limits).await?;
        assert_eq!(reopened.tail(StreamPath::new("first")?).await?, 1);
        assert_eq!(reopened.tail(StreamPath::new("other")?).await?, 1);
        Ok::<_, Box<dyn std::error::Error>>(())
    })
}

#[tokio::test]
async fn failed_seal_poisons_before_any_later_group_can_seal()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let stream = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    deferring_durability(stream.append(keyed_append("accepted", 0)?)).await?;
    let path = directory.path().join(JOURNAL_FILE);
    let before = std::fs::read(&path)?;
    {
        let mut journal = stream.inner.journal.lock().unwrap();
        journal.file = Arc::new(File::open(&path)?); // deterministic physical write failure
        (&*journal.file).seek(SeekFrom::End(0))?;
    }
    assert_eq!(stream.flush(), Err(StreamError::Unavailable));
    assert_eq!(stream.flush(), Err(StreamError::Unavailable));
    assert_eq!(std::fs::read(&path)?, before);
    drop(stream);
    let reopened = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    assert_eq!(reopened.tail(StreamPath::new("accepted")?).await?, 1);
    Ok(())
}
