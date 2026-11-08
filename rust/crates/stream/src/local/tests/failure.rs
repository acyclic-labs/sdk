use super::super::*;

// The OS refuses a real journal write through a held read-only descriptor.
// Keep its lease so the descriptor cache cannot reopen it with write access.
#[tokio::test]
async fn failed_physical_write_keeps_its_cause_and_ends_followers()
-> Result<(), Box<dyn std::error::Error>> {
    for native in [false, true] {
        let root = tempfile::tempdir()?;
        let stream = LocalStream::open(root.path(), LocalStreamLimits::default()).await?;
        let path = StreamPath::new("failure")?;
        stream.append(super::keyed_append("failure", 0)?).await?;
        let mut follow = stream
            .follow(crate::FollowRequest::new(path.clone(), 1))
            .await?;
        assert!(futures::poll!(follow.next()).is_pending());
        let file_path = root.path().join(JOURNAL_FILE);
        let read_only = acyclic_native_runtime::CachedFile::new(
            std::fs::File::open(&file_path)?,
            &file_path,
            true,
            false,
        )?;
        let held = read_only.lease()?;
        stream
            .inner
            .journal
            .lock()
            .map_err(|_| "journal poisoned")?
            .file = read_only;
        let meter = Arc::new(crate::test_meter::Meter::local());
        let work = if native {
            Some(crate::native::Work::new(meter.original())?)
        } else {
            None
        };
        let frame = PreparedFrame::encode(
            &WriteRequest::Append(super::keyed_append("failure", 1)?),
            SystemUnixMillisClock.now_unix_millis(),
            work.clone(),
        )?;
        let visibility = stream.inner.visibility.write().await;
        let publication = stream.inner.publication.write().await;
        assert_eq!(stream.persist(frame).await, Err(StreamError::Unavailable));
        let cause = stream.failure().ok_or("missing original failure")?;
        assert_eq!(cause.operation, "write");
        assert_eq!(cause.cause, "io");
        assert!(cause.io_kind.is_some());
        assert!(cause.raw_os_error.is_some());
        if let Some(work) = work {
            assert_eq!(work.failure("write"), Some(cause));
            assert!(
                work.finish::<(), StreamError>(Err(StreamError::Unavailable))
                    .is_err()
            );
        }
        stream.inner.poison();
        assert_eq!(
            stream.failure(),
            Some(cause),
            "a secondary failure replaced the OS cause"
        );
        drop(publication);
        drop(visibility);
        assert!(matches!(
            follow.next().await,
            Some(Err(StreamError::Unavailable))
        ));
        assert!(follow.next().await.is_none());
        drop(follow);
        drop(held);
        drop(stream);
        meter.assert_refunded();
        let reopened = LocalStream::open(root.path(), LocalStreamLimits::default()).await?;
        assert_eq!(
            reopened.tail(path).await?,
            1,
            "failed physical write fabricated a commit"
        );
    }
    Ok(())
}
