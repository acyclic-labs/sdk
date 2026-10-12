use super::*;

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one admission/cancellation/release scenario keeps its owned worker cleanup explicit"
)]
async fn admitted_read_drop_retains_root_until_worker_finishes() {
    use crate::body::StoredBody;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    // Release the blocked worker during unwinding too.
    struct ReleaseOnDrop(mpsc::Sender<()>);
    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }

    {
        let directory = tempfile::tempdir().unwrap_or_else(|_| unreachable!());
        let bytes = acyclic_stream::OwnedBytes::from_static(b"pending private journal body");
        fs::write(directory.path().join(JOURNAL_FILE), &bytes).unwrap_or_else(|_| unreachable!());
        let (admitted_tx, admitted_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (completed_tx, completed_rx) = mpsc::channel();
        let release = ReleaseOnDrop(release_tx);
        let mut root = LocalRoot::new(directory.path().to_path_buf());
        root.read_gate = Some(JournalReadGate {
            admitted: admitted_tx,
            release: std::sync::Mutex::new(release_rx),
            completed: completed_tx,
        });
        let root = Arc::new(root);
        let weak = Arc::downgrade(&root);
        let body = StoredBody::Local {
            root: Arc::clone(&root),
            digest: *blake3::hash(&bytes).as_bytes(),
            length: bytes.len(),
            location: LocalBodyLocation::Journal { offset: 0 },
        };
        let mut read = Box::pin(body.read(0, bytes.len(), crate::body::BodyReadMode::Ordinary));
        let waker = futures::task::noop_waker();
        let mut context = std::task::Context::from_waker(&waker);
        assert!(read.as_mut().poll(&mut context).is_pending());
        admitted_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap_or_else(|_| unreachable!());
        drop(read);
        drop(body);
        drop(root);
        let retained_while_blocked = weak.upgrade().is_some();
        drop(release);
        let completed = completed_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap_or_else(|_| unreachable!())
            .unwrap_or_else(|_| unreachable!());
        assert_eq!(completed, bytes, "the detached read still finishes safely");
        assert_eq!(completed.retained_capacity(), Some(bytes.len()));
        assert!(retained_while_blocked, "the admitted worker owns its root");
        let deadline = Instant::now() + Duration::from_secs(5);
        while weak.strong_count() != 0 && Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert_eq!(
            weak.strong_count(),
            0,
            "the completed worker releases its root"
        );
    }
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one ordered scenario keeps each fault next to the outcome it checks"
)]
async fn physical_reads_validate_ranges_and_corruption() {
    {
        let directory = tempfile::tempdir().unwrap_or_else(|_| unreachable!());
        fs::create_dir_all(directory.path().join("segments")).unwrap_or_else(|_| unreachable!());
        let body = acyclic_stream::OwnedBytes::from_static(b"private body bytes");
        let digest = *blake3::hash(&body).as_bytes();
        let (id, offsets) = persist_segment(
            directory.path(),
            &[(digest, body.clone())],
            LocalDurability::FullFlush,
            &mut Publication::new(None),
        )
        .unwrap_or_else(|_| unreachable!());
        assert!(
            write_segment::<acyclic_stream::OwnedBytes>(
                directory.path(),
                &[],
                LocalDurability::FullFlush,
                &mut Publication::new(None)
            )
            .is_err()
        );
        let location = LocalBodyLocation::Segment {
            id,
            offset: *offsets.first().unwrap_or_else(|| unreachable!()),
        };
        let root = Arc::new(LocalRoot::new(directory.path().to_path_buf()));
        let read = |location: &LocalBodyLocation, start, end| {
            futures::executor::block_on(read_body_at(
                &root,
                &digest,
                body.len(),
                location,
                start,
                end,
                crate::body::BodyReadMode::Ordinary,
            ))
        };
        let selected = read(&location, 1, 5).unwrap_or_else(|_| unreachable!());
        assert_eq!(selected, body.slice(1..5));
        assert!(read(&location, 5, 1).is_err());
        let references = [LocalBodyReference {
            digest,
            length: body.len(),
            location: location.clone(),
        }]
        .into_iter()
        .collect();
        assert!(validate_referenced_segments(directory.path(), &references, 1024).is_ok());
        let segment = segment_path(directory.path(), &id);
        OpenOptions::new()
            .write(true)
            .open(&segment)
            .unwrap_or_else(|_| unreachable!())
            .set_len(SEGMENT_HEADER_BYTES as u64)
            .unwrap_or_else(|_| unreachable!());
        assert!(read(&location, 0, body.len()).is_err());
        assert!(validate_referenced_segments(directory.path(), &references, 1024).is_err());
        fs::write(directory.path().join(JOURNAL_FILE), &body).unwrap_or_else(|_| unreachable!());
        let journal = LocalBodyLocation::Journal { offset: 0 };
        assert!(read(&journal, 0, body.len()).is_ok());
        fs::write(directory.path().join(JOURNAL_FILE), vec![0; body.len()])
            .unwrap_or_else(|_| unreachable!());
        assert!(read(&journal, 0, body.len()).is_err());
    }
}
