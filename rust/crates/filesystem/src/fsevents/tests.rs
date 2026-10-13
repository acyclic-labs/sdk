use super::notify_adapter::translate;
use super::*;

#[test]
fn admitted_native_cleanup_keeps_the_original_meter_after_cancellation()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let mut start = DemandWork::new(
        WorkBudget::UNBOUNDED,
        &crate::cancellation::CancellationToken::new(),
    );
    let mut stream = EventStream::start(
        &[root.path().to_path_buf()],
        Arc::new(|_| {}),
        None,
        &mut start,
    )?;
    let cancellation = crate::cancellation::CancellationToken::new();
    let budget = WorkBudget {
        backend_write_operations: 4,
        backend_read_operations: 1,
        ..WorkBudget::default()
    };
    let mut stop = DemandWork::new(budget, &cancellation);
    // Exercise the mandatory continuation engine with a real native stream,
    // rather than public precancellation (which must leave its owner live).
    stop.work.admit(&stream.cleanup_work(), &stop.budget)?;
    cancellation.cancel();
    stream.cleanup_admitted(&mut stop)?;
    assert!(matches!(stop.check(), Err(DemandError::Cancelled)));
    assert!(stream.is_stopped());
    assert_eq!(stream.logical_owned_bytes(), 0);
    assert_eq!(stop.work.backend_write_operations, 4);
    assert_eq!(stop.work.backend_read_operations, 1);
    assert_eq!(stop.budget, budget);
    drop(stream);
    Ok(())
}

#[test]
fn measured_stop_consumes_the_actual_native_handles_before_drop()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let mut start = DemandWork::new(
        WorkBudget {
            backend_read_operations: 32,
            backend_write_operations: 32,
            items_examined: 1,
            bytes_copied: 32 * 1024,
            allocation_operations: 16,
            peak_allocation_bytes: 32 * 1024,
            ..WorkBudget::default()
        },
        &crate::cancellation::CancellationToken::new(),
    );
    let mut stream = EventStream::start(
        &[root.path().to_path_buf()],
        Arc::new(|_| {}),
        None,
        &mut start,
    )?;
    assert!(!stream.stream.is_null());
    assert!(!stream.queue.is_null());
    assert!(stream.logical_owned_bytes() > 0);
    assert_eq!(start.live_allocation_bytes(), 0);
    let mut stop = DemandWork::new(
        WorkBudget {
            backend_write_operations: 4,
            backend_read_operations: 1,
            ..WorkBudget::default()
        },
        &crate::cancellation::CancellationToken::new(),
    );
    stream.stop_with_work(&mut stop)?;
    assert!(stream.stream.is_null());
    assert!(stream.queue.is_null());
    assert!(stream.is_stopped());
    assert_eq!(stream.logical_owned_bytes(), 0);
    assert_eq!(
        stop.work,
        WorkCounters {
            backend_write_operations: 4,
            backend_read_operations: 1,
            ..WorkCounters::default()
        }
    );
    let before = stop.work;
    stream.stop_with_work(&mut stop)?;
    assert_eq!(stop.work, before);
    drop(stream);
    Ok(())
}

#[test]
fn translation_matches_the_notify_backend() {
    assert!(translate(HISTORY_DONE | ITEM_CREATED).is_empty());
    let rescan = translate(MUST_SCAN_SUBDIRS | KERNEL_DROPPED);
    assert_eq!(rescan.len(), 1);
    assert_eq!(rescan[0].kind, EventKind::Other);
    assert_eq!(rescan[0].flag(), Some(Flag::Rescan));
    assert_eq!(rescan[0].info(), Some("rescan: kernel dropped"));
    let kinds = |flags| {
        translate(flags)
            .into_iter()
            .map(|event| event.kind)
            .collect::<Vec<_>>()
    };
    assert_eq!(
        kinds(ITEM_CREATED | ITEM_MODIFIED | IS_FILE),
        [
            EventKind::Create(CreateKind::File),
            EventKind::Modify(ModifyKind::Data(DataChange::Content)),
        ]
    );
    assert_eq!(
        kinds(ITEM_REMOVED | ITEM_RENAMED | IS_DIR),
        [
            EventKind::Remove(RemoveKind::Folder),
            EventKind::Modify(ModifyKind::Name(RenameMode::Any)),
        ]
    );
    assert_eq!(
        translate(ITEM_CREATED | IS_SYMLINK)[0].info(),
        Some("is: symlink")
    );
    assert_eq!(kinds(ITEM_CREATED), [EventKind::Create(CreateKind::Any)]);
}

#[test]
fn watcher_delivers_file_events_without_linking_core_services() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let root = directory.path().canonicalize().expect("canonical root");
    let (sender, receiver) = std::sync::mpsc::channel();
    let mut watcher = FsEventsWatcher::new(sender, Config::default()).expect("watcher");
    watcher
        .watch(&root, RecursiveMode::Recursive)
        .expect("watch");
    let file = root.join("created");
    std::fs::write(&file, b"event").expect("write");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        let event = receiver
            .recv_timeout(remaining)
            .expect("an FSEvents event")
            .expect("event");
        if event.paths.contains(&file) {
            break;
        }
    }
    watcher.unwatch(&root).expect("unwatch");
    assert!(matches!(
        watcher.unwatch(&root),
        Err(error) if matches!(error.kind, notify::ErrorKind::WatchNotFound)
    ));
}
