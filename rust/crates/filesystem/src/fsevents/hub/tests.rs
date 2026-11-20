use super::*;
use std::time::{Duration, Instant};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const DEADLINE: Duration = Duration::from_secs(10);
const REPLAY_CHILD: &str = "ACYCLIC_FSEVENTS_REPLAY_CHILD";

/// Controlled replay state must not affect other tests sharing the process hub.
fn isolated_replay(name: &str) -> TestResult {
    let output = std::process::Command::new(std::env::current_exe()?)
        .args(["--exact", name, "--nocapture"])
        .env(REPLAY_CHILD, "1")
        .output()?;
    assert!(
        output.status.success(),
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

fn hub() -> TestResult<&'static Hub> {
    let cancellation = crate::CancellationToken::new();
    let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
    Ok(Hub::get(&mut work)?)
}

fn watch(root: &Path, handler: Arc<BorrowedEventHandlerFn>) -> TestResult<EventStream> {
    let cancellation = crate::CancellationToken::new();
    let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
    Ok(EventStream::start(
        &[root.to_path_buf()],
        handler,
        None,
        &mut work,
    )?)
}

/// Sibling roots in one canonical parent, so none is beneath another.
fn roots(count: usize) -> TestResult<(tempfile::TempDir, Vec<PathBuf>)> {
    let parent = tempfile::tempdir()?;
    let base = parent.path().canonicalize()?;
    let roots = (0..count)
        .map(|index| {
            let root = base.join(format!("root-{index}"));
            std::fs::create_dir(&root).map(|()| root)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((parent, roots))
}

/// Starts one watch per root from several threads, as concurrent agents do.
fn watch_all(
    roots: &[PathBuf],
    handler: impl Fn(usize) -> Arc<BorrowedEventHandlerFn> + Sync,
) -> TestResult<Vec<EventStream>> {
    let started = Mutex::new(Vec::with_capacity(roots.len()));
    std::thread::scope(|scope| {
        let workers = roots
            .chunks(roots.len().div_ceil(8))
            .enumerate()
            .map(|(chunk, part)| {
                let (started, handler) = (&started, &handler);
                scope.spawn(move || -> Result<(), String> {
                    for (offset, root) in part.iter().enumerate() {
                        let index = chunk * roots.len().div_ceil(8) + offset;
                        let stream = watch(root, handler(index)).map_err(|e| e.to_string())?;
                        started.lock().map_err(|_| "poisoned")?.push(stream);
                    }
                    Ok(())
                })
            })
            .collect::<Vec<_>>();
        workers
            .into_iter()
            .try_for_each(|worker| worker.join().map_err(|_| "watch worker panicked")?)
    })?;
    Ok(started.into_inner().map_err(|_| "poisoned")?)
}

fn wait_until(events: &std::sync::mpsc::Receiver<()>, mut done: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + DEADLINE;
    while !done() {
        if events
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .is_err()
        {
            return false;
        }
    }
    true
}

fn receive_roots(events: &std::sync::mpsc::Receiver<usize>, count: usize) -> TestResult {
    let deadline = Instant::now() + DEADLINE;
    let mut seen = std::collections::HashSet::new();
    while seen.len() != count {
        seen.insert(events.recv_timeout(deadline.saturating_duration_since(Instant::now()))?);
    }
    Ok(())
}

/// The native path of this process's registration marker.
fn marker_path(hub: &Hub, marker: u64) -> TestResult<std::ffi::CString> {
    Ok(std::ffi::CString::new(
        hub.marker_path(marker).as_os_str().as_bytes().to_vec(),
    )?)
}

/// A registration whose handler sends each batch's ids and paths.
fn recording(
    root: &Path,
) -> (
    Arc<Registration>,
    std::sync::mpsc::Receiver<Vec<(u64, PathBuf)>>,
) {
    let (changed, events) = std::sync::mpsc::channel();
    let registration = Arc::new(Registration::new(
        vec![root.to_path_buf()],
        Arc::new(move |batch| {
            let batch = batch.expect("native stream delivery failed");
            let received = batch
                .ids()
                .iter()
                .copied()
                .zip(batch.iter().map(|(path, _)| path.to_path_buf()))
                .collect();
            changed.send(received).expect("receiver alive");
        }),
    ));
    (registration, events)
}

/// Whether the event requires rescanning `root`.
fn rescans(path: &Path, flags: u32, root: &Path) -> bool {
    flags & (USER_DROPPED | KERNEL_DROPPED) != 0
        || flags & MUST_SCAN_SUBDIRS != 0 && root.starts_with(path)
}

#[test]
fn pending_registration_replays_changes_across_history_done() -> TestResult {
    if std::env::var_os(REPLAY_CHILD).is_none() {
        return isolated_replay(
            "fsevents::hub::tests::pending_registration_replays_changes_across_history_done",
        );
    }
    let hub = hub()?;
    let (_parent, roots) = roots(2)?;
    let streams = (0..SHARDS)
        .map(|_| watch(&roots[0], Arc::new(|_| {})))
        .collect::<Result<Vec<_>, _>>()?;
    let suspended = hub.suspend_after_delivery();
    // Narrow simulated coverage during replay so the second root must wait
    // for a replacement with its saved floor.
    for shard in &mut hub.state().shards {
        shard.root = Some(roots[0].clone());
        shard.replaying = true;
    }
    let (changed, events) = std::sync::mpsc::channel();
    let registration = Arc::new(Registration::new(
        vec![roots[1].clone()],
        Arc::new(move |batch| {
            let batch = batch.expect("native stream delivery failed");
            changed.send(batch.ids().to_vec()).expect("receiver alive");
        }),
    ));
    let marker = hub.install(&registration)?;
    let (old, floor, arms) = {
        let mut state = hub.state();
        let shard = state.of(&registration).ok_or("missing shard")?;
        let pending = shard.pending.first().ok_or("missing pending route")?;
        (shard.stream, pending.floor, shard.arms)
    };
    let path = std::ffi::CString::new(roots[1].as_os_str().as_bytes())?;
    let marked = marker_path(hub, marker)?;
    hub.deliver(
        old,
        &[marked.as_ptr(), path.as_ptr(), path.as_ptr(), path.as_ptr()],
        &[ITEM_CREATED, ITEM_CREATED, HISTORY_DONE, ITEM_CREATED],
        &[floor, floor + 1, floor + 1, floor + 2],
    );
    assert!(events.try_recv().is_err());
    let (saved, preserved) = std::sync::mpsc::channel();
    let pending = registration.clone();
    let check: Box<Box<dyn FnOnce() + Send>> = Box::new(Box::new(move || {
        let mut state = hub.state();
        let floor = state.of(&pending).and_then(|shard| {
            shard
                .routes
                .values()
                .flatten()
                .find(|route| Arc::ptr_eq(&route.registration, &pending))
                .map(|route| route.floor)
        });
        let _ = saved.send(floor);
    }));
    extern "C" fn check_floor(context: *mut c_void) {
        // SAFETY: the queue owns this boxed Send closure, consumed exactly once.
        let check = unsafe { Box::from_raw(context.cast::<Box<dyn FnOnce() + Send>>()) };
        check();
    }
    // SAFETY: this serial check follows retirement and replacement, and is
    // queued before the new stream exists, so its callbacks cannot advance the
    // route first. The queue takes ownership of the check until it runs.
    unsafe { dispatch_async_f(hub.queue(), Box::into_raw(check).cast(), check_floor) };
    drop(suspended);
    assert_eq!(preserved.recv_timeout(DEADLINE)?, Some(floor));
    hub.wait_for_arm(&registration, arms, DEADLINE)?;
    let suspended = hub.suspend_after_delivery();
    // Native retirement and replay run before the controlled replay below.
    // Give that injected batch its own original cut, independent of unrelated
    // host events that advanced the real stream during the queue handoff.
    while events.try_recv().is_ok() {}
    let stream = {
        let mut state = hub.state();
        let shard = state.of(&registration).ok_or("missing shard")?;
        assert_eq!(shard.arms, arms + 1, "pending routes need their own replay");
        shard.replaying = true;
        for route in shard.routes.values_mut().flatten() {
            route.floor = floor;
        }
        shard.stream
    };
    hub.deliver(
        stream,
        &[path.as_ptr(); 3],
        &[ITEM_CREATED, ITEM_CREATED, HISTORY_DONE],
        &[floor + 1, floor + 2, floor + 2],
    );
    assert_eq!(events.try_recv()?, vec![floor + 1, floor + 2]);
    hub.unregister(&registration);
    drop(suspended);
    drop(streams);
    Ok(())
}

#[test]
fn replay_filters_old_moves_before_synthesizing_root_changes() -> TestResult {
    if std::env::var_os(REPLAY_CHILD).is_none() {
        return isolated_replay(
            "fsevents::hub::tests::replay_filters_old_moves_before_synthesizing_root_changes",
        );
    }
    let hub = hub()?;
    let (_parent, roots) = roots(1)?;
    let existing = watch(&roots[0], Arc::new(|_| {}))?;
    let suspended = hub.suspend_after_delivery();
    let registered = existing.registration.as_ref().ok_or("unregistered")?;
    let native = hub.state().of(registered).ok_or("missing shard")?.stream;
    let (changed, events) = std::sync::mpsc::channel();
    let registration = Arc::new(Registration::new(
        vec![roots[0].clone()],
        Arc::new(move |batch| {
            let batch = batch.expect("native stream delivery failed");
            changed
                .send(
                    batch
                        .ids()
                        .iter()
                        .copied()
                        .zip(batch.flags.iter().copied())
                        .collect::<Vec<_>>(),
                )
                .expect("receiver alive");
        }),
    ));
    let marker = hub.install(&registration)?;
    let floor = {
        let mut state = hub.state();
        let shard = state.of(&registration).ok_or("missing shard")?;
        assert_eq!(
            shard.stream, native,
            "coverage stays on the original stream"
        );
        shard
            .routes
            .values()
            .flatten()
            .find(|route| Arc::ptr_eq(&route.registration, &registration))
            .ok_or("covered registration must join the covering stream")?
            .floor
    };
    let marked = marker_path(hub, marker)?;
    hub.deliver(native, &[marked.as_ptr()], &[ITEM_CREATED], &[floor]);
    hub.state()
        .of(&registration)
        .ok_or("missing shard")?
        .replaying = true;
    let root = std::ffi::CString::new(roots[0].as_os_str().as_bytes())?;
    let parent =
        std::ffi::CString::new(roots[0].parent().ok_or("no parent")?.as_os_str().as_bytes())?;
    hub.deliver(
        native,
        &[
            root.as_ptr(),
            parent.as_ptr(),
            root.as_ptr(),
            parent.as_ptr(),
            root.as_ptr(),
        ],
        &[
            IS_DIR | ITEM_RENAMED,
            IS_DIR | ITEM_REMOVED,
            IS_DIR | ITEM_REMOVED,
            IS_DIR | ITEM_RENAMED,
            ROOT_CHANGED,
        ],
        &[floor - 1, floor, floor + 1, floor + 2, 0],
    );
    let delivered = events.try_recv()?;
    assert_eq!(
        delivered.iter().map(|&(id, _)| id).collect::<Vec<_>>(),
        vec![floor + 1, floor + 2, 0]
    );
    assert!(delivered.iter().all(|&(_, flag)| flag & ROOT_CHANGED != 0));
    assert!(events.try_recv().is_err());
    let unrelated = c"/unrelated-volume".as_ptr();
    hub.deliver(
        native,
        &[unrelated, unrelated],
        &[
            MUST_SCAN_SUBDIRS | USER_DROPPED,
            MUST_SCAN_SUBDIRS | KERNEL_DROPPED,
        ],
        &[0, floor],
    );
    assert_eq!(
        events.try_recv()?,
        vec![
            (0, MUST_SCAN_SUBDIRS | USER_DROPPED),
            (floor, MUST_SCAN_SUBDIRS | KERNEL_DROPPED),
        ],
        "an active route must observe stream-wide loss even without a newer event id"
    );
    hub.unregister(&registration);
    drop(suspended);
    drop(existing);
    Ok(())
}

#[test]
fn shared_coverage_preserves_root_and_ancestor_moves() -> TestResult {
    const ROOTS: usize = 32;
    let outer = tempfile::tempdir()?;
    let parent = outer.path().canonicalize()?.join("parent");
    std::fs::create_dir(&parent)?;
    let roots = (0..ROOTS)
        .map(|index| {
            let root = parent.join(format!("root-{index}"));
            std::fs::create_dir(&root).map(|()| root)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let (changed, events) = std::sync::mpsc::channel();
    let (ready, received_ready) = std::sync::mpsc::channel();
    let streams = watch_all(&roots, |index| {
        let changed = changed.clone();
        let ready = ready.clone();
        let touched = roots[index].join("ready");
        Arc::new(move |events: Result<EventBatch<'_>, Arc<DemandError>>| {
            let events = events.expect("native stream delivery failed");
            if events.iter().any(|(_, flags)| flags & ROOT_CHANGED != 0) {
                let _ = changed.send(index);
            }
            if events
                .iter()
                .any(|(path, flags)| path == touched || rescans(path, flags, &touched))
            {
                let _ = ready.send(index);
            }
        })
    })?;
    for root in &roots {
        std::fs::write(root.join("ready"), b"ready")?;
    }
    receive_roots(&received_ready, roots.len())?;
    std::fs::rename(&roots[0], parent.join("moved-root"))?;
    assert_eq!(events.recv_timeout(DEADLINE)?, 0);
    std::fs::rename(&parent, outer.path().join("moved-parent"))?;
    receive_roots(&events, roots.len())?;
    drop(streams);
    Ok(())
}

#[test]
fn a_panicking_handler_does_not_stop_the_hub() -> TestResult {
    hub()?;
    let (_parent, roots) = roots(2)?;
    let panicking = watch(&roots[0], Arc::new(|_| panic!("handler panic")))?;
    let seen = Arc::new(AtomicUsize::new(0));
    let (changed, events) = std::sync::mpsc::channel();
    let counting = watch(&roots[1], {
        let seen = Arc::clone(&seen);
        Arc::new(move |events: Result<EventBatch<'_>, Arc<DemandError>>| {
            let events = events.expect("native stream delivery failed");
            seen.fetch_add(events.iter().len(), Ordering::AcqRel);
            let _ = changed.send(());
        })
    })?;
    for round in 0..2 {
        let before = seen.load(Ordering::Acquire);
        std::fs::write(roots[0].join(format!("panic-{round}")), b"panic")?;
        std::fs::write(roots[1].join(format!("count-{round}")), b"count")?;
        assert!(wait_until(&events, || seen.load(Ordering::Acquire) > before));
    }
    drop((panicking, counting));
    Ok(())
}

#[test]
fn writes_before_registration_stay_below_its_cut() -> TestResult {
    let (_parent, roots) = roots(1)?;
    let (changed, events) = std::sync::mpsc::channel();
    let before = roots[0].join("before");
    let after = roots[0].join("after");
    // A write that returns just before registration is the one `fseventsd`
    // can number after the current event id.
    std::fs::write(&before, b"before")?;
    let stream = watch(
        &roots[0],
        Arc::new(move |events: Result<EventBatch<'_>, Arc<DemandError>>| {
            let events = events.expect("native stream delivery failed");
            for (path, flags) in events.iter() {
                let _ = changed.send((path.to_path_buf(), flags));
            }
        }),
    )?;
    std::fs::write(&after, b"after")?;
    // Live delivery is in id order, so `before` would precede `after`.
    let deadline = Instant::now() + DEADLINE;
    loop {
        let (path, flags) =
            events.recv_timeout(deadline.saturating_duration_since(Instant::now()))?;
        assert!(
            path == after || rescans(&path, flags, &roots[0]),
            "a write that returned before registration reached the watch: {path:?}"
        );
        if path == after {
            break;
        }
    }
    drop(stream);
    Ok(())
}

#[test]
fn the_marker_is_the_live_cut_and_a_gap_ends_its_wait() -> TestResult {
    if std::env::var_os(REPLAY_CHILD).is_none() {
        return isolated_replay(
            "fsevents::hub::tests::the_marker_is_the_live_cut_and_a_gap_ends_its_wait",
        );
    }
    let hub = hub()?;
    let (_parent, roots) = roots(2)?;
    let existing = watch(&roots[0], Arc::new(|_| {}))?;
    let suspended = hub.suspend_after_delivery();
    let native = hub
        .state()
        .of(existing.registration.as_ref().ok_or("unregistered")?)
        .ok_or("missing shard")?
        .stream;
    let (registration, events) = recording(&roots[1]);
    let marker = hub.install(&registration)?;
    let path = std::ffi::CString::new(roots[1].as_os_str().as_bytes())?;
    let marked = marker_path(hub, marker)?;
    let cut = hub.current_event_id() + 10;
    // Earlier writes can be numbered above the provisional floor, but never
    // above the marker: the route waits for it, and its id is the floor.
    hub.deliver(
        native,
        &[path.as_ptr(), marked.as_ptr(), path.as_ptr(), path.as_ptr()],
        &[ITEM_CREATED, ITEM_CREATED, ITEM_REMOVED, ITEM_CREATED],
        &[cut - 1, cut, cut, cut + 1],
    );
    assert_eq!(events.try_recv()?, vec![(cut + 1, roots[1].clone())]);
    assert!(events.try_recv().is_err());
    hub.unregister(&registration);
    // A stream gap carries no cut, so a waiting route rescans instead.
    let (gapped, gaps) = recording(&roots[1]);
    hub.install(&gapped)?;
    hub.deliver(
        native,
        &[path.as_ptr(), path.as_ptr()],
        &[ITEM_CREATED, MUST_SCAN_SUBDIRS | KERNEL_DROPPED],
        &[cut + 2, cut + 3],
    );
    assert_eq!(gaps.try_recv()?, vec![(cut + 3, roots[1].clone())]);
    hub.unregister(&gapped);
    drop(suspended);
    drop(existing);
    Ok(())
}

#[test]
fn a_marker_seen_during_replay_replays_its_route_from_the_marker() -> TestResult {
    if std::env::var_os(REPLAY_CHILD).is_none() {
        return isolated_replay(
            "fsevents::hub::tests::a_marker_seen_during_replay_replays_its_route_from_the_marker",
        );
    }
    let hub = hub()?;
    let (_parent, roots) = roots(2)?;
    let existing = watch(&roots[0], Arc::new(|_| {}))?;
    let suspended = hub.suspend_after_delivery();
    let native = {
        let mut state = hub.state();
        let shard = state
            .of(existing.registration.as_ref().ok_or("unregistered")?)
            .ok_or("missing shard")?;
        shard.replaying = true;
        shard.stream
    };
    let (registration, events) = recording(&roots[1]);
    let marker = hub.install(&registration)?;
    let path = std::ffi::CString::new(roots[1].as_os_str().as_bytes())?;
    let marked = marker_path(hub, marker)?;
    let cut = hub.current_event_id() + 10;
    // History is in path order: this later event passes before the marker.
    hub.deliver(
        native,
        &[path.as_ptr(), marked.as_ptr()],
        &[ITEM_CREATED, ITEM_CREATED],
        &[cut + 1, cut],
    );
    assert!(events.try_recv().is_err());
    {
        let mut state = hub.state();
        let shard = state.of(&registration).ok_or("missing shard")?;
        assert!(
            shard
                .routes
                .values()
                .flatten()
                .all(|route| !Arc::ptr_eq(&route.registration, &registration)),
            "a route that skipped history above its cut must not stay live"
        );
        let pending = shard
            .pending
            .iter()
            .find(|route| Arc::ptr_eq(&route.registration, &registration))
            .ok_or("the route must replay from its cut")?;
        assert_eq!((pending.floor, pending.marker), (cut, None));
    }
    hub.unregister(&registration);
    drop(suspended);
    drop(existing);
    Ok(())
}
