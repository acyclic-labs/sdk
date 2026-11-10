use super::*;
use std::time::{Duration, Instant};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const DEADLINE: Duration = Duration::from_secs(10);
const CAP_CHILD: &str = "ACYCLIC_FSEVENTS_CAP_CHILD";
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

fn suspend_callbacks(hub: &Hub) -> Suspended {
    extern "C" fn suspend(queue: *mut c_void) {
        // SAFETY: the live serial queue is resumed by the returned guard.
        unsafe { dispatch_suspend(queue) };
    }
    // SAFETY: suspend runs after all earlier callbacks and stops subsequent
    // callbacks before returning, without leaving an in-flight delivery.
    unsafe { dispatch_sync_f(hub.queue(), hub.queue(), suspend) };
    Suspended(hub.queue())
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

fn wait_for_arm(hub: &Hub, registration: &Registration, before: u64) -> bool {
    let (mut state, _) = hub
        .armed
        .wait_timeout_while(hub.state(), DEADLINE, |state| {
            state
                .of(registration)
                .is_some_and(|shard| shard.arms <= before)
        })
        .unwrap_or_else(PoisonError::into_inner);
    state
        .of(registration)
        .is_some_and(|shard| shard.arms > before)
}

fn receive_roots(events: &std::sync::mpsc::Receiver<usize>, count: usize) -> TestResult {
    let deadline = Instant::now() + DEADLINE;
    let mut seen = std::collections::HashSet::new();
    while seen.len() != count {
        seen.insert(events.recv_timeout(deadline.saturating_duration_since(Instant::now()))?);
    }
    Ok(())
}

fn threads() -> i32 {
    // SAFETY: plain old data, filled by the kernel below.
    let mut info: libc::proc_taskinfo = unsafe { std::mem::zeroed() };
    let size = i32::try_from(size_of::<libc::proc_taskinfo>()).unwrap_or(i32::MAX);
    // SAFETY: the buffer is a live `proc_taskinfo` of the given size.
    let read = unsafe {
        libc::proc_pidinfo(
            std::process::id().cast_signed(),
            libc::PROC_PIDTASKINFO,
            0,
            (&raw mut info).cast(),
            size,
        )
    };
    if read == size { info.pti_threadnum } else { -1 }
}

/// Whether a rescan at `path` covers `root`.
fn rescans(path: &Path, flags: u32, root: &Path) -> bool {
    flags & (MUST_SCAN_SUBDIRS | USER_DROPPED | KERNEL_DROPPED) != 0 && root.starts_with(path)
}

#[test]
fn a_thousand_roots_share_the_bounded_streams() -> TestResult {
    const ROOTS: usize = 1000;
    let hub = hub()?;
    let before = threads();
    let (_parent, roots) = roots(ROOTS)?;
    let seen = Arc::new(
        (0..ROOTS)
            .map(|_| AtomicBool::new(false))
            .collect::<Vec<_>>(),
    );
    let started = Instant::now();
    let (changed, events) = std::sync::mpsc::channel();
    let streams = watch_all(&roots, |index| {
        let changed = changed.clone();
        let (seen, touched) = (Arc::clone(&seen), roots[index].join("touched"));
        Arc::new(move |events: Result<EventBatch<'_>, Arc<DemandError>>| {
            let events = events.expect("native stream delivery failed");
            if events
                .iter()
                .any(|(path, flags)| path == touched || rescans(path, flags, &touched))
            {
                seen[index].store(true, Ordering::Release);
            }
            let _ = changed.send(());
        })
    })?;
    let registered = started.elapsed();
    let streams_live = hub.streams();
    assert!(streams_live <= SHARDS, "{streams_live} streams");
    for root in &roots {
        std::fs::write(root.join("touched"), b"event")?;
    }
    let delivered = wait_until(&events, || {
        seen.iter().all(|seen| seen.load(Ordering::Acquire))
    });
    let missing = seen
        .iter()
        .filter(|seen| !seen.load(Ordering::Acquire))
        .count();
    println!(
        "fsevents hub: {ROOTS} roots in {registered:?} on {streams_live} streams; \
         {} process threads, {before} before",
        threads()
    );
    assert!(delivered, "{missing} of {ROOTS} roots saw no event");
    drop(streams);
    Ok(())
}

#[test]
fn a_burst_across_many_roots_loses_nothing() -> TestResult {
    const ROOTS: usize = 128;
    const FILES: usize = 32;
    struct Seen {
        names: Mutex<std::collections::HashSet<PathBuf>>,
        rescanned: AtomicBool,
    }
    hub()?;
    let (_parent, roots) = roots(ROOTS)?;
    let seen = Arc::new(
        (0..ROOTS)
            .map(|_| Seen {
                names: Mutex::new(std::collections::HashSet::new()),
                rescanned: AtomicBool::new(false),
            })
            .collect::<Vec<_>>(),
    );
    let (changed, events) = std::sync::mpsc::channel();
    let streams = watch_all(&roots, |index| {
        let changed = changed.clone();
        let (seen, root) = (Arc::clone(&seen), roots[index].clone());
        Arc::new(move |events: Result<EventBatch<'_>, Arc<DemandError>>| {
            let events = events.expect("native stream delivery failed");
            let seen = &seen[index];
            let mut names = seen.names.lock().unwrap_or_else(PoisonError::into_inner);
            for (path, flags) in events.iter() {
                if rescans(path, flags, &root) {
                    seen.rescanned.store(true, Ordering::Release);
                } else if path.parent() == Some(root.as_path()) {
                    names.insert(path.to_path_buf());
                }
            }
            let _ = changed.send(());
        })
    })?;
    for file in 0..FILES {
        for root in &roots {
            std::fs::write(root.join(format!("file-{file}")), b"burst")?;
        }
    }
    let covered = |seen: &Seen| {
        seen.rescanned.load(Ordering::Acquire)
            || seen
                .names
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .len()
                == FILES
    };
    let delivered = wait_until(&events, || seen.iter().all(covered));
    let rescanned = seen
        .iter()
        .filter(|seen| seen.rescanned.load(Ordering::Acquire))
        .count();
    let lost = seen.iter().filter(|seen| !covered(seen)).count();
    println!(
        "fsevents burst: {} events over {ROOTS} roots, {rescanned} flagged rescans",
        ROOTS * FILES
    );
    assert!(delivered, "{lost} of {ROOTS} roots lost events");
    drop(streams);
    Ok(())
}

#[test]
fn rearming_loses_and_repeats_nothing() -> TestResult {
    const FILES: usize = 256;
    const ROUNDS: usize = 32;
    const GROWTH: usize = 4 * SHARDS;
    let hub = hub()?;
    let (_parent, roots) = roots(1 + GROWTH)?;
    let watched = roots[0].clone();
    let delivered = Arc::new(Mutex::new(Vec::<(PathBuf, u32, u64)>::new()));
    let (changed, events) = std::sync::mpsc::channel();
    let stream = watch(&watched, {
        let delivered = Arc::clone(&delivered);
        Arc::new(move |events: Result<EventBatch<'_>, Arc<DemandError>>| {
            let events = events.expect("native stream delivery failed");
            let ids = events.ids();
            let mut delivered = delivered.lock().unwrap_or_else(PoisonError::into_inner);
            for ((path, flags), &id) in events.iter().zip(ids) {
                delivered.push((path.to_path_buf(), flags, id));
            }
            let _ = changed.send(());
        })
    })?;
    let registration = stream.registration.clone().ok_or("unregistered")?;
    let arms = hub.arms(&registration);
    // Writes are still in flight on the old stream when each swap stops
    // it, so the replacement's replay must cover exactly what it missed.
    let mut others = Vec::new();
    for (round, growth) in roots[1..].chunks(GROWTH / ROUNDS).enumerate() {
        for file in 0..FILES / ROUNDS {
            std::fs::write(watched.join(format!("file-{round}-{file}")), b"rearm")?;
        }
        let before = hub.arms(&registration);
        for root in growth {
            others.push(watch(root, Arc::new(|_| {}))?);
        }
        // A shard that is replaying re-arms once its replay is done.
        hub.rearm(&registration);
        if !wait_for_arm(hub, &registration, before) {
            return Err(format!("the watched shard stopped re-arming in round {round}").into());
        }
    }
    let rearmed = hub.arms(&registration) - arms;
    let names = |delivered: &[(PathBuf, u32, u64)]| {
        delivered
            .iter()
            .filter(|(path, ..)| path.parent() == Some(watched.as_path()))
            .map(|(path, ..)| path.clone())
            .collect::<std::collections::HashSet<_>>()
    };
    let complete = wait_until(&events, || {
        let delivered = delivered.lock().unwrap_or_else(PoisonError::into_inner);
        names(&delivered).len() == FILES
            || delivered
                .iter()
                .any(|(path, flags, _)| rescans(path, *flags, &watched))
    });
    drop(others);
    drop(stream);
    let delivered = delivered.lock().unwrap_or_else(PoisonError::into_inner);
    println!(
        "fsevents re-arm: {} events for {FILES} files across {rearmed} re-arms",
        delivered.len()
    );
    assert!(
        rearmed >= ROUNDS as u64,
        "only {rearmed} re-arms raced {ROUNDS} rounds of writes"
    );
    assert!(
        complete,
        "{} of {FILES} files seen",
        names(&delivered).len()
    );
    // A replay delivers history in path order, so ids are unique, not sorted.
    let mut ids = delivered.iter().map(|(.., id)| *id).collect::<Vec<_>>();
    ids.sort_unstable();
    for pair in ids.windows(2) {
        let twice = |(.., id): &&(PathBuf, u32, u64)| *id == pair[0];
        assert!(
            pair[0] < pair[1],
            "event {} delivered twice: {:?}",
            pair[0],
            delivered
                .iter()
                .enumerate()
                .filter(|(_, event)| twice(event))
                .collect::<Vec<_>>()
        );
    }
    Ok(())
}

#[test]
fn pending_registration_replays_changes_across_history_done() -> TestResult {
    if std::env::var_os(REPLAY_CHILD).is_none() {
        return isolated_replay(
            "fsevents::hub::tests::pending_registration_replays_changes_across_history_done",
        );
    }
    let hub = hub()?;
    let (_parent, roots) = roots(1)?;
    let streams = (0..SHARDS)
        .map(|_| watch(&roots[0], Arc::new(|_| {})))
        .collect::<Result<Vec<_>, _>>()?;
    let suspended = suspend_callbacks(hub);
    // Hold native callbacks and simulate an unfinished replay on every shard,
    // so the next registration must wait with its saved floor.
    for shard in &mut hub.state().shards {
        shard.replaying = true;
    }
    let (changed, events) = std::sync::mpsc::channel();
    let registration = Arc::new(Registration::new(
        vec![roots[0].clone()],
        Arc::new(move |batch| {
            let batch = batch.expect("native stream delivery failed");
            changed.send(batch.ids().to_vec()).expect("receiver alive");
        }),
    ));
    hub.register(&registration)?;
    let (old, floor, arms) = {
        let mut state = hub.state();
        let shard = state.of(&registration).ok_or("missing shard")?;
        let pending = shard.pending.first().ok_or("missing pending route")?;
        (shard.stream, pending.floor, shard.arms)
    };
    let path = std::ffi::CString::new(roots[0].as_os_str().as_bytes())?;
    hub.deliver(
        old,
        &[path.as_ptr(); 3],
        &[ITEM_CREATED, HISTORY_DONE, ITEM_CREATED],
        &[floor + 1, floor + 1, floor + 2],
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
    assert!(wait_for_arm(hub, &registration, arms));
    let suspended = suspend_callbacks(hub);
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
    let (changed, events) = std::sync::mpsc::channel();
    let stream = watch(
        &roots[0],
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
    )?;
    let suspended = suspend_callbacks(hub);
    let registration = stream.registration.as_ref().ok_or("unregistered")?;
    let floor = hub.current_event_id();
    let native = {
        let mut state = hub.state();
        let shard = state.of(registration).ok_or("missing shard")?;
        shard.replaying = true;
        for route in shard.routes.values_mut().flatten() {
            route.floor = floor;
        }
        shard.stream
    };
    // Discard real startup deliveries before injecting the replay batch.
    while events.try_recv().is_ok() {}
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
    drop(suspended);
    drop(stream);
    Ok(())
}

#[test]
fn shared_coverage_preserves_root_and_ancestor_moves() -> TestResult {
    let outer = tempfile::tempdir()?;
    let parent = outer.path().canonicalize()?.join("parent");
    std::fs::create_dir(&parent)?;
    let roots = (0..2 * SHARDS)
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

/// The negative control: a stream per watch, as before the hub, runs out.
#[test]
fn a_stream_per_watch_hits_the_process_cap() -> TestResult {
    if std::env::var_os(CAP_CHILD).is_some() {
        return count_raw_streams();
    }
    // A child process, so exhausting its streams starves no other test.
    let output = std::process::Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "fsevents::hub::tests::a_stream_per_watch_hits_the_process_cap",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CAP_CHILD, "1")
        .output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "{stdout}{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let cap = stdout
        .lines()
        .find_map(|line| line.split_once("raw fsevents streams: "))
        .map(|(_, cap)| cap)
        .ok_or_else(|| {
            format!(
                "the child reported no cap: {stdout}{}",
                String::from_utf8_lossy(&output.stderr)
            )
        })?;
    println!("raw fsevents streams: {cap}");
    let cap = cap
        .strip_prefix("failed at ")
        .ok_or("a stream per watch never failed")?
        .parse::<usize>()?;
    assert!((SHARDS..=1000).contains(&cap), "failed at {cap}");
    Ok(())
}

fn count_raw_streams() -> TestResult {
    extern "C" fn ignore(
        _: FsEventStreamRef,
        _: *mut c_void,
        _: usize,
        _: *mut c_void,
        _: *const u32,
        _: *const u64,
    ) {
    }
    const LIMIT: usize = 1024;
    let cancellation = crate::CancellationToken::new();
    let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let api = Api::get(&mut work)?;
    let (_parent, roots) = roots(LIMIT)?;
    let context = FsEventStreamContext {
        version: 0,
        info: std::ptr::null_mut(),
        retain: None,
        release: None,
        copy_description: None,
    };
    let mut running = Vec::new();
    let mut failed = None;
    for (index, root) in roots.iter().enumerate() {
        let (paths, bytes) = api.path_array(std::slice::from_ref(root), &mut work)?;
        // SAFETY: as in the hub, with a no-op callback and no info.
        let stream = unsafe {
            (api.stream_create)(
                std::ptr::null(),
                ignore,
                &raw const context,
                paths,
                EVENT_ID_SINCE_NOW,
                0.0,
                CREATE_FILE_EVENTS | CREATE_NO_DEFER,
            )
        };
        // SAFETY: the stream, if any, retains the array.
        unsafe { (api.release)(paths) };
        work.release(bytes)?;
        if stream.is_null() {
            failed = Some(index + 1);
            break;
        }
        // SAFETY: a serial queue per stream, as each watch made before the hub.
        let queue =
            unsafe { dispatch_queue_create(c"acyclic.fsevents".as_ptr(), std::ptr::null()) };
        // SAFETY: both handles are live.
        unsafe { (api.stream_set_dispatch_queue)(stream, queue) };
        // SAFETY: the stream has its queue.
        let started = unsafe { (api.stream_start)(stream) } != 0;
        running.push((stream, queue, started));
        if !started {
            failed = Some(index + 1);
            break;
        }
    }
    if failed.is_some() {
        // Keep the actual exhausted native streams alive while a shared
        // registration fails. Its error must be delivered outside both
        // locks, before startup releases the callback producer.
        let hub = hub()?;
        let (failure, received) = std::sync::mpsc::channel();
        // Hold the actual callback queue. Startup must reach its funded drain
        // before returning an error; otherwise its queued failure outlives the
        // registration's original ownership. Both outcomes publish an event,
        // so the original broken handoff fails without a readiness timeout.
        let suspended = Suspended::new(hub.queue());
        let (observed, observation) = std::sync::mpsc::channel();
        let root = roots[0].clone();
        let starter = std::thread::spawn(move || {
            let draining = observed.clone();
            BEFORE_DRAIN.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    let _ = draining.send(true);
                }));
            });
            let refused = watch(
                &root,
                Arc::new(move |events| {
                    if let Err(error) = events {
                        let unlocked = hub.arm.try_lock().is_ok() && hub.state.try_lock().is_ok();
                        let _ = failure.send((error, unlocked));
                    }
                }),
            )
            .is_err();
            BEFORE_DRAIN.with(|hook| drop(hook.borrow_mut().take()));
            let _ = observed.send(false);
            refused
        });
        let draining = observation.recv()?;
        drop(suspended);
        let refused = starter
            .join()
            .map_err(|_| "native startup worker panicked")?;
        assert!(refused, "exhausted native capacity admitted another stream");
        assert!(
            draining,
            "refused startup released its queued callback owner"
        );
        let (error, unlocked) = received.recv()?;
        assert!(
            unlocked,
            "failure callback retained a routing or arm lock: {error}"
        );
    }
    for (stream, queue, started) in running {
        // SAFETY: each stream is ours; the barrier drains its queue first.
        unsafe {
            if started {
                (api.stream_stop)(stream);
            }
            (api.stream_invalidate)(stream);
            dispatch_sync_f(queue, std::ptr::null_mut(), drain);
            (api.stream_release)(stream);
            dispatch_release(queue);
        }
    }
    match failed {
        Some(count) => println!("raw fsevents streams: failed at {count}"),
        None => println!("raw fsevents streams: all {LIMIT} ran"),
    }
    Ok(())
}
