use super::*;
use std::time::{Duration, Instant};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const DEADLINE: Duration = Duration::from_secs(10);
const CAP_CHILD: &str = "ACYCLIC_FSEVENTS_CAP_CHILD";

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

fn wait_until(mut done: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + DEADLINE;
    while !done() {
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    true
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
    let streams = watch_all(&roots, |index| {
        let (seen, touched) = (Arc::clone(&seen), roots[index].join("touched"));
        Arc::new(move |events: EventBatch<'_>| {
            if events
                .iter()
                .any(|(path, flags)| path == touched || rescans(path, flags, &touched))
            {
                seen[index].store(true, Ordering::Release);
            }
        })
    })?;
    let registered = started.elapsed();
    let streams_live = hub.streams();
    assert!(streams_live <= SHARDS, "{streams_live} streams");
    for root in &roots {
        std::fs::write(root.join("touched"), b"event")?;
    }
    let delivered = wait_until(|| seen.iter().all(|seen| seen.load(Ordering::Acquire)));
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
    let streams = watch_all(&roots, |index| {
        let (seen, root) = (Arc::clone(&seen), roots[index].clone());
        Arc::new(move |events: EventBatch<'_>| {
            let seen = &seen[index];
            let mut names = seen.names.lock().unwrap_or_else(PoisonError::into_inner);
            for (path, flags) in events.iter() {
                if rescans(path, flags, &root) {
                    seen.rescanned.store(true, Ordering::Release);
                } else if path.parent() == Some(root.as_path()) {
                    names.insert(path.to_path_buf());
                }
            }
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
    let delivered = wait_until(|| seen.iter().all(covered));
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
    let stream = watch(&watched, {
        let delivered = Arc::clone(&delivered);
        Arc::new(move |events: EventBatch<'_>| {
            let ids = events.ids();
            let mut delivered = delivered.lock().unwrap_or_else(PoisonError::into_inner);
            for ((path, flags), &id) in events.iter().zip(ids) {
                delivered.push((path.to_path_buf(), flags, id));
            }
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
        if !wait_until(|| hub.arms(&registration) > before) {
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
    let complete = wait_until(|| {
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
fn a_panicking_handler_does_not_stop_the_hub() -> TestResult {
    hub()?;
    let (_parent, roots) = roots(2)?;
    let panicking = watch(&roots[0], Arc::new(|_| panic!("handler panic")))?;
    let seen = Arc::new(AtomicUsize::new(0));
    let counting = watch(&roots[1], {
        let seen = Arc::clone(&seen);
        Arc::new(move |events: EventBatch<'_>| {
            seen.fetch_add(events.iter().len(), Ordering::AcqRel);
        })
    })?;
    for round in 0..2 {
        let before = seen.load(Ordering::Acquire);
        std::fs::write(roots[0].join(format!("panic-{round}")), b"panic")?;
        std::fs::write(roots[1].join(format!("count-{round}")), b"count")?;
        assert!(wait_until(|| seen.load(Ordering::Acquire) > before));
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
    assert!(output.status.success(), "{stdout}");
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
