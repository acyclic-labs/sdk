use acyclic_fs::DemandError;
use acyclic_fs::fsevents_test_support::{
    DROPPED, EventBatch, EventHandler, RESCAN, ROOT_MOVED, RawStreams, SharedHub, Watch,
    thread_count,
};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError, mpsc};
use std::time::{Duration, Instant};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
type Delivery = Result<(), Arc<DemandError>>;

const DEADLINE: Duration = Duration::from_secs(10);
const CAP_CHILD: &str = "ACYCLIC_FSEVENTS_CAP_CHILD";

fn watch(root: &Path, handler: Arc<EventHandler>) -> TestResult<Watch> {
    Ok(SharedHub::watch(root, handler)?)
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
    handler: impl Fn(usize, &Path) -> TestResult<Arc<EventHandler>> + Sync,
) -> TestResult<Vec<Watch>> {
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
                        let handler = handler(index, root).map_err(|error| error.to_string())?;
                        let stream = watch(root, handler).map_err(|error| error.to_string())?;
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

fn wait_until(
    events: &mpsc::Receiver<Delivery>,
    mut done: impl FnMut() -> bool,
) -> TestResult<bool> {
    let deadline = Instant::now() + DEADLINE;
    while !done() {
        match events.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
            Ok(delivery) => delivery?,
            Err(_) => return Ok(false),
        }
    }
    Ok(true)
}

fn report_error<'a>(
    events: Result<EventBatch<'a>, Arc<DemandError>>,
    changed: &mpsc::Sender<Delivery>,
) -> Option<EventBatch<'a>> {
    match events {
        Ok(batch) => Some(batch),
        Err(error) => {
            let _ = changed.send(Err(error));
            None
        }
    }
}

/// Whether the event requires rescanning `root`.
fn rescans(path: &Path, flags: u32, root: &Path) -> bool {
    flags & DROPPED != 0 || flags & RESCAN != 0 && root.starts_with(path)
}

#[test]
fn a_thousand_roots_share_the_bounded_streams() -> TestResult {
    const ROOTS: usize = 1000;
    let hub = SharedHub::get()?;
    let before = thread_count();
    let (_parent, roots) = roots(ROOTS)?;
    let seen = Arc::new(
        (0..ROOTS)
            .map(|_| AtomicBool::new(false))
            .collect::<Vec<_>>(),
    );
    let remaining = Arc::new(AtomicUsize::new(ROOTS));
    let started = Instant::now();
    let (changed, events) = mpsc::channel();
    let streams = watch_all(&roots, |index, root| {
        let changed = changed.clone();
        let remaining = Arc::clone(&remaining);
        let (seen, touched) = (Arc::clone(&seen), root.join("touched"));
        Ok(Arc::new(move |events| {
            let Some(events) = report_error(events, &changed) else {
                return;
            };
            if events
                .iter()
                .any(|(path, flags)| path == touched || rescans(path, flags, &touched))
                && seen
                    .get(index)
                    .is_some_and(|seen| !seen.swap(true, Ordering::AcqRel))
                && remaining.fetch_sub(1, Ordering::AcqRel) == 1
            {
                let _ = changed.send(Ok(()));
            }
        }))
    })?;
    let registered = started.elapsed();
    let streams_live = hub.streams();
    assert!(
        streams_live <= SharedHub::STREAM_CAPACITY,
        "{streams_live} streams"
    );
    for root in &roots {
        std::fs::write(root.join("touched"), b"event")?;
    }
    let delivered = match events.recv_timeout(DEADLINE) {
        Ok(delivery) => {
            delivery?;
            true
        }
        Err(_) => false,
    };
    let missing = remaining.load(Ordering::Acquire);
    println!(
        "fsevents hub: {ROOTS} roots in {registered:?} on {streams_live} streams; \
         {} process threads, {before} before",
        thread_count()
    );
    assert!(
        delivered && missing == 0,
        "{missing} of {ROOTS} roots saw no event"
    );
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
    SharedHub::get()?;
    let (_parent, roots) = roots(ROOTS)?;
    let seen = Arc::new(
        (0..ROOTS)
            .map(|_| Seen {
                names: Mutex::new(std::collections::HashSet::new()),
                rescanned: AtomicBool::new(false),
            })
            .collect::<Vec<_>>(),
    );
    let (changed, events) = mpsc::channel();
    let streams = watch_all(&roots, |index, root| {
        let changed = changed.clone();
        let (seen, root) = (Arc::clone(&seen), root.to_path_buf());
        Ok(Arc::new(move |events| {
            let Some(events) = report_error(events, &changed) else {
                return;
            };
            let Some(seen) = seen.get(index) else { return };
            let mut names = seen.names.lock().unwrap_or_else(PoisonError::into_inner);
            for (path, flags) in events.iter() {
                if rescans(path, flags, &root) {
                    seen.rescanned.store(true, Ordering::Release);
                } else if path.parent() == Some(root.as_path()) {
                    names.insert(path.to_path_buf());
                }
            }
            let _ = changed.send(Ok(()));
        }))
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
    let delivered = wait_until(&events, || seen.iter().all(covered))?;
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
    const GROWTH: usize = 64;
    let hub = SharedHub::get()?;
    let (_parent, roots) = roots(1 + GROWTH)?;
    let (watched, growth) = roots.split_first().ok_or("missing initial watch root")?;
    let delivered = Arc::new(Mutex::new(Vec::<(PathBuf, u32, u64)>::new()));
    let (changed, events) = mpsc::channel();
    let stream = watch(watched, {
        let delivered = Arc::clone(&delivered);
        Arc::new(move |events: Result<EventBatch<'_>, Arc<DemandError>>| {
            let Some(events) = report_error(events, &changed) else {
                return;
            };
            let ids = events.ids();
            let mut delivered = delivered.lock().unwrap_or_else(PoisonError::into_inner);
            for ((path, flags), &id) in events.iter().zip(ids) {
                delivered.push((path.to_path_buf(), flags, id));
            }
            let _ = changed.send(Ok(()));
        })
    })?;
    let before_handoff = watched.join("before-handoff");
    let covers_watched = |(path, flags, _): &(PathBuf, u32, u64)| rescans(path, *flags, watched);
    std::fs::write(&before_handoff, b"already delivered")?;
    eprintln!("FSEvents re-arm waiting for the original stream");
    if !wait_until(&events, || {
        delivered
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .any(|event| event.0 == before_handoff || covers_watched(event))
    })? {
        return Err("the original FSEvents stream did not report the first write".into());
    }

    // Stop the original stream with a known delivered write and a burst that
    // its callbacks cannot have observed. Replay covers the burst and filters
    // the events already delivered by the original stream.
    let suspended = hub.pause_after_delivery();
    let replay_start = delivered
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .len();
    let mut written = std::collections::HashSet::with_capacity(FILES);
    for file in 0..FILES {
        let path = watched.join(format!("file-{file}"));
        std::fs::write(&path, b"during handoff")?;
        written.insert(path);
    }
    let others = growth
        .iter()
        .map(|root| watch(root, Arc::new(|_| {})))
        .collect::<Result<Vec<_>, _>>()?;
    let replacement = hub.rearm(&stream)?;
    drop(suspended);
    eprintln!("FSEvents re-arm waiting for the replacement stream");
    replacement.wait(DEADLINE)?;

    let files_seen = |delivered: &[(PathBuf, u32, u64)]| {
        delivered
            .iter()
            .map(|(path, ..)| path)
            .filter(|&path| written.contains(path))
            .collect::<std::collections::HashSet<_>>()
            .len()
    };
    eprintln!("FSEvents re-arm waiting for {FILES} writes to replay");
    let complete = wait_until(&events, || {
        let delivered = delivered.lock().unwrap_or_else(PoisonError::into_inner);
        files_seen(&delivered) == written.len()
            || delivered.iter().skip(replay_start).any(covers_watched)
    })?;
    drop(others);
    drop(stream);
    let delivered = delivered.lock().unwrap_or_else(PoisonError::into_inner);
    println!(
        "fsevents re-arm: {} events for {FILES} files",
        delivered.len()
    );
    assert!(complete, "{} of {FILES} files seen", files_seen(&delivered));
    // A replay delivers history in path order. Stream gaps and native root
    // changes with id zero can repeat control-message ids.
    let mut ids = delivered
        .iter()
        .filter(|(_, flags, id)| flags & DROPPED == 0 && (*id != 0 || flags & ROOT_MOVED == 0))
        .map(|(.., id)| *id)
        .collect::<Vec<_>>();
    ids.sort_unstable();
    for [prior, next] in ids.array_windows::<2>() {
        let twice = |(.., id): &&(PathBuf, u32, u64)| id == prior;
        assert!(
            prior < next,
            "event {} delivered twice: {:?}",
            prior,
            delivered
                .iter()
                .enumerate()
                .filter(|(_, event)| twice(event))
                .collect::<Vec<_>>()
        );
    }
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
    assert!((16..=1000).contains(&cap), "failed at {cap}");
    Ok(())
}

fn count_raw_streams() -> TestResult {
    const LIMIT: usize = 1024;
    let (_parent, roots) = roots(LIMIT)?;
    let raw_streams = RawStreams::start(&roots)?;
    let failed = raw_streams.refused_at();
    if failed.is_some() {
        // Keep the actual exhausted native streams alive while a shared
        // registration fails. Its error must be delivered outside both
        // locks, before startup releases the callback producer.
        let hub = SharedHub::get()?;
        let (failure, received) = std::sync::mpsc::channel();
        // Hold the actual callback queue. Startup must reach its funded drain
        // before returning an error; otherwise its queued failure outlives the
        // registration's original ownership. Both outcomes publish an event,
        // so the original broken handoff fails without a readiness timeout.
        let suspended = hub.pause_delivery();
        let (observed, observation) = std::sync::mpsc::channel();
        let root = roots.first().ok_or("missing first probe root")?.clone();
        let starter = std::thread::spawn(move || {
            let draining = observed.clone();
            SharedHub::before_next_drain(move || {
                let _ = draining.send(true);
            });
            let refused = watch(
                &root,
                Arc::new(move |events| {
                    if let Err(error) = events {
                        let unlocked = hub.locks_available();
                        let _ = failure.send((error, unlocked));
                    }
                }),
            )
            .is_err();
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
    drop(raw_streams);
    match failed {
        Some(count) => println!("raw fsevents streams: failed at {count}"),
        None => println!("raw fsevents streams: all {LIMIT} ran"),
    }
    Ok(())
}
