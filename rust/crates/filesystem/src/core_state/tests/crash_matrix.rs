//! Bounded physical WAL-cut supervisors leave native host I/O available.
use super::*;
use std::{panic::AssertUnwindSafe, sync::Arc};

// A process supervisor can wait indefinitely for a child or crash marker.
// Keep those waits off the shared native host-I/O service: provider opens and
// terminal cleanup must make progress while every crash supervisor is busy.
fn cut_workers() -> usize {
    std::thread::available_parallelism()
        .map_or(1, std::num::NonZero::get)
        .saturating_mul(2)
        .clamp(2, 16)
}

// Preserve the original service's aggregate process bound across independent
// crash matrices, while reserving the native host service for actual host I/O.
static ACTIVE_CUTS: std::sync::Mutex<usize> = std::sync::Mutex::new(0);
static CUT_FINISHED: std::sync::Condvar = std::sync::Condvar::new();

fn catch_cut(job: impl FnOnce() -> usize) -> std::thread::Result<usize> {
    let mut active = ACTIVE_CUTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    while *active >= cut_workers() {
        active = CUT_FINISHED
            .wait(active)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
    }
    *active += 1;
    drop(active);
    let outcome = std::panic::catch_unwind(AssertUnwindSafe(job));
    let mut active = ACTIVE_CUTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *active -= 1;
    drop(active);
    CUT_FINISHED.notify_one();
    outcome
}

fn finish_cuts<F: FnOnce() -> usize + Send + 'static>(jobs: Vec<F>) -> Vec<usize> {
    let workers = cut_workers().min(jobs.len());
    let jobs = std::sync::Mutex::new(jobs.into_iter());
    std::thread::scope(|scope| {
        let workers: Vec<_> = (0..workers)
            .map(|_| {
                scope.spawn(|| {
                    let _inline = acyclic_native_runtime::InlineBlocking::enter();
                    let mut outcomes = Vec::new();
                    loop {
                        let job = jobs.lock().unwrap().next();
                        let Some(job) = job else { break };
                        outcomes.push(catch_cut(job));
                    }
                    outcomes
                })
            })
            .collect();
        // Join every supervisor before inspecting any panic. The temporary
        // directory therefore outlives all child kills and recovery reads.
        let outcomes: Vec<_> = workers
            .into_iter()
            .flat_map(|worker| worker.join().unwrap())
            .collect();
        let mut finished: Vec<_> = outcomes
            .into_iter()
            .map(|outcome| match outcome {
                Ok(point) => point,
                Err(panic) => std::panic::resume_unwind(panic),
            })
            .collect();
        finished.sort_unstable();
        finished
    })
}

pub(in crate::core_state) fn assert_core_commits_are_crash_atomic(owned: bool) {
    let directory = tempfile::tempdir().unwrap();
    let reference_store = crash_store(&directory.path().join("reference"), owned);
    let mut snapshots = vec![core_snapshot(&reference_store)];
    for step in 0..CRASH_GROUPS[CRASH_GROUPS.len() - 1].end {
        crash_step(&reference_store, step);
        snapshots.push(core_snapshot(&reference_store));
    }
    drop(reference_store);

    // One complete script discovers the actual physical cut count. Every
    // subsequent child still starts cold and is killed at exactly one cut.
    let count = run_cut(directory.path(), &snapshots, owned, 0).unwrap();
    assert!(count > 16, "crash points were not exercised");
    assert!(count < 1_000, "crash script never completed");
    let snapshots = Arc::new(snapshots);
    let jobs = (1..=count)
        .map(|point| {
            let directory = directory.path().to_path_buf();
            let snapshots = snapshots.clone();
            move || {
                assert_eq!(run_cut(&directory, &snapshots, owned, point), None);
                point
            }
        })
        .collect();
    let points = finish_cuts(jobs);
    assert_eq!(points, (1..=count).collect::<Vec<_>>());
}

fn run_cut(
    directory: &Path,
    snapshots: &[CoreSnapshot],
    owned: bool,
    point: usize,
) -> Option<usize> {
    let executable = std::env::current_exe().unwrap();
    let run = directory.join(format!("run-{point}"));
    let count = run.join("count");
    let root = run.join("state");
    let acknowledged = run.join("acknowledged");
    let report = run.join("crash-point");
    std::fs::create_dir_all(&run).unwrap();
    let mut child = std::process::Command::new(&executable);
    child
        .args([
            "--exact",
            "core_state::tests::core_crash_child_runs_until_its_crash_point",
            "--nocapture",
        ])
        .env("ACYCLIC_CORE_STATE_CRASH_ROOT", &root)
        .env("ACYCLIC_CORE_STATE_CRASH_COUNT", &count)
        .env("ACYCLIC_CORE_STATE_CRASH_ACKNOWLEDGED", &acknowledged)
        .env("ACYCLIC_CORE_STATE_CRASH_POINT", point.to_string())
        .env("ACYCLIC_CORE_STATE_CRASH_REPORT", &report)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    if owned {
        child.env("ACYCLIC_CORE_STATE_CRASH_OWNED", "1");
    }
    let mut tree = acyclic_native_runtime::ProcessTree::spawn(&mut child).unwrap();
    let mut cut_reached = false;
    let outcome = tree.wait_with_output(
        std::time::Duration::from_secs(120),
        0,
        None,
        || {
            if report.try_exists()? {
                cut_reached = true;
                Err(std::io::ErrorKind::Interrupted.into())
            } else {
                Ok(())
            }
        },
        |_| Ok(acyclic_native_runtime::ProcessInput::default()),
        None,
        None,
    );
    // A published cut deliberately stops capture, whose supervisor then
    // hard-kills and reaps the child while its WAL guards are still held.
    // An unrelated failure must never be accepted as an exercised cut.
    let crashed = match outcome {
        Ok(output) => {
            assert!(
                output.status.success(),
                "crash child failed before publishing point {point}: {}",
                output.status
            );
            assert!(!cut_reached && !report.exists());
            false
        }
        Err(failure) => {
            assert!(
                cut_reached
                    && failure.error.kind() == std::io::ErrorKind::Interrupted
                    && failure.cleanup_completed,
                "crash point {point} was not killed and reaped: {failure:?}"
            );
            true
        }
    };
    if !crashed {
        assert_eq!(
            core_snapshot(&crash_store(&root, owned)),
            snapshots[snapshots.len() - 1]
        );
        return Some(std::fs::read_to_string(count).unwrap().parse().unwrap());
    }
    let report = std::fs::read_to_string(&report).unwrap();
    let (label, flushed) = report.split_once('\n').unwrap();
    let flushed = flushed.parse().unwrap();
    let done: usize =
        std::fs::read_to_string(&acknowledged).map_or(0, |count| count.parse().unwrap());
    let in_flight = CRASH_GROUPS
        .iter()
        .find(|group| group.start == done)
        .unwrap();
    let lost = run.join("power-loss");
    copy_tree(&root, &lost);
    lose_unflushed_writes(&lost, flushed);
    for (recovered, outcome) in [(&root, "kill"), (&lost, "power loss")] {
        let snapshot = core_snapshot(&crash_store(recovered, !owned));
        assert!(
            snapshots[done..=in_flight.end].contains(&snapshot),
            "{outcome} at point {point} ({label}) after {done} commits exposed {snapshot:?}"
        );
    }
    None
}

#[test]
fn cut_waits_leave_native_host_io_available() {
    use std::sync::mpsc;
    let (started, entered) = mpsc::channel();
    let mut releases = Vec::new();
    let jobs: Vec<_> = (0..cut_workers())
        .map(|point| {
            let (release, wait) = mpsc::channel();
            releases.push(release);
            let started = started.clone();
            move || {
                started.send(()).unwrap();
                wait.recv().unwrap();
                point
            }
        })
        .collect();
    let supervisor = std::thread::spawn(move || finish_cuts(jobs));
    for _ in 0..cut_workers() {
        entered.recv().unwrap();
    }
    // All supervisor slots are occupied by original blocking waits. An
    // unrelated real host-I/O request must still complete before release.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let progressed = runtime.block_on(async {
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            acyclic_native_runtime::run_blocking_io(|| 42, None).unwrap(),
        )
        .await
    });
    // Release and join even when the negative control starves the host pool.
    for release in releases {
        release.send(()).unwrap();
    }
    assert_eq!(
        supervisor.join().unwrap(),
        (0..cut_workers()).collect::<Vec<_>>()
    );
    assert_eq!(progressed.unwrap().unwrap(), 42);
}

#[test]
fn a_panicking_cut_still_drains_every_other_cut_before_returning() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let finished = Arc::new(AtomicUsize::new(0));
    let result = std::panic::catch_unwind(|| {
        let jobs: Vec<_> = (0..37)
            .map(|point| {
                let finished = finished.clone();
                move || {
                    if point == 0 {
                        panic!("deliberate cut failure");
                    }
                    finished.fetch_add(1, Ordering::SeqCst);
                    point
                }
            })
            .collect();
        finish_cuts(jobs)
    });
    assert!(result.is_err());
    assert_eq!(finished.load(Ordering::SeqCst), 36);
}
