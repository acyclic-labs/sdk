//! Independent physical WAL cuts share the existing native host service.
use super::*;
use futures::FutureExt as _;
use std::{future::Future, panic::AssertUnwindSafe, sync::Arc};

// Drain every accepted operation before propagating any panic. In particular,
// the parent temporary directory outlives all hard-kills and recovery checks.
async fn finish_jobs<F: Future>(jobs: Vec<F>) -> Vec<F::Output> {
    let outcomes = futures::future::join_all(
        jobs.into_iter()
            .map(|job| AssertUnwindSafe(job).catch_unwind()),
    )
    .await;
    outcomes
        .into_iter()
        .map(|result| match result {
            Ok(value) => value,
            Err(panic) => std::panic::resume_unwind(panic),
        })
        .collect()
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
            acyclic_native_runtime::run_blocking_io(
                move || {
                    // This is an existing host worker, outside Tokio. Its synchronous
                    // recovery reads may execute nested host work on this same worker.
                    let _blocking = acyclic_native_runtime::InlineBlocking::enter();
                    assert_eq!(run_cut(&directory, &snapshots, owned, point), None);
                    point
                },
                None,
            )
            .unwrap()
        })
        .collect();
    let finished = futures::executor::block_on(finish_jobs(jobs));
    let points: Vec<_> = finished.into_iter().map(|result| result.unwrap()).collect();
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
fn every_cut_is_admitted_before_waiting_for_the_first_completion() {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc,
    };
    acyclic_native_runtime::initialize_native_io_services().unwrap();
    let polled = Arc::new(AtomicUsize::new(0));
    let mut release = Vec::new();
    let jobs: Vec<_> = (0..2)
        .map(|value| {
            let (send, receive) = mpsc::channel();
            release.push(send);
            let polled = polled.clone();
            async move {
                polled.fetch_add(1, Ordering::SeqCst);
                acyclic_native_runtime::run_blocking_io(
                    move || {
                        receive.recv().unwrap();
                        value
                    },
                    None,
                )
                .unwrap()
                .await
                .unwrap()
            }
        })
        .collect();
    let pending = finish_jobs(jobs);
    futures::pin_mut!(pending);
    assert!(pending.as_mut().now_or_never().is_none());
    let before_completion = polled.load(Ordering::SeqCst);
    // Release actual workers before asserting, including in the serial control.
    for send in release {
        send.send(()).unwrap();
    }
    assert_eq!(futures::executor::block_on(pending), vec![0, 1]);
    assert_eq!(
        before_completion, 2,
        "cut admission waits for an earlier completion"
    );
}
