//! Outstanding process waits hold no thread per waiting child.
//!
//! Every child here blocks reading a pipe whose only writer this test holds,
//! and exits once that writer closes, so the test decides exactly which
//! waits can finish.
use super::*;
use crate::tests::passed_in_exclusive_process;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

const OUTSTANDING: &str = "process_tree::scale_tests::outstanding_waits_hold_no_thread_per_child";
const CONTROL: &str = "process_tree::scale_tests::a_thread_per_wait_starves_the_bounded_pool";

/// The scale target, when the platform's default limits allow it.
#[cfg(unix)]
const TARGET: usize = 10_000;

/// Descriptors kept free for the runtime, the test harness and spawning.
#[cfg(unix)]
const DESCRIPTOR_MARGIN: u64 = 64;

/// One blocked child and the writer whose close releases it.
struct Blocked {
    tree: ProcessTree,
    release: std::io::PipeWriter,
}

fn blocked() -> io::Result<Blocked> {
    let (reader, release) = std::io::pipe()?;
    #[cfg(unix)]
    let mut command = Command::new("cat");
    #[cfg(windows)]
    let mut command = Command::new(
        std::path::Path::new(&std::env::var_os("SystemRoot").ok_or(io::ErrorKind::NotFound)?)
            .join("System32")
            .join("sort.exe"),
    );
    command
        .stdin(Stdio::from(reader))
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    Ok(Blocked {
        tree: ProcessTree::spawn(&mut command)?,
        release,
    })
}

/// How many children this platform's default limits allow at once: each
/// costs one release writer here, and a pidfd on Linux, without raising
/// any limit.
#[cfg(unix)]
fn children() -> io::Result<usize> {
    let limit = |resource| {
        // SAFETY: plain output fields; all-zero is a valid initial value.
        let mut limit: libc::rlimit = unsafe { std::mem::zeroed() };
        // SAFETY: a writable rlimit owned by this frame.
        if unsafe { libc::getrlimit(resource, &raw mut limit) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(limit.rlim_cur)
    };
    let per_child = if cfg!(target_os = "linux") { 2 } else { 1 };
    let open = crate::testing::process_resources()?.descriptors;
    let descriptors =
        limit(libc::RLIMIT_NOFILE)?.saturating_sub(open + DESCRIPTOR_MARGIN) / per_child;
    // The user's process limit counts every process the user runs.
    let processes = limit(libc::RLIMIT_NPROC)? / 2;
    let allowed = descriptors.min(processes);
    Ok(usize::try_from(allowed).map_or(TARGET, |allowed| allowed.min(TARGET)))
}

/// Windows children are full processes on a developer workstation; a few
/// hundred already exceed the blocking pool many times over.
#[cfg(windows)]
#[expect(
    clippy::unnecessary_wraps,
    reason = "every platform reports its limit the same way"
)]
fn children() -> io::Result<usize> {
    Ok(256)
}

fn threads() -> io::Result<u64> {
    Ok(crate::testing::process_resources()?.threads)
}

struct Quiet;

impl CaptureHandler for Quiet {
    fn check(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn observe(&mut self, _: ProcessOutput<'_>) -> io::Result<ProcessInput> {
        Ok(ProcessInput::default())
    }
}

fn runtime() -> io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread().build()
}

/// Drives `runtime` until `ready` holds or `limit` passes.
fn drive(runtime: &tokio::runtime::Runtime, limit: Duration, ready: impl Fn() -> bool) {
    let deadline = Instant::now() + limit;
    runtime.block_on(async {
        while !ready() && Instant::now() < deadline {
            tokio::task::yield_now().await;
            std::thread::park_timeout(Duration::from_millis(1));
        }
    });
}

#[test]
fn outstanding_waits_hold_no_thread_per_child() -> io::Result<()> {
    if passed_in_exclusive_process(OUTSTANDING)? {
        return Ok(());
    }
    crate::initialize_native_io_services()?;
    let pool = crate::workers(&crate::BLOCKING_WORKERS, false)?.exclusive_threads();
    let runtime = runtime()?;
    // One finished capture starts the pool and the readiness hub, so the
    // baseline already holds every thread a capture needs.
    let warm = blocked()?;
    drop(warm.release);
    let outcome = runtime.block_on(warm.tree.capture(
        CaptureSchedule::new(Duration::from_secs(30)),
        0,
        None,
        Quiet,
    ))?;
    assert!(outcome.result.is_ok_and(|output| output.status.success()));
    let baseline = threads()?;
    let count = children()?;
    assert!(
        count > 4 * pool,
        "{count} children cannot exceed the {pool}-thread pool"
    );
    let done = Arc::new(AtomicUsize::new(0));
    let mut releases = Vec::with_capacity(count);
    let mut tasks = Vec::with_capacity(count);
    for _ in 0..count {
        let Blocked { tree, release } = blocked()?;
        releases.push(release);
        let done = Arc::clone(&done);
        tasks.push(runtime.spawn(async move {
            let outcome = tree
                .capture(
                    CaptureSchedule::new(Duration::from_secs(60)),
                    0,
                    None,
                    Quiet,
                )
                .await;
            done.fetch_add(1, Ordering::AcqRel);
            outcome
        }));
    }
    // Every capture reaches its wait: one turn each on the pool, then none.
    drive(&runtime, Duration::from_secs(30), || {
        events::armed_waits() >= count
    });
    assert_eq!(
        events::armed_waits(),
        count,
        "every child has one wait armed"
    );
    let outstanding = threads()?;
    println!("{count} outstanding waits: {baseline} threads before, {outstanding} with all armed");
    assert!(
        outstanding <= baseline + 1,
        "{count} outstanding waits grew the process from {baseline} to {outstanding} threads"
    );
    // Only the last child is released, behind every other outstanding wait.
    let last = releases.pop().ok_or(io::ErrorKind::InvalidInput)?;
    drop(last);
    drive(&runtime, Duration::from_secs(30), || {
        done.load(Ordering::Acquire) >= 1
    });
    assert_eq!(
        done.load(Ordering::Acquire),
        1,
        "the released wait finished"
    );
    drop(releases);
    drive(&runtime, Duration::from_secs(60), || {
        done.load(Ordering::Acquire) >= count
    });
    for task in tasks {
        let outcome = runtime.block_on(task).map_err(io::Error::other)??;
        assert!(outcome.result.is_ok_and(|output| output.status.success()));
    }
    assert_eq!(events::armed_waits(), 0);
    Ok(())
}

/// Negative control: a wait that blocks its worker, as every process wait
/// did before the hub, needs one thread per outstanding child. With one
/// more child than the pool has workers, releasing the last child cannot
/// finish its wait, which queues behind waits that hold every worker.
#[test]
fn a_thread_per_wait_starves_the_bounded_pool() -> io::Result<()> {
    if passed_in_exclusive_process(CONTROL)? {
        return Ok(());
    }
    crate::initialize_native_io_services()?;
    let pool = crate::workers(&crate::BLOCKING_WORKERS, false)?.exclusive_threads();
    let runtime = runtime()?;
    let done = Arc::new(AtomicUsize::new(0));
    let mut releases = Vec::with_capacity(pool + 1);
    let mut tasks = Vec::with_capacity(pool + 1);
    for _ in 0..=pool {
        let Blocked { mut tree, release } = blocked()?;
        releases.push(release);
        let done = Arc::clone(&done);
        tasks.push(runtime.spawn(async move {
            let status =
                crate::run_blocking_io(move || tree.wait(Duration::from_secs(60)), None)?.await?;
            done.fetch_add(1, Ordering::AcqRel);
            status
        }));
    }
    let last = releases.pop().ok_or(io::ErrorKind::InvalidInput)?;
    drop(last);
    drive(&runtime, Duration::from_secs(2), || {
        done.load(Ordering::Acquire) >= 1
    });
    assert_eq!(
        done.load(Ordering::Acquire),
        0,
        "a released child's wait finished although {pool} blocked waits hold every worker"
    );
    drop(releases);
    drive(&runtime, Duration::from_secs(60), || {
        done.load(Ordering::Acquire) > pool
    });
    for task in tasks {
        assert!(runtime.block_on(task).map_err(io::Error::other)??.success());
    }
    Ok(())
}
