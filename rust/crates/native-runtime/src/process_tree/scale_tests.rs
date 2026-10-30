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
    let outcome =
        runtime.block_on(
            warm.tree
                .capture(CaptureSchedule::Unbounded, 0, None, Quiet),
        )?;
    assert!(outcome.result.is_ok_and(|output| output.status.success()));
    let baseline = threads()?;
    let count = children()?;
    assert!(
        count > 4 * pool,
        "{count} children cannot exceed the {pool}-thread pool"
    );
    let done = Arc::new(AtomicUsize::new(0));
    let mut tasks = Vec::with_capacity(count);
    let mut releases = Vec::with_capacity(count);
    let mut armed = Vec::with_capacity(count);
    for _ in 0..count {
        let Blocked { mut tree, release } = blocked()?;
        releases.push(release);
        let (sender, receiver) = tokio::sync::oneshot::channel();
        tree.capture_armed = Some(sender);
        armed.push(receiver);
        let done = Arc::clone(&done);
        tasks.push(runtime.spawn(async move {
            let outcome = tree
                .capture(CaptureSchedule::Unbounded, 0, None, Quiet)
                .await;
            done.fetch_add(1, Ordering::AcqRel);
            outcome
        }));
    }
    // Each receipt belongs to its original capture's first complete native
    // registration. Failure or loss of that producer closes its own receipt.
    for receiver in armed {
        runtime
            .block_on(receiver)
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "capture lost before arming"))?;
    }
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
    let last_task = tasks.pop().ok_or(io::ErrorKind::InvalidInput)?;
    drop(last);
    let outcome = runtime.block_on(last_task).map_err(io::Error::other)??;
    assert!(outcome.result.is_ok_and(|output| output.status.success()));
    assert_eq!(
        done.load(Ordering::Acquire),
        1,
        "only the released wait finished"
    );
    drop(releases);
    for task in tasks {
        let outcome = runtime.block_on(task).map_err(io::Error::other)??;
        assert!(outcome.result.is_ok_and(|output| output.status.success()));
    }
    assert_eq!(done.load(Ordering::Acquire), count);
    assert_eq!(events::armed_waits(), 0);
    Ok(())
}

/// An unstarted capture owns the receipt producer as well as the actual tree.
/// Cancellation must close that receipt after cleaning up the same child.
#[test]
fn cancelled_capture_closes_its_unissued_arm_receipt() -> io::Result<()> {
    let runtime = runtime()?;
    let Blocked { mut tree, release } = blocked()?;
    let (sender, receiver) = tokio::sync::oneshot::channel();
    tree.capture_armed = Some(sender);
    let capture = tree.capture(CaptureSchedule::Unbounded, 0, None, Quiet);
    drop(capture);
    assert!(runtime.block_on(receiver).is_err());
    drop(release);
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
    // Release writers drop before completions if an assertion unwinds.
    let mut tasks = Vec::with_capacity(pool);
    let mut releases = Vec::with_capacity(pool);
    for _ in 0..pool {
        let Blocked { mut tree, release } = blocked()?;
        releases.push(release);
        let (entered, mut entry) = tokio::sync::oneshot::channel();
        let mut task = crate::run_blocking_io(
            move || {
                let _ = entered.send(());
                tree.wait_exit(None)
            },
            None,
        )?;
        runtime.block_on(std::future::poll_fn(|context| {
            use std::future::Future as _;
            use std::pin::Pin;
            use std::task::Poll;
            if let Poll::Ready(result) = Pin::new(&mut task).poll(context) {
                return Poll::Ready(Err(io::Error::other(format!(
                    "blocked wait ended before its release: {result:?}"
                ))));
            }
            Pin::new(&mut entry).poll(context).map(|result| {
                result.map_err(|_| {
                    io::Error::new(io::ErrorKind::BrokenPipe, "wait worker lost before entry")
                })
            })
        }))?;
        tasks.push(task);
    }
    // Each worker has entered its original wait. Only this last child exits;
    // its native exit event proves it is ready before the absence assertion.
    let Blocked { mut tree, release } = blocked()?;
    let exited = tree.readiness()?;
    let (entered, mut entry) = tokio::sync::oneshot::channel();
    let mut last = crate::run_blocking_io(
        move || {
            let _ = entered.send(());
            tree.wait_exit(None)
        },
        None,
    )?;
    runtime.block_on(std::future::poll_fn(|context| {
        use std::future::Future as _;
        use std::task::Poll;
        assert!(std::pin::Pin::new(&mut last).poll(context).is_pending());
        Poll::Ready(())
    }));
    drop(release);
    runtime.block_on(exited);
    assert!(
        matches!(
            entry.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ),
        "the queued wait entered while every worker was occupied"
    );
    // Removing the original pipe owners releases every worker and the queued
    // wait. The same completions must all succeed, without a second submission.
    drop(releases);
    for task in tasks {
        assert!(runtime.block_on(task)??.success());
    }
    assert!(runtime.block_on(last)??.success());
    runtime
        .block_on(entry)
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "queued wait lost before entry"))?;
    Ok(())
}
