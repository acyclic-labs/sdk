mod health;
use super::*;
pub(crate) use crate::test_fixtures::ReadBudgetMeter;
pub(crate) use acyclic_test_support::waker::complete;
use acyclic_test_support::waker::thread_waker as completion_waker;
use std::fs::OpenOptions;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Poll, Wake};
use std::time::Instant;

mod admission;
mod completion;
mod custody;
#[cfg(windows)]
mod device_control;
mod namespace;
mod native_allocation;
#[cfg(all(target_os = "linux", target_pointer_width = "64"))]
mod positional_interrupted;
mod prepared_order;
mod range;
mod reservations;
mod retained_future;
mod source;
mod spare;
use source::SourceFixture;
mod write_validation;

/// Polls a pending host task until a worker queue accepted its job, parking
/// until its admission waker fires between polls.
pub(crate) fn submit<T>(task: &mut NativeCompletion<T>) {
    let waker = completion_waker();
    while task.pending.is_some() {
        assert!(
            Pin::new(&mut *task)
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        if task.pending.is_some() {
            std::thread::park();
        }
    }
}

const EXCLUSIVE_CHILD: &str = "ACYCLIC_NATIVE_RUNTIME_EXCLUSIVE_TEST";
static EXCLUSIVE_PROCESS: AtomicBool = AtomicBool::new(false);

/// Whether this process runs a single test claimed by
/// [`passed_in_exclusive_process`], so that test owns the process-global pools.
pub(crate) fn in_exclusive_process() -> bool {
    EXCLUSIVE_PROCESS.load(Ordering::Acquire) || std::env::var_os(EXCLUSIVE_CHILD).is_some()
}

/// Reuses a process selecting exactly `test`, or runs it alone in a fresh
/// process and returns true once it passed there. Returns false in the process
/// that owns the test body.
///
/// The worker pools are process-global, so a test that asserts exact charges
/// or forces admission order must not share them with concurrent tests.
pub(crate) fn passed_in_exclusive_process(test: &str) -> io::Result<bool> {
    let test = test
        .strip_prefix(concat!(env!("CARGO_CRATE_NAME"), "::"))
        .unwrap_or(test);
    let started = format!("native exclusive test started: {test}\n");
    if std::env::var_os(EXCLUSIVE_CHILD).is_some_and(|name| name == test) {
        eprint!("{started}");
        return Ok(false);
    }
    let (mut exact, mut selected) = (false, false);
    let single = std::env::args_os().skip(1).all(|argument| {
        match argument.to_str() {
            Some("--exact") => exact = true,
            Some("--nocapture" | "--test-threads=1" | "--include-ignored" | "--ignored") => {}
            Some(name) if name == test && !selected => selected = true,
            _ => return false,
        }
        true
    });
    if single && exact && selected {
        EXCLUSIVE_PROCESS.store(true, Ordering::Release);
        eprintln!("using existing single-test process for {test}");
        return Ok(false);
    }
    let output = std::process::Command::new(std::env::current_exe()?)
        .args(["--exact", test, "--nocapture"])
        .env(EXCLUSIVE_CHILD, test)
        .output()?;
    assert!(
        output.status.success(),
        "{test} failed in its exclusive process:
{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    if !output
        .stderr
        .windows(started.len())
        .any(|bytes| bytes == started.as_bytes())
    {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "exclusive-test-start: {test} did not claim its process; recovery: cargo test --locked -p acyclic-native-runtime --all-features --lib -- --list"
            ),
        ));
    }
    Ok(true)
}

#[test]
fn an_unselected_exclusive_test_cannot_report_success() {
    let error = passed_in_exclusive_process("tests::missing_exclusive_test").unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::NotFound);
}

/// Holds every worker of a pool and fills its queue, so the next submission
/// waits for admission until this drops.
pub(crate) struct Saturation {
    gate: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
    _held: Vec<NativeCompletion<()>>,
}

impl Saturation {
    pub(crate) fn new(pool: &'static LazyLock<io::Result<NativeWorkers>>) -> io::Result<Self> {
        let threads = workers(pool, false)?.exclusive_threads();
        let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let mut held = Vec::new();
        loop {
            let started = started_tx.clone();
            let closed = Arc::clone(&gate);
            let mut job = task_completion(
                move || {
                    let _ = started.send(());
                    let (open, changed) = &*closed;
                    let mut open = open.lock().unwrap();
                    while !*open {
                        open = changed.wait(open).unwrap();
                    }
                    Ok(())
                },
                None,
                None,
                None,
                pool,
            )?;
            assert!(
                Pin::new(&mut job)
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            if job.waiter.is_some() {
                // The queue is full; dropping the waiting job unregisters it.
                return Ok(Self { gate, _held: held });
            }
            held.push(job);
            if held.len() == threads {
                // Every worker holds one gated job, so queued jobs stay queued.
                for _ in 0..threads {
                    started_rx.recv().map_err(io::Error::other)?;
                }
            }
        }
    }
}

impl Drop for Saturation {
    fn drop(&mut self) {
        let (open, changed) = &*self.gate;
        *open
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        changed.notify_all();
    }
}

fn bounded_write_file(path: &Path, writable: bool) -> io::Result<NativeFile> {
    initialize_native_io_services()?;
    let mut options = OpenOptions::new();
    options.read(true).write(writable);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt as _;
        options.custom_flags(windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OVERLAPPED);
        // SAFETY: the exact open above supplies the required flag.
        unsafe {
            NativeFile::from_overlapped_file_unchecked(options.open(path)?, None)
                .map_err(|(error, _)| error)
        }
    }
    #[cfg(not(windows))]
    NativeFile::from_file(options.open(path)?, None).map_err(|(error, _)| error)
}

struct ObservedWriteBytes {
    bytes: Vec<u8>,
    dropped: Arc<AtomicBool>,
}

impl AsRef<[u8]> for ObservedWriteBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for ObservedWriteBytes {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Release);
    }
}

struct ObservedWriteCustody {
    bytes_dropped: Arc<AtomicBool>,
    retired: Arc<AtomicBool>,
    meter: Arc<ReadBudgetMeter>,
    actual_prefix: u64,
}

impl Drop for ObservedWriteCustody {
    fn drop(&mut self) {
        assert!(self.bytes_dropped.load(Ordering::Acquire));
        assert_eq!(self.meter.read.load(Ordering::Acquire), self.actual_prefix);
        self.retired.store(true, Ordering::Release);
    }
}

fn prepared_metadata_task(
    file: File,
    meter: Arc<dyn NativeReadMeter>,
) -> io::Result<NativeCompletion<io::Result<u64>>> {
    let mut operation =
        NativeOperationReservation::new(Arc::clone(&meter), NativeOperationKind::Read, 1)?;
    run_blocking_io(
        move || {
            let (result, recorded) =
                operation.attempt(|| file.metadata().map(|metadata| metadata.len()));
            result.and_then(|length| recorded.map(|()| length))
        },
        Some((meter, true)),
    )
}
