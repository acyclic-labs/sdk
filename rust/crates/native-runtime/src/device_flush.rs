//! One device drain shared by every concurrent durability request on a device.
//!
//! A full flush has two parts: handing a file's writes to the device, and
//! draining the device's volatile cache. Only Apple separates them (`fsync`,
//! then `F_FULLFSYNC`), and there every drain serializes at the device, so
//! concurrent commits each paid a whole drain. Here each request first stages
//! its own file, then waits for the first drain that *starts* after it staged;
//! that drain covers every write staged before it, whoever issues it.
//! Requests arriving while a drain runs share the next one, so coalescing
//! comes only from overlap: there is no timer or batching window, and a lone
//! request issues exactly one drain. Processes share drains the same way,
//! through two counters in a per-device file each maps, with the file's lock
//! held across a drain. The file lives in the user's system temporary
//! directory, never `TMPDIR`: processes that each get their own `TMPDIR`
//! (every GraphCoder case, every sandboxed tool) would otherwise each drain
//! alone and saturate the device together. Per request the cost is O(1): two
//! locks, at most one drain, one wake.
//!
//! Linux and Windows have no such split: `fdatasync` and `FlushFileBuffers`
//! must run per file to make that file's own metadata durable, and the Linux
//! block layer already merges concurrent cache flushes into one. They keep
//! their native full flush.
#[cfg(loom)]
use loom::sync::{Condvar, Mutex};
use std::io;
use std::sync::PoisonError;
#[cfg(not(loom))]
use std::sync::{Condvar, Mutex};

#[derive(Default)]
struct Progress {
    /// Drains started so far; the running one, if any, is the last.
    started: u64,
    /// The latest drain that finished successfully.
    completed: u64,
    /// Whether a drain is running.
    draining: bool,
}

/// Coalesces drains of one device among the threads of this process.
pub(crate) struct Coalescer {
    progress: Mutex<Progress>,
    drained: Condvar,
    /// Negative control: join the running drain, which may have started
    /// before the caller staged.
    #[cfg(test)]
    join_running: bool,
    #[cfg(all(test, not(loom)))]
    arrivals: tests::Arrivals,
}

impl Default for Coalescer {
    fn default() -> Self {
        Self {
            progress: Mutex::new(Progress::default()),
            drained: Condvar::new(),
            #[cfg(test)]
            join_running: false,
            #[cfg(all(test, not(loom)))]
            arrivals: tests::Arrivals::default(),
        }
    }
}

impl Coalescer {
    /// Returns once a drain that started after this call began has finished.
    /// The caller has already staged its writes, so that drain covered them.
    /// This caller runs `drain` itself only when no drain is running and none
    /// that started after it has finished. A failed drain acknowledges nobody:
    /// its error returns to the thread that ran it, and each waiter drains
    /// again.
    pub(crate) fn flush(&self, drain: impl FnOnce() -> io::Result<()>) -> io::Result<()> {
        let mut progress = self.progress.lock().unwrap_or_else(PoisonError::into_inner);
        // The running drain may have started before this caller staged.
        let target = progress.started + 1;
        #[cfg(test)]
        let target = if self.join_running && progress.draining {
            progress.started
        } else {
            target
        };
        #[cfg(all(test, not(loom)))]
        self.arrivals.arrive();
        loop {
            if progress.completed >= target {
                return Ok(());
            }
            if !progress.draining {
                progress.started += 1;
                progress.draining = true;
                let number = progress.started;
                drop(progress);
                let result = drain();
                let mut progress = self.progress.lock().unwrap_or_else(PoisonError::into_inner);
                progress.draining = false;
                if result.is_ok() {
                    progress.completed = number;
                }
                drop(progress);
                self.drained.notify_all();
                return result;
            }
            progress = self
                .drained
                .wait(progress)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }
}

/// Stages `descriptor` with `stage`, then shares one `drain` of its device
/// with every concurrent request in this process and, through per-device
/// counters every process of this user maps, in every other process.
#[cfg(all(target_vendor = "apple", not(loom)))]
pub(crate) fn full(
    descriptor: std::os::fd::BorrowedFd<'_>,
    stage: impl FnOnce(std::os::fd::BorrowedFd<'_>) -> io::Result<()>,
    drain: impl FnOnce(std::os::fd::BorrowedFd<'_>) -> io::Result<()>,
) -> io::Result<()> {
    match stage(descriptor) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::Interrupted => return Err(error),
        // A descriptor that cannot be staged takes its own drain, which
        // reports its own failure.
        Err(_) => return drain(descriptor),
    }
    let Ok(Some(id)) = apple::local_device(descriptor) else {
        return drain(descriptor);
    };
    let device = apple::device(id);
    device
        .coalescer
        .flush(|| device.across_processes(|| drain(descriptor)))
}

#[cfg(all(target_vendor = "apple", not(loom)))]
mod apple {
    #![allow(unsafe_code, reason = "maps the device's shared drain counters")]
    use super::Coalescer;
    use crate::{FileLock, FileLockMode};
    use std::collections::BTreeMap;
    use std::io;
    use std::os::fd::AsRawFd as _;
    use std::os::unix::ffi::OsStrExt as _;
    use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _};
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    #[cfg(not(test))]
    use std::sync::{Mutex, OnceLock, PoisonError};

    pub(super) struct Device {
        pub(super) coalescer: Coalescer,
        /// This user's drain counters for the device, shared by every
        /// process. Without them, drains coalesce only within this process.
        shared: Option<Shared>,
    }

    /// The same two counters as `Coalescer`, in a file every process maps,
    /// with the file's lock as the drain lock.
    struct Shared {
        lock: FileLock,
        /// Drains started, and the latest that finished successfully, by any
        /// process. The mapping lives as long as the process.
        started: &'static AtomicU64,
        completed: &'static AtomicU64,
    }

    #[cfg(not(test))]
    pub(super) fn device(id: u64) -> Arc<Device> {
        static DEVICES: OnceLock<Mutex<BTreeMap<u64, Arc<Device>>>> = OnceLock::new();
        let mut devices = DEVICES
            .get_or_init(Mutex::default)
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        Arc::clone(devices.entry(id).or_insert_with(|| {
            Arc::new(Device {
                coalescer: Coalescer::default(),
                shared: shared(id).ok(),
            })
        }))
    }

    /// Unit tests observe the exact native calls of their own thread, so
    /// each test thread shares drains only with itself. Sharing across
    /// threads is proved on `Coalescer`, and across processes below.
    #[cfg(test)]
    pub(super) fn device(id: u64) -> Arc<Device> {
        thread_local! {
            static DEVICES: std::cell::RefCell<BTreeMap<u64, Arc<Device>>> =
                const { std::cell::RefCell::new(BTreeMap::new()) };
        }
        DEVICES.with_borrow_mut(|devices| {
            Arc::clone(devices.entry(id).or_insert_with(|| {
                Arc::new(Device {
                    coalescer: Coalescer::default(),
                    shared: None,
                })
            }))
        })
    }

    /// The local device holding `descriptor`'s file, or `None` on a
    /// filesystem with no local device, such as an NFS or FUSE-T view. There
    /// a full flush is the filesystem's own request, never a device drain
    /// another file's request could share, and every mount has a fresh
    /// device number: tracking them would grow the device table and leave a
    /// counter file behind per mount.
    pub(super) fn local_device(descriptor: std::os::fd::BorrowedFd<'_>) -> io::Result<Option<u64>> {
        let mut filesystem = std::mem::MaybeUninit::<libc::statfs>::uninit();
        // SAFETY: fstatfs fills `filesystem` from the held live descriptor.
        if unsafe { libc::fstatfs(descriptor.as_raw_fd(), filesystem.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: fstatfs succeeded, so it initialized `filesystem`.
        let flags = unsafe { filesystem.assume_init() }.f_flags;
        if flags & libc::MNT_LOCAL.cast_unsigned() == 0 {
            return Ok(None);
        }
        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        // SAFETY: fstat fills `stat` from the held live descriptor.
        if unsafe { libc::fstat(descriptor.as_raw_fd(), stat.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: fstat succeeded, so it initialized `stat`.
        Ok(Some(
            i64::from(unsafe { stat.assume_init() }.st_dev).cast_unsigned(),
        ))
    }

    const COUNTERS: usize = 2 * size_of::<u64>();

    /// The device's counter file in this user's private temporary directory,
    /// as the system names it for the user, so every process of the user
    /// finds the same file whatever its environment says.
    fn counter_path(id: u64) -> io::Result<PathBuf> {
        let mut directory = [0_u8; libc::PATH_MAX as usize];
        // SAFETY: confstr writes at most `directory.len()` bytes and returns
        // the length the whole value needs, its NUL included.
        let needed = unsafe {
            libc::confstr(
                libc::_CS_DARWIN_USER_TEMP_DIR,
                directory.as_mut_ptr().cast(),
                directory.len(),
            )
        };
        let directory = match needed {
            0 => return Err(io::Error::last_os_error()),
            needed => directory
                .get(..needed - 1)
                .ok_or(io::ErrorKind::InvalidData)?,
        };
        Ok(PathBuf::from(std::ffi::OsStr::from_bytes(directory))
            .join(format!("acyclic-device-v1-{id:016x}.drain")))
    }

    /// Maps the device's counter file. A file another user could write
    /// would let them forge a finished drain, so anything not owned by this
    /// user, or open to anyone else, is refused.
    fn shared(id: u64) -> io::Result<Shared> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(counter_path(id)?)?;
        let metadata = file.metadata()?;
        if metadata.uid() != rustix::process::geteuid().as_raw() || metadata.mode() & 0o077 != 0 {
            return Err(io::ErrorKind::PermissionDenied.into());
        }
        // Growing a new file zeroes its counters. Growing to the same size
        // changes nothing, so a concurrent opener cannot reset them.
        if metadata.len() < COUNTERS as u64 {
            file.set_len(COUNTERS as u64)?;
        }
        // SAFETY: a fresh shared mapping of the file's first bytes, which
        // hold the counters; it is never unmapped.
        let page = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                COUNTERS,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                0,
            )
        };
        if page == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        let counters = page.cast::<AtomicU64>();
        // SAFETY: the mapping is page aligned, `COUNTERS` long, lives for the
        // process, and every process accesses it only atomically.
        let (started, completed) = unsafe { (&*counters, &*counters.add(1)) };
        Ok(Shared {
            lock: FileLock::new(file),
            started,
            completed,
        })
    }

    impl Device {
        /// Runs `drain` unless a drain by any process, started after this
        /// call began, has finished once this caller holds the drain lock.
        pub(super) fn across_processes(
            &self,
            drain: impl FnOnce() -> io::Result<()>,
        ) -> io::Result<()> {
            match &self.shared {
                Some(shared) => shared.settle(shared.ticket(), drain),
                None => drain(),
            }
        }
    }

    impl Shared {
        /// Every drain numbered above the count read here stores its number
        /// before it starts, so it starts after this read.
        fn ticket(&self) -> u64 {
            self.started.load(Ordering::SeqCst) + 1
        }

        fn settle(&self, ticket: u64, drain: impl FnOnce() -> io::Result<()>) -> io::Result<()> {
            // Waits in the kernel while another process drains.
            self.lock.lock(FileLockMode::Exclusive)?;
            let result = (|| {
                if self.completed.load(Ordering::SeqCst) >= ticket {
                    return Ok(());
                }
                // A process that died mid-drain left `started` ahead of
                // `completed`; numbers only grow.
                let number = self.started.load(Ordering::SeqCst) + 1;
                self.started.store(number, Ordering::SeqCst);
                drain()?;
                self.completed.store(number, Ordering::SeqCst);
                Ok(())
            })();
            result.and(self.lock.unlock())
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::io::{BufRead as _, Read as _, Write as _};
        use std::os::unix::fs::PermissionsExt as _;
        use std::process::{Child, ChildStdout, Command, Stdio};

        const LOCK_CHILD: &str = "ACYCLIC_DEVICE_DRAIN_LOCK_CHILD";

        struct DrainProcess {
            child: Child,
            output: io::BufReader<ChildStdout>,
        }

        impl DrainProcess {
            fn spawn(id: u64, mode: &str) -> io::Result<Self> {
                let mut child = Command::new(std::env::current_exe()?)
                    .args([
                        "--exact",
                        "device_flush::apple::tests::child_holds_or_waits_for_drain",
                        "--ignored",
                        "--nocapture",
                    ])
                    .env(LOCK_CHILD, format!("{id:016x}:{mode}"))
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .spawn()?;
                let output =
                    io::BufReader::new(child.stdout.take().ok_or(io::ErrorKind::BrokenPipe)?);
                Ok(Self { child, output })
            }

            fn event(&mut self, expected: &str) -> io::Result<()> {
                let mut line = String::new();
                loop {
                    line.clear();
                    if self.output.read_line(&mut line)? == 0 {
                        return Err(io::Error::other(format!(
                            "drain process ended before {expected}"
                        )));
                    }
                    if line.trim_end().ends_with(expected) {
                        return Ok(());
                    }
                }
            }
        }

        impl Drop for DrainProcess {
            fn drop(&mut self) {
                let _ = self.child.kill();
                let _ = self.child.wait();
            }
        }

        fn event(name: &str) -> io::Result<()> {
            println!("{name}");
            io::stdout().flush()
        }

        #[test]
        #[ignore = "subprocess of a_waiting_process_recovers_when_the_drain_holder_exits"]
        fn child_holds_or_waits_for_drain() -> io::Result<()> {
            let arguments = std::env::var(LOCK_CHILD).map_err(io::Error::other)?;
            let (id, mode) = arguments
                .split_once(':')
                .ok_or(io::ErrorKind::InvalidInput)?;
            let shared = shared(u64::from_str_radix(id, 16).map_err(io::Error::other)?)?;
            let ticket = shared.ticket();
            if mode == "hold" {
                shared.settle(ticket, || {
                    event("DRAIN_HELD")?;
                    io::stdin().read_exact(&mut [0])
                })
            } else {
                assert_eq!(
                    shared
                        .lock
                        .try_lock(FileLockMode::Exclusive)
                        .err()
                        .map(|error| error.kind()),
                    Some(io::ErrorKind::WouldBlock),
                    "another process must exclude this drain"
                );
                event("DRAIN_BLOCKED")?;
                shared.settle(ticket, || event("DRAIN_RECOVERED"))
            }
        }

        /// The real process lock excludes a competing drain. Killing its
        /// holder releases the wait without certifying the unfinished drain.
        #[test]
        fn a_waiting_process_recovers_when_the_drain_holder_exits() -> io::Result<()> {
            let id = (u64::from(std::process::id()) << 32) | 0xdead_fade;
            let path = counter_path(id)?;
            let result = (|| {
                let shared = shared(id)?;
                let before = shared.started.load(Ordering::SeqCst);
                let completed = shared.completed.load(Ordering::SeqCst);
                let mut holder = DrainProcess::spawn(id, "hold")?;
                holder.event("DRAIN_HELD")?;
                let mut waiter = DrainProcess::spawn(id, "wait")?;
                waiter.event("DRAIN_BLOCKED")?;
                assert_eq!(shared.started.load(Ordering::SeqCst), before + 1);
                assert_eq!(shared.completed.load(Ordering::SeqCst), completed);
                holder.child.kill()?;
                holder.child.wait()?;
                waiter.event("DRAIN_RECOVERED")?;
                assert!(waiter.child.wait()?.success());
                assert_eq!(shared.started.load(Ordering::SeqCst), before + 2);
                assert_eq!(shared.completed.load(Ordering::SeqCst), before + 2);
                Ok(())
            })();
            std::fs::remove_file(path)?;
            result
        }

        /// Two mappings stand for two processes. A ticket taken before
        /// another process's drain started is settled by that drain; one
        /// taken while it ran is not (the negative control). A counter file
        /// anyone else could write is refused.
        #[test]
        fn processes_share_a_drain_that_started_after_their_ticket() -> io::Result<()> {
            let id = (u64::from(std::process::id()) << 32) | 0xdead_beef;
            let path = counter_path(id)?;
            let result = (|| {
                let (first, second) = (shared(id)?, shared(id)?);
                let drains = std::cell::Cell::new(0);
                let drain = || {
                    drains.set(drains.get() + 1);
                    Ok(())
                };
                let early = second.ticket();
                first.settle(first.ticket(), drain)?;
                second.settle(early, drain)?;
                assert_eq!(
                    drains.get(),
                    1,
                    "a drain started after the ticket covers it"
                );
                let mut late = 0;
                first.settle(first.ticket(), || {
                    late = second.ticket();
                    drain()
                })?;
                second.settle(late, drain)?;
                assert_eq!(drains.get(), 3, "a drain running at the ticket does not");
                assert_eq!(second.completed.load(Ordering::SeqCst), 3);
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o620))?;
                assert_eq!(
                    shared(id).err().map(|error| error.kind()),
                    Some(io::ErrorKind::PermissionDenied)
                );
                Ok(())
            })();
            std::fs::remove_file(&path)?;
            result
        }

        const CHILD: &str = "ACYCLIC_DEVICE_FLUSH_CHILD";

        /// Run only as the child of the test below, in its own process.
        #[test]
        #[ignore = "a child process of processes_with_their_own_tmpdir_share_one_drain"]
        fn child_settles_one_drain() -> io::Result<()> {
            let id = std::env::var(CHILD).map_err(io::Error::other)?;
            let shared = shared(u64::from_str_radix(&id, 16).map_err(io::Error::other)?)?;
            shared.settle(shared.ticket(), || Ok(()))
        }

        /// A process with its own `TMPDIR` drains after this process takes
        /// its ticket; that drain covers this process. With the counters
        /// under `TMPDIR` the two would never meet and this process would
        /// drain again (the negative control is this test on that layout).
        #[test]
        fn processes_with_their_own_tmpdir_share_one_drain() -> io::Result<()> {
            let id = (u64::from(std::process::id()) << 32) | 0x7e57_d1a0;
            let own = tempfile::tempdir()?;
            let result = (|| {
                let parent = shared(id)?;
                let ticket = parent.ticket();
                let child = std::process::Command::new(std::env::current_exe()?)
                    .args([
                        "--exact",
                        "device_flush::apple::tests::child_settles_one_drain",
                        "--ignored",
                        "--test-threads=1",
                    ])
                    .env(CHILD, format!("{id:016x}"))
                    .env("TMPDIR", own.path())
                    .output()?;
                assert!(
                    child.status.success()
                        && String::from_utf8_lossy(&child.stdout).contains("1 passed"),
                    "child failed: {child:?}"
                );
                let mut drains = 0;
                parent.settle(ticket, || {
                    drains += 1;
                    Ok(())
                })?;
                assert_eq!(drains, 0, "the other process's drain covers this ticket");
                Ok(())
            })();
            std::fs::remove_file(counter_path(id)?)?;
            result
        }
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Lets a test hold the first drain until every request has registered.
    #[derive(Default)]
    pub(super) struct Arrivals {
        count: std::sync::Mutex<usize>,
        changed: std::sync::Condvar,
    }

    impl Arrivals {
        pub(super) fn arrive(&self) {
            *self.count.lock().unwrap() += 1;
            self.changed.notify_all();
        }

        fn wait_for(&self, count: usize) {
            let mut arrived = self.count.lock().unwrap();
            while *arrived < count {
                arrived = self.changed.wait(arrived).unwrap();
            }
        }
    }

    #[test]
    fn one_request_drains_once_and_concurrent_requests_share_the_next_drain() {
        let lone = Coalescer::default();
        let drains = AtomicUsize::new(0);
        lone.flush(|| {
            drains.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
        assert_eq!(drains.load(Ordering::SeqCst), 1);

        const REQUESTS: usize = 64;
        let shared = Arc::new(Coalescer::default());
        let drains = Arc::new(AtomicUsize::new(0));
        let threads: Vec<_> = (0..REQUESTS)
            .map(|_| {
                let (shared, drains) = (Arc::clone(&shared), Arc::clone(&drains));
                std::thread::spawn(move || {
                    let coalescer = Arc::clone(&shared);
                    shared.flush(|| {
                        // The first drain runs until every request is queued
                        // behind it; all of them then share the second.
                        if drains.fetch_add(1, Ordering::SeqCst) == 0 {
                            coalescer.arrivals.wait_for(REQUESTS);
                        }
                        Ok(())
                    })
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap().unwrap();
        }
        assert_eq!(drains.load(Ordering::SeqCst), 2);
    }
}

#[cfg(loom)]
mod model {
    use super::*;
    use loom::sync::Arc;
    use loom::sync::atomic::{AtomicUsize, Ordering};

    /// Each thread stages one write, then flushes. A drain covers exactly
    /// the writes staged when it starts. Thread 0's drain may fail.
    fn requests(join_running: bool, fail_first: bool) {
        let mut builder = loom::model::Builder::new();
        builder.preemption_bound = Some(3);
        builder.check(move || {
            let coalescer = Arc::new(Coalescer {
                join_running,
                ..Coalescer::default()
            });
            let staged = Arc::new(AtomicUsize::new(0));
            let durable = Arc::new(AtomicUsize::new(0));
            let uncovered = Arc::new(AtomicUsize::new(0));
            let violations = Arc::clone(&uncovered);
            let request = move |id: usize| {
                let (coalescer, staged, durable, uncovered) = (
                    Arc::clone(&coalescer),
                    Arc::clone(&staged),
                    Arc::clone(&durable),
                    Arc::clone(&uncovered),
                );
                move || {
                    staged.fetch_or(1 << id, Ordering::SeqCst);
                    let result = coalescer.flush(|| {
                        let covered = staged.load(Ordering::SeqCst);
                        if fail_first && id == 0 {
                            return Err(io::ErrorKind::Other.into());
                        }
                        durable.fetch_or(covered, Ordering::SeqCst);
                        Ok(())
                    });
                    if result.is_ok() && durable.load(Ordering::SeqCst) & (1 << id) == 0 {
                        uncovered.fetch_add(1, Ordering::SeqCst);
                    }
                }
            };
            let first = loom::thread::spawn(request(0));
            let second = loom::thread::spawn(request(1));
            request(2)();
            first.join().unwrap();
            second.join().unwrap();
            assert_eq!(
                violations.load(Ordering::SeqCst),
                0,
                "acknowledged before a covering drain"
            );
        });
    }

    #[test]
    fn every_acknowledgement_follows_a_drain_that_started_after_its_write() {
        requests(false, false);
    }

    #[test]
    fn a_failed_drain_acknowledges_nobody_and_strands_nobody() {
        requests(false, true);
    }

    #[test]
    #[should_panic(expected = "acknowledged before a covering drain")]
    fn negative_control_joining_the_running_drain_acknowledges_uncovered_writes() {
        requests(true, false);
    }
}
