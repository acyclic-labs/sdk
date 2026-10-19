use super::*;
use std::io;
use std::os::fd::{AsRawFd as _, FromRawFd as _};
use std::os::unix::process::CommandExt as _;
use std::process::{Child, Command, ExitStatus};

#[allow(
    unsafe_code,
    reason = "the fork-inheritance control needs CLOEXEC pipes and an async-signal-safe pre_exec barrier"
)]
fn pipe() -> io::Result<(File, File)> {
    let mut descriptors = [-1; 2];
    // SAFETY: descriptors provides two writable slots. CLOEXEC keeps these
    // fixture endpoints out of the executed program.
    if unsafe { libc::pipe2(descriptors.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful pipe2 returned two distinct, otherwise unowned fds.
    Ok(unsafe {
        (
            File::from_raw_fd(descriptors[0]),
            File::from_raw_fd(descriptors[1]),
        )
    })
}

struct ForkControl {
    ready: File,
    release: File,
    spawning: Option<std::thread::JoinHandle<io::Result<Child>>>,
}

impl ForkControl {
    #[allow(
        unsafe_code,
        reason = "only async-signal-safe descriptor operations may run between fork and exec"
    )]
    fn start() -> io::Result<Self> {
        let (ready, child_ready) = pipe()?;
        let (child_release, release) = pipe()?;
        let mut control = Self {
            ready,
            release,
            spawning: None,
        };
        // Retain both parent endpoints until spawn returns: pre_exec closes
        // their raw descriptor numbers even if this thread starts unwinding
        // before the spawning thread has forked.
        let unused_ready_fd = control.ready.as_raw_fd();
        let unused_release_fd = control.release.as_raw_fd();
        control.spawning = Some(
            std::thread::Builder::new()
                .name("core-state-fork-control".to_owned())
                .spawn(move || {
                    let ready_fd = child_ready.as_raw_fd();
                    let release_fd = child_release.as_raw_fd();
                    let mut command = Command::new("/bin/true");
                    // SAFETY: pipe owners live through spawn. The child uses
                    // only stack bytes and async-signal-safe libc operations;
                    // no allocation, locks, destructors or runtime calls.
                    unsafe {
                        command.pre_exec(move || {
                            // CLOEXEC has not acted yet. Without these closes
                            // the child would retain its own release writer,
                            // preventing EOF if the parent disappears.
                            if libc::close(unused_ready_fd) != 0
                                || libc::close(unused_release_fd) != 0
                            {
                                libc::_exit(124);
                            }
                            let mut byte = 1_u8;
                            loop {
                                let count = libc::write(ready_fd, (&raw const byte).cast(), 1);
                                if count == 1 {
                                    break;
                                }
                                if count >= 0 || *libc::__errno_location() != libc::EINTR {
                                    libc::_exit(125);
                                }
                            }
                            loop {
                                let count = libc::read(release_fd, (&raw mut byte).cast(), 1);
                                if count == 1 && byte == 1 {
                                    return Ok(());
                                }
                                if count >= 0 || *libc::__errno_location() != libc::EINTR {
                                    libc::_exit(126);
                                }
                            }
                        });
                    }
                    // Command::spawn waits for exec; it must not run on the
                    // thread responsible for releasing the pre_exec barrier.
                    let child = command.spawn();
                    drop(child_ready);
                    drop(child_release);
                    child
                })?,
        );
        control.await_child()?;
        Ok(control)
    }

    #[allow(unsafe_code, reason = "bounded poll over one owned fixture pipe")]
    fn await_child(&mut self) -> io::Result<()> {
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(io::Error::new(io::ErrorKind::TimedOut, "pre_exec barrier"));
            }
            let mut descriptor = libc::pollfd {
                fd: self.ready.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            // SAFETY: the owned reader remains valid for this one-pollfd call.
            let ready = unsafe {
                libc::poll(
                    &mut descriptor,
                    1,
                    remaining.as_millis().max(1) as libc::c_int,
                )
            };
            if ready < 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(error);
            }
            if ready == 0 {
                continue;
            }
            let mut byte = [0];
            self.ready.read_exact(&mut byte)?;
            return if byte == [1] {
                Ok(())
            } else {
                Err(io::Error::new(io::ErrorKind::InvalidData, "pre_exec signal"))
            };
        }
    }

    fn finish(&mut self) -> Result<ExitStatus, String> {
        let released = self.release.write_all(&[1]);
        let reaped = self.join_and_reap();
        let status = reaped?;
        released.map_err(|error| format!("release child to exec: {error}"))?;
        Ok(status)
    }

    fn join_and_reap(&mut self) -> Result<ExitStatus, String> {
        let spawning = self
            .spawning
            .take()
            .ok_or_else(|| "barrier controller already joined".to_owned())?;
        let mut child = spawning
            .join()
            .map_err(|_| "barrier controller panicked".to_owned())?
            .map_err(|error| format!("exec true: {error}"))?;
        match child.wait() {
            Ok(status) => Ok(status),
            Err(error) => {
                let killed = child.kill();
                let reaped = child.wait();
                Err(format!(
                    "wait true: {error}; cleanup kill: {killed:?}; cleanup wait: {reaped:?}"
                ))
            }
        }
    }
}

impl Drop for ForkControl {
    fn drop(&mut self) {
        if self.spawning.is_some() {
            if let Err(error) = self.finish() {
                // Cleanup must not cause a second panic during an unwind.
                let _ = writeln!(io::stderr(), "fork control cleanup failed: {error}");
            }
        }
    }
}

#[test]
fn ordinary_owner_releases_only_after_clones_and_admitted_transactions_drop() {
    let directory = tempfile::tempdir().unwrap();
    let owner = LocalCoreStateStore::open_owned(directory.path()).unwrap();
    let clone = owner.clone();
    let admitted = clone.namespace().unwrap();
    let mut control = ForkControl::start().expect("child reached pre_exec");
    let live_conflict = is_ownership_conflict(&LocalCoreStateStore::open_owned(directory.path()));
    drop(owner);
    let clone_conflict = is_ownership_conflict(&LocalCoreStateStore::open_owned(directory.path()));
    drop(clone);
    let admission_conflict =
        is_ownership_conflict(&LocalCoreStateStore::open_owned(directory.path()));
    drop(admitted);
    let dropped_error = LocalCoreStateStore::open_owned(directory.path()).err();
    let status = control.finish().expect("release, join and reap child");
    assert!(status.success(), "true status: {status}");
    assert!(live_conflict, "live owner must exclude another owner");
    assert!(clone_conflict, "a store clone must retain exclusive ownership");
    assert!(admission_conflict, "an admitted transaction must retain its owner Arc");
    LocalCoreStateStore::open_owned(directory.path()).expect("exec closed inherited descriptors");
    assert!(
        dropped_error.is_none(),
        "last owner/transaction drop must release before child exec: {dropped_error:?}"
    );
}

#[test]
fn ordinary_shared_transaction_releases_before_a_forked_child_execs() {
    let directory = tempfile::tempdir().unwrap();
    let store = LocalCoreStateStore::new(directory.path());
    let (mut control, live_conflict) = futures::executor::block_on(store.transaction(|namespace| {
        let control = ForkControl::start()?;
        let live_conflict = is_ownership_conflict(&LocalCoreStateStore::open_owned(&namespace.root));
        Ok((control, live_conflict))
    }))
    .unwrap();
    let released_error = LocalCoreStateStore::open_owned(directory.path()).err();
    let status = control.finish().expect("release, join and reap child");
    assert!(status.success(), "true status: {status}");
    assert!(live_conflict, "a live shared transaction must exclude an owner");
    LocalCoreStateStore::open_owned(directory.path()).expect("exec closed inherited descriptors");
    assert!(
        released_error.is_none(),
        "completed shared transaction must release before child exec: {released_error:?}"
    );
}

fn record_release_control(unwind: bool) {
    let directory = tempfile::tempdir().unwrap();
    let namespace = LocalCoreStateStore::new(directory.path()).namespace().unwrap();
    let mut paths = namespace.record("logical-release", &[1; 16]).unwrap();
    let mut probe = namespace.record("logical-release", &[1; 16]).unwrap();
    let mut control = None;
    let mut live_conflict = false;
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        with_lock_with_retry(
            &mut paths,
            |_| {
                control = Some(ForkControl::start()?);
                live_conflict = matches!(
                    with_lock_with_retry(&mut probe, |_| Ok(()), |_| false),
                    Err(LocalCoreStateStoreError::LockTimeout(_))
                );
                assert!(!unwind, "fixture action unwinds while holding its record lock");
                Ok(())
            },
            |_| false,
        )
    }));
    let released_error = with_lock_with_retry(&mut probe, |_| Ok(()), |_| false).err();
    let status = control
        .as_mut()
        .expect("record action reached pre_exec")
        .finish()
        .expect("release, join and reap child");
    assert!(status.success(), "true status: {status}");
    if unwind {
        assert!(outcome.is_err(), "fixture must exercise the real action-unwind path");
    } else {
        outcome.expect("normal action must not panic").unwrap();
    }
    assert!(live_conflict, "a live record action must exclude another writer");
    with_lock_with_retry(&mut probe, |_| Ok(()), |_| false)
        .expect("exec closed the inherited record descriptor");
    assert!(
        released_error.is_none(),
        "record action end (unwind={unwind}) must release before child exec: {released_error:?}"
    );
}

#[test]
fn ordinary_record_explicit_release_precedes_a_forked_child_exec() {
    record_release_control(false);
}

#[test]
fn ordinary_record_unwind_releases_before_a_forked_child_execs() {
    record_release_control(true);
}

#[test]
fn native_funded_transaction_drop_releases_once_before_child_exec() {
    let directory = tempfile::tempdir().unwrap();
    let cancellation = crate::CancellationToken::new();
    let budget = super::native::finite_budget();
    let work = crate::NativeMaterializationWork::new(budget, &cancellation).unwrap();
    let store = LocalCoreStateStore::new(directory.path()).materialization(work.clone());
    let namespace = store.namespace().unwrap();
    let mut control = ForkControl::start().expect("child reached pre_exec");
    let live_conflict = is_ownership_conflict(&LocalCoreStateStore::open_owned(directory.path()));
    let before = work.receipt();
    drop(namespace);
    let after = work.receipt();
    let released_error = LocalCoreStateStore::open_owned(directory.path()).err();
    let status = control.finish().expect("release, join and reap child");
    assert!(status.success(), "true status: {status}");
    assert!(live_conflict, "live native admission must exclude an owner");
    assert!(released_error.is_none(), "native cleanup must unlock: {released_error:?}");
    assert_eq!(after.backend_write_operations - before.backend_write_operations, 1);
    after.verify(budget).unwrap();
}

#[test]
fn explicit_native_release_does_not_unlock_a_reacquired_file_description_on_drop() {
    let directory = tempfile::tempdir().unwrap();
    let cancellation = crate::CancellationToken::new();
    let budget = super::native::finite_budget();
    let work = crate::NativeMaterializationWork::new(budget, &cancellation).unwrap();
    let store = LocalCoreStateStore::new(directory.path()).materialization(work.clone());
    let namespace = store.namespace().unwrap();
    let mut paths = namespace.record("logical-release", &[2; 16]).unwrap();
    let mut lock = acquire_lock(&mut paths, LockMode::Exclusive).unwrap();
    // dup shares the same Linux open file description. Reacquiring through it
    // makes even an uncharged, duplicate unlock in OwnerLock::drop observable.
    let same_description = lock.0.try_clone().unwrap();
    let probe = OpenOptions::new().read(true).write(true).open(&paths.lock).unwrap();
    let before = work.receipt();
    lock.unlock().unwrap();
    let after_unlock = work.receipt();
    same_description.try_lock_exclusive().unwrap();
    drop(lock);
    let after_drop = work.receipt();
    let exclusion = probe.try_lock_exclusive();
    FileExt::unlock(&same_description).unwrap();
    assert!(
        exclusion.as_ref().is_err_and(acyclic_native_runtime::is_exclusive_lock_contention),
        "dropping an explicitly released guard must not issue another unlock: {exclusion:?}"
    );
    probe.try_lock_exclusive().unwrap();
    FileExt::unlock(&probe).unwrap();
    assert_eq!(
        after_unlock.backend_write_operations - before.backend_write_operations,
        1
    );
    assert_eq!(after_drop, after_unlock, "released drop must spend no further work");
    after_drop.verify(budget).unwrap();
}

#[test]
fn explicit_ordinary_release_does_not_unlock_a_reacquired_file_description_on_drop() {
    let directory = tempfile::tempdir().unwrap();
    let namespace = LocalCoreStateStore::new(directory.path()).namespace().unwrap();
    let mut paths = namespace.record("logical-release", &[3; 16]).unwrap();
    let mut lock = acquire_lock(&mut paths, LockMode::Exclusive).unwrap();
    let same_description = lock.0.try_clone().unwrap();
    let probe = OpenOptions::new().read(true).write(true).open(&paths.lock).unwrap();
    lock.unlock().unwrap();
    same_description.try_lock_exclusive().unwrap();
    drop(lock);
    let exclusion = probe.try_lock_exclusive();
    FileExt::unlock(&same_description).unwrap();
    assert!(
        exclusion.as_ref().is_err_and(acyclic_native_runtime::is_exclusive_lock_contention),
        "ordinary released drop must not unlock a new holder: {exclusion:?}"
    );
    probe.try_lock_exclusive().unwrap();
    FileExt::unlock(&probe).unwrap();
}
