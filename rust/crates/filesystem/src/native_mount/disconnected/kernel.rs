use crate::sync::native::{Condvar, Mutex};
use rustix::fs::{AtFlags, Statx};
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::fs::MetadataExt;
use std::os::unix::process::ExitStatusExt;
use std::sync::Arc;

// Lock order: owner state -> one namespace, flock or opener-gate lock. Kernel
// locks never nest or call an owner/ledger. Fixture metadata is immutable before
// actors start; it is not a synchronization dependency between syscalls.
// Arc owns only the fake syscall fixture; all protocol synchronization, including
// the production recovery owner's Arc, remains instrumented by the native shim.
loom::lazy_static! {
    static ref CONTEXT: Arc<Kernel> = Arc::new(Kernel::new());
}

pub(in crate::native_mount) struct Kernel {
    fixture: std::sync::OnceLock<Fixture>,
    pinned: std::sync::OnceLock<(RawFd, u64)>,
    recovery_fence: std::sync::OnceLock<(u64, u64, RawFd)>,
    state: Mutex<State>,
    // Registry and destination flock state/wakeups are independent OS resources.
    locks: [Mutex<HashMap<(u64, u64), RawFd>>; 2],
    gate: Mutex<bool>,
    unlocked: [Condvar; 2],
    acquired: Condvar,
}
struct Fixture {
    base: Statx,
    helper_stat: rustix::fs::Stat,
    early_release: bool,
    verify_recovery: bool,
}
struct State {
    mount: Option<u64>,
    helper_running: bool,
    helper_exited: bool,
    early_releases: usize,
    live_detaches: usize,
    mount_before_exit: usize,
}

pub(in crate::native_mount) fn current() -> Option<Arc<Kernel>> {
    Some(Arc::clone(&CONTEXT))
}

impl Kernel {
    fn new() -> Self {
        Self {
            fixture: std::sync::OnceLock::new(),
            pinned: std::sync::OnceLock::new(),
            recovery_fence: std::sync::OnceLock::new(),
            state: Mutex::new(State {
                mount: Some(0),
                helper_running: false,
                helper_exited: false,
                early_releases: 0,
                live_detaches: 0,
                mount_before_exit: 0,
            }),
            locks: std::array::from_fn(|_| Mutex::new(HashMap::new())),
            gate: Mutex::new(false),
            unlocked: std::array::from_fn(|_| Condvar::new()),
            acquired: Condvar::new(),
        }
    }

    pub(super) fn install(
        base: Statx,
        helper_stat: rustix::fs::Stat,
        early_release: bool,
        verify_recovery: bool,
    ) -> Arc<Self> {
        let kernel = current().unwrap();
        assert!(
            kernel
                .fixture
                .set(Fixture {
                    base,
                    helper_stat,
                    early_release,
                    verify_recovery
                })
                .is_ok()
        );
        kernel
    }

    pub(in crate::native_mount) fn early_release(&self) -> bool {
        self.fixture.get().unwrap().early_release
    }

    pub(in crate::native_mount) fn stat(&self, fd: RawFd, flags: AtFlags) -> io::Result<Statx> {
        // The probe descriptor is opened before actors start. Its held mount
        // identity never follows namespace remounts, exactly like O_PATH.
        let (pinned_fd, mount) = *self
            .pinned
            .get_or_init(|| (fd, self.state.lock().unwrap().mount.unwrap_or(2)));
        assert_eq!(pinned_fd, fd, "the model has one held probe descriptor");
        if flags.contains(AtFlags::STATX_FORCE_SYNC) && mount == 0 {
            return Err(io::Error::from_raw_os_error(libc::ENOTCONN));
        }
        let mut stat = self.fixture.get().unwrap().base;
        stat.stx_mnt_id = mount;
        Ok(stat)
    }

    pub(in crate::native_mount) fn current_stat(&self) -> Statx {
        let state = self.state.lock().unwrap();
        let mut stat = self.fixture.get().unwrap().base;
        stat.stx_mnt_id = state.mount.unwrap_or(2);
        stat
    }

    fn identity(file: &File) -> (u64, u64) {
        let metadata = file.metadata().unwrap();
        (metadata.dev(), metadata.ino())
    }

    pub(in crate::native_mount) fn lock_domain(file: &File) -> usize {
        let path = std::fs::read_link(format!("/proc/self/fd/{}", file.as_raw_fd())).unwrap();
        usize::from(
            path.file_name() != Some(std::ffi::OsStr::new(super::super::NATIVE_REGISTRY_NAME)),
        )
    }

    pub(in crate::native_mount) fn try_lock(&self, file: &File, domain: usize) -> io::Result<()> {
        let identity = Self::identity(file);
        let mut locks = self.locks[domain].lock().unwrap();
        if let Some(owner) = locks.get(&identity) {
            if *owner == file.as_raw_fd() {
                return Ok(());
            }
            return Err(io::Error::from_raw_os_error(libc::EWOULDBLOCK));
        }
        locks.insert(identity, file.as_raw_fd());
        drop(locks);
        // Immutable observation of the actual first recovery fence. This is
        // captured before the competing helper-phase opener starts.
        if domain == 1 && self.fixture.get().unwrap().verify_recovery {
            self.recovery_fence
                .get_or_init(|| (identity.0, identity.1, file.as_raw_fd()));
        }
        Ok(())
    }

    pub(super) fn wait_for_helper(&self) {
        let mut ready = self.gate.lock().unwrap();
        while !*ready {
            ready = self.acquired.wait(ready).unwrap();
        }
    }

    pub(super) fn wait_for_retirement(&self) {
        for (locks, unlocked) in self.locks.iter().zip(&self.unlocked) {
            let mut owners = locks.lock().unwrap();
            while !owners.is_empty() {
                owners = unlocked.wait(owners).unwrap();
            }
        }
    }

    pub(in crate::native_mount) fn unlock(&self, file: &File, domain: usize) {
        let identity = Self::identity(file);
        let released = {
            let mut locks = self.locks[domain].lock().unwrap();
            if locks.get(&identity) == Some(&file.as_raw_fd()) {
                locks.remove(&identity);
                self.unlocked[domain].notify_all();
                true
            } else {
                false
            }
        };
        if released
            && domain == 1
            && self.recovery_fence.get() == Some(&(identity.0, identity.1, file.as_raw_fd()))
        {
            let mut state = self.state.lock().unwrap();
            if !state.helper_exited {
                state.early_releases += 1;
            }
        }
    }

    pub(in crate::native_mount) fn wait_for_unlock(&self, file: &File, domain: usize) {
        let identity = Self::identity(file);
        let mut locks = self.locks[domain].lock().unwrap();
        while locks
            .get(&identity)
            .is_some_and(|fd| *fd != file.as_raw_fd())
        {
            locks = self.unlocked[domain].wait(locks).unwrap();
        }
    }

    // The real startup fence acquisition calls this replacement mount syscall.
    pub(super) fn mount(&self) {
        let mut state = self.state.lock().unwrap();
        if state.helper_running {
            state.mount_before_exit += 1;
        }
        state.mount = Some(1);
    }

    pub(super) fn assert_safe(&self) {
        let (live_detaches, mount_before_exit, helper_running, early_releases) = {
            let state = self.state.lock().unwrap();
            (
                state.live_detaches,
                state.mount_before_exit,
                state.helper_running,
                state.early_releases,
            )
        };
        assert_eq!(
            live_detaches, 0,
            "helper detached a live remount after early fence release"
        );
        assert_eq!(
            mount_before_exit, 0,
            "remount acquired the fence before helper exit"
        );
        assert!(!helper_running, "helper custody was abandoned");
        assert_eq!(
            early_releases, 0,
            "recovery fence released before helper exit"
        );
    }

    pub(in crate::native_mount) fn helper_stat(&self) -> rustix::fs::Stat {
        self.fixture.get().unwrap().helper_stat
    }
}

// An OS-process handle at the existing closed-process syscall boundary. The
// production FuseRecoveryControl still owns spawn, wait, terminate and custody.
pub(in crate::native_mount) struct Helper {
    kernel: Option<Arc<Kernel>>,
    real: Option<acyclic_native_runtime::ProcessTree>,
    exited: bool,
    unmounted: bool,
}
impl From<acyclic_native_runtime::ProcessTree> for Helper {
    fn from(real: acyclic_native_runtime::ProcessTree) -> Self {
        Self {
            kernel: None,
            real: Some(real),
            exited: false,
            unmounted: false,
        }
    }
}
impl Helper {
    pub(in crate::native_mount) fn spawn_closed_measured(
        _program: &std::ffi::CStr,
        _args: &[&std::ffi::CStr],
        _meter: &dyn acyclic_native_runtime::NativeReadMeter,
        out: &mut Option<Self>,
    ) -> io::Result<()> {
        let kernel = current().ok_or_else(|| io::Error::other("missing modeled kernel"))?;
        {
            let mut state = kernel.state.lock().unwrap();
            state.helper_running = true;
            if kernel.fixture.get().unwrap().verify_recovery
                && kernel.recovery_fence.get().is_none()
            {
                state.early_releases += 1;
            }
        }
        // Admission and stale-identity paths have independent exhaustive cases.
        // Here custody is checked on every original-fence release, and the
        // competing opener explores the complete spawned-helper interval.
        *kernel.gate.lock().unwrap() = true;
        kernel.acquired.notify_all();
        *out = Some(Self {
            kernel: Some(kernel),
            real: None,
            exited: false,
            unmounted: false,
        });
        Ok(())
    }

    pub(in crate::native_mount) fn try_wait(
        &mut self,
        meter: Option<&dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> io::Result<Option<std::process::ExitStatus>> {
        if let Some(real) = &mut self.real {
            return real.try_wait(meter);
        }
        // The child can finish umount while still alive. A pending observation
        // makes the real wait loop retain custody across that interval. The
        // separate modeled mutex transitions already explore both sides of
        // unmount and exit; extra clock yields add only stuttering schedules.
        if !self.unmounted {
            let mut state = self.kernel.as_ref().unwrap().state.lock().unwrap();
            if state.mount == Some(1) {
                state.live_detaches += 1;
            }
            state.mount = None;
            self.unmounted = true;
            return Ok(None);
        }
        {
            let mut state = self.kernel.as_ref().unwrap().state.lock().unwrap();
            state.helper_running = false;
            state.helper_exited = true;
        }
        self.exited = true;
        Ok(Some(std::process::ExitStatus::from_raw(0)))
    }

    pub(in crate::native_mount) fn terminate(
        &mut self,
        meter: Option<&dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> io::Result<()> {
        if let Some(real) = &mut self.real {
            return real.terminate(meter);
        }
        {
            let mut state = self.kernel.as_ref().unwrap().state.lock().unwrap();
            state.helper_running = false;
            state.helper_exited = true;
        }
        self.exited = true;
        Ok(())
    }

    pub(in crate::native_mount) fn is_reaped(&self) -> bool {
        self.real
            .as_ref()
            .map_or(self.exited, |real| real.is_reaped())
    }
}
